(ns hive-weave.virtual-test
  "Virtual threads run the work; a gate bounds it.

   The pairing is the point: removing the thread ceiling is only safe when the
   ceiling that mattered moves onto the resource. These tests assert both
   halves - that the executor really is unbounded in threads, and that the gate
   in front of it still admits no more than its permits at once.

   Every test asserts on every JVM. An earlier version wrapped the bodies in
   `(when java-21+? ...)`, which on a pre-21 runner ran no assertions at all -
   and a test that asserts nothing is a test that cannot fail. The JVM-specific
   halves are now explicit branches, and the pool fallback is reached through
   `:virtual? false` rather than by being on an old JVM."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-weave.gate :as gate]
            [hive-weave.pool :as pool])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private java-21+? (pool/virtual-threads?))

(defn- peak-concurrency
  "Run `n` tasks through `submit`, each holding for a moment, and report the
   highest number observed running at the same time."
  [submit n]
  (let [live (atom 0)
        peak (atom 0)
        done (CountDownLatch. n)]
    (dotimes [_ n]
      (submit (fn []
                (let [now (swap! live inc)]
                  (swap! peak max now)
                  (Thread/sleep 20)
                  (swap! live dec)
                  (.countDown done)))))
    (.await done 60 TimeUnit/SECONDS)
    @peak))

(deftest virtual-threads-are-detected-not-assumed
  (testing "the predicate agrees with the running JVM"
    (let [major (parse-long (first (str/split (System/getProperty "java.version") #"[.-]")))]
      (is (= (>= major 21) java-21+?)))))

(deftest virtual-executor-is-available-or-refuses-clearly
  (if java-21+?
    (testing "200 blocking tasks run at once, which a 32-thread pool could not"
      (let [e (pool/virtual-executor)]
        (try
          (is (>= (peak-concurrency #(pool/submit! e %) 200) 100))
          (finally (pool/shutdown! e)))))
    (testing "an older JVM is told so, rather than quietly handed a pool"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Java 21"
                            (pool/virtual-executor))))))

(deftest io-executor-runs-the-work-on-either-jvm
  (doseq [virtual? (if java-21+? [true false] [false])]
    (let [e (pool/io-executor {:name "io-test" :virtual? virtual?})]
      (try
        (is (= :ran @(pool/submit! e (fn [] :ran)))
            (str "virtual? " virtual?))
        (is (false? (:shutdown? (pool/pool-stats e)))
            "stats must read the same shape whichever executor this is")
        (finally (pool/shutdown! e))))))

(deftest io-executor-falls-back-to-a-bounded-pool
  (testing ":virtual? false is a real ThreadPoolExecutor with the asked-for size"
    (let [e (pool/io-executor {:name "io-fallback" :virtual? false :fallback-size 8})]
      (try
        (is (= 8 (:max-pool-size (pool/pool-stats e))))
        (is (= :caller-runs (:rejection (pool/pool-stats e))))
        (finally (pool/shutdown! e))))))

(deftest a-gate-bounds-what-the-executor-would-not
  (testing "the permits, not the threads, decide how much runs at once"
    (let [e (pool/io-executor {:name "gated-io"})
          g (gate/gate {:name "io-gate" :permits 8 :timeout-ms 30000})
          peak (peak-concurrency #(pool/submit! e (fn [] (gate/gate-run g %))) 200)]
      (try
        (is (<= peak 8) (str "gate admitted " peak " at once, not 8"))
        (is (>= peak 2) "a gate that admits one at a time is a lock, not a bound")
        (finally (pool/shutdown! e))))))

(deftest the-pairing-keeps-every-task
  (testing "io-executor + gate queues rather than shedding"
    (let [e (pool/io-executor {:name "io-gated"})
          g (gate/gate {:name "keep-gate" :permits 4 :timeout-ms 30000})
          ran (atom 0)
          done (CountDownLatch. 50)]
      (try
        (dotimes [_ 50]
          (pool/submit! e (fn []
                            (gate/gate-run g (fn [] (swap! ran inc) (Thread/sleep 5)))
                            (.countDown done))))
        (is (.await done 60 TimeUnit/SECONDS))
        (is (= 50 @ran) "nothing was refused: the gate queues, it does not drop")
        (finally (pool/shutdown! e))))))
