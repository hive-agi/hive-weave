(ns hive-weave.virtual-test
  "Virtual threads run the work; a gate bounds it.

   The pairing is the point: removing the thread ceiling is only safe when the
   ceiling that mattered moves onto the resource. These tests assert both
   halves - that the executor really is unbounded in threads, and that the gate
   in front of it still admits no more than its permits at once."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-weave.gate :as gate]
            [hive-weave.pool :as pool])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private java-21+?
  (pool/virtual-threads?))

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
    (let [major (parse-long (first (clojure.string/split (System/getProperty "java.version") #"[.-]")))]
      (is (= (>= major 21) (pool/virtual-threads?))))))

(deftest io-executor-runs-the-work
  (let [e (pool/io-executor {:name "io-test"})]
    (try
      (is (= :ran @(pool/submit! e (fn [] :ran))))
      (is (false? (:shutdown? (pool/pool-stats e)))
          "stats must read the same shape whichever executor this is")
      (finally (pool/shutdown! e)))))

(deftest virtual-executor-does-not-ration-threads
  (when java-21+?
    (testing "200 blocking tasks run at once, which a 32-thread pool could not"
      (let [e (pool/virtual-executor)]
        (try
          (is (>= (peak-concurrency #(pool/submit! e %) 200) 100))
          (finally (pool/shutdown! e)))))))

(deftest a-gate-bounds-what-virtual-threads-would-not
  (when java-21+?
    (testing "the permits, not the threads, decide how much runs at once"
      (let [e (pool/virtual-executor)
            g (gate/gate {:name "virtual-gate" :permits 8 :timeout-ms 30000})
            peak (peak-concurrency #(pool/submit! e (fn [] (gate/gate-run g %))) 200)]
        (try
          (is (<= peak 8) (str "gate admitted " peak " at once, not 8"))
          (is (>= peak 2) "a gate that admits one at a time is a lock, not a bound")
          (finally (pool/shutdown! e)))))))

(deftest the-pairing-is-what-the-guidance-recommends
  (testing "io-executor + gate keeps every task, unlike a pool that sheds"
    (when java-21+?
      (let [e (pool/io-executor {:name "io-gated"})
            g (gate/gate {:name "io-gate" :permits 4 :timeout-ms 30000})
            ran (atom 0)
            done (CountDownLatch. 50)]
        (try
          (dotimes [_ 50]
            (pool/submit! e (fn []
                              (gate/gate-run g (fn [] (swap! ran inc) (Thread/sleep 5)))
                              (.countDown done))))
          (is (.await done 60 TimeUnit/SECONDS))
          (is (= 50 @ran) "nothing was refused: the gate queues, it does not drop")
          (finally (pool/shutdown! e)))))))
