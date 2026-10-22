(ns hive-weave.safe-cancel-test
  "A caller that stops waiting must free the pooled task's thread."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-weave.pool :as pool]
            [hive-weave.safe :as safe]
            [hive-weave.guarded :as guarded])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private bound-ms 500)

(defn- active [p] (:active (pool/pool-stats p)))

(defn- idle-within?
  "Poll until the pool has no active thread, up to `ms`."
  [p ms]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond (zero? (active p)) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (Thread/sleep 10) (recur))))))

(defn- slow-sleeper
  "Interrupt-respecting slow task; counts down `started` first."
  [^CountDownLatch started]
  (fn [] (.countDown started) (Thread/sleep 10000) :finished))

(defn- cooperative-spinner
  "Ignores interrupts entirely; stops only on the cancel token."
  [^CountDownLatch started]
  (fn []
    (.countDown started)
    (loop [] (when-not (some-> safe/*cancel* deref) (recur)))
    :stopped))

(defn- cancel-caller-of!
  "Run (call-fn task) on a caller future, wait for the task to start, then
   future-cancel the caller."
  [call-fn task ^CountDownLatch started]
  (let [caller (future (call-fn task))]
    (is (.await started 2000 TimeUnit/MILLISECONDS) "task started")
    (future-cancel caller)
    caller))

(deftest caller-interrupt-cancels-pooled-future
  (let [p       (pool/make-pool {:name "cancel-int" :size 1})
        started (CountDownLatch. 1)]
    (try
      (cancel-caller-of! #(safe/safe-future-call {:timeout-ms 60000 :pool p} %)
                         (slow-sleeper started) started)
      (is (idle-within? p bound-ms)
          (str "pool thread still busy " bound-ms " ms after caller cancel"))
      (testing "pool stays reusable"
        (is (= (r/ok 42) (safe/safe-future-call {:timeout-ms 1000 :pool p} (constantly 42)))))
      (finally (pool/shutdown! p)))))

(deftest timeout-flips-cancel-token-for-interrupt-ignoring-work
  (let [p       (pool/make-pool {:name "cancel-tok" :size 1})
        started (CountDownLatch. 1)]
    (try
      (is (= :weave/timeout
             (:error (safe/safe-future-call {:timeout-ms 50 :pool p}
                                            (cooperative-spinner started)))))
      (is (idle-within? p bound-ms)
          (str "pool thread still busy " bound-ms " ms after timeout"))
      (is (= (r/ok :again) (safe/safe-future-call {:timeout-ms 1000 :pool p} (constantly :again))))
      (finally (pool/shutdown! p)))))

(deftest caller-interrupt-flips-cancel-token
  (let [p       (pool/make-pool {:name "cancel-tok-int" :size 1})
        started (CountDownLatch. 1)]
    (try
      (cancel-caller-of! #(safe/safe-future-call {:timeout-ms 60000 :pool p} %)
                         (cooperative-spinner started) started)
      (is (idle-within? p bound-ms))
      (finally (pool/shutdown! p)))))

(deftest token-is-nil-while-caller-waits
  (let [p (pool/make-pool {:name "cancel-nil" :size 1})]
    (try
      (is (= (r/ok [true nil false])
             (safe/safe-future-call {:timeout-ms 1000 :pool p}
                                    (fn [] [(some? safe/*cancel*) @safe/*cancel* (safe/cancelled?)]))))
      (is (nil? safe/*cancel*) "unbound outside a task")
      (finally (pool/shutdown! p)))))

(deftest check-cancel-throws-once-cancelled
  (let [[token cancel!] (safe/cancel-token)]
    (binding [safe/*cancel* token]
      (is (nil? (safe/check-cancel!)))
      (cancel!)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"cancelled" (safe/check-cancel!))))))

(deftest guarded-future-call-caller-interrupt-frees-pool
  (let [p       (pool/make-pool {:name "g-cancel" :size 1})
        started (CountDownLatch. 1)]
    (try
      (cancel-caller-of! #(guarded/guarded-future-call {:timeout-ms 60000 :pool p} %)
                         (cooperative-spinner started) started)
      (is (idle-within? p bound-ms))
      (finally (pool/shutdown! p)))))

(deftest guarded-await-caller-interrupt-frees-pool
  (let [p       (pool/make-pool {:name "ga-cancel" :size 1})
        started (CountDownLatch. 1)]
    (try
      (cancel-caller-of! #(guarded/guarded-await! p % {:timeout-ms 60000})
                         (slow-sleeper started) started)
      (is (idle-within? p bound-ms))
      (is (r/ok? (guarded/guarded-await! p (constantly 1) {:timeout-ms 1000})))
      (finally (pool/shutdown! p)))))

(deftest guarded-await-timeout-flips-token
  (let [p       (pool/make-pool {:name "ga-tok" :size 1})
        started (CountDownLatch. 1)]
    (try
      (is (= :weave/timeout
             (:error (guarded/guarded-await! p (cooperative-spinner started) {:timeout-ms 50}))))
      (is (idle-within? p bound-ms))
      (finally (pool/shutdown! p)))))
