(ns hive-weave.safe-test
  "hive-weave.safe / guarded on a caller-supplied bounded pool."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [hive-dsl.result :as r]
            [hive-weave.pool :as pool]
            [hive-weave.safe :as safe]
            [hive-weave.guarded :as guarded])
  (:import [java.util.concurrent CountDownLatch]))

(defn- thread-name [] (.getName (Thread/currentThread)))

(deftest safe-future-call-runs-on-the-given-pool
  (let [p (pool/make-pool {:name "safe-pool" :size 1})]
    (try
      (let [res (safe/safe-future-call {:timeout-ms 1000 :pool p} thread-name)]
        (is (r/ok? res))
        (is (str/starts-with? (:ok res) "safe-pool-")))
      (finally (pool/shutdown! p)))))

(deftest safe-future-call-without-pool-is-unchanged
  (is (= (r/ok 7) (safe/safe-future-call {:timeout-ms 1000} (constantly 7)))))

(deftest safe-future-call-bounds-abandoned-work
  (testing "timed-out work that ignores interrupts occupies the pool, never extra threads"
    (let [p     (pool/make-pool {:name "safe-bound" :size 1 :queue-capacity 1 :rejection :abort})
          latch (CountDownLatch. 1)
          stuck (fn [] (loop [] (when (pos? (.getCount latch)) (recur))))]
      (try
        (is (= :weave/timeout (:error (safe/safe-future-call {:timeout-ms 20 :pool p} stuck))))
        (is (= :weave/timeout (:error (safe/safe-future-call {:timeout-ms 20 :pool p} stuck))))
        (let [res (safe/safe-future-call {:timeout-ms 20 :pool p} stuck)]
          (is (= :weave/rejected (:error res)))
          (is (= {:max-pool-size 1 :rejection :abort}
                 (select-keys (:pool res) [:max-pool-size :rejection]))))
        (finally (.countDown latch) (pool/shutdown! p))))))

(deftest safe-future-call-reports-exceptions-on-pool
  (let [p (pool/make-pool {:name "safe-ex" :size 1})]
    (try
      (is (= :weave/exception
             (:error (safe/safe-future-call {:timeout-ms 1000 :pool p}
                                            #(throw (ex-info "boom" {}))))))
      (finally (pool/shutdown! p)))))

(deftest guarded-future-call-runs-on-the-given-pool
  (let [p (pool/make-pool {:name "guarded-pool" :size 1})]
    (try
      (let [res (guarded/guarded-future-call {:timeout-ms 1000 :pool p} thread-name)]
        (is (str/starts-with? (:ok res) "guarded-pool-")))
      (finally (pool/shutdown! p)))))

(deftest guarded-future-call-rejects-when-pool-is-full
  (let [p      (pool/make-pool {:name "guarded-full" :size 1 :queue-capacity 1 :rejection :abort})
        latch  (CountDownLatch. 1)
        events (atom [])]
    (try
      (dotimes [_ 2] (pool/submit! p (fn [] (.await latch))))
      (let [res (guarded/guarded-future-call {:timeout-ms 100 :pool p
                                              :alert! #(swap! events conj (:event %))}
                                             (constantly :never))]
        (is (= :weave/rejected (:error res)))
        (is (= [:weave/task-rejected] @events)))
      (finally (.countDown latch) (pool/shutdown! p)))))
