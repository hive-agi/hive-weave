(ns hive-weave.stack-test
  "The failure this ns exists for cannot be asserted from inside the JVM: a
   native stack overflow is a SIGSEGV, not a throwable. So what is tested here
   is everything around it. The call really runs on a thread of its own with the
   stack that was asked for, values and throwables cross back intact, and a
   depth that overflows a small stack survives a big one."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-weave.parallel :as par]
            [hive-weave.pool :as pool]
            [hive-weave.stack :as stack]))

(defn- deep
  "Recursion that is not tail-recursive, so it consumes real stack frames."
  [n]
  (if (zero? n) 0 (inc (deep (dec n)))))

(deftest the-value-comes-back
  (is (= 42 (stack/call-with-stack (fn [] 42))))
  (is (= :from-macro (stack/with-stack :from-macro)))
  (is (= :sized (stack/with-stack-of (* 1024 1024) :sized))))

(deftest it-runs-on-a-thread-of-its-own
  (let [here  (Thread/currentThread)
        there (stack/call-with-stack (fn [] (Thread/currentThread)))]
    (is (not= here there) "a different stack means a different thread")))

(deftest a-throwable-crosses-back-to-the-caller
  (testing "so the error handling around the call keeps working unchanged"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
                          (stack/call-with-stack (fn [] (throw (ex-info "boom" {}))))))
    (is (= :caught
           (try (stack/call-with-stack (fn [] (throw (ex-info "boom" {}))))
                (catch clojure.lang.ExceptionInfo _ :caught))))))

(deftest a-bigger-stack-holds-a-deeper-call
  (let [depth 20000]
    (testing "a depth that overflows a small stack"
      (is (thrown? StackOverflowError
                   (stack/call-with-stack (* 128 1024) (fn [] (deep depth))))))
    (testing "fits in the default big one, which is the whole point"
      (is (= depth (stack/call-with-stack (fn [] (deep depth))))))))

(deftest the-factory-sizes-every-thread-it-makes
  (let [factory (stack/thread-factory {:name "probe" :stack-bytes (* 8 1024 1024)})
        t       (.newThread factory (fn []))]
    (is (.isDaemon t) "a factory thread never blocks JVM shutdown")
    (is (re-find #"^probe-\d+$" (.getName t)))))

(deftest pools-take-the-stack-option
  (testing "a pool built with :stack-bytes runs deep native-shaped work"
    (let [p (pool/make-pool {:name "deep-pool" :size 2 :stack-bytes (* 16 1024 1024)})]
      (try
        (is (= 20000 @(pool/submit! p (fn [] (deep 20000)))))
        (finally (pool/shutdown! p {:await-ms 1000})))))
  (testing "and without it the pool is exactly what it always was"
    (let [p (pool/make-pool {:name "plain-pool" :size 1})]
      (try
        (is (= 3 @(pool/submit! p (fn [] (+ 1 2)))))
        (finally (pool/shutdown! p {:await-ms 1000}))))))

(deftest bounded-pmap-takes-the-stack-option
  (testing "fan-out over deep work survives when the workers are sized for it"
    (is (= [20000 20000 20000]
           (par/bounded-pmap {:concurrency 3 :timeout-ms 30000
                              :stack-bytes (* 16 1024 1024)}
                             (fn [_] (deep 20000))
                             [1 2 3]))))
  (testing "the default path is unchanged"
    (is (= [2 3 4] (par/bounded-pmap {:concurrency 2} inc [1 2 3])))))
