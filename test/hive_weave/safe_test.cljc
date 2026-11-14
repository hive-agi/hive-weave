(ns hive-weave.safe-test
  "hive-weave.safe on a caller-supplied bounded pool (JVM), and the
   cross-platform contract of the cancel token and safe-future-call on
   node (synchronous thunk, advisory timeout, :weave/exception mapping)."
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing]])
            [hive-dsl.result :as r]
            [hive-weave.safe :as safe]
            #?@(:clj [[clojure.string :as str]
                      [hive-weave.pool :as pool]
                      [hive-weave.guarded :as guarded]]))
  #?(:clj (:import [java.util.concurrent CountDownLatch])))

;; =============================================================================
;; Cross-platform: the cancel token and the Result contract
;; =============================================================================

(deftest cancel-token-is-un-cancelled-until-flipped
  (let [[token cancel!] (safe/cancel-token)]
    (testing "a fresh token answers nil, then true once cancelled"
      (is (nil? @token))
      (is (false? (boolean @token)))
      (cancel!)
      (is (true? @token)))
    (testing "cancel! is idempotent"
      (cancel!)
      (is (true? @token)))))

(deftest token-is-nil-while-caller-waits
  (testing "inside a call *cancel* is bound and un-cancelled"
    (is (= (r/ok [true nil false])
           (safe/safe-future-call {:timeout-ms 1000}
                                  (fn [] [(some? safe/*cancel*)
                                          @safe/*cancel*
                                          (safe/cancelled?)])))))
  (testing "outside a call it is unbound"
    (is (nil? safe/*cancel*))))

(deftest check-cancel-throws-once-cancelled
  (let [[token cancel!] (safe/cancel-token)]
    (binding [safe/*cancel* token]
      (is (nil? (safe/check-cancel!)))
      (cancel!)
      (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                            #"cancelled"
                            (safe/check-cancel!))))))

(deftest safe-future-call-returns-ok-for-a-value
  (is (= (r/ok 7) (safe/safe-future-call {:timeout-ms 1000} (constantly 7)))))

(deftest safe-future-call-maps-a-throw-to-exception
  (let [res (safe/safe-future-call {:timeout-ms 1000} #(throw (ex-info "boom" {})))]
    (is (= :weave/exception (:error res)))
    (is (= "boom" (:message res)))
    (is (string? (:class res)))))

(deftest safe-future-call-requires-a-positive-timeout
  (is (thrown? #?(:clj AssertionError :cljs js/Error)
               (safe/safe-future-call {:timeout-ms 0} (constantly :x)))))

(deftest safe-future-macro-runs-the-body
  (is (= (r/ok 3) (safe/safe-future {:timeout-ms 1000} 3))))

;; =============================================================================
;; JVM: bounded pool, interruption, timed deref
;; =============================================================================

#?(:clj
   (defn- thread-name [] (.getName (Thread/currentThread))))

#?(:clj
   (deftest safe-future-call-runs-on-the-given-pool
     (let [p (pool/make-pool {:name "safe-pool" :size 1})]
       (try
         (let [res (safe/safe-future-call {:timeout-ms 1000 :pool p} thread-name)]
           (is (r/ok? res))
           (is (str/starts-with? (:ok res) "safe-pool-")))
         (finally (pool/shutdown! p))))))

#?(:clj
   (deftest safe-future-call-without-pool-is-unchanged
     (is (= (r/ok 7) (safe/safe-future-call {:timeout-ms 1000} (constantly 7))))))

#?(:clj
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
           (finally (.countDown latch) (pool/shutdown! p)))))))

#?(:clj
   (deftest safe-future-call-reports-exceptions-on-pool
     (let [p (pool/make-pool {:name "safe-ex" :size 1})]
       (try
         (is (= :weave/exception
                (:error (safe/safe-future-call {:timeout-ms 1000 :pool p}
                                               #(throw (ex-info "boom" {}))))))
         (finally (pool/shutdown! p))))))

#?(:clj
   (deftest deref-safe-times-out-to-the-fallback
     (is (= ::fallback (safe/deref-safe (promise) 20 ::fallback)))
     (is (= 42 (safe/deref-safe (delay 42) 1000 ::fallback)))))

#?(:clj
   (deftest deref-safe!-throws-on-timeout
     (is (thrown-with-msg? clojure.lang.ExceptionInfo #"timed out"
                           (safe/deref-safe! (promise) 20)))
     (is (= 42 (safe/deref-safe! (delay 42) 1000)))))

#?(:clj
   (deftest deref-cancelling-flips-the-token-on-timeout
     (let [cancelled (atom 0)
           fut       (future (Thread/sleep 10000) :late)]
       (is (= ::timed-out (safe/deref-cancelling fut 20 #(swap! cancelled inc))))
       (is (= 1 @cancelled) "cancel! ran once for the abandoned deref")
       (is (true? (future-cancelled? fut))))))

#?(:clj
   (deftest guarded-future-call-runs-on-the-given-pool
     (let [p (pool/make-pool {:name "guarded-pool" :size 1})]
       (try
         (let [res (guarded/guarded-future-call {:timeout-ms 1000 :pool p} thread-name)]
           (is (str/starts-with? (:ok res) "guarded-pool-")))
         (finally (pool/shutdown! p))))))

#?(:clj
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
         (finally (.countDown latch) (pool/shutdown! p))))))

;; =============================================================================
;; ClojureScript: synchronous thunk, advisory timeout
;; =============================================================================

#?(:cljs
   (deftest deref-safe-derefs-synchronously
     (testing "node has no blocking timed deref: the ref's value is returned"
       (is (= 42 (safe/deref-safe (atom 42) 5000 ::fallback)))
       (is (= 42 (safe/deref-safe! (atom 42) 5000))))))

#?(:cljs
   (deftest timeout-is-advisory-on-node
     (testing "synchronous node work cannot be interrupted — the thunk completes"
       (is (= (r/ok :late)
              (safe/safe-future-call {:timeout-ms 1}
                                     (fn [] (dotimes [_ 100000] nil) :late)))))))

#?(:cljs
   (deftest pool-option-is-accepted-and-unused
     (testing "no executor on node yet: :pool is ignored, the thunk still runs"
       (is (= (r/ok :ran)
              (safe/safe-future-call {:timeout-ms 1000 :pool ::whatever}
                                     (constantly :ran)))))))
