(ns hive-weave.pool-test
  "Tests for hive-weave.pool — focus on IBindingConveyor + bound-future
   conveyance correctness and swappability."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-weave.pool :as pool]))

(def ^:dynamic *probe* :root)

(use-fixtures :each
  (fn [f]
    (let [prior (pool/get-conveyor)]
      (try (f)
           (finally (pool/set-conveyor! prior))))))

;; =============================================================================
;; bound-future
;; =============================================================================

(deftest bound-future-conveys-binding
  (testing "default conveyor (BoundFnConveyor) propagates bound dynvars
            (note: clojure.core/future auto-conveys via binding-conveyor-fn,
            so bound-future is belt-and-suspenders. The canonical leak
            boundary is pool/submit! — see submit!-with-noop-loses-frame.)"
    (binding [*probe* :test]
      (is (= :test @(pool/bound-future *probe*))))))

(deftest capturing-conveyor-records-frame
  (testing "CapturingConveyor stores the frame for inspection"
    (let [snap (atom nil)]
      (pool/set-conveyor! (pool/->CapturingConveyor snap))
      (binding [*probe* :test]
        @(pool/bound-future *probe*))
      (is (some? @snap))
      (is (contains? @snap #'*probe*))
      (is (= :test (get @snap #'*probe*))))))

(deftest fixed-frame-conveyor-installs-snapshot-frame
  (testing "FixedFrameConveyor injects a snapshotted frame into the work-fn,
            simulating the test-isolation case where bindings are set AFTER
            a long-lived go-loop / executor has already captured root."
    (let [frame  (binding [*probe* :captured]
                   (pool/capture-frame))
          result (atom nil)
          conv   (pool/->FixedFrameConveyor frame)
          f      (pool/convey conv (fn [] (reset! result *probe*)))]
      ;; Call f on a thread with NO test binding — the captured frame should win.
      (.get (.submit (java.util.concurrent.Executors/newSingleThreadExecutor)
                     ^Callable f))
      (is (= :captured @result)))))

(deftest set-conveyor-returns-prior
  (testing "set-conveyor! returns the conveyor it replaced"
    (let [first  (pool/->BoundFnConveyor)
          second (pool/->NoopConveyor)
          _      (pool/set-conveyor! first)
          ret    (pool/set-conveyor! second)]
      (is (= first ret)))))

(deftest get-conveyor-allocates-fresh-when-atom-nil
  (testing "Regression: after a hot-reload, defonce-cached conveyor instances
            referenced an OLD BoundFnConveyor class while the new protocol var
            saw a different class identity, producing
              \"No implementation of method: :convey of protocol:
               #'hive-weave.pool/IBindingConveyor found for class:
               hive_weave.pool.BoundFnConveyor\".
            Fix: defonce now holds an atom of nil; get-conveyor lazily
            constructs a fresh BoundFnConveyor every time the atom is nil,
            so the conveyor's class always matches the currently-loaded
            protocol var."
    (reset! @#'hive-weave.pool/active-conveyor nil)
    (let [c (pool/get-conveyor)]
      (is (some? c))
      (is (instance? hive_weave.pool.BoundFnConveyor c)
          "fallback yields a BoundFnConveyor instance")
      (is (satisfies? hive-weave.pool/IBindingConveyor c)
          "fallback instance satisfies the protocol")
      ;; Exercise the protocol method itself.
      (binding [*probe* :test]
        (is (= :test ((pool/convey c (fn [] *probe*)))))))))

;; =============================================================================
;; submit! routes through conveyor
;; =============================================================================

(deftest submit!-uses-active-conveyor
  (testing "submit! conveys bindings via active IBindingConveyor"
    (let [p (pool/make-pool {:name "test" :size 1})]
      (try
        (binding [*probe* :test]
          (let [fut (pool/submit! p (fn [] *probe*))]
            (is (= :test (.get fut)))))
        (finally (pool/shutdown! p))))))

(deftest submit!-with-noop-loses-frame
  (testing "swap NoopConveyor — submit! body sees root binding"
    (pool/set-conveyor! (pool/->NoopConveyor))
    (let [p (pool/make-pool {:name "test-noop" :size 1})]
      (try
        (binding [*probe* :test]
          (let [fut (pool/submit! p (fn [] *probe*))]
            (is (= :root (.get fut)))))
        (finally (pool/shutdown! p))))))

;; =============================================================================
;; Rejection policy + executor-agnostic surface
;; =============================================================================

(defn- saturate!
  "Occupy every worker and queue slot of p with tasks parked on latch."
  [p ^java.util.concurrent.CountDownLatch latch n]
  (dotimes [_ n] (pool/submit! p (fn [] (.await latch)))))

(deftest abort-rejection-surfaces-saturation
  (testing ":rejection :abort throws RejectedExecutionException instead of running on the caller"
    (let [p     (pool/make-pool {:name "test-abort" :size 1 :queue-capacity 1 :rejection :abort})
          latch (java.util.concurrent.CountDownLatch. 1)]
      (try
        (saturate! p latch 2)
        (is (thrown? java.util.concurrent.RejectedExecutionException
                     (pool/submit! p (fn [] :never))))
        (finally (.countDown latch) (pool/shutdown! p))))))

(deftest caller-runs-is-the-default
  (testing "a saturated default pool runs overflow work on the submitting thread"
    (let [p     (pool/make-pool {:name "test-cr" :size 1 :queue-capacity 1})
          latch (java.util.concurrent.CountDownLatch. 1)
          me    (Thread/currentThread)]
      (try
        (saturate! p latch 2)
        (is (identical? me (.get ^java.util.concurrent.Future
                                 (pool/submit! p (fn [] (Thread/currentThread))))))
        (finally (.countDown latch) (pool/shutdown! p))))))

(deftest unknown-rejection-policy-is-refused
  (is (thrown? AssertionError (pool/make-pool {:name "bad" :size 1 :rejection :drop}))))

(deftest submit!-on-shutdown-pool-runs-on-caller
  (let [p (pool/make-pool {:name "test-down" :size 1 :rejection :abort})]
    (pool/shutdown! p)
    (is (= :ran (.get ^java.util.concurrent.Future (pool/submit! p (fn [] :ran)))))))

(deftest pool-stats-accepts-any-executor-service
  (testing "a non-ThreadPoolExecutor executor yields stats instead of ClassCastException"
    (let [fj (java.util.concurrent.ForkJoinPool. 2)]
      (try
        (let [s (pool/pool-stats fj)]
          (is (false? (:shutdown? s)))
          (is (= "java.util.concurrent.ForkJoinPool" (:executor s))))
        (finally (.shutdown fj))))
    (let [p (pool/make-pool {:name "test-stats" :size 2 :rejection :abort})]
      (try
        (is (= {:max-pool-size 2 :rejection :abort :shutdown? false}
               (select-keys (pool/pool-stats p) [:max-pool-size :rejection :shutdown?])))
        (finally (pool/shutdown! p))))))

(deftest await!-accepts-any-executor-service
  (let [fj (java.util.concurrent.ForkJoinPool. 1)]
    (try
      (is (= 42 (pool/await! fj (fn [] 42) {:timeout-ms 1000})))
      (finally (.shutdown fj)))))
