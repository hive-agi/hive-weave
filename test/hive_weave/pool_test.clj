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
