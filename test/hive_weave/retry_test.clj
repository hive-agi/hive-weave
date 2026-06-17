(ns hive-weave.retry-test
  "Tests for hive-weave.retry/with-recovery.

   Pins the retry contract: bounded per-attempt timeout, optional
   recover! between attempts, single retry, timeouts skip retry,
   default `:on-failure` throws but is overridable for opt-in
   Result-returning callers."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-weave.retry :as retry]))

;; =============================================================================
;; Success — happy path, no retry, no recover
;; =============================================================================

(deftest success-no-retry
  (testing "f returns immediately → no recover!, no retry"
    (let [calls    (atom 0)
          recovers (atom 0)
          result   (retry/with-recovery
                     {:timeout-ms 1000
                      :name       "smoke"
                      :recover!   (fn [] (swap! recovers inc))}
                     (fn []
                       (swap! calls inc)
                       :ok))]
      (is (= :ok result))
      (is (= 1 @calls)   "thunk called exactly once")
      (is (= 0 @recovers) "recover! never invoked on success path"))))

;; =============================================================================
;; Transient — fail once, recover, succeed on retry
;; =============================================================================

(deftest transient-then-success
  (testing "first attempt throws, recover! flips state, retry succeeds"
    (let [calls    (atom 0)
          recovers (atom 0)
          fail?    (atom true)
          result   (retry/with-recovery
                     {:timeout-ms 1000
                      :name       "transient"
                      :recover!   (fn []
                                    (swap! recovers inc)
                                    (reset! fail? false))}
                     (fn []
                       (swap! calls inc)
                       (if @fail?
                         (throw (ex-info "transient" {}))
                         :recovered)))]
      (is (= :recovered result))
      (is (= 2 @calls)    "thunk called twice — initial + retry")
      (is (= 1 @recovers) "recover! called exactly once between attempts"))))

;; =============================================================================
;; Timeout — retry is skipped (alive but slow)
;; =============================================================================

(deftest timeout-skips-retry
  (testing "first-attempt timeout surfaces immediately — recover! not called,
            no retry. Default on-failure throws ex-info carrying the
            :weave/timeout Result."
    (let [calls    (atom 0)
          recovers (atom 0)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"with-recovery failed: slow"
            (retry/with-recovery
              {:timeout-ms 50
               :name       "slow"
               :recover!   (fn [] (swap! recovers inc))}
              (fn []
                (swap! calls inc)
                (Thread/sleep 500)
                :never))))
      (is (= 0 @recovers) "recover! NOT called on timeout (would just re-time-out)")
      ;; calls may be 0 or 1 depending on whether the inner future
      ;; managed to enter the body before the deref-timeout fired.
      ;; The contract is that at most ONE attempt happened.
      (is (<= @calls 1) "no retry attempt was made"))))

;; =============================================================================
;; Terminal — both attempts fail, on-failure invoked with retry's Result
;; =============================================================================

(deftest terminal-failure-throws
  (testing "f throws every time → recover! called once, retry also fails,
            on-failure throws with the retry Result."
    (let [calls    (atom 0)
          recovers (atom 0)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"with-recovery failed: doomed"
            (retry/with-recovery
              {:timeout-ms 1000
               :name       "doomed"
               :recover!   (fn [] (swap! recovers inc))}
              (fn []
                (swap! calls inc)
                (throw (ex-info "boom" {}))))))
      (is (= 2 @calls)    "two attempts before giving up")
      (is (= 1 @recovers) "recover! invoked between the two attempts"))))

;; =============================================================================
;; Custom :retry-on — caller decides what's retryable
;; =============================================================================

(deftest custom-retry-on
  (testing ":retry-on (constantly false) → never retry, even on error.
            recover! never called, terminal failure on first attempt."
    (let [calls    (atom 0)
          recovers (atom 0)
          custom-on-failure (fn [_label result] result)
          result   (retry/with-recovery
                     {:timeout-ms 1000
                      :name       "no-retry"
                      :recover!   (fn [] (swap! recovers inc))
                      :retry-on   (constantly false)
                      :on-failure custom-on-failure}
                     (fn []
                       (swap! calls inc)
                       (throw (ex-info "boom" {}))))]
      (is (r/err? result))
      (is (= :weave/exception (:error result)))
      (is (= 1 @calls)    "no retry attempt")
      (is (= 0 @recovers) "recover! not called when retry-on rejects"))))

;; =============================================================================
;; Custom :on-failure — return a Result instead of throwing
;; =============================================================================

(deftest custom-on-failure-returns-fallback
  (testing "Caller can opt out of throwing by passing a Result-returning
            on-failure. Aligns with `safe-deref-silent-drop` principle:
            failures must be distinguishable from missing data — passing
            the raw Result to on-failure preserves that signal."
    (let [fallback {:ok? false :reason :degraded}
          result   (retry/with-recovery
                     {:timeout-ms 1000
                      :name       "fallback"
                      :on-failure (fn [_label _r] fallback)}
                     (fn [] (throw (ex-info "boom" {}))))]
      (is (= fallback result)))))

;; =============================================================================
;; Pre-condition — :timeout-ms required
;; =============================================================================

(deftest timeout-ms-must-be-pos-int
  (testing "missing :timeout-ms triggers AssertionError via :pre"
    (is (thrown? AssertionError
          (retry/with-recovery {} (fn [] :ok))))
    (is (thrown? AssertionError
          (retry/with-recovery {:timeout-ms 0} (fn [] :ok))))
    (is (thrown? AssertionError
          (retry/with-recovery {:timeout-ms -1} (fn [] :ok))))))
