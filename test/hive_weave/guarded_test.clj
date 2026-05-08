(ns hive-weave.guarded-test
  "Unit tests for hive-weave.guarded — bounded execution with cleanup
   hooks + injectable alert.

   Coverage map:
     - happy path        : sub-timeout thunk returns (r/ok v).
     - timeout           : long thunk → (r/err :weave/timeout ...);
                           :on-cancel runs; :alert! fires with
                           :weave/task-killed event.
     - exception         : thunk throws → (r/err :weave/exception ...);
                           :alert! fires with :weave/task-failed event;
                           :on-cancel does NOT run (cleanup is
                           timeout-specific by design).
     - alert! coercion   : nil → no-op, fn → invoked, non-fn → ignored
                           with warning (no throw).
     - cleanup isolation : a slow :on-cancel does not block result
                           propagation past the cleanup-timeout-ms cap.
     - pool variant      : guarded-await! happy + timeout w/ pool stats
                           in the alert payload."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-weave.guarded :as guarded]
            [hive-weave.pool :as pool]))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn- collecting-alert!
  "Returns [coll-atom alert-fn]. The alert fn appends every event map
   to the atom so tests can assert on shape + count."
  []
  (let [coll (atom [])]
    [coll (fn [ev] (swap! coll conj ev))]))

(defn- ms-since [^long started-ms]
  (- (System/currentTimeMillis) started-ms))

;; =============================================================================
;; guarded-future-call — happy path
;; =============================================================================

(deftest guarded-future-call-happy-path
  (testing "fast thunk returns (r/ok v); no alert fires"
    (let [[events alert!] (collecting-alert!)
          result (guarded/guarded-future-call
                  {:timeout-ms 5000
                   :name       "happy"
                   :alert!     alert!}
                  (fn [] (+ 2 3)))]
      (is (r/ok? result))
      (is (= 5 (:ok result)))
      (is (empty? @events) "no alert on success"))))

;; =============================================================================
;; guarded-future-call — timeout
;; =============================================================================

(deftest guarded-future-call-timeout-fires-cleanup-and-alert
  (testing "slow thunk → :weave/timeout err, :on-cancel runs, :alert! fires"
    (let [cleanup-ran? (atom false)
          [events alert!] (collecting-alert!)
          started (System/currentTimeMillis)
          result (guarded/guarded-future-call
                  {:timeout-ms         100
                   :name               "slow"
                   :on-cancel          (fn [] (reset! cleanup-ran? true))
                   :cleanup-timeout-ms 1000
                   :alert!             alert!}
                  (fn [] (Thread/sleep 5000) :should-not-return))]
      (is (r/err? result))
      (is (= :weave/timeout (:error result)))
      (is (= "slow" (:name result)))
      (is (= 100 (:timeout-ms result)))
      ;; Cleanup runs on the cleanup pool (separate thread); allow a
      ;; brief settling window before asserting it ran.
      (Thread/sleep 200)
      (is @cleanup-ran? ":on-cancel ran on cleanup pool")
      (is (= 1 (count @events)) "exactly one alert fired")
      (let [ev (first @events)]
        (is (= :weave/task-killed (:event ev)))
        (is (= "slow" (:name ev)))
        (is (= :timeout (:reason ev)))
        (is (= :ok (:cleanup-result ev))))
      ;; Sanity: result returned roughly at timeout (≤ 2x), not after
      ;; the 5s thunk would have completed.
      (is (< (ms-since started) 2000)
          "guarded returned promptly on timeout"))))

;; =============================================================================
;; guarded-future-call — exception path
;; =============================================================================

(deftest guarded-future-call-exception-fires-alert-no-cleanup
  (testing "thunk throws → :weave/exception err; alert fires; cleanup skipped"
    (let [cleanup-ran? (atom false)
          [events alert!] (collecting-alert!)
          result (guarded/guarded-future-call
                  {:timeout-ms 5000
                   :name       "boom"
                   :on-cancel  (fn [] (reset! cleanup-ran? true))
                   :alert!     alert!}
                  (fn [] (throw (ex-info "synthetic" {:tag :test}))))]
      (is (r/err? result))
      (is (= :weave/exception (:error result)))
      (is (= "synthetic" (:message result)))
      (Thread/sleep 100)
      (is (false? @cleanup-ran?) "cleanup is timeout-specific, skipped on exception")
      (is (= 1 (count @events)))
      (let [ev (first @events)]
        (is (= :weave/task-failed (:event ev)))
        (is (= :exception (:reason ev)))
        (is (= "synthetic" (get-in ev [:exception :message])))))))

;; =============================================================================
;; guarded-future-call — :alert! coercion
;; =============================================================================

(deftest guarded-future-call-alert-coercion
  (testing "nil :alert! is a no-op; fn :alert! is invoked; non-fn is ignored"
    ;; nil — must not throw, must still return the err
    (let [r (guarded/guarded-future-call
             {:timeout-ms 100 :name "nil-alert"}
             (fn [] (Thread/sleep 5000) :nope))]
      (is (r/err? r)))
    ;; non-fn (a map) — internal warning, no throw, no invocation
    (let [r (guarded/guarded-future-call
             {:timeout-ms 100
              :name       "bad-alert"
              :alert!     {:not :a-fn}}
             (fn [] (Thread/sleep 5000) :nope))]
      (is (r/err? r)))))

;; =============================================================================
;; guarded-future-call — cleanup isolation
;; =============================================================================

(deftest guarded-future-call-cleanup-cap-does-not-block-caller
  (testing "slow :on-cancel is bounded by :cleanup-timeout-ms; result returns first"
    (let [[events alert!] (collecting-alert!)
          started (System/currentTimeMillis)
          result (guarded/guarded-future-call
                  {:timeout-ms         50
                   :name               "stuck-cleanup"
                   :on-cancel          (fn [] (Thread/sleep 10000))
                   :cleanup-timeout-ms 200
                   :alert!             alert!}
                  (fn [] (Thread/sleep 2000) :nope))]
      (is (r/err? result))
      ;; Result must return within ~ timeout + cleanup-cap, NOT block
      ;; on the 10s sleep inside :on-cancel. We allow generous slack
      ;; (1500ms) for CI scheduling jitter.
      (is (< (ms-since started) 1500)
          "caller is not blocked by stuck :on-cancel")
      (Thread/sleep 300)
      (let [ev (first @events)]
        (is (= :timeout (:cleanup-result ev))
            "cleanup-result records the cleanup-pool timeout")))))

;; =============================================================================
;; guarded-await! — pool variant
;; =============================================================================

(deftest guarded-await-happy-path
  (testing "pool variant returns (r/ok v) on success"
    (let [p (pool/make-pool {:name "test-happy" :size 2})
          [events alert!] (collecting-alert!)]
      (try
        (let [result (guarded/guarded-await!
                      p
                      (fn [] (* 7 6))
                      {:timeout-ms 5000
                       :name       "pool-happy"
                       :alert!     alert!})]
          (is (r/ok? result))
          (is (= 42 (:ok result)))
          (is (empty? @events)))
        (finally (pool/shutdown! p))))))

(deftest guarded-await-timeout-includes-pool-stats
  (testing "pool variant timeout payload carries :pool-stats"
    (let [p (pool/make-pool {:name "test-slow" :size 1})
          [events alert!] (collecting-alert!)]
      (try
        (let [result (guarded/guarded-await!
                      p
                      (fn [] (Thread/sleep 5000) :nope)
                      {:timeout-ms 100
                       :name       "pool-slow"
                       :alert!     alert!})]
          (is (r/err? result))
          (is (= :weave/timeout (:error result)))
          (is (map? (:pool-stats result))
              "pool-stats embedded in err data")
          (is (= 1 (count @events)))
          (let [ev (first @events)]
            (is (= :weave/task-killed (:event ev)))
            (is (map? (:pool-stats ev))
                "pool-stats embedded in alert payload too")))
        (finally (pool/shutdown! p))))))
