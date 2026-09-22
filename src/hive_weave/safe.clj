(ns hive-weave.safe
  "Safe execution primitives — the antidote to bare @ and raw future.

   Every bare `@(future ...)` or `@(promise)` is a potential hang.
   This namespace provides bounded alternatives that always terminate:

   - `deref-safe`  — deref with timeout + fallback (never hangs)
   - `deref-safe!` — deref with timeout, throws on timeout (never hangs)
   - `safe-future`  — future with timeout + Result return
   - `safe-future!` — future with timeout, throws on timeout

   All primitives return within their timeout budget. No exceptions."
  (:require [hive-dsl.result :as r]
            [taoensso.timbre :as log]
            [hive-weave.pool :as pool])
  (:import [java.util.concurrent RejectedExecutionException]))

;; =============================================================================
;; Safe Deref
;; =============================================================================

(defn deref-safe
  "Deref with timeout and fallback. Never hangs.

   (deref-safe my-promise 5000 [])       ;; => value or [] after 5s
   (deref-safe my-future 10000 nil)      ;; => value or nil after 10s"
  [ref timeout-ms fallback]
  (let [result (deref ref timeout-ms ::timed-out)]
    (if (= result ::timed-out)
      (do (log/debug "deref-safe: timed out after" timeout-ms "ms")
          fallback)
      result)))

(defn deref-safe!
  "Deref with timeout. Throws on timeout. Never hangs indefinitely.

   (deref-safe! my-promise 5000)    ;; => value or throws after 5s"
  [ref timeout-ms]
  (let [result (deref ref timeout-ms ::timed-out)]
    (if (= result ::timed-out)
      (throw (ex-info (str "deref-safe!: timed out after " timeout-ms "ms")
                      {:timeout-ms timeout-ms}))
      result)))

;; =============================================================================
;; Safe Future
;; =============================================================================

(defn safe-future-call
  "Execute f with a timeout. Returns Result.

   (safe-future-call {:timeout-ms 5000} #(expensive-computation))
   (safe-future-call {:timeout-ms 5000 :pool io-pool} #(slow-read))

   Options:
     :timeout-ms — max execution time (required)
     :name       — diagnostic label (optional)
     :pool       — ExecutorService to run f on (optional)

   Without :pool, f runs on `clojure.core/future`, whose pool is UNBOUNDED. A
   timeout only interrupts, so work that does not observe interruption keeps
   its thread; N timed-out calls hold N threads. Pass a bounded :pool wherever
   timeouts are expected, and the ceiling is the pool instead of the heap.
   A saturated :abort pool returns (err :weave/rejected ...) rather than
   running the work on the caller."
  [{:keys [timeout-ms name pool] :or {name "anonymous"}} f]
  {:pre [(pos-int? timeout-ms)]}
  (let [wrapped (fn []
                  (try
                    (f)
                    (catch Throwable t
                      {::exception t})))
        fut     (try
                  (if pool
                    (pool/submit! pool wrapped)
                    (future (wrapped)))
                  (catch RejectedExecutionException _
                    ::rejected))]
    (if (= ::rejected fut)
      (do (log/warn "safe-future" name "rejected: pool saturated")
          (r/err :weave/rejected {:name name
                                  :pool (pool/pool-stats pool)}))
      (let [result (deref fut timeout-ms ::timed-out)]
        (cond
          (= result ::timed-out)
          (do (log/warn "safe-future" name "timed out after" timeout-ms "ms")
              (future-cancel fut)
              (r/err :weave/timeout {:name name :timeout-ms timeout-ms}))

          (and (map? result) (::exception result))
          (let [ex (::exception result)]
            (r/err :weave/exception {:name name
                                     :message (.getMessage ^Throwable ex)
                                     :class (str (class ex))}))

          :else
          (r/ok result))))))

(defmacro safe-future
  "Execute body in a future with timeout. Returns Result.

   (safe-future {:timeout-ms 5000}
     (expensive-computation))
   ;; => (ok result) or (err :weave/timeout {...})"
  [opts & body]
  `(safe-future-call ~opts (fn [] ~@body)))
