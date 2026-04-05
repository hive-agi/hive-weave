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
            [taoensso.timbre :as log]))

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
  "Execute f in a future with timeout. Returns Result.

   (safe-future-call {:timeout-ms 5000} #(expensive-computation))
   ;; => (ok result) or (err :weave/timeout {...}) or (err :weave/exception {...})

   Options:
     :timeout-ms — max execution time (required)
     :name       — diagnostic label (optional)"
  [{:keys [timeout-ms name] :or {name "anonymous"}} f]
  {:pre [(pos-int? timeout-ms)]}
  (let [fut (future
              (try
                (f)
                (catch Exception e
                  {::exception e})))
        result (deref fut timeout-ms ::timed-out)]
    (cond
      (= result ::timed-out)
      (do (log/warn "safe-future" name "timed out after" timeout-ms "ms")
          (future-cancel fut)
          (r/err :weave/timeout {:name name :timeout-ms timeout-ms}))

      (and (map? result) (::exception result))
      (let [ex (::exception result)]
        (r/err :weave/exception {:name name
                                 :message (.getMessage ^Exception ex)
                                 :class (str (class ex))}))

      :else
      (r/ok result))))

(defmacro safe-future
  "Execute body in a future with timeout. Returns Result.

   (safe-future {:timeout-ms 5000}
     (expensive-computation))
   ;; => (ok result) or (err :weave/timeout {...})"
  [opts & body]
  `(safe-future-call ~opts (fn [] ~@body)))
