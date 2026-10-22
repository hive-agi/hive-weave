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
  (:import))

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

(def ^:dynamic *cancel*
  "Cancel token of the running safe-future-call task, or nil outside one.

   An IDeref: @*cancel* is nil while the caller still waits and true once the
   caller timed out, was interrupted or otherwise abandoned the call.
   Cooperative work polls it (see `cancelled?` and `check-cancel!`) or hands
   it to a library that accepts an IDeref cancel flag."
  nil)

(defn cancel-token
  "A fresh, un-cancelled token: [token cancel!]. `token` is an IDeref that
   answers nil until `(cancel!)` runs, true after."
  []
  (let [flag (atom false)]
    [(reify clojure.lang.IDeref
       (deref [_] (when @flag true)))
     (fn [] (reset! flag true))]))

(defn cancelled?
  "True when the current safe-future-call task was abandoned by its caller,
   or the current thread is interrupted."
  []
  (boolean (or (some-> *cancel* deref)
               (.isInterrupted (Thread/currentThread)))))

(defn check-cancel!
  "Throw (ex-info ... {:weave/cancelled true}) when `cancelled?`. A cheap poll
   for loops inside a safe-future-call task."
  []
  (when (cancelled?)
    (throw (ex-info "hive-weave: task cancelled" {:weave/cancelled true}))))

(defn deref-cancelling
  "Deref `fut` up to `timeout-ms`, answering its value or ::timed-out. When the
   deref does not complete (timeout, caller interrupt, any throw) run
   `(cancel!)` and future-cancel `fut` before returning or rethrowing."
  [fut timeout-ms cancel!]
  (let [done? (volatile! false)]
    (try
      (let [v (deref fut timeout-ms ::timed-out)]
        (when-not (= v ::timed-out) (vreset! done? true))
        v)
      (finally
        (when-not @done?
          (when cancel! (cancel!))
          (future-cancel fut))))))

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

   Cancellation: whenever the deref does not complete — timeout, an interrupt
   of the calling thread (e.g. the caller's own future-cancel), or any throw —
   the task's Future is cancelled with interrupt (a queued task never starts)
   and its cancel token flips. Inside f, `*cancel*` is that token; poll it with
   `cancelled?` / `check-cancel!` so work that ignores interrupts can stop too.
   A caller interrupt rethrows the InterruptedException after cancelling.

   Without :pool, f runs on `clojure.core/future`, whose pool is UNBOUNDED.
   Work that neither observes interruption nor polls the token keeps its
   thread; N timed-out calls hold N threads. Pass a bounded :pool wherever
   timeouts are expected, and the ceiling is the pool instead of the heap.
   A saturated :abort pool returns (err :weave/rejected ...) rather than
   running the work on the caller."
  [{:keys [timeout-ms name pool] :or {name "anonymous"}} f]
  {:pre [(pos-int? timeout-ms)]}
  (let [[token cancel!] (cancel-token)
        wrapped (fn []
                  (binding [*cancel* token]
                    (try
                      (f)
                      (catch Throwable t
                        {::exception t}))))
        fut     (try
                  (if pool
                    (pool/submit! pool wrapped)
                    (future (wrapped)))
                  (catch RuntimeException e
                    (if (pool/rejected? e) ::rejected (throw e))))]
    (if (= ::rejected fut)
      (do (log/warn "safe-future" name "rejected: pool saturated")
          (r/err :weave/rejected {:name name
                                  :pool (pool/pool-stats pool)}))
      (let [result (deref-cancelling fut timeout-ms cancel!)]
        (cond
          (= result ::timed-out)
          (do (log/warn "safe-future" name "timed out after" timeout-ms "ms")
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
