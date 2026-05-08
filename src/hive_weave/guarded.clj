(ns hive-weave.guarded
  "Guarded execution — bounded futures/pool tasks with cleanup hooks.

   Extends `hive-weave.safe` and `hive-weave.pool` with two missing
   pieces for production runs:

   - `:on-cancel` — a 0-arg cleanup thunk invoked when the task is
     killed by timeout. Runs on a *separate* cleanup pool so a hung
     task body can't block its own cleanup. Capped by
     `:cleanup-timeout-ms`. Used to release resources the task held
     (datahike connections, LSP probes, scan-state atoms, file locks).

   - `:alert!` — an injectable 1-arg fn invoked with a structured event
     map on timeout *or* exception. Keeps `hive-weave` decoupled from
     `hive-events` — callers wire their own emitter (telemetry, hivemind
     shout, dashboard, ...). When omitted, no alert side-effect runs.

   Failure mode: fail loud — every guarded call returns a hive-dsl
   Result. Callers must explicitly opt out via their own try/catch if
   they want exceptions to surface raw.

   Quick reference:
     (require '[hive-weave.guarded :as wg])

     (wg/guarded-future-call
       {:timeout-ms 60000
        :name       \"carto-scan-hive-knowledge\"
        :on-cancel  (fn [] (reset-scan-state! :hive-knowledge))
        :alert!     (fn [ev] (events/emit! [:weave/task-killed ev]))}
       (fn [] (run-scan-async! [:hive-knowledge])))

     (wg/guarded-await!
       my-pool
       (fn [] (long-running-write))
       {:timeout-ms 30000
        :name       \"datahike-write\"
        :on-cancel  (fn [] (reopen-conn!))
        :alert!     telemetry-emit})"
  (:require [hive-dsl.result :as r]
            [hive-weave.pool :as pool]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent
            Executors
            ExecutorService
            Future
            TimeUnit
            TimeoutException]))

;; =============================================================================
;; Cleanup pool — single shared daemon executor for :on-cancel hooks
;; =============================================================================
;;
;; Cleanup hooks are by definition side-effecting and must terminate so
;; resources actually get released. Running them on a dedicated, bounded
;; pool gives us:
;;   - Isolation from caller threads (caller doesn't pay cleanup latency).
;;   - Daemon threads (no JVM-shutdown hang).
;;   - Per-hook timeout enforcement via Future.get with timeout.

(defonce ^:private cleanup-pool
  (delay
    (Executors/newFixedThreadPool
     2
     (reify java.util.concurrent.ThreadFactory
       (newThread [_ runnable]
         (doto (Thread. runnable "hive-weave-cleanup")
           (.setDaemon true)))))))

(defn- run-cleanup!
  "Submit `on-cancel` to the cleanup pool, block up to
   `cleanup-timeout-ms`. Logs (does not throw) on cleanup failure or
   timeout — the caller is already in a failure path, masking it would
   defeat fail-loud. Returns :ok | :timeout | :error."
  [name on-cancel cleanup-timeout-ms]
  (when on-cancel
    (let [^ExecutorService pool @cleanup-pool
          fut (.submit pool ^Callable (fn [] (on-cancel) :ok))]
      (try
        (.get ^Future fut (long cleanup-timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          (log/warn "guarded" name "cleanup timed out after"
                    cleanup-timeout-ms "ms — abandoning")
          (.cancel ^Future fut true)
          :timeout)
        (catch Exception e
          (log/warn e "guarded" name "cleanup failed:" (.getMessage e))
          :error)))))

(defn- ->alert!
  "Coerce :alert! option to a 1-arg fn. nil → no-op. Symbol/var →
   resolve. Map → ignored (caller error). Fn → identity."
  [alert!]
  (cond
    (nil? alert!)  (constantly nil)
    (fn?  alert!)  alert!
    :else          (do (log/warn "guarded :alert! must be fn — ignoring"
                                 (class alert!))
                       (constantly nil))))

;; =============================================================================
;; Guarded future-call — standalone (no pool dependency)
;; =============================================================================

(defn guarded-future-call
  "Run `f` in a future under `:timeout-ms`. On timeout: future-cancel
   (interrupt), submit `:on-cancel` to the cleanup pool, fire `:alert!`,
   return `(r/err :weave/timeout {...})`. On exception: fire `:alert!`,
   return `(r/err :weave/exception {...})`. Otherwise `(r/ok value)`.

   Options:
     :timeout-ms          — required, pos-int.
     :name                — diagnostic label (default \"guarded\").
     :on-cancel           — 0-arg cleanup thunk; only fires on timeout.
     :cleanup-timeout-ms  — wall-time cap on :on-cancel (default 5000).
     :alert!              — (event-map) -> any. Event keys:
                              :event :weave/task-killed | :weave/task-failed
                              :name :timeout-ms :elapsed-ms :reason
                              :cleanup-result (only on timeout)
                              :exception (only on :weave/task-failed)"
  [{:keys [timeout-ms name on-cancel cleanup-timeout-ms alert!]
    :or   {name "guarded" cleanup-timeout-ms 5000}}
   f]
  {:pre [(pos-int? timeout-ms) (fn? f)]}
  (let [emit!     (->alert! alert!)
        started   (System/currentTimeMillis)
        bf        (pool/convey-fn f)
        fut       (future
                    (try
                      (bf)
                      (catch Throwable t {::exception t})))
        result    (deref fut timeout-ms ::timed-out)
        elapsed   (- (System/currentTimeMillis) started)]
    (cond
      (= result ::timed-out)
      (let [_ (future-cancel fut)
            cleanup-result (run-cleanup! name on-cancel cleanup-timeout-ms)]
        (log/warn "guarded" name "timed out after" timeout-ms "ms — cleanup:" cleanup-result)
        (emit! {:event          :weave/task-killed
                :name           name
                :timeout-ms     timeout-ms
                :elapsed-ms     elapsed
                :reason         :timeout
                :cleanup-result cleanup-result})
        (r/err :weave/timeout {:name           name
                               :timeout-ms     timeout-ms
                               :elapsed-ms     elapsed
                               :cleanup-result cleanup-result}))

      (and (map? result) (::exception result))
      (let [^Throwable ex (::exception result)]
        (log/warn ex "guarded" name "threw:" (.getMessage ex))
        (emit! {:event      :weave/task-failed
                :name       name
                :elapsed-ms elapsed
                :reason     :exception
                :exception  {:message (.getMessage ex)
                             :class   (str (class ex))}})
        (r/err :weave/exception {:name       name
                                 :elapsed-ms elapsed
                                 :message    (.getMessage ex)
                                 :class      (str (class ex))}))

      :else
      (r/ok result))))

(defmacro guarded-future
  "Macro form of guarded-future-call. Body is wrapped as the thunk.

     (guarded-future {:timeout-ms 60000 :name \"scan\"
                      :on-cancel #(reset-state!)
                      :alert! emit-event}
       (run-scan! ...))"
  [opts & body]
  `(guarded-future-call ~opts (fn [] ~@body)))

;; =============================================================================
;; Guarded await! — pool-aware variant
;; =============================================================================

(defn guarded-await!
  "Pool-bound counterpart to `guarded-future-call`. Submits `f` to
   `pool` (a `java.util.concurrent.ThreadPoolExecutor` from
   `hive-weave.pool/make-pool`) and blocks up to `:timeout-ms`. Same
   timeout / cancel / alert / cleanup contract as
   `guarded-future-call`, plus pool-stats embedded in the alert event
   so callers can see saturation when timeouts cluster.

   Options: see `guarded-future-call`. Additionally:
     :pool — required (the ThreadPoolExecutor)."
  [^java.util.concurrent.ThreadPoolExecutor exec ^Callable f
   {:keys [timeout-ms name on-cancel cleanup-timeout-ms alert!]
    :or   {name "guarded" cleanup-timeout-ms 5000}}]
  {:pre [(pos-int? timeout-ms) (fn? f)]}
  (let [emit!   (->alert! alert!)
        started (System/currentTimeMillis)
        fut     (pool/submit! exec f)]
    (try
      (let [v (.get ^Future fut (long timeout-ms) TimeUnit/MILLISECONDS)]
        (r/ok v))
      (catch TimeoutException _
        (.cancel ^Future fut true)
        (let [elapsed        (- (System/currentTimeMillis) started)
              cleanup-result (run-cleanup! name on-cancel cleanup-timeout-ms)
              stats          (pool/pool-stats exec)]
          (log/warn "guarded-pool" name "timed out after" timeout-ms
                    "ms — cleanup:" cleanup-result "pool:" stats)
          (emit! {:event          :weave/task-killed
                  :name           name
                  :timeout-ms     timeout-ms
                  :elapsed-ms     elapsed
                  :reason         :timeout
                  :cleanup-result cleanup-result
                  :pool-stats     stats})
          (r/err :weave/timeout {:name           name
                                 :timeout-ms     timeout-ms
                                 :elapsed-ms     elapsed
                                 :cleanup-result cleanup-result
                                 :pool-stats     stats})))
      (catch Exception e
        (let [elapsed (- (System/currentTimeMillis) started)
              ex      (or (.getCause e) e)]
          (log/warn ex "guarded-pool" name "failed:" (.getMessage ex))
          (emit! {:event      :weave/task-failed
                  :name       name
                  :elapsed-ms elapsed
                  :reason     :exception
                  :exception  {:message (.getMessage ^Throwable ex)
                               :class   (str (class ex))}})
          (r/err :weave/exception {:name       name
                                   :elapsed-ms elapsed
                                   :message    (.getMessage ^Throwable ex)
                                   :class      (str (class ex))}))))))

(defmacro with-guarded-await
  "Macro form of guarded-await!. Submits body to pool with cleanup +
   alert hooks.

     (with-guarded-await my-pool
       {:timeout-ms 60000
        :name       \"datahike-tx\"
        :on-cancel  #(reopen-conn!)
        :alert!     emit-event}
       (transact! ...))"
  [pool opts & body]
  `(guarded-await! ~pool (fn [] ~@body) ~opts))
