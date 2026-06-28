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

   ClojureScript note: the JVM impls run the body on a `future`/pool and
   bound it with a timed `.get`/`deref`. Node is single-threaded, so
   synchronous work cannot be interrupted — the :cljs branches run the
   thunk to completion and treat `:timeout-ms` as advisory. Because the
   body always finishes, `:on-cancel` never fires on cljs (there is no
   timeout to trigger it) and the dedicated cleanup pool is a JVM-only
   construct. Thrown errors still map to `:weave/exception` and fire
   `:alert!`.

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
            [hive-weave.platform :as platform]
            [taoensso.timbre :as log])
  #?(:clj (:import [java.util.concurrent
                    Executors
                    ExecutorService
                    Future
                    TimeUnit
                    TimeoutException]))
  #?(:cljs (:require-macros [hive-weave.guarded])))

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
;;
;; ClojureScript has no executor on single-threaded node, so there is no
;; cleanup pool — `run-cleanup!` runs the thunk inline (see below).

#?(:clj
   (defonce ^:private cleanup-pool
     (delay
       (Executors/newFixedThreadPool
        2
        (reify java.util.concurrent.ThreadFactory
          (newThread [_ runnable]
            (doto (Thread. runnable "hive-weave-cleanup")
              (.setDaemon true))))))))

(defn- run-cleanup!
  "Submit `on-cancel` to the cleanup pool, block up to
   `cleanup-timeout-ms`. Logs (does not throw) on cleanup failure or
   timeout — the caller is already in a failure path, masking it would
   defeat fail-loud. Returns :ok | :timeout | :error.

   ClojureScript: runs `on-cancel` inline (single-threaded node has no
   cleanup pool); `cleanup-timeout-ms` is advisory. Returns :ok | :error."
  [name on-cancel cleanup-timeout-ms]
  (when on-cancel
    #?(:clj
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
             (log/warn e "guarded" name "cleanup failed:" (ex-message e))
             :error)))
       :cljs
       (try
         (on-cancel)
         :ok
         (catch :default e
           (log/warn e "guarded" name "cleanup failed:" (ex-message e))
           :error)))))

(defn- ->alert!
  "Coerce :alert! option to a 1-arg fn. nil → no-op. Symbol/var →
   resolve. Map → ignored (caller error). Fn → identity."
  [alert!]
  (cond
    (nil? alert!)  (constantly nil)
    (fn?  alert!)  alert!
    :else          (do (log/warn "guarded :alert! must be fn — ignoring"
                                 (platform/ex-class-name alert!))
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
                              :exception (only on :weave/task-failed)

   ClojureScript: runs `f` synchronously; `:timeout-ms` is advisory
   (synchronous node work cannot be interrupted) so the body always runs
   to completion and `:on-cancel` never fires. Thrown errors map to
   `:weave/exception` and fire `:alert!`."
  [{:keys [timeout-ms name on-cancel cleanup-timeout-ms alert!]
    :or   {name "guarded" cleanup-timeout-ms 5000}}
   f]
  {:pre [(pos-int? timeout-ms) (fn? f)]}
  (let [emit! (->alert! alert!)]
    #?(:clj
       (let [started   (platform/now-ms)
             bf        (pool/convey-fn f)
             fut       (future
                         (try
                           (bf)
                           (catch Throwable t {::exception t})))
             result    (deref fut timeout-ms ::timed-out)
             elapsed   (- (platform/now-ms) started)]
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
           (let [ex (::exception result)]
             (log/warn ex "guarded" name "threw:" (ex-message ex))
             (emit! {:event      :weave/task-failed
                     :name       name
                     :elapsed-ms elapsed
                     :reason     :exception
                     :exception  {:message (ex-message ex)
                                  :class   (platform/ex-class-name ex)}})
             (r/err :weave/exception {:name       name
                                      :elapsed-ms elapsed
                                      :message    (ex-message ex)
                                      :class      (platform/ex-class-name ex)}))

           :else
           (r/ok result)))
       :cljs
       (let [started (platform/now-ms)]
         (try
           (r/ok (f))
           (catch :default t
             (let [elapsed (- (platform/now-ms) started)]
               (log/warn t "guarded" name "threw:" (ex-message t))
               (emit! {:event      :weave/task-failed
                       :name       name
                       :elapsed-ms elapsed
                       :reason     :exception
                       :exception  {:message (ex-message t)
                                    :class   (platform/ex-class-name t)}})
               (r/err :weave/exception {:name       name
                                        :elapsed-ms elapsed
                                        :message    (ex-message t)
                                        :class      (platform/ex-class-name t)}))))))))

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
     :pool — required (the ThreadPoolExecutor).

   ClojureScript: `pool` is the synchronous pool map from `make-pool`;
   `submit!` runs the conveyed thunk and returns a `delay` that is
   deref'd synchronously. `:timeout-ms` is advisory and `:on-cancel`
   never fires. Thrown errors map to `:weave/exception` and fire
   `:alert!`."
  [exec f
   {:keys [timeout-ms name on-cancel cleanup-timeout-ms alert!]
    :or   {name "guarded" cleanup-timeout-ms 5000}}]
  {:pre [(pos-int? timeout-ms) (fn? f)]}
  (let [emit!   (->alert! alert!)
        started (platform/now-ms)]
    #?(:clj
       (let [fut (pool/submit! exec f)]
         (try
           (let [v (.get ^Future fut (long timeout-ms) TimeUnit/MILLISECONDS)]
             (r/ok v))
           (catch TimeoutException _
             (.cancel ^Future fut true)
             (let [elapsed        (- (platform/now-ms) started)
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
             (let [elapsed (- (platform/now-ms) started)
                   ex      (or (.getCause e) e)]
               (log/warn ex "guarded-pool" name "failed:" (ex-message ex))
               (emit! {:event      :weave/task-failed
                       :name       name
                       :elapsed-ms elapsed
                       :reason     :exception
                       :exception  {:message (ex-message ex)
                                    :class   (platform/ex-class-name ex)}})
               (r/err :weave/exception {:name       name
                                        :elapsed-ms elapsed
                                        :message    (ex-message ex)
                                        :class      (platform/ex-class-name ex)})))))
       :cljs
       (try
         (r/ok (deref (pool/submit! exec f)))
         (catch :default e
           (let [elapsed (- (platform/now-ms) started)]
             (log/warn e "guarded-pool" name "failed:" (ex-message e))
             (emit! {:event      :weave/task-failed
                     :name       name
                     :elapsed-ms elapsed
                     :reason     :exception
                     :exception  {:message (ex-message e)
                                  :class   (platform/ex-class-name e)}})
             (r/err :weave/exception {:name       name
                                      :elapsed-ms elapsed
                                      :message    (ex-message e)
                                      :class      (platform/ex-class-name e)})))))))

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
