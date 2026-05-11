(ns hive-weave.pool
  "Bounded thread-pool primitives — factory + safe submit/await.

   Extends hive-weave with a pool abstraction so downstream code does
   not reach into java.util.concurrent directly (DIP).

   Responsibilities:
   - Construct a bounded `ThreadPoolExecutor` with CallerRunsPolicy
     backpressure and a named thread factory (for JVM diagnostics).
   - Expose `submit!` returning an opaque Future-like handle.
   - Expose `await!` — submit + block up to a timeout, returning a
     fallback on timeout/error. Never hangs.
   - Re-export `pool-stats` and `shutdown!` for lifecycle.

   Callers keep pool *instances* in their own registry (e.g. named
   io/compute/event/memory pools) and hand them to `await!` when they
   need bounded, isolated execution for a piece of work.

   Quick start:
     (require '[hive-weave.pool :as wp])

     (def db-pool (wp/make-pool {:name \"db\" :size 8}))

     (wp/await! db-pool
                (fn [] (query-database ...))
                {:timeout-ms 5000 :fallback ::db-timeout})
     ;; => result or ::db-timeout"
  (:require [taoensso.timbre :as log])
  (:import [java.util.concurrent
            ThreadPoolExecutor
            LinkedBlockingQueue
            ThreadFactory
            TimeoutException
            TimeUnit
            ThreadPoolExecutor$CallerRunsPolicy
            Future
            RejectedExecutionException]))

;; =============================================================================
;; Thread Factory
;; =============================================================================

(defn- named-thread-factory
  "ThreadFactory that names threads `<prefix>-<n>` and sets them daemon
   so they don't block JVM shutdown."
  ^ThreadFactory [^String prefix]
  (let [counter (atom 0)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. runnable (str prefix "-" (swap! counter inc)))
          (.setDaemon true))))))

;; =============================================================================
;; Pool Factory
;; =============================================================================

(def ^:private default-queue-capacity
  "Bounded queue capacity for a pool. Tasks beyond this trigger CallerRunsPolicy."
  256)

(defn make-pool
  "Create a bounded fixed-size ThreadPoolExecutor.

   Options:
     :name           — thread-name prefix and diagnostic label (required)
     :size           — fixed pool size (required)
     :queue-capacity — bounded LinkedBlockingQueue capacity (default 256)
     :keep-alive-s   — idle keep-alive in seconds (default 60)

   CallerRunsPolicy is always used: when both workers and queue are
   saturated, the submitting thread runs the task itself. This provides
   upstream backpressure instead of unbounded thread creation or
   silent task drops."
  ^ThreadPoolExecutor
  [{:keys [name size queue-capacity keep-alive-s]
    :or   {queue-capacity default-queue-capacity
           keep-alive-s   60}}]
  {:pre [(string? name) (pos-int? size)]}
  (ThreadPoolExecutor.
   (int size)                                           ; core pool size
   (int size)                                           ; max pool size (fixed)
   (long keep-alive-s)
   TimeUnit/SECONDS
   (LinkedBlockingQueue. (int queue-capacity))
   (named-thread-factory name)
   (ThreadPoolExecutor$CallerRunsPolicy.)))

;; =============================================================================
;; Binding Conveyor (DIP) — make dynvar conveyance swappable across thread boundaries
;; =============================================================================
;;
;; All async boundaries (futures, go-blocks, pool tasks) lose dynamic var
;; bindings unless the work-fn is wrapped with `clojure.core/bound-fn*`.
;; Routing every async submission through a single conveyor makes that
;; behaviour swappable: tests can install a `CapturingConveyor` to inspect
;; what frame leaked, or a `NoopConveyor` to assert the leak repros.

(defprotocol IBindingConveyor
  (convey [this f] "Wrap thunk f so dynamic-var bindings transfer to the executing thread."))

(defrecord BoundFnConveyor []
  IBindingConveyor
  (convey [_ f] (clojure.core/bound-fn* f)))

(defrecord NoopConveyor []
  IBindingConveyor
  (convey [_ f] f))

(defrecord CapturingConveyor [captured]
  IBindingConveyor
  (convey [_ f]
    (reset! captured (get-thread-bindings))
    (clojure.core/bound-fn* f)))

(defrecord FixedFrameConveyor [^Object frame]
  IBindingConveyor
  (convey [_ f]
    ;; Install a captured thread-binding frame on the executing thread —
    ;; needed when the work-fn runs inside a long-lived go-loop / executor
    ;; whose own frame was captured at module load (root bindings) but
    ;; tests set bindings later. Snapshot frame at fixture start via
    ;; `(clojure.lang.Var/cloneThreadBindingFrame)`.
    (fn []
      (let [prior (clojure.lang.Var/cloneThreadBindingFrame)]
        (try
          (clojure.lang.Var/resetThreadBindingFrame frame)
          (f)
          (finally
            (clojure.lang.Var/resetThreadBindingFrame prior)))))))

(defn capture-frame
  "Snapshot the current thread's binding frame. Pair with FixedFrameConveyor
   to inject this frame into work-fns running on other threads."
  []
  (clojure.lang.Var/cloneThreadBindingFrame))

(defonce ^:private active-conveyor
  ;; Initialized to nil so get-conveyor allocates a fresh BoundFnConveyor
  ;; on every nil access. This avoids the defonce stale-class pitfall: when
  ;; this namespace is hot-reloaded, defprotocol/defrecord rebuild fresh
  ;; vars + classes, but a defonce-held instance still references the OLD
  ;; class — which the new protocol var does not recognise, producing
  ;; "No implementation of method: :convey of protocol:
  ;; #'hive-weave.pool/IBindingConveyor found for class:
  ;; hive_weave.pool.BoundFnConveyor" at the next async submission.
  ;; The atom remains writable so set-conveyor! can install test doubles.
  (atom nil))

(defn set-conveyor!
  "Install conveyor c as the active binding conveyor. Returns prior conveyor."
  [c]
  {:pre [(satisfies? IBindingConveyor c)]}
  (let [prior @active-conveyor]
    (reset! active-conveyor c)
    prior))

(defn get-conveyor
  "Return active conveyor. Falls back to BoundFnConveyor if atom is nil."
  []
  (or @active-conveyor (->BoundFnConveyor)))

(defn convey-fn
  "Wrap thunk f via the active conveyor — the canonical entry point for
   any async submission that must preserve dynamic-var bindings."
  [f]
  (convey (get-conveyor) f))

(defmacro bound-future
  "Drop-in replacement for `clojure.core/future` that conveys the caller's
   dynamic-var bindings through the active IBindingConveyor."
  [& body]
  `(let [f# (convey-fn (fn [] ~@body))]
     (future-call f#)))

;; =============================================================================
;; Submit / Await
;; =============================================================================

(defn- rejected-fallback-future
  "Synthetic Future that wraps an already-computed value. Used when
   the pool is shut down and we ran `f` on the caller thread as a
   fallback."
  ^Future [result]
  (reify Future
    (get [_] result)
    (get [_ _timeout _unit] result)
    (isDone [_] true)
    (isCancelled [_] false)
    (cancel [_ _] false)))

(defn submit!
  "Submit `f` to `pool`, returning a java.util.concurrent.Future.

   `f` is wrapped via the active `IBindingConveyor` (default
   `BoundFnConveyor`, equivalent to `clojure.core/bound-fn*`) so the
   caller's dynamic var frame is conveyed to the pool thread. This
   matches the behaviour of `clojure.core/future` and avoids a silent
   trap where code relying on `binding` loses its frame at the pool
   boundary.

   On RejectedExecutionException (pool shut down), runs `f` on the
   caller thread and returns a synthetic already-completed Future."
  ^Future [^ThreadPoolExecutor pool ^Callable f]
  (let [bf (convey-fn f)]
    (try
      (.submit pool ^Callable bf)
      (catch RejectedExecutionException _
        (rejected-fallback-future (bf))))))

(defn await!
  "Submit `f` to `pool` and block on its result up to `:timeout-ms`.

   On timeout, cancels the task (with interrupt) and returns `:fallback`.
   On exception during execution, logs and returns `:fallback`.

   Never hangs indefinitely.

   Options:
     :timeout-ms — max wait in ms (required)
     :fallback   — value returned on timeout or exception (default nil)
     :name       — diagnostic label used in logs (default \"pool-task\")"
  [^ThreadPoolExecutor pool ^Callable f
   {:keys [timeout-ms fallback name]
    :or   {name "pool-task"}}]
  {:pre [(pos-int? timeout-ms)]}
  (let [fut (submit! pool f)]
    (try
      (.get ^Future fut (long timeout-ms) TimeUnit/MILLISECONDS)
      (catch TimeoutException _
        (.cancel ^Future fut true)
        (log/warn "pool" name "task timed out after" timeout-ms "ms")
        fallback)
      (catch Exception e
        (log/warn e "pool" name "task failed:" (.getMessage e))
        fallback))))

(defmacro with-pool-await
  "Submit body to `pool`, block up to (:timeout-ms opts), return
   (:fallback opts) on timeout/exception.

   (with-pool-await memory-pool {:timeout-ms 30000 :fallback ::failed}
     (chroma/add-entry! ...))"
  [pool opts & body]
  `(await! ~pool (fn [] ~@body) ~opts))

;; =============================================================================
;; Diagnostics / Lifecycle
;; =============================================================================

(defn pool-stats
  "Snapshot of a pool's runtime counters."
  [^ThreadPoolExecutor pool]
  {:active         (.getActiveCount pool)
   :queued         (.size (.getQueue pool))
   :pool-size      (.getPoolSize pool)
   :max-pool-size  (.getMaximumPoolSize pool)
   :completed-tasks (.getCompletedTaskCount pool)})

(defn shutdown!
  "Orderly shutdown: stop accepting new tasks, wait up to
   `:await-ms` for in-flight tasks, then force-shutdown.
   Default `:await-ms` is 5000."
  [^ThreadPoolExecutor pool & [{:keys [await-ms] :or {await-ms 5000}}]]
  (.shutdown pool)
  (when-not (.awaitTermination pool (long await-ms) TimeUnit/MILLISECONDS)
    (.shutdownNow pool)))
