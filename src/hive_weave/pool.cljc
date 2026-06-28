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
     ;; => result or ::db-timeout

   ClojureScript: there is no real executor on single-threaded node, so
   the pool degenerates to an honest synchronous impl. `make-pool` returns
   a map `{:cljs-pool true :sem ... :name ... :size ...}`; `submit!` runs
   the (conveyed) thunk and returns a `delay` (so the result stays 1-arg
   derefable like a Future); `await!` runs the thunk synchronously under a
   best-effort permit on the platform semaphore and returns the value (or
   `:fallback` on a thrown error). Timeouts are advisory — synchronous node
   work cannot be interrupted. Thread-binding capture (FixedFrameConveyor /
   CapturingConveyor) has no analog on node, so conveyance is identity there."
  (:require [taoensso.timbre :as log]
            [hive-weave.platform :as platform])
  #?(:clj (:import [java.util.concurrent
                    ThreadPoolExecutor
                    LinkedBlockingQueue
                    ThreadFactory
                    TimeoutException
                    TimeUnit
                    ThreadPoolExecutor$CallerRunsPolicy
                    Future
                    RejectedExecutionException]))
  #?(:cljs (:require-macros [hive-weave.pool])))

;; =============================================================================
;; Thread Factory
;; =============================================================================

#?(:clj
   (defn- named-thread-factory
     "ThreadFactory that names threads `<prefix>-<n>` and sets them daemon
      so they don't block JVM shutdown."
     ^ThreadFactory [^String prefix]
     (let [counter (atom 0)]
       (reify ThreadFactory
         (newThread [_ runnable]
           (doto (Thread. runnable (str prefix "-" (swap! counter inc)))
             (.setDaemon true)))))))

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
   silent task drops.

   ClojureScript: returns a synchronous pool map
   `{:cljs-pool true :sem <platform-sem> :name name :size size}`."
  [{:keys [name size queue-capacity keep-alive-s]
    :or   {queue-capacity default-queue-capacity
           keep-alive-s   60}}]
  {:pre [(string? name) (pos-int? size)]}
  #?(:clj
     (ThreadPoolExecutor.
      (int size)                                          ; core pool size
      (int size)                                          ; max pool size (fixed)
      (long keep-alive-s)
      TimeUnit/SECONDS
      (LinkedBlockingQueue. (int queue-capacity))
      (named-thread-factory name)
      (ThreadPoolExecutor$CallerRunsPolicy.))
     :cljs
     {:cljs-pool true
      :sem       (platform/make-semaphore size)
      :name      name
      :size      size}))

;; =============================================================================
;; Binding Conveyor (DIP) — make dynvar conveyance swappable across thread boundaries
;; =============================================================================
;;
;; All async boundaries (futures, go-blocks, pool tasks) lose dynamic var
;; bindings unless the work-fn is wrapped with `clojure.core/bound-fn*`.
;; Routing every async submission through a single conveyor makes that
;; behaviour swappable: tests can install a `CapturingConveyor` to inspect
;; what frame leaked, or a `NoopConveyor` to assert the leak repros.
;;
;; ClojureScript has no thread-binding frames (single-threaded node), so the
;; conveyors degenerate to identity there.

(defprotocol IBindingConveyor
  (convey [this f] "Wrap thunk f so dynamic-var bindings transfer to the executing thread."))

(defrecord BoundFnConveyor []
  IBindingConveyor
  (convey [_ f] #?(:clj (clojure.core/bound-fn* f) :cljs f)))

(defrecord NoopConveyor []
  IBindingConveyor
  (convey [_ f] f))

(defrecord CapturingConveyor [captured]
  IBindingConveyor
  (convey [_ f]
    #?(:clj
       (do
         (reset! captured (get-thread-bindings))
         (clojure.core/bound-fn* f))
       :cljs
       ;; no thread-binding frame to capture on single-threaded node
       f)))

#?(:clj
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
               (clojure.lang.Var/resetThreadBindingFrame prior))))))))

#?(:clj
   (defn capture-frame
     "Snapshot the current thread's binding frame. Pair with FixedFrameConveyor
      to inject this frame into work-fns running on other threads."
     []
     (clojure.lang.Var/cloneThreadBindingFrame)))

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
   dynamic-var bindings through the active IBindingConveyor.

   ClojureScript: there is no real future on single-threaded node, so the
   body runs synchronously and the macro yields a `delay` (1-arg derefable,
   like a Future handle)."
  [& body]
  (if (:ns &env)
    `(let [f# (convey-fn (fn [] ~@body))]
       (delay (f#)))
    `(let [f# (convey-fn (fn [] ~@body))]
       (future-call f#))))

;; =============================================================================
;; Submit / Await
;; =============================================================================

#?(:clj
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
       (cancel [_ _] false))))

(defn submit!
  "Submit `f` to `pool`, returning a java.util.concurrent.Future.

   `f` is wrapped via the active `IBindingConveyor` (default
   `BoundFnConveyor`, equivalent to `clojure.core/bound-fn*`) so the
   caller's dynamic var frame is conveyed to the pool thread. This
   matches the behaviour of `clojure.core/future` and avoids a silent
   trap where code relying on `binding` loses its frame at the pool
   boundary.

   On RejectedExecutionException (pool shut down), runs `f` on the
   caller thread and returns a synthetic already-completed Future.

   ClojureScript: runs the conveyed thunk and returns a `delay` so the
   result stays 1-arg derefable like a Future handle."
  [pool f]
  (let [bf (convey-fn f)]
    #?(:clj
       (try
         (.submit ^ThreadPoolExecutor pool ^Callable bf)
         (catch RejectedExecutionException _
           (rejected-fallback-future (bf))))
       :cljs
       (delay (bf)))))

(defn await!
  "Submit `f` to `pool` and block on its result up to `:timeout-ms`.

   On timeout, cancels the task (with interrupt) and returns `:fallback`.
   On exception during execution, logs and returns `:fallback`.

   Never hangs indefinitely.

   Options:
     :timeout-ms — max wait in ms (required)
     :fallback   — value returned on timeout or exception (default nil)
     :name       — diagnostic label used in logs (default \"pool-task\")

   ClojureScript: runs `f` synchronously under a best-effort permit on the
   pool's platform semaphore and returns its value (or `:fallback` on a
   thrown error). The timeout is advisory — synchronous node work cannot be
   interrupted."
  [pool f
   {:keys [timeout-ms fallback name]
    :or   {name "pool-task"}}]
  {:pre [(pos-int? timeout-ms)]}
  #?(:clj
     (let [fut (submit! pool f)]
       (try
         (.get ^Future fut (long timeout-ms) TimeUnit/MILLISECONDS)
         (catch TimeoutException _
           (.cancel ^Future fut true)
           (log/warn "pool" name "task timed out after" timeout-ms "ms")
           fallback)
         (catch Exception e
           (log/warn e "pool" name "task failed:" (.getMessage e))
           fallback)))
     :cljs
     (let [bf        (convey-fn f)
           sem       (:sem pool)
           acquired? (boolean (and sem (platform/sem-try-acquire! sem 1)))]
       (try
         (bf)
         (catch :default e
           (log/warn e "pool" name "task failed:" (ex-message e))
           fallback)
         (finally
           (when acquired? (platform/sem-release! sem 1)))))))

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
  [pool]
  #?(:clj
     {:active          (.getActiveCount ^ThreadPoolExecutor pool)
      :queued          (.size (.getQueue ^ThreadPoolExecutor pool))
      :pool-size       (.getPoolSize ^ThreadPoolExecutor pool)
      :max-pool-size   (.getMaximumPoolSize ^ThreadPoolExecutor pool)
      :completed-tasks (.getCompletedTaskCount ^ThreadPoolExecutor pool)}
     :cljs
     {:active          0
      :queued          (if-let [sem (:sem pool)] (platform/sem-queue-length sem) 0)
      :pool-size       (:size pool)
      :max-pool-size   (:size pool)
      :completed-tasks 0}))

(defn shutdown!
  "Orderly shutdown: stop accepting new tasks, wait up to
   `:await-ms` for in-flight tasks, then force-shutdown.
   Default `:await-ms` is 5000.

   ClojureScript: no-op (the synchronous pool owns no threads)."
  [pool & [{:keys [await-ms] :or {await-ms 5000}}]]
  #?(:clj
     (do
       (.shutdown ^ThreadPoolExecutor pool)
       (when-not (.awaitTermination ^ThreadPoolExecutor pool (long await-ms) TimeUnit/MILLISECONDS)
         (.shutdownNow ^ThreadPoolExecutor pool)))
     :cljs
     nil))
