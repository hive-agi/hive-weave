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
  (:require [taoensso.timbre :as log]
            [hive-weave.stack :as stack])
  (:import [java.util.concurrent
            ThreadPoolExecutor
            LinkedBlockingQueue
            ThreadFactory
            TimeoutException
            TimeUnit
            ThreadPoolExecutor$CallerRunsPolicy
            Future
            RejectedExecutionException]
[java.util.concurrent ThreadPoolExecutor$AbortPolicy]
[java.util.concurrent ExecutorService]
[java.util.concurrent RejectedExecutionHandler]))

;; =============================================================================
;; Thread Factory
;; =============================================================================

(defn- named-thread-factory
  "ThreadFactory that names threads `<prefix>-<n>` and sets them daemon
   so they don't block JVM shutdown.

   `stack-bytes`, when given, sizes every thread's stack explicitly. Native work
   that recurses (an ONNX session build, a deep JNI call) overflows the default
   stack INSIDE native code, which is a SIGSEGV that kills the process rather
   than a StackOverflowError anyone can catch. See hive-weave.stack."
  (^ThreadFactory [^String prefix] (named-thread-factory prefix nil))
  (^ThreadFactory [^String prefix stack-bytes]
   (if stack-bytes
     (stack/thread-factory {:name prefix :stack-bytes stack-bytes})
     (let [counter (atom 0)]
       (reify ThreadFactory
         (newThread [_ runnable]
           (doto (Thread. runnable (str prefix "-" (swap! counter inc)))
             (.setDaemon true))))))))

;; =============================================================================
;; Pool Factory
;; =============================================================================

(def ^:private default-queue-capacity
  "Bounded queue capacity for a pool. Tasks beyond this trigger CallerRunsPolicy."
  256)

(def ^:private rejection-handlers
  {:caller-runs #(ThreadPoolExecutor$CallerRunsPolicy.)
   :abort       #(ThreadPoolExecutor$AbortPolicy.)})

(defn- rejection-of
  "The :rejection keyword a pool was built with, or :custom."
  [^ThreadPoolExecutor pool]
  (condp instance? (.getRejectedExecutionHandler pool)
    ThreadPoolExecutor$CallerRunsPolicy :caller-runs
    ThreadPoolExecutor$AbortPolicy      :abort
    :custom))

(def ^:private rejection-handlers
  {:caller-runs #(ThreadPoolExecutor$CallerRunsPolicy.)
   :abort       #(ThreadPoolExecutor$AbortPolicy.)})

(defn- rejection-of
  "The :rejection policy a pool was built with, or :custom."
  [^ThreadPoolExecutor pool]
  (condp instance? (.getRejectedExecutionHandler pool)
    ThreadPoolExecutor$CallerRunsPolicy :caller-runs
    ThreadPoolExecutor$AbortPolicy      :abort
    :custom))

(defn make-pool
  "Create a bounded fixed-size ThreadPoolExecutor.

   Options:
     :name           thread-name prefix and diagnostic label (required)
     :size           fixed pool size (required)
     :queue-capacity bounded LinkedBlockingQueue capacity (default 256)
     :keep-alive-s   idle keep-alive in seconds (default 60)
     :stack-bytes    explicit worker stack size (default: the JVM's)
     :rejection      what happens once workers AND queue are full:
                       :caller-runs (default) — the submitting thread runs it
                       :abort — submit! throws RejectedExecutionException

   :caller-runs never drops work but blocks the submitter, which is wrong when
   the submitter is a request thread that should shed load instead. Choose
   :abort there and answer the rejection.

   Pass :stack-bytes when the tasks call into native code that recurses. A
   native stack overflow is a SIGSEGV, not an exception, so a pool sized for
   Clojure work will take the whole process down with it (hive-weave.stack).
   Note that CallerRunsPolicy runs a rejected task on the CALLER's stack, which
   this option cannot size: keep the queue big enough that native work is not
   pushed back onto the submitter."
  ^ThreadPoolExecutor
  [{:keys [name size queue-capacity keep-alive-s stack-bytes rejection]
    :or   {queue-capacity default-queue-capacity
           keep-alive-s   60
           rejection      :caller-runs}}]
  {:pre [(string? name) (pos-int? size) (contains? rejection-handlers rejection)]}
  (ThreadPoolExecutor.
   (int size)                                           ; core pool size
   (int size)                                           ; max pool size (fixed)
   (long keep-alive-s)
   TimeUnit/SECONDS
   (LinkedBlockingQueue. (int queue-capacity))
   (named-thread-factory name stack-bytes)
   ^RejectedExecutionHandler ((rejection-handlers rejection))))

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

   A rejection from a SHUT DOWN pool runs `f` on the caller thread and returns
   an already-completed Future: the work was accepted before the shutdown race
   and still has to happen. A rejection from a SATURATED `:abort` pool is
   rethrown — that one is load shedding, and swallowing it would turn the
   policy the caller asked for back into :caller-runs."
  ^Future [^ExecutorService pool ^Callable f]
  (let [bf (convey-fn f)]
    (try
      (.submit pool ^Callable bf)
      (catch RejectedExecutionException e
        (if (.isShutdown pool)
          (rejected-fallback-future (bf))
          (throw e))))))

(defn await!
  "Submit `f` to `pool` and block on its result up to `:timeout-ms`.

   On timeout, cancels the task (with interrupt) and returns `:fallback`.
   On exception during execution, logs and returns `:fallback`.
   A saturated `:abort` pool also yields `:fallback`, logged as a rejection.

   Never hangs indefinitely.

   This BLOCKS the calling thread. Inside an async server (an aleph handler,
   a netty event-loop thread) that is the thing you are trying to avoid:
   there, submit! and compose on the Future instead.

   Options:
     :timeout-ms — max wait in ms (required)
     :fallback   — value returned on timeout, exception or rejection (default nil)
     :name       — diagnostic label used in logs (default \"pool-task\")"
  [^ExecutorService pool ^Callable f
   {:keys [timeout-ms fallback name]
    :or   {name "pool-task"}}]
  {:pre [(pos-int? timeout-ms)]}
  (let [fut (try
              (submit! pool f)
              (catch RejectedExecutionException _
                (log/warn "pool" name "rejected: saturated")
                ::rejected))]
    (if (= ::rejected fut)
      fallback
      (try
        (.get ^Future fut (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException _
          (.cancel ^Future fut true)
          (log/warn "pool" name "task timed out after" timeout-ms "ms")
          fallback)
        (catch Exception e
          (log/warn e "pool" name "task failed:" (.getMessage e))
          fallback)))))

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
  "Snapshot of an executor's runtime counters.

   Every ExecutorService answers: :executor (class name), :shutdown?,
   :terminated?. A ThreadPoolExecutor additionally answers its counters and
   the :rejection policy it was built with.

   Written against the INTERFACE because the executors weave has to live
   beside are not all ThreadPoolExecutors: manifold's and aleph's are
   dirigiste `Executor`s, which extend AbstractExecutorService, and a
   ThreadPoolExecutor-shaped read of one throws ClassCastException."
  [^ExecutorService pool]
  (merge {:executor    (.getName (class pool))
          :shutdown?   (.isShutdown pool)
          :terminated? (.isTerminated pool)}
         (when (instance? ThreadPoolExecutor pool)
           (let [^ThreadPoolExecutor tpe pool]
             {:active          (.getActiveCount tpe)
              :queued          (.size (.getQueue tpe))
              :pool-size       (.getPoolSize tpe)
              :max-pool-size   (.getMaximumPoolSize tpe)
              :completed-tasks (.getCompletedTaskCount tpe)
              :rejection       (rejection-of tpe)}))))

(defn shutdown!
  "Orderly shutdown: stop accepting new tasks, wait up to
   `:await-ms` for in-flight tasks, then force-shutdown.
   Default `:await-ms` is 5000."
  [^ExecutorService pool & [{:keys [await-ms] :or {await-ms 5000}}]]
  (.shutdown pool)
  (when-not (.awaitTermination pool (long await-ms) TimeUnit/MILLISECONDS)
    (.shutdownNow pool)))
