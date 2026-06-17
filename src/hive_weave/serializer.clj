;; -*- mode: clojure -*-
(ns hive-weave.serializer
  "FIFO serializer — single-writer queue for resources that don't
   tolerate concurrent writers (konserve filestore, datahike writer,
   external services with strict request ordering).

   ## When to reach for this

   - **A `gate`** (`hive-weave.gate`) is the right choice when the
     caller needs the result back synchronously and you can afford
     to block on the permit. Concurrent reads, bounded fan-out, etc.
   - **A `pool`** (`hive-weave.pool`) is the right choice when work
     items are independent and ordering doesn't matter.
   - **A serializer is the right choice when**:
     1. Concurrent execution corrupts the resource (konserve `.ksv.new
        -> .ksv` rename race; SQLite WAL on a single file; an HTTP API
        with strict request ordering).
     2. Callers don't need the result synchronously — they fire-and-
        forget and check completion via a side channel (a promise the
        submitter can deref later, or just log on failure).
     3. Ordering matters (FIFO).
     4. Repeated submissions for the same key can be coalesced (only
        the latest matters — e.g. an upsert that's idempotent on a
        unique key, where an in-flight submission about to be
        overwritten can be dropped).

   ## Type model

   Two closed ADTs make the message and outcome surface explicit:

     SerializerMsg — what flows through the queue
       :msg/task         { :key, :f, :promise }
       :msg/poison

     SubmitOutcome — what `submit!` returns
       :submit/ok        { :promise }
       :submit/timeout   { :queue-size :timeout-ms }
       :submit/closed

     TaskOutcome   — what the submission promise resolves to
       :task/ok          { :value }
       :task/failed      { :class :message }

   Domain ADTs make the worker loop a `adt-case` exhaustive match —
   no string-typing, no half-cases.

   ## Backpressure model

   The serializer holds a single dedicated worker thread + a bounded
   `LinkedBlockingQueue`. `submit!` enqueues; if the queue is full,
   the submitter **blocks up to `:submit-timeout-ms`** waiting for
   space (true backpressure — the caller can't outrun the worker).

   ## Coalescing

   When `:coalesce-key-fn` is provided (or a `:key` is passed to
   `submit!`), every submission carries a key. Before enqueuing,
   the queue is scanned for a pending task with the same key; if
   found, the pending task is **replaced** by the new one. Coalescing
   is opt-in.

   ## Lifecycle

   `serializer` returns a record. `close!` shuts down the worker
   gracefully (drains the queue then exits via a `:msg/poison`
   message). Repeated `close!` calls are no-ops. After close,
   `submit!` returns `(submit-outcome :submit/closed)`."
  (:require [hive-dsl.adt :as adt :refer [defadt adt-case]]
            [hive-dsl.result :as r]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent
            LinkedBlockingQueue
            TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean]))

;; =============================================================================
;; ADTs
;; =============================================================================

(defadt SerializerMsg
  "Messages flowing through a serializer's work queue.

     :msg/task    — work envelope: caller's fn + the promise to fill
     :msg/poison  — shutdown sentinel; worker drains then exits"
  [:msg/task {:key     any?
              :f       fn?
              :promise (some-fn fn? #(instance? clojure.lang.IDeref %))}]
  :msg/poison)

(defadt SubmitOutcome
  "What `submit!` returns. Wraps the submission promise on success;
   carries diagnostic data on rejection."
  [:submit/ok      {:promise (some-fn fn? #(instance? clojure.lang.IDeref %))}]
  [:submit/timeout {:queue-size int? :timeout-ms int? :name string?}]
  [:submit/closed  {:name string?}])

(defadt TaskOutcome
  "What the submission promise resolves to once the worker runs the
   task. `:task/ok` carries whatever `f` returned; `:task/failed`
   describes the throwable."
  [:task/ok     {:value any?}]
  [:task/failed {:key any? :class string? :message string?}])

;; =============================================================================
;; Serializer record
;; =============================================================================

(defrecord Serializer
  [name
   ^LinkedBlockingQueue queue
   ^Thread worker
   ^AtomicBoolean closed?
   submit-timeout-ms
   coalesce-key-fn])

;; =============================================================================
;; Worker loop — exhaustive ADT match
;; =============================================================================

(defn- run-task!
  "Execute a `:msg/task` envelope and deliver the TaskOutcome onto
   its promise. Exceptions are caught and modelled as `:task/failed`
   — the worker thread MUST NOT die on a single bad task."
  [{:keys [key f promise] :as _msg}]
  (deliver promise
           (try
             (task-outcome :task/ok {:value (f)})
             (catch Throwable t
               (log/warn "serializer task failed:"
                         {:key key
                          :class (str (class t))
                          :message (.getMessage t)})
               (task-outcome :task/failed
                             {:key     key
                              :class   (str (class t))
                              :message (.getMessage t)})))))

(defn- worker-loop
  "Pull `SerializerMsg` values off the queue and dispatch via
   `adt-case` (exhaustive — adding a variant is a compile-time error
   here)."
  [^LinkedBlockingQueue queue serializer-name]
  (try
    (loop []
      (let [msg (.take queue)]
        (adt-case SerializerMsg msg
          :msg/task   (do (run-task! msg) (recur))
          :msg/poison (log/debug "serializer" serializer-name
                                 "worker exiting (poison)"))))
    (catch InterruptedException _
      (log/debug "serializer" serializer-name "worker interrupted"))
    (catch Throwable t
      (log/error "serializer" serializer-name "worker died:" (.getMessage t)))))

;; =============================================================================
;; Public API
;; =============================================================================

(defn serializer
  "Create a FIFO serializer.

   Options:
     :name              — diagnostic name (default \"serializer\")
     :queue-capacity    — bounded queue depth (default 256). Submitters
                          block when full.
     :submit-timeout-ms — max time `submit!` will wait for queue space
                          before giving up (default 30000).
     :coalesce-key-fn   — `(fn [submit-opts])` returning a coalescing
                          key (or nil). When supplied, a queued task
                          with the same key is replaced by an incoming
                          submission. Set to `nil` to disable
                          coalescing (default)."
  [{:keys [name queue-capacity submit-timeout-ms coalesce-key-fn]
    :or   {name              "serializer"
           queue-capacity    256
           submit-timeout-ms 30000}}]
  (let [queue   (LinkedBlockingQueue. (int queue-capacity))
        closed? (AtomicBoolean. false)
        worker  (doto (Thread. ^Runnable (fn [] (worker-loop queue name))
                               (str "hive-weave-serializer-" name))
                  (.setDaemon true)
                  (.start))]
    (->Serializer name queue worker closed? submit-timeout-ms coalesce-key-fn)))

(defn- coalesce!
  "Drop the pending task with the same key from the queue. Linear
   scan over the queue snapshot — acceptable for the modest queue
   sizes a serializer is sized for. Best-effort: a task may slip
   through if the worker pulls it concurrently.

   No-op when `coalesce-key-fn` is nil (coalescing is opt-in at
   serializer-construction time — passing `:key` alone does NOT
   enable it, since `:key` is also valid as opaque diagnostic metadata)."
  [^LinkedBlockingQueue queue coalesce-key-fn task-key]
  (when (and coalesce-key-fn task-key)
    (let [snapshot (vec (.toArray queue))
          target   (some (fn [m]
                           (when (and (= :msg/task (adt/adt-variant m))
                                      (= task-key (:key m)))
                             m))
                         snapshot)]
      (when target (.remove queue target)))))

(defn submit!
  "Enqueue `f` for serialized execution. Returns a `SubmitOutcome`:

     :submit/ok      — accepted; @(:promise outcome) resolves to a
                       `TaskOutcome`
     :submit/timeout — queue full for longer than :submit-timeout-ms
     :submit/closed  — close! has been called

   Submission may **block** up to `:submit-timeout-ms` if the queue
   is full (true backpressure — caller can't outrun the worker).

   `:key` enables coalescing — a pending task with the same key is
   replaced before enqueue. Useful for repeated-state-write workloads
   where the upsert is idempotent on a unique key.

   Usage — fire-and-forget with side-channel logging:

     (let [outcome (submit! s {:key project-id} (fn [] (persist! state)))]
       (when (= :submit/timeout (:adt/variant outcome))
         (log/warn \"serializer rejected\" outcome)))"
  ([s f] (submit! s {} f))
  ([^Serializer s {:keys [key]} f]
   (cond
     (.get ^AtomicBoolean (:closed? s))
     (submit-outcome :submit/closed {:name (:name s)})

     :else
     (let [^LinkedBlockingQueue q (:queue s)
           tms                    (:submit-timeout-ms s)
           p                      (promise)
           msg                    (serializer-msg
                                    :msg/task
                                    {:key key :f f :promise p})]
       (coalesce! q (:coalesce-key-fn s) key)
       (if (.offer q msg tms TimeUnit/MILLISECONDS)
         (submit-outcome :submit/ok {:promise p})
         (submit-outcome :submit/timeout
                         {:queue-size (.size q)
                          :timeout-ms tms
                          :name       (:name s)}))))))

(defn submit-and-wait!
  "Submit and block on the result promise. Returns a `TaskOutcome`
   on success, or a `SubmitOutcome` describing the rejection.

   `:wait-timeout-ms` defaults to twice `:submit-timeout-ms`.
   On wait-timeout: `(task-outcome :task/failed {...})` with class
   :weave.serializer/wait-timeout."
  ([s f] (submit-and-wait! s {} f nil))
  ([s opts f] (submit-and-wait! s opts f nil))
  ([^Serializer s opts f wait-timeout-ms]
   (let [outcome (submit! s opts f)]
     (adt-case SubmitOutcome outcome
       :submit/ok
       (let [^clojure.lang.IDeref p (:promise outcome)
             tms (or wait-timeout-ms (* 2 (:submit-timeout-ms s)))
             res (deref p tms ::timeout)]
         (if (= res ::timeout)
           (task-outcome :task/failed
                         {:key     (:key opts)
                          :class   "weave.serializer/wait-timeout"
                          :message (str "Task accepted but did not finish within "
                                        tms "ms.")})
           res))

       :submit/timeout outcome
       :submit/closed  outcome))))

;; =============================================================================
;; Diagnostics + Lifecycle
;; =============================================================================

(defn stats
  "Current serializer state for observability."
  [^Serializer s]
  {:name          (:name s)
   :queue-size    (.size ^LinkedBlockingQueue (:queue s))
   :remaining     (.remainingCapacity ^LinkedBlockingQueue (:queue s))
   :closed?       (.get ^AtomicBoolean (:closed? s))
   :worker-alive? (.isAlive ^Thread (:worker s))})

(defn close!
  "Drain the queue, then shut down the worker. Idempotent. After
   close, `submit!` returns `(submit-outcome :submit/closed)`.

   Returns Result with the final stats."
  [^Serializer s]
  (let [^AtomicBoolean closed? (:closed? s)]
    (if (.compareAndSet closed? false true)
      (let [^LinkedBlockingQueue q (:queue s)
            ^Thread worker         (:worker s)]
        (.put q (serializer-msg :msg/poison))
        (when-not (.isAlive worker)
          (log/debug "serializer" (:name s) "worker already dead at close"))
        (r/ok (stats s)))
      (r/ok (assoc (stats s) :already-closed? true)))))

;; =============================================================================
;; Result-shape helpers (interop for callers that want :ok/:err idiom)
;; =============================================================================

(defn submit-ok?
  "True when a SubmitOutcome is `:submit/ok`."
  [outcome]
  (= :submit/ok (adt/adt-variant outcome)))

(defn task-ok?
  "True when a TaskOutcome is `:task/ok`."
  [outcome]
  (= :task/ok (adt/adt-variant outcome)))
