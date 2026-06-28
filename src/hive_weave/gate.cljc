(ns hive-weave.gate
  "Concurrency gate — bounded-permit execution with timeout.

   A gate wraps a counting semaphore with:
   - Timeout-aware permit acquisition
   - Result integration (gate-run returns ok/err instead of throwing)
   - Diagnostics (available permits, queue length)

   Three ways to use:

   1. `with-gate` — bracket macro, throws on timeout
   2. `gate-run`  — function, returns Result
   3. `deref-gate` — gated deref for promises/futures with timeout

   Also satisfies hive-weave.budget/IBudgetGate so consumers can depend on
   the unit-agnostic protocol regardless of whether the underlying gate is
   slot-permit-based (this ns) or byte-cost-based (hive-weave.budget).

   ClojureScript: the JVM Semaphore is replaced by the platform atom-semaphore
   and acquisition is synchronous and non-blocking (single-threaded node has
   no real contention — a sync thunk runs to completion between acquire and
   release). The timed `(deref derefable timeout-ms ::timeout)` of `deref-gate`
   degrades to a plain `(deref derefable)`; the :timeout-ms budget is advisory
   on cljs.

   Usage:
     (def db-read (gate {:permits 4 :timeout-ms 15000 :name \"db-read\"}))

     (with-gate db-read (query-database ...))
     (deref-gate db-read (chroma/query coll embedding))"
  (:require [hive-dsl.result :as r]
            [hive-weave.budget :as budget]
            [hive-weave.platform :as platform]
            [taoensso.timbre :as log])
  #?(:cljs (:require-macros [hive-weave.gate]))
  #?(:clj (:import [java.util.concurrent Semaphore TimeUnit])))

;; =============================================================================
;; Gate Record
;; =============================================================================

(defrecord Gate [name permits timeout-ms semaphore])

;; =============================================================================
;; Constructor
;; =============================================================================

(defn gate
  "Create a concurrency gate.

   Options:
     :permits    — max concurrent executions (default 1, i.e. serialized)
     :timeout-ms — max wait for permit acquisition (default 30000)
     :name       — diagnostic name (default \"gate\")
     :fair?      — FIFO ordering for waiters (default true)"
  [{:keys [permits timeout-ms name fair?]
    :or   {permits 1 timeout-ms 30000 name "gate" fair? true}}]
  (->Gate name permits timeout-ms
          #?(:clj  (Semaphore. (int permits) (boolean fair?))
             :cljs (platform/make-semaphore permits))))

;; =============================================================================
;; Core Execution
;; =============================================================================

(defn gate-run
  "Execute f under the gate's permit. Returns Result.

   On success: (ok <return-value>)
   On timeout: (err :gate/timeout {...})
   On error:   (err :gate/execution-failed {...})"
  [g f]
  (let [sem (:semaphore g)
        tms (:timeout-ms g)]
    (if #?(:clj  (.tryAcquire ^Semaphore sem tms TimeUnit/MILLISECONDS)
           :cljs (platform/sem-try-acquire! sem 1))
      (try
        (r/ok (f))
        (catch #?(:clj Exception :cljs :default) e
          (r/err :gate/execution-failed
                 {:name    (:name g)
                  :message (ex-message e)
                  :class   (platform/ex-class-name e)}))
        (finally
          #?(:clj  (.release ^Semaphore sem)
             :cljs (platform/sem-release! sem 1))))
      (r/err :gate/timeout
             {:name       (:name g)
              :timeout-ms tms
              :permits    (:permits g)
              :available  #?(:clj  (.availablePermits ^Semaphore sem)
                             :cljs (platform/sem-available sem))
              :hint       "Too many concurrent operations or downstream is locked"}))))

(defn gate-run!
  "Execute f under the gate's permit. Throws on timeout."
  [g f]
  (let [sem (:semaphore g)
        tms (:timeout-ms g)]
    (if #?(:clj  (.tryAcquire ^Semaphore sem tms TimeUnit/MILLISECONDS)
           :cljs (platform/sem-try-acquire! sem 1))
      (try
        (f)
        (finally
          #?(:clj  (.release ^Semaphore sem)
             :cljs (platform/sem-release! sem 1))))
      (throw (ex-info (str "Gate '" (:name g) "' timed out after " tms "ms")
                      {:name       (:name g)
                       :timeout-ms tms
                       :permits    (:permits g)
                       :available  #?(:clj  (.availablePermits ^Semaphore sem)
                                      :cljs (platform/sem-available sem))})))))

;; =============================================================================
;; Sugar Macros
;; =============================================================================

(defmacro with-gate
  "Execute body under gate permit. Throws on timeout.

   (with-gate db-read (query-database ...))"
  [g & body]
  `(gate-run! ~g (fn [] ~@body)))

(defmacro with-gate-result
  "Execute body under gate permit. Returns Result.

   (with-gate-result db-write (insert-record! ...))"
  [g & body]
  `(gate-run ~g (fn [] ~@body)))

;; =============================================================================
;; Gated Deref
;; =============================================================================

(defn deref-gate
  "Deref a promise/future under the gate's permit with timeout.
   Replaces bare `@(chroma/query ...)` with bounded concurrency + timeout.

   ClojureScript: single-threaded — the deref is synchronous and the
   timeout-ms is advisory (a non-blocking deref cannot time out)."
  ([g derefable]
   (deref-gate g derefable (:timeout-ms g)))
  ([g derefable timeout-ms]
   (let [sem   (:semaphore g)
         gname (:name g)]
     (if #?(:clj  (.tryAcquire ^Semaphore sem timeout-ms TimeUnit/MILLISECONDS)
            :cljs (platform/sem-try-acquire! sem 1))
       (try
         (let [result #?(:clj  (deref derefable timeout-ms ::timeout)
                         :cljs (deref derefable))]
           (if (= result ::timeout)
             (throw (ex-info (str "Gate '" gname "' deref timed out after " timeout-ms "ms")
                             {:name gname :timeout-ms timeout-ms
                              :hint "Downstream may be locked or unresponsive"}))
             result))
         (finally
           #?(:clj  (.release ^Semaphore sem)
              :cljs (platform/sem-release! sem 1))))
       (throw (ex-info (str "Gate '" gname "' full — too many concurrent operations")
                       {:name gname :timeout-ms timeout-ms
                        :permits (:permits g)
                        :available #?(:clj  (.availablePermits ^Semaphore sem)
                                      :cljs (platform/sem-available sem))}))))))

;; =============================================================================
;; Diagnostics
;; =============================================================================

(defn gate-stats
  "Current gate state for diagnostics."
  [g]
  (let [sem (:semaphore g)]
    {:name         (:name g)
     :permits      (:permits g)
     :available    #?(:clj  (.availablePermits ^Semaphore sem)
                      :cljs (platform/sem-available sem))
     :queue-length #?(:clj  (.getQueueLength ^Semaphore sem)
                      :cljs (platform/sem-queue-length sem))}))

(extend-type Gate
  budget/IBudgetGate
  (-acquire [g cost timeout-ms]
    (let [sem     (:semaphore g)
          permits (:permits g)
          cost    (int (or cost 1))
          tms     (long (or timeout-ms (:timeout-ms g) 30000))]
      (cond
        (or (not (pos? cost)) (> cost permits))
        (r/err :budget/over-capacity
               {:name      (:name g)
                :capacity  permits
                :unit      :slot
                :requested cost
                :hint      "cost exceeds gate permit count; cannot ever admit"})

        :else
        (if #?(:clj  (.tryAcquire ^Semaphore sem cost tms TimeUnit/MILLISECONDS)
               :cljs (platform/sem-try-acquire! sem cost))
          (r/ok cost)
          (r/err :budget/timeout
                 {:name       (:name g)
                  :capacity   permits
                  :unit       :slot
                  :requested  cost
                  :available  #?(:clj  (.availablePermits ^Semaphore sem)
                                 :cljs (platform/sem-available sem))
                  :timeout-ms tms})))))

  (-release [g cost]
    (let [sem (:semaphore g)]
      #?(:clj  (.release ^Semaphore sem (int (or cost 1)))
         :cljs (platform/sem-release! sem (int (or cost 1))))
      nil))

  (-stats [g]
    (let [sem       (:semaphore g)
          permits   (:permits g)
          available #?(:clj  (.availablePermits ^Semaphore sem)
                       :cljs (platform/sem-available sem))]
      {:name           (:name g)
       :capacity       permits
       :unit           :slot
       :inflight       (- permits available)
       :available      available
       :queued         #?(:clj  (.getQueueLength ^Semaphore sem)
                          :cljs (platform/sem-queue-length sem))
       :queue-length   #?(:clj  (.getQueueLength ^Semaphore sem)
                          :cljs (platform/sem-queue-length sem))
       :admitted-total nil
       :rejected-total nil
       :timeout-ms     (:timeout-ms g)})))
