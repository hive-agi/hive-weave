(ns hive-weave.gate
  "Concurrency gate — bounded-permit execution with timeout.

   A gate wraps a java.util.concurrent.Semaphore with:
   - Timeout-aware permit acquisition
   - Result integration (gate-run returns ok/err instead of throwing)
   - Diagnostics (available permits, queue length)

   Three ways to use:

   1. `with-gate` — bracket macro, throws on timeout
   2. `gate-run`  — function, returns Result
   3. `deref-gate` — gated deref for promises/futures with timeout

   Usage:
     (def db-read (gate {:permits 4 :timeout-ms 15000 :name \"db-read\"}))

     (with-gate db-read (query-database ...))
     (deref-gate db-read (chroma/query coll embedding))"
  (:require [hive-dsl.result :as r]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent Semaphore TimeUnit]))

;; =============================================================================
;; Gate Record
;; =============================================================================

(defrecord Gate [name permits timeout-ms ^Semaphore semaphore])

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
          (Semaphore. (int permits) (boolean fair?))))

;; =============================================================================
;; Core Execution
;; =============================================================================

(defn gate-run
  "Execute f under the gate's permit. Returns Result.

   On success: (ok <return-value>)
   On timeout: (err :gate/timeout {...})
   On error:   (err :gate/execution-failed {...})"
  [^Gate g f]
  (let [^Semaphore sem (:semaphore g)
        tms             (:timeout-ms g)]
    (if (.tryAcquire sem tms TimeUnit/MILLISECONDS)
      (try
        (r/ok (f))
        (catch Exception e
          (r/err :gate/execution-failed
                 {:name    (:name g)
                  :message (.getMessage e)
                  :class   (str (class e))}))
        (finally
          (.release sem)))
      (r/err :gate/timeout
             {:name       (:name g)
              :timeout-ms tms
              :permits    (:permits g)
              :available  (.availablePermits sem)
              :hint       "Too many concurrent operations or downstream is locked"}))))

(defn gate-run!
  "Execute f under the gate's permit. Throws on timeout."
  [^Gate g f]
  (let [^Semaphore sem (:semaphore g)
        tms             (:timeout-ms g)]
    (if (.tryAcquire sem tms TimeUnit/MILLISECONDS)
      (try
        (f)
        (finally
          (.release sem)))
      (throw (ex-info (str "Gate '" (:name g) "' timed out after " tms "ms")
                      {:name       (:name g)
                       :timeout-ms tms
                       :permits    (:permits g)
                       :available  (.availablePermits sem)})))))

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
   Replaces bare `@(chroma/query ...)` with bounded concurrency + timeout."
  ([^Gate g derefable]
   (deref-gate g derefable (:timeout-ms g)))
  ([^Gate g derefable timeout-ms]
   (let [^Semaphore sem (:semaphore g)
         gname           (:name g)]
     (if (.tryAcquire sem timeout-ms TimeUnit/MILLISECONDS)
       (try
         (let [result (deref derefable timeout-ms ::timeout)]
           (if (= result ::timeout)
             (throw (ex-info (str "Gate '" gname "' deref timed out after " timeout-ms "ms")
                             {:name gname :timeout-ms timeout-ms
                              :hint "Downstream may be locked or unresponsive"}))
             result))
         (finally
           (.release sem)))
       (throw (ex-info (str "Gate '" gname "' full — too many concurrent operations")
                       {:name gname :timeout-ms timeout-ms
                        :permits (:permits g)
                        :available (.availablePermits sem)}))))))

;; =============================================================================
;; Diagnostics
;; =============================================================================

(defn gate-stats
  "Current gate state for diagnostics."
  [^Gate g]
  (let [^Semaphore sem (:semaphore g)]
    {:name         (:name g)
     :permits      (:permits g)
     :available    (.availablePermits sem)
     :queue-length (.getQueueLength sem)}))
