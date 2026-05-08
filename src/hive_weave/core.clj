(ns hive-weave.core
  "hive-weave — bounded, timed, safe execution primitives.

   Weaves parallel/concurrent/async strands with built-in safety:
   every operation has a timeout, every parallel fan-out is bounded.

   Modules:
     hive-weave.safe      — safe deref, safe future (never hangs)
     hive-weave.gate      — semaphore-bounded execution
     hive-weave.parallel  — bounded-pmap, fork-join, fan-out
     hive-weave.pool      — bounded ThreadPoolExecutor + safe await!
     hive-weave.timed     — timed interceptors, timed handlers

   Quick start:
     (require '[hive-weave.core :as weave])

     ;; Safe deref (replaces bare @)
     (weave/deref-safe my-promise 5000 fallback)

     ;; Bounded parallel map (replaces pmap)
     (weave/bounded-pmap {:concurrency 4 :timeout-ms 5000}
       fetch-preview ids)

     ;; Fork-join with budget
     (weave/fork-join {:budget-ms 15000}
       [:tags  #(query-tags tags)     {}]
       [:kg    #(expand-kg ids)       #{}])

     ;; Gate for resource protection
     (def db-gate (weave/gate {:permits 4 :timeout-ms 10000 :name \"db\"}))
     (weave/with-gate db-gate (query ...))

     ;; Timed interceptor (for hive-events)
     (weave/->timed-interceptor
       :id :my/enrichment :timeout-ms 15000
       :after (fn [ctx] ...))"
  (:require [hive-weave.safe :as safe]
            [hive-weave.gate :as gate]
            [hive-weave.parallel :as par]
            [hive-weave.pool :as pool]
            [hive-weave.timed :as timed]
            [hive-weave.guarded :as guarded]))

;;; --- Safe Deref & Future ---
(def deref-safe safe/deref-safe)
(def deref-safe! safe/deref-safe!)
(def safe-future-call safe/safe-future-call)
(defmacro safe-future [opts & body] `(safe/safe-future ~opts ~@body))

;;; --- Gate ---
(def gate gate/gate)
(def gate-run gate/gate-run)
(def gate-run! gate/gate-run!)
(defmacro with-gate [g & body] `(gate/with-gate ~g ~@body))
(defmacro with-gate-result [g & body] `(gate/with-gate-result ~g ~@body))
(def deref-gate gate/deref-gate)
(def gate-stats gate/gate-stats)

;;; --- Parallel ---
(def bounded-pmap par/bounded-pmap)
(def fork-join par/fork-join)
(def fan-out par/fan-out)

;;; --- Pool ---
(def make-pool pool/make-pool)
(def pool-submit! pool/submit!)
(def pool-await! pool/await!)
(def pool-stats pool/pool-stats)
(def pool-shutdown! pool/shutdown!)
(defmacro with-pool-await [pool opts & body]
  `(pool/with-pool-await ~pool ~opts ~@body))

;;; --- Timed ---
(def wrap-timed timed/wrap-timed)
(def ->timed-interceptor timed/->timed-interceptor)
(def timed-handler timed/timed-handler)

(def guarded-future-call
  "Run a thunk under :timeout-ms with cleanup hook + injectable alert.
   On timeout: future-cancel, run :on-cancel on a separate cleanup pool,
   fire :alert!, return (r/err :weave/timeout {...}). See
   `hive-weave.guarded` ns doc for the full contract."
  guarded/guarded-future-call)

(def guarded-await!
  "Pool-bound counterpart to guarded-future-call. Submits to a
   ThreadPoolExecutor (via hive-weave.pool/make-pool) and applies the
   same timeout / cleanup / alert contract, plus pool-stats embedded
   in the alert event so saturation is visible when timeouts cluster."
  guarded/guarded-await!)

(defmacro guarded-future
  "Macro form of guarded-future-call. Body is wrapped as the thunk."
  [opts & body]
  `(guarded/guarded-future ~opts ~@body))

(defmacro with-guarded-await
  "Macro form of guarded-await!. Submits body to pool with cleanup +
   alert hooks."
  [pool opts & body]
  `(guarded/with-guarded-await ~pool ~opts ~@body))