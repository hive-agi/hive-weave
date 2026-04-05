(ns hive-weave.core
  "hive-weave — bounded, timed, safe execution primitives.

   Weaves parallel/concurrent/async strands with built-in safety:
   every operation has a timeout, every parallel fan-out is bounded.

   Modules:
     hive-weave.safe      — safe deref, safe future (never hangs)
     hive-weave.gate      — semaphore-bounded execution
     hive-weave.parallel  — bounded-pmap, fork-join, fan-out
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
            [hive-weave.timed :as timed]))

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

;;; --- Timed ---
(def wrap-timed timed/wrap-timed)
(def ->timed-interceptor timed/->timed-interceptor)
(def timed-handler timed/timed-handler)
