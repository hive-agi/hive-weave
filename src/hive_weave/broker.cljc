(ns hive-weave.broker
  "Resource broker — combines a budget gate (cost-based admission) with a heap
   sentinel (JVM pressure signal). Consumers (hive-mcp, hive-knowledge, ...)
   depend on IResourceBroker; the default impl applies a policy table at
   submit-time:

     :normal   → pass through with caller's timeout
     :high     → extend timeout (heap pressure: give GC headroom)
     :critical → reject-fast with :retry-after-ms (don't compound pressure)"
  (:require [hive-dsl.result :as r]
            [hive-weave.budget :as budget]
            [hive-weave.heap :as heap]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defprotocol IResourceBroker
  (submit [b workload]
    "Workload: {:cost N :thunk fn :timeout-ms? N}. Returns Result.
     Errors: :broker/heap-critical, :budget/over-capacity, :budget/timeout,
     :budget/execution-failed.")
  (broker-stats [b]
    "Merged {:gate ... :heap ...} snapshot."))

(def default-policy-opts
  {:high-timeout-multiplier 2.0
   :critical-retry-after-ms 1000})

(defn decide-policy
  "Pure: (state base-timeout-ms opts) → [action arg].
   Action variants:
     [:pass   timeout-ms]
     [:extend timeout-ms]   ; surface as extended-timeout pass-through
     [:reject {:retry-after-ms N :state :critical}]"
  [state base-timeout-ms {:keys [high-timeout-multiplier critical-retry-after-ms]
                          :or   {high-timeout-multiplier 2.0
                                 critical-retry-after-ms 1000}}]
  (case state
    :normal   [:pass   base-timeout-ms]
    :high     [:extend (long (* base-timeout-ms high-timeout-multiplier))]
    :critical [:reject {:retry-after-ms critical-retry-after-ms
                        :state          :critical}]))

(defrecord DefaultBroker [gate sentinel policy-opts])

(defn- current-heap-state [b]
  (:state (heap/snapshot (:sentinel b))))

(extend-type DefaultBroker
  IResourceBroker
  (submit [b {:keys [cost thunk timeout-ms]}]
    (assert (and cost thunk) "workload requires :cost and :thunk")
    (let [base               (or timeout-ms 30000)
          state              (current-heap-state b)
          [action arg]       (decide-policy state base (:policy-opts b))]
      (case action
        :reject (r/err :broker/heap-critical arg)
        :pass   (budget/with-budget (:gate b) cost arg thunk)
        :extend (budget/with-budget (:gate b) cost arg thunk))))

  (broker-stats [b]
    {:gate (budget/byte-gate-stats (:gate b))
     :heap (heap/snapshot (:sentinel b))}))

(defn start-broker!
  "Options:
     :gate-opts     — passed to byte-budget-gate
                      (default {:capacity 4096 :unit :mib :name \"broker-gate\"})
     :sentinel-opts — passed to start-sentinel!
                      (default {:name \"broker-heap\"})
     :policy-opts   — merged onto default-policy-opts
                      (default {})

   Caller must stop-broker! to release the sentinel's scheduler."
  ([] (start-broker! {}))
  ([{:keys [gate-opts sentinel-opts policy-opts]
     :or   {gate-opts     {:capacity 4096 :unit :mib :name "broker-gate"}
            sentinel-opts {:name "broker-heap"}
            policy-opts   {}}}]
   (->DefaultBroker (budget/byte-budget-gate gate-opts)
                    (heap/start-sentinel! sentinel-opts)
                    (merge default-policy-opts policy-opts))))

(defn stop-broker! [b]
  (heap/stop-sentinel! (:sentinel b))
  b)
