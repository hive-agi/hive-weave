(ns hive-weave.broker.schema
  "Malli value objects for hive-weave.broker: policy options, the policy
   decision variants, broker workloads and every Result submit can yield.
   The IResourceBroker predicate lives in hive-weave.broker."
  (:require [hive-weave.budget.schema :as bs]
            [hive-weave.heap.schema :as hs]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def PolicyOpts
  "Policy table knobs. Keys are optional (defaults apply) but never nil."
  [:map
   [:high-timeout-multiplier {:optional true} [:and number? pos?]]
   [:critical-retry-after-ms {:optional true} nat-int?]])

(def RejectArg
  [:map {:closed true}
   [:retry-after-ms nat-int?]
   [:state [:= :critical]]])

(def Decision
  "decide-policy output: [action arg]."
  [:multi {:dispatch first}
   [:pass   [:tuple [:= :pass] :int]]
   [:extend [:tuple [:= :extend] :int]]
   [:reject [:tuple [:= :reject] RejectArg]]])

(def BrokerWorkload
  "What submit takes. :timeout-ms defaults to 30000."
  [:map
   [:cost bs/Cost]
   [:thunk ifn?]
   [:timeout-ms {:optional true} [:maybe nat-int?]]])

(def HeapCriticalError
  [:map {:closed true}
   [:error [:= :broker/heap-critical]]
   [:retry-after-ms nat-int?]
   [:state [:= :critical]]])

(def SubmitResult
  "submit: a with-budget Result, or a reject-fast under :critical heap."
  [:or
   [:map {:closed true} [:ok :any]]
   [:multi {:dispatch :error}
    [:broker/heap-critical HeapCriticalError]
    [:budget/over-capacity bs/OverCapacityError]
    [:budget/timeout bs/TimeoutError]
    [:budget/execution-failed bs/ExecutionFailedError]]])

(def BrokerStats
  [:map {:closed true}
   [:gate bs/GateStats]
   [:heap hs/Snapshot]])

(def BrokerOpts
  "start-broker! options; each is optional."
  [:maybe
   [:map
    [:gate-opts {:optional true} bs/GateOpts]
    [:sentinel-opts {:optional true} hs/SentinelOpts]
    [:policy-opts {:optional true} PolicyOpts]]])

(def HeapState hs/HeapState)
