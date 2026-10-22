(ns hive-weave.budget.schema
  "Malli value objects for hive-weave.budget: gate options, stats, the
   Result variants every admission path can yield, and the byte-fork-join
   workload shapes. The IBudgetGate predicate lives in hive-weave.budget
   (it needs the protocol).")

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def Cost
  "Units requested from a gate. Coerced with `int`; non-positive or
   over-capacity costs are rejected as a value, not thrown."
  [:or :int :double])

(def TimeoutMs
  "Milliseconds to wait for admission; nil defers to the gate's default."
  [:maybe nat-int?])

(def GateOpts
  "Options for byte-budget-gate. :capacity is required."
  [:map
   [:capacity pos-int?]
   [:unit {:optional true} :keyword]
   [:timeout-ms {:optional true} nat-int?]
   [:name {:optional true} :string]
   [:fair? {:optional true} :boolean]])

(def GateStats
  "Snapshot returned by -stats / byte-gate-stats. The admission totals are
   nil for hive-weave.gate.Gate, which does not count them."
  [:map {:closed true}
   [:name :string]
   [:capacity pos-int?]
   [:unit :keyword]
   [:inflight :int]
   [:available :int]
   [:queued :int]
   [:queue-length nat-int?]
   [:admitted-total [:maybe nat-int?]]
   [:rejected-total [:maybe nat-int?]]
   [:timeout-ms nat-int?]])

(def OverCapacityError
  "Open: hive-weave.gate.Gate adds a :hint."
  [:map
   [:error [:= :budget/over-capacity]]
   [:name :string]
   [:capacity pos-int?]
   [:unit :keyword]
   [:requested :int]
   [:hint {:optional true} :string]])

(def TimeoutError
  "ByteBudgetGate reports :inflight, hive-weave.gate.Gate :available."
  [:map
   [:error [:= :budget/timeout]]
   [:name :string]
   [:capacity pos-int?]
   [:unit :keyword]
   [:requested :int]
   [:inflight {:optional true} :int]
   [:available {:optional true} :int]
   [:timeout-ms nat-int?]])

(def ExecutionFailedError
  [:map {:closed true}
   [:error [:= :budget/execution-failed]]
   [:name :string]
   [:message [:maybe :string]]
   [:class :string]])

(def AcquireResult
  "-acquire: {:ok granted-cost} or an admission error."
  [:or
   [:map {:closed true} [:ok pos-int?]]
   [:multi {:dispatch :error}
    [:budget/over-capacity OverCapacityError]
    [:budget/timeout TimeoutError]]])

(def BudgetError
  "Any failed with-budget Result."
  [:multi {:dispatch :error}
   [:budget/over-capacity OverCapacityError]
   [:budget/timeout TimeoutError]
   [:budget/execution-failed ExecutionFailedError]])

(def BudgetResult
  "with-budget: {:ok thunk-value} or a BudgetError."
  [:or
   [:map {:closed true} [:ok :any]]
   BudgetError])

(def Task
  "A byte-fork-join task: [k thunk cost] or [k thunk cost fallback]."
  [:schema [:cat :any ifn? Cost [:? :any]]])

(def Workload
  "A parsed Task (a Workload record)."
  [:map
   [:key :any]
   [:thunk ifn?]
   [:cost Cost]
   [:fallback :any]])

(def OutcomeSource
  "What resolve-outcome reads off a Workload: only :key and :fallback."
  [:map
   [:key :any]
   [:fallback {:optional true} :any]])

(def Submission
  "A Workload in flight on a future."
  [:map
   [:workload Workload]
   [:future [:fn {:error/message "future"} future?]]])

(def Deadline
  "Absolute epoch-millis deadline."
  :int)

(def Outcome
  "[key value-or-fallback] — the collected form of one Submission."
  [:tuple :any :any])
