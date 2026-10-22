(ns hive-weave.heap.schema
  "Malli value objects for hive-weave.heap. Drives the m/=> contracts on the
   sampler, the pure hysteresis state machine and the sentinel lifecycle.

   The Sentinel record predicate lives in hive-weave.heap itself (it needs the
   record class); everything here is plain data.")

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def HeapState
  "The 3-state pressure level the sentinel publishes."
  [:enum :normal :high :critical])

(def Fraction
  "Heap occupancy as used/max. Not clamped: a caller may feed any real."
  [:and number? [:fn {:error/message "finite"} #(not (Double/isNaN (double %)))]])

(def Watermarks
  "Hysteresis thresholds. Open: unknown keys are tolerated."
  [:map
   [:low-mark number?]
   [:high-mark number?]
   [:critical-mark number?]])

(def HeapSample
  "One Runtime memory reading (a HeapSample record, which is a map)."
  [:map
   [:used :int]
   [:max pos-int?]
   [:total nat-int?]
   [:free nat-int?]
   [:fraction :double]
   [:ts pos-int?]])

(def SentinelOpts
  "Options for start-sentinel!. Every key is optional."
  [:maybe
   [:map
    [:name {:optional true} :string]
    [:interval-ms {:optional true} pos-int?]
    [:watermarks {:optional true} Watermarks]
    [:start-state {:optional true} HeapState]]])

(def Snapshot
  "Point-in-time read of a sentinel."
  [:map {:closed true}
   [:sample HeapSample]
   [:state HeapState]
   [:ticks nat-int?]
   [:name :string]])

(def ObserverKey
  "Key an observer is registered under (any value usable as a map key)."
  :any)
