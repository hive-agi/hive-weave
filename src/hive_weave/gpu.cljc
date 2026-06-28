(ns hive-weave.gpu
  "VRAM-budget gate. Thin alias of hive-weave.budget with :unit :mib;
   preserves the historical :gpu/* error keys for existing callers."
  (:require [hive-dsl.result :as r]
            [hive-weave.budget :as budget]
            [hive-weave.parallel :as par]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:private err-remap
  {:budget/over-capacity    :gpu/over-budget
   :budget/timeout          :gpu/admission-timeout
   :budget/execution-failed :gpu/execution-failed})

(defn- remap-err [result]
  (if (and (map? result) (r/err? result))
    (update result :error #(get err-remap % %))
    result))

(defn vram-budget-gate
  [{:keys [budget-mb timeout-ms name fair?]
    :or   {budget-mb 4096 timeout-ms 30000 name "vram-gate" fair? true}}]
  (budget/byte-budget-gate
   {:capacity budget-mb :unit :mib :timeout-ms timeout-ms :name name :fair? fair?}))

(defn gpu-gate-stats [g]
  (let [s (budget/byte-gate-stats g)]
    (-> s
        (assoc :budget-mb   (:capacity s)
               :inflight-mb (:inflight s))
        (dissoc :capacity :inflight))))

(defn with-vram-budget [g vram-mb thunk]
  (remap-err (budget/with-budget g vram-mb thunk)))

(defn gpu-fork-join
  [{:keys [gate total-ms] :or {total-ms 30000}} & tasks]
  (apply budget/byte-fork-join {:gate gate :total-ms total-ms} tasks))

(def fork-join par/fork-join)
