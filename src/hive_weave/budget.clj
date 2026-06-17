(ns hive-weave.budget
  "Unit-agnostic budget gate. 1 permit = 1 unit; caller picks the unit
   (bytes/MiB/slots/etc). Saturation policy: block up to :timeout-ms,
   then (r/err :budget/timeout ...)."
  (:require [hive-dsl.result :as r])
  (:import [java.util.concurrent Semaphore TimeUnit]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defprotocol IBudgetGate
  (-acquire [g cost timeout-ms]
    "Reserve cost units. Returns Result. On ok, caller MUST -release the same cost.")
  (-release [g cost]
    "Release cost units. Call exactly once per successful -acquire.")
  (-stats [g]
    "Snapshot: {:name :capacity :unit :inflight :available :queued
                :admitted-total :rejected-total :timeout-ms}."))

(defrecord ByteBudgetGate
  [name capacity unit timeout-ms ^Semaphore semaphore
   inflight queued admitted-total rejected-total])

(defn- over-capacity-err [g cost]
  (swap! (:rejected-total g) inc)
  (r/err :budget/over-capacity
         {:name      (:name g)
          :capacity  (:capacity g)
          :unit      (:unit g)
          :requested cost}))

(defn- timeout-err [g cost]
  (r/err :budget/timeout
         {:name       (:name g)
          :capacity   (:capacity g)
          :unit       (:unit g)
          :requested  cost
          :inflight   @(:inflight g)
          :timeout-ms (:timeout-ms g)}))

(extend-type ByteBudgetGate
  IBudgetGate
  (-acquire [g cost timeout-ms]
    (let [^Semaphore sem (:semaphore g)
          capacity       (:capacity g)
          cost           (int cost)
          tms            (int (or timeout-ms (:timeout-ms g)))]
      (cond
        (or (not (pos? cost)) (> cost capacity))
        (over-capacity-err g cost)

        :else
        (do
          (swap! (:queued g) inc)
          (try
            (if (.tryAcquire sem cost tms TimeUnit/MILLISECONDS)
              (do
                (swap! (:inflight g) + cost)
                (swap! (:admitted-total g) inc)
                (r/ok cost))
              (timeout-err g cost))
            (finally
              (swap! (:queued g) dec)))))))

  (-release [g cost]
    (let [^Semaphore sem (:semaphore g)
          cost           (int cost)]
      (swap! (:inflight g) - cost)
      (.release sem cost)
      nil))

  (-stats [g]
    (let [^Semaphore sem (:semaphore g)]
      {:name           (:name g)
       :capacity       (:capacity g)
       :unit           (:unit g)
       :inflight       @(:inflight g)
       :available      (.availablePermits sem)
       :queued         @(:queued g)
       :queue-length   (.getQueueLength sem)
       :admitted-total @(:admitted-total g)
       :rejected-total @(:rejected-total g)
       :timeout-ms     (:timeout-ms g)})))

(defn byte-budget-gate
  "Options: :capacity (required), :unit (default :mib), :timeout-ms (30000),
   :name (\"byte-budget-gate\"), :fair? (true)."
  [{:keys [capacity unit timeout-ms name fair?]
    :or   {unit :mib timeout-ms 30000 name "byte-budget-gate" fair? true}}]
  (assert (and (integer? capacity) (pos? capacity))
          "byte-budget-gate requires positive :capacity")
  (->ByteBudgetGate name capacity unit timeout-ms
                    (Semaphore. (int capacity) (boolean fair?))
                    (atom 0) (atom 0) (atom 0) (atom 0)))

(defn byte-gate-stats [g] (-stats g))

(defn with-budget
  "Acquire cost units, run thunk, release. Returns Result.
   Errors: :budget/over-capacity, :budget/timeout, :budget/execution-failed."
  ([g cost thunk]
   (with-budget g cost nil thunk))
  ([g cost timeout-ms thunk]
   (let [acq (-acquire g cost (or timeout-ms (:timeout-ms g) 30000))]
     (if (r/ok? acq)
       (try
         (r/ok (thunk))
         (catch Exception e
           (r/err :budget/execution-failed
                  {:name    (:name g)
                   :message (.getMessage e)
                   :class   (str (class e))}))
         (finally
           (-release g cost)))
       acq))))

(defn with-byte-budget [g cost thunk] (with-budget g cost thunk))

;; --- byte-fork-join ---

(defrecord Workload [key thunk cost fallback])
(defrecord Submission [workload future])

(defn ->workload
  "Parse [k thunk cost] or [k thunk cost fallback] into a Workload."
  [task]
  (let [n (count task)]
    (cond
      (= n 3) (->Workload (nth task 0) (nth task 1) (nth task 2) nil)
      (= n 4) (->Workload (nth task 0) (nth task 1) (nth task 2) (nth task 3))
      :else
      (throw (ex-info "byte-fork-join task must be [k thunk cost] or [k thunk cost fallback]"
                      {:task task :count n})))))

(defn ->deadline [total-ms]
  (+ (System/currentTimeMillis) total-ms))

(defn remaining-ms [deadline]
  (max 0 (- deadline (System/currentTimeMillis))))

(defn resolve-outcome
  "Map an awaited outcome onto [key value-or-fallback]. Total fn."
  [{:keys [key fallback]} outcome]
  (cond
    (= outcome ::timed-out)               [key fallback]
    (and (map? outcome) (r/ok? outcome))  [key (:ok outcome)]
    (and (map? outcome) (r/err? outcome)) [key fallback]
    :else                                 [key outcome]))

(defn submit-workload [gate workload]
  (->Submission workload
                (future (with-budget gate (:cost workload) (:thunk workload)))))

(defn await-outcome [submission deadline]
  (deref (:future submission) (remaining-ms deadline) ::timed-out))

(defn cancel-if-timed-out [submission outcome]
  (when (= outcome ::timed-out)
    (future-cancel (:future submission)))
  outcome)

(defn collect-submission [submission deadline]
  (->> (await-outcome submission deadline)
       (cancel-if-timed-out submission)
       (resolve-outcome (:workload submission))))

(defn byte-fork-join
  "Concurrent dispatch under one IBudgetGate. Tasks: [k thunk cost] or
   [k thunk cost fallback]. Returns {k result-or-fallback}.

   Options: :gate (required), :total-ms (default 30000)."
  [{:keys [gate total-ms] :or {total-ms 30000}} & tasks]
  (assert gate "byte-fork-join requires :gate (an IBudgetGate impl)")
  (let [workloads   (mapv ->workload tasks)
        submissions (mapv #(submit-workload gate %) workloads)
        deadline    (->deadline total-ms)]
    (into {} (map #(collect-submission % deadline)) submissions)))
