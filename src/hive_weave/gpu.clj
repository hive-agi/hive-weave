(ns hive-weave.gpu
  "VRAM-budget admission control for GPU work.

   Sibling to hive-weave.gate (which counts permits) — this counts MiB.
   The unit is the only difference; the underlying primitive is still
   java.util.concurrent.Semaphore (which IS a counted permit pool — N MiB
   of declared budget exactly maps onto N permits).

   Why a separate ns from gate:
     - The mental model is different. Callers think 'budget' not 'slots'.
     - Stats track inflight-mb + queued-tasks, not just available-permits.
     - Admission policy includes :gpu/over-budget rejection (a single task
       declaring more MiB than total budget cannot ever admit; gate would
       block forever).

   Usage:
     (def gpu-budget (vram-budget-gate {:budget-mb 6144 :name \"rtx4070\"}))

     (gpu-fork-join {:gate gpu-budget :total-ms 30000}
       [:embed-a #(emb/embed-text exec \"...\") 800]
       [:embed-b #(emb/embed-text exec \"...\") 800])

   The gate is constructed once per JVM (singleton over a physical GPU).
   gpu-fork-join admits tasks BEFORE creating their futures so we never
   have N futures racing for the same MiB.

   Counter discipline (memory 20260425182616-2e52e87b):
     :inflight-mb bumps at acquire-time (emit-time), decrements at finally.
     Never collect-time. Sample mid-run via gpu-gate-stats."
  (:require [hive-dsl.result :as r]
            [hive-weave.parallel :as par]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent Semaphore TimeUnit]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; VramBudgetGate record
;; =============================================================================

(defrecord VramBudgetGate
  [name
   budget-mb
   timeout-ms
   ^Semaphore semaphore
   inflight-mb     ; atom<long> — emit-time counter
   queued          ; atom<long> — admission queue depth
   admitted-total  ; atom<long> — lifetime admitted-task count
   rejected-total  ; atom<long> — lifetime over-budget rejections
   ])

;; =============================================================================
;; Constructor
;; =============================================================================

(defn vram-budget-gate
  "Create a VRAM-budget gate.

   Options:
     :budget-mb   — total MiB available to admit (default 4096)
     :timeout-ms  — max wait for admission per task (default 30000)
     :name        — diagnostic name (default \"vram-gate\")
     :fair?       — FIFO ordering for waiters (default true)

   The semaphore's permit count IS the budget in MiB (1 permit = 1 MiB).
   Acquiring N permits 'reserves' N MiB; releasing N permits 'frees' it."
  [{:keys [budget-mb timeout-ms name fair?]
    :or   {budget-mb 4096 timeout-ms 30000 name "vram-gate" fair? true}}]
  (->VramBudgetGate name
                    budget-mb
                    timeout-ms
                    (Semaphore. (int budget-mb) (boolean fair?))
                    (atom 0)
                    (atom 0)
                    (atom 0)
                    (atom 0)))

;; =============================================================================
;; Diagnostics
;; =============================================================================

(defn gpu-gate-stats
  "Snapshot of gate state for observability."
  [^VramBudgetGate g]
  (let [^Semaphore sem (:semaphore g)]
    {:name           (:name g)
     :budget-mb      (:budget-mb g)
     :inflight-mb    @(:inflight-mb g)
     :available-mb   (.availablePermits sem)
     :queued         @(:queued g)
     :queue-length   (.getQueueLength sem)
     :admitted-total @(:admitted-total g)
     :rejected-total @(:rejected-total g)}))

;; =============================================================================
;; Admission primitive
;; =============================================================================

(defn- over-budget-err
  [g vram-mb]
  (swap! (:rejected-total g) inc)
  (r/err :gpu/over-budget
         {:name      (:name g)
          :budget-mb (:budget-mb g)
          :requested vram-mb
          :hint      "Single task declared more MiB than total budget; cannot ever admit"}))

(defn- timeout-err
  [g vram-mb]
  (r/err :gpu/admission-timeout
         {:name        (:name g)
          :budget-mb   (:budget-mb g)
          :requested   vram-mb
          :inflight-mb @(:inflight-mb g)
          :timeout-ms  (:timeout-ms g)}))

(defn with-vram-budget
  "Execute thunk after acquiring vram-mb worth of budget.
   Returns Result.

   - (err :gpu/over-budget ...) when vram-mb > total budget (cannot ever admit)
   - (err :gpu/admission-timeout ...) when permits not granted within timeout
   - (err :gpu/execution-failed ...) when thunk throws
   - (ok <return>) otherwise

   Counter discipline: :inflight-mb bumps AT acquire-time (post-tryAcquire),
   decrements in finally. :queued bumps before tryAcquire, decrements after.
   This way gpu-gate-stats reads are always consistent with what's running."
  [^VramBudgetGate g vram-mb thunk]
  (let [^Semaphore sem (:semaphore g)
        budget-mb      (:budget-mb g)
        timeout-ms     (:timeout-ms g)
        vram-mb        (int vram-mb)]
    (cond
      (or (not (pos? vram-mb)) (> vram-mb budget-mb))
      (over-budget-err g vram-mb)

      :else
      (do
        (swap! (:queued g) inc)
        (try
          (if (.tryAcquire sem vram-mb timeout-ms TimeUnit/MILLISECONDS)
            (do
              (swap! (:inflight-mb g) + vram-mb)
              (swap! (:admitted-total g) inc)
              (try
                (r/ok (thunk))
                (catch Exception e
                  (r/err :gpu/execution-failed
                         {:name    (:name g)
                          :message (.getMessage e)
                          :class   (str (class e))}))
                (finally
                  (swap! (:inflight-mb g) - vram-mb)
                  (.release sem vram-mb))))
            (timeout-err g vram-mb))
          (finally
            (swap! (:queued g) dec)))))))

;; =============================================================================
;; gpu-fork-join — VRAM-aware concurrent dispatch
;; =============================================================================

(defn gpu-fork-join
  "Execute named tasks concurrently under a VRAM budget.

   Each task is a vector [k thunk vram-mb] or [k thunk vram-mb fallback].
   Returns a map of {k result} where each value is the thunk return on
   admit+success, the fallback (or nil) on timeout/over-budget/error.

   The gate ensures sum of inflight :vram-mb is always <= budget-mb. Tasks
   that cannot fit wait at the semaphore until permits free up; tasks that
   exceed total budget are rejected immediately.

   Options:
     :gate      — VramBudgetGate (REQUIRED)
     :total-ms  — collective timeout budget across all tasks (default 30000)

   Example:
     (gpu-fork-join {:gate gpu :total-ms 60000}
       [:a #(do-work :a) 800]
       [:b #(do-work :b) 800 :fallback-b])
     ;; => {:a <result-a> :b <result-b-or-fallback>}"
  [{:keys [^VramBudgetGate gate total-ms]
    :or   {total-ms 30000}}
   & tasks]
  (assert gate "gpu-fork-join requires :gate (a VramBudgetGate)")
  (let [futures
        (into {}
              (map (fn [task]
                     (let [n        (count task)
                           [k thunk vram-mb fallback]
                           (cond
                             (= n 3) [(nth task 0) (nth task 1) (nth task 2) nil]
                             (= n 4) (vec task)
                             :else (throw (ex-info "gpu-fork-join task must be [k thunk vram-mb] or [k thunk vram-mb fallback]"
                                                   {:task task})))]
                       [k {:future (future
                                     (with-vram-budget gate vram-mb thunk))
                           :fallback fallback}])))
              tasks)
        deadline (+ (System/currentTimeMillis) total-ms)]
    (into {}
          (map (fn [[k {:keys [future fallback]}]]
                 (let [remaining (max 0 (- deadline (System/currentTimeMillis)))
                       result    (deref future remaining ::timed-out)]
                   (cond
                     (= result ::timed-out)
                     (do (future-cancel future)
                         (log/debug "gpu-fork-join:" k "timed out")
                         [k fallback])

                     (and (map? result) (r/ok? result))
                     [k (:ok result)]

                     (and (map? result) (r/err? result))
                     (do (log/warn "gpu-fork-join:" k "err" (:error result))
                         [k fallback])

                     :else
                     [k result]))))
          futures)))

;; =============================================================================
;; Backwards-compat helpers
;; =============================================================================

(def fork-join
  "Alias to hive-weave.parallel/fork-join — non-GPU work continues to use it.
   Re-exported here so callers doing both kinds of work import one ns."
  par/fork-join)
