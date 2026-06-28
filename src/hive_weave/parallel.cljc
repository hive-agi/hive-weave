(ns hive-weave.parallel
  "Bounded parallel execution — safe alternatives to pmap and raw futures.

   - `bounded-pmap`  — pmap with concurrency limit + per-item timeout
   - `fork-join`     — concurrent futures with collective timeout budget
   - `fan-out`       — fire N tasks, collect results with timeout

   Unlike `pmap`, these primitives:
   1. Bound concurrency (no unbounded thread creation)
   2. Have timeouts (no indefinite hangs)
   3. Return fallback values on timeout (graceful degradation)

   ClojureScript: node is single-threaded, so there are no real futures and
   nothing to interrupt. The :cljs branches run items SEQUENTIALLY (a plain
   map) under the platform synchronous semaphore; the per-item/budget timeouts
   are advisory (a synchronous thunk runs to completion). The JVM branches keep
   their Semaphore + future + timed-deref impls verbatim."
  (:require [hive-dsl.result :as r]
            [hive-weave.platform :as platform]
            [hive-weave.safe :as safe]
            [taoensso.timbre :as log])
  #?(:clj (:import [java.util.concurrent Semaphore TimeUnit])))

;; =============================================================================
;; Bounded pmap
;; =============================================================================

(defn bounded-pmap
  "Like pmap but with bounded concurrency and per-item timeout.

   Options:
     :concurrency — max parallel workers (default 4)
     :timeout-ms  — per-item timeout in ms (default 10000)
     :fallback    — value for timed-out/failed items (default nil)

   (bounded-pmap {:concurrency 3 :timeout-ms 5000}
     fetch-entry-preview entry-ids)
   ;; => [result1 result2 nil result4 ...]  (nil = timed out)

   ClojureScript: items run sequentially under the platform sync semaphore;
   :timeout-ms is advisory (synchronous node work cannot be interrupted)."
  [{:keys [concurrency timeout-ms fallback]
    :or   {concurrency 4 timeout-ms 10000 fallback nil}}
   f coll]
  (if (empty? coll)
    []
    #?(:clj
       (let [sem (Semaphore. (int concurrency) true)
             process (fn [item]
                       (if (.tryAcquire sem timeout-ms TimeUnit/MILLISECONDS)
                         (try
                           (r/rescue-log "bounded-pmap" fallback
                             (let [fut (future (f item))
                                   result (deref fut timeout-ms ::timed-out)]
                               (if (= result ::timed-out)
                                 (do (future-cancel fut)
                                     (log/debug "bounded-pmap: item timed out after" timeout-ms "ms")
                                     fallback)
                                 result)))
                           (finally
                             (.release sem)))
                         (do (log/debug "bounded-pmap: semaphore acquire timed out")
                             fallback)))
             ;; Launch all items eagerly (bounded by semaphore)
             futures (mapv #(future (process %)) coll)]
         (mapv #(deref % (* 2 timeout-ms) fallback) futures))
       :cljs
       ;; single-threaded: run sequentially under the sync semaphore. Each
       ;; synchronous (f item) runs to completion between acquire and release,
       ;; so a cost-1 acquire against `concurrency` permits always succeeds.
       (let [sem (platform/make-semaphore (int concurrency))
             process (fn [item]
                       (if (platform/sem-try-acquire! sem 1)
                         (try
                           (r/rescue-log "bounded-pmap" fallback
                             (f item))
                           (finally
                             (platform/sem-release! sem 1)))
                         (do (log/debug "bounded-pmap: semaphore acquire timed out")
                             fallback)))]
         (mapv process coll)))))

;; =============================================================================
;; Fork-Join
;; =============================================================================

(defn fork-join
  "Execute named tasks concurrently with a collective timeout budget.
   Each task is a [key thunk] or [key thunk fallback] triple.
   Returns a map of {key result} — timed-out tasks get their fallback.

   (fork-join {:budget-ms 15000}
     [:tags   #(query-tags candidate-tags)   {}]
     [:kg     #(expand-via-kg vanilla-ids)   #{}])
   ;; => {:tags {...} :kg #{...}}

   Options:
     :budget-ms — total time budget for all tasks (default 15000)

   ClojureScript: single-threaded — thunks run sequentially in order;
   :budget-ms is advisory (no real futures/deadline)."
  [{:keys [budget-ms] :or {budget-ms 15000}} & tasks]
  #?(:clj
     (let [futures (into {}
                         (map (fn [task]
                                (let [[k thunk fallback] (if (= 3 (count task))
                                                           task
                                                           [(first task) (second task) nil])]
                                  [k {:future (future
                                                (r/rescue-log (str "fork-join " k) {::failed true}
                                                  (thunk)))
                                      :fallback fallback}])))
                         tasks)
           deadline (+ (System/currentTimeMillis) budget-ms)]
       (into {}
             (map (fn [[k {:keys [future fallback]}]]
                    (let [remaining (max 0 (- deadline (System/currentTimeMillis)))
                          result (deref future remaining ::timed-out)]
                      (cond
                        (= result ::timed-out)
                        (do (future-cancel future)
                            (log/debug "fork-join:" k "timed out")
                            [k fallback])

                        (and (map? result) (::failed result))
                        [k fallback]

                        :else
                        [k result]))))
             futures))
     :cljs
     ;; single-threaded: run each thunk synchronously, in order, under the
     ;; same rescue-log guard. No real futures, no deadline (budget-ms advisory).
     (into {}
           (map (fn [task]
                  (let [[k thunk fallback] (if (= 3 (count task))
                                             task
                                             [(first task) (second task) nil])
                        result (r/rescue-log (str "fork-join " k) {::failed true}
                                 (thunk))]
                    (if (and (map? result) (::failed result))
                      [k fallback]
                      [k result]))))
           tasks)))

;; =============================================================================
;; Fan-out
;; =============================================================================

(defn fan-out
  "Apply f to each item in coll concurrently, collect results with timeout.
   Like bounded-pmap but returns a Result for the whole batch.

   (fan-out {:concurrency 4 :timeout-ms 5000} fetch-preview ids)
   ;; => (ok [r1 r2 r3 ...]) — all completed
   ;; => (ok [r1 nil r3 ...]) — some timed out (nils)

   Always returns (ok ...) — individual failures become nil/fallback."
  [opts f coll]
  (r/ok (bounded-pmap opts f coll)))
