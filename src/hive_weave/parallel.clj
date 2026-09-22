(ns hive-weave.parallel
  "Bounded parallel execution — safe alternatives to pmap and raw futures.

   - `bounded-pmap`  — pmap with concurrency limit + per-item timeout
   - `fork-join`     — concurrent futures with collective timeout budget
   - `fan-out`       — fire N tasks, collect results with timeout

   Unlike `pmap`, these primitives:
   1. Bound concurrency (no unbounded thread creation)
   2. Have timeouts (no indefinite hangs)
   3. Return fallback values on timeout (graceful degradation)"
  (:require [hive-dsl.result :as r]
            [taoensso.timbre :as log]
            [hive-weave.stack :as stack]
            [hive-weave.pool :as pool])
  (:import [java.util.concurrent Executors TimeUnit TimeoutException]
[java.util.concurrent Semaphore]
[java.util.concurrent ExecutorService]))

;; =============================================================================
;; Bounded pmap
;; =============================================================================

(defn bounded-pmap
  "Like pmap but with bounded concurrency and per-item timeout.

   Options:
     :concurrency max items in flight at once (default 4)
     :timeout-ms  per-item timeout in ms (default 10000)
     :fallback    value for timed-out/failed items (default nil)
     :stack-bytes explicit worker stack size (default: the JVM's)
     :virtual?    run on virtual threads (default: whatever this JVM has)

   (bounded-pmap {:concurrency 3 :timeout-ms 5000}
     fetch-entry-preview entry-ids)
   ;; => [result1 result2 nil result4 ...]  (nil = timed out)

   On a JVM with virtual threads the bound is a SEMAPHORE, not a thread count:
   every item is submitted at once and waits its turn for a permit. The number
   of items running together is exactly `:concurrency` either way, so the
   observable behaviour is unchanged; what goes away is the fixed pool of N
   platform threads that each call used to create and destroy. A caller that
   ran four of these per request was starting and stopping sixteen OS threads
   to do sixteen short reads. The carrier threads underneath the virtual ones
   are not free either, but they are created once, up to the core count, and
   shared by every fan-out in the process rather than minted per call.

   Below 21 the fixed pool is still the mechanism, and `:virtual? false` forces
   it on any JVM, which is how that branch stays testable.

   Pass :stack-bytes when `f` calls into native code that recurses: the default
   worker stack is sized for Clojure, and a native overflow is a SIGSEGV that
   takes the process down instead of yielding the fallback (hive-weave.stack).
   A virtual thread's stack is not configurable, so :stack-bytes always means
   platform threads and overrides :virtual? rather than being quietly dropped."
  [{:keys [concurrency timeout-ms fallback stack-bytes virtual?]
    :or   {concurrency 4 timeout-ms 10000 fallback nil}}
   f coll]
  (if (empty? coll)
    []
    (let [virtual? (if stack-bytes
                     false
                     (if (some? virtual?) virtual? (pool/virtual-threads?)))
          ^ExecutorService pool
          (cond
            virtual?    (pool/virtual-executor)
            stack-bytes (Executors/newFixedThreadPool
                         (int concurrency)
                         (stack/thread-factory {:name "bounded-pmap" :stack-bytes stack-bytes}))
            :else       (Executors/newFixedThreadPool (int concurrency)))
          ;; Fair, so a long collection cannot starve its own tail.
          ^Semaphore gate (when virtual? (Semaphore. (int concurrency) true))
          run (if gate
                (fn [item]
                  ;; An interrupt during the wait leaves the callable throwing,
                  ;; which the collector below reads as a failed item. The
                  ;; permit is released only on the path that took one.
                  (.acquire gate)
                  (try
                    (r/rescue-log "bounded-pmap" fallback (f item))
                    (finally (.release gate))))
                (fn [item]
                  (r/rescue-log "bounded-pmap" fallback (f item))))]
      (try
        (let [tasks (mapv (fn [item]
                            (.submit pool
                                     ^java.util.concurrent.Callable
                                     (fn [] (run item))))
                          coll)
              batches (long (Math/ceil (/ (count coll) (double concurrency))))
              deadline (+ (System/currentTimeMillis) (* timeout-ms batches))]
          (mapv (fn [task]
                  (let [remaining (max 1 (- deadline (System/currentTimeMillis)))]
                    (try
                      (.get task remaining TimeUnit/MILLISECONDS)
                      (catch TimeoutException _
                        (.cancel task true)
                        (log/debug "bounded-pmap: item timed out after" timeout-ms "ms")
                        fallback)
                      (catch Exception _
                        fallback))))
                tasks))
        (finally
          (.shutdownNow pool))))))

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
     :budget-ms — total time budget for all tasks (default 15000)"
  [{:keys [budget-ms] :or {budget-ms 15000}} & tasks]
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
          futures)))

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
