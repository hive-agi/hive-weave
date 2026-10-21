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
          (mapv (fn [^java.util.concurrent.Future task]
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

(defn missing-indices
  "Indices of `results` that hold `sentinel`: the items a bounded-pmap run
   could not answer (timed out or threw). Pure."
  [sentinel results]
  (into [] (keep-indexed (fn [i v] (when (identical? sentinel v) i))) results))

(defn bounded-pmap-strict
  "Like `bounded-pmap`, but an item that times out or throws is an ERROR, not
   a fallback value. Throws ex-info {:type :hive-weave/pmap-incomplete
   :missing [index ...] :count n} when any item is missing; otherwise returns
   the full result vector.

   Use it for partition-and-combine work, where a substituted nil is
   indistinguishable from \"this chunk found nothing\" and a sum would come
   back silently low. Accepts the same options as `bounded-pmap` except
   :fallback, which it owns."
  [opts f coll]
  (let [sentinel (Object.)
        results  (bounded-pmap (assoc opts :fallback sentinel) f coll)
        missing  (missing-indices sentinel results)]
    (if (seq missing)
      (throw (ex-info (str "bounded-pmap-strict: " (count missing) " of "
                           (count results) " items timed out or failed")
                      {:type    :hive-weave/pmap-incomplete
                       :missing missing
                       :count   (count results)}))
      results)))

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

(defn map-within
  "Apply `f` to every item of `coll` on `:pool`, all under ONE deadline.
   Returns a vector of Results, one per item, in input order:

     (ok value)                      the item finished inside the deadline
     (err :weave/timeout   {...})    the deadline passed first; the task is cancelled
     (err :weave/exception {...})    `f` threw
     (err :weave/rejected  {...})    a saturated :abort pool refused the item

   Every error carries :index (position in `coll`) and :name.

   Options:
     :pool        ExecutorService to run on (required). It is NOT shut down: the
                  caller owns it, which is what makes the concurrency bound a
                  property of the pool rather than of one call.
     :deadline-ms wall-clock budget for the WHOLE batch (required, pos-int)
     :name        diagnostic label (default map-within)

   The difference from `bounded-pmap`: its timeout is per item and the batch
   bound is that figure times the number of waves, so a stalled backend costs a
   caller `timeout x waves`. Here the budget is the one number the caller
   states, however many items there are. Items still queued or running when it
   passes are cancelled, so an expired batch does not keep the pool busy.

   Items go through `hive-weave.pool/submit!`, so the caller's dynamic bindings
   reach the pool threads. Pair this with a pool built with `:rejection :abort`:
   a :caller-runs pool runs the overflow on the submitting thread, where the
   deadline cannot interrupt it. Nothing is thrown for a failed item; the Result
   carries it, so a caller can tell no-data from the-read-failed."
  [{:keys [^ExecutorService pool deadline-ms name] :or {name "map-within"}} f coll]
  {:pre [(some? pool) (pos-int? deadline-ms)]}
  (let [deadline (+ (System/nanoTime) (* (long deadline-ms) 1000000))
        wrap     (fn [item]
                   (fn []
                     (try
                       (f item)
                       (catch Throwable t {::exception t}))))
        futures  (mapv (fn [item]
                         (try
                           (pool/submit! pool (wrap item))
                           (catch RuntimeException e
                             (if (pool/rejected? e) ::rejected (throw e)))))
                       coll)
        ctx      (fn [i] {:name name :index i})
        timeout  (fn [i] (r/err :weave/timeout (assoc (ctx i) :timeout-ms deadline-ms)))]
    (try
      (into []
            (map-indexed
             (fn [i fut]
               (if (= ::rejected fut)
                 (r/err :weave/rejected (assoc (ctx i) :pool (pool/pool-stats pool)))
                 (let [remaining-ns (max 0 (- deadline (System/nanoTime)))]
                   (try
                     (let [v (.get ^java.util.concurrent.Future fut remaining-ns TimeUnit/NANOSECONDS)]
                       (if-let [ex (and (map? v) (::exception v))]
                         (r/err :weave/exception
                                (assoc (ctx i)
                                       :message (.getMessage ^Throwable ex)
                                       :class (str (class ex))))
                         (r/ok v)))
                     (catch TimeoutException _
                       (.cancel ^java.util.concurrent.Future fut true)
                       (log/debug "map-within:" name "item" i "hit the" deadline-ms "ms deadline")
                       (timeout i))
                     (catch java.util.concurrent.CancellationException _
                       (timeout i))))))
             futures))
      (finally
        ;; Reached early only if the collecting thread is interrupted: do not
        ;; leave the rest of the batch holding pool threads.
        (doseq [fut futures]
          (when (instance? java.util.concurrent.Future fut)
            (.cancel ^java.util.concurrent.Future fut true)))))))
