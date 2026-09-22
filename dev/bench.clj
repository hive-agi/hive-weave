(ns bench
  "Bounded-execution shootout: hive-weave against the JDK pools, virtual
   threads, dirigiste/manifold (what aleph runs), claypoole, promesa,
   core.async and bare futures.

   Every competitor is reduced to one shape — submit a thunk, count rejections,
   shut down — so the only thing that varies is the executor. Run it from the
   :bench alias; see the (comment ...) block at the bottom.

   Metrics per run: wall ms to drain, completed, rejected, p50/p99 end-to-end
   latency, peak live threads, and how long the SUBMITTER was blocked (which is
   what CallerRuns costs you)."
  (:require [hive-weave.pool :as wp]
            [manifold.executor :as ex]
            [com.climate.claypoole :as cp]
            [promesa.exec :as px]
            [clojure.core.async :as a]
            [clojure.data.json :as json]
            [hive-weave.gate :as gate])
  (:import [java.util.concurrent Executors ExecutorService CountDownLatch
            TimeUnit RejectedExecutionException ThreadPoolExecutor
            LinkedBlockingQueue ThreadPoolExecutor$AbortPolicy]
           [java.lang.management ManagementFactory]))

(def cores (.availableProcessors (Runtime/getRuntime)))
(def ceiling 32)
(def queue-cap 256)

(defn load-avg [] (.getSystemLoadAverage (ManagementFactory/getOperatingSystemMXBean)))
(defn live-threads [] (.getThreadCount (ManagementFactory/getThreadMXBean)))
(defn reset-peak! [] (.resetPeakThreadCount (ManagementFactory/getThreadMXBean)))
(defn peak-threads [] (.getPeakThreadCount (ManagementFactory/getThreadMXBean)))

;; ---------------------------------------------------------------------------
;; Workloads
;; ---------------------------------------------------------------------------

(defn spin-us
  "Burn `us` microseconds of CPU. Not a sleep: a sleeping thread is not a busy
   worker, and the two produce opposite pool behaviour."
  [us]
  (let [end (+ (System/nanoTime) (* 1000 (long us)))]
    (loop [x 0] (if (< (System/nanoTime) end) (recur (inc x)) x))))

(def scenarios
  [{:id :cpu-20us :label "CPU 20us x 50k" :n 50000 :task #(spin-us 20)}
   {:id :cpu-2ms  :label "CPU 2ms x 2k"   :n 2000  :task #(spin-us 2000)}
   {:id :io-10ms  :label "IO 10ms x 2k"   :n 2000  :task #(Thread/sleep 10)}
   {:id :io-50ms  :label "IO 50ms x 500"  :n 500   :task #(Thread/sleep 50)}])

;; ---------------------------------------------------------------------------
;; Competitors
;; ---------------------------------------------------------------------------

(defn- es-runner
  "CallerRuns shape: a rejection is run by the SUBMITTER. Nothing is dropped;
   the caller pays."
  [^ExecutorService es rejected]
  {:submit (fn [thunk]
             (try (.execute es ^Runnable thunk)
                  (catch RejectedExecutionException _ (swap! rejected inc) (thunk))))
   :close  (fn [] (.shutdown es) (.awaitTermination es 10 TimeUnit/SECONDS))})

(defn- es-runner-strict
  "Abort shape: a rejection is DROPPED and counted, which is what an :abort pool
   and aleph's default executor do."
  [^ExecutorService es rejected drop!]
  {:submit (fn [thunk]
             (try (.execute es ^Runnable thunk)
                  (catch RejectedExecutionException _ (swap! rejected inc) (drop!))))
   :close  (fn [] (.shutdown es) (.awaitTermination es 10 TimeUnit/SECONDS))})

(defn jdk-tpe ^ThreadPoolExecutor [n queue]
  (ThreadPoolExecutor. (int n) (int n) 60 TimeUnit/SECONDS
                       (LinkedBlockingQueue. (int queue))
                       (Executors/defaultThreadFactory)
                       (ThreadPoolExecutor$AbortPolicy.)))

(defn competitors
  "[key description (fn [rejected drop!] -> {:submit :close})]"
  []
  [[:weave-caller-runs "hive-weave make-pool, CallerRuns (default), q256"
    (fn [rej _drop]
      (es-runner (wp/make-pool {:name "weave-cr" :size ceiling :queue-capacity queue-cap}) rej))]

   [:weave-abort "hive-weave make-pool :rejection :abort, q256"
    (fn [rej drop]
      (es-runner-strict (wp/make-pool {:name "weave-ab" :size ceiling
                                       :queue-capacity queue-cap :rejection :abort})
                        rej drop))]

   [:weave-512 "hive-weave make-pool, 512 threads (the ceiling raised)"
    (fn [rej _drop]
      (es-runner (wp/make-pool {:name "weave-512" :size 512 :queue-capacity queue-cap}) rej))]

   [:virtual-gated "hive-weave io-executor (virtual threads) behind a 32-permit gate"
    (fn [rej _drop]
      (let [e (wp/io-executor {:name "weave-virtual"})
            g (gate/gate {:name "weave-virtual-gate" :permits ceiling :timeout-ms 120000})]
        {:submit (fn [thunk]
                   (try (.execute ^ExecutorService e ^Runnable (fn [] (gate/gate-run g thunk)))
                        (catch RejectedExecutionException _ (swap! rej inc) (thunk))))
         :close (fn [] (.shutdown ^ExecutorService e)
                  (.awaitTermination ^ExecutorService e 10 TimeUnit/SECONDS))}))]

   [:jdk-fixed "JDK newFixedThreadPool, unbounded queue"
    (fn [rej _drop] (es-runner (Executors/newFixedThreadPool ceiling) rej))]

   [:jdk-bounded "JDK ThreadPoolExecutor, q256 + AbortPolicy"
    (fn [rej drop] (es-runner-strict (jdk-tpe ceiling queue-cap) rej drop))]

   [:jdk-virtual "JDK virtual threads, thread-per-task"
    (fn [rej _drop] (es-runner (Executors/newVirtualThreadPerTaskExecutor) rej))]

   [:dirigiste-util "manifold utilization-executor (aleph's default shape)"
    (fn [rej drop] (es-runner-strict (ex/utilization-executor 0.9 ceiling) rej drop))]

   [:dirigiste-q256 "dirigiste instrumented + utilization controller, q256"
    (fn [rej drop]
      (es-runner-strict
       (ex/instrumented-executor
        {:controller (io.aleph.dirigiste.Executors/utilizationController 0.9 ceiling)
         :queue-length queue-cap
         :initial-thread-count 1})
       rej drop))]

   [:claypoole "claypoole threadpool"
    (fn [rej _drop] (es-runner (cp/threadpool ceiling) rej))]

   [:promesa "promesa fixed executor"
    (fn [rej _drop] (es-runner (px/fixed-executor :parallelism ceiling) rej))]

   [:core-async-thread "core.async/thread, cached and unbounded"
    (fn [_rej _drop] {:submit (fn [thunk] (a/thread (thunk))) :close (fn [] nil)})]

   [:clojure-future "clojure.core/future, unbounded"
    (fn [_rej _drop] {:submit (fn [thunk] (future (thunk))) :close (fn [] nil)})]])

;; ---------------------------------------------------------------------------
;; Runner
;; ---------------------------------------------------------------------------

(defn pct [^longs arr p]
  (let [v (vec (sort (remove neg? (seq arr))))]
    (when (seq v) (/ (double (nth v (min (dec (count v)) (int (* p (count v)))))) 1e6))))

(defn median [xs]
  (let [v (vec (sort xs))] (nth v (quot (count v) 2))))

(defn run-once [factory task n]
  (let [lat       (long-array n -1)
        latch     (CountDownLatch. n)
        rejected  (atom 0)
        drop!     (fn [] (.countDown latch))
        {:keys [submit close]} (factory rejected drop!)
        _         (reset-peak!)
        t0        (System/nanoTime)
        submit-ns (volatile! 0)]
    (dotimes [i n]
      (let [s (System/nanoTime)]
        (submit (fn []
                  (task)
                  (aset lat i (- (System/nanoTime) s))
                  (.countDown latch)))
        (vswap! submit-ns + (- (System/nanoTime) s))))
    (let [drained? (.await latch 180 TimeUnit/SECONDS)
          wall     (/ (double (- (System/nanoTime) t0)) 1e6)]
      (close)
      {:wall-ms      (Math/round wall)
       :submit-ms    (Math/round (/ (double @submit-ns) 1e6))
       :completed    (- n @rejected)
       :rejected     @rejected
       :drained?     drained?
       :p50-ms       (pct lat 0.5)
       :p99-ms       (pct lat 0.99)
       :peak-threads (peak-threads)
       :throughput   (double (/ (- n @rejected) (max 1e-9 (/ wall 1000.0))))})))

(defn run-all [{:keys [trials] :or {trials 3}}]
  (vec
   (for [{:keys [id label n task]} scenarios
         [k desc factory] (competitors)]
     (let [_    (run-once factory task (max 100 (quot n 20)))
           _    (System/gc)
           runs (vec (for [_ (range trials)] (run-once factory task n)))
           pick (fn [kw] (median (map kw runs)))]
       (println (format "%-18s %-9s wall=%6dms thr=%9.0f/s rej=%5d p99=%s"
                        (name k) (name id) (long (pick :wall-ms)) (pick :throughput)
                        (long (pick :rejected)) (pick :p99-ms)))
       {:scenario (name id) :scenario-label label :competitor (name k) :description desc
        :n n
        :wall-ms (pick :wall-ms) :throughput (pick :throughput)
        :rejected (pick :rejected) :completed (pick :completed)
        :p50-ms (pick :p50-ms) :p99-ms (pick :p99-ms)
        :submit-ms (pick :submit-ms) :peak-threads (pick :peak-threads)
        :runs runs}))))

(defn env []
  {:java (System/getProperty "java.version")
   :os (System/getProperty "os.name")
   :cores cores :ceiling ceiling :queue-capacity queue-cap
   :load-avg (load-avg) :live-threads (live-threads)
   :date (str (java.time.LocalDate/now))})

(defn go!
  "Run everything and write {:env ... :rows [...]} as JSON to `out`."
  [out]
  (let [rows (run-all {:trials 3})
        data {:env (env) :rows rows}]
    (spit out (json/write-str data))
    (println "wrote" out)
    (:env data)))

;; ---------------------------------------------------------------------------
;; The site contract
;;
;; hive-mcp-www renders a measurement from data/bench.json: tasks x arms x
;; metrics, each cell a mean with a 95% interval. Emitting THAT shape is what
;; lets the site draw this benchmark with the machinery it already has (Hanami
;; specs, vl-convert renders, the Measured section) instead of a second one.

(def site-arms
  "Four arms, chosen so every claim the section makes is a bar somebody can
   read, and no more: the bounded default, the same pool shedding instead of
   blocking the caller, the same pool with its ceiling raised, and the JVM's
   own answer to blocking IO. The other nine executors stay in this namespace
   for whoever wants the long table.

   `:id` is the site-facing arm id and `:competitor` is the key in this
   namespace's own results. The baseline is spelled \"baseline\" because that is
   the id hive-mcp-www's KPI strip subtracts; an arm named anything else would
   be reported as its own improvement over itself."
  [{:id "baseline"    :competitor "weave-caller-runs" :label "hive-weave, 32 threads"}
   {:id "weave-abort" :competitor "weave-abort"       :label "hive-weave, 32 threads, :abort"}
   {:id "weave-512"   :competitor "weave-512"         :label "hive-weave, 512 threads"}
   {:id "jdk-virtual" :competitor "jdk-virtual"       :label "JDK virtual threads"}])

(def site-tasks
  [{:id "io-10ms"  :title "2,000 tasks, 10 ms of IO each"   :difficulty "io"}
   {:id "io-50ms"  :title "500 tasks, 50 ms of IO each"     :difficulty "io"}
   {:id "cpu-20us" :title "50,000 tasks, 20 us of CPU each" :difficulty "cpu"}
   {:id "cpu-2ms"  :title "2,000 tasks, 2 ms of CPU each"   :difficulty "cpu"}])

(def site-metrics
  [{:id "wall_ms"   :label "time to drain the burst"           :unit "ms" :lower_is_better true}
   {:id "p99_ms"    :label "p99 latency of the tasks that ran" :unit "ms" :lower_is_better true}
   {:id "pass_rate" :label "share of the burst that ran"       :unit "ratio"}])

(def t975
  "Student t at 97.5% for n-1 degrees of freedom. Three trials is a small
   sample and the interval says so rather than pretending to be normal."
  {1 12.706, 2 4.303, 3 3.182, 4 2.776, 5 2.571})

(defn mean [xs] (/ (reduce + 0.0 xs) (count xs)))

(defn ci95
  "[lo hi] for xs. One trial, or identical trials, yields a zero-width interval
   rather than a fabricated one."
  [xs]
  (let [n (count xs)
        m (mean xs)]
    (if (< n 2)
      [m m]
      (let [ss (reduce + (map (fn [x] (let [d (- x m)] (* d d))) xs))
            sd (Math/sqrt (/ ss (dec n)))
            half (* (get t975 (dec n) 1.96) (/ sd (Math/sqrt n)))]
        [(- m half) (+ m half)]))))

(defn metric-samples [row metric]
  (case metric
    "wall_ms"   (map :wall-ms (:runs row))
    "p99_ms"    (map :p99-ms (:runs row))
    "pass_rate" (map (fn [r] (/ (double (:completed r)) (:n row))) (:runs row))))

(defn site-cells [rows]
  (vec (for [t site-tasks
             a site-arms
             m site-metrics
             :let [row (first (filter (fn [r] (and (= (:id t) (:scenario r))
                                                   (= (:competitor a) (:competitor r))))
                                      rows))]
             :when row
             :let [xs (vec (metric-samples row (:id m)))]]
         {:task (:id t) :arm (:id a) :metric (:id m)
          :n (count xs) :mean (mean xs) :ci95 (ci95 xs)})))

(defn cell-mean [cells arm metric task]
  (:mean (first (filter (fn [c] (and (= arm (:arm c)) (= metric (:metric c)) (= task (:task c))))
                        cells))))

(defn geo-ratio
  "Geometric mean of the per-task ratios against the baseline. The tasks differ
   by three orders of magnitude, so averaging raw means would let the
   50,000-task workload decide every number."
  [cells arm metric baseline]
  (let [logs (keep (fn [t]
                     (let [b (cell-mean cells baseline metric (:id t))
                           v (cell-mean cells arm metric (:id t))]
                       (when (and b v (pos? b) (pos? v)) (Math/log (/ v b)))))
                   site-tasks)]
    (when (seq logs) (Math/exp (mean logs)))))

(def summary-metrics
  "The two the KPI strip earns its space with: how long the burst took, and how
   much of it actually ran. p99 is charted but not carded, because a third card
   per arm turns a summary into a second table."
  #{"wall_ms" "pass_rate"})

(defn site-summary [cells baseline]
  (vec (for [a site-arms
             m site-metrics
             :when (contains? summary-metrics (:id m))
             :let [arm (:id a)
                   metric (:id m)
                   means (keep (fn [t] (cell-mean cells arm metric (:id t))) site-tasks)
                   ratio (when (not= arm baseline) (geo-ratio cells arm metric baseline))]
             :when (seq means)]
         (cond-> {:arm arm :metric metric :mean (mean means)}
           ratio (assoc :vs_baseline_ratio ratio)))))

(def site-method
  (str "Thirteen JVM executors were given the same four bursts: one thread submits every task as "
       "fast as it can, and a latch waits for each task the executor accepted. Four are drawn here; "
       "the rest, including claypoole, promesa, core.async, bare futures and the executor aleph runs "
       "by default, are in hive-weave's dev/bench.clj. Pools are capped at 32 threads and, where "
       "they have one, a 256-slot queue, except the arm whose ceiling is deliberately raised to 512 "
       "and virtual threads, which have no ceiling by construction. Each cell is three trials after "
       "a warm-up, as a mean with a t-based 95% interval. Share of the burst that ran is charted "
       "beside the clock, because an executor that refuses work finishes early."))

(defn site-bench
  "The measurement in hive-mcp-www's bench.json contract."
  [{:keys [rows env sha]}]
  (let [cells (site-cells rows)]
    {:generated (str (java.time.Instant/now))
     :lab "hive-weave dev/bench.clj"
     :runner {:kind "jvm" :model (str "OpenJDK " (:java env)) :version (str (:cores env) " cores")}
     :repo {:name "hive-weave" :sha (or sha "working tree")}
     :trials_per_cell 3
     :method site-method
     :tasks site-tasks
     ;; :competitor is this namespace's own join key, not part of the contract
     :arms (mapv (fn [a] (dissoc a :competitor)) site-arms)
     :metrics site-metrics
     :cells cells
     :summary (site-summary cells "baseline")
     :missing []}))

(defn site-bench!
  "Read a go! JSON file and write the site-shaped measurement to `out`."
  [in out sha]
  (let [d (json/read-str (slurp in) :key-fn keyword)
        b (site-bench {:rows (:rows d) :env (:env d) :sha sha})]
    (spit out (json/write-str b))
    (println "wrote" out "with" (count (:cells b)) "cells")
    (select-keys b [:lab :trials_per_cell :generated])))

(comment
  ;; clojure -M:bench -m nrepl.cmdline  (or spawn a REPL on the :bench alias)
  (env)
  (run-once (last (first (competitors))) #(spin-us 20) 1000)
  (go! "/tmp/weave-bench.json")
  )
