(ns hive-weave.gpu-admission-test
  "Property test for hive-weave.gpu admission control.

   Core invariant:
     For any budget B and any sequence of tasks declaring :vram-mb in [1, B],
     the sum of currently-inflight :vram-mb is ALWAYS <= B.

   Verified by sampling :inflight-mb at admission entry across many
   randomized iterations and asserting the invariant."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-weave.gpu :as g]))

;; =============================================================================
;; Invariant sampler
;; =============================================================================

(defn- sample-task
  "Build a task vector for gpu-fork-join that records inflight-mb post-admit
   into `samples` atom. The sample includes the budget so a separate pass
   can check the invariant."
  [gate samples k vram-mb]
  [k
   (fn []
     (let [stats (g/gpu-gate-stats gate)]
       (swap! samples conj stats)
       ;; tiny jitter so concurrent tasks actually overlap
       (Thread/sleep (long (rand-int 8)))
       :ok))
   vram-mb])

(defn- run-trial!
  "Run one admission trial with random task vram-mb declarations.
   Returns the recorded inflight samples."
  [budget-mb n-tasks max-vram-mb]
  (let [gate    (g/vram-budget-gate
                  {:budget-mb  budget-mb
                   :timeout-ms 30000
                   :name       (str "trial-" (rand-int 9999))})
        samples (atom [])
        tasks   (mapv (fn [i]
                        (sample-task gate samples (keyword (str "t" i))
                                     (inc (rand-int max-vram-mb))))
                      (range n-tasks))
        result  (apply g/gpu-fork-join {:gate gate :total-ms 60000} tasks)]
    {:result   result
     :samples  @samples
     :final    (g/gpu-gate-stats gate)
     :budget   budget-mb}))

;; =============================================================================
;; Tests
;; =============================================================================

(deftest over-budget-task-is-rejected
  (testing "Single task with :vram-mb > budget rejects with :gpu/over-budget"
    (let [gate (g/vram-budget-gate {:budget-mb 100 :name "rej"})
          r    (g/with-vram-budget gate 200 (fn [] :unreached))]
      (is (r/err? r))
      (is (= :gpu/over-budget (:error r))))))

(deftest empty-task-list-returns-empty-map
  (let [gate (g/vram-budget-gate {:budget-mb 100 :name "empty"})]
    (is (= {} (g/gpu-fork-join {:gate gate :total-ms 1000})))))

(deftest concurrent-tasks-fit-when-budget-allows
  (testing "Tasks that fit simultaneously all run, return their values"
    (let [gate (g/vram-budget-gate {:budget-mb 100 :name "fit"})
          r    (g/gpu-fork-join {:gate gate :total-ms 5000}
                 [:a #(do (Thread/sleep 30) :a) 30]
                 [:b #(do (Thread/sleep 30) :b) 30]
                 [:c #(do (Thread/sleep 30) :c) 30])]
      (is (= {:a :a :b :b :c :c} r))
      (is (= 0 (:inflight-mb (g/gpu-gate-stats gate))))
      (is (= 3 (:admitted-total (g/gpu-gate-stats gate)))))))

(deftest property-inflight-never-exceeds-budget
  (testing "Across random trials, inflight-mb stays <= budget at all sampled points"
    (let [trials   30
          violations
          (atom [])]
      (dotimes [_ trials]
        (let [budget   (+ 50 (rand-int 200))           ; 50..250
              n-tasks  (+ 5 (rand-int 15))             ; 5..20
              max-vram (max 5 (quot budget 3))         ; ensure most tasks fit concurrently
              outcome  (run-trial! budget n-tasks max-vram)]
          (doseq [s (:samples outcome)]
            (when (> (:inflight-mb s) (:budget outcome))
              (swap! violations conj {:budget    (:budget outcome)
                                      :inflight  (:inflight-mb s)
                                      :sample    s})))
          (is (= 0 (:inflight-mb (:final outcome)))
              "Final inflight must be 0 (all permits released)")))
      (is (empty? @violations)
          (str "Inflight exceeded budget at " (count @violations) " sample point(s)")))))

(deftest property-rejected-tasks-fall-back
  (testing "Tasks declaring vram-mb > budget receive their fallback (or nil)"
    (let [gate (g/vram-budget-gate {:budget-mb 50 :name "fallback"})
          r    (g/gpu-fork-join {:gate gate :total-ms 5000}
                 [:fits     #(do :fits) 20]
                 [:too-big  #(do :unreached) 200 :sentinel])]
      (is (= :fits (:fits r)))
      (is (= :sentinel (:too-big r)))
      (is (pos? (:rejected-total (g/gpu-gate-stats gate)))))))

(deftest stats-track-emit-time-counters
  (testing "admitted-total + rejected-total bump at emit-time, not collect-time"
    (let [gate (g/vram-budget-gate {:budget-mb 100 :name "counters"})]
      (g/with-vram-budget gate 50 (fn [] :a))
      (g/with-vram-budget gate 50 (fn [] :b))
      (g/with-vram-budget gate 200 (fn [] :over))   ; rejected
      (let [s (g/gpu-gate-stats gate)]
        (is (= 2 (:admitted-total s)))
        (is (= 1 (:rejected-total s)))
        (is (= 0 (:inflight-mb s)))))))
