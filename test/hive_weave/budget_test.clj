(ns hive-weave.budget-test
  "Tests for hive-weave.budget — IBudgetGate protocol + ByteBudgetGate impl.

   Mirrors hive-weave.gpu-admission-test (which is the same suite via the
   :gpu/* alias layer). This file exercises the unit-agnostic surface and
   verifies that hive-weave.gate.Gate also satisfies IBudgetGate."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-weave.budget :as b]
            [hive-weave.gate :as gate]))

(deftest byte-budget-gate-rejects-over-capacity
  (let [g (b/byte-budget-gate {:capacity 100 :name "rej"})
        ret (b/with-budget g 200 (fn [] :unreached))]
    (is (r/err? ret))
    (is (= :budget/over-capacity (:error ret)))))

(deftest byte-budget-gate-times-out
  (testing "tryAcquire returns false when capacity is held; surface :budget/timeout"
    (let [g (b/byte-budget-gate {:capacity 10 :timeout-ms 100 :name "tmo"})
          ;; reserve all capacity in a background future
          hold (future (b/with-budget g 10 (fn [] (Thread/sleep 500) :held)))
          _    (Thread/sleep 50)
          ret  (b/with-budget g 5 (fn [] :unreached))]
      (is (r/err? ret))
      (is (= :budget/timeout (:error ret)))
      @hold)))

(deftest with-budget-counter-discipline
  (testing "inflight returns to 0 after success; admitted-total bumps emit-time"
    (let [g (b/byte-budget-gate {:capacity 100 :name "counters"})]
      (b/with-budget g 50 (fn [] :a))
      (b/with-budget g 50 (fn [] :b))
      (b/with-budget g 200 (fn [] :over))    ; rejected
      (let [s (b/byte-gate-stats g)]
        (is (= 2 (:admitted-total s)))
        (is (= 1 (:rejected-total s)))
        (is (= 0 (:inflight s)))))))

(deftest with-budget-releases-on-exception
  (testing "finally releases permits even when thunk throws"
    (let [g (b/byte-budget-gate {:capacity 100 :name "ex"})
          ret (b/with-budget g 50 (fn [] (throw (RuntimeException. "boom"))))]
      (is (r/err? ret))
      (is (= :budget/execution-failed (:error ret)))
      (is (= 0 (:inflight (b/byte-gate-stats g))))
      (is (= 100 (:available (b/byte-gate-stats g)))))))

(deftest byte-fork-join-fits-and-falls-back
  (let [g (b/byte-budget-gate {:capacity 50 :name "fj"})
        r (b/byte-fork-join {:gate g :total-ms 5000}
            [:fits    #(do :fits) 20]
            [:too-big #(do :unreached) 200 :sentinel])]
    (is (= :fits (:fits r)))
    (is (= :sentinel (:too-big r)))
    (is (pos? (:rejected-total (b/byte-gate-stats g))))))

(deftest byte-fork-join-empty-task-list
  (let [g (b/byte-budget-gate {:capacity 100 :name "empty"})]
    (is (= {} (b/byte-fork-join {:gate g :total-ms 1000})))))

;; --- L1 pure helpers ---

(deftest workload-parsing
  (is (= (b/->Workload :k :thunk 5 nil)
         (b/->workload [:k :thunk 5])))
  (is (= (b/->Workload :k :thunk 5 :fb)
         (b/->workload [:k :thunk 5 :fb])))
  (is (thrown? clojure.lang.ExceptionInfo (b/->workload [:k :thunk]))))

(deftest resolve-outcome-shape
  (let [w (b/->Workload :k :thunk 1 :fb)]
    (is (= [:k :fb]    (b/resolve-outcome w :hive-weave.budget/timed-out)))
    (is (= [:k :hello] (b/resolve-outcome w (r/ok :hello))))
    (is (= [:k :fb]    (b/resolve-outcome w (r/err :budget/timeout {}))))
    (is (= [:k :raw]   (b/resolve-outcome w :raw)))))

;; --- IBudgetGate conformance: Gate also satisfies the protocol ---

(deftest gate-satisfies-ibudget-gate
  (let [g     (gate/gate {:permits 3 :name "g-conformance" :timeout-ms 1000})
        acq   (b/-acquire g 1 1000)]
    (is (r/ok? acq))
    (let [s (b/-stats g)]
      (is (= :slot (:unit s)))
      (is (= 3     (:capacity s)))
      (is (= 1     (:inflight s)))
      (is (= 2     (:available s))))
    (b/-release g 1)
    (is (= 0 (:inflight (b/-stats g))))))

(deftest gate-rejects-cost-exceeding-permits
  (let [g (gate/gate {:permits 2 :name "g-over"})
        r (b/-acquire g 5 100)]
    (is (r/err? r))
    (is (= :budget/over-capacity (:error r)))))
