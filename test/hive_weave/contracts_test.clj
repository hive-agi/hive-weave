(ns hive-weave.contracts-test
  "Contract facets for the broker public API: hive-weave.heap,
   hive-weave.budget, hive-weave.broker.

   1. Schema-synthesized trifectas (hive-schemas.test) over the PURE fns —
      next-state, decide-policy, resolve-outcome, remaining-ms — with a :rel
      that states the documented behaviour, not merely the shape.
   2. Instrumented lifecycle: every m/=> in the three namespaces is
      instrumented (inputs AND outputs checked, violations throw) while real
      sentinels, gates and brokers run their normal paths.
   3. A local density gate: every public fn of the three namespaces carries
      an m/=> contract."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-schemas.test :as hst]
            [hive-weave.broker :as br]
            [hive-weave.broker.schema :as brs]
            [hive-weave.budget :as b]
            [hive-weave.budget.schema :as bs]
            [hive-weave.gate :as gate]
            [hive-weave.heap :as h]
            [hive-weave.heap.schema :as hs]
            [malli.core :as m]
            [malli.instrument :as mi]))

(def contracted-nss '[hive-weave.heap hive-weave.budget hive-weave.broker])

;; ============================================================================
;; 1. Schema-synthesized — pure fns
;; ============================================================================

(def wm {:low-mark 0.70 :high-mark 0.85 :critical-mark 0.95})

(hst/deftrifecta-from-schema next-state-hysteresis
  hive-weave.heap/next-state
  {:in        [:cat hs/HeapState [:double {:min 0.0 :max 1.2}] [:= {} wm]]
   :out       hs/HeapState
   :rel       (fn [[prev f {:keys [low-mark high-mark critical-mark]}] out]
                (= out
                   (cond
                     (>= f critical-mark) :critical
                     (< f low-mark)       :normal
                     ;; [low, high): hold :normal/:high; :critical steps down
                     (< f high-mark)      (if (= :normal prev) :normal :high)
                     ;; [high, critical): escalate :normal; hold :high/:critical
                     :else                (if (= :normal prev) :high prev))))
   :num-tests 200})

(hst/deftrifecta-from-schema decide-policy-table
  hive-weave.broker/decide-policy
  {:in        [:cat brs/HeapState [:int {:min 0 :max 10000000}]
               [:maybe [:map
                        [:high-timeout-multiplier {:optional true} [:double {:min 0.5 :max 10.0}]]
                        [:critical-retry-after-ms {:optional true} nat-int?]]]]
   :out       brs/Decision
   :rel       (fn [[state base opts] [action arg]]
                (case state
                  :normal   (and (= :pass action) (= base arg))
                  :high     (and (= :extend action)
                                 (= arg (long (* base (:high-timeout-multiplier opts 2.0)))))
                  :critical (and (= :reject action)
                                 (= arg {:retry-after-ms (:critical-retry-after-ms opts 1000)
                                         :state          :critical}))))
   ;; Decision is a :multi of tuples — schema-mutant synthesis has no key to
   ;; corrupt; the :rel above pins every variant's value instead.
   :mutation  false
   :num-tests 200})

(def outcome-in
  [:or [:= ::b/timed-out] [:map {:closed true} [:ok :int]]
   [:map {:closed true} [:error :keyword]] :int :string])

(hst/deftrifecta-from-schema resolve-outcome-total
  hive-weave.budget/resolve-outcome
  {:in        [:cat [:map [:key :keyword] [:fallback :int]] outcome-in]
   :out       bs/Outcome
   :rel       (fn [[{:keys [key fallback]} o] [k v]]
                (and (= key k)
                     (= v (cond (= o ::b/timed-out) fallback
                                (and (map? o) (contains? o :ok)) (:ok o)
                                (map? o) fallback
                                :else o))))
   :mutation  false
   :num-tests 200})

(hst/deftrifecta-from-schema remaining-ms-never-negative
  hive-weave.budget/remaining-ms
  {:in        [:cat :int]
   :out       nat-int?
   :rel       (fn [[deadline] out]
                (or (zero? out) (<= out (- deadline (System/currentTimeMillis) -1000))))
   :num-tests 100})

;; ============================================================================
;; 2. Instrumented lifecycle — effectful fns checked at their boundary
;; ============================================================================

(use-fixtures :once
  (fn [f]
    (mi/instrument! {:filters [(apply mi/-filter-ns contracted-nss)]})
    (try (f) (finally (mi/unstrument! {:filters [(apply mi/-filter-ns contracted-nss)]})))))

(deftest heap-lifecycle-conforms
  (let [s (h/start-sentinel! {:name "ctr-heap" :interval-ms 20})]
    (try
      (is (m/validate hs/HeapSample (h/sample!)))
      (Thread/sleep 60)
      (is (m/validate hs/Snapshot (h/snapshot s)))
      (is (identical? s (h/register-observer! s :k (fn [_ _ _]))))
      (is (identical? s (h/unregister-observer! s :k)))
      (finally (is (identical? s (h/stop-sentinel! s))))))
  (testing "the instrumented boundary rejects a non-state"
    (is (thrown? Exception (h/next-state :bogus 0.5 wm)))))

(deftest budget-paths-conform
  (let [g (b/byte-budget-gate {:capacity 10 :timeout-ms 50 :name "ctr-gate"})]
    (is (= {:ok :v} (b/with-budget g 3 (fn [] :v))))
    (is (= {:ok 1} (b/with-byte-budget g 1 (constantly 1))))
    (is (= :budget/over-capacity (:error (b/with-budget g 99 (fn [] :x)))))
    (is (= :budget/execution-failed
           (:error (b/with-budget g 1 100 (fn [] (throw (ex-info "boom" {})))))))
    (is (m/validate bs/GateStats (b/byte-gate-stats g)))
    (is (= {:a 1 :b :fb}
           (b/byte-fork-join {:gate g :total-ms 2000}
                             [:a (constantly 1) 2]
                             [:b (constantly 2) 99 :fb])))
    (testing "the instrumented boundary rejects a malformed task"
      (is (thrown? Exception (b/->workload [:k]))))))

(deftest gate-impl-conforms-to-ibudget-contract
  (testing "hive-weave.gate.Gate satisfies the same -acquire/-stats contract"
    (let [g (gate/gate {:permits 2 :name "ctr-slot" :timeout-ms 50})]
      (is (r/ok? (b/-acquire g 1 50)))
      (is (m/validate bs/GateStats (b/-stats g)))
      (is (= :budget/over-capacity (:error (b/-acquire g 5 50))))
      (is (nil? (b/-release g 1))))))

(deftest broker-lifecycle-conforms
  (let [bk (br/start-broker! {:gate-opts     {:capacity 50 :name "ctr-broker"}
                              :sentinel-opts {:name "ctr-broker" :interval-ms 20}})]
    (try
      (Thread/sleep 50)
      (is (m/validate brs/SubmitResult (br/submit bk {:cost 5 :thunk (fn [] :done)})))
      (is (m/validate brs/BrokerStats (br/broker-stats bk)))
      (finally (is (identical? bk (br/stop-broker! bk))))))
  (testing "critical heap rejects fast and the rejection conforms"
    (let [bk (br/start-broker! {:gate-opts     {:capacity 50 :name "ctr-crit"}
                                :sentinel-opts {:name "ctr-crit" :interval-ms 20
                                                :start-state :critical
                                                :watermarks {:low-mark 1.5 :high-mark 1.6
                                                             :critical-mark 0.0}}})]
      (try
        (let [ret (br/submit bk {:cost 5 :thunk (fn [] :unreached)})]
          (is (= :broker/heap-critical (:error ret)))
          (is (m/validate brs/SubmitResult ret)))
        (finally (br/stop-broker! bk))))))

;; ============================================================================
;; 3. Local density gate
;; ============================================================================

(deftest every-public-fn-is-contracted
  (let [fs (m/function-schemas)]
    (doseq [n contracted-nss
            [s v] (ns-publics n)
            :when (and (fn? @v)
                       (not (:macro (meta v)))
                       (not (re-find #"^(->|map->)[A-Z]" (name s))))]
      (is (get-in fs [n s]) (str n "/" s " has no m/=> contract")))))
