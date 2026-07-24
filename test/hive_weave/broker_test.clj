(ns hive-weave.broker-test
  (:require [clojure.test :refer [deftest is]]
            [hive-dsl.result :as r]
            [hive-weave.broker :as br]))

;; --- Pure policy fn ---

(deftest decide-policy-normal
  (is (= [:pass 5000] (br/decide-policy :normal 5000 {}))))

(deftest decide-policy-high-extends
  (let [[action ms] (br/decide-policy :high 5000 {})]
    (is (= :extend action))
    (is (= 10000 ms))))

(deftest decide-policy-high-custom-multiplier
  (let [[action ms] (br/decide-policy :high 1000 {:high-timeout-multiplier 3.5})]
    (is (= :extend action))
    (is (= 3500 ms))))

(deftest decide-policy-critical-rejects
  (let [[action arg] (br/decide-policy :critical 5000 {})]
    (is (= :reject action))
    (is (= 1000 (:retry-after-ms arg)))
    (is (= :critical (:state arg)))))

(deftest decide-policy-critical-custom-retry
  (let [[_ arg] (br/decide-policy :critical 5000 {:critical-retry-after-ms 500})]
    (is (= 500 (:retry-after-ms arg)))))

;; --- Submit: pass-through under normal ---

(deftest submit-passes-under-normal
  (let [b (br/start-broker!
           {:gate-opts     {:capacity 100 :unit :mib :name "br-normal"}
            :sentinel-opts {:name "br-normal" :interval-ms 50
                            :start-state :normal
                            :watermarks {:low-mark 0.0 :high-mark 1.5 :critical-mark 1.6}}})]
    (try
      ;; Watermarks are impossible to cross — state stays :normal regardless of sample
      (Thread/sleep 80)
      (let [ret (br/submit b {:cost 10 :thunk (fn [] :ok-result)})]
        (is (r/ok? ret))
        (is (= :ok-result (:ok ret))))
      (finally (br/stop-broker! b)))))

;; --- Submit: reject under forced :critical ---

(deftest submit-rejects-under-critical
  (let [b (br/start-broker!
           {:gate-opts     {:capacity 100 :unit :mib :name "br-crit"}
            :sentinel-opts {:name "br-crit" :interval-ms 30
                            ;; force any sample into :critical
                            :start-state :critical
                            :watermarks  {:low-mark 1.5 :high-mark 1.6 :critical-mark 0.0}}
            :policy-opts   {:critical-retry-after-ms 250}})]
    (try
      (Thread/sleep 80)
      (let [ret (br/submit b {:cost 10 :thunk (fn [] :unreached)})]
        (is (r/err? ret))
        (is (= :broker/heap-critical (:error ret)))
        (is (= 250 (:retry-after-ms ret))))
      (finally (br/stop-broker! b)))))

;; --- broker-stats merges gate + heap snapshots ---

(deftest broker-stats-shape
  (let [b (br/start-broker!
           {:gate-opts     {:capacity 50 :unit :mib :name "br-stats"}
            :sentinel-opts {:name "br-stats" :interval-ms 50}})]
    (try
      (Thread/sleep 80)
      (let [s (br/broker-stats b)]
        (is (= 50      (-> s :gate :capacity)))
        (is (= :mib    (-> s :gate :unit)))
        (is (some?     (-> s :heap :sample)))
        (is (#{:normal :high :critical} (-> s :heap :state))))
      (finally (br/stop-broker! b)))))
