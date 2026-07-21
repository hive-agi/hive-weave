(ns hive-weave.parallel-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-weave.parallel :as par]))

(deftest bounded-pmap-correctness-test
  (testing "returns results in input order"
    (is (= (mapv inc (range 100))
           (par/bounded-pmap {:concurrency 4} inc (range 100)))))
  (testing "empty coll"
    (is (= [] (par/bounded-pmap {} inc []))))
  (testing "exception yields fallback"
    (is (= [1 :fb 3]
           (par/bounded-pmap {:concurrency 2 :fallback :fb}
                             #(if (= % 2) (throw (ex-info "boom" {})) %)
                             [1 2 3]))))
  (testing "timeout yields fallback"
    (is (= [:fb]
           (par/bounded-pmap {:concurrency 1 :timeout-ms 50 :fallback :fb}
                             (fn [_] (Thread/sleep 5000) :done)
                             [:x])))))

(deftest bounded-pmap-thread-bound-test
  (testing "thread creation stays near :concurrency for large colls"
    (let [tmx (java.lang.management.ManagementFactory/getThreadMXBean)
          before (.getThreadCount tmx)
          _ (par/bounded-pmap {:concurrency 4 :timeout-ms 10000} inc (range 5000))
          after (.getThreadCount tmx)]
      (is (< (- after before) 20)))))

(deftest fan-out-test
  (testing "wraps bounded-pmap in ok Result"
    (let [res (par/fan-out {:concurrency 2} inc [1 2 3])]
      (is (= [2 3 4] (:ok res))))))
