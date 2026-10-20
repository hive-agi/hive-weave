(ns hive-weave.parallel-strict-test
  "bounded-pmap-strict: a missing item is an error, never a substituted value."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-weave.parallel :as par]))

(def ^:private s ::missing)

(defn missing-of
  "One-argument view of par/missing-indices with a fixed sentinel."
  [results]
  (par/missing-indices s results))

(deftrifecta missing-indices-trifecta
  #'hive-weave.parallel-strict-test/missing-of
  {:golden-path "test/golden/missing_indices.edn"
   :cases       {:none   [1 2 3]
                 :empty  []
                 :middle [1 s 3]
                 :all    [s s]
                 :nil-is-a-value [nil 0 s]}
   :gen         (gen/vector (gen/one-of [gen/small-integer (gen/return s) (gen/return nil)]))
   :pred        #(and (vector? %) (every? nat-int? %) (= % (sort %)))
   :num-tests   100
   :mutations   [["never-missing" (fn [_] [])]
                 ["nil-counts-as-missing"
                  (fn [rs] (into [] (keep-indexed (fn [i v] (when (or (nil? v) (= s v)) i))) rs))]]})

(deftest strict-returns-full-vector-when-all-answer
  (is (= (mapv inc (range 20))
         (par/bounded-pmap-strict {:concurrency 4} inc (range 20))))
  (is (= [] (par/bounded-pmap-strict {} inc [])))
  (testing "a legitimate nil result is NOT treated as missing"
    (is (= [nil 1] (par/bounded-pmap-strict {:concurrency 2} #(when (pos? %) %) [0 1])))))

(deftest strict-throws-on-failed-item
  (let [e (try (par/bounded-pmap-strict {:concurrency 2}
                                        #(if (= % 2) (throw (ex-info "boom" {})) %)
                                        [1 2 3])
               nil
               (catch clojure.lang.ExceptionInfo e e))]
    (is (some? e))
    (is (= :hive-weave/pmap-incomplete (:type (ex-data e))))
    (is (= [1] (:missing (ex-data e))))
    (is (= 3 (:count (ex-data e))))))

(deftest strict-throws-on-timeout
  (let [e (try (par/bounded-pmap-strict {:concurrency 1 :timeout-ms 50}
                                        (fn [_] (Thread/sleep 5000) :done)
                                        [:x])
               nil
               (catch clojure.lang.ExceptionInfo e e))]
    (is (= [0] (:missing (ex-data e))))))
