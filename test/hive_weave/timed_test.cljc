(ns hive-weave.timed-test
  "timed: wrap-timed / timed-handler return f's value when f finishes in budget."
  ;; test.check and hive-test.trifecta ship no ClojureScript source, so the
  ;; property harness is JVM-only; the plain deftests below run on both hosts.
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.test.check.generators :as gen])
            #?(:clj [hive-test.trifecta :refer [deftrifecta]])
            [hive-weave.timed :as timed]))

(defn wrapped-inc
  "wrap-timed over inc with a generous budget, applied to n."
  [n]
  ((timed/wrap-timed inc {:timeout-ms 5000 :name "inc"}) n))

#?(:clj
   (deftrifecta wrap-timed-trifecta
     #'hive-weave.timed-test/wrapped-inc
     {:golden-path "test/golden/timed_wrap_inc.edn"
      :cases       {:zero 0 :one 1 :negative -7 :large 1000000}
      :gen         gen/small-integer
      :pred        integer?
      :num-tests   50
      :mutations   [["passthrough" identity]
                    ["dec-instead" dec]]}))

(deftest timed-handler-returns-result-in-budget
  (let [h (timed/timed-handler + {:timeout-ms 5000 :fallback ::fallback})]
    (is (= 6 (h 1 2 3)))))

(deftest timed-interceptor-shape
  (let [ic (timed/->timed-interceptor :id :t/x :timeout-ms 5000 :after inc)]
    (testing "keeps id, marks timed, wraps :after, leaves missing :before identity"
      (is (= :t/x (:id ic)))
      (is (true? (:timed? ic)))
      (is (= 2 ((:after ic) 1)))
      (is (= identity (:before ic))))))

#?(:clj
   (deftest wrap-timed-passes-input-through-on-timeout
     (let [f (timed/wrap-timed (fn [_] (Thread/sleep 2000) :late)
                               {:timeout-ms 50 :name "slow"})]
       (is (= :in (f :in))))))
