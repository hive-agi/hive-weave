(ns hive-weave.async-test
  "Trifecta for hive-weave.async. Property + mutation facets over the pure
   constructors are synthesized from the malli schemas by hive-schemas.test;
   hand-written below only what a schema cannot state."
  {:clj-kondo/config '{:linters {:deprecated-var {:level :off}}}}
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as a]
            [hive-schemas.test :as hst]
            [hive-dsl.result :as r]
            [hive-weave.async :as wa]
            [hive-weave.async.schema :as schema]
            [hive-weave.pool :as pool]))

(def ^:dynamic *probe* :root)

(use-fixtures :each
  (fn [f]
    (let [prior (pool/get-conveyor)]
      (try (f)
           (finally (pool/set-conveyor! prior))))))

(defn- take!! [ch] (first (a/alts!! [ch (a/timeout 3000)])))

;; ============================================================================
;; Schema-synthesized
;; ============================================================================

(hst/deftrifecta-from-schema normalize-opts-conforms
  hive-weave.async/normalize-opts
  {:in        [:maybe schema/GoOpts]
   :out       schema/NormalizedOpts
   :rel       (fn [in out]
                (and (= (:timeout-ms out) (:timeout-ms in))
                     (if (and (string? (:name in)) (pos? (count (:name in))))
                       (= (:name out) (:name in))
                       (= (:name out) wa/default-label))))
   :num-tests 100})

(hst/deftrifecta-from-schema timeout-error-conforms
  hive-weave.async/timeout-error
  {:in        [:cat schema/Label pos-int?]
   :out       schema/GoError
   :rel       (fn [[label ms] out]
                (and (= :weave/timeout (:error out))
                     (= label (:name out))
                     (= ms (:timeout-ms out))))
   :num-tests 100})

(hst/deftrifecta-from-schema exception-error-conforms
  hive-weave.async/exception-error
  {:in        [:cat schema/Label schema/ExPayload]
   :out       schema/GoError
   :rel       (fn [[label payload] out]
                (and (= :weave/exception (:error out))
                     (= label (:name out))
                     (= (:message payload) (:message out))))
   :num-tests 100})

;; ============================================================================
;; safe-go — failure as a value
;; ============================================================================

(deftest a-throwing-body-yields-an-error-instead-of-vanishing
  (testing "plain go loses the exception; safe-go returns it"
    (is (nil? (take!! (a/go (throw (ex-info "gone" {}))))))
    (let [res (take!! (wa/safe-go {:name "kept"} (throw (ex-info "kaboom" {:k 1}))))]
      (is (false? (r/ok? res)))
      (is (= :weave/exception (:error res)))
      (is (= "kept" (:name res)))
      (is (= "kaboom" (:message res)))
      (is (re-find #"ExceptionInfo" (:class res))))))

(deftest a-successful-body-yields-ok
  (testing "the value is wrapped, not passed through bare"
    (is (= {:ok 42} (take!! (wa/safe-go {} (inc 41)))))
    (is (true? (r/ok? (take!! (wa/safe-go {} :v)))))))

(deftest an-unnamed-block-still-carries-a-label
  (testing "errors are triageable even when the caller named nothing"
    (is (= wa/default-label
           (:name (take!! (wa/safe-go {} (throw (ex-info "x" {})))))))))

;; ============================================================================
;; safe-go — park ops must survive the macro
;; ============================================================================

(deftest the-body-can-park
  (testing "park ops work inside the body"
    (let [ch (a/chan 1)]
      (a/>!! ch 7)
      (is (= {:ok 14} (take!! (wa/safe-go {} (* 2 (a/<! ch)))))))))

(deftest a-throw-after-a-park-is-still-caught
  (testing "a throw after a park is caught"
    (let [ch (a/chan 1)]
      (a/>!! ch :v)
      (let [res (take!! (wa/safe-go {:name "late"} (a/<! ch) (throw (ex-info "after" {}))))]
        (is (= :weave/exception (:error res)))
        (is (= "after" (:message res)))))))

(deftest safe-go-loop-parks-across-iterations
  (testing "parking survives every iteration, not just the first"
    (let [ch (a/chan 4)]
      (a/>!! ch 1) (a/>!! ch 2) (a/close! ch)
      (is (= {:ok 3}
             (take!! (wa/safe-go-loop {:name "sum"} [acc 0]
                       (if-let [v (a/<! ch)] (recur (+ acc v)) acc))))))))

(deftest safe-go-loop-reports-a-throw-from-a-later-iteration
  (testing "the Result contract holds through recur"
    (let [res (take!! (wa/safe-go-loop {:name "l"} [n 0]
                        (if (< n 2) (recur (inc n)) (throw (ex-info "iter" {})))))]
      (is (= :weave/exception (:error res)))
      (is (= "iter" (:message res))))))

;; ============================================================================
;; safe-go — timeout
;; ============================================================================

(deftest a-block-that-outruns-its-budget-yields-a-timeout-error
  (let [res (take!! (wa/safe-go {:timeout-ms 100 :name "slow"}
                      (a/<! (a/timeout 5000))
                      :never))]
    (is (= :weave/timeout (:error res)))
    (is (= "slow" (:name res)))
    (is (= 100 (:timeout-ms res)))))

(deftest a-block-inside-its-budget-is-untouched
  (is (= {:ok :made-it}
         (take!! (wa/safe-go {:timeout-ms 3000 :name "fast"}
                   (a/<! (a/timeout 20))
                   :made-it)))))

(deftest omitting-a-timeout-leaves-the-block-untimed
  (testing "an absent :timeout-ms stays nil"
    (is (nil? (:timeout-ms (wa/normalize-opts {}))))
    (is (= {:ok :done} (take!! (wa/safe-go {} :done))))))

;; ============================================================================
;; bound-go — deprecated, but must not rot
;; ============================================================================

(deftest bound-go-is-marked-deprecated
  (is (some? (:deprecated (meta #'wa/bound-go))))
  (is (some? (:deprecated (meta #'wa/bound-go-loop)))))

(deftest bound-go-body-can-park
  (let [ch (a/chan 1)]
    (a/>!! ch :x)
    (is (= "got::x" (take!! (wa/bound-go (str "got:" (a/<! ch))))))))

(deftest bound-go-loop-body-can-park-repeatedly
  (let [in (a/chan 4) out (a/chan 4)]
    (wa/bound-go-loop []
      (when-let [v (a/<! in)]
        (a/>! out (inc v))
        (recur)))
    (a/>!! in 1) (a/>!! in 2)
    (is (= [2 3] [(take!! out) (take!! out)]))))

(deftest bound-go-conveys-binding
  (binding [*probe* :test]
    (is (= :test (take!! (wa/bound-go *probe*))))))

(deftest bound-go-capture
  (testing "the conveyor is consulted"
    (let [snap (atom nil)]
      (pool/set-conveyor! (pool/->CapturingConveyor snap))
      (binding [*probe* :test]
        (take!! (wa/bound-go *probe*)))
      (is (= :test (get @snap #'*probe*))))))

(deftest bound-go-restores-the-executing-threads-frame
  (testing "a pooled go-thread is not left holding the caller's bindings"
    (binding [*probe* :test]
      (take!! (wa/bound-go :done)))
    (is (= :root (take!! (a/go *probe*))))))
