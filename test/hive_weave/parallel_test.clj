(ns hive-weave.parallel-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-weave.parallel :as par]
            [hive-weave.pool :as pool]))

(def ^:private java-21+? (pool/virtual-threads?))

(defn- executors
  "The values of :virtual? this JVM can actually run. Both branches are
   asserted wherever this appears, so the platform-thread path is reached by
   parameter rather than by being on an old JDK."
  []
  (if java-21+? [true false] [false]))

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
  (testing "a 5000-item fan-out is nowhere near 5000 threads, on either executor"
    ;; The bound is no longer the same object on both paths. A fixed pool grows
    ;; to :concurrency; the virtual executor grows its CARRIER pool toward the
    ;; core count, once, for the whole process. Both are constants that ignore
    ;; the size of the collection, which is the property worth asserting.
    (doseq [v (executors)]
      (let [tmx (java.lang.management.ManagementFactory/getThreadMXBean)
            before (.getThreadCount tmx)
            _ (par/bounded-pmap {:concurrency 4 :timeout-ms 20000 :virtual? v}
                                inc (range 5000))
            after (.getThreadCount tmx)
            grew (- after before)]
        (is (< grew (+ 8 (.availableProcessors (Runtime/getRuntime))))
            (str ":virtual? " v " grew the thread count by " grew))))))

;; -----------------------------------------------------------------------------
;; Virtual threads carry the work; the semaphore carries the bound.
;;
;; Both executors are asserted on every JVM. `:virtual?` is a PARAMETER, not a
;; property of the machine the suite happens to run on, so the platform-thread
;; branch is reachable here rather than only on a JDK nobody develops against.
;; -----------------------------------------------------------------------------


(defn- peak-in-flight
  "Highest number of items observed running at the same time."
  [opts n]
  (let [live (atom 0)
        peak (atom 0)]
    (par/bounded-pmap opts
                      (fn [_]
                        (let [now (swap! live inc)]
                          (swap! peak max now)
                          (Thread/sleep 15)
                          (swap! live dec)))
                      (range n))
    @peak))

(deftest bounded-pmap-holds-its-bound-on-either-executor
  (doseq [v (executors)]
    (let [peak (peak-in-flight {:concurrency 4 :timeout-ms 20000 :virtual? v} 40)]
      (is (<= peak 4) (str ":virtual? " v " let " peak " items run at once, not 4"))
      (is (>= peak 2) (str ":virtual? " v " ran them one at a time, which is not a bound")))))

(deftest bounded-pmap-results-do-not-depend-on-the-executor
  (testing "same values, same order, whichever thread ran them"
    (doseq [v (executors)]
      (is (= (mapv inc (range 200))
             (par/bounded-pmap {:concurrency 4 :timeout-ms 20000 :virtual? v}
                               inc (range 200)))
          (str ":virtual? " v)))))

(deftest bounded-pmap-still-falls-back-per-item
  (testing "a thrown item yields the fallback and its siblings survive"
    (doseq [v (executors)]
      (is (= [0 :boom 2]
             (par/bounded-pmap {:concurrency 2 :timeout-ms 20000 :virtual? v
                                :fallback :boom}
                               (fn [i] (if (= i 1) (throw (ex-info "no" {})) i))
                               (range 3)))
          (str ":virtual? " v)))))

(deftest stack-bytes-outranks-virtual
  (testing "a caller that needs a sized stack gets platform threads, not a silent drop"
    (let [names (par/bounded-pmap {:concurrency 2 :timeout-ms 20000
                                   :virtual? true :stack-bytes (* 8 1024 1024)}
                                  (fn [_] (.getName (Thread/currentThread)))
                                  (range 4))]
      (is (every? #(str/starts-with? (str %) "bounded-pmap-") names)
          (str "ran on " (vec names) " rather than the sized platform threads")))))

(deftest fan-out-test
  (testing "wraps bounded-pmap in ok Result"
    (let [res (par/fan-out {:concurrency 2} inc [1 2 3])]
      (is (= [2 3 4] (:ok res))))))
