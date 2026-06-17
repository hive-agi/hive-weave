(ns hive-weave.heap-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-weave.heap :as h]))

(def wm {:low-mark 0.70 :high-mark 0.85 :critical-mark 0.95})

(deftest next-state-from-normal
  (is (= :normal   (h/next-state :normal 0.50 wm)))
  (is (= :normal   (h/next-state :normal 0.84999 wm)))
  (is (= :high     (h/next-state :normal 0.85 wm)))
  (is (= :high     (h/next-state :normal 0.90 wm)))
  (is (= :critical (h/next-state :normal 0.95 wm)))
  (is (= :critical (h/next-state :normal 0.99 wm))))

(deftest next-state-from-high-hysteresis
  (testing "stays :high between low-mark and critical-mark"
    (is (= :high   (h/next-state :high 0.85 wm)))
    (is (= :high   (h/next-state :high 0.80 wm)))
    (is (= :high   (h/next-state :high 0.70 wm)))
    (is (= :normal (h/next-state :high 0.69 wm)))
    (is (= :critical (h/next-state :high 0.95 wm)))))

(deftest next-state-from-critical-hysteresis
  (testing "de-escalates to :high below high-mark, to :normal below low-mark"
    (is (= :critical (h/next-state :critical 0.99 wm)))
    (is (= :critical (h/next-state :critical 0.95 wm)))
    (is (= :high     (h/next-state :critical 0.84 wm)))
    (is (= :high     (h/next-state :critical 0.70 wm)))
    (is (= :normal   (h/next-state :critical 0.69 wm)))))

(deftest sample-shape
  (let [s (h/sample!)]
    (is (instance? hive_weave.heap.HeapSample s))
    (is (pos? (:max s)))
    (is (>= (:used s) 0))
    (is (<= (:used s) (:max s)))
    (is (>= (:fraction s) 0.0))
    (is (<= (:fraction s) 1.0))))

(deftest sentinel-start-tick-stop
  (let [s (h/start-sentinel! {:name "test" :interval-ms 50})]
    (try
      (Thread/sleep 200)
      (let [snap (h/snapshot s)]
        (is (pos? (:ticks snap)) "sentinel ticked at least once")
        (is (some? (:sample snap)))
        (is (#{:normal :high :critical} (:state snap))))
      (finally
        (h/stop-sentinel! s)))))

(deftest sentinel-observer-receives-transitions
  (let [observed (atom [])
        ;; force-transition watermarks so any sample triggers
        forced   {:low-mark 0.0 :high-mark 0.0 :critical-mark 1.01}
        s        (h/start-sentinel! {:name "obs" :interval-ms 30
                                     :watermarks forced
                                     :start-state :normal})]
    (try
      (h/register-observer! s :rec (fn [prev new sample]
                                     (swap! observed conj [prev new (:fraction sample)])))
      (Thread/sleep 150)
      (is (some (fn [[p n _]] (and (= p :normal) (= n :high))) @observed)
          (str "expected at least one :normal→:high transition, got " @observed))
      (finally
        (h/stop-sentinel! s)))))
