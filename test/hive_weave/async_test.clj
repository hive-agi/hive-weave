(ns hive-weave.async-test
  "Tests for hive-weave.async — bound-go / bound-go-loop conveyance."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as a]
            [hive-weave.async :as wa]
            [hive-weave.pool :as pool]))

(def ^:dynamic *probe* :root)

(use-fixtures :each
  (fn [f]
    (let [prior (pool/get-conveyor)]
      (try (f)
           (finally (pool/set-conveyor! prior))))))

(defn- <!! [ch] (a/<!! ch))

;; =============================================================================
;; bound-go
;; =============================================================================

(deftest bound-go-conveys-binding
  (testing "bound-go propagates bound dynvar to go-block.
            (note: core.async/go itself captures bindings via
            getThreadBindingFrame at entry — bound-go is belt-and-suspenders
            and exposes the conveyor swap point for inspection.)"
    (binding [*probe* :test]
      (let [ch (wa/bound-go *probe*)]
        (is (= :test (<!! ch)))))))

(deftest bound-go-capture
  (testing "CapturingConveyor records the frame from caller of bound-go"
    (let [snap (atom nil)]
      (pool/set-conveyor! (pool/->CapturingConveyor snap))
      (binding [*probe* :test]
        (<!! (wa/bound-go *probe*)))
      (is (= :test (get @snap #'*probe*))))))

;; =============================================================================
;; bound-go-loop
;; =============================================================================

(deftest bound-go-loop-conveys-binding
  (testing "bound-go-loop propagates bound dynvar across iterations
            (top-of-loop dispatch — bindings read before any park)"
    (binding [*probe* :test]
      (let [in   (a/chan 4)
            out  (a/chan 4)
            done (wa/bound-go-loop [collected []]
                   (if-let [_ (a/<! in)]
                     (recur (conj collected *probe*))
                     (a/>! out collected)))]
        (a/>!! in :a) (a/>!! in :b) (a/close! in)
        (<!! done)
        (is (= [:test :test] (<!! out)))))))
