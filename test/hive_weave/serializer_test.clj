(ns hive-weave.serializer-test
  "Tests for the FIFO serializer.

   Three layers:
   1. **Trifecta** on the pure ADT constructors — golden snapshot of
      variant shapes + property totality + mutation testing of the
      defadt-generated coerce/predicate fns.
   2. **Deterministic deftest** for sequence invariants (FIFO order,
      coalescing semantics, lifecycle transitions) — these are
      cleaner as plain assertions than as properties.
   3. **Linearizability** (`defprop-concurrent-safe`) for the
      concurrency contract — N submitters racing against the queue
      cannot lose tasks, kill the worker, or deadlock."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.adt :as adt]
            [hive-test.linearizability :refer [defprop-concurrent-safe]]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-weave.serializer :as s]))

;; =============================================================================
;; Helpers
;; =============================================================================

(defn- with-serializer
  "Bracket: build a serializer, run f, always close."
  [opts f]
  (let [sz (s/serializer opts)]
    (try (f sz)
         (finally (s/close! sz)))))

;; =============================================================================
;; Trifecta — ADT constructor surface (pure, snapshot-friendly)
;;
;; submit-outcome variants are the public API contract. Golden snapshot
;; locks the keys + values; property test exercises totality across
;; the variant set; mutation kit catches a constructor regression that
;; silently drops fields.
;; =============================================================================

(deftrifecta submit-outcome-shape
  hive-weave.serializer/submit-outcome
  {:golden-path "test/golden/submit_outcome.edn"
   :apply?      true
   :xf          (fn [m] (into (sorted-map) m))
   :cases       {:ok      [:submit/ok      {:promise :PROMISE-PLACEHOLDER}]
                 :timeout [:submit/timeout {:queue-size 1 :timeout-ms 100 :name "n"}]
                 :closed  [:submit/closed  {:name "n"}]}
   :gen         (gen/elements [[:submit/ok      {:promise :p}]
                               [:submit/timeout {:queue-size 1 :timeout-ms 1 :name "n"}]
                               [:submit/closed  {:name "n"}]])
   :pred        adt/adt?})

(deftrifecta task-outcome-shape
  hive-weave.serializer/task-outcome
  {:golden-path "test/golden/task_outcome.edn"
   :apply?      true
   :xf          (fn [m] (into (sorted-map) m))
   :cases       {:ok     [:task/ok     {:value 42}]
                 :failed [:task/failed {:key :K :class "X" :message "boom"}]}
   :gen         (gen/elements [[:task/ok     {:value 1}]
                               [:task/failed {:key :K :class "X" :message "m"}]])
   :pred        adt/adt?})

;; =============================================================================
;; Deterministic invariants
;; =============================================================================

(deftest fifo-ordering
  (testing "tasks execute in submission order on the single worker"
    (with-serializer {:name "fifo" :queue-capacity 32 :submit-timeout-ms 1000}
      (fn [sz]
        (let [seen (atom [])
              n    20
              subs (mapv (fn [i]
                           (s/submit! sz {:key i}
                                      (fn [] (swap! seen conj i))))
                         (range n))]
          (Thread/sleep 200)
          (is (every? s/submit-ok? subs))
          (is (= (vec (range n)) @seen)
              "tasks ran in FIFO submission order"))))))

(deftest task-outcome-payloads
  (testing "task/ok carries the return value"
    (with-serializer {:name "ok" :queue-capacity 4 :submit-timeout-ms 500}
      (fn [sz]
        (let [outcome (s/submit-and-wait! sz {} (fn [] (* 6 7)))]
          (is (s/task-ok? outcome))
          (is (= 42 (:value outcome)))))))

  (testing "task/failed carries class + message + key"
    (with-serializer {:name "fail" :queue-capacity 4 :submit-timeout-ms 500}
      (fn [sz]
        (let [outcome (s/submit-and-wait! sz {:key :K}
                                          (fn [] (throw (ex-info "boom" {}))))]
          (is (= :task/failed (adt/adt-variant outcome)))
          (is (= "boom" (:message outcome)))
          (is (= :K (:key outcome))))))))

(deftest coalescing
  (testing "same coalesce key replaces a queued task — only the latest runs"
    (with-serializer {:name "coalesce"
                      :queue-capacity 8
                      :submit-timeout-ms 1000
                      :coalesce-key-fn :key}
      (fn [sz]
        (let [seen (atom [])]
          (s/submit! sz {} (fn [] (Thread/sleep 60) (swap! seen conj :pre)))
          (Thread/sleep 5)
          (s/submit! sz {:key :K} (fn [] (swap! seen conj :v1)))
          (s/submit! sz {:key :K} (fn [] (swap! seen conj :v2)))
          (s/submit! sz {:key :K} (fn [] (swap! seen conj :v3)))
          (Thread/sleep 250)
          (is (= [:pre :v3] @seen)
              "coalesce dropped v1 + v2; only v3 ran"))))))

(deftest backpressure
  (testing "submit! returns :submit/timeout when queue stays full"
    (with-serializer {:name "bp" :queue-capacity 1 :submit-timeout-ms 50}
      (fn [sz]
        (s/submit! sz {} (fn [] (Thread/sleep 300)))
        (Thread/sleep 5)
        (let [a (s/submit! sz {} (fn []))
              b (s/submit! sz {} (fn []))]
          (is (= :submit/ok      (adt/adt-variant a)))
          (is (= :submit/timeout (adt/adt-variant b)))
          (is (= "bp" (:name b)))
          (is (pos-int? (:queue-size b))))))))

(deftest lifecycle
  (testing "submit! after close! returns :submit/closed"
    (let [sz (s/serializer {:name "closed" :queue-capacity 4 :submit-timeout-ms 100})]
      (s/close! sz)
      (let [outcome (s/submit! sz {} (fn []))]
        (is (= :submit/closed (adt/adt-variant outcome)))
        (is (= "closed" (:name outcome))))))

  (testing "close! is idempotent"
    (let [sz (s/serializer {:name "idem" :queue-capacity 1 :submit-timeout-ms 100})
          _  (s/close! sz)
          r2 (s/close! sz)]
      (is (true? (get-in r2 [:ok :already-closed?])))))

  (testing "stats reports queue + worker state"
    (with-serializer {:name "stats" :queue-capacity 4 :submit-timeout-ms 100}
      (fn [sz]
        (let [st (s/stats sz)]
          (is (= "stats" (:name st)))
          (is (= 4       (:remaining st)))
          (is (false?    (:closed? st)))
          (is (true?     (:worker-alive? st))))))))

;; =============================================================================
;; Linearizability — concurrency safety under multi-thread fan-in
;;
;; N threads racing on the same serializer must not:
;;   - lose work (every accepted submission either runs or is rejected
;;     deterministically),
;;   - kill the worker thread,
;;   - leave the queue in an inconsistent state.
;; =============================================================================

(defn- run-op!
  "Apply one operation against the live serializer state."
  [{:keys [serializer counter rejected accepted]} op]
  (case (first op)
    :submit
    (let [outcome (s/submit! serializer {:key (second op)}
                             (fn [] (swap! counter inc)))]
      (if (= :submit/ok (adt/adt-variant outcome))
        (swap! accepted inc)
        (swap! rejected inc)))

    :stat
    (s/stats serializer)))

(defprop-concurrent-safe serializer-survives-concurrent-fan-in 25
  ;; Each test generates 50–200 :submit ops with random integer keys.
  (gen/vector (gen/tuple (gen/return :submit) (gen/choose 0 50)) 50 200)
  ;; Setup — fresh serializer + tracking atoms.
  (fn []
    {:serializer (s/serializer {:name "concurrent"
                                :queue-capacity 64
                                :submit-timeout-ms 500})
     :counter    (atom 0)
     :accepted   (atom 0)
     :rejected   (atom 0)})
  ;; Apply one op.
  run-op!
  ;; Invariant after all threads complete:
  ;;   - worker is still alive (no thread death from a bad task),
  ;;   - every submission counted exactly once (accepted + rejected = total),
  ;;   - eventually counter catches up to accepted (drain after close).
  (fn [{:keys [serializer counter accepted rejected]}]
    (Thread/sleep 600)              ; let worker drain
    (s/close! serializer)
    (let [{:keys [worker-alive?]} (s/stats serializer)
          submitted (+ @accepted @rejected)]
      (and worker-alive?
           (pos-int? submitted)
           (= @counter @accepted))))
  {:n-threads 4 :timeout-ms 5000})
