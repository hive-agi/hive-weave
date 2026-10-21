(ns hive-weave.single-flight-test
  "Pins the single-flight contract: concurrent callers of one key share one
   computation, a failure reaches the computing caller as itself and every
   waiter as an ex-info carrying it, and a settled key runs again."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-weave.single-flight :as sf]))

(defn- await-count
  "Block until `counter` reaches `n`, bounded by `timeout-ms`, then give the
   callers that just bumped it a moment to reach the registry. A waiter parks
   on a promise, so its arrival is not otherwise observable."
  [counter n timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (when (and (< @counter n) (< (System/currentTimeMillis) deadline))
        (Thread/sleep 5)
        (recur))))
  (Thread/sleep 200))

(deftest concurrent-callers-share-one-computation
  (testing "callers that arrive while a key is in flight wait for its result"
    (let [reg     (sf/registry)
          runs    (atom 0)
          started (promise)
          release (promise)
          entered (atom 0)
          leader  (future (sf/run reg :k (fn []
                                           (swap! runs inc)
                                           (deliver started true)
                                           @release
                                           :v)))
          _       (deref started 2000 ::timeout)
          waiters (doall (repeatedly 4 #(future
                                          (swap! entered inc)
                                          (sf/run reg :k (fn [] (swap! runs inc) :other)))))]
      (is (contains? (sf/in-flight reg) :k) "the key is registered while computing")
      (await-count entered 4 2000)
      (deliver release true)
      (is (= :v (deref leader 2000 ::timeout)))
      (is (= [:v :v :v :v] (mapv #(deref % 2000 ::timeout) waiters)))
      (is (= 1 @runs) "the compute fn ran once for five callers")
      (is (empty? (sf/in-flight reg)) "the key is released once settled"))))

(deftest distinct-keys-do-not-share
  (testing "a second key computes on its own"
    (let [reg (sf/registry)]
      (is (= 1 (sf/run reg :a (constantly 1))))
      (is (= 2 (sf/run reg :b (constantly 2)))))))

(deftest settled-key-computes-again
  (testing "single-flight deduplicates in-flight work only; it does not cache"
    (let [reg  (sf/registry)
          runs (atom 0)]
      (is (= 1 (sf/run reg :k #(swap! runs inc))))
      (is (= 2 (sf/run reg :k #(swap! runs inc))))
      (is (= 2 @runs)))))

(deftest failure-reaches-leader-and-waiters
  (testing "the computing caller gets the cause; a waiter gets an ex-info wrapping it"
    (let [reg     (sf/registry)
          started (promise)
          release (promise)
          entered (atom 0)
          boom    (ex-info "boom" {:why :test})
          leader  (future
                    (try (sf/run reg :k (fn [] (deliver started true) @release (throw boom)))
                         (catch Throwable t t)))
          _       (deref started 2000 ::timeout)
          waiter  (future
                    (swap! entered inc)
                    (try (sf/run reg :k (constantly :never))
                         (catch Throwable t t)))]
      (await-count entered 1 2000)
      (deliver release true)
      (is (identical? boom (deref leader 2000 ::timeout)))
      (let [w (deref waiter 2000 ::timeout)]
        (is (= {:key :k} (ex-data w)))
        (is (identical? boom (ex-cause w))))
      (is (empty? (sf/in-flight reg)) "a failed key is released too"))))

(deftest default-registry-arity
  (testing "the two-arity form runs against the namespace's shared registry"
    (is (= :ok (sf/single-flight ::default-key (constantly :ok))))
    (is (not (contains? (sf/in-flight) ::default-key)))))
