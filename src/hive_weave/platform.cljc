(ns hive-weave.platform
  "Platform abstraction seam for hive-weave.

   JVM subsystems keep their java.util.concurrent impls verbatim; this
   namespace supplies the cross-platform clock plus the ClojureScript async
   building blocks (a core.async counting semaphore and a timeout race) used
   by the :cljs branches of the subsystems."
  (:require [clojure.core.async :as a]))

;; =============================================================================
;; Clock
;; =============================================================================

(defn now-ms
  "Wall-clock milliseconds."
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (js/Date.now)))

(defn mono-ns
  "Monotonic nanoseconds for measuring durations."
  []
  #?(:clj  (System/nanoTime)
     :cljs (long (* 1e6 (js/performance.now)))))

;; =============================================================================
;; Exceptions
;; =============================================================================

(defn ex-class-name
  "Printable class/type name of a caught error."
  [e]
  #?(:clj  (str (class e))
     :cljs (pr-str (type e))))

;; =============================================================================
;; ClojureScript async primitives (core.async)
;; =============================================================================

#?(:cljs
   (defn make-semaphore
     "Async counting semaphore. 1 permit = 1 caller-chosen unit."
     [permits]
     (atom {:available permits :waiters []})))

#?(:cljs
   (defn- try-take!
     [sem cost]
     (let [[old _] (swap-vals! sem (fn [s]
                                     (if (>= (:available s) cost)
                                       (update s :available - cost)
                                       s)))]
       (>= (:available old) cost))))

#?(:cljs
   (defn sem-acquire!
     "Chan yielding :acquired when `cost` permits are reserved, or :timeout
      after timeout-ms."
     [sem cost timeout-ms]
     (let [ch (a/chan 1)]
       (if (try-take! sem cost)
         (a/put! ch :acquired)
         (let [done   (atom false)
               waiter {:cost cost :ch ch :done done}]
           (swap! sem update :waiters conj waiter)
           (a/go
             (a/<! (a/timeout timeout-ms))
             (when (compare-and-set! done false true)
               (swap! sem update :waiters
                      (fn [ws] (filterv #(not (identical? % waiter)) ws)))
               (a/put! ch :timeout)))))
       ch)))

#?(:cljs
   (defn sem-release!
     "Return `cost` permits and grant waiting acquirers in FIFO order."
     [sem cost]
     (swap! sem update :available + cost)
     (loop []
       (when (let [s (deref sem)
                   w (first (:waiters s))]
               (when (and w (>= (:available s) (:cost w)))
                 (if (compare-and-set! (:done w) false true)
                   (do (swap! sem (fn [st] (-> st
                                               (update :available - (:cost w))
                                               (update :waiters #(vec (rest %))))))
                       (a/put! (:ch w) :acquired)
                       true)
                   (do (swap! sem update :waiters #(vec (rest %)))
                       true))))
         (recur)))))

#?(:cljs
   (defn sem-stats
     [sem]
     (let [s (deref sem)]
       {:available (:available s) :queue-length (count (:waiters s))})))

;; Synchronous (non-blocking) primitives — the honest single-threaded path
;; used by the :cljs branches of budget/gate/parallel/pool. On single-threaded
;; node a synchronous thunk runs to completion between acquire and release, so
;; no real contention occurs: try-acquire either succeeds immediately or the
;; request exceeds capacity.

#?(:cljs
   (defn sem-try-acquire!
     "Non-blocking acquire of `cost` permits. Returns true when reserved
      (caller MUST sem-release! the same cost), false when unavailable."
     [sem cost]
     (let [[old _] (swap-vals! sem (fn [s]
                                     (if (>= (:available s) cost)
                                       (update s :available - cost)
                                       s)))]
       (>= (:available old) cost))))

#?(:cljs
   (defn sem-available
     "Currently-available permit count."
     [sem]
     (:available (deref sem))))

#?(:cljs
   (defn sem-queue-length
     "Number of async waiters parked on the semaphore (0 on sync paths)."
     [sem]
     (count (:waiters (deref sem)))))

#?(:cljs
   (defn await-chan
     "Race src against a timeout. Chan yields [:value v] when src produces v,
      or [:timeout] after timeout-ms."
     [src timeout-ms]
     (a/go
       (let [[v port] (a/alts! [src (a/timeout timeout-ms)])]
         (if (= port src) [:value v] [:timeout])))))
