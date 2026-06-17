(ns hive-weave.heap
  "JVM heap pressure sentinel. Samples Runtime memory periodically, derives a
   3-state pressure level (:normal :high :critical) with hysteresis, and
   publishes both to atoms that observers can read or subscribe to.

   Does NOT enforce admission policy — that's the broker's job. The sentinel
   is the signal; consumers decide what to do with it."
  (:require [taoensso.timbre :as log])
  (:import [java.util.concurrent Executors ScheduledExecutorService
                                 ThreadFactory TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; --- Sampling ---

(defrecord HeapSample [used max total free fraction ts])

(defn sample!
  "Read current JVM heap state. used = total - free; fraction = used / max."
  []
  (let [rt    (Runtime/getRuntime)
        total (.totalMemory rt)
        free  (.freeMemory rt)
        max-h (.maxMemory rt)
        used  (- total free)]
    (->HeapSample used max-h total free
                  (double (/ used max-h))
                  (System/currentTimeMillis))))

;; --- Pure state machine ---

(def default-watermarks
  {:low-mark      0.70
   :high-mark     0.85
   :critical-mark 0.95})

(defn next-state
  "Pure: prev-state × fraction × watermarks → new-state.

   Hysteresis bands:
     :normal   --(>= high-mark)-->   :high
     :normal   --(>= critical-mark)--> :critical
     :high     --(>= critical-mark)--> :critical
     :high     --(<  low-mark)-->    :normal
     :critical --(<  high-mark)-->   :high
     :critical --(<  low-mark)-->    :normal"
  [prev fraction {:keys [low-mark high-mark critical-mark]}]
  (case prev
    :normal   (cond
                (>= fraction critical-mark) :critical
                (>= fraction high-mark)     :high
                :else                       :normal)
    :high     (cond
                (>= fraction critical-mark) :critical
                (< fraction low-mark)       :normal
                :else                       :high)
    :critical (cond
                (< fraction low-mark)       :normal
                (< fraction high-mark)      :high
                :else                       :critical)))

;; --- Sentinel ---

(defrecord Sentinel
  [name interval-ms watermarks
   sample-atom state-atom
   ^ScheduledExecutorService executor
   observers     ; atom<{kw → fn}> ; fn :: prev-state new-state sample → any
   tick-count])  ; AtomicLong

(defn- daemon-factory [name]
  (reify ThreadFactory
    (newThread [_ r]
      (doto (Thread. r (str "heap-sentinel-" name))
        (.setDaemon true)))))

(defn- on-tick! [^Sentinel s]
  (let [sample  (sample!)
        prev    @(:state-atom s)
        new     (next-state prev (:fraction sample) (:watermarks s))]
    (reset! (:sample-atom s) sample)
    (when (not= prev new)
      (reset! (:state-atom s) new)
      (log/debug "heap-sentinel" (:name s) "transition" prev "->" new
                 "fraction" (:fraction sample))
      (doseq [[_ obs] @(:observers s)]
        (try (obs prev new sample)
             (catch Exception e
               (log/warn "heap-sentinel observer threw:" (.getMessage e))))))
    (.incrementAndGet ^AtomicLong (:tick-count s))))

(defn snapshot
  "Read current sample + state without waiting for next tick."
  [^Sentinel s]
  {:sample @(:sample-atom s)
   :state  @(:state-atom s)
   :ticks  (.get ^AtomicLong (:tick-count s))
   :name   (:name s)})

(defn register-observer!
  "Register an observer fn keyed by k. fn :: prev-state new-state sample → _.
   Returns the sentinel for chaining."
  [^Sentinel s k f]
  (swap! (:observers s) assoc k f)
  s)

(defn unregister-observer! [^Sentinel s k]
  (swap! (:observers s) dissoc k)
  s)

(defn start-sentinel!
  "Start a daemon-thread sentinel that samples heap every :interval-ms.

   Options:
     :name          — diagnostic name (default \"default\")
     :interval-ms   — sample interval (default 1000)
     :watermarks    — {:low-mark :high-mark :critical-mark} (default 0.70/0.85/0.95)
     :start-state   — initial state (default :normal)"
  ([] (start-sentinel! {}))
  ([{:keys [name interval-ms watermarks start-state]
     :or   {name        "default"
            interval-ms 1000
            watermarks  default-watermarks
            start-state :normal}}]
   (let [executor (Executors/newSingleThreadScheduledExecutor
                   (daemon-factory name))
         sentinel (->Sentinel name interval-ms watermarks
                              (atom (sample!))
                              (atom start-state)
                              executor
                              (atom {})
                              (AtomicLong. 0))]
     (.scheduleAtFixedRate executor
                           ^Runnable #(on-tick! sentinel)
                           (long interval-ms)
                           (long interval-ms)
                           TimeUnit/MILLISECONDS)
     sentinel)))

(defn stop-sentinel! [^Sentinel s]
  (.shutdownNow ^ScheduledExecutorService (:executor s))
  s)
