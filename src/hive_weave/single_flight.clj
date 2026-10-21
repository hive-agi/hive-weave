(ns hive-weave.single-flight
  "Single-flight: concurrent callers of one key share one computation.

   The first caller for a key registers a promise and runs the compute fn;
   every caller that arrives while it is in flight derefs that promise
   instead of computing. Once the computation settles the key is released,
   so the next caller computes again: this deduplicates in-flight work, it
   does not cache results.

   Copied from hive-mcp.tools.catchup.bundle-cache/single-flight (the same
   shape as hive-cache.core/with-single-flight, without event emission) so
   that both can delegate here.")

(defrecord ^:private Failed [cause])

(defn registry
  "A fresh in-flight registry: key -> promise of the pending result."
  []
  (atom {}))

(defonce ^:private default-registry (registry))

(defn in-flight
  "The keys currently being computed in `reg` (the shared registry when
   called without one)."
  ([] (in-flight default-registry))
  ([reg] (set (keys @reg))))

(defn run
  "Run `compute-fn` for `key` in `reg` unless a computation for `key` is
   already in flight, in which case wait for it and return its result.
   A failure is re-thrown to the computing caller as itself and surfaced to
   waiters as an ex-info {:key key} whose cause is the failure."
  [reg key compute-fn]
  (let [p       (promise)
        [old _] (swap-vals! reg (fn [m] (if (contains? m key) m (assoc m key p))))]
    (if-let [existing (get old key)]
      (let [v @existing]
        (if (instance? Failed v)
          (throw (ex-info "single-flight compute failed" {:key key} (:cause v)))
          v))
      (try
        (let [v (compute-fn)]
          (deliver p v)
          v)
        (catch Throwable t
          (deliver p (->Failed t))
          (throw t))
        (finally
          (swap! reg dissoc key))))))

(defn single-flight
  "`run` against the namespace's shared registry."
  [key compute-fn]
  (run default-registry key compute-fn))
