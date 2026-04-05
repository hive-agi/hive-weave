(ns hive-weave.timed
  "Timed wrappers for interceptors and handlers.

   Wraps any function with a timeout budget so it can't hang the caller.
   Primary use: event system interceptors that may call external services.

   - `wrap-timed`            — wrap any (fn [x] -> x) with timeout
   - `->timed-interceptor`   — interceptor with timeout on :before/:after
   - `timed-handler`         — handler with timeout + fallback"
  (:require [taoensso.timbre :as log]))

;; =============================================================================
;; Core: wrap any function with a timeout
;; =============================================================================

(defn wrap-timed
  "Wrap f with a timeout. Returns a new function that:
   - Runs f in a future
   - Returns f's result if it completes within timeout-ms
   - Returns passthrough (the original input) on timeout
   - Logs a warning on timeout

   Options:
     :timeout-ms — max execution time (required)
     :name       — diagnostic label (optional)

   (def safe-enrich (wrap-timed enrich-fn {:timeout-ms 15000 :name \"enrichment\"}))"
  [f {:keys [timeout-ms name] :or {name "anonymous"}}]
  {:pre [(pos-int? timeout-ms)]}
  (fn [input]
    (let [fut (future (f input))
          result (deref fut timeout-ms ::timed-out)]
      (if (= result ::timed-out)
        (do (log/warn "wrap-timed" name "exceeded" timeout-ms "ms — passing through")
            (future-cancel fut)
            input)
        result))))

;; =============================================================================
;; Timed Interceptor
;; =============================================================================

(defn ->timed-interceptor
  "Create an interceptor with a timeout budget on :before and/or :after.

   If either phase exceeds timeout-ms, it is cancelled and the context
   passes through unchanged (graceful degradation).

   Usage:
     (->timed-interceptor
       :id :hk/smart-search-enrichment
       :timeout-ms 15000
       :after (fn [ctx] ...expensive enrichment...))

   On timeout: logs warning, context passes through, chain continues."
  [& {:keys [id before after timeout-ms] :as m}]
  {:pre [(pos-int? timeout-ms)]}
  (let [wrap (fn [f direction]
               (if (or (nil? f) (= f identity))
                 identity
                 (wrap-timed f {:timeout-ms timeout-ms
                                :name (str (or id :unnamed) " " direction)})))]
    (merge {:id (or id :unnamed-timed)
            :before (wrap before :before)
            :after (wrap after :after)
            :timed? true
            :timeout-ms timeout-ms}
           (dissoc m :id :before :after :timeout-ms))))

;; =============================================================================
;; Timed Handler
;; =============================================================================

(defn timed-handler
  "Wrap a handler function with a timeout + fallback.
   Unlike wrap-timed (which passes through input), this returns a
   specific fallback value on timeout.

   (def safe-search
     (timed-handler search-fn {:timeout-ms 10000 :fallback {:error \"timeout\"}}))
   (safe-search params)  ;; => result or {:error \"timeout\"}"
  [f {:keys [timeout-ms fallback name] :or {name "handler"}}]
  {:pre [(pos-int? timeout-ms)]}
  (fn [& args]
    (let [fut (future (apply f args))
          result (deref fut timeout-ms ::timed-out)]
      (if (= result ::timed-out)
        (do (log/warn "timed-handler" name "exceeded" timeout-ms "ms — returning fallback")
            (future-cancel fut)
            fallback)
        result))))
