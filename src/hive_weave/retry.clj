(ns hive-weave.retry
  "Bounded retry with pluggable recovery.

   `with-recovery` runs a thunk under a `safe-future-call` timeout.
   On non-timeout failure, calls `recover!` once and retries the thunk.
   Timeouts surface immediately because retry only doubles latency
   when the operation is alive but slow; on a dead resource it is the
   recovery hook (reopen connection, recreate client, refresh cache)
   that restores liveness, not another attempt at the same call.

   Generalizes the `read-with-retry` / `write-with-retry` pattern from
   `hive-mcp.knowledge-graph.store.datahike` so other stores
   (Milvus/Qdrant/Chroma/NATS) can opt into the same auto-heal
   contract without rewriting the classify-and-reopen loop.

   Design rules (anchored in repo memory):

   - **Fail loud** (decision 20260428174346-1a85b1ae): terminal failure
     throws — never silently substitute a default. Callers can opt out
     via `:on-failure` returning a Result, but the *default* is throw.
   - **No silent-drop** (principle 20260413204752-2d78b124): the
     failure shape is a hive-dsl Result, not a sentinel value, so
     callers that opt out can still distinguish 'no data' from
     'call failed'.
   - **Single-retry by design**: needs more attempts? Compose with
     `hive-weave.parallel` or add an explicit `:max-attempts` knob
     when a real caller demands it. Don't make this fn a swiss-army
     knife."
  (:require [hive-dsl.result :as r]
            [hive-weave.safe :as safe]
            [taoensso.timbre :as log]))

(defn- non-timeout-error?
  "Default `:retry-on` predicate — every Result error EXCEPT
   `:weave/timeout`. Timeouts are honored as 'alive but slow', not
   transient failures: retry only doubles wall-clock cost when the
   operation will time out again on the second attempt."
  [result]
  (and (r/err? result) (not= :weave/timeout (:error result))))

(defn- default-on-failure
  "Default `:on-failure` — throw ex-info with the operation label and
   terminal Result. Aligns with hive-mcp's 'fail loud' KG-backend
   policy: dead resources surface, never silently fall back."
  [label result]
  (throw (ex-info (str "with-recovery failed: " label)
                  {:operation label
                   :result    result})))

(defn with-recovery
  "Run thunk `f` under `:timeout-ms`. On non-timeout failure, call
   `recover!` once then retry. Returns the success value or invokes
   `:on-failure` with the terminal Result.

   Options:
     :timeout-ms — per-attempt budget in ms (required, pos-int)
     :name       — diagnostic label (default 'retry'); appears in
                   logs and propagates into the retry attempt's name
                   suffix `<name>/retry`.
     :recover!   — 0-arg fn called between attempts on retryable
                   failure. Should be idempotent and safe across
                   threads (e.g. compare-and-set on a connection
                   atom). Optional — omit when the caller has no
                   recovery hook to run.
     :retry-on   — (Result -> bool); predicate deciding whether the
                   first-attempt Result is retryable. Default:
                   `non-timeout-error?` (any error except
                   `:weave/timeout`).
     :on-failure — (label, Result) -> any; invoked on terminal
                   failure (non-retryable first error, or retry
                   itself errored). Default throws ex-info; pass a
                   custom fn returning a Result/fallback to opt out
                   of throwing.

   Examples:

     ;; Datahike auto-heal — reopen on writer death, retry once
     (with-recovery
       {:timeout-ms 120000
        :name       \"transact\"
        :recover!   #(reset-conn! store)
        :on-failure (fn [l r] (throw-write-failed! l r))}
       #(deref (d/transact! conn tx-data)))

     ;; HTTP client — rebuild on selector death
     (with-recovery
       {:timeout-ms 30000
        :name       \"chroma-search\"
        :recover!   #(swap! client-atom (constantly (mk-client)))}
       #(.send @client-atom req))

     ;; Opt out of throwing — return Result instead
     (with-recovery
       {:timeout-ms 5000
        :name       \"probe\"
        :on-failure (fn [_ r] r)}
       (fn [] :ok))"
  [{:keys [timeout-ms name recover! retry-on on-failure]
    :or   {name       "retry"
           retry-on   non-timeout-error?
           on-failure default-on-failure}}
   f]
  {:pre [(pos-int? timeout-ms)]}
  (let [first-r (safe/safe-future-call
                  {:timeout-ms timeout-ms :name name}
                  f)]
    (cond
      (r/ok? first-r)
      (:ok first-r)

      (not (retry-on first-r))
      (on-failure name first-r)

      :else
      (do
        (log/warn "with-recovery: first attempt failed, recovering and retrying once"
                  {:operation name :error (:error first-r)})
        (when recover! (recover!))
        (let [retry-r (safe/safe-future-call
                        {:timeout-ms timeout-ms :name (str name "/retry")}
                        f)]
          (if (r/ok? retry-r)
            (:ok retry-r)
            (on-failure name retry-r)))))))
