(ns hive-weave.async
  "core.async go blocks whose failure is a VALUE.

   `safe-go` / `safe-go-loop` return a channel yielding a hive-dsl Result:

     {:ok v}
     {:error :weave/exception :name label :message .. :class ..}
     {:error :weave/timeout   :name label :timeout-ms ms}

   Park ops (`<!`, `>!`, `alts!`) work normally inside the body. There is no
   `safe-go-call` counterpart to `hive-weave.safe/safe-future-call`: the body
   must stay lexically inside `a/go`.

   `bound-go` / `bound-go-loop` are DEPRECATED since 0.3.0."
  (:require [clojure.core.async :as a]
            [hive-dsl.result :as r]
            [hive-weave.async.schema :as schema]
            [hive-weave.pool :as pool]
            [malli.core :as m]))

;; SPDX-License-Identifier: LicenseRef-Proprietary
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>

;; =============================================================================
;; Pure — opts + error construction
;; =============================================================================

(def default-label
  "Label carried by a go block the caller did not name."
  "anonymous")

(defn normalize-opts
  "Apply defaults to caller-supplied GoOpts. A nil :timeout-ms is preserved:
   an untimed block is a choice, not a missing value."
  [opts]
  {:name       (let [n (:name opts)]
                 (if (and (string? n) (pos? (count n))) n default-label))
   :timeout-ms (:timeout-ms opts)})

(defn timeout-error
  "Result for a go block that outlived its :timeout-ms."
  [label timeout-ms]
  (r/err :weave/timeout {:name label :timeout-ms timeout-ms}))

(defn exception-error
  "Result for a go block whose body threw. Takes the Throwable already reduced
   to data (see `ex-payload`)."
  [label {:keys [message class]}]
  (r/err :weave/exception {:name label :message message :class class}))

(defn ex-payload
  "Reduce a Throwable to the host-neutral shape `exception-error` takes."
  [^Throwable t]
  {:message (ex-message t)
   :class   (str (class t))})

(m/=> normalize-opts [:=> [:cat [:maybe schema/GoOpts]] schema/NormalizedOpts])
(m/=> timeout-error [:=> [:cat schema/Label pos-int?] schema/GoError])
(m/=> exception-error [:=> [:cat schema/Label schema/ExPayload] schema/GoError])

;; =============================================================================
;; Macros — the body must stay LEXICALLY inside a/go
;; =============================================================================

(defmacro safe-go
  "Run body in a go block; return a channel yielding a Result.

   opts — {:timeout-ms pos-int (optional), :name string (optional)}
   A throwing body yields (err :weave/exception ..). With :timeout-ms, a body
   that outruns its budget yields (err :weave/timeout ..) and the inner block
   is abandoned, not cancelled."
  [opts & body]
  `(let [{label# :name ms# :timeout-ms} (normalize-opts ~opts)
         inner# (a/go (try
                        (r/ok (do ~@body))
                        (catch Throwable t#
                          (exception-error label# (ex-payload t#)))))]
     (if ms#
       (a/go (let [[v# port#] (a/alts! [inner# (a/timeout ms#)])]
               (if (and (identical? port# inner#) (some? v#))
                 v#
                 (timeout-error label# ms#))))
       inner#)))

(defmacro safe-go-loop
  "`safe-go` over a `loop`. Same Result contract; `recur` targets the loop."
  [opts bindings & body]
  `(safe-go ~opts (loop ~bindings ~@body)))

;; =============================================================================
;; Deprecated — binding conveyance, which core.async >= 1.6 does itself
;; =============================================================================

(defmacro bound-go
  "DEPRECATED since 0.3.0 — prefer `clojure.core.async/go`, or `safe-go` for
   failures as values. Retained as the `IBindingConveyor` swap point.

   Installs the caller's binding frame by value inside the go block and
   restores the executing thread's prior frame on exit."
  {:deprecated "0.3.0"}
  [& body]
  `(let [frame# (do (pool/convey-fn (fn [])) (pool/capture-frame))]
     (a/go
       (let [prior# (pool/capture-frame)]
         (try
           (clojure.lang.Var/resetThreadBindingFrame frame#)
           ~@body
           (finally
             (clojure.lang.Var/resetThreadBindingFrame prior#)))))))

(defmacro bound-go-loop
  "DEPRECATED — see `bound-go`. Prefer `clojure.core.async/go-loop`, or
   `safe-go-loop` when you want failures as values."
  {:deprecated "0.3.0"}
  [bindings & body]
  #_{:clj-kondo/ignore [:deprecated-var]}
  `(bound-go (loop ~bindings ~@body)))
