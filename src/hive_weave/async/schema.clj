(ns hive-weave.async.schema
  "Malli value objects for hive-weave.async. Drives the m/=> contracts on the
   pure constructors and the synthesized tests.")

;; SPDX-License-Identifier: LicenseRef-Proprietary
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>

(def Label
  "Diagnostic label for a go block, carried into every error."
  [:and :string [:fn {:error/message "non-blank"} (fn [s] (pos? (count s)))]])

(def GoOpts
  "Caller-supplied options for safe-go / safe-go-loop. Open: unknown keys are
   tolerated and every key is optional."
  [:map
   [:timeout-ms {:optional true} pos-int?]
   [:name {:optional true} :string]])

(def NormalizedOpts
  "GoOpts after defaulting. :timeout-ms is nillable — nil means untimed."
  [:map {:closed true}
   [:name Label]
   [:timeout-ms [:maybe pos-int?]]])

(def ExPayload
  "A Throwable reduced to data."
  [:map {:closed true}
   [:message [:maybe :string]]
   [:class :string]])

(def ErrorKind
  "Which way a safe-go block failed."
  [:enum :weave/timeout :weave/exception])

(def GoError
  "A failed safe-go Result: category under :error, data merged flat."
  [:multi {:dispatch :error}
   [:weave/timeout
    [:map {:closed true}
     [:error [:= :weave/timeout]]
     [:name Label]
     [:timeout-ms pos-int?]]]
   [:weave/exception
    [:map {:closed true}
     [:error [:= :weave/exception]]
     [:name Label]
     [:message [:maybe :string]]
     [:class :string]]]])

(def Result
  "hive-dsl Result as produced by safe-go: {:ok v} on success, or a
   flat GoError map on failure."
  [:or
   [:map {:closed true} [:ok :any]]
   GoError])