(ns ^:typed.clojure hive-weave.typed-anns
  "Annotations-as-data for the :typed rung of hive-weave.async.
   Checked by the :typed alias; the checker itself is never a runtime dep."
  {:clj-kondo/config '{:linters {:unresolved-symbol {:level :off}
                                 :unresolved-var    {:level :off}}}}
  (:require [typed.clojure :as t]
            [hive-weave.async]))

;; SPDX-License-Identifier: LicenseRef-Proprietary
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>

(t/defalias Label t/Str)

(t/defalias GoOpts
  (t/HMap :optional {:timeout-ms t/Int
                     :name       t/Str}))

(t/defalias NormalizedOpts
  (t/HMap :mandatory {:name       Label
                      :timeout-ms (t/Nilable t/Int)}
          :complete? true))

(t/defalias ExPayload
  (t/HMap :mandatory {:message (t/Nilable t/Str)
                      :class   t/Str}
          :complete? true))

(t/defalias GoError (t/Map t/Kw t/Any))

(t/ann hive-weave.async/default-label Label)
(t/ann hive-weave.async/normalize-opts [(t/Nilable GoOpts) :-> NormalizedOpts])
(t/ann hive-weave.async/timeout-error [Label t/Int :-> GoError])
(t/ann hive-weave.async/exception-error [Label ExPayload :-> GoError])
(t/ann hive-weave.async/ex-payload [Throwable :-> ExPayload])
