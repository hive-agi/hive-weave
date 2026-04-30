(ns hive-weave.async
  "core.async go-block macros that convey dynamic-var bindings.

   `clojure.core.async/go` and `go-loop` do NOT preserve dynamic-var
   bindings across the go-fiber boundary — handlers run with root bindings
   instead of the caller's frame. `bound-go` and `bound-go-loop` route the
   body through `hive-weave.pool/convey-fn` so the active
   `IBindingConveyor` (default `BoundFnConveyor`) installs the frame on
   the executing thread.

   Caveat: bindings transfer at go-block entry. Park ops (`<!`, `>!`)
   may resume on a different thread; for cross-park preservation, re-wrap
   inside the loop body. For top-of-loop dispatch (the common pattern
   where bindings are read before any park), entry-time conveyance is
   sufficient.

   Usage:
     (require '[hive-weave.async :as wa])

     (wa/bound-go
       (println *test-conn*))   ;; sees caller's bound *test-conn*

     (wa/bound-go-loop []
       (when-let [event (<! ch)]
         (handle event)
         (recur)))"
  (:require [clojure.core.async :as a]
            [hive-weave.pool :as pool]))

(defmacro bound-go
  "Drop-in replacement for `clojure.core.async/go` that conveys the
   caller's dynamic-var bindings via the active `IBindingConveyor`."
  [& body]
  `(let [f# (pool/convey-fn (fn [] ~@body))]
     (a/go (f#))))

(defmacro bound-go-loop
  "Drop-in replacement for `clojure.core.async/go-loop` that conveys
   the caller's dynamic-var bindings via the active `IBindingConveyor`."
  [bindings & body]
  `(bound-go (loop ~bindings ~@body)))
