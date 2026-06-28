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

   ClojureScript: core.async `go`/`go-loop` run on a single-threaded event
   loop, so there is no cross-thread boundary to convey dynamic-var binding
   frames across. The conveyance is therefore a meaningless no-op on cljs and
   the macros expand to a plain `clojure.core.async/go` / `go-loop`.

   Usage:
     (require '[hive-weave.async :as wa])

     (wa/bound-go
       (println *test-conn*))   ;; sees caller's bound *test-conn*

     (wa/bound-go-loop []
       (when-let [event (<! ch)]
         (handle event)
         (recur)))"
  (:require [clojure.core.async :as a]
            [hive-weave.pool :as pool])
  #?(:cljs (:require-macros [hive-weave.async])))

(defmacro bound-go
  "Drop-in replacement for `clojure.core.async/go` that conveys the
   caller's dynamic-var bindings via the active `IBindingConveyor`.

   ClojureScript: expands to a plain `clojure.core.async/go`; the
   conveyance is a no-op on the single-threaded event loop."
  [& body]
  (if (:ns &env)
    ;; cljs: single-threaded event loop — no cross-thread frame to convey.
    `(a/go ~@body)
    ;; clj: route the body through convey-fn so the IBindingConveyor
    ;; installs the caller's binding frame on the executing thread.
    `(let [f# (pool/convey-fn (fn [] ~@body))]
       (a/go (f#)))))

(defmacro bound-go-loop
  "Drop-in replacement for `clojure.core.async/go-loop` that conveys
   the caller's dynamic-var bindings via the active `IBindingConveyor`.

   ClojureScript: expands to a plain `clojure.core.async/go-loop`; the
   conveyance is a no-op on the single-threaded event loop."
  [bindings & body]
  (if (:ns &env)
    ;; cljs: single-threaded event loop — conveyance is a no-op.
    `(a/go-loop ~bindings ~@body)
    `(bound-go (loop ~bindings ~@body))))
