(ns hive-weave.stack
  "Threads sized for the work, not for the default.

   Every other thread primitive in this library inherits the JVM's default
   thread stack, which is fine for Clojure work and quietly fatal for native
   work that recurses. A native stack overflow is not a StackOverflowError: it
   is a SIGSEGV that kills the process outright, with no Result, no exception
   and no hs_err file for anyone to read afterwards.

   Measured case (vtranslate, 2026-09-14, linux-x86_64 / JDK 26): ONNX Runtime
   1.29 and later recurse deeply while building and optimizing a session graph,
   on the CALLER's stack. Creating a session on a default-stack thread killed
   the engine; the same call under -Xss4m succeeded. Older runtimes merely
   happened to fit, so the crash looked like a version regression it was not.

   A launch flag fixes that only for whoever remembers to pass it, and a library
   whose whole job is handing work to threads is the place that should not need
   remembering: use `call-with-stack` for a one-shot call, or pass `:stack-bytes`
   to `pool/make-pool` / `parallel/bounded-pmap` when the work those threads run
   is native."
  (:import [java.util.concurrent ThreadFactory]))

(def default-stack-bytes
  "16 MiB. -Xss4m was enough for the ONNX session that motivated this; the
   margin costs nothing, since a stack is reserved address space and only the
   pages actually touched are committed."
  (* 16 1024 1024))

(defn thread-factory
  "A ThreadFactory whose threads carry an explicit stack size.

   Options:
     :name        thread-name prefix, `<prefix>-<n>` (default \"weave-stack\")
     :stack-bytes requested stack size (default `default-stack-bytes`)
     :daemon?     daemon threads, so they never block JVM shutdown (default true)

   The JVM treats stack size as a HINT: a platform may round it or ignore it
   entirely. Every platform this library targets honours it."
  ^ThreadFactory [{:keys [name stack-bytes daemon?]
                   :or   {name "weave-stack" stack-bytes default-stack-bytes daemon? true}}]
  (let [counter (atom 0)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. nil runnable (str name "-" (swap! counter inc)) (long stack-bytes))
          (.setDaemon (boolean daemon?)))))))

(defn call-with-stack
  "Invoke `f` on a fresh thread with an explicit stack and return its value.

   Arities: `(call-with-stack f)` uses `default-stack-bytes`,
   `(call-with-stack stack-bytes f)` names the size, and
   `(call-with-stack {:stack-bytes n :name \"...\"} f)` also names the thread, so
   a thread dump taken during the call says which seam the native frames belong
   to.

   A throwable from `f` is rethrown on the CALLING thread, so this is invisible
   to the error handling around it: try/catch and Result guards keep working
   exactly as they did. Blocking, so the caller waits for the thread to finish,
   and an interrupt of the caller surfaces there as InterruptedException.

   One thread per call is the point. This is for a rare, expensive native call
   (loading a model, building a session), not for a hot path; for repeated work
   build a pool with `thread-factory` instead."
  ([f] (call-with-stack default-stack-bytes f))
  ([stack-bytes-or-opts f]
   (let [{:keys [stack-bytes name]
          :or   {stack-bytes default-stack-bytes name "weave-stack-call"}}
         (if (map? stack-bytes-or-opts)
           stack-bytes-or-opts
           {:stack-bytes stack-bytes-or-opts})
         result (volatile! nil)
         thrown (volatile! nil)
         t      (Thread. nil
                         ^Runnable (fn []
                                     (try
                                       (vreset! result (f))
                                       (catch Throwable e (vreset! thrown e))))
                         ^String name
                         (long stack-bytes))]
     (.start t)
     (.join t)
     (if-let [e @thrown]
       (throw e)
       @result))))

(defmacro with-stack
  "Evaluate `body` on a thread with `default-stack-bytes` of stack.
   => the body's value; a throwable is rethrown on the caller."
  [& body]
  `(call-with-stack (fn [] ~@body)))

(defmacro with-stack-of
  "`with-stack` with an explicit stack size in bytes."
  [stack-bytes & body]
  `(call-with-stack ~stack-bytes (fn [] ~@body)))
