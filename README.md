# hive-weave

<!-- hive-badges -->

[![Clojars Project](https://img.shields.io/clojars/v/io.github.hive-agi/hive-weave.svg)](https://clojars.org/io.github.hive-agi/hive-weave)
[![cljdoc](https://cljdoc.org/badge/io.github.hive-agi/hive-weave)](https://cljdoc.org/d/io.github.hive-agi/hive-weave/CURRENT)
[![release](https://github.com/hive-agi/hive-weave/actions/workflows/release.yml/badge.svg)](https://github.com/hive-agi/hive-weave/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

<!-- /hive-badges -->

**Bounded, timed, safe execution primitives for Clojure.** Every operation has a
timeout, every parallel fan-out is bounded, and failure arrives as a value
rather than as a stack trace on some other thread.

`hive-weave` is the antidote to bare `@(future …)`. A bare deref is an
unbounded wait: if the work never completes, the caller never returns, and the
symptom shows up somewhere else entirely. Every primitive here terminates.

## Coordinates

```clojure
;; deps.edn
io.github.hive-agi/hive-weave {:mvn/version "0.3.3"}
```

Results are [hive-dsl](https://github.com/hive-agi/hive-dsl) `Result` values —
`(r/ok v)` / `(r/err kind msg)` — so a timeout composes like any other failure.

## Usage

```clojure
(require '[hive-weave.core :as weave]
         '[hive-dsl.result :as r])

;; A deref that cannot hang
(weave/deref-safe some-future 5000 ::timed-out)

;; A concurrency gate: at most 4 in flight, 10s to acquire a permit
(def db-gate (weave/gate {:permits 4 :timeout-ms 10000 :name "db"}))
(weave/gate-run db-gate #(query! conn sql))   ;; => (r/ok …) | (r/err :gate/timeout …)

;; Bounded parallelism instead of pmap
(require '[hive-weave.parallel :as par])
(par/bounded-pmap {:concurrency 8 :timeout-ms 2000} f xs)
```

## What is in the box

| Namespace | Provides |
|---|---|
| `hive-weave.safe` | `deref-safe`, `safe-future-call` — bounded alternatives to `@` and raw `future` |
| `hive-weave.gate` | Semaphore-backed concurrency gate with timeout + diagnostics |
| `hive-weave.budget` | Unit-agnostic budget gate (1 permit = 1 unit; bytes, MiB, slots) |
| `hive-weave.parallel` | `bounded-pmap`, `fork-join`, `fan-out` — fan-out with a collective budget |
| `hive-weave.pool` | Thread-pool factory + safe submit/await, so callers never touch `java.util.concurrent` |
| `hive-weave.async` | `safe-go` / `safe-go-loop` — go blocks whose failure is a value |
| `hive-weave.retry` | `with-recovery` — bounded retry with a recovery hook between attempts |
| `hive-weave.heap` | JVM heap pressure sentinel with hysteresis (`:normal` / `:high` / `:critical`) |
| `hive-weave.broker` | `IResourceBroker` — budget gate plus heap sentinel, admission by policy table |
| `hive-weave.serializer` | FIFO single-writer queue for resources that reject concurrent writers |
| `hive-weave.timed` | Timeout wrappers for interceptors and handlers |
| `hive-weave.guarded` | Bounded futures/pool tasks with cleanup hooks |
| `hive-weave.gpu` | VRAM budget gate — `budget` with `:unit :mib` and the legacy `:gpu/*` error keys |

## Why retry does not retry a timeout

`with-recovery` surfaces timeouts immediately and retries only other failures.
Retrying doubles latency when the operation is alive but slow; when the resource
is dead it is the recovery hook — reopen the connection, recreate the client —
that restores liveness, not another attempt at the same call.

## License

MIT.
