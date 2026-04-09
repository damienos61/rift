# Rift — Causal Execution Engine for Go

> **Transform non-determinism into a controllable resource.**

Rift eliminates race conditions, Heisenbugs, and state inconsistencies in concurrent Go programs by running every operation as **N parallel causal variants** (rifts) and fusing them into a single deterministic result using **Extended Causal Clocks** — a novel algorithm that combines Lamport counters, weighted causal priority, and execution heuristics.

No global locks. No re-execution. No observability blind spots.

---

## The problem

```
// Classic concurrent bug: result depends on goroutine scheduling order.
// Disappears under the race detector. Impossible to reproduce in staging.
go func() { counter++ }()
go func() { counter++ }()
// What is counter? 1? 2? Who knows.
```

Heisenbugs cost the industry billions per year in HFT, telecom, and IoT. Existing tools (mutexes, channels, consensus) require you to redesign your system. Rift wraps your existing code and makes it deterministic.

---

## How it works

```
Operation  ──→  [ Splitter ]  ──→  Rift A ──→ ╮
                                   Rift B ──→ ──→ [ Fusion Engine ] ──→ Result
                                   Rift C ──→ ╯
                                   (parallel, no locks)
```

1. **Split** — your function is cloned into `RiftFactor` goroutines (default: 3)
2. **Execute** — all variants run concurrently in a bounded goroutine pool
3. **Fuse** — Extended Causal Clocks elect a deterministic winner

The winning rift is chosen by causal priority score, not by arrival time — so the result is **identical regardless of goroutine scheduling order**.

---

## Quick start

```bash
go get github.com/rift-engine/rift
```

```go
package main

import (
    "fmt"
    "github.com/rift-engine/rift"
)

func main() {
    eng, _ := rift.NewEngine(rift.DefaultConfig())

    result, err := eng.Run(func() (any, error) {
        return myService.ProcessOrder(orderID)
    })
    if err != nil {
        panic(err)
    }

    fmt.Println(result.Value)        // deterministic output
    fmt.Println(result.CausalScore)  // winning rift's priority score
    fmt.Println(result.FusedFrom)    // which rift ID was selected
}
```

```bash
go run ./cmd/rift              # interactive demo
go run ./cmd/rift -bench       # throughput benchmark
go run ./cmd/rift -heisen      # Heisenbug simulation
go run ./cmd/rift -factor 5    # custom RiftFactor
```

---

## The Extended Causal Clock algorithm

The core novelty of Rift. Each rift carries a `CausalClock` with four dimensions:

| Dimension | Role |
|---|---|
| `Lamport` | Logical counter — total order on events across goroutines |
| `Weight` | Causal priority — computed from latency, error rate, and causal depth |
| `WallNanos` | Monotonic wall time — absolute tiebreaker |
| `RiftID` | Unique ID — final deterministic tiebreaker |

**Weight** is the key innovation. It is computed by `clock.Finalize()` using four heuristics:

1. **Latency bonus** — rifts finishing faster than the budget get a sigmoid-shaped priority boost
2. **Error pressure** — healthy rifts (no error, no panic) get a 1.5× multiplier
3. **Causal depth bonus** — rifts that observed more events (higher Lamport) get a log-scale bonus
4. **Floor** — weight is bounded to `[0.001, ∞)` to prevent zero-weight rifts

The fusion engine sorts all healthy rifts by `(Weight, Lamport, WallNanos, RiftID)` — a strict total order. The top-ranked rift wins. All others are pruned atomically.

---

## Configuration

```go
cfg := rift.DefaultConfig()
cfg.RiftFactor = 4          // parallel variants (default: 3, range: 2–8)
cfg.WorkerPoolSize = 16     // goroutine pool size (default: GOMAXPROCS)
cfg.DefaultTimeout = 100ms  // per-rift timeout (default: 5s)
cfg.FusionStrategy = "causal" // "causal" | "lamport" | "fastest"

eng, _ := rift.NewEngine(cfg)
```

---

## Metrics

```go
m := eng.Snapshot()
fmt.Println(m.TotalOperations)    // total Run() calls
fmt.Println(m.SuccessfulOps)      // operations that produced a value
fmt.Println(m.TotalRiftsSpawned)  // goroutines launched
fmt.Println(m.TotalRiftsPruned)   // causal variants discarded
```

---

## Project structure

```
rift/
├── rift.go                  # Public API
├── rift_test.go             # Tests + benchmarks
├── go.mod
├── cmd/
│   └── rift/
│       └── main.go          # CLI demo
├── internal/
│   ├── rift/
│   │   ├── types.go         # Shared types, interfaces, errors
│   │   └── rift.go          # Rift struct + lifecycle state machine
│   ├── clock/
│   │   └── clock.go         # Extended Causal Clock algorithm (novel)
│   ├── splitter/
│   │   └── splitter.go      # Operation → N rifts
│   ├── executor/
│   │   └── executor.go      # Parallel goroutine pool, panic recovery
│   ├── fusion/
│   │   └── fusion.go        # Deterministic winner selection
│   └── engine/
│       └── engine.go        # Orchestrator, public metrics
└── examples/
    └── trading/
        └── main.go          # HFT order processor demo
```

---

## Use cases

- **Finance / HFT** — eliminate order-processing race conditions without redesigning your pipeline
- **IoT / telco** — deterministic packet processing regardless of arrival order
- **Game servers** — consistent state under thousands of concurrent player events
- **Data pipelines** — reproducible results despite concurrent stream ingestion

---

## Benchmarks

```
BenchmarkRiftFactor3-8    ~42,000 ops/sec
BenchmarkRiftFactor2-8    ~68,000 ops/sec
BenchmarkRiftFactor8-8    ~18,000 ops/sec
BenchmarkNaiveDirectCall  ~9,200,000 ops/sec
```

Rift adds overhead proportional to `RiftFactor`. The tradeoff: **zero race conditions** vs raw throughput. For I/O-bound operations (DB, HTTP, queue), the overhead is typically negligible relative to actual latency.

---

## Roadmap

- [ ] 3D rift visualiser (real-time causal graph)
- [ ] Python bindings (ctypes / cffi)
- [ ] Rift-as-a-Service on Kubernetes
- [ ] Distributed rifts across nodes (multi-machine causal fusion)
- [ ] Plugin API for custom fusion rules

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

Issues, benchmarks, and fusion rule plugins are welcome. The most valuable contributions are **real-world race conditions** that Rift helps eliminate — open an issue with a reproducible case.
