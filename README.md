# Rift v0.9 — Causal Execution Engine for Go

> **Transform non-determinism into a controllable resource.**

Rift eliminates race conditions, Heisenbugs, and state inconsistencies in concurrent Go programs by running every operation as **N parallel causal variants** (rifts) and fusing them into a single deterministic result using **Extended Causal Clocks v2 (ECC-2)** — a novel algorithm combining Lamport counters, weighted causal priority, execution entropy, and causal depth.

No global locks. No re-execution. No observability blind spots.

---

## What's new in v0.9

| Feature | Description |
|---|---|
| **ECC-2** | New `Entropy` + `Depth` dimensions in `CausalClock` for richer causal discrimination |
| **Circuit Breaker** | Rolling-window fault gate — open/half-open/closed states, configurable threshold & cooldown |
| **Adaptive RiftFactor** | Self-tuning concurrency: scales up on high error rate, scales down when healthy |
| **Retry Policy** | Exponential backoff per rift for transient failures, before fusion |
| **Entropy Fusion** | New `"entropy"` strategy: selects the variant that explored the most diverse code path |
| **TelemetryHook** | Zero-cost pluggable interface for span/trace export — nil = no overhead |
| **Richer Metrics** | Mean latency, mean entropy delta, live circuit state, error rate, active RiftFactor |
| **RunWithTimeout** | Per-call timeout override on the public API |
| **EntropyDelta** | `Result.EntropyDelta`: how much causal path diversity the winning rift contributed |
| **Retries in Result** | `Result.Retries`: total retry count across all variants, for observability |

---

## The problem

```go
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
Operation ──→ [ Splitter ] ──→ Rift A ──→ ╮
                               Rift B ──→ ──→ [ ECC-2 Fusion Engine ] ──→ Deterministic Result
                               Rift C ──→ ╯
                               (parallel, no locks, retry on failure)
```

1. **Split** — your function is cloned into `RiftFactor` goroutines (default: 3)
2. **Execute** — all variants run concurrently in a bounded adaptive pool; transient failures are retried with exponential backoff
3. **Fuse** — ECC-2 elects a deterministic winner by causal priority score

The winning rift is chosen by a strict total order — identical every time, regardless of goroutine scheduling.

---

## Quick start

```bash
go get github.com/damienos61/rift
```

```go
package main

import (
    "fmt"
    "github.com/damienos61/rift"
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
    fmt.Println(result.CausalScore)  // ECC-2 priority score of the winner
    fmt.Println(result.EntropyDelta) // causal path diversity consumed (v0.9)
    fmt.Println(result.Retries)      // total retries across all variants (v0.9)
}
```

---

## CLI demo

```bash
go run ./cmd/rift                       # interactive demo
go run ./cmd/rift -bench                # throughput benchmark
go run ./cmd/rift -heisen               # Heisenbug simulation
go run ./cmd/rift -factor 5             # custom RiftFactor
go run ./cmd/rift -strategy entropy     # entropy-weighted fusion
go run ./cmd/rift -circuit              # circuit breaker demo
go run ./cmd/rift -adaptive             # adaptive RiftFactor demo
```

---

## ECC-2: Extended Causal Clock v2

The core novelty of Rift. Each rift carries a `CausalClock` with **five dimensions** (v0.9 adds Entropy and Depth):

| Dimension | Role |
|---|---|
| `Lamport` | Logical counter — total order on events across goroutines |
| `Weight` | Causal priority — computed from latency, health, depth, entropy |
| `WallNanos` | Monotonic wall time — absolute tiebreaker |
| `RiftID` | Unique ID — final deterministic tiebreaker |
| `Entropy` ⭐ new | Path diversity score ∈ [0,1] — richer causal view = higher score |
| `Depth` ⭐ new | Observed causal chain length — longer chain = more events seen |

### ECC-2 Weight formula

```
Weight = baseWeight
       × sigmoidBonus(budget/actual)   // H1: latency (smoother sigmoid k=2.2)
       × healthMultiplier               // H2: 1.55× healthy, 0.08× failed
       × (1 + log1p(Lamport) × 0.05)  // H3: Lamport depth
       × (1 + 0.15 × Entropy)          // H4: entropy (NEW v0.9)
       × (1 + log1p(Depth) × 0.02)    // H5: causal depth (NEW v0.9)
```

### Total order for fusion

```
(Weight → Entropy → Lamport → WallNanos → RiftID)
```

This is a **strict total order** — no two distinct clocks can be equal. The fusion engine always produces the same winner, regardless of goroutine scheduling.

### Entropy computation

Entropy is computed via a Murmur3-inspired hash mixer on a per-rift path hash accumulated during execution. The provider maintains a cross-rift entropy pool: each rift's hash is XOR'd into the pool, so subsequent rifts start from a richer entropic seed — amplifying causal diversity across the entire engine lifetime.

---

## Configuration

```go
cfg := rift.DefaultConfig()

// Core
cfg.RiftFactor     = 4          // parallel variants (default: 3, range: 2–8)
cfg.WorkerPoolSize = 16         // goroutine pool (default: 2×GOMAXPROCS in v0.9)
cfg.DefaultTimeout = 100*time.Millisecond
cfg.FusionStrategy = "causal"   // "causal" | "entropy" | "lamport" | "fastest"

// Circuit Breaker (v0.9)
cfg.CircuitBreaker.Enabled   = true
cfg.CircuitBreaker.Threshold = 0.5           // open at 50% error rate
cfg.CircuitBreaker.Window    = 100           // over last 100 operations
cfg.CircuitBreaker.CoolDown  = 10*time.Second

// Retry Policy (v0.9)
cfg.Retry.MaxAttempts = 2
cfg.Retry.Backoff     = 1*time.Millisecond   // initial; doubles each attempt
cfg.Retry.MaxBackoff  = 50*time.Millisecond  // cap

// Adaptive RiftFactor (v0.9)
cfg.Adaptive.Enabled       = true
cfg.Adaptive.MinFactor     = 2
cfg.Adaptive.MaxFactor     = 8
cfg.Adaptive.ErrorRateUp   = 0.3   // +1 factor when error rate > 30%
cfg.Adaptive.ErrorRateDown = 0.05  // -1 factor when error rate < 5%

// Telemetry (v0.9) — nil = zero overhead
cfg.Telemetry = myTracer // implements rift.TelemetryHook

eng, _ := rift.NewEngine(cfg)
```

---

## Fusion strategies

| Strategy | Description | Use case |
|---|---|---|
| `"causal"` | ECC-2 full order (default) | Production — best determinism |
| `"entropy"` | Entropy-primary ordering | Chaos testing, path diversity |
| `"lamport"` | Pure Lamport counter | Classic comparison baseline |
| `"fastest"` | Lowest wall time wins | Benchmarks only (non-deterministic) |

---

## Telemetry hook

```go
type myTracer struct{}

func (t *myTracer) OnSplit(opID rift.OperationID, factor int)                         { /* emit span */ }
func (t *myTracer) OnConverge(riftID rift.RiftID, d time.Duration, healthy bool)      { /* emit span */ }
func (t *myTracer) OnFuse(opID rift.OperationID, winner rift.RiftID, score float64, pruned int) { /* emit span */ }
func (t *myTracer) OnError(opID rift.OperationID, err error)                          { /* emit span */ }

cfg.Telemetry = &myTracer{}
```

All hook methods are called synchronously but must be **non-blocking** and **must not panic**.

---

## Metrics (v0.9)

```go
m := eng.Snapshot()
fmt.Println(m.TotalOperations)    // total Run() calls
fmt.Println(m.SuccessfulOps)      // operations that produced a value
fmt.Println(m.FailedOps)          // operations that returned an error
fmt.Println(m.TotalRiftsSpawned)  // goroutines launched
fmt.Println(m.TotalRiftsPruned)   // causal variants discarded
fmt.Println(m.MeanLatencyMs)      // mean end-to-end latency in milliseconds
fmt.Println(m.MeanEntropyDelta)   // mean winner entropy advantage over losers
fmt.Println(m.CircuitState)       // "closed" | "open" | "half-open"
fmt.Println(m.ErrorRate)          // ops-level error rate [0,1]
fmt.Println(m.ActiveRiftFactor)   // current adaptive RiftFactor
```

---

## Benchmarks (v0.9, Intel Xeon @ 2.80GHz)

```
BenchmarkRiftFactor2-2       338,958 ops/sec   (1,784 B/op,  31 allocs/op)
BenchmarkRiftFactor3-2       162,542 ops/sec   (2,600 B/op,  43 allocs/op)
BenchmarkEntropyFusion-2     155,226 ops/sec   (2,600 B/op,  43 allocs/op)
BenchmarkRiftFactor8-2        61,576 ops/sec   (6,680 B/op, 103 allocs/op)
BenchmarkNaiveDirectCall-2 1,000,000,000 ops/sec (0 B/op, 0 allocs/op)
```

Rift adds overhead proportional to `RiftFactor`. For I/O-bound operations (DB, HTTP, queue), the overhead is typically negligible relative to actual latency. The tradeoff: **zero race conditions** vs raw throughput.

---

## Project structure

```
rift/
├── rift.go                  # Public API — all callers start here
├── rift_test.go             # Tests + benchmarks (16 tests)
├── go.mod
├── cmd/
│   └── rift/
│       └── main.go          # CLI demo (7 modes)
├── internal/
│   ├── rift/
│   │   ├── types.go         # Shared types, interfaces, errors, config
│   │   └── rift.go          # Rift struct + lifecycle state machine
│   ├── clock/
│   │   └── clock.go         # ECC-2 algorithm (core novelty)
│   ├── splitter/
│   │   └── splitter.go      # Operation → N rifts
│   ├── executor/
│   │   └── executor.go      # Adaptive pool, retry, panic recovery
│   ├── fusion/
│   │   └── fusion.go        # Deterministic winner selection (4 strategies)
│   └── engine/
│       └── engine.go        # Orchestrator: circuit breaker, adaptive factor, metrics
└── examples/
    └── trading/
        └── main.go          # HFT order processor demo (full v0.9 feature usage)
```

---

## Use cases

- **Finance / HFT** — eliminate order-processing race conditions; circuit breaker protects under market stress; adaptive factor scales with volatility
- **IoT / telco** — deterministic packet processing regardless of arrival order; retry policy for intermittent connectivity
- **Game servers** — consistent state under thousands of concurrent player events
- **Data pipelines** — reproducible results despite concurrent stream ingestion
- **Chaos engineering** — `"entropy"` fusion strategy for maximum path diversity exploration

---

## Roadmap

- [ ] Distributed rifts across nodes (multi-machine causal fusion)
- [ ] 3D real-time causal graph visualiser
- [ ] Python bindings (ctypes / cffi)
- [ ] Rift-as-a-Service on Kubernetes
- [ ] Plugin API for custom fusion rules
- [ ] OpenTelemetry native adapter for TelemetryHook

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

Issues, benchmarks, and fusion rule plugins are welcome. The most valuable contributions are **real-world race conditions** that Rift helps eliminate — open an issue with a reproducible case.
