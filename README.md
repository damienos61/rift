# Rift — Causal Execution Engine for Go

> **Turn non-determinism from an enemy into a tool.**

[![Go 1.22+](https://img.shields.io/badge/go-1.22+-blue.svg)](https://golang.org/dl/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Race-free](https://img.shields.io/badge/race--detector-passing-brightgreen.svg)](#running-the-tests)
[![Zero dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#installation)

Rift is a Go library that makes concurrent operations **deterministic** — without rewriting your code. It does this by running every operation as several parallel variants called *rifts*, then picking the best result using an algorithm called **Extended Causal Clocks (ECC-2)**.

---

## Table of contents

1. [Why Rift exists](#why-rift-exists)
2. [How it works — the big picture](#how-it-works--the-big-picture)
3. [Installation](#installation)
4. [Quick start (5 minutes)](#quick-start-5-minutes)
5. [The ECC-2 algorithm — deep dive](#the-ecc-2-algorithm--deep-dive)
6. [Features](#features)
   - [Circuit breaker](#circuit-breaker)
   - [Retry policy](#retry-policy)
   - [Adaptive RiftFactor](#adaptive-riftfactor)
   - [Load shedding](#load-shedding)
   - [Warmup](#warmup)
   - [Health probe](#health-probe)
   - [Telemetry hook](#telemetry-hook)
7. [Configuration reference](#configuration-reference)
8. [Metrics reference](#metrics-reference)
9. [Running the CLI demos](#running-the-cli-demos)
10. [Running the tests](#running-the-tests)
11. [Benchmarks](#benchmarks)
12. [Project structure](#project-structure)
13. [Use cases](#use-cases)
14. [Version history](#version-history)
15. [Roadmap](#roadmap)
16. [Contributing](#contributing)

---

## Why Rift exists

### The problem: Heisenbugs

A **Heisenbug** is a bug that disappears or changes when you try to observe it. The most common cause in Go programs is a **race condition** — when two goroutines access shared data at the same time and the result depends on which one runs first.

```go
// Without Rift — classic race condition
var counter int64

go func() { counter++ }()
go func() { counter++ }()

// What is counter now? 1 or 2?
// It depends on the goroutine scheduler — which you cannot control.
// Under the race detector: likely 2. In production: sometimes 1.
// This bug disappears when you add logging. It's a Heisenbug.
```

Traditional solutions — mutexes, channels, distributed locks — all require you to **redesign** your code around the concurrency primitive. That is expensive, error-prone, and sometimes impossible with existing codebases.

### What Rift does instead

Rift wraps your existing function and runs it as **N parallel copies** (called rifts). Each copy executes independently, in its own goroutine, with no shared mutable state injected at split time. When all copies finish, a deterministic **fusion algorithm** picks the winner based on causal priority — not based on which goroutine happened to run first.

The result is always the same, regardless of goroutine scheduling. The Heisenbug has nowhere to hide.

```go
// With Rift — same function, deterministic result
eng, _ := rift.NewEngine(rift.DefaultConfig())

result, err := eng.Run(func() (any, error) {
    counter := atomic.AddInt64(&sharedCounter, 1)
    return counter, nil
})
// result.Value is always the causally-correct answer.
// No rewrite needed. No locks added to your code.
```

---

## How it works — the big picture

Every call to `eng.Run(fn)` goes through three steps:

```
Your function
      │
      ▼
┌─────────────┐
│   SPLIT     │  Clone fn into N goroutines (RiftFactor, default: 3)
└─────────────┘
      │
      ├──▶  Rift A  ──▶ [runs fn] ──▶ result A + ECC-2 clock A ──▶ ╮
      ├──▶  Rift B  ──▶ [runs fn] ──▶ result B + ECC-2 clock B ──▶ ──▶ ┌──────────┐
      └──▶  Rift C  ──▶ [runs fn] ──▶ result C + ECC-2 clock C ──▶ ╯   │  FUSE    │──▶ Deterministic Result
           (all parallel, no global locks, panics caught)               └──────────┘
```

**Step 1 — Split:** The `Splitter` creates N independent `Rift` objects, each wrapping your function in a closure-safe copy. The RiftFactor controls how many variants run (default 3, range 2–32).

**Step 2 — Execute:** The `Executor` runs all rifts concurrently inside a bounded goroutine pool (default: 2×GOMAXPROCS). Each rift gets its own ECC-2 `CausalClock` before it starts. Panics inside your function are caught and converted to errors — they never crash the engine.

**Step 3 — Fuse:** The `FusionEngine` receives all completed rifts and selects the winner using ECC-2 clock comparison. The winning rift's result becomes the return value of `eng.Run()`. All other rifts are marked as pruned.

**The key guarantee:** The winner is chosen by **causal priority score**, not by arrival time. Two calls with the same inputs on the same machine will always produce the same winner, regardless of how the goroutine scheduler decided to order things.

---

## Installation

```bash
go get github.com/damienos61/rift
```

**Requirements:** Go 1.22 or later. Zero external dependencies — only the Go standard library.

---

## Quick start (5 minutes)

### Step 1 — Create an engine

```go
package main

import (
    "fmt"
    "github.com/damienos61/rift"
)

func main() {
    // DefaultConfig gives you sensible production defaults:
    // RiftFactor=3, timeout=5s, fusion strategy="causal"
    eng, err := rift.NewEngine(rift.DefaultConfig())
    if err != nil {
        panic(err)
    }
    // eng is safe to use from multiple goroutines simultaneously.
```

### Step 2 — Run your function through Rift

```go
    result, err := eng.Run(func() (any, error) {
        // Put any function here — DB query, HTTP call, computation, etc.
        // Rift will run it 3 times in parallel and pick the best result.
        return "hello from rift", nil
    })

    if err != nil {
        fmt.Println("all variants failed:", err)
        return
    }

    fmt.Println(result.Value)        // "hello from rift"
    fmt.Println(result.CausalScore)  // e.g. 3.4104 — ECC-2 priority score
    fmt.Println(result.EntropyDelta) // e.g. 0.12   — path diversity delta
    fmt.Println(result.FusedFrom)    // e.g. 7      — which rift ID won
    fmt.Println(result.Retries)      // 0           — retries used
}
```

### Step 3 — Run the demos to see it in action

```bash
git clone https://github.com/damienos61/rift
cd rift
go run ./cmd/rift              # basic demo — 8 operations, shows scores and entropy
go run ./cmd/rift -heisen      # Heisenbug simulation with 200 goroutines
go run ./cmd/rift -bench       # throughput benchmark
```

**Expected output from the basic demo:**

```
╔══════════════════════════════════════════════════════════╗
║       R I F T  v1.3.0 — Causal Execution Engine         ║
╚══════════════════════════════════════════════════════════╝
  RiftFactor : 3  |  Strategy : causal

  op=1 | value=result-1     | score=3.3113 | entropy=0.1204 | retries=0
  op=2 | value=result-2     | score=3.4104 | entropy=0.0891 | retries=0
  ...

─── Metrics ─────────────────────────────────────────────
  ops:     total=8  success=8  failed=0  shed=0
  rifts:   spawned=24  pruned=16
  engine:  latency=0.02ms  entropy=0.1031  error_rate=0.0%
  circuit: closed  active_factor=3  strategy=causal
```

---

## The ECC-2 algorithm — deep dive

The **Extended Causal Clock v2 (ECC-2)** is the heart of Rift. It is the algorithm that decides which of the N parallel rifts "wins". Understanding it helps you understand why Rift is deterministic and what the numbers in the output mean.

### What is a CausalClock?

Every rift carries a `CausalClock` — a data structure with 6 fields that describe the rift's causal context:

```go
type CausalClock struct {
    RiftID     RiftID   // unique ID of this rift variant
    Lamport    uint64   // logical event counter (like a Lamport timestamp)
    Weight     float64  // the main priority score — computed by Finalize()
    WallNanos  int64    // wall clock time when this rift finished (nanoseconds)
    Entropy    float64  // execution path diversity score in (0, 1)
    Depth      uint32   // how many causal events this rift observed
    Generation uint32   // monotonic counter — prevents clock confusion under churn
}
```

At the start of execution, each rift is assigned a clock via `Tick()`. At the end of execution, `Finalize()` recomputes the `Weight` using 5 heuristics.

### The 5 Weight heuristics (applied in order)

`Weight` starts at 1.0 and is multiplied by each heuristic in sequence:

```
Initial Weight = 1.0
```

**H1 — Latency bonus** (sigmoid-shaped, range [0.5, 2.0])

A rift that finishes faster than the expected latency budget gets a bonus. One that finishes much slower gets a penalty. The transition is smooth (sigmoid) so there's no sharp cutoff.

```
ratio = latencyBudget / actualDuration

if ratio >> 1 (much faster than budget) → bonus ~2.0
if ratio == 1 (exactly on budget)       → neutral ~1.0
if ratio << 1 (much slower)             → penalty ~0.5

Weight *= 0.5 + 1.5 / (1 + exp(-2.2 × (ratio - 1)))
```

**H2 — Health bonus**

A rift that returned a value without error gets a significant boost. A rift that failed or panicked is heavily penalized.

```
if healthy:    Weight *= 1.55   (+55% boost)
if failed:     Weight *= 0.08   (-92% penalty)
```

This is the most important heuristic — it ensures that in the presence of transient failures, healthy rifts almost always win.

**H3 — Lamport depth bonus** (log-scale)

A rift that has a higher Lamport counter has "witnessed" more causal events in the system. More witnessed events = richer causal view = slight priority boost. The log scale prevents runaway amplification.

```
if Lamport > 1:
    Weight *= 1.0 + log1p(Lamport) × 0.05
```

**H4 — Entropy bonus** (new in v0.9)

Entropy measures how diverse the execution path of this rift was. It is computed by mixing the rift's path hash (derived from its RiftID and Lamport counter) with a shared entropy pool that accumulates contributions from all rifts. A rift with a more unique execution path gets a small boost.

```
entropy = normEntropy(pathHash XOR entropyPool)   -- result in (0, 1)
Weight *= 1.0 + 0.15 × entropy
```

The entropy pool is a cross-rift accumulator — every time a rift finishes, it adds its path hash to the pool. This means each rift's entropy score depends not just on itself but on the entire ensemble of rifts that ran before it.

**H5 — Causal depth bonus** (new in v0.9)

`Depth` tracks how many times `Converge()` has been called on this rift. In the current implementation, each rift converges exactly once, so `Depth = 1` after execution. The bonus is small but ensures deeper causal chains are preferred.

```
if Depth > 0:
    Weight *= 1.0 + log1p(Depth) × 0.02
```

**Floor clamp:** After all heuristics, weight is clamped to a minimum of 0.001. This prevents zero-weight rifts from causing division issues in downstream calculations. NaN and Inf values (from degenerate inputs) are also replaced with the floor.

### The total score formula

The final `CausalScore` you see in `result.CausalScore` is computed by `clock.Score()`:

```
Score = Weight × (1 + 0.15 × Entropy)
      + log1p(Lamport) × 0.01
      + log1p(Depth)   × 0.02
      + 1 / (1 + WallNanos / 1e9) × 0.001
```

Higher is better. The score is used for logging and telemetry — the actual comparison for winner selection uses the 5-dimension total order below.

### The 5-dimension total order (fusion comparison)

When the fusion engine compares two rifts, it uses this strict priority order:

| Priority | Dimension | Rule |
|---|---|---|
| 1st | `Weight` | Higher wins. Difference must be > 1×10⁻⁹ to count. |
| 2nd | `Entropy` | Higher wins. Difference must be > 1×10⁻⁹ to count. |
| 3rd | `Lamport` | Higher wins. (More events witnessed.) |
| 4th | `WallNanos` | **Lower wins.** (Finished sooner.) |
| 5th | `RiftID` | Higher wins. Absolute tiebreaker. |

This is a **strict total order** — no two distinct clocks can ever compare equal (RiftID guarantees uniqueness). This means the fusion engine always produces the same winner, call after call, regardless of goroutine scheduling. That is the determinism guarantee.

### What is EntropyDelta?

`result.EntropyDelta` is the difference between the winner's entropy and the average entropy of the losing rifts:

```
EntropyDelta = winner.Entropy - mean(loser1.Entropy, loser2.Entropy, ...)
```

A positive EntropyDelta (e.g., `+0.12`) means the winner explored a more diverse execution path than the average of the losers — it had a richer causal view. A negative value means the winner was the "least diverse" rift but won on Weight or Lamport.

This metric is useful for monitoring: sustained negative EntropyDelta over many operations may indicate that all rifts are taking identical code paths, which reduces causal coverage.

### The Lamport counter — why it matters

Rift uses a single process-wide **atomic Lamport counter**. Every time a rift starts (`Tick`) or finishes (`Finalize`), the counter is incremented. This means:

- The counter is a total order over all events in the engine
- A rift that starts later (higher Lamport) has genuinely "seen more" of the engine's state
- The counter never resets — it is monotonically increasing for the lifetime of the process

The `Generation` counter (added in v1.3.0) is a separate monotonic counter that increments once per `Tick()` call. It is stored in the clock but is currently used as an additional uniqueness signal — it prevents ABA-style confusion if the engine processes an extremely large number of operations and RiftIDs wrap around.

---

## Features

### Circuit breaker

The circuit breaker is a **fault gate** that automatically stops accepting operations when too many of them are failing. This protects downstream systems (databases, APIs) from being hammered when they are already overloaded.

**States:**

```
CLOSED ──(error rate ≥ threshold)──▶ OPEN ──(after cool-down)──▶ HALF-OPEN
  ▲                                                                    │
  │                                                         (probe OK) │
  └────────────────────────────────────────────────────────────────────┘
                                                         (probe fails) │
                                                                ▼
                                                              OPEN (again)
```

- **CLOSED** (normal): all operations pass through
- **OPEN**: all operations immediately return `ErrCircuitOpen` — no goroutines wasted
- **HALF-OPEN**: after the cool-down period, one probe operation is allowed through. If it succeeds, the circuit closes. If it fails, the circuit opens again.

**Configuration:**

```go
cfg.CircuitBreaker = rift.CircuitBreakerConfig{
    Enabled:   true,
    Threshold: 0.5,              // open when 50% of ops in the window fail
    Window:    100,              // rolling window: last 100 operations
    CoolDown:  10*time.Second,   // wait 10s before probing
}
```

**How the window works:** The circuit breaker maintains a ring buffer of the last `Window` operation outcomes (true=success, false=failure). After each operation, it recomputes the failure rate across the entire window. If the rate meets or exceeds `Threshold`, the circuit opens.

**Demo:**

```bash
go run ./cmd/rift -circuit
```

```
═══ Circuit Breaker Demo ═══
  Injecting failures to trip the breaker...
  op=01 err=true  circuit=closed
  op=02 err=true  circuit=closed
  ...
  op=10 err=true  circuit=open
  op=11 err=true  circuit=open    ← engine stops trying
  ...
  Post-saturation: err=true (expect circuit-open)
  Waiting 2s for cool-down...
  Probe OK: recovered  circuit=closed  ← back to normal
```

---

### Retry policy

When a rift fails (error or panic), Rift can automatically retry it with **exponential backoff** before moving to fusion. Each retry uses a fresh ECC-2 clock tick, so the retry's causal timestamp correctly reflects its position in the event timeline.

```go
cfg.Retry = rift.RetryPolicy{
    MaxAttempts: 3,                       // try up to 3 times per rift
    Backoff:     1*time.Millisecond,      // start with 1ms delay
    MaxBackoff:  50*time.Millisecond,     // cap the delay at 50ms
}
// Actual delays: attempt 2 waits 1ms, attempt 3 waits 2ms, capped at 50ms
```

**How backoff works:**

```
Attempt 1: run immediately
Attempt 2: wait Backoff (1ms), then run
Attempt 3: wait min(Backoff×2, MaxBackoff) = min(2ms, 50ms) = 2ms, then run
```

Retries are per-rift — each of the N rifts retries independently. So with `RiftFactor=3` and `MaxAttempts=3`, up to 9 total executions of your function can occur. As soon as any rift converges successfully, it stops retrying.

`result.Retries` tells you the total number of retry attempts across all rifts for that operation.

---

### Adaptive RiftFactor

The adaptive factor automatically **adjusts the number of rifts** based on the live error rate. When things go wrong, Rift becomes more defensive by spawning more variants. When things are healthy, it reduces overhead.

```go
cfg.Adaptive = rift.AdaptiveConfig{
    Enabled:       true,
    MinFactor:     2,    // never go below 2 rifts (minimum for fusion to work)
    MaxFactor:     8,    // never go above 8 rifts
    ErrorRateUp:   0.3,  // if error rate > 30%, increase RiftFactor by 1
    ErrorRateDown: 0.05, // if error rate < 5%,  decrease RiftFactor by 1
}
```

The decision is made over a rolling window of the last 20 operations. `eng.Snapshot().ActiveRiftFactor` tells you the current live value.

**Demo (4 phases — healthy → degraded → recovering → healthy):**

```bash
go run ./cmd/rift -adaptive
```

```
  Phase: healthy      (fail_rate=0%)
  active_factor=2  error_rate=0.0%  circuit=closed

  Phase: degraded     (fail_rate=60%)
  active_factor=6  error_rate=42.0%  circuit=closed   ← scaled up under stress

  Phase: recovering   (fail_rate=10%)
  active_factor=5  error_rate=30.0%  circuit=closed

  Phase: healthy      (fail_rate=0%)
  active_factor=2  error_rate=5.0%   circuit=closed   ← scaled back down
```

---

### Load shedding

When the engine is **saturated** — more operations arriving than it can process — the load shedder immediately rejects excess operations with `ErrShed` instead of letting them pile up in a queue. This is the correct behavior in high-throughput systems: a fast, explicit rejection is better than a slow timeout.

**How it works:** The shedder uses a token-bucket backed by a buffered channel of capacity `MaxQueueLen`. Each operation acquires a token before starting and releases it when done. If no token is available, the operation is shed immediately.

```go
cfg.Shed = rift.ShedPolicy{
    Enabled:     true,
    MaxQueueLen: 1000,    // allow up to 1000 in-flight operations
    Strategy:    "newest", // currently: reject new arrivals when full
}
```

**Handling shed errors:**

```go
result, err := eng.Run(myFn)
if errors.Is(err, rift.ErrShed) {
    // Operation was dropped — queue your retry logic here,
    // or return 503 to the caller
    return
}
```

**Demo:**

```bash
go run ./cmd/rift -shed
```

```
═══ Load-Shedding Demo (v1.3.0) ═══
  Sending 500 ops into an engine with MaxQueueLen=50.
  processed=50  shed=450  engine_shed_count=450
```

`eng.Snapshot().ShedOps` gives you the total count of shed operations since engine creation.

---

### Warmup

The first few operations on a new engine may be slower than normal because the Go runtime hasn't yet scheduled the goroutine pool. The warmup feature runs a configurable number of no-op operations at engine creation time to pre-heat the pool, eliminating cold-start latency spikes.

```go
cfg.Warmup = rift.WarmupConfig{
    Enabled: true,
    Ops:     10,               // run 10 no-op operations at startup
    Timeout: 2*time.Second,    // if warmup doesn't finish in 2s, fail with ErrWarmupTimeout
}

eng, err := rift.NewEngine(cfg)
if errors.Is(err, rift.ErrWarmupTimeout) {
    // Warmup timed out — the system may be under severe load at startup
}
```

This is particularly useful in Kubernetes pods where the first request arrives immediately after the container starts.

---

### Health probe

The health probe gives you a **liveness and readiness check** that is compatible with Kubernetes `/healthz` and `/readyz` endpoints.

```go
h := eng.Health()

h.Live   // bool — true if the engine is running (always true if you have an Engine)
h.Ready  // bool — true if: (1) circuit is closed AND (2) error rate ≤ 50%
h.Reason // string — human-readable reason when Ready=false, e.g. "circuit breaker open"
```

**Rules for Ready=false:**

| Condition | Reason field |
|---|---|
| Circuit is open | `"circuit breaker open"` |
| Circuit is half-open | `"circuit breaker half-open"` |
| Error rate > 50% | `"error rate too high"` |

**Example Kubernetes handler:**

```go
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    h := eng.Health()
    if !h.Live {
        http.Error(w, "engine not live", 503)
        return
    }
    w.WriteHeader(200)
    fmt.Fprintln(w, "live")
})

http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    h := eng.Health()
    if !h.Ready {
        http.Error(w, h.Reason, 503)
        return
    }
    w.WriteHeader(200)
    fmt.Fprintln(w, "ready")
})
```

**Demo:**

```bash
go run ./cmd/rift -health
```

```
═══ Health Probe Demo (v1.3.0) ═══
  Healthy engine  — live=true  ready=true
  Stressed engine — live=true  ready=false  reason="circuit breaker open"
```

---

### Telemetry hook

The telemetry hook lets you plug in your own tracing or metrics system without any import coupling. You implement a small interface and Rift calls your methods at key lifecycle points. All methods must be non-blocking and must not panic.

```go
type TelemetryHook interface {
    // Called once per Run() call, right after the split.
    OnSplit(opID OperationID, factor int)

    // Called once per rift, immediately after it finishes (success or error).
    OnConverge(riftID RiftID, duration time.Duration, healthy bool)

    // Called once per Run() call, after fusion selects a winner.
    OnFuse(opID OperationID, winner RiftID, score float64, pruned int)

    // Called when the fused result is an error.
    OnError(opID OperationID, err error)

    // Called when an operation is dropped by the load shedder (v1.3.0).
    OnShed(opID OperationID)
}
```

**Example — connecting to your own tracing system:**

```go
type myTracer struct{}

func (t *myTracer) OnSplit(opID rift.OperationID, factor int) {
    mySpan.Start(fmt.Sprintf("rift.split op=%d factor=%d", opID, factor))
}
func (t *myTracer) OnConverge(riftID rift.RiftID, d time.Duration, healthy bool) {
    myMetrics.Record("rift.converge.latency_ms", d.Milliseconds())
}
func (t *myTracer) OnFuse(opID rift.OperationID, winner rift.RiftID, score float64, pruned int) {
    myMetrics.Record("rift.causal_score", score)
}
func (t *myTracer) OnError(opID rift.OperationID, err error) {
    myAlerts.Trigger("rift.operation_failed", err)
}
func (t *myTracer) OnShed(opID rift.OperationID) {
    myMetrics.Increment("rift.shed_total")
}

cfg := rift.DefaultConfig()
cfg.Telemetry = &myTracer{}
eng, _ := rift.NewEngine(cfg)
```

---

## Configuration reference

```go
cfg := rift.DefaultConfig()  // always start from defaults

// ── Core ──────────────────────────────────────────────────────────────────────
cfg.RiftFactor     = 3                // Number of parallel variants [2, 32]
cfg.WorkerPoolSize = 0                // Goroutine pool size. 0 = 2×GOMAXPROCS
cfg.DefaultTimeout = 5*time.Second    // Per-rift timeout. 0 = no timeout.

// FusionStrategy selects how the winner is chosen:
//   "causal"   (default) — ECC-2 total order (Weight, Entropy, Lamport, Wall, ID)
//   "entropy"  — Entropy is primary; useful for chaos testing
//   "lamport"  — Pure Lamport counter ordering (classic vector clock)
//   "fastest"  — Lowest wall time wins (non-deterministic; benchmarks only)
cfg.FusionStrategy = "causal"

// ── Circuit breaker ──────────────────────────────────────────────────────────
cfg.CircuitBreaker = rift.CircuitBreakerConfig{
    Enabled:   false,             // disabled by default
    Threshold: 0.5,               // open at 50% error rate
    Window:    100,               // rolling window size (number of operations)
    CoolDown:  10*time.Second,    // half-open probe delay after opening
}

// ── Retry ────────────────────────────────────────────────────────────────────
cfg.Retry = rift.RetryPolicy{
    MaxAttempts: 1,               // 1 = no retry (default). Set ≥ 2 for retries.
    Backoff:     1*time.Millisecond,
    MaxBackoff:  50*time.Millisecond,
}

// ── Adaptive RiftFactor ──────────────────────────────────────────────────────
cfg.Adaptive = rift.AdaptiveConfig{
    Enabled:       false,         // disabled by default
    MinFactor:     2,
    MaxFactor:     6,
    ErrorRateUp:   0.3,           // increase factor when error rate > 30%
    ErrorRateDown: 0.05,          // decrease factor when error rate < 5%
}

// ── Load shedding ────────────────────────────────────────────────────────────
cfg.Shed = rift.ShedPolicy{
    Enabled:     false,           // disabled by default
    MaxQueueLen: 1000,            // max concurrent in-flight operations
    Strategy:    "newest",        // "newest": reject new arrivals when full
}

// ── Warmup ───────────────────────────────────────────────────────────────────
cfg.Warmup = rift.WarmupConfig{
    Enabled: false,               // disabled by default
    Ops:     10,                  // number of no-op warmup operations
    Timeout: 2*time.Second,       // max time allowed for warmup
}

// ── Telemetry ────────────────────────────────────────────────────────────────
cfg.Telemetry = nil               // nil = no telemetry (default). Set to your hook.

eng, err := rift.NewEngine(cfg)
```

**Sentinel errors:**

| Error | When returned |
|---|---|
| `rift.ErrNilOperation` | `eng.Run(nil)` |
| `rift.ErrInvalidRiftFactor` | `RiftFactor < 2` |
| `rift.ErrOperationTimeout` | A rift exceeded its timeout |
| `rift.ErrNoRiftsConverged` | Internal error — zero rifts were created |
| `rift.ErrFusionConflict` | All rifts failed — result contains best-effort error |
| `rift.ErrCircuitOpen` | Circuit breaker is open |
| `rift.ErrMaxRetriesExceeded` | (internal, surfaced via ErrFusionConflict) |
| `rift.ErrShed` | Operation rejected by the load shedder |
| `rift.ErrWarmupTimeout` | Warmup did not complete within its deadline |

---

## Metrics reference

```go
m := eng.Snapshot()
```

| Field | Type | Description |
|---|---|---|
| `TotalOperations` | `uint64` | Total calls to `Run()` or `RunWithTimeout()` that passed the shedder and circuit breaker |
| `SuccessfulOps` | `uint64` | Operations that returned a non-nil value without error |
| `FailedOps` | `uint64` | Operations where all rifts failed (all variants errored or panicked) |
| `ShedOps` | `uint64` | Operations rejected by the load shedder before they were split |
| `TotalRiftsSpawned` | `uint64` | Total goroutines launched across all operations |
| `TotalRiftsPruned` | `uint64` | Rifts discarded after fusion (always `TotalRiftsSpawned - TotalOperations`) |
| `RiftFactor` | `int` | The configured RiftFactor from `Config` |
| `ActiveRiftFactor` | `int` | The current live RiftFactor (differs from `RiftFactor` when Adaptive is enabled) |
| `FusionStrategy` | `string` | Active fusion strategy name |
| `MeanLatencyMs` | `float64` | Mean wall time per `Run()` call in milliseconds, computed over all completed operations |
| `MeanEntropyDelta` | `float64` | Mean `EntropyDelta` across all completed operations |
| `CircuitState` | `string` | `"closed"` \| `"open"` \| `"half-open"` |
| `ErrorRate` | `float64` | `FailedOps / TotalOperations`, range [0, 1] |

---

## Running the CLI demos

All demos are in `cmd/rift/main.go` and are run with `go run ./cmd/rift [flags]`.

```bash
# Basic demo — 8 operations showing score, entropy, and metrics
go run ./cmd/rift

# With a custom RiftFactor and fusion strategy
go run ./cmd/rift -factor 5 -strategy entropy

# Heisenbug simulation — 200 concurrent goroutines racing on a counter
go run ./cmd/rift -heisen

# Throughput benchmark — 20,000 operations, 8 workers
go run ./cmd/rift -bench

# Circuit breaker demo — inject failures, watch circuit open and recover
go run ./cmd/rift -circuit

# Adaptive RiftFactor demo — 4 phases of varying error rates
go run ./cmd/rift -adaptive

# Load-shedding demo — 500 ops into a MaxQueueLen=50 engine
go run ./cmd/rift -shed

# Health probe demo — healthy vs stressed engine comparison
go run ./cmd/rift -health
```

**HFT trading example** (demonstrates all features together):

```bash
go run ./examples/trading
```

This runs a simulated HFT order processor with 15% transient exchange failure rate, circuit breaker, retry, adaptive factor, load shedding, and telemetry — all enabled simultaneously.

---

## Running the tests

```bash
# Run all 22 tests
go test ./...

# Run with the race detector — must produce zero race warnings
go test -race ./...

# Run a specific test with verbose output
go test -run TestHeisenBugElimination -v
go test -run TestCircuitBreakerClosesAfterCoolDown -v
go test -run TestLoadSheddingRejectsWhenSaturated -v

# Run all benchmarks with memory stats
go test -bench=. -benchmem

# Run benchmarks for 5 seconds each (more stable numbers)
go test -bench=. -benchtime=5s -benchmem
```

### Test results (v1.3.0)

| Test | What it verifies |
|---|---|
| `TestRunBasic` | Engine returns the correct value |
| `TestRunPropagatesError` | Errors from your function are returned correctly |
| `TestRunPanicRecovery` | Panics inside your function are caught, never crash the engine |
| `TestRunNilFunction` | `eng.Run(nil)` returns `ErrNilOperation` |
| `TestCausalScorePositive` | `result.CausalScore` is always > 0 for healthy operations |
| `TestEntropyDeltaFinite` | `result.EntropyDelta` is never NaN or Inf |
| `TestHeisenBugElimination` | 100 concurrent goroutines — every rift converges to a value |
| `TestRunWithTimeout` | Operations that exceed their timeout return `ErrOperationTimeout` |
| `TestCircuitBreakerOpens` | Circuit opens after enough failures |
| `TestCircuitBreakerClosesAfterCoolDown` | Circuit recovers after cool-down |
| `TestAdaptiveFactorBounds` | ActiveRiftFactor always stays within [MinFactor, MaxFactor] |
| `TestRetryPolicyConverges` | Retry eventually finds a healthy variant |
| `TestEntropyFusionStrategy` | The `"entropy"` strategy returns the correct value |
| `TestLoadSheddingRejectsWhenSaturated` | At least some ops are shed when pool is tiny |
| `TestHealthProbeHealthyEngine` | Healthy engine: Live=true, Ready=true |
| `TestHealthProbeStressedEngine` | Stressed engine: Live=true, Ready=false |
| `TestWarmupCompletes` | Engine with warmup enabled works correctly after creation |
| `TestCustomRiftFactor/factor=2` | RiftFactor=2 works |
| `TestCustomRiftFactor/factor=4` | RiftFactor=4 works |
| `TestCustomRiftFactor/factor=8` | RiftFactor=8 works |
| `TestCustomRiftFactor/factor=16` | RiftFactor=16 works |
| `TestRiftFactorAboveMaxRejected` | RiftFactor=1 returns an error at engine creation |
| `TestMetricsAccuracy` | TotalRiftsSpawned = TotalOperations × RiftFactor |
| `TestTelemetryHook` | OnSplit called N times, OnFuse called N times, OnConverge called N×RiftFactor times |
| `TestConcurrentSafety` | 50 concurrent goroutines using the same engine — zero errors |
| **Race detector** | **`go test -race ./...` — zero data races detected** |

---

## Benchmarks

Measured on Intel Xeon Platinum 8581C @ 2.10GHz, Go 1.22, Linux amd64, `go test -bench=. -benchtime=3s -benchmem`.

| Benchmark | ops/sec | ns/op | Bytes/op | Allocs/op | Notes |
|---|---|---|---|---|---|
| `BenchmarkRiftFactor2` | ~95k | 10,477 | 1,736 | 29 | Minimum overhead, 2 variants |
| `BenchmarkRiftFactor3` | ~66k | 15,063 | 2,520 | 40 | Default config |
| `BenchmarkRiftFactor8` | ~25k | 39,713 | 6,440 | 95 | Max coverage |
| `BenchmarkEntropyFusion` | ~67k | 14,975 | 2,520 | 40 | Same as factor=3, different fusion |
| `BenchmarkWithCircuitBreaker` | ~6M | 163 | 0 | 0 | Circuit breaker overhead only |
| `BenchmarkNaiveDirectCall` | ~1.1B | 0.91 | 0 | 0 | Raw function call — baseline |

**Reading the numbers:**

- Rift adds overhead proportional to `RiftFactor` because it genuinely runs your function multiple times. For CPU-bound micro-benchmarks (like `return 1+1`) this overhead is visible and expected.
- For **I/O-bound operations** (database queries, HTTP calls, gRPC) that typically take 1–50ms, Rift's overhead of 10–40µs is 25–5000× smaller than the operation itself — effectively zero.
- The `BenchmarkWithCircuitBreaker` result (163ns, 0 allocs) shows what happens when the circuit is closed and all overhead is just the check — it is extremely cheap.

**When to use which RiftFactor:**

| Scenario | Recommended RiftFactor |
|---|---|
| Minimum overhead, very stable systems | 2 |
| General production use | 3 (default) |
| High error rate environments, HFT | 4–6 |
| Maximum causal coverage, chaos testing | 8+ |
| Adaptive (auto-tunes) | start at 3, enable `Adaptive` |

---

## Project structure

```
rift/
│
├── rift.go              # Public API — the only file you need to import
│                        # Re-exports all types; wraps the internal engine.
│
├── rift_test.go         # 22 tests + 6 benchmarks (all in package rift_test)
│
├── go.mod               # Module: github.com/damienos61/rift, go 1.22
├── README.md            # This file
├── CHANGELOG.md         # Version history with detailed change notes
├── LICENSE              # MIT
├── .gitignore
│
├── cmd/
│   └── rift/
│       └── main.go      # Interactive CLI — all demos and flags
│
├── examples/
│   └── trading/
│       └── main.go      # HFT order processor — all features at once
│
└── internal/            # Internal packages — not part of the public API
    │
    ├── rift/
    │   ├── types.go     # All shared types: Config, Result, CausalClock,
    │   │                # State, Operation, all interfaces, sentinel errors
    │   └── rift.go      # Rift struct + state machine + FNV-1a path hash
    │
    ├── clock/
    │   └── clock.go     # ECC-2 implementation: Tick, Finalize, Compare, Score
    │                    # The core algorithm — see the deep dive section above
    │
    ├── splitter/
    │   └── splitter.go  # Creates N Rift objects from one Operation
    │                    # Closure-safe capture, MaxRiftFactor=32 guard
    │
    ├── executor/
    │   └── executor.go  # Runs rifts concurrently, manages the semaphore pool,
    │                    # handles retry backoff, catches panics
    │
    ├── fusion/
    │   └── fusion.go    # 3-pass selection: filter → rank → select+prune
    │                    # All 4 strategies: causal, entropy, lamport, fastest
    │
    └── engine/
        └── engine.go    # Top-level orchestrator: wires everything together
                         # Circuit breaker, adaptive factor, shedder, metrics,
                         # warmup, health probe
```

---

## Use cases

Rift is a good fit when:

- Your code has **non-deterministic behaviour** under concurrency that is hard to reproduce
- You need **deterministic results** from concurrent operations without rewriting your code
- You want **automatic fault tolerance** (retry, circuit breaking) with minimal configuration
- You are building services that need **Kubernetes-compatible health checks**
- You want **visibility** into operation latency and error rates without adding external dependencies

**Common patterns:**

| Scenario | Rift features to enable |
|---|---|
| Database reads with occasional timeouts | `Retry`, `CircuitBreaker` |
| gRPC services under variable load | `Adaptive`, `Shed`, `HealthProbe` |
| HFT order processing | `Retry`, `CircuitBreaker`, `Adaptive`, `Telemetry` |
| IoT sensor stream aggregation | `Adaptive`, `Shed` |
| Chaos/fault injection testing | `FusionStrategy="entropy"` |
| Kubernetes sidecar or init container | `Warmup`, `HealthProbe` |

---

## Version history

| Version | Release | Key additions |
|---|---|---|
| v0.1.0 | Apr 2026 | Rift Execution Model, ECC-1 (Lamport + Weight + Wall), basic fusion, `"causal"` / `"lamport"` / `"fastest"` strategies, CLI demo |
| v0.9.0 | Apr 2026 | ECC-2 (`Entropy` + `Depth` dimensions), `CircuitBreaker`, `AdaptiveRiftFactor`, `RetryPolicy`, `TelemetryHook`, `"entropy"` strategy, enriched `Metrics` |
| **v1.3.0** | **Apr 2026** | **`ShedPolicy` (load shedding + `ErrShed`), `WarmupConfig` (+ `ErrWarmupTimeout`), `Health()` probe, `CausalClock.Generation`, `MaxRiftFactor=32`, deterministic FNV-1a path hash, `BenchmarkWithCircuitBreaker`, 22 tests, `go test -race` clean** |

---

## Roadmap

Items below are planned but **not yet implemented**. They are not present in v1.3.0.

- [ ] **Distributed rifts** — run causal variants across multiple machines, fuse over the network
- [ ] **3D rift visualiser** — real-time causal graph of rifts and their clocks
- [ ] **Python bindings** — use Rift from Python via CGo or a gRPC bridge
- [ ] **OpenTelemetry exporter** — built-in OTLP export, not just the hook interface
- [ ] **Rift-as-a-Service** — deploy the engine on Kubernetes, expose it as a gRPC service
- [ ] **Custom fusion plugin API** — register your own fusion strategy at runtime

---

## Contributing

The most valuable contributions are **real-world cases** where Rift does or does not work. If you have a concurrent bug that Rift helped fix, or a benchmark showing how it behaves in your system, please open an issue.

For code contributions:

1. Fork the repository
2. Make your change
3. Run `go test -race ./...` — must be clean
4. Run `go vet ./...` — must be clean
5. Open a pull request with a clear description of what changed and why

---

## License

MIT — see [LICENSE](LICENSE)
