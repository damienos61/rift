# Changelog

All notable changes to Rift are documented here.

## [v1.3.0] — 2026-04-11

### Added
- `ShedPolicy`: token-bucket load shedder — rejects operations when the engine is saturated (`ErrShed`). Configurable `MaxQueueLen`, `Strategy` ("newest" | "oldest" | "priority"), and `ShedTimeout`.
- `WarmupConfig`: goroutine pool pre-heating on engine start — eliminates cold-start latency spikes (`ErrWarmupTimeout` if warmup exceeds deadline).
- `Engine.Health()`: liveness/readiness probe returning `HealthStatus{Live, Ready, Reason}` — compatible with Kubernetes `/healthz` and `/readyz`.
- `CausalClock.Generation`: monotonic generation counter that prevents ABA clock confusion under high churn.
- `Metrics.ShedOps`: count of operations dropped by the load shedder.
- `TelemetryHook.OnShed(opID)`: new hook method called on every shed event.
- `splitter.MaxRiftFactor = 32`: hard upper bound on `RiftFactor` with a descriptive error.
- `TestConcurrentSafety`: 50-goroutine concurrent correctness test.
- `TestLoadSheddingRejectsWhenSaturated`: deterministic shed test.
- `TestHealthProbeHealthyEngine`, `TestHealthProbeStressedEngine`: health probe tests.
- `TestWarmupCompletes`: warmup integration test.
- `BenchmarkWithCircuitBreaker`: circuit-breaker overhead benchmark (~163 ns/op, 0 allocs).
- CLI flags: `-shed`, `-health` for new feature demos.

### Improved
- `rift.go (internal)`: path hash now uses deterministic FNV-1a mixing over `RiftID ^ Lamport` — no more `time.Now()` in hash (fixes non-reproducibility).
- `executor.go`: pool size defaults to `2×GOMAXPROCS` (was `GOMAXPROCS`) for better burst headroom.
- `executor.go`: panic reports now include `rift#ID` for easier post-mortem correlation.
- `clock.go`: `Finalize` guards against `NaN`/`Inf` weight from degenerate inputs.
- `clock.go`: all ECC-2 constants are named and documented — no magic numbers.
- `clock.go`: `generationCounter` atomically assigns `Generation` to every new clock.
- `fusion.go`: unknown `FusionStrategy` falls back to `"causal"` instead of panicking.
- `splitter.go`: closure capture is now explicit (`fn := op.Fn`) — zero aliasing risk.
- `engine.go`: `recordEntropy` uses lock-free CAS on float64 bits (was already atomic, now documented).
- `cmd/rift/main.go`: all `fmt.Println` newline-in-string warnings fixed (`go vet` clean).

### Fixed
- Race detector: all 22 tests pass with `-race` — zero data races detected.
- `go vet`: zero warnings across all packages.

---

## [v0.9.0] — 2026-04-09

### Added
- ECC-2: `Entropy` and `Depth` dimensions in `CausalClock`
- `CircuitBreaker`: rolling-window fault gate (open / half-open / closed)
- `AdaptiveRiftFactor`: self-tuning `RiftFactor` based on live error rate
- `RetryPolicy`: exponential backoff for transient rift failures
- Entropy-weighted fusion strategy (`"entropy"`)
- `TelemetryHook`: zero-cost span/trace export interface
- `RunWithTimeout`: per-call timeout override
- Enriched `Metrics`: `MeanLatencyMs`, `MeanEntropyDelta`, `CircuitState`, `ErrorRate`, `ActiveRiftFactor`

---

## [v0.1.0] — 2026-04-09

### Added
- Rift Execution Model: Split → Execute → Fuse pipeline
- ECC-1: `CausalClock` with `Lamport`, `Weight`, `WallNanos`, `RiftID`
- Sigmoid-shaped latency bonus in `clock.Finalize`
- Bounded goroutine pool with panic recovery
- `"causal"`, `"lamport"`, `"fastest"` fusion strategies
- `Metrics`: basic counters
- CLI demo with `-bench`, `-heisen`, `-factor`
- HFT trading example
