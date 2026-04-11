// Package rift is the public API of the Rift Execution Model v1.3.0.
//
// # What's new in v1.3.0
//
//   - ShedPolicy: load-shedding when the engine is saturated
//   - WarmupConfig: goroutine pool pre-heating for cold-start latency elimination
//   - HealthProbe: liveness/readiness API (Kubernetes-compatible)
//   - ErrShed + ErrWarmupTimeout: new sentinel errors
//   - Metrics.ShedOps: count of operations dropped under load
//   - CausalClock.Generation: ABA-safe clock recycling
//   - All v0.9 features preserved (CircuitBreaker, Adaptive, Retry, Telemetry, ECC-2)
//
// # Quick start
//
//	eng, _ := rift.NewEngine(rift.DefaultConfig())
//
//	result, err := eng.Run(func() (any, error) {
//	    return myService.ProcessOrder(orderID)
//	})
//	if err != nil { ... }
//	fmt.Println(result.Value)        // deterministic output
//	fmt.Println(result.CausalScore)  // ECC-2 priority score
//	fmt.Println(result.EntropyDelta) // causal path diversity consumed
//
// # Design philosophy
//
// Rift treats non-determinism as a resource, not a risk.
// Every operation is torn into N causal variants (rifts) that run in parallel.
// Extended Causal Clocks (ECC-2) then elect a deterministic winner —
// without global locks, re-execution, or observability blind spots.
package rift

import (
	"time"

	"github.com/damienos61/rift/internal/engine"
	r "github.com/damienos61/rift/internal/rift"
)

// ─── Re-exported types ────────────────────────────────────────────────────────

// Config controls engine behaviour. See internal/rift.Config for field docs.
type Config = r.Config

// Result is the deterministic output of a Run call.
type Result = r.Result

// TelemetryHook is the pluggable span export interface.
type TelemetryHook = r.TelemetryHook

// CircuitBreakerConfig configures the per-engine fault gate.
type CircuitBreakerConfig = r.CircuitBreakerConfig

// RetryPolicy controls transient rift failure recovery.
type RetryPolicy = r.RetryPolicy

// AdaptiveConfig enables self-tuning RiftFactor.
type AdaptiveConfig = r.AdaptiveConfig

// ShedPolicy controls load-shedding under saturation (v1.3.0).
type ShedPolicy = r.ShedPolicy

// WarmupConfig pre-heats the goroutine pool on engine start (v1.3.0).
type WarmupConfig = r.WarmupConfig

// CausalClock is the Extended Causal Clock (ECC-2) attached to each Rift.
type CausalClock = r.CausalClock

// OperationID uniquely identifies an incoming operation.
type OperationID = r.OperationID

// RiftID uniquely identifies a causal variant.
type RiftID = r.RiftID

// Metrics is the snapshot type returned by Engine.Snapshot().
type Metrics = engine.Metrics

// HealthStatus is returned by Engine.Health() (v1.3.0).
type HealthStatus = engine.HealthStatus

// ─── Sentinel errors ─────────────────────────────────────────────────────────

var (
	ErrNoRiftsConverged   = r.ErrNoRiftsConverged
	ErrFusionConflict     = r.ErrFusionConflict
	ErrOperationTimeout   = r.ErrOperationTimeout
	ErrInvalidRiftFactor  = r.ErrInvalidRiftFactor
	ErrNilOperation       = r.ErrNilOperation
	ErrCircuitOpen        = r.ErrCircuitOpen
	ErrMaxRetriesExceeded = r.ErrMaxRetriesExceeded
	ErrShed               = r.ErrShed          // v1.3.0
	ErrWarmupTimeout      = r.ErrWarmupTimeout // v1.3.0
)

// DefaultConfig returns production-safe defaults.
var DefaultConfig = r.DefaultConfig

// ─── Engine ───────────────────────────────────────────────────────────────────

// Engine is the public handle for the Rift engine.
// Safe for concurrent use from multiple goroutines.
type Engine struct{ inner *engine.RiftEngine }

// NewEngine creates and validates a new Engine from cfg.
func NewEngine(cfg Config) (*Engine, error) {
	inner, err := engine.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{inner: inner}, nil
}

// Run executes fn through the Rift Execution Model and returns a
// deterministic Result. Safe to call concurrently.
func (e *Engine) Run(fn func() (any, error)) (*Result, error) {
	return e.inner.Run(fn)
}

// RunWithTimeout is like Run but overrides the engine's DefaultTimeout
// for this specific operation.
func (e *Engine) RunWithTimeout(fn func() (any, error), timeout time.Duration) (*Result, error) {
	return e.inner.RunWithTimeout(fn, timeout)
}

// Snapshot returns a copy of the engine's runtime metrics.
func (e *Engine) Snapshot() Metrics {
	return e.inner.Snapshot()
}

// Health returns a liveness/readiness probe result (v1.3.0).
// Suitable for Kubernetes /healthz and /readyz endpoints.
func (e *Engine) Health() HealthStatus {
	return e.inner.Health()
}
