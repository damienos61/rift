// Package rift is the public API of the Rift Execution Model v0.9.
//
// # What's new in v0.9
//
//   - ECC-2: Entropy + Depth dimensions in CausalClock
//   - CircuitBreaker: automatic fault gate under high error rates
//   - AdaptiveFactor: self-tuning RiftFactor based on live error rate
//   - RetryPolicy: exponential backoff for transient rift failures
//   - Entropy-weighted fusion strategy
//   - TelemetryHook: zero-cost span/trace export interface
//   - Richer Metrics: latency, entropy delta, circuit state, error rate
//   - RunWithTimeout: per-call timeout override
//
// # Quick start
//
//	eng, _ := rift.NewEngine(rift.DefaultConfig())
//	result, err := eng.Run(func() (any, error) {
//	    return myService.ProcessOrder(orderID)
//	})
//	fmt.Println(result.Value)        // deterministic output
//	fmt.Println(result.CausalScore)  // ECC-2 priority score
//	fmt.Println(result.EntropyDelta) // causal path diversity consumed
package rift

import (
	"time"

	"github.com/damienos61/rift/internal/engine"
	r "github.com/damienos61/rift/internal/rift"
)

// ─── Re-exported types ────────────────────────────────────────────────────────

// Config controls engine behaviour.
type Config = r.Config

// Result is the deterministic output of a Run call.
type Result = r.Result

// TelemetryHook is the pluggable span export interface (v0.9).
type TelemetryHook = r.TelemetryHook

// CircuitBreakerConfig configures the per-engine fault gate (v0.9).
type CircuitBreakerConfig = r.CircuitBreakerConfig

// RetryPolicy controls transient rift failure recovery (v0.9).
type RetryPolicy = r.RetryPolicy

// AdaptiveConfig enables self-tuning RiftFactor (v0.9).
type AdaptiveConfig = r.AdaptiveConfig

// CausalClock is the extended logical clock (ECC-2) attached to each Rift.
type CausalClock = r.CausalClock

// OperationID uniquely identifies an incoming operation.
type OperationID = r.OperationID

// RiftID uniquely identifies a causal variant.
type RiftID = r.RiftID

// Metrics is the snapshot type returned by Engine.Snapshot().
type Metrics = engine.Metrics

// ─── Sentinel errors ─────────────────────────────────────────────────────────

var (
	ErrNoRiftsConverged   = r.ErrNoRiftsConverged
	ErrFusionConflict     = r.ErrFusionConflict
	ErrOperationTimeout   = r.ErrOperationTimeout
	ErrInvalidRiftFactor  = r.ErrInvalidRiftFactor
	ErrNilOperation       = r.ErrNilOperation
	ErrCircuitOpen        = r.ErrCircuitOpen
	ErrMaxRetriesExceeded = r.ErrMaxRetriesExceeded
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
// for this specific operation (v0.9).
func (e *Engine) RunWithTimeout(fn func() (any, error), timeout time.Duration) (*Result, error) {
	return e.inner.RunWithTimeout(fn, timeout)
}

// Snapshot returns a copy of the engine's runtime metrics (v0.9: enriched).
func (e *Engine) Snapshot() Metrics {
	return e.inner.Snapshot()
}
