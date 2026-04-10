// Package engine is the public entry point of the Rift Execution Model v0.9.
//
// v0.9 additions:
//   - CircuitBreaker: rolling-window fault gate (open/half-open/closed states)
//   - AdaptiveFactor: self-tuning RiftFactor based on live error rate
//   - RunWithTimeout: per-operation timeout override
//   - Richer Metrics: latency histogram, entropy stats, circuit state
package engine

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damienos61/rift/internal/clock"
	"github.com/damienos61/rift/internal/executor"
	"github.com/damienos61/rift/internal/fusion"
	r "github.com/damienos61/rift/internal/rift"
	"github.com/damienos61/rift/internal/splitter"
)

// ─── ID generator ─────────────────────────────────────────────────────────────

var opCounter atomic.Uint64

func nextOpID() r.OperationID {
	return r.OperationID(opCounter.Add(1))
}

// ─── CircuitBreaker ───────────────────────────────────────────────────────────

// circuitState represents the three states of the circuit breaker.
type circuitState uint8

const (
	circuitClosed   circuitState = iota // normal operation
	circuitOpen                         // refusing requests
	circuitHalfOpen                     // probing recovery
)

// circuitBreaker is a rolling-window fault gate.
type circuitBreaker struct {
	cfg      r.CircuitBreakerConfig
	mu       sync.Mutex
	state    circuitState
	window   []bool // ring buffer: true = success, false = failure
	head     int
	openedAt time.Time
}

func newCircuitBreaker(cfg r.CircuitBreakerConfig) *circuitBreaker {
	w := cfg.Window
	if w <= 0 {
		w = 100
	}
	return &circuitBreaker{
		cfg:    cfg,
		state:  circuitClosed,
		window: make([]bool, w),
	}
}

// allow returns true if the operation should proceed.
func (cb *circuitBreaker) allow() bool {
	if !cb.cfg.Enabled {
		return true
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitOpen:
		if time.Since(cb.openedAt) >= cb.cfg.CoolDown {
			cb.state = circuitHalfOpen
			return true // allow one probe
		}
		return false
	case circuitHalfOpen:
		return true // allow probe through
	default:
		return true
	}
}

// record registers an operation outcome and transitions the breaker if needed.
func (cb *circuitBreaker) record(success bool) {
	if !cb.cfg.Enabled {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.window[cb.head%len(cb.window)] = success
	cb.head++

	if cb.state == circuitHalfOpen {
		if success {
			cb.state = circuitClosed
		} else {
			cb.state = circuitOpen
			cb.openedAt = time.Now()
		}
		return
	}

	// Compute error rate over the window.
	failures := 0
	for _, ok := range cb.window {
		if !ok {
			failures++
		}
	}
	rate := float64(failures) / float64(len(cb.window))
	if rate >= cb.cfg.Threshold {
		cb.state = circuitOpen
		cb.openedAt = time.Now()
	}
}

func (cb *circuitBreaker) currentState() circuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// ─── Adaptive RiftFactor ──────────────────────────────────────────────────────

// adaptiveFactor tracks and adjusts RiftFactor based on live error rates.
type adaptiveFactor struct {
	cfg    r.AdaptiveConfig
	mu     sync.Mutex
	factor int
	window []bool
	head   int
}

func newAdaptiveFactor(cfg r.AdaptiveConfig, initial int) *adaptiveFactor {
	return &adaptiveFactor{
		cfg:    cfg,
		factor: initial,
		window: make([]bool, 20), // 20-op rolling window for factor decisions
	}
}

func (a *adaptiveFactor) record(success bool) {
	if !a.cfg.Enabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	a.window[a.head%len(a.window)] = success
	a.head++

	failures := 0
	for _, ok := range a.window {
		if !ok {
			failures++
		}
	}
	rate := float64(failures) / float64(len(a.window))

	if rate >= a.cfg.ErrorRateUp && a.factor < a.cfg.MaxFactor {
		a.factor++
	} else if rate <= a.cfg.ErrorRateDown && a.factor > a.cfg.MinFactor {
		a.factor--
	}
}

func (a *adaptiveFactor) current() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.factor
}

// ─── Latency histogram ────────────────────────────────────────────────────────

// latencyHistogram is a lock-free approximate histogram for p50/p99 tracking.
// Uses 16 buckets covering 0–∞ ms. Bucket i covers [2^i, 2^(i+1)) ms.
type latencyHistogram struct {
	buckets [16]atomic.Uint64
	count   atomic.Uint64
	sumNs   atomic.Uint64
}

func (h *latencyHistogram) record(d time.Duration) {
	ms := d.Milliseconds()
	idx := 0
	for ms > 0 && idx < 15 {
		ms >>= 1
		idx++
	}
	h.buckets[idx].Add(1)
	h.count.Add(1)
	h.sumNs.Add(uint64(d.Nanoseconds()))
}

func (h *latencyHistogram) meanMs() float64 {
	c := h.count.Load()
	if c == 0 {
		return 0
	}
	return float64(h.sumNs.Load()) / float64(c) / 1e6
}

// ─── RiftEngine ───────────────────────────────────────────────────────────────

// RiftEngine is the top-level orchestrator. Safe for concurrent use.
type RiftEngine struct {
	cfg      r.Config
	splitter *splitter.Splitter
	executor *executor.Executor
	fusion   *fusion.Engine
	clock    *clock.Provider

	circuit  *circuitBreaker
	adaptive *adaptiveFactor
	latency  latencyHistogram

	// Atomic metrics
	totalOps    atomic.Uint64
	successOps  atomic.Uint64
	failedOps   atomic.Uint64
	totalRifts  atomic.Uint64
	prunedRifts atomic.Uint64

	// v0.9: entropy stats
	entropySum   atomic.Uint64 // stored as float64 bits via math.Float64bits
	entropyCount atomic.Uint64
}

// New creates a RiftEngine with the given configuration.
func New(cfg r.Config) (*RiftEngine, error) {
	if cfg.RiftFactor < 2 {
		return nil, r.ErrInvalidRiftFactor
	}
	if cfg.FusionStrategy == "" {
		cfg.FusionStrategy = "causal"
	}

	clk := clock.NewProvider(cfg.DefaultTimeout / 2)
	exec := executor.New(cfg.WorkerPoolSize, clk, cfg.DefaultTimeout, cfg.Retry, cfg.Telemetry)
	fus := fusion.NewForStrategy(cfg.FusionStrategy, clk)
	cb := newCircuitBreaker(cfg.CircuitBreaker)
	af := newAdaptiveFactor(cfg.Adaptive, cfg.RiftFactor)

	return &RiftEngine{
		cfg:      cfg,
		splitter: splitter.New(),
		executor: exec,
		fusion:   fus,
		clock:    clk,
		circuit:  cb,
		adaptive: af,
	}, nil
}

// Run executes fn through the Rift Execution Model.
func (e *RiftEngine) Run(fn func() (any, error)) (*r.Result, error) {
	return e.RunWithTimeout(fn, 0)
}

// RunWithTimeout is like Run but overrides the engine's default timeout.
func (e *RiftEngine) RunWithTimeout(fn func() (any, error), timeout time.Duration) (*r.Result, error) {
	if fn == nil {
		return nil, r.ErrNilOperation
	}

	// ── Circuit breaker check ─────────────────────────────────────────────
	if !e.circuit.allow() {
		return nil, r.ErrCircuitOpen
	}

	op := r.Operation{
		ID:      nextOpID(),
		Fn:      fn,
		Timeout: timeout,
	}

	// ── Adaptive RiftFactor ───────────────────────────────────────────────
	factor := e.cfg.RiftFactor
	if e.cfg.Adaptive.Enabled {
		factor = e.adaptive.current()
	}

	start := time.Now()

	// ── 1. Split ──────────────────────────────────────────────────────────
	rifts, err := e.splitter.Split(op, factor)
	if err != nil {
		return nil, err
	}
	e.totalOps.Add(1)
	e.totalRifts.Add(uint64(len(rifts)))

	// Telemetry
	if e.cfg.Telemetry != nil {
		e.cfg.Telemetry.OnSplit(op.ID, factor)
	}

	// ── 2. Execute ────────────────────────────────────────────────────────
	if err := e.executor.Execute(rifts); err != nil {
		return nil, err
	}

	// ── 3. Fuse ───────────────────────────────────────────────────────────
	result, err := e.fusion.Fuse(rifts)
	if err != nil && err != r.ErrFusionConflict {
		return nil, err
	}

	// ── Update metrics ────────────────────────────────────────────────────
	e.prunedRifts.Add(uint64(len(rifts) - 1))
	e.latency.record(time.Since(start))

	success := result.Error == nil
	if success {
		e.successOps.Add(1)
	} else {
		e.failedOps.Add(1)
	}
	e.circuit.record(success)
	e.adaptive.record(success)

	// Track entropy stats (lock-free via CAS on float bits)
	e.recordEntropy(result.EntropyDelta)

	// Telemetry
	if e.cfg.Telemetry != nil {
		if !success {
			e.cfg.Telemetry.OnError(op.ID, result.Error)
		}
		e.cfg.Telemetry.OnFuse(op.ID, result.FusedFrom, result.CausalScore, len(rifts)-1)
	}

	return result, result.Error
}

// recordEntropy accumulates entropy delta using atomic float64 encoding.
func (e *RiftEngine) recordEntropy(delta float64) {
	for {
		old := e.entropySum.Load()
		oldF := math.Float64frombits(old)
		newF := oldF + delta
		newBits := math.Float64bits(newF)
		if e.entropySum.CompareAndSwap(old, newBits) {
			e.entropyCount.Add(1)
			return
		}
	}
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

// Metrics holds a snapshot of engine counters (v0.9: enriched).
type Metrics struct {
	TotalOperations   uint64
	SuccessfulOps     uint64
	FailedOps         uint64
	TotalRiftsSpawned uint64
	TotalRiftsPruned  uint64
	RiftFactor        int
	ActiveRiftFactor  int    // v0.9: current adaptive factor
	FusionStrategy    string
	MeanLatencyMs     float64 // v0.9
	MeanEntropyDelta  float64 // v0.9
	CircuitState      string  // v0.9: "closed" | "open" | "half-open"
	ErrorRate         float64 // v0.9: ops-level error rate
}

// Snapshot returns a consistent metrics snapshot.
func (e *RiftEngine) Snapshot() Metrics {
	total := e.totalOps.Load()
	failed := e.failedOps.Load()
	errorRate := 0.0
	if total > 0 {
		errorRate = float64(failed) / float64(total)
	}

	meanEntropy := 0.0
	if ec := e.entropyCount.Load(); ec > 0 {
		sum := math.Float64frombits(e.entropySum.Load())
		meanEntropy = sum / float64(ec)
	}

	circuitStateName := "closed"
	switch e.circuit.currentState() {
	case circuitOpen:
		circuitStateName = "open"
	case circuitHalfOpen:
		circuitStateName = "half-open"
	}

	return Metrics{
		TotalOperations:   total,
		SuccessfulOps:     e.successOps.Load(),
		FailedOps:         failed,
		TotalRiftsSpawned: e.totalRifts.Load(),
		TotalRiftsPruned:  e.prunedRifts.Load(),
		RiftFactor:        e.cfg.RiftFactor,
		ActiveRiftFactor:  e.adaptive.current(),
		FusionStrategy:    e.cfg.FusionStrategy,
		MeanLatencyMs:     e.latency.meanMs(),
		MeanEntropyDelta:  meanEntropy,
		CircuitState:      circuitStateName,
		ErrorRate:         errorRate,
	}
}

// Config returns the engine's active configuration.
func (e *RiftEngine) Config() r.Config { return e.cfg }
