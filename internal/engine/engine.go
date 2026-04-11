// Package engine is the orchestrator of the Rift Execution Model v1.3.0.
//
// # v1.3.0 additions
//
//   - Load-shedder: rejects excess operations when the pool is saturated
//   - Warmup: pre-heats the goroutine pool on engine creation
//   - HealthProbe: liveness/readiness API for Kubernetes health checks
//   - ShedCount tracking in Metrics
//   - Atomic float64 entropy stats using CAS (no mutex)
//   - All circuit breaker and adaptive factor code preserved from v0.9
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

func nextOpID() r.OperationID { return r.OperationID(opCounter.Add(1)) }

// ─── Circuit breaker ──────────────────────────────────────────────────────────

type circuitState uint8

const (
	circuitClosed   circuitState = iota
	circuitOpen
	circuitHalfOpen
)

type circuitBreaker struct {
	cfg      r.CircuitBreakerConfig
	mu       sync.Mutex
	state    circuitState
	window   []bool
	head     int
	openedAt time.Time
}

func newCircuitBreaker(cfg r.CircuitBreakerConfig) *circuitBreaker {
	w := cfg.Window
	if w <= 0 {
		w = 100
	}
	return &circuitBreaker{cfg: cfg, state: circuitClosed, window: make([]bool, w)}
}

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
			return true
		}
		return false
	default:
		return true
	}
}

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
	failures := 0
	for _, ok := range cb.window {
		if !ok {
			failures++
		}
	}
	if float64(failures)/float64(len(cb.window)) >= cb.cfg.Threshold {
		cb.state = circuitOpen
		cb.openedAt = time.Now()
	}
}

func (cb *circuitBreaker) currentState() circuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// ─── Adaptive factor ──────────────────────────────────────────────────────────

type adaptiveFactor struct {
	cfg    r.AdaptiveConfig
	mu     sync.Mutex
	factor int
	window []bool
	head   int
}

func newAdaptiveFactor(cfg r.AdaptiveConfig, initial int) *adaptiveFactor {
	return &adaptiveFactor{cfg: cfg, factor: initial, window: make([]bool, 20)}
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

// ─── Load shedder (v1.3.0) ───────────────────────────────────────────────────

// shedder rejects excess operations when the engine is saturated.
// It uses a token-bucket backed by a buffered channel.
type shedder struct {
	cfg    r.ShedPolicy
	tokens chan struct{}
}

func newShedder(cfg r.ShedPolicy) *shedder {
	cap := cfg.MaxQueueLen
	if cap <= 0 {
		cap = 1000
	}
	s := &shedder{cfg: cfg, tokens: make(chan struct{}, cap)}
	// Pre-fill tokens
	for i := 0; i < cap; i++ {
		s.tokens <- struct{}{}
	}
	return s
}

// acquire attempts to acquire a shed token.
// Returns true (proceed) or false (shed this operation).
func (s *shedder) acquire() bool {
	if !s.cfg.Enabled {
		return true
	}
	select {
	case <-s.tokens:
		return true
	default:
		return false // token pool exhausted → shed
	}
}

// release returns a token after an operation completes.
func (s *shedder) release() {
	if !s.cfg.Enabled {
		return
	}
	select {
	case s.tokens <- struct{}{}:
	default:
		// Pool full — discard (happens if shed was never acquired).
	}
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
	shedder  *shedder
	latency  latencyHistogram

	totalOps    atomic.Uint64
	successOps  atomic.Uint64
	failedOps   atomic.Uint64
	shedOps     atomic.Uint64 // v1.3.0
	totalRifts  atomic.Uint64
	prunedRifts atomic.Uint64

	entropySum   atomic.Uint64
	entropyCount atomic.Uint64
}

// New creates a RiftEngine, validates config, and optionally warms up the pool.
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

	e := &RiftEngine{
		cfg:      cfg,
		splitter: splitter.New(),
		executor: exec,
		fusion:   fus,
		clock:    clk,
		circuit:  newCircuitBreaker(cfg.CircuitBreaker),
		adaptive: newAdaptiveFactor(cfg.Adaptive, cfg.RiftFactor),
		shedder:  newShedder(cfg.Shed),
	}

	// v1.3.0: warm up the goroutine pool if configured.
	if cfg.Warmup.Enabled {
		if err := e.warmup(cfg.Warmup); err != nil {
			return nil, err
		}
	}

	return e, nil
}

// warmup runs no-op operations to pre-heat the goroutine pool.
func (e *RiftEngine) warmup(cfg r.WarmupConfig) error {
	ops := cfg.Ops
	if ops <= 0 {
		ops = 10
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < ops; i++ {
			_, err := e.Run(func() (any, error) { return nil, nil })
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return r.ErrWarmupTimeout
	}
}

// Run executes fn through the Rift Execution Model.
func (e *RiftEngine) Run(fn func() (any, error)) (*r.Result, error) {
	return e.RunWithTimeout(fn, 0)
}

// RunWithTimeout is like Run but overrides the engine's DefaultTimeout.
func (e *RiftEngine) RunWithTimeout(fn func() (any, error), timeout time.Duration) (*r.Result, error) {
	if fn == nil {
		return nil, r.ErrNilOperation
	}

	// v1.3.0: load shedding check
	if !e.shedder.acquire() {
		e.shedOps.Add(1)
		if e.cfg.Telemetry != nil {
			e.cfg.Telemetry.OnShed(nextOpID())
		}
		return nil, r.ErrShed
	}
	defer e.shedder.release()

	// Circuit breaker check
	if !e.circuit.allow() {
		return nil, r.ErrCircuitOpen
	}

	op := r.Operation{ID: nextOpID(), Fn: fn, Timeout: timeout}

	factor := e.cfg.RiftFactor
	if e.cfg.Adaptive.Enabled {
		factor = e.adaptive.current()
	}

	start := time.Now()

	// 1. Split
	rifts, err := e.splitter.Split(op, factor)
	if err != nil {
		return nil, err
	}
	e.totalOps.Add(1)
	e.totalRifts.Add(uint64(len(rifts)))

	if e.cfg.Telemetry != nil {
		e.cfg.Telemetry.OnSplit(op.ID, factor)
	}

	// 2. Execute
	if err := e.executor.Execute(rifts); err != nil {
		return nil, err
	}

	// 3. Fuse
	result, fuseErr := e.fusion.Fuse(rifts)
	if fuseErr != nil && fuseErr != r.ErrFusionConflict {
		return nil, fuseErr
	}

	// Update metrics
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
	e.recordEntropy(result.EntropyDelta)

	if e.cfg.Telemetry != nil {
		if !success {
			e.cfg.Telemetry.OnError(op.ID, result.Error)
		}
		e.cfg.Telemetry.OnFuse(op.ID, result.FusedFrom, result.CausalScore, len(rifts)-1)
	}

	return result, result.Error
}

// recordEntropy accumulates entropy delta using lock-free CAS on float bits.
func (e *RiftEngine) recordEntropy(delta float64) {
	for {
		old := e.entropySum.Load()
		newBits := math.Float64bits(math.Float64frombits(old) + delta)
		if e.entropySum.CompareAndSwap(old, newBits) {
			e.entropyCount.Add(1)
			return
		}
	}
}

// ─── Health probe (v1.3.0) ────────────────────────────────────────────────────

// HealthStatus is the result of a health probe.
type HealthStatus struct {
	Live    bool   // true = engine is running and accepting ops
	Ready   bool   // true = circuit closed and error rate below 50%
	Reason  string // human-readable explanation when not ready
}

// Health returns a liveness/readiness snapshot (Kubernetes-ready).
func (e *RiftEngine) Health() HealthStatus {
	hs := HealthStatus{Live: true}
	m := e.Snapshot()
	if m.CircuitState != "closed" {
		hs.Ready = false
		hs.Reason = "circuit breaker " + m.CircuitState
		return hs
	}
	if m.ErrorRate > 0.5 {
		hs.Ready = false
		hs.Reason = "error rate too high"
		return hs
	}
	hs.Ready = true
	return hs
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

// Metrics holds a snapshot of engine counters.
type Metrics struct {
	TotalOperations   uint64
	SuccessfulOps     uint64
	FailedOps         uint64
	ShedOps           uint64  // v1.3.0
	TotalRiftsSpawned uint64
	TotalRiftsPruned  uint64
	RiftFactor        int
	ActiveRiftFactor  int
	FusionStrategy    string
	MeanLatencyMs     float64
	MeanEntropyDelta  float64
	CircuitState      string
	ErrorRate         float64
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
		meanEntropy = math.Float64frombits(e.entropySum.Load()) / float64(ec)
	}

	stateStr := "closed"
	switch e.circuit.currentState() {
	case circuitOpen:
		stateStr = "open"
	case circuitHalfOpen:
		stateStr = "half-open"
	}

	return Metrics{
		TotalOperations:   total,
		SuccessfulOps:     e.successOps.Load(),
		FailedOps:         failed,
		ShedOps:           e.shedOps.Load(),
		TotalRiftsSpawned: e.totalRifts.Load(),
		TotalRiftsPruned:  e.prunedRifts.Load(),
		RiftFactor:        e.cfg.RiftFactor,
		ActiveRiftFactor:  e.adaptive.current(),
		FusionStrategy:    e.cfg.FusionStrategy,
		MeanLatencyMs:     e.latency.meanMs(),
		MeanEntropyDelta:  meanEntropy,
		CircuitState:      stateStr,
		ErrorRate:         errorRate,
	}
}

// Config returns the engine's active configuration.
func (e *RiftEngine) Config() r.Config { return e.cfg }
