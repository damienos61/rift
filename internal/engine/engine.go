// Package engine is the public entry point of the Rift Execution Model.
// It wires together the Splitter, Executor, and FusionEngine into a single
// coherent API: submit an Operation, receive a deterministic Result.
package engine

import (
	"sync/atomic"
	"time"

	"github.com/rift-engine/rift/internal/clock"
	"github.com/rift-engine/rift/internal/executor"
	"github.com/rift-engine/rift/internal/fusion"
	r "github.com/rift-engine/rift/internal/rift"
	"github.com/rift-engine/rift/internal/splitter"
)

// ─── ID generator ─────────────────────────────────────────────────────────────

var opCounter atomic.Uint64

func nextOpID() r.OperationID {
	return r.OperationID(opCounter.Add(1))
}

// ─── RiftEngine ───────────────────────────────────────────────────────────────

// RiftEngine is the top-level orchestrator. It accepts Operations and returns
// deterministic Results, shielding the caller from all concurrency concerns.
//
// RiftEngine is safe for concurrent use from multiple goroutines.
type RiftEngine struct {
	cfg      r.Config
	splitter *splitter.Splitter
	executor *executor.Executor
	fusion   *fusion.Engine
	clock    *clock.Provider

	// Metrics (atomic counters — no mutex needed).
	totalOps     atomic.Uint64
	successOps   atomic.Uint64
	failedOps    atomic.Uint64
	totalRifts   atomic.Uint64
	prunedRifts  atomic.Uint64
}

// New creates a RiftEngine with the given configuration.
// Use rift.DefaultConfig() for a production-safe starting point.
func New(cfg r.Config) (*RiftEngine, error) {
	if cfg.RiftFactor < 2 {
		return nil, r.ErrInvalidRiftFactor
	}
	if cfg.FusionStrategy == "" {
		cfg.FusionStrategy = "causal"
	}

	clk := clock.NewProvider(cfg.DefaultTimeout / 2)
	exec := executor.New(cfg.WorkerPoolSize, clk, cfg.DefaultTimeout)
	fus := fusion.NewForStrategy(cfg.FusionStrategy, clk)

	return &RiftEngine{
		cfg:      cfg,
		splitter: splitter.New(),
		executor: exec,
		fusion:   fus,
		clock:    clk,
	}, nil
}

// Run executes fn using the Rift Execution Model and returns a deterministic
// Result. It is the single API method callers need.
//
//	result, err := engine.Run(func() (any, error) {
//	    return myService.ProcessOrder(orderID)
//	})
func (e *RiftEngine) Run(fn func() (any, error)) (*r.Result, error) {
	return e.RunWithTimeout(fn, 0)
}

// RunWithTimeout is like Run but overrides the engine's default timeout
// for this specific operation.
func (e *RiftEngine) RunWithTimeout(fn func() (any, error), timeout time.Duration) (*r.Result, error) {
	if fn == nil {
		return nil, r.ErrNilOperation
	}

	op := r.Operation{
		ID:      nextOpID(),
		Fn:      fn,
		Timeout: timeout,
	}

	// ── 1. Split ──────────────────────────────────────────────────────────
	rifts, err := e.splitter.Split(op, e.cfg.RiftFactor)
	if err != nil {
		return nil, err
	}
	e.totalOps.Add(1)
	e.totalRifts.Add(uint64(len(rifts)))

	// ── 2. Execute (parallel, no global locks) ────────────────────────────
	if err := e.executor.Execute(rifts); err != nil {
		return nil, err
	}

	// ── 3. Fuse (deterministic, O(N log N) where N = RiftFactor) ─────────
	result, err := e.fusion.Fuse(rifts)
	if err != nil && err != r.ErrFusionConflict {
		return nil, err
	}

	// Update metrics.
	pruned := uint64(len(rifts) - 1)
	e.prunedRifts.Add(pruned)
	if result.Error == nil {
		e.successOps.Add(1)
	} else {
		e.failedOps.Add(1)
	}

	return result, result.Error
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

// Metrics holds a snapshot of engine counters.
type Metrics struct {
	TotalOperations  uint64
	SuccessfulOps    uint64
	FailedOps        uint64
	TotalRiftsSpawned uint64
	TotalRiftsPruned  uint64
	RiftFactor       int
	FusionStrategy   string
}

// Snapshot returns a consistent metrics snapshot (atomic reads).
func (e *RiftEngine) Snapshot() Metrics {
	return Metrics{
		TotalOperations:   e.totalOps.Load(),
		SuccessfulOps:     e.successOps.Load(),
		FailedOps:         e.failedOps.Load(),
		TotalRiftsSpawned: e.totalRifts.Load(),
		TotalRiftsPruned:  e.prunedRifts.Load(),
		RiftFactor:        e.cfg.RiftFactor,
		FusionStrategy:    e.cfg.FusionStrategy,
	}
}

// Config returns the engine's active configuration.
func (e *RiftEngine) Config() r.Config { return e.cfg }
