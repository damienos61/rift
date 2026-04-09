// Package rift is the public API of the Rift Execution Model.
//
// Rift eliminates race conditions and Heisenbugs in concurrent Go programs
// by transforming non-determinism into a controllable resource. Every
// operation is "torn" into N causal variants (rifts) executed in parallel;
// an Extended Causal Clock algorithm then fuses them into a single
// deterministic result — without global locks and without re-execution.
//
// # Quick start
//
//	eng, _ := rift.NewEngine(rift.DefaultConfig())
//
//	result, err := eng.Run(func() (any, error) {
//	    return myService.ProcessOrder(orderID)
//	})
//
// # How it works
//
//  1. SPLIT  — the operation is cloned into RiftFactor goroutines
//  2. EXECUTE — all variants run concurrently, fully isolated
//  3. FUSE   — Extended Causal Clocks elect a deterministic winner
//
// See the README and docs/ for a full explanation of the algorithm.
package rift

import (
	"github.com/damienos61/rift/internal/engine"
	r "github.com/damienos61/rift/internal/rift"
)

// Re-export the types callers need so they never have to import internal/.

// Config controls engine behaviour. See internal/rift.Config for field docs.
type Config = r.Config

// Result is the deterministic output of a Run call.
type Result = r.Result

// DefaultConfig returns production-safe defaults.
var DefaultConfig = r.DefaultConfig

// Engine is the public handle for the Rift engine.
type Engine struct{ inner *engine.RiftEngine }

// NewEngine creates and validates a new Engine from cfg.
func NewEngine(cfg Config) (*Engine, error) {
	inner, err := engine.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{inner: inner}, nil
}

// Run executes fn through the Rift Execution Model.
// It is safe to call from multiple goroutines simultaneously.
func (e *Engine) Run(fn func() (any, error)) (*Result, error) {
	return e.inner.Run(fn)
}

// Snapshot returns a copy of the engine's runtime metrics.
func (e *Engine) Snapshot() engine.Metrics {
	return e.inner.Snapshot()
}
