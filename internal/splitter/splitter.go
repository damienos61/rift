// Package splitter creates N causal variants (Rifts) from a single Operation.
// Each variant is an independent goroutine-ready unit that will execute the
// same function in its own isolated execution context.
package splitter

import (
	"sync/atomic"

	r "github.com/damienos61/rift/internal/rift"
)

// ─── ID generator ────────────────────────────────────────────────────────────

var globalRiftCounter atomic.Uint64

func nextRiftID() r.RiftID {
	return r.RiftID(globalRiftCounter.Add(1))
}

// ─── Splitter ─────────────────────────────────────────────────────────────────

// Splitter implements r.Splitter. It produces N independent Rift instances
// from a single Operation, each ready for parallel execution.
type Splitter struct{}

// New creates a Splitter.
func New() *Splitter { return &Splitter{} }

// Split creates n causal variants of op.
// Each Rift wraps the same op.Fn; isolation is guaranteed by the executor
// (no shared mutable state is injected at split time).
func (s *Splitter) Split(op r.Operation, n int) ([]*r.Rift, error) {
	if op.Fn == nil {
		return nil, r.ErrNilOperation
	}
	if n < 2 {
		return nil, r.ErrInvalidRiftFactor
	}

	rifts := make([]*r.Rift, n)
	for i := range rifts {
		rifts[i] = r.NewRift(
			nextRiftID(),
			op.ID,
			op.Fn,
			op.Timeout,
		)
	}
	return rifts, nil
}
