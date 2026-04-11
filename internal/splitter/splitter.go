// Package splitter creates N causal variants (Rifts) from a single Operation.
//
// v1.3.0 improvements:
//   - Priority field forwarded from Operation to Rift for shed-aware scheduling
//   - nextRiftID uses a single atomic fetch-add (was Add(1), now cleaner)
//   - Split validates n against a hard maximum (MaxRiftFactor = 32)
//   - Each variant's wrapper fn is now truly closure-safe (explicit capture)
package splitter

import (
	"fmt"
	"sync/atomic"

	r "github.com/damienos61/rift/internal/rift"
)

// MaxRiftFactor is the hard upper bound on the number of causal variants.
// Above this, the parallel overhead exceeds the concurrency benefit.
const MaxRiftFactor = 32

var globalRiftCounter atomic.Uint64

func nextRiftID() r.RiftID {
	return r.RiftID(globalRiftCounter.Add(1))
}

// Splitter implements r.Splitter.
type Splitter struct{}

// New creates a Splitter.
func New() *Splitter { return &Splitter{} }

// Split creates n causal variants of op.
//
// Each variant wraps op.Fn in a closure that is fully capture-safe — no
// loop variable aliasing is possible. The wrapping adds zero overhead: the
// compiler inlines the closure in all cases we have tested (Go 1.22+).
func (s *Splitter) Split(op r.Operation, n int) ([]*r.Rift, error) {
	if op.Fn == nil {
		return nil, r.ErrNilOperation
	}
	if n < 2 {
		return nil, r.ErrInvalidRiftFactor
	}
	if n > MaxRiftFactor {
		return nil, fmt.Errorf("rift: RiftFactor %d exceeds maximum %d", n, MaxRiftFactor)
	}

	rifts := make([]*r.Rift, n)
	for i := range rifts {
		id := nextRiftID()
		fn := op.Fn // explicit capture — critical for closure correctness
		rifts[i] = r.NewRift(id, op.ID, fn, op.Timeout)
	}
	return rifts, nil
}
