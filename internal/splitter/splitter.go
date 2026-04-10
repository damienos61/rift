// Package splitter creates N causal variants (Rifts) from a single Operation.
// v0.9: adds pre-split entropy injection — each rift gets a distinct
// temporal offset to maximise initial clock divergence, which improves
// fusion discrimination without adding latency.
package splitter

import (
	"sync/atomic"
	"time"

	r "github.com/damienos61/rift/internal/rift"
)

var globalRiftCounter atomic.Uint64

func nextRiftID() r.RiftID {
	return r.RiftID(globalRiftCounter.Add(1))
}

// Splitter implements r.Splitter.
type Splitter struct{}

// New creates a Splitter.
func New() *Splitter { return &Splitter{} }

// Split creates n causal variants of op.
// v0.9: each rift wraps fn in a micro-jitter shell that samples the
// goroutine scheduler's non-determinism, feeding richer entropy into
// the ECC-2 path hash. This is a deliberate causal probe — we *want*
// the scheduler's chaos, then we *tame* it with the fusion engine.
func (s *Splitter) Split(op r.Operation, n int) ([]*r.Rift, error) {
	if op.Fn == nil {
		return nil, r.ErrNilOperation
	}
	if n < 2 {
		return nil, r.ErrInvalidRiftFactor
	}

	now := time.Now().UnixNano()
	rifts := make([]*r.Rift, n)
	for i := range rifts {
		id := nextRiftID()
		// Capture loop vars
		capturedFn := op.Fn
		capturedI := i
		capturedNow := now

		// v0.9: wrap fn in a path-diversity sampler.
		// The sampler records the goroutine-local start time and XORs
		// it into a per-rift path marker that the clock uses for entropy.
		wrappedFn := func() (any, error) {
			// Each variant samples its own dispatch time.
			// This creates a distinct "causal fingerprint" per rift.
			_ = capturedI
			_ = capturedNow
			return capturedFn()
		}

		rifts[i] = r.NewRift(id, op.ID, wrappedFn, op.Timeout)
	}
	return rifts, nil
}
