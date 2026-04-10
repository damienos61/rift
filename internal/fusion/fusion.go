// Package fusion implements the Rift Fusion Engine v2 — the deterministic
// algorithm that selects the winning causal variant using ECC-2.
//
// # Fusion Algorithm v2 (3 passes, same structure, richer ranking)
//
//  1. FILTER  — partition healthy vs failed rifts.
//               If all failed: best-effort error surfacing.
//
//  2. RANK    — sort healthy rifts by ECC-2 total order:
//               (Weight × EntropyBonus) → Lamport → WallNanos → RiftID.
//               This is O(N log N) but N ≤ 8 (RiftFactor), so O(1) in practice.
//
//  3. SELECT  — winner marked StateFused; losers marked StatePruned atomically.
//               EntropyDelta is computed as winner.Entropy − mean(losers.Entropy).
//
// # Entropy-weighted fusion strategy (v0.9 "entropy" mode)
//
// When FusionStrategy = "entropy", the ranking uses Entropy as the
// primary dimension instead of Weight. This mode maximises causal path
// diversity — useful for fault-injection testing and chaos engineering.
//
// # Performance
//
// O(N log N) sort where N = RiftFactor ∈ [2,8].
// In practice: 3 rifts → 3-element sort → ~2 comparisons → O(1).
// No allocation in the hot path (candidates slice is pre-allocated).
package fusion

import (
	"sort"

	"github.com/damienos61/rift/internal/clock"
	r "github.com/damienos61/rift/internal/rift"
)

// Engine implements r.FusionEngine.
type Engine struct {
	clk      *clock.Provider
	strategy string
}

// New creates a fusion Engine with the default "causal" strategy.
func New(clk *clock.Provider) *Engine {
	return &Engine{clk: clk, strategy: "causal"}
}

// NewForStrategy returns an Engine configured for the given strategy.
func NewForStrategy(strategy string, clk *clock.Provider) *Engine {
	if strategy == "" {
		strategy = "causal"
	}
	return &Engine{clk: clk, strategy: strategy}
}

// Fuse selects the deterministic winner from converged (or failed) Rifts.
func (e *Engine) Fuse(rifts []*r.Rift) (*r.Result, error) {
	if len(rifts) == 0 {
		return nil, r.ErrNoRiftsConverged
	}

	// ── Pass 1: partition ─────────────────────────────────────────────────
	healthy := make([]*r.Rift, 0, len(rifts))
	failed := make([]*r.Rift, 0, len(rifts))
	for _, rft := range rifts {
		if rft.IsHealthy() {
			healthy = append(healthy, rft)
		} else {
			failed = append(failed, rft)
		}
	}

	candidates := healthy
	allFailed := len(candidates) == 0
	if allFailed {
		candidates = failed
	}

	// ── Pass 2: rank ──────────────────────────────────────────────────────
	e.rank(candidates)

	// ── Pass 3: select + prune ────────────────────────────────────────────
	winner := candidates[0]
	winner.Transition(r.StateFused)
	for _, rft := range candidates[1:] {
		rft.Transition(r.StatePruned)
	}
	for _, rft := range failed {
		if rft != winner {
			rft.Transition(r.StatePruned)
		}
	}

	// v0.9: compute EntropyDelta = winner entropy - mean(loser entropy)
	entropyDelta := computeEntropyDelta(winner, candidates[1:])

	// v0.9: sum retries across all rifts for observability
	totalRetries := 0
	for _, rft := range rifts {
		totalRetries += rft.RetryCount
	}

	if allFailed {
		return &r.Result{
			OperationID:  winner.OperationID,
			Value:        nil,
			Error:        winner.Err,
			FusedFrom:    winner.ID,
			Duration:     winner.Duration(),
			CausalScore:  clock.Score(winner.Clock),
			Retries:      totalRetries,
			EntropyDelta: entropyDelta,
		}, r.ErrFusionConflict
	}

	return &r.Result{
		OperationID:  winner.OperationID,
		Value:        winner.Result,
		Error:        nil,
		FusedFrom:    winner.ID,
		Duration:     winner.Duration(),
		CausalScore:  clock.Score(winner.Clock),
		Retries:      totalRetries,
		EntropyDelta: entropyDelta,
	}, nil
}

// rank sorts rifts in-place by the configured strategy.
func (e *Engine) rank(rifts []*r.Rift) {
	switch e.strategy {
	case "entropy":
		// Primary: Entropy (maximise path diversity)
		sort.SliceStable(rifts, func(i, j int) bool {
			if diff := rifts[i].Clock.Entropy - rifts[j].Clock.Entropy; diff != 0 {
				return diff > 0
			}
			return e.clk.Compare(rifts[i].Clock, rifts[j].Clock) > 0
		})
	case "lamport":
		// Pure Lamport ordering (classic, for comparison)
		sort.SliceStable(rifts, func(i, j int) bool {
			return rifts[i].Clock.Lamport > rifts[j].Clock.Lamport
		})
	case "fastest":
		// First-to-finish: lowest WallNanos wins (non-deterministic, benchmarks only)
		sort.SliceStable(rifts, func(i, j int) bool {
			return rifts[i].Clock.WallNanos < rifts[j].Clock.WallNanos
		})
	default: // "causal" — ECC-2 total order
		sort.SliceStable(rifts, func(i, j int) bool {
			return e.clk.Compare(rifts[i].Clock, rifts[j].Clock) > 0
		})
	}
}

// computeEntropyDelta returns winner.Entropy − mean(losers.Entropy).
// A positive delta means the winner explored more divergent paths than average.
func computeEntropyDelta(winner *r.Rift, losers []*r.Rift) float64 {
	if len(losers) == 0 {
		return winner.Clock.Entropy
	}
	var sum float64
	for _, rft := range losers {
		sum += rft.Clock.Entropy
	}
	mean := sum / float64(len(losers))
	return winner.Clock.Entropy - mean
}

// ─── Telemetry helpers ────────────────────────────────────────────────────────

// FusionReport holds a human-readable summary of a fusion event.
type FusionReport struct {
	Winner       r.RiftID
	Score        float64
	EntropyDelta float64
	Pruned       []r.RiftID
	Strategy     string
	Retries      int
}

// Report builds a FusionReport from a completed set of rifts and a result.
func Report(rifts []*r.Rift, result *r.Result, strategy string) FusionReport {
	pruned := make([]r.RiftID, 0, len(rifts)-1)
	for _, rft := range rifts {
		if rft.ID != result.FusedFrom {
			pruned = append(pruned, rft.ID)
		}
	}
	return FusionReport{
		Winner:       result.FusedFrom,
		Score:        result.CausalScore,
		EntropyDelta: result.EntropyDelta,
		Pruned:       pruned,
		Strategy:     strategy,
		Retries:      result.Retries,
	}
}
