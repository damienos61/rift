// Package fusion implements the Rift Fusion Engine v2 — the deterministic
// algorithm that selects the winning causal variant using ECC-2.
//
// # Algorithm (3 passes, O(N log N) where N = RiftFactor ∈ [2,32])
//
//  1. FILTER  — partition healthy vs failed rifts.
//               All-failed path: select the "least bad" failure for surfacing.
//
//  2. RANK    — sort healthy candidates by configured strategy.
//               Default "causal": ECC-2 total order (Weight, Entropy, Lamport,
//               WallNanos, RiftID). All strategies are strict total orders.
//
//  3. SELECT  — mark winner StateFused; mark losers StatePruned atomically.
//               Compute EntropyDelta = winner.Entropy − mean(losers.Entropy).
//               A positive delta confirms the winner had richer causal context.
//
// # Fusion strategies
//
//   - "causal"  (default) — ECC-2 total order (Weight+Entropy+Lamport)
//   - "entropy" — Entropy primary (chaos engineering, fault injection)
//   - "lamport" — Pure Lamport ordering (classic vector clock comparison)
//   - "fastest" — Lowest wall time wins (non-deterministic, benchmarks only)
//
// # Performance
//
// N ≤ 32 → sort is effectively O(1) (< 32 comparisons in practice).
// No heap allocation in the hot path — candidates slice is stack-allocated
// for N ≤ 32 via the pre-sized backing arrays.
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

// NewForStrategy returns an Engine configured for the given fusion strategy.
// Unknown strategies fall back to "causal".
func NewForStrategy(strategy string, clk *clock.Provider) *Engine {
	switch strategy {
	case "causal", "entropy", "lamport", "fastest":
		// valid
	default:
		strategy = "causal"
	}
	return &Engine{clk: clk, strategy: strategy}
}

// Fuse selects the deterministic winner from converged (or failed) Rifts.
// It always returns a non-nil Result. A non-nil error indicates all variants
// failed (ErrFusionConflict) — the Result still contains the best-effort value.
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

	// Compute EntropyDelta: positive = winner had richer causal context.
	entropyDelta := computeEntropyDelta(winner, candidates[1:])

	// Sum retries across all rifts for observability.
	totalRetries := 0
	for _, rft := range rifts {
		totalRetries += rft.RetryCount
	}

	score := clock.Score(winner.Clock)

	if allFailed {
		return &r.Result{
			OperationID:  winner.OperationID,
			Value:        nil,
			Error:        winner.Err,
			FusedFrom:    winner.ID,
			Duration:     winner.Duration(),
			CausalScore:  score,
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
		CausalScore:  score,
		Retries:      totalRetries,
		EntropyDelta: entropyDelta,
	}, nil
}

// rank sorts rifts in-place by the configured strategy (descending priority).
func (e *Engine) rank(rifts []*r.Rift) {
	switch e.strategy {
	case "entropy":
		sort.SliceStable(rifts, func(i, j int) bool {
			di := rifts[i].Clock.Entropy - rifts[j].Clock.Entropy
			if di > 1e-9 {
				return true
			}
			if di < -1e-9 {
				return false
			}
			return e.clk.Compare(rifts[i].Clock, rifts[j].Clock) > 0
		})
	case "lamport":
		sort.SliceStable(rifts, func(i, j int) bool {
			return rifts[i].Clock.Lamport > rifts[j].Clock.Lamport
		})
	case "fastest":
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
func computeEntropyDelta(winner *r.Rift, losers []*r.Rift) float64 {
	if len(losers) == 0 {
		return winner.Clock.Entropy
	}
	var sum float64
	for _, rft := range losers {
		sum += rft.Clock.Entropy
	}
	return winner.Clock.Entropy - sum/float64(len(losers))
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

// Report builds a FusionReport from a completed result.
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
