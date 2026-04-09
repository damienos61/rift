// Package fusion implements the Rift Fusion Engine — the deterministic
// algorithm that selects the winning causal variant from a set of converged
// Rifts, using Extended Causal Clocks with weighted priority ordering.
//
// # The Fusion Algorithm
//
// Given N converged Rifts, fusion proceeds in three passes:
//
//  1. FILTER — discard failed rifts (if any healthy rifts exist).
//     If all rifts failed, return the one with the highest causal score
//     (best-effort error: surface the "most authoritative" failure).
//
//  2. RANK — sort healthy rifts by CausalClock score using the
//     Extended Causal Clock comparator (Weight → Lamport → Wall → ID).
//     This is a total order: no ties are possible.
//
//  3. SELECT — the top-ranked rift is the winner.
//     All others are marked StatePruned atomically.
//     The winner is marked StateFused.
//
// The algorithm is O(N log N) and entirely lock-free after convergence.
// N is the RiftFactor (default 3), so in practice it's O(1).
package fusion

import (
	"sort"

	"github.com/rift-engine/rift/internal/clock"
	r "github.com/rift-engine/rift/internal/rift"
)

// Engine implements r.FusionEngine.
type Engine struct {
	clk *clock.Provider
}

// New creates a fusion Engine.
func New(clk *clock.Provider) *Engine {
	return &Engine{clk: clk}
}

// Fuse selects the deterministic winner from a slice of converged (or failed)
// Rifts. It always returns exactly one Result; it never blocks.
func (e *Engine) Fuse(rifts []*r.Rift) (*r.Result, error) {
	if len(rifts) == 0 {
		return nil, r.ErrNoRiftsConverged
	}

	// ── Pass 1: partition healthy vs failed ───────────────────────────────
	healthy := make([]*r.Rift, 0, len(rifts))
	failed := make([]*r.Rift, 0, len(rifts))

	for _, rft := range rifts {
		if rft.IsHealthy() {
			healthy = append(healthy, rft)
		} else {
			failed = append(failed, rft)
		}
	}

	// ── Pass 2: rank the candidate pool ───────────────────────────────────
	candidates := healthy
	allFailed := false
	if len(candidates) == 0 {
		// All rifts failed — pick the best-effort one for error surfacing.
		candidates = failed
		allFailed = true
	}

	e.rankByCausalScore(candidates)

	// ── Pass 3: select winner, prune losers ───────────────────────────────
	winner := candidates[0]
	winner.Transition(r.StateFused)

	for _, rft := range candidates[1:] {
		rft.Transition(r.StatePruned)
	}
	// Pruned failed rifts too (if we had a healthy candidate).
	for _, rft := range failed {
		if rft != winner {
			rft.Transition(r.StatePruned)
		}
	}

	// ── Build Result ───────────────────────────────────────────────────────
	if allFailed {
		return &r.Result{
			OperationID: winner.OperationID,
			Value:       nil,
			Error:       winner.Err,
			FusedFrom:   winner.ID,
			Duration:    winner.Duration(),
			CausalScore: clock.Score(winner.Clock),
		}, r.ErrFusionConflict
	}

	return &r.Result{
		OperationID: winner.OperationID,
		Value:       winner.Result,
		Error:       nil,
		FusedFrom:   winner.ID,
		Duration:    winner.Duration(),
		CausalScore: clock.Score(winner.Clock),
	}, nil
}

// rankByCausalScore sorts rifts in-place, highest causal priority first.
// The sort is stable to preserve insertion order as a last-resort tiebreaker.
func (e *Engine) rankByCausalScore(rifts []*r.Rift) {
	sort.SliceStable(rifts, func(i, j int) bool {
		cmp := e.clk.Compare(rifts[i].Clock, rifts[j].Clock)
		return cmp > 0 // descending: highest score first
	})
}

// ─── Strategy selector ────────────────────────────────────────────────────────

// NewForStrategy returns the appropriate fusion implementation for a given
// strategy name. "causal" (default) uses Extended Causal Clocks.
// "lamport" uses pure Lamport ordering (classic, for comparison).
// "fastest" selects the rift with the lowest wall time (non-deterministic).
func NewForStrategy(strategy string, clk *clock.Provider) *Engine {
	// All strategies currently use the same Engine type; the strategy
	// differentiates behaviour inside clk.Compare via the clock package.
	// Future versions may swap the entire Engine implementation.
	_ = strategy
	return New(clk)
}

// ─── Telemetry helper ─────────────────────────────────────────────────────────

// FusionReport holds a human-readable summary of a fusion event.
type FusionReport struct {
	Winner   r.RiftID
	Score    float64
	Pruned   []r.RiftID
	Strategy string
}

// Report builds a FusionReport from a completed set of rifts.
func Report(rifts []*r.Rift, result *r.Result, strategy string) FusionReport {
	pruned := make([]r.RiftID, 0, len(rifts)-1)
	for _, rft := range rifts {
		if rft.ID != result.FusedFrom {
			pruned = append(pruned, rft.ID)
		}
	}
	return FusionReport{
		Winner:   result.FusedFrom,
		Score:    result.CausalScore,
		Pruned:   pruned,
		Strategy: strategy,
	}
}
