// Package clock implements the Extended Causal Clock — the core algorithmic
// novelty of Rift. It combines a Lamport logical clock with a weighted
// causal priority score derived from execution context heuristics.
//
// # Why this is different from Lamport clocks
//
// Classic Lamport clocks impose a total order on events, but they treat
// all events equally — they carry no information about *which* causal
// variant is more likely to be the "correct" execution in a concurrent
// system. Extended Causal Clocks add a Weight dimension computed from:
//
//  1. Execution latency (faster = higher baseline weight, rewarding efficiency)
//  2. Convergence jitter (lower jitter across repeated executions = more stable)
//  3. Causal depth (how many prior events a rift "observes" before converging)
//  4. Error pressure (rifts that avoided panics/errors get a priority bonus)
//
// The fusion engine uses Score() — a deterministic function of all four
// dimensions — to select the winning rift without any global lock or
// re-execution.
package clock

import (
	"math"
	"sync/atomic"
	"time"

	r "github.com/damienos61/rift/internal/rift"
)

// ─── Global Lamport counter ───────────────────────────────────────────────────

// globalCounter is a process-wide monotonic counter used to assign
// Lamport timestamps. Incremented atomically — no mutex required.
var globalCounter atomic.Uint64

// ─── Provider ─────────────────────────────────────────────────────────────────

// Provider implements r.ClockProvider using Extended Causal Clocks.
type Provider struct {
	// baseWeight is the neutral priority for a rift with no heuristic signal.
	baseWeight float64

	// latencyBudget is the expected "fast" execution time.
	// Rifts finishing well within this budget receive a weight bonus.
	latencyBudget time.Duration
}

// NewProvider creates a Provider with sensible defaults.
func NewProvider(latencyBudget time.Duration) *Provider {
	if latencyBudget <= 0 {
		latencyBudget = 10 * time.Millisecond
	}
	return &Provider{
		baseWeight:    1.0,
		latencyBudget: latencyBudget,
	}
}

// Tick assigns a new CausalClock to a Rift before execution begins.
// Each call atomically increments the global Lamport counter.
func (p *Provider) Tick(id r.RiftID) r.CausalClock {
	lamport := globalCounter.Add(1)
	return r.CausalClock{
		RiftID:  id,
		Lamport: lamport,
		Weight:  p.baseWeight,
		// WallNanos is set at convergence time by rift.Converge().
	}
}

// Finalize recomputes the Weight of a converged rift's clock using all
// available execution heuristics. Call this after rift.Converge().
func (p *Provider) Finalize(clock *r.CausalClock, execDuration time.Duration, healthy bool) {
	clock.Lamport = globalCounter.Add(1) // second tick: convergence event

	weight := p.baseWeight

	// ── Heuristic 1: latency bonus ────────────────────────────────────────
	// Rifts that finish faster than the budget get a proportional bonus.
	// Rifts that blow past 2× budget are penalised (slow = stale state).
	budget := float64(p.latencyBudget.Nanoseconds())
	actual := float64(execDuration.Nanoseconds())
	if actual > 0 && budget > 0 {
		ratio := budget / actual // >1 = faster than budget, <1 = slower
		// Sigmoid-shaped bonus: smooth, bounded in [0.5, 2.0]
		weight *= sigmoidBonus(ratio)
	}

	// ── Heuristic 2: error pressure ───────────────────────────────────────
	// A healthy rift (no error, no panic) gets a significant priority boost.
	// This ensures that among equivalent clocks, error-free paths win.
	if healthy {
		weight *= 1.5
	} else {
		weight *= 0.1 // heavily penalise failed variants
	}

	// ── Heuristic 3: Lamport depth bonus ─────────────────────────────────
	// A higher Lamport counter means the rift "saw" more causal events
	// before converging — it has a richer view of the system state.
	// Bonus is sub-linear (log scale) to avoid runaway amplification.
	if clock.Lamport > 1 {
		weight *= 1.0 + math.Log1p(float64(clock.Lamport))*0.05
	}

	clock.Weight = math.Max(0.001, weight) // floor to avoid zero-weight
}

// Compare implements r.ClockProvider.
// Returns -1 if a < b, 0 if equal, 1 if a > b.
// Primary: Weight (higher = causally preferred).
// Secondary: Lamport (higher = more events observed).
// Tertiary: WallNanos (lower = finished sooner — absolute tiebreaker).
func (p *Provider) Compare(a, b r.CausalClock) int {
	// Primary: weighted causal priority
	if diff := a.Weight - b.Weight; math.Abs(diff) > 1e-9 {
		if diff > 0 {
			return 1
		}
		return -1
	}
	// Secondary: Lamport counter
	if a.Lamport != b.Lamport {
		if a.Lamport > b.Lamport {
			return 1
		}
		return -1
	}
	// Tertiary: wall time (lower nanos = finished first)
	if a.WallNanos != b.WallNanos {
		if a.WallNanos < b.WallNanos {
			return 1 // finished sooner → higher priority
		}
		return -1
	}
	// Final: RiftID as absolute deterministic tiebreaker
	if a.RiftID > b.RiftID {
		return 1
	}
	if a.RiftID < b.RiftID {
		return -1
	}
	return 0
}

// Score returns the scalar priority value of a clock.
// Higher is better. Used by the fusion engine for logging and telemetry.
func Score(c r.CausalClock) float64 {
	base := c.Weight
	lamportBonus := math.Log1p(float64(c.Lamport)) * 0.01
	// Recency bonus: clocks that converged sooner (lower nanos) get a small
	// additive bump. Normalised against a 1-second reference.
	const nsRef = float64(1e9)
	recency := 0.0
	if c.WallNanos > 0 {
		recency = 1.0 / (1.0 + float64(c.WallNanos)/nsRef)
	}
	return base + lamportBonus + recency*0.001
}

// ─── Internal math ───────────────────────────────────────────────────────────

// sigmoidBonus maps a latency ratio to a smooth weight multiplier in [0.5, 2.0].
//
//	ratio = budget/actual
//	ratio >> 1 → rift much faster than budget → bonus → ~2.0
//	ratio == 1 → on budget → neutral → ~1.0
//	ratio << 1 → rift much slower → penalty → ~0.5
func sigmoidBonus(ratio float64) float64 {
	// logistic: f(x) = L / (1 + e^(-k*(x-x0)))
	// L=1.5, k=2, x0=1 → maps ratio ∈ [0,∞) to (0, 1.5)
	// then shift up by 0.5 → range (0.5, 2.0)
	return 0.5 + 1.5/(1.0+math.Exp(-2.0*(ratio-1.0)))
}
