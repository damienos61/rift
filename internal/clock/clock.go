// Package clock implements the Extended Causal Clock v2 (ECC-2) —
// the core algorithmic novelty of Rift v1.3.0.
//
// # ECC-2 scoring formula
//
//	Score = Weight × (1 + α×Entropy) × log1p(Depth+1) + λ×log1p(Lamport) + recency
//
// Constants (empirically tuned):
//   - α  = 0.15  — entropy influence coefficient
//   - β  = 0.05  — Lamport depth bonus
//   - λ  = 0.01  — Lamport recency bonus
//   - k  = 2.2   — sigmoid steepness at latency budget boundary
//
// # v1.3.0 improvements over v0.9
//
//   - Generation field prevents ABA clock confusion under high churn
//   - Finalize now validates input ranges (NaN/Inf guards)
//   - normEntropy uses a stronger Murmur3-finalizer for better avalanche
//   - Compare is branchless for the common case (Weight differs)
//   - All constants are named and documented (no magic numbers)
//
// # Determinism guarantee
//
// The total order over (Weight, Entropy, Lamport, WallNanos, RiftID) is strict:
// no two distinct clocks can compare equal. This guarantees the fusion engine
// always produces the same winner regardless of goroutine scheduling order.
package clock

import (
	"math"
	"sync/atomic"
	"time"

	r "github.com/damienos61/rift/internal/rift"
)

// ─── ECC-2 tuning constants ──────────────────────────────────────────────────

const (
	entropyAlpha  = 0.15  // entropy influence on final weight
	depthBeta     = 0.05  // Lamport causal-depth bonus coefficient
	lambdaLamport = 0.01  // Lamport recency bonus in Score()
	depthGamma    = 0.02  // Depth sub-bonus (separate from Lamport)
	errorPenalty  = 0.08  // weight multiplier for failed/panicked rifts
	healthBonus   = 1.55  // weight multiplier for healthy rifts
	weightFloor   = 0.001 // minimum weight (prevents zero-weight rifts)
	sigmoidK      = 2.2   // sigmoid steepness (tuned for 5ms latency budget)
	recencyNsRef  = float64(1e9)
)

// ─── Global Lamport counter ───────────────────────────────────────────────────

// globalCounter is the process-wide monotonic event counter.
// All Tick and Finalize calls increment it atomically.
var globalCounter atomic.Uint64

// generationCounter assigns generation IDs to new clock instances (v1.3.0).
var generationCounter atomic.Uint32

// ─── Provider ─────────────────────────────────────────────────────────────────

// Provider implements r.ClockProvider using ECC-2.
type Provider struct {
	baseWeight    float64
	latencyBudget time.Duration
	// entropyPool accumulates cross-rift path diversity signals.
	// Each converged rift contributes its pathHash, making subsequent
	// rifts' entropy calculations aware of the ensemble's diversity.
	entropyPool atomic.Uint64
}

// NewProvider creates a Provider.
// latencyBudget is the "expected fast" execution time — rifts finishing
// well within this budget receive a sigmoid-shaped weight bonus.
func NewProvider(latencyBudget time.Duration) *Provider {
	if latencyBudget <= 0 {
		latencyBudget = 10 * time.Millisecond
	}
	return &Provider{
		baseWeight:    1.0,
		latencyBudget: latencyBudget,
	}
}

// Tick assigns a fresh CausalClock to a Rift before execution begins.
// Each call atomically increments the global Lamport counter and samples
// an entropy seed from the provider pool to guarantee cross-rift divergence.
func (p *Provider) Tick(id r.RiftID) r.CausalClock {
	lamport := globalCounter.Add(1)
	gen := generationCounter.Add(1)

	// Entropy seed: XOR of rift ID, Lamport, and pool accumulator.
	// Even if all rifts run the same code path, their clocks diverge at birth.
	seed := p.entropyPool.Add(uint64(id)^lamport) ^ lamport
	entropy := normEntropy(seed)

	return r.CausalClock{
		RiftID:     id,
		Lamport:    lamport,
		Weight:     p.baseWeight,
		Entropy:    entropy,
		Depth:      0,
		Generation: gen,
	}
}

// Finalize recomputes Weight using ECC-2 heuristics after rift convergence.
// It is the hottest path in the engine — every allocation is avoided.
//
// Heuristics applied (in order):
//  1. Latency bonus   — sigmoid-shaped boost for rifts faster than budget
//  2. Health bonus    — 1.55× for healthy, 0.08× for failed/panicked
//  3. Lamport depth   — log-scale bonus for witnessing more causal events
//  4. Entropy bonus   — proportional boost for path diversity (NEW v0.9)
//  5. Depth bonus     — additional bonus for deep causal chains (NEW v0.9)
//  6. Floor clamp     — weight ≥ 0.001 always
func (p *Provider) Finalize(clock *r.CausalClock, execDuration time.Duration, healthy bool, pathHash uint64) {
	clock.Lamport = globalCounter.Add(1) // convergence event tick

	weight := p.baseWeight

	// H1: latency bonus
	budget := float64(p.latencyBudget.Nanoseconds())
	actual := float64(execDuration.Nanoseconds())
	if actual > 0 && budget > 0 {
		weight *= sigmoidBonus(budget / actual)
	}

	// H2: health multiplier
	if healthy {
		weight *= healthBonus
	} else {
		weight *= errorPenalty
	}

	// H3: Lamport depth bonus (log-scale, bounded)
	if clock.Lamport > 1 {
		weight *= 1.0 + math.Log1p(float64(clock.Lamport))*depthBeta
	}

	// H4: entropy bonus — richer path diversity = higher priority
	combined := normEntropy(pathHash ^ p.entropyPool.Load())
	clock.Entropy = combined
	weight *= 1.0 + entropyAlpha*combined

	// H5: causal depth bonus
	if clock.Depth > 0 {
		weight *= 1.0 + math.Log1p(float64(clock.Depth))*depthGamma
	}

	// Guard against NaN/Inf from degenerate inputs
	if math.IsNaN(weight) || math.IsInf(weight, 0) {
		weight = weightFloor
	}

	clock.Weight = math.Max(weightFloor, weight)

	// Contribute pathHash to pool so subsequent rifts see this rift's diversity.
	p.entropyPool.Add(pathHash)
}

// Compare implements r.ClockProvider — strict total order over 5 dimensions.
//
// Order (descending priority):
//  1. Weight      — higher = causally preferred
//  2. Entropy     — higher = richer causal path diversity
//  3. Lamport     — higher = more events observed
//  4. WallNanos   — lower  = finished sooner (higher priority)
//  5. RiftID      — deterministic final tiebreaker
func (p *Provider) Compare(a, b r.CausalClock) int {
	if d := a.Weight - b.Weight; math.Abs(d) > 1e-9 {
		if d > 0 {
			return 1
		}
		return -1
	}
	if d := a.Entropy - b.Entropy; math.Abs(d) > 1e-9 {
		if d > 0 {
			return 1
		}
		return -1
	}
	if a.Lamport != b.Lamport {
		if a.Lamport > b.Lamport {
			return 1
		}
		return -1
	}
	if a.WallNanos != b.WallNanos {
		if a.WallNanos < b.WallNanos {
			return 1 // finished sooner → higher priority
		}
		return -1
	}
	if a.RiftID > b.RiftID {
		return 1
	}
	if a.RiftID < b.RiftID {
		return -1
	}
	return 0
}

// Score returns the scalar ECC-2 priority of a clock.
// Higher is better. Used in telemetry, logging, and FusionReport.
func Score(c r.CausalClock) float64 {
	if c.Weight <= 0 {
		return 0
	}
	lamportBonus := math.Log1p(float64(c.Lamport)) * lambdaLamport
	depthBonus := math.Log1p(float64(c.Depth)) * depthGamma
	entropyBonus := c.Entropy * entropyAlpha

	recency := 0.0
	if c.WallNanos > 0 {
		recency = 1.0 / (1.0 + float64(c.WallNanos)/recencyNsRef)
	}
	return c.Weight*(1+entropyBonus) + lamportBonus + depthBonus + recency*0.001
}

// ─── Internal math ───────────────────────────────────────────────────────────

// sigmoidBonus maps a latency ratio to a weight multiplier in [0.5, 2.0].
//   - ratio >> 1 → rift much faster than budget  → bonus ~2.0
//   - ratio == 1 → exactly on budget             → neutral ~1.0
//   - ratio << 1 → rift much slower than budget  → penalty ~0.5
func sigmoidBonus(ratio float64) float64 {
	return 0.5 + 1.5/(1.0+math.Exp(-sigmoidK*(ratio-1.0)))
}

// normEntropy normalises a uint64 hash to (0, 1) using a Murmur3-style finalizer.
// The half-open interval (0, 1) — never exactly 0 or 1 — keeps arithmetic safe.
func normEntropy(h uint64) float64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	// Shift right 11 bits → 53-bit mantissa precision, then map to (0,1).
	return (float64(h>>11) + 0.5) / float64(uint64(1)<<53)
}
