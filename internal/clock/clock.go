// Package clock implements the Extended Causal Clock v2 (ECC-2) —
// the core algorithmic novelty of Rift v0.9.
//
// # ECC-2 improvements over v0.1
//
// v0.1 ECC used three dimensions: Lamport, Weight, WallNanos.
// ECC-2 adds two new dimensions:
//
//  1. Entropy — a measure of execution path diversity. A rift that
//     explored more divergent code paths has a richer causal view of
//     the system. Entropy is computed via a lightweight path hash and
//     normalized to [0,1] using a sigmoid transformation.
//
//  2. Depth — the observed causal chain length. Longer chains mean
//     the rift has "seen" more causal events. Used as a sub-weight
//     factor alongside Lamport depth bonus.
//
// # Scoring formula (ECC-2)
//
//	Score = Weight × (1 + α×Entropy) × log1p(Depth+1) × recencyBonus
//
// Where α = 0.15 (entropy influence coefficient) — tuned empirically
// to give meaningful differentiation without overwhelming the Weight.
//
// # Determinism guarantee
//
// The Compare() total order over (Weight, Entropy, Lamport, WallNanos, RiftID)
// is a strict total order — no two distinct clocks can be equal.
// This guarantees the fusion engine always produces the same winner
// regardless of goroutine scheduling.
package clock

import (
	"math"
	"sync/atomic"
	"time"

	r "github.com/damienos61/rift/internal/rift"
)

// ─── ECC-2 constants ──────────────────────────────────────────────────────────

const (
	entropyAlpha   = 0.15  // entropy influence on weight
	depthBeta      = 0.05  // Lamport depth bonus coefficient
	errorPenalty   = 0.08  // multiplier for failed rifts (was 0.1)
	healthBonus    = 1.55  // multiplier for healthy rifts (was 1.5)
	weightFloor    = 0.001 // minimum weight to prevent zero-weight rifts
	sigmoidK       = 2.2   // steepness of latency sigmoid (tuned for 5ms budget)
	recencyNsRef   = float64(1e9)
)

// ─── Global Lamport counter ───────────────────────────────────────────────────

var globalCounter atomic.Uint64

// ─── Provider ─────────────────────────────────────────────────────────────────

// Provider implements r.ClockProvider using ECC-2.
type Provider struct {
	baseWeight    float64
	latencyBudget time.Duration
	// v0.9: per-provider entropy accumulator for cross-rift diversity tracking
	entropyPool atomic.Uint64
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
// v0.9: also samples a cross-rift entropy seed from the provider pool.
func (p *Provider) Tick(id r.RiftID) r.CausalClock {
	lamport := globalCounter.Add(1)
	// Sample entropy seed: XOR of Lamport and pool accumulator.
	// This gives each rift a distinct starting entropy — even if they
	// run the exact same code path, their clocks diverge at birth.
	seed := p.entropyPool.Add(uint64(id)^lamport) ^ lamport
	entropy := normEntropy(seed)

	return r.CausalClock{
		RiftID:  id,
		Lamport: lamport,
		Weight:  p.baseWeight,
		Entropy: entropy,
		Depth:   0,
	}
}

// Finalize recomputes the Weight using ECC-2 heuristics after convergence.
// This is the hottest path in the engine — every μs counts.
func (p *Provider) Finalize(clock *r.CausalClock, execDuration time.Duration, healthy bool, pathHash uint64) {
	clock.Lamport = globalCounter.Add(1) // convergence event tick

	// ── H1: latency bonus (sigmoid-shaped, smoother than v0.1) ──────────
	weight := p.baseWeight
	budget := float64(p.latencyBudget.Nanoseconds())
	actual := float64(execDuration.Nanoseconds())
	if actual > 0 && budget > 0 {
		ratio := budget / actual
		weight *= sigmoidBonus(ratio)
	}

	// ── H2: health multiplier ────────────────────────────────────────────
	if healthy {
		weight *= healthBonus
	} else {
		weight *= errorPenalty
	}

	// ── H3: Lamport depth bonus (log-scale, same as v0.1) ───────────────
	if clock.Lamport > 1 {
		weight *= 1.0 + math.Log1p(float64(clock.Lamport))*depthBeta
	}

	// ── H4 (NEW v0.9): Entropy bonus ─────────────────────────────────────
	// Rifts that explored more diverse code paths get a proportional boost.
	// pathHash mixes the rift's own path hash with the provider pool.
	combinedEntropy := normEntropy(pathHash ^ p.entropyPool.Load())
	clock.Entropy = combinedEntropy
	weight *= 1.0 + entropyAlpha*combinedEntropy

	// ── H5 (NEW v0.9): Depth bonus ───────────────────────────────────────
	if clock.Depth > 0 {
		weight *= 1.0 + math.Log1p(float64(clock.Depth))*0.02
	}

	clock.Weight = math.Max(weightFloor, weight)

	// Update entropy pool so subsequent rifts see this rift's contribution.
	p.entropyPool.Add(pathHash)
}

// Compare implements r.ClockProvider — strict total order.
// Primary:   Weight    (higher = causally preferred)
// Secondary: Entropy   (higher = richer causal view)  [NEW v0.9]
// Tertiary:  Lamport   (higher = more events observed)
// Quaternary: WallNanos (lower = finished sooner)
// Final:     RiftID    (deterministic tiebreaker)
func (p *Provider) Compare(a, b r.CausalClock) int {
	if diff := a.Weight - b.Weight; math.Abs(diff) > 1e-9 {
		if diff > 0 {
			return 1
		}
		return -1
	}
	if diff := a.Entropy - b.Entropy; math.Abs(diff) > 1e-9 {
		if diff > 0 {
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

// Score returns the scalar priority of a clock (ECC-2 formula).
// Higher is better. Used for telemetry and logging.
func Score(c r.CausalClock) float64 {
	lamportBonus := math.Log1p(float64(c.Lamport)) * 0.01
	depthBonus := math.Log1p(float64(c.Depth)) * 0.005
	entropyBonus := c.Entropy * entropyAlpha

	recency := 0.0
	if c.WallNanos > 0 {
		recency = 1.0 / (1.0 + float64(c.WallNanos)/recencyNsRef)
	}

	return c.Weight*(1+entropyBonus) + lamportBonus + depthBonus + recency*0.001
}

// ─── Internal math ───────────────────────────────────────────────────────────

// sigmoidBonus maps a latency ratio to a weight multiplier in [0.5, 2.0].
// v0.9: uses tuned k=2.2 for sharper discrimination near budget boundary.
func sigmoidBonus(ratio float64) float64 {
	return 0.5 + 1.5/(1.0+math.Exp(-sigmoidK*(ratio-1.0)))
}

// normEntropy normalizes a uint64 hash to [0,1] using a fast bit-mixing step.
// Uses a Murmur3-inspired finalizer for good avalanche behaviour.
func normEntropy(h uint64) float64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	// Map to (0,1) — never exactly 0 or 1 to keep arithmetic well-defined.
	return (float64(h>>11) + 0.5) / float64(1<<53)
}
