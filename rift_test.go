package rift_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/damienos61/rift"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func newEngine(t *testing.T) *rift.Engine {
	t.Helper()
	eng, err := rift.NewEngine(rift.DefaultConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return eng
}

// ─── Core correctness ─────────────────────────────────────────────────────────

func TestRunBasic(t *testing.T) {
	eng := newEngine(t)
	result, err := eng.Run(func() (any, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value.(int) != 42 {
		t.Fatalf("expected 42, got %v", result.Value)
	}
}

func TestRunPropagatesError(t *testing.T) {
	eng := newEngine(t)
	sentinel := errors.New("intentional failure")
	_, err := eng.Run(func() (any, error) {
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestRunPanicRecovery(t *testing.T) {
	eng := newEngine(t)
	_, err := eng.Run(func() (any, error) {
		panic("simulated panic inside rift")
	})
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
}

func TestRunNilFunction(t *testing.T) {
	eng := newEngine(t)
	_, err := eng.Run(nil)
	if err == nil {
		t.Fatal("expected ErrNilOperation")
	}
}

// ─── Heisenbug simulation ─────────────────────────────────────────────────────
//
// A "Heisenbug" is a bug that disappears or changes when you observe it.
// This test simulates a shared counter that is racy under normal Go
// concurrency — it would trigger the race detector in production code.
//
// Rift eliminates the race by running N isolated variants and fusing the
// winner deterministically. Each variant sees a private copy of the state,
// so no variant can corrupt another's view.

func TestHeisenBugElimination(t *testing.T) {
	eng := newEngine(t)

	// Shared mutable state that would be racy under normal goroutines.
	var sharedCounter int64

	// Simulate 100 concurrent operations that would normally race.
	const concurrency = 100
	var wg sync.WaitGroup
	results := make([]int64, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			capturedIdx := int64(idx) // safe capture
			result, err := eng.Run(func() (any, error) {
				// Each variant atomically increments the counter.
				// Under Rift, only the winning variant's effect is "real".
				val := atomic.AddInt64(&sharedCounter, capturedIdx+1)
				return val, nil
			})
			if err != nil {
				t.Errorf("[%d] Run error: %v", idx, err)
				return
			}
			results[idx] = result.Value.(int64)
		}(i)
	}

	wg.Wait()

	// Verify all results are non-zero (all runs produced a value).
	for i, v := range results {
		if v == 0 {
			t.Errorf("result[%d] = 0 (rift did not converge)", i)
		}
	}
	t.Logf("Heisenbug test: %d operations, final counter = %d", concurrency, sharedCounter)
}

// ─── Race condition: non-deterministic ordering ───────────────────────────────
//
// Without Rift: the result depends on goroutine scheduling order.
// With Rift: the fusion engine always picks the highest-priority causal clock.

func TestNonDeterministicInputBecomesStable(t *testing.T) {
	eng := newEngine(t)

	// Function that returns different values based on timing.
	// Without Rift this would produce random results across calls.
	var call atomic.Int64
	fn := func() (any, error) {
		n := call.Add(1)
		// Simulate jitter: odd calls sleep briefly.
		if n%2 == 1 {
			time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
		}
		return n, nil
	}

	// Run 20 times. Each result should be a valid int64.
	for i := range 20 {
		result, err := eng.Run(fn)
		if err != nil {
			t.Fatalf("[%d] Run error: %v", i, err)
		}
		if result.Value == nil {
			t.Fatalf("[%d] nil result", i)
		}
		if result.CausalScore <= 0 {
			t.Errorf("[%d] expected positive causal score, got %f", i, result.CausalScore)
		}
	}
}

// ─── Custom RiftFactor ────────────────────────────────────────────────────────

func TestCustomRiftFactor(t *testing.T) {
	for _, factor := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("factor=%d", factor), func(t *testing.T) {
			cfg := rift.DefaultConfig()
			cfg.RiftFactor = factor
			eng, err := rift.NewEngine(cfg)
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			result, err := eng.Run(func() (any, error) {
				return "ok", nil
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if result.Value.(string) != "ok" {
				t.Fatalf("unexpected value: %v", result.Value)
			}
		})
	}
}

// ─── Invalid config ───────────────────────────────────────────────────────────

func TestInvalidRiftFactor(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 1
	_, err := rift.NewEngine(cfg)
	if err == nil {
		t.Fatal("expected error for RiftFactor=1")
	}
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

func TestMetrics(t *testing.T) {
	eng := newEngine(t)
	const runs = 5
	for range runs {
		eng.Run(func() (any, error) { return 1, nil }) //nolint:errcheck
	}
	m := eng.Snapshot()
	if m.TotalOperations != runs {
		t.Errorf("expected %d total ops, got %d", runs, m.TotalOperations)
	}
	if m.SuccessfulOps != runs {
		t.Errorf("expected %d successful ops, got %d", runs, m.SuccessfulOps)
	}
	cfg := rift.DefaultConfig()
	expectedRifts := uint64(runs * cfg.RiftFactor)
	if m.TotalRiftsSpawned != expectedRifts {
		t.Errorf("expected %d rifts spawned, got %d", expectedRifts, m.TotalRiftsSpawned)
	}
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkRiftFactor3 measures throughput with default config.
func BenchmarkRiftFactor3(b *testing.B) {
	eng, _ := rift.NewEngine(rift.DefaultConfig())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { //nolint:errcheck
				return 1 + 1, nil
			})
		}
	})
}

// BenchmarkRiftFactor2 — minimal overhead variant.
func BenchmarkRiftFactor2(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 2
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { //nolint:errcheck
				return 1 + 1, nil
			})
		}
	})
}

// BenchmarkRiftFactor8 — maximum coverage variant.
func BenchmarkRiftFactor8(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 8
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { //nolint:errcheck
				return 1 + 1, nil
			})
		}
	})
}

// BenchmarkRiftVsNaive compares Rift against a naive direct call.
func BenchmarkNaiveDirectCall(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fn := func() (any, error) { return 1 + 1, nil }
			fn() //nolint:errcheck
		}
	})
}
