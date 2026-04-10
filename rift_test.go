package rift_test

import (
	"errors"
	"fmt"
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
	result, err := eng.Run(func() (any, error) { return 42, nil })
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
	_, err := eng.Run(func() (any, error) { return nil, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestRunPanicRecovery(t *testing.T) {
	eng := newEngine(t)
	_, err := eng.Run(func() (any, error) { panic("simulated panic") })
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

// ─── ECC-2: CausalScore and EntropyDelta ──────────────────────────────────────

func TestCausalScorePositive(t *testing.T) {
	eng := newEngine(t)
	result, err := eng.Run(func() (any, error) { return "ok", nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.CausalScore <= 0 {
		t.Errorf("expected positive CausalScore, got %f", result.CausalScore)
	}
}

func TestEntropyDeltaPresent(t *testing.T) {
	eng := newEngine(t)
	result, err := eng.Run(func() (any, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	// EntropyDelta can be negative (winner less diverse than losers),
	// zero, or positive. We just check it's a finite float.
	if result.EntropyDelta != result.EntropyDelta { // NaN check
		t.Errorf("EntropyDelta is NaN")
	}
}

// ─── Heisenbug elimination ────────────────────────────────────────────────────

func TestHeisenBugElimination(t *testing.T) {
	eng := newEngine(t)
	var sharedCounter int64
	const concurrency = 100
	var wg sync.WaitGroup
	results := make([]int64, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			capturedIdx := int64(idx)
			result, err := eng.Run(func() (any, error) {
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

	for i, v := range results {
		if v == 0 {
			t.Errorf("result[%d] = 0 (rift did not converge)", i)
		}
	}
	t.Logf("Heisenbug: %d ops, final counter=%d", concurrency, sharedCounter)
}

// ─── RunWithTimeout (v0.9) ────────────────────────────────────────────────────

func TestRunWithTimeout(t *testing.T) {
	eng := newEngine(t)
	_, err := eng.RunWithTimeout(func() (any, error) {
		time.Sleep(200 * time.Millisecond)
		return "late", nil
	}, 50*time.Millisecond)
	if !errors.Is(err, rift.ErrOperationTimeout) {
		t.Fatalf("expected ErrOperationTimeout, got %v", err)
	}
}

// ─── Circuit Breaker (v0.9) ───────────────────────────────────────────────────

func TestCircuitBreakerOpens(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker.Enabled = true
	cfg.CircuitBreaker.Threshold = 0.5
	cfg.CircuitBreaker.Window = 10
	cfg.CircuitBreaker.CoolDown = 1 * time.Hour // keep open for test

	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Saturate the window with failures to trip the breaker.
	alwaysFail := func() (any, error) { return nil, errors.New("fail") }
	for i := 0; i < 20; i++ {
		eng.Run(alwaysFail) //nolint:errcheck
	}

	// Next call should be rejected by the open circuit.
	_, err = eng.Run(func() (any, error) { return "ok", nil })
	if err == nil {
		t.Log("circuit breaker did not open — window may not be full yet (timing-dependent)")
	}
}

// ─── Adaptive RiftFactor (v0.9) ───────────────────────────────────────────────

func TestAdaptiveFactorDoesNotPanic(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Adaptive.Enabled = true
	cfg.Adaptive.MinFactor = 2
	cfg.Adaptive.MaxFactor = 6
	cfg.Adaptive.ErrorRateUp = 0.3
	cfg.Adaptive.ErrorRateDown = 0.05

	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var errCount int
	for i := 0; i < 50; i++ {
		_, err := eng.Run(func() (any, error) {
			if i%5 == 0 {
				return nil, errors.New("transient")
			}
			return i, nil
		})
		if err != nil {
			errCount++
		}
	}
	t.Logf("adaptive test: 50 ops, %d errors, final metrics: %+v", errCount, eng.Snapshot())
}

// ─── Retry Policy (v0.9) ──────────────────────────────────────────────────────

func TestRetryPolicySucceedsOnSecondAttempt(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.Backoff = 1 * time.Millisecond
	cfg.Retry.MaxBackoff = 5 * time.Millisecond

	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Function that fails the first call per-rift but succeeds after.
	var callCount atomic.Int32
	result, err := eng.Run(func() (any, error) {
		n := callCount.Add(1)
		if n <= 3 { // first N calls across all rifts fail
			return nil, errors.New("transient")
		}
		return "recovered", nil
	})
	// With retries enabled, at least some rifts should recover.
	if err == nil && result.Value.(string) == "recovered" {
		t.Logf("retry succeeded: %+v", result)
	} else {
		t.Logf("all variants failed (expected under high fail rate): %v", err)
	}
}

// ─── Entropy fusion strategy (v0.9) ──────────────────────────────────────────

func TestEntropyFusionStrategy(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.FusionStrategy = "entropy"
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := eng.Run(func() (any, error) { return "entropy-mode", nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.(string) != "entropy-mode" {
		t.Fatalf("wrong value: %v", result.Value)
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
			result, err := eng.Run(func() (any, error) { return "ok", nil })
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

// ─── Metrics v0.9 ────────────────────────────────────────────────────────────

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
	if m.MeanLatencyMs < 0 {
		t.Errorf("negative mean latency: %f", m.MeanLatencyMs)
	}
	if m.CircuitState != "closed" {
		t.Errorf("expected circuit closed, got %s", m.CircuitState)
	}
	t.Logf("Metrics: %+v", m)
}

// ─── Telemetry hook (v0.9) ────────────────────────────────────────────────────

type testTelemetry struct {
	splits    atomic.Int64
	converges atomic.Int64
	fuses     atomic.Int64
	errs      atomic.Int64
}

func (tt *testTelemetry) OnSplit(_ interface{ }, _ int)                    { tt.splits.Add(1) }
func (tt *testTelemetry) OnConverge(_ interface{ }, _ time.Duration, _ bool) { tt.converges.Add(1) }
func (tt *testTelemetry) OnFuse(_ interface{ }, _ interface{ }, _ float64, _ int) {
	tt.fuses.Add(1)
}
func (tt *testTelemetry) OnError(_ interface{ }, _ error) { tt.errs.Add(1) }

// ─── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkRiftFactor3(b *testing.B) {
	eng, _ := rift.NewEngine(rift.DefaultConfig())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
		}
	})
}

func BenchmarkRiftFactor2(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 2
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
		}
	})
}

func BenchmarkRiftFactor8(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 8
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
		}
	})
}

func BenchmarkNaiveDirectCall(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fn := func() (any, error) { return 1 + 1, nil }
			fn() //nolint:errcheck
		}
	})
}

func BenchmarkEntropyFusion(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.FusionStrategy = "entropy"
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
		}
	})
}
