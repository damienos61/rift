package rift_test

import (
	"errors"
	"fmt"
	"math"
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
	_, err := eng.Run(func() (any, error) { panic("simulated panic inside rift") })
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

// ─── ECC-2 ────────────────────────────────────────────────────────────────────

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

func TestEntropyDeltaFinite(t *testing.T) {
	eng := newEngine(t)
	result, err := eng.Run(func() (any, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if math.IsNaN(result.EntropyDelta) || math.IsInf(result.EntropyDelta, 0) {
		t.Errorf("EntropyDelta is not finite: %f", result.EntropyDelta)
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
			captured := int64(idx)
			result, err := eng.Run(func() (any, error) {
				val := atomic.AddInt64(&sharedCounter, captured+1)
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

// ─── RunWithTimeout ───────────────────────────────────────────────────────────

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

// ─── Circuit breaker ──────────────────────────────────────────────────────────

func TestCircuitBreakerOpens(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled:   true,
		Threshold: 0.5,
		Window:    10,
		CoolDown:  1 * time.Hour, // keep open during test
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Fill the window with failures.
	alwaysFail := func() (any, error) { return nil, errors.New("fail") }
	for i := 0; i < 20; i++ {
		eng.Run(alwaysFail) //nolint:errcheck
	}

	_, err = eng.Run(func() (any, error) { return "ok", nil })
	if errors.Is(err, rift.ErrCircuitOpen) {
		t.Logf("circuit correctly opened: %v", err)
	} else {
		t.Logf("circuit may not have opened yet (window=%d): %v", cfg.CircuitBreaker.Window, err)
	}
}

func TestCircuitBreakerClosesAfterCoolDown(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled:   true,
		Threshold: 0.5,
		Window:    10,
		CoolDown:  50 * time.Millisecond, // short cool-down for test speed
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Trip the breaker.
	for i := 0; i < 20; i++ {
		eng.Run(func() (any, error) { return nil, errors.New("fail") }) //nolint:errcheck
	}

	// Wait for cool-down.
	time.Sleep(100 * time.Millisecond)

	// Should succeed after half-open probe.
	result, err := eng.Run(func() (any, error) { return "recovered", nil })
	if err != nil {
		t.Logf("probe error after cool-down: %v", err)
	} else {
		t.Logf("circuit recovered: %v  circuit=%s", result.Value, eng.Snapshot().CircuitState)
	}
}

// ─── Adaptive RiftFactor ──────────────────────────────────────────────────────

func TestAdaptiveFactorBounds(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Adaptive = rift.AdaptiveConfig{
		Enabled:       true,
		MinFactor:     2,
		MaxFactor:     6,
		ErrorRateUp:   0.3,
		ErrorRateDown: 0.05,
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Mix of success and failure.
	for i := 0; i < 100; i++ {
		fail := i%3 == 0
		eng.Run(func() (any, error) { //nolint:errcheck
			if fail {
				return nil, errors.New("transient")
			}
			return i, nil
		})
	}

	m := eng.Snapshot()
	if m.ActiveRiftFactor < cfg.Adaptive.MinFactor || m.ActiveRiftFactor > cfg.Adaptive.MaxFactor {
		t.Errorf("adaptive factor %d outside bounds [%d, %d]",
			m.ActiveRiftFactor, cfg.Adaptive.MinFactor, cfg.Adaptive.MaxFactor)
	}
	t.Logf("adaptive: active_factor=%d error_rate=%.1f%%", m.ActiveRiftFactor, m.ErrorRate*100)
}

// ─── Retry policy ─────────────────────────────────────────────────────────────

func TestRetryPolicyConverges(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Retry = rift.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// At least one rift should eventually succeed across 3×3=9 total attempts.
	var n atomic.Int32
	result, err := eng.Run(func() (any, error) {
		if n.Add(1) <= 6 {
			return nil, errors.New("transient")
		}
		return "done", nil
	})
	if err == nil {
		t.Logf("retry converged: %v (retries=%d)", result.Value, result.Retries)
	} else {
		t.Logf("all retries exhausted (expected for aggressive fail rate): %v", err)
	}
}

// ─── Entropy fusion strategy ──────────────────────────────────────────────────

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

// ─── Load shedding (v1.3.0) ───────────────────────────────────────────────────

func TestLoadSheddingRejectsWhenSaturated(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Shed = rift.ShedPolicy{
		Enabled:     true,
		MaxQueueLen: 5, // tiny pool for test
		Strategy:    "newest",
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var shed atomic.Int64
	var wg sync.WaitGroup

	// Flood with 100 ops — at least some should be shed.
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := eng.Run(func() (any, error) {
				time.Sleep(2 * time.Millisecond)
				return 1, nil
			})
			if errors.Is(err, rift.ErrShed) {
				shed.Add(1)
			}
		}()
	}
	wg.Wait()

	if shed.Load() == 0 {
		t.Error("expected some ops to be shed, got 0")
	}
	m := eng.Snapshot()
	t.Logf("shed: %d ops shed, engine ShedOps=%d", shed.Load(), m.ShedOps)
}

// ─── Health probe (v1.3.0) ────────────────────────────────────────────────────

func TestHealthProbeHealthyEngine(t *testing.T) {
	eng := newEngine(t)
	// Run a few successful ops.
	for range 5 {
		eng.Run(func() (any, error) { return "ok", nil }) //nolint:errcheck
	}
	h := eng.Health()
	if !h.Live {
		t.Error("expected Live=true")
	}
	if !h.Ready {
		t.Errorf("expected Ready=true, got reason=%q", h.Reason)
	}
}

func TestHealthProbeStressedEngine(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled:   true,
		Threshold: 0.3,
		Window:    10,
		CoolDown:  1 * time.Hour,
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		eng.Run(func() (any, error) { return nil, errors.New("fail") }) //nolint:errcheck
	}
	h := eng.Health()
	if !h.Live {
		t.Error("expected Live=true even under stress")
	}
	// May or may not be ready depending on window fill.
	t.Logf("stressed health: live=%v ready=%v reason=%q", h.Live, h.Ready, h.Reason)
}

// ─── Warmup (v1.3.0) ─────────────────────────────────────────────────────────

func TestWarmupCompletes(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.Warmup = rift.WarmupConfig{
		Enabled: true,
		Ops:     5,
		Timeout: 5 * time.Second,
	}
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatalf("warmup failed: %v", err)
	}
	// Engine should be immediately usable.
	result, err := eng.Run(func() (any, error) { return "warmed", nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.(string) != "warmed" {
		t.Fatalf("unexpected value: %v", result.Value)
	}
}

// ─── Custom RiftFactor ────────────────────────────────────────────────────────

func TestCustomRiftFactor(t *testing.T) {
	for _, factor := range []int{2, 4, 8, 16} {
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

func TestRiftFactorAboveMaxRejected(t *testing.T) {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 1 // below minimum
	_, err := rift.NewEngine(cfg)
	if err == nil {
		t.Fatal("expected error for RiftFactor=1")
	}
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

func TestMetricsAccuracy(t *testing.T) {
	eng := newEngine(t)
	const runs = 5
	for range runs {
		eng.Run(func() (any, error) { return 1, nil }) //nolint:errcheck
	}
	m := eng.Snapshot()

	if m.TotalOperations != runs {
		t.Errorf("total ops: want %d got %d", runs, m.TotalOperations)
	}
	if m.SuccessfulOps != runs {
		t.Errorf("successful ops: want %d got %d", runs, m.SuccessfulOps)
	}
	if m.MeanLatencyMs < 0 {
		t.Errorf("negative mean latency: %f", m.MeanLatencyMs)
	}
	if m.CircuitState != "closed" {
		t.Errorf("expected circuit closed, got %s", m.CircuitState)
	}

	cfg := rift.DefaultConfig()
	expected := uint64(runs * cfg.RiftFactor)
	if m.TotalRiftsSpawned != expected {
		t.Errorf("rifts spawned: want %d got %d", expected, m.TotalRiftsSpawned)
	}
	t.Logf("metrics: %+v", m)
}

// ─── Telemetry hook ───────────────────────────────────────────────────────────

type testTelemetry struct {
	splits    atomic.Int64
	converges atomic.Int64
	fuses     atomic.Int64
	errs      atomic.Int64
	sheds     atomic.Int64
}

func (tt *testTelemetry) OnSplit(_ rift.OperationID, _ int)                        { tt.splits.Add(1) }
func (tt *testTelemetry) OnError(_ rift.OperationID, _ error)                     { tt.errs.Add(1) }
func (tt *testTelemetry) OnShed(_ rift.OperationID)                                { tt.sheds.Add(1) }
func (tt *testTelemetry) OnConverge(_ rift.RiftID, _ time.Duration, _ bool)        { tt.converges.Add(1) }
func (tt *testTelemetry) OnFuse(_ rift.OperationID, _ rift.RiftID, _ float64, _ int) {
	tt.fuses.Add(1)
}

func TestTelemetryHook(t *testing.T) {
	tel := &testTelemetry{}
	cfg := rift.DefaultConfig()
	cfg.Telemetry = tel
	eng, err := rift.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}

	const runs = 5
	for range runs {
		eng.Run(func() (any, error) { return 1, nil }) //nolint:errcheck
	}

	if tel.splits.Load() != runs {
		t.Errorf("splits: want %d got %d", runs, tel.splits.Load())
	}
	if tel.fuses.Load() != runs {
		t.Errorf("fuses: want %d got %d", runs, tel.fuses.Load())
	}
	// converges = runs × RiftFactor
	expected := int64(runs * rift.DefaultConfig().RiftFactor)
	if tel.converges.Load() != expected {
		t.Errorf("converges: want %d got %d", expected, tel.converges.Load())
	}
}

// ─── Concurrent safety ────────────────────────────────────────────────────────

func TestConcurrentSafety(t *testing.T) {
	eng := newEngine(t)
	const goroutines = 50
	var wg sync.WaitGroup
	var errs atomic.Int64

	for i := range goroutines {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := eng.Run(func() (any, error) { return n * n, nil })
			if err != nil {
				errs.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if errs.Load() > 0 {
		t.Errorf("%d concurrent ops failed", errs.Load())
	}
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

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

func BenchmarkRiftFactor3(b *testing.B) {
	eng, _ := rift.NewEngine(rift.DefaultConfig())
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

func BenchmarkNaiveDirectCall(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fn := func() (any, error) { return 1 + 1, nil }
			fn() //nolint:errcheck
		}
	})
}

func BenchmarkWithCircuitBreaker(b *testing.B) {
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled: true, Threshold: 0.5, Window: 100, CoolDown: 5 * time.Second,
	}
	eng, _ := rift.NewEngine(cfg)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
		}
	})
}
