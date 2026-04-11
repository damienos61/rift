// Command rift is the interactive CLI demo for Rift v1.3.0.
//
// Usage:
//
//	go run ./cmd/rift                  # interactive demo
//	go run ./cmd/rift -bench           # throughput benchmark
//	go run ./cmd/rift -heisen          # Heisenbug simulation
//	go run ./cmd/rift -circuit         # circuit breaker demo
//	go run ./cmd/rift -adaptive        # adaptive RiftFactor demo
//	go run ./cmd/rift -shed            # load-shedding demo (v1.3.0)
//	go run ./cmd/rift -health          # health probe demo (v1.3.0)
//	go run ./cmd/rift -factor 5        # custom RiftFactor
//	go run ./cmd/rift -strategy entropy # entropy-weighted fusion
package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/damienos61/rift"
)

func main() {
	bench    := flag.Bool("bench", false, "throughput benchmark")
	heisen   := flag.Bool("heisen", false, "Heisenbug simulation")
	circuit  := flag.Bool("circuit", false, "circuit breaker demo")
	adaptive := flag.Bool("adaptive", false, "adaptive RiftFactor demo")
	shed     := flag.Bool("shed", false, "load-shedding demo (v1.3.0)")
	health   := flag.Bool("health", false, "health probe demo (v1.3.0)")
	factor   := flag.Int("factor", 3, "RiftFactor (2–32)")
	strategy := flag.String("strategy", "causal", "fusion: causal|entropy|lamport|fastest")
	flag.Parse()

	switch {
	case *bench:
		runBench(*factor, *strategy)
	case *heisen:
		runHeisen(*factor)
	case *circuit:
		runCircuit()
	case *adaptive:
		runAdaptive()
	case *shed:
		runShed()
	case *health:
		runHealth()
	default:
		runDemo(*factor, *strategy)
	}
}

// ─── Demo ─────────────────────────────────────────────────────────────────────

func runDemo(factor int, strategy string) {
	fmt.Printf("╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║       R I F T  v1.3.0 — Causal Execution Engine         ║\n")
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n")
	fmt.Printf("  RiftFactor : %d  |  Strategy : %s\n\n", factor, strategy)

	cfg := rift.DefaultConfig()
	cfg.RiftFactor = factor
	cfg.FusionStrategy = strategy
	eng := must(rift.NewEngine(cfg))

	for i := 1; i <= 8; i++ {
		result, err := eng.Run(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			return fmt.Sprintf("result-%d", i), nil
		})
		if err != nil {
			fmt.Printf("  op=%d ERROR: %v\n", i, err)
			continue
		}
		fmt.Printf("  op=%d | value=%-12v | score=%.4f | entropy=%.4f | retries=%d\n",
			i, result.Value, result.CausalScore, result.EntropyDelta, result.Retries)
	}
	printMetrics(eng)
}

// ─── Bench ────────────────────────────────────────────────────────────────────

func runBench(factor int, strategy string) {
	fmt.Printf("═══ Rift v1.3.0 Benchmark (factor=%d, strategy=%s) ═══\n", factor, strategy)

	cfg := rift.DefaultConfig()
	cfg.RiftFactor = factor
	cfg.FusionStrategy = strategy
	eng := must(rift.NewEngine(cfg))

	const (
		warmupOps = 500
		benchOps  = 20_000
		workers   = 8
	)

	// Warmup
	for range warmupOps {
		eng.Run(func() (any, error) { return 1, nil }) //nolint:errcheck
	}

	var ops atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	start := time.Now()
	for range benchOps {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			eng.Run(func() (any, error) { return 1 + 1, nil }) //nolint:errcheck
			ops.Add(1)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	n := ops.Load()
	fmt.Printf("  %d ops in %v → %.0f ops/sec\n", n, elapsed.Round(time.Millisecond),
		float64(n)/elapsed.Seconds())
	printMetrics(eng)
}

// ─── Heisenbug ────────────────────────────────────────────────────────────────

func runHeisen(factor int) {
	fmt.Printf("═══ Heisenbug Simulation (goroutines=200, factor=%d) ═══\n", factor)
	fmt.Println("  Without Rift → non-deterministic result.")
	fmt.Println("  With Rift    → ECC-2 elects deterministic winner every time.")

	cfg := rift.DefaultConfig()
	cfg.RiftFactor = factor
	eng := must(rift.NewEngine(cfg))

	var counter int64
	var wins, losses atomic.Int64
	done := make(chan struct{}, 200)

	for i := 0; i < 200; i++ {
		go func(idx int64) {
			_, err := eng.Run(func() (any, error) {
				v := atomic.AddInt64(&counter, idx+1)
				return v, nil
			})
			if err != nil {
				losses.Add(1)
			} else {
				wins.Add(1)
			}
			done <- struct{}{}
		}(int64(i))
	}
	for range 200 {
		<-done
	}
	fmt.Printf("  wins=%d  losses=%d  final_counter=%d\n", wins.Load(), losses.Load(), counter)
	printMetrics(eng)
}

// ─── Circuit breaker ──────────────────────────────────────────────────────────

func runCircuit() {
	fmt.Println("═══ Circuit Breaker Demo ═══")
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled: true, Threshold: 0.5, Window: 10, CoolDown: 2 * time.Second,
	}
	eng := must(rift.NewEngine(cfg))

	fmt.Println("  Injecting failures to trip the breaker...")
	for i := range 15 {
		_, err := eng.Run(func() (any, error) { return nil, fmt.Errorf("service down") })
		state := eng.Snapshot().CircuitState
		fmt.Printf("  op=%02d err=%v circuit=%s\n", i+1, err != nil, state)
	}

	_, err := eng.Run(func() (any, error) { return "ok", nil })
	fmt.Printf("\n  Post-saturation: err=%v (expect circuit-open)\n", err)
	fmt.Println("  Waiting 2s for cool-down...")
	time.Sleep(2 * time.Second)

	result, err := eng.Run(func() (any, error) { return "recovered", nil })
	if err == nil {
		fmt.Printf("  Probe OK: %v  circuit=%s\n", result.Value, eng.Snapshot().CircuitState)
	} else {
		fmt.Printf("  Probe result: %v\n", err)
	}
	printMetrics(eng)
}

// ─── Adaptive ─────────────────────────────────────────────────────────────────

func runAdaptive() {
	fmt.Println("═══ Adaptive RiftFactor Demo ═══")
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 3
	cfg.Adaptive = rift.AdaptiveConfig{
		Enabled: true, MinFactor: 2, MaxFactor: 8,
		ErrorRateUp: 0.3, ErrorRateDown: 0.05,
	}
	eng := must(rift.NewEngine(cfg))

	phases := []struct {
		name     string
		failRate float64
		ops      int
	}{
		{"healthy", 0.0, 20},
		{"degraded", 0.6, 20},
		{"recovering", 0.1, 20},
		{"healthy", 0.0, 20},
	}

	opNum := 0
	for _, p := range phases {
		fmt.Printf("\n  Phase: %-12s (fail_rate=%.0f%%)\n", p.name, p.failRate*100)
		for range p.ops {
			opNum++
			fail := rand.Float64() < p.failRate
			eng.Run(func() (any, error) { //nolint:errcheck
				if fail {
					return nil, fmt.Errorf("simulated failure")
				}
				return opNum, nil
			})
		}
		m := eng.Snapshot()
		fmt.Printf("  active_factor=%d  error_rate=%.1f%%  circuit=%s\n",
			m.ActiveRiftFactor, m.ErrorRate*100, m.CircuitState)
	}
	printMetrics(eng)
}

// ─── Load shedding (v1.3.0) ───────────────────────────────────────────────────

func runShed() {
	fmt.Println("═══ Load-Shedding Demo (v1.3.0) ═══")
	fmt.Println("  Sending 500 ops into an engine with MaxQueueLen=50.")

	cfg := rift.DefaultConfig()
	cfg.Shed = rift.ShedPolicy{
		Enabled: true, MaxQueueLen: 50, Strategy: "newest",
	}
	eng := must(rift.NewEngine(cfg))

	var processed, shed atomic.Int64
	var wg sync.WaitGroup

	for i := range 500 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := eng.Run(func() (any, error) {
				time.Sleep(time.Millisecond)
				return idx, nil
			})
			if errors.Is(err, rift.ErrShed) {
				shed.Add(1)
			} else {
				processed.Add(1)
			}
		}(i)
	}
	wg.Wait()

	m := eng.Snapshot()
	fmt.Printf("  processed=%d  shed=%d  engine_shed_count=%d\n",
		processed.Load(), shed.Load(), m.ShedOps)
	printMetrics(eng)
}

// ─── Health probe (v1.3.0) ────────────────────────────────────────────────────

func runHealth() {
	fmt.Println("═══ Health Probe Demo (v1.3.0) ═══")

	// Healthy engine
	eng := must(rift.NewEngine(rift.DefaultConfig()))
	h := eng.Health()
	fmt.Printf("  Healthy engine  — live=%v  ready=%v\n", h.Live, h.Ready)

	// Stressed engine
	cfg := rift.DefaultConfig()
	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled: true, Threshold: 0.3, Window: 10, CoolDown: 1 * time.Hour,
	}
	stressedEng := must(rift.NewEngine(cfg))
	for range 20 {
		stressedEng.Run(func() (any, error) { return nil, fmt.Errorf("fail") }) //nolint:errcheck
	}
	h2 := stressedEng.Health()
	fmt.Printf("  Stressed engine — live=%v  ready=%v  reason=%q\n",
		h2.Live, h2.Ready, h2.Reason)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func must(eng *rift.Engine, err error) *rift.Engine {
	if err != nil {
		fmt.Fprintf(os.Stderr, "rift: fatal: %v\n", err)
		os.Exit(1)
	}
	return eng
}

func printMetrics(eng *rift.Engine) {
	m := eng.Snapshot()
	fmt.Printf("\n─── Metrics ─────────────────────────────────────────────\n")
	fmt.Printf("  ops:     total=%d  success=%d  failed=%d  shed=%d\n",
		m.TotalOperations, m.SuccessfulOps, m.FailedOps, m.ShedOps)
	fmt.Printf("  rifts:   spawned=%d  pruned=%d\n",
		m.TotalRiftsSpawned, m.TotalRiftsPruned)
	fmt.Printf("  engine:  latency=%.2fms  entropy=%.4f  error_rate=%.1f%%\n",
		m.MeanLatencyMs, m.MeanEntropyDelta, m.ErrorRate*100)
	fmt.Printf("  circuit: %s  active_factor=%d  strategy=%s\n",
		m.CircuitState, m.ActiveRiftFactor, m.FusionStrategy)
}
