// Package main demonstrates Rift v1.3.0 in an HFT order processing scenario.
//
// This example showcases:
//   - CircuitBreaker protecting the order pipeline under market stress
//   - AdaptiveRiftFactor scaling under error spikes (simulating volatility)
//   - RetryPolicy recovering from transient exchange connectivity failures
//   - Entropy-aware fusion for maximum causal path diversity
//   - Load-shedding via ShedPolicy when order flow exceeds engine capacity
//   - TelemetryHook capturing spans for latency and error monitoring
//   - HealthProbe for readiness checks before starting order flow
package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/damienos61/rift"
)

// ─── Mock order types ─────────────────────────────────────────────────────────

type Order struct {
	ID     int64
	Symbol string
	Qty    int
	Price  float64
}

type Fill struct {
	OrderID   int64
	FillPrice float64
	Timestamp time.Time
}

// ─── Mock exchange ────────────────────────────────────────────────────────────

type mockExchange struct {
	failRate float64
}

func (e *mockExchange) Submit(o Order) (Fill, error) {
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	if rand.Float64() < e.failRate {
		return Fill{}, fmt.Errorf("exchange: transient connectivity error")
	}
	return Fill{
		OrderID:   o.ID,
		FillPrice: o.Price * (1 + (rand.Float64()-0.5)*0.001),
		Timestamp: time.Now(),
	}, nil
}

// ─── Telemetry hook ───────────────────────────────────────────────────────────

type tradingTelemetry struct {
	splits    atomic.Int64
	converges atomic.Int64
	fuses     atomic.Int64
	errors    atomic.Int64
	sheds     atomic.Int64 // v1.3.0
	totalLat  atomic.Int64
}

func (t *tradingTelemetry) OnSplit(_ rift.OperationID, _ int)    { t.splits.Add(1) }
func (t *tradingTelemetry) OnError(_ rift.OperationID, _ error) { t.errors.Add(1) }
func (t *tradingTelemetry) OnShed(_ rift.OperationID)           { t.sheds.Add(1) } // v1.3.0
func (t *tradingTelemetry) OnConverge(_ rift.RiftID, d time.Duration, _ bool) {
	t.converges.Add(1)
	t.totalLat.Add(d.Nanoseconds())
}
func (t *tradingTelemetry) OnFuse(_ rift.OperationID, _ rift.RiftID, _ float64, _ int) {
	t.fuses.Add(1)
}
func (t *tradingTelemetry) MeanLatencyMs() float64 {
	c := t.converges.Load()
	if c == 0 {
		return 0
	}
	return float64(t.totalLat.Load()) / float64(c) / 1e6
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	fmt.Println("═══ Rift v1.3.0 — HFT Order Processor Demo ═══")
	fmt.Println()

	tel := &tradingTelemetry{}
	exchange := &mockExchange{failRate: 0.15}

	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 3
	cfg.FusionStrategy = "causal"
	cfg.DefaultTimeout = 20 * time.Millisecond

	cfg.CircuitBreaker = rift.CircuitBreakerConfig{
		Enabled:   true,
		Threshold: 0.6,
		Window:    20,
		CoolDown:  500 * time.Millisecond,
	}
	cfg.Retry = rift.RetryPolicy{
		MaxAttempts: 2,
		Backoff:     1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}
	cfg.Adaptive = rift.AdaptiveConfig{
		Enabled:       true,
		MinFactor:     2,
		MaxFactor:     6,
		ErrorRateUp:   0.3,
		ErrorRateDown: 0.05,
	}
	// v1.3.0: shed excess orders if engine is saturated
	cfg.Shed = rift.ShedPolicy{
		Enabled:     true,
		MaxQueueLen: 500,
		Strategy:    "newest",
	}
	cfg.Telemetry = tel

	eng, err := rift.NewEngine(cfg)
	if err != nil {
		panic(err)
	}

	// v1.3.0: health check before starting order flow
	health := eng.Health()
	fmt.Printf("Engine health — live=%v ready=%v\n\n", health.Live, health.Ready)

	symbols := []string{"AAPL", "MSFT", "NVDA", "TSLA", "AMZN"}
	var orderID int64
	var totalFills, totalErrors, totalSheds int

	fmt.Printf("%-6s %-6s %-8s %-10s %-8s %-8s %s\n",
		"ORDER", "SYMBOL", "FILL_PX", "SCORE", "ENTROPY", "LAT", "STATUS")
	fmt.Println("────────────────────────────────────────────────────────────────")

	for i := 0; i < 20; i++ {
		id := atomic.AddInt64(&orderID, 1)
		o := Order{
			ID:     id,
			Symbol: symbols[rand.Intn(len(symbols))],
			Qty:    100 + rand.Intn(900),
			Price:  100 + rand.Float64()*900,
		}

		start := time.Now()
		result, err := eng.Run(func() (any, error) {
			return exchange.Submit(o)
		})
		elapsed := time.Since(start)

		switch {
		case err == rift.ErrShed:
			totalSheds++
			m := eng.Snapshot()
			fmt.Printf("%-6d %-6s %-8s %-10s %-8s %-8s SHED circuit=%s\n",
				id, o.Symbol, "—", "—", "—",
				elapsed.Round(time.Millisecond), m.CircuitState)
		case err != nil:
			totalErrors++
			m := eng.Snapshot()
			fmt.Printf("%-6d %-6s %-8s %-10s %-8s %-8s ERR=%v circuit=%s\n",
				id, o.Symbol, "—", "—", "—",
				elapsed.Round(time.Millisecond), err, m.CircuitState)
		default:
			totalFills++
			fill := result.Value.(Fill)
			fmt.Printf("%-6d %-6s %-8.2f %-10.4f %-8.4f %-8s OK\n",
				id, o.Symbol, fill.FillPrice, result.CausalScore,
				result.EntropyDelta, elapsed.Round(time.Millisecond))
		}
		time.Sleep(10 * time.Millisecond)
	}

	m := eng.Snapshot()
	fmt.Printf("\n═══ Session Summary ════════════════════════════════\n")
	fmt.Printf("  Orders: %d fills | %d errors | %d shed\n", totalFills, totalErrors, totalSheds)
	fmt.Printf("  Rifts spawned: %d | pruned: %d\n", m.TotalRiftsSpawned, m.TotalRiftsPruned)
	fmt.Printf("  Mean latency: %.2fms | error rate: %.1f%%\n", m.MeanLatencyMs, m.ErrorRate*100)
	fmt.Printf("  Active factor: %d | circuit: %s\n", m.ActiveRiftFactor, m.CircuitState)
	fmt.Printf("  Mean entropy delta: %.4f\n", m.MeanEntropyDelta)
	fmt.Printf("  Telemetry — splits=%d converges=%d fuses=%d errors=%d sheds=%d mean_lat=%.2fms\n",
		tel.splits.Load(), tel.converges.Load(), tel.fuses.Load(),
		tel.errors.Load(), tel.sheds.Load(), tel.MeanLatencyMs())

	// v1.3.0: final health check
	health = eng.Health()
	fmt.Printf("  Final health — live=%v ready=%v\n", health.Live, health.Ready)
}
