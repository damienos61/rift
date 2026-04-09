// Example: HFT trading order processor using Rift.
// Demonstrates how Rift eliminates race conditions in a high-frequency
// trading scenario where order arrival order is non-deterministic.
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/damienos61/rift"
)

type Order struct {
	ID       int
	Symbol   string
	Quantity int
	Price    float64
}

func processOrder(order Order) (any, error) {
	// Simulate variable network/exchange latency.
	latency := time.Duration(rand.Intn(5)) * time.Millisecond
	time.Sleep(latency)

	// Simulate occasional transient failures (5% rate).
	if rand.Float64() < 0.05 {
		return nil, fmt.Errorf("exchange timeout for order %d", order.ID)
	}

	return fmt.Sprintf("ORDER_%d FILLED @ %.2f x %d", order.ID, order.Price, order.Quantity), nil
}

func main() {
	cfg := rift.DefaultConfig()
	cfg.RiftFactor = 4 // 4 variants for HFT — wider coverage, still fast
	cfg.DefaultTimeout = 50 * time.Millisecond

	eng, err := rift.NewEngine(cfg)
	if err != nil {
		panic(err)
	}

	orders := []Order{
		{1, "AAPL", 100, 189.42},
		{2, "MSFT", 250, 415.10},
		{3, "NVDA", 50, 875.33},
		{4, "TSLA", 400, 177.85},
		{5, "GOOG", 30, 171.95},
	}

	fmt.Println("Rift HFT Order Processor — processing 5 orders with 4 causal variants each")
	fmt.Println(string(make([]byte, 70)))

	for _, order := range orders {
		o := order // capture
		start := time.Now()
		result, err := eng.Run(func() (any, error) {
			return processOrder(o)
		})
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("ORDER %d  FAILED  (%v)\n", o.ID, err)
			continue
		}
		fmt.Printf("ORDER %d  %-40v  score=%.3f  %v\n",
			o.ID, result.Value, result.CausalScore, elapsed.Round(time.Microsecond))
	}

	m := eng.Snapshot()
	fmt.Printf("\n%d orders processed, %d rifts spawned, %d pruned\n",
		m.TotalOperations, m.TotalRiftsSpawned, m.TotalRiftsPruned)
}
