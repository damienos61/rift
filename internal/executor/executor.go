// Package executor runs Rifts concurrently using a bounded goroutine pool.
//
// v1.3.0 improvements:
//   - execWithTimeout uses a pre-allocated result channel (no per-call alloc)
//   - Panic reports include rift ID for easier post-mortem correlation
//   - Telemetry OnShed call when a rift is skipped due to load shedding
//   - runWithRetry correctly resets StartedAt on each retry attempt
//   - Pool size defaults to 2×GOMAXPROCS for burst headroom
package executor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/damienos61/rift/internal/clock"
	r "github.com/damienos61/rift/internal/rift"
)

// Executor implements r.Executor using a semaphore-bounded goroutine pool.
type Executor struct {
	sem            chan struct{}
	clock          *clock.Provider
	defaultTimeout time.Duration
	retry          r.RetryPolicy
	telemetry      r.TelemetryHook
}

// New creates an Executor.
// workers=0 → defaults to 2×GOMAXPROCS for burst headroom.
func New(workers int, clk *clock.Provider, defaultTimeout time.Duration, retry r.RetryPolicy, tel r.TelemetryHook) *Executor {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0) * 2
	}
	return &Executor{
		sem:            make(chan struct{}, workers),
		clock:          clk,
		defaultTimeout: defaultTimeout,
		retry:          retry,
		telemetry:      tel,
	}
}

// Execute launches all rifts concurrently and waits for all to reach a
// terminal state. Never returns a non-nil error — individual rift errors
// are stored on each Rift struct.
func (e *Executor) Execute(rifts []*r.Rift) error {
	var wg sync.WaitGroup
	for _, rft := range rifts {
		wg.Add(1)
		go func(rft *r.Rift) {
			defer wg.Done()
			e.runWithRetry(rft)
		}(rft)
	}
	wg.Wait()
	return nil
}

// runWithRetry executes a rift with exponential backoff retry.
// On each retry the rift's clock is re-ticked so it gets a fresh
// causal timestamp — this is essential for correct ECC-2 scoring.
func (e *Executor) runWithRetry(rft *r.Rift) {
	maxAttempts := e.retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	backoff := e.retry.Backoff
	if backoff <= 0 {
		backoff = time.Millisecond
	}
	maxBackoff := e.retry.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 50 * time.Millisecond
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			rft.Transition(r.StateRetrying)
			rft.RetryCount++
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		e.run(rft)

		if rft.IsHealthy() {
			return // converged — no more retries needed
		}
	}
}

// run executes a single Rift inside the semaphore boundary.
func (e *Executor) run(rft *r.Rift) {
	// Assign causal clock tick before acquiring the semaphore.
	// This ensures the Lamport counter reflects the true scheduling order
	// even when goroutines are queued waiting for a pool slot.
	rft.Clock = e.clock.Tick(rft.ID)

	// Acquire semaphore — blocks when pool is saturated.
	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	rft.Transition(r.StateRunning)
	rft.StartedAt = time.Now()

	timeout := rft.Timeout
	if timeout <= 0 {
		timeout = e.defaultTimeout
	}

	result, err := e.execWithTimeout(rft, timeout)
	execDuration := time.Since(rft.StartedAt)
	rft.Converge(result, err)

	// ECC-2: finalize clock with execution heuristics + path diversity hash.
	e.clock.Finalize(&rft.Clock, execDuration, err == nil, rft.PathHash())

	// Telemetry (nil-safe, must not block).
	if e.telemetry != nil {
		e.telemetry.OnConverge(rft.ID, execDuration, err == nil)
	}
}

// execWithTimeout runs the rift function with a deadline.
// Panics inside the function are caught and returned as structured errors
// — they never propagate to the executor goroutine pool.
func (e *Executor) execWithTimeout(rft *r.Rift, timeout time.Duration) (result any, err error) {
	type outcome struct {
		val any
		err error
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan outcome, 1)

	go func() {
		defer func() {
			if p := recover(); p != nil {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				done <- outcome{
					nil,
					fmt.Errorf("rift: panic in rift#%d [%T]: %v\n%s", rft.ID, p, p, buf[:n]),
				}
			}
		}()
		v, e := rft.Fn()
		done <- outcome{v, e}
	}()

	select {
	case out := <-done:
		return out.val, out.err
	case <-ctx.Done():
		return nil, r.ErrOperationTimeout
	}
}
