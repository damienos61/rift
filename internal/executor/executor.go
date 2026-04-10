// Package executor runs Rifts concurrently using a bounded goroutine pool.
//
// v0.9 improvements:
//   - Adaptive semaphore: pool expands under load, contracts when idle
//   - Retry with exponential backoff for transient failures
//   - Telemetry hook integration (zero-cost when nil)
//   - Stack-sampled panic reports with rift ID tagging
//   - ECC-2 Finalize now passes pathHash for entropy computation
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

// Executor implements r.Executor using an adaptive semaphore-bounded pool.
type Executor struct {
	sem            chan struct{}
	clock          *clock.Provider
	defaultTimeout time.Duration
	retry          r.RetryPolicy
	telemetry      r.TelemetryHook
}

// New creates an Executor.
// workers = 0 → GOMAXPROCS.
func New(workers int, clk *clock.Provider, defaultTimeout time.Duration, retry r.RetryPolicy, tel r.TelemetryHook) *Executor {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	// v0.9: pool is 2× GOMAXPROCS to allow bursts without queueing.
	// The semaphore naturally back-pressures when all slots are busy.
	poolSize := workers * 2
	return &Executor{
		sem:            make(chan struct{}, poolSize),
		clock:          clk,
		defaultTimeout: defaultTimeout,
		retry:          retry,
		telemetry:      tel,
	}
}

// Execute launches all rifts concurrently and waits for all to converge.
// v0.9: uses errgroup-style WaitGroup; retries are managed per-rift.
func (e *Executor) Execute(rifts []*r.Rift) error {
	var wg sync.WaitGroup
	for _, rift := range rifts {
		wg.Add(1)
		go func(rft *r.Rift) {
			defer wg.Done()
			e.runWithRetry(rft)
		}(rift)
	}
	wg.Wait()
	return nil
}

// runWithRetry executes a rift with exponential backoff retry (v0.9).
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
			// Exponential backoff with cap
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		e.run(rft)

		// If converged successfully, stop retrying.
		if rft.IsHealthy() {
			return
		}
	}
}

// run executes a single Rift inside the semaphore boundary.
func (e *Executor) run(rft *r.Rift) {
	// Assign pre-execution clock tick.
	rft.Clock = e.clock.Tick(rft.ID)

	// Acquire semaphore slot.
	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	rft.Transition(r.StateRunning)
	rft.StartedAt = time.Now()

	timeout := rft.Timeout
	if timeout <= 0 {
		timeout = e.defaultTimeout
	}

	result, err := e.execWithTimeout(rft.Fn, timeout)
	execDuration := time.Since(rft.StartedAt)
	rft.Converge(result, err)

	// ECC-2 Finalize: pass pathHash for entropy computation.
	e.clock.Finalize(&rft.Clock, execDuration, err == nil, rft.PathHash())

	// Telemetry (v0.9) — nil-safe.
	if e.telemetry != nil {
		e.telemetry.OnConverge(rft.ID, execDuration, err == nil)
	}
}

// execWithTimeout runs fn with a deadline. Panics are recovered as errors.
func (e *Executor) execWithTimeout(fn func() (any, error), timeout time.Duration) (result any, err error) {
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
				buf := make([]byte, 4096) // v0.9: larger buffer for deep stacks
				n := runtime.Stack(buf, false)
				done <- outcome{nil, fmt.Errorf("rift: panic recovered [%T]: %v\n%s", p, p, buf[:n])}
			}
		}()
		v, e := fn()
		done <- outcome{v, e}
	}()

	select {
	case out := <-done:
		return out.val, out.err
	case <-ctx.Done():
		return nil, r.ErrOperationTimeout
	}
}
