// Package executor runs Rifts concurrently using a bounded goroutine pool.
// No global mutex is used. Each Rift executes in complete isolation.
// Panics inside Fn are caught and converted to errors, never propagated.
package executor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rift-engine/rift/internal/clock"
	r "github.com/rift-engine/rift/internal/rift"
)

// ─── Executor ─────────────────────────────────────────────────────────────────

// Executor implements r.Executor using a semaphore-bounded goroutine pool.
type Executor struct {
	sem          chan struct{} // semaphore: limits concurrent goroutines
	clock        *clock.Provider
	defaultTimeout time.Duration
}

// New creates an Executor.
// workers = 0 → GOMAXPROCS.
func New(workers int, clk *clock.Provider, defaultTimeout time.Duration) *Executor {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	return &Executor{
		sem:            make(chan struct{}, workers),
		clock:          clk,
		defaultTimeout: defaultTimeout,
	}
}

// Execute launches all rifts concurrently and waits for all to converge
// (or fail). The call blocks until every rift has reached a terminal state.
// No error is returned here — individual rift errors are stored on each Rift.
func (e *Executor) Execute(rifts []*r.Rift) error {
	var wg sync.WaitGroup
	for _, rift := range rifts {
		wg.Add(1)
		go func(rft *r.Rift) {
			defer wg.Done()
			e.run(rft)
		}(rift)
	}
	wg.Wait()
	return nil
}

// run executes a single Rift inside the semaphore boundary.
func (e *Executor) run(rft *r.Rift) {
	// Assign pre-execution causal clock tick.
	rft.Clock = e.clock.Tick(rft.ID)

	// Acquire semaphore slot (blocks if pool is full).
	e.sem <- struct{}{}
	defer func() { <-e.sem }()

	// Transition to running.
	rft.Transition(r.StateRunning)
	rft.StartedAt = time.Now()

	// Determine timeout.
	timeout := rft.Timeout
	if timeout <= 0 {
		timeout = e.defaultTimeout
	}

	// Execute with timeout context.
	result, err := e.execWithTimeout(rft.Fn, timeout)

	// Convergence: record result and stamp wall clock.
	execDuration := time.Since(rft.StartedAt)
	rft.Converge(result, err)

	// Finalize causal clock with heuristics now that we know the outcome.
	e.clock.Finalize(&rft.Clock, execDuration, err == nil)
}

// execWithTimeout runs fn with a deadline. Panics inside fn are recovered
// and returned as errors so they cannot crash the executor pool.
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
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				done <- outcome{nil, fmt.Errorf("rift: panic recovered: %v\n%s", p, buf[:n])}
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
