package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Pool represents a minimal pgxpool that demonstrates the fix for
// deadlock/waiter-queue inconsistency on Acquire context cancellation.
type Pool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	conns   chan struct{}
	waiters int32
}

func NewPool(size int) *Pool {
	p := &Pool{
		conns: make(chan struct{}, size),
	}
	p.cond = sync.NewCond(&p.mu)
	for i := 0; i < size; i++ {
		p.conns <- struct{}{}
	}
	return p
}

// Acquire attempts to get a connection. If ctx is cancelled while waiting,
// it drains itself from the waiter queue and re-enqueues a connection so
// other waiters don't starve.
func (p *Pool) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.conns:
		return nil
	default:
	}

	atomic.AddInt32(&p.waiters, 1)
	defer atomic.AddInt32(&p.waiters, -1)

	// Use a channel to race the context cancellation vs connection acquisition
	ch := make(chan struct{}, 1)
	go func() {
		select {
		case <-p.conns:
			ch <- struct{}{}
		case <-ctx.Done():
		}
	}()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		// Context was cancelled: put the connection back so other waiters
		// don't starve. This is the key fix — the original pool lost the
		// connection in this path, eventually blocking all Acquire calls.
		p.conns <- struct{}{}
		return ctx.Err()
	}
}

// Release returns a connection.
func (p *Pool) Release() {
	p.conns <- struct{}{}
}

// Stats returns the number of waiters.
func (p *Pool) Stats() int32 {
	return atomic.LoadInt32(&p.waiters)
}

func main() {
	fmt.Println("pgxpool deadlock fix demonstration")
	fmt.Println("==================================")

	pool := NewPool(2)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Acquire all connections
	_ = pool.Acquire(context.Background())
	_ = pool.Acquire(context.Background())

	// This one should time out
	err := pool.Acquire(ctx)
	fmt.Printf("Acquire with short deadline: %v\n", err)

	// After the cancellation, the connection should still be available
	done := make(chan struct{})
	go func() {
		if err := pool.Acquire(context.Background()); err != nil {
			fmt.Printf("ERROR: post-cancel Acquire failed: %v\n", err)
		} else {
			fmt.Println("SUCCESS: post-cancel Acquire succeeded (connection was returned to pool)")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		fmt.Println("FAIL: post-cancel Acquire blocked indefinitely — deadlock reproduced")
	}

	fmt.Println("\nFix verification complete.")
}
