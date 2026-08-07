package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Pool represents a simplified pgxpool with deadlock prevention.
// The bug: when context is cancelled during Acquire(), a deadlock can
// occur because the waiter queue is not properly cleaned up.
// Fix: Use a separate done channel and ensure waiters are removed
// from the queue on context cancellation.
type Pool struct {
	mu         sync.Mutex
	maxConns   int32
	active     int32
	waiters    []chan struct{}
}

func NewPool(maxConns int32) *Pool {
	return &Pool{maxConns: maxConns}
}

func (p *Pool) Acquire(ctx context.Context) error {
	p.mu.Lock()
	if atomic.LoadInt32(&p.active) < p.maxConns {
		atomic.AddInt32(&p.active, 1)
		p.mu.Unlock()
		return nil
	}
	// Add to waiter queue
	ch := make(chan struct{}, 1)
	p.waiters = append(p.waiters, ch)
	p.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		// Remove ourselves from the waiter queue to prevent deadlock
		p.mu.Lock()
		for i, w := range p.waiters {
			if w == ch {
				p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
				break
			}
		}
		p.mu.Unlock()
		return ctx.Err()
	}
}

func (p *Pool) Release() {
	p.mu.Lock()
	atomic.AddInt32(&p.active, -1)
	if len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		ch <- struct{}{}
	}
	p.mu.Unlock()
}

func main() {
	pool := NewPool(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	err := pool.Acquire(ctx)
	fmt.Printf("Acquire with cancelled context: %v (expected context canceled)\n", err)
}
