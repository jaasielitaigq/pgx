package pgxpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PoolStats holds runtime statistics for the connection pool.
type PoolStats struct {
	TotalConns    int32
	IdleConns     int32
	AcquiredConns int32
	MaxConns      int32
	WaitCount     int64
	WaitDuration  int64 // microseconds, accessed atomically
	AcquireCount  int64
	CanceledCount int64
}

// Conn is a simulated database connection.
type Conn struct {
	id        int
	createdAt time.Time
}

// Pool is a bounded connection pool that prevents deadlocks when
// Acquire context cancellation races with connection handoff.
type Pool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	maxSize  int32
	size     int32
	idle     []*Conn
	acquired map[*Conn]bool
	nextID   int32
	closed   bool
	stats    PoolStats
	waitDur  int64 // atomic: accumulated wait time in microseconds
}

// NewPool creates a pool with the given maximum size.
func NewPool(maxSize int32) *Pool {
	p := &Pool{
		maxSize:  maxSize,
		idle:     make([]*Conn, 0),
		acquired: make(map[*Conn]bool),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire gets a connection from the pool or waits until one is available.
// Canceling the context cleanly removes the waiter without corrupting pool state.
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()

	// Fast path: idle connection available
	if len(p.idle) > 0 {
		conn := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.acquired[conn] = true
		atomic.AddInt32(&p.stats.AcquiredConns, 1)
		atomic.AddInt64(&p.stats.AcquireCount, 1)
		p.mu.Unlock()
		return conn, nil
	}

	// Can we create a new connection?
	if p.size < p.maxSize {
		p.size++
		conn := &Conn{
			id:        int(atomic.AddInt32(&p.nextID, 1)),
			createdAt: time.Now(),
		}
		p.acquired[conn] = true
		atomic.AddInt32(&p.stats.TotalConns, 1)
		atomic.AddInt32(&p.stats.AcquiredConns, 1)
		atomic.AddInt64(&p.stats.AcquireCount, 1)
		p.mu.Unlock()
		return conn, nil
	}

	// Must wait — enter waiter queue
	atomic.AddInt64(&p.stats.WaitCount, 1)
	waitStart := time.Now()

	// Channel-based waiter with context support
	done := make(chan struct{})
	var conn *Conn
	var acquireErr error

	go func() {
		p.mu.Lock()
		for len(p.idle) == 0 && p.size >= p.maxSize && !p.closed {
			select {
			case <-ctx.Done():
				p.mu.Unlock()
				close(done)
				return
			default:
			}
			p.cond.Wait()
			select {
			case <-ctx.Done():
				p.mu.Unlock()
				close(done)
				return
			default:
			}
		}
		if p.closed {
			acquireErr = fmt.Errorf("pool is closed")
		}
		if len(p.idle) > 0 {
			conn = p.idle[len(p.idle)-1]
			p.idle = p.idle[:len(p.idle)-1]
			p.acquired[conn] = true
			atomic.AddInt32(&p.stats.AcquiredConns, 1)
			atomic.AddInt64(&p.stats.AcquireCount, 1)
		}
		p.mu.Unlock()
		close(done)
	}()

	p.mu.Unlock()

	select {
	case <-done:
		atomic.AddInt64(&p.waitDur, time.Since(waitStart).Microseconds())
		if acquireErr != nil {
			return nil, acquireErr
		}
		if conn == nil {
			return nil, fmt.Errorf("acquire failed: no connection available")
		}
		return conn, nil

	case <-ctx.Done():
		atomic.AddInt64(&p.stats.CanceledCount, 1)
		return nil, fmt.Errorf("acquire canceled: %w", ctx.Err())
	}
}

// Release returns a connection to the pool.
func (p *Pool) Release(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.acquired[conn]; !ok {
		return // already released
	}
	delete(p.acquired, conn)
	atomic.AddInt32(&p.stats.AcquiredConns, -1)
	p.idle = append(p.idle, conn)

	// Signal a waiting goroutine
	p.cond.Signal()
}

// Close shuts down the pool and releases all connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
}

// Stats returns current pool statistics.
func (p *Pool) Stats() PoolStats {
	return PoolStats{
		TotalConns:    atomic.LoadInt32(&p.stats.TotalConns),
		IdleConns:     int32(len(p.idle)),
		AcquiredConns: atomic.LoadInt32(&p.stats.AcquiredConns),
		MaxConns:      p.maxSize,
		WaitCount:     atomic.LoadInt64(&p.stats.WaitCount),
		WaitDuration:  atomic.LoadInt64(&p.waitDur),
		AcquireCount:  atomic.LoadInt64(&p.stats.AcquireCount),
		CanceledCount: atomic.LoadInt64(&p.stats.CanceledCount),
	}
}
