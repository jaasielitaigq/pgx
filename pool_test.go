package pgxpool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPoolAcquireRelease(t *testing.T) {
	pool := NewPool(2)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	pool.Release(conn)
	stats := pool.Stats()
	if stats.IdleConns != 1 {
		t.Errorf("IdleConns = %d, want 1", stats.IdleConns)
	}
}

func TestPoolMaxSizeEnforced(t *testing.T) {
	pool := NewPool(2)
	defer pool.Close()

	c1, _ := pool.Acquire(context.Background())
	c2, _ := pool.Acquire(context.Background())

	// Third acquire should block — use short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := pool.Acquire(ctx)
	if err == nil {
		t.Fatal("expected timeout error when pool is full")
	}

	// Release one, acquire should succeed
	pool.Release(c1)
	c3, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	_ = c2
	_ = c3
}

func TestPoolAcquireCancelStress(t *testing.T) {
	// Reproduce the deadlock scenario: high concurrent acquires with
	// aggressive timeouts against a small pool.
	pool := NewPool(3)
	defer pool.Close()

	var wg sync.WaitGroup
	errorsCh := make(chan error, 200)
	successCh := make(chan struct{}, 200)

	// Hold connections to saturate the pool
	held := make([]*Conn, 3)
	for i := 0; i < 3; i++ {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Hold acquire %d: %v", i, err)
		}
		held[i] = c
	}

	// Spawn goroutines trying to acquire with aggressive timeouts
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for attempt := 0; attempt < 5; attempt++ {
				timeout := time.Duration(1+attempt%3) * time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				conn, err := pool.Acquire(ctx)
				cancel()
				if err != nil {
					errorsCh <- err
				} else {
					successCh <- struct{}{}
					time.Sleep(time.Microsecond)
					pool.Release(conn)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorsCh)
	close(successCh)

	errorCount := len(errorsCh)
	successCount := len(successCh)
	t.Logf("Errors: %d, Success: %d", errorCount, successCount)

	// Release held connections
	for _, c := range held {
		pool.Release(c)
	}

	// Pool should be functional after stress
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Pool not functional after stress: %v", err)
	}
	pool.Release(conn)

	// Verify stats are consistent
	stats := pool.Stats()
	t.Logf("Stats: total=%d idle=%d acquired=%d canceled=%d",
		stats.TotalConns, stats.IdleConns, stats.AcquiredConns, stats.CanceledCount)

	if stats.TotalConns != 3 {
		t.Errorf("TotalConns = %d, want 3", stats.TotalConns)
	}
}

func TestPoolCancelNoDeadlock(t *testing.T) {
	// The key regression test: verify that cancelling many waiters
	// does not deadlock the pool.
	pool := NewPool(2)
	defer pool.Close()

	// Saturate pool
	c1, _ := pool.Acquire(context.Background())
	c2, _ := pool.Acquire(context.Background())

	// Spawn waiters that will all cancel
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			pool.Acquire(ctx) // expected to fail with deadline exceeded
		}()
	}
	wg.Wait()

	// Release connections — pool should work
	pool.Release(c1)
	pool.Release(c2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Pool deadlocked after cancellations: %v", err)
	}
	pool.Release(conn)
}

func TestPoolStatsConsistency(t *testing.T) {
	pool := NewPool(5)
	defer pool.Close()

	conns := make([]*Conn, 0, 5)
	for i := 0; i < 5; i++ {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		conns = append(conns, c)
	}

	stats := pool.Stats()
	if stats.AcquiredConns != 5 {
		t.Errorf("AcquiredConns = %d, want 5", stats.AcquiredConns)
	}
	if stats.IdleConns != 0 {
		t.Errorf("IdleConns = %d, want 0", stats.IdleConns)
	}

	for _, c := range conns {
		pool.Release(c)
	}

	stats = pool.Stats()
	if stats.AcquiredConns != 0 {
		t.Errorf("AcquiredConns = %d, want 0", stats.AcquiredConns)
	}
	if stats.IdleConns != 5 {
		t.Errorf("IdleConns = %d, want 5", stats.IdleConns)
	}
}

func TestPoolCloseReleasesWaiters(t *testing.T) {
	pool := NewPool(1)
	c1, _ := pool.Acquire(context.Background())

	// Start a waiter
	waiterDone := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(context.Background())
		waiterDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	pool.Release(c1)
	pool.Close()

	select {
	case err := <-waiterDone:
		if err == nil {
			t.Log("Waiter got connection before close")
		} else {
			t.Logf("Waiter error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Waiter not released after Close")
	}
}

func TestPoolDoubleRelease(t *testing.T) {
	pool := NewPool(1)
	defer pool.Close()

	conn, _ := pool.Acquire(context.Background())
	pool.Release(conn)
	pool.Release(conn) // should not panic or corrupt state

	stats := pool.Stats()
	if stats.IdleConns != 1 {
		t.Errorf("IdleConns = %d, want 1 after double release", stats.IdleConns)
	}
}
