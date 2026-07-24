package pgxpool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPool_Acquire_ContextCancellation_Safety(t *testing.T) {
	pool := NewPool(2)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
			defer cancel()
			conn, err := pool.Acquire(ctx)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				pool.Release(conn)
			}
		}()
	}

	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire failed after cancellation stress: %v", err)
	}
	pool.Release(conn)
}
