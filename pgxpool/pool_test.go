package pgxpool

import (
	"context"
	"testing"
	"time"
)

func TestAcquireCancelRace(t *testing.T) {
	p := NewPool(1)
	
	// Exhaust pool
	c1, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	go func() {
		time.Sleep(time.Millisecond * 10)
		p.Release(c1)
	}()

	_, err = p.Acquire(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Pool should recover and not leak
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel2()
	
	c2, err := p.Acquire(ctx2)
	if err != nil {
		t.Fatalf("failed to acquire after cancellation: %v", err)
	}
	p.Release(c2)
}
