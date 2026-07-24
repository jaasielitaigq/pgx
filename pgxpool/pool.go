package pgxpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAcquireCanceled = errors.New("pgxpool: acquire canceled")

type Conn struct {
	ID string
}

type Pool struct {
	mu           sync.Mutex
	maxConns     int32
	idleConns    []*Conn
	acquiredCount int32
	waiterCount  int32
}

func NewPool(maxConns int32) *Pool {
	return &Pool{
		maxConns:  maxConns,
		idleConns: make([]*Conn, 0),
	}
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	atomic.AddInt32(&p.waiterCount, 1)
	defer atomic.AddInt32(&p.waiterCount, -1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	p.mu.Lock()
	if len(p.idleConns) > 0 {
		conn := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		atomic.AddInt32(&p.acquiredCount, 1)
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.Release(conn)
			return nil, ctx.Err()
		default:
			return conn, nil
		}
	}

	if atomic.LoadInt32(&p.acquiredCount) < p.maxConns {
		conn := &Conn{ID: "conn-" + time.Now().Format("150405.000")}
		atomic.AddInt32(&p.acquiredCount, 1)
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.Release(conn)
			return nil, ctx.Err()
		default:
			return conn, nil
		}
	}
	p.mu.Unlock()

	return nil, ErrAcquireCanceled
}

func (p *Pool) Release(conn *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.AddInt32(&p.acquiredCount, -1)
	p.idleConns = append(p.idleConns, conn)
}

func (p *Pool) Stats() (idle, acquired, waiters int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int32(len(p.idleConns)), atomic.LoadInt32(&p.acquiredCount), atomic.LoadInt32(&p.waiterCount)
}
