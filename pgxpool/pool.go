package pgxpool

import (
	"context"
	"sync"
)

type Conn struct {
	ID int
}

type Pool struct {
	mu          sync.Mutex
	idle        []*Conn
	waiters     []chan *Conn
	activeConns int
	maxConns    int
}

func NewPool(max int) *Pool {
	return &Pool{maxConns: max}
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	if len(p.idle) > 0 {
		c := p.idle[0]
		p.idle = p.idle[1:]
		p.activeConns++
		p.mu.Unlock()
		return c, nil
	}
	if p.activeConns < p.maxConns {
		c := &Conn{ID: p.activeConns + 1}
		p.activeConns++
		p.mu.Unlock()
		return c, nil
	}

	ch := make(chan *Conn, 1)
	p.waiters = append(p.waiters, ch)
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		p.mu.Lock()
		// Remove from waiters cleanly
		for i, w := range p.waiters {
			if w == ch {
				p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
				break
			}
		}
		// Safely return connection if it was assigned right at cancellation
		select {
		case c := <-ch:
			p.idle = append(p.idle, c)
			p.activeConns--
		default:
		}
		p.mu.Unlock()
		return nil, ctx.Err()
	case c := <-ch:
		return c, nil
	}
}

func (p *Pool) Release(c *Conn) {
	p.mu.Lock()
	if len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		p.mu.Unlock()
		w <- c
		return
	}
	p.idle = append(p.idle, c)
	p.activeConns--
	p.mu.Unlock()
}
