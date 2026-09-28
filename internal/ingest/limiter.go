package ingest

import (
	"sync"
	"time"
)

type Limiter struct {
	mu                  sync.Mutex
	rate, burst, tokens float64
	last                time.Time
}

func NewLimiter(rate, burst int) *Limiter {
	if burst < 1 {
		burst = 1
	}
	return &Limiter{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

func (l *Limiter) Allow() bool {
	if l == nil || l.rate <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.rate
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
