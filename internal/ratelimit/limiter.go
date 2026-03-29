package ratelimit

import (
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
)

type Limiter struct {
	attempts map[string][]time.Time
	mu       sync.Mutex
	max      int
	window   time.Duration
}

func New(max int, window time.Duration) *Limiter {
	return &Limiter{
		attempts: make(map[string][]time.Time),
		max:      max,
		window:   window,
	}
}

func (l *Limiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-l.window)

	// Filter to recent attempts only
	recent := l.attempts[ip][:0]
	for _, t := range l.attempts[ip] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}

	if len(recent) >= l.max {
		l.attempts[ip] = recent
		return false
	}

	l.attempts[ip] = append(recent, now)
	return true
}

func (l *Limiter) Cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := time.Now().Add(-l.window)
	for ip, times := range l.attempts {
		recent := times[:0]
		for _, t := range times {
			if t.After(cutoff) {
				recent = append(recent, t)
			}
		}
		if len(recent) == 0 {
			delete(l.attempts, ip)
		} else {
			l.attempts[ip] = recent
		}
	}
}

func Middleware(limiter *Limiter) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ip := c.RealIP()
			if !limiter.Allow(ip) {
				return c.JSON(http.StatusTooManyRequests, map[string]string{
					"error": "too many attempts, try again later",
				})
			}
			return next(c)
		}
	}
}
