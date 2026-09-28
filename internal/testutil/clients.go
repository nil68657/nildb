package testutil

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client returns a go-redis v9 client for the server speaking RESP proto
// (2 or 3). It authenticates with Cfg.RequirePass when one is set, does
// not retry failed commands, and is closed on t.Cleanup.
func (e *Env) Client(t testing.TB, proto int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{
		Addr:         e.Addr,
		Protocol:     proto,
		Password:     e.Cfg.RequirePass,
		MaxRetries:   -1,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}
