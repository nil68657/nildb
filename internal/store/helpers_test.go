package store

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock for lease and filter tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// testConfig is a small-footprint config in a fresh temp dir, on the
// engine TestEngine names.
func testConfig(t testing.TB, clock func() time.Time) Config {
	t.Helper()
	return Config{
		Dir:                 t.TempDir(),
		Engine:              TestEngine(),
		BlockCacheBytes:     8 << 20,
		AnalyticsCacheBytes: 4 << 20,
		WriteBufferBytes:    32 << 20,
		Fsync:               FsyncNo,
		Clock:               clock,
	}
}

// rocksOnly skips a test on the Rust engines; why names the RocksDB
// feature it needs.
func rocksOnly(t testing.TB, why string) {
	t.Helper()
	if e := TestEngine(); e != EngineRocksDB {
		t.Skipf("RocksDB only (%s); this run uses engine %s", why, e)
	}
}

// rustOnly skips a test on RocksDB; why names what only the Rust engines
// have.
func rustOnly(t testing.TB, why string) {
	t.Helper()
	if TestEngine() == EngineRocksDB {
		t.Skipf("Rust engines only (%s)", why)
	}
}

// openTest opens a store in a temp dir and closes it at cleanup.
func openTest(t testing.TB, mut func(*Config)) *Store {
	t.Helper()
	if testing.Short() {
		t.Skip("opens the engine")
	}
	cfg := testConfig(t, nil)
	if mut != nil {
		mut(&cfg)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func mustGet(t testing.TB, r Reader, cf CF, key []byte) ([]byte, bool) {
	t.Helper()
	v, ok, err := r.Get(cf, key)
	if err != nil {
		t.Fatalf("Get(%s, %q): %v", cf, key, err)
	}
	return v, ok
}

func wantValue(t testing.TB, r Reader, cf CF, key, want []byte) {
	t.Helper()
	v, ok := mustGet(t, r, cf, key)
	if !ok {
		t.Fatalf("Get(%s, %x): absent, want %q", cf, key, want)
	}
	if !bytes.Equal(v, want) {
		t.Fatalf("Get(%s, %x) = %q, want %q", cf, key, v, want)
	}
}

func wantAbsent(t testing.TB, r Reader, cf CF, key []byte) {
	t.Helper()
	if v, ok := mustGet(t, r, cf, key); ok {
		t.Fatalf("Get(%s, %x) = %q, want absent", cf, key, v)
	}
}

func commit(t testing.TB, txn Txn) {
	t.Helper()
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// collect returns every key of an iterator walk as copies.
func collect(t testing.TB, it Iterator, forward bool) []string {
	t.Helper()
	var out []string
	for ; it.Valid(); func() {
		if forward {
			it.Next()
		} else {
			it.Prev()
		}
	}() {
		out = append(out, string(it.Key()))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return out
}
