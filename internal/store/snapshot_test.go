package store

import (
	"errors"
	"testing"
	"time"
)

func TestLeaseCapLookupAndExpiry(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) {
		c.Clock = clock.Now
		c.MaxSnapshots = 3
		c.LeaseTTL = 10 * time.Second
		c.LeaseMax = 30 * time.Second
	})
	txn := s.Begin()
	txn.Put(CFDoc, []byte("k"), []byte("old"))
	commit(t, txn)

	a, err := s.Lease("conn:1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if a.Owner != "conn:1" || !a.Expires.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("lease = %+v, want the 10 s default TTL", a)
	}
	b, err := s.Lease("conn:2", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Expires.Equal(clock.Now().Add(30 * time.Second)) {
		t.Fatalf("lease with a 1 h TTL expires %v, want LeaseMax", b.Expires)
	}
	c, err := s.Lease("conn:3", 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lease("conn:4", 0); !errors.Is(err, ErrTooManySnapshots) {
		t.Fatalf("fourth lease: %v, want ErrTooManySnapshots", err)
	}
	// Command-scoped snapshots are not counted.
	cmd := s.Snapshot()
	s.Release(cmd)

	if got := s.Leases(); len(got) != 3 || got[0] != a || got[1] != b || got[2] != c {
		t.Fatalf("Leases = %v", got)
	}
	if sn, ok := s.Lookup(b.ID); !ok || sn != b {
		t.Fatalf("Lookup(%d) = %v, %v", b.ID, sn, ok)
	}
	if _, ok := s.Lookup(cmd.ID); ok {
		t.Fatal("Lookup found a command-scoped snapshot")
	}
	if _, ok := s.Lookup(999); ok {
		t.Fatal("Lookup found an unknown id")
	}

	txn = s.Begin()
	txn.Put(CFDoc, []byte("k"), []byte("new"))
	commit(t, txn)
	wantValue(t, s.At(a), CFDoc, []byte("k"), []byte("old"))

	// After 15 s only a (10 s TTL) has expired.
	clock.Advance(15 * time.Second)
	if n := s.ReleaseExpired(); n != 1 {
		t.Fatalf("ReleaseExpired released %d, want 1", n)
	}
	if _, _, err := s.At(a).Get(CFDoc, []byte("k")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("read on an expired lease: %v", err)
	}
	if _, ok := s.Lookup(a.ID); ok {
		t.Fatal("Lookup found an expired lease")
	}
	d, err := s.Lease("conn:4", 0)
	if err != nil {
		t.Fatalf("lease after a slot freed: %v", err)
	}

	// At the cap, Lease releases expired leases before refusing: c (20 s)
	// has expired by 25 s, b (30 s) has not.
	clock.Advance(10 * time.Second)
	if _, err := s.Lease("conn:5", 0); err != nil {
		t.Fatalf("lease at the cap with an expired one: %v", err)
	}
	if _, ok := s.Lookup(c.ID); ok {
		t.Fatal("c survived its TTL")
	}
	// Lookup releases a lease whose TTL has passed.
	clock.Advance(5 * time.Second)
	if _, ok := s.Lookup(b.ID); ok {
		t.Fatal("b survived its TTL")
	}
	s.Release(d)
	s.Release(d)
	for _, l := range s.Leases() {
		if l == b || l == c || l == d {
			t.Fatalf("released lease %d still listed", l.ID)
		}
	}
}

func TestLeaseJanitor(t *testing.T) {
	old := janitorInterval
	janitorInterval = 10 * time.Millisecond
	defer func() { janitorInterval = old }()
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now; c.LeaseTTL = time.Minute })

	l, err := s.Lease("janitor", 0)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if len(s.Leases()) != 1 {
		t.Fatal("janitor released a live lease")
	}
	clock.Advance(time.Minute)
	deadline := time.Now().Add(5 * time.Second)
	for len(s.Leases()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor did not release the expired lease")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, _, err := s.At(l).Get(CFDoc, []byte("k")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("read after the janitor released the lease: %v", err)
	}
}

func TestCloseReleasesSnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	s, err := Open(testConfig(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.Lease("x", 0)
	if err != nil {
		t.Fatal(err)
	}
	cmd := s.Snapshot()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.At(l).Get(CFDoc, []byte("k")); err == nil {
		t.Fatal("read on a lease after Close succeeded")
	}
	s.Release(l)
	s.Release(cmd)
	if _, err := s.Lease("y", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Lease after Close: %v", err)
	}
	if s.Snapshot() != nil {
		t.Fatal("Snapshot after Close returned a snapshot")
	}
}
