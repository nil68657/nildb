package store

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newLockStore returns a Store with only its lock manager set up; locks
// need no database.
func newLockStore() *Store { return &Store{} }

// TestLockOrderingRace has 1,000 goroutines lock random 3-key sets across
// the striped kinds and bump a plain counter per key. Missing exclusion
// shows up under -race; a lock-order bug shows up as a deadlock.
func TestLockOrderingRace(t *testing.T) {
	s := newLockStore()
	type slot struct{ n int }
	const perKind = 16
	kinds := []LockKind{LockDoc, LockUniq, LockRedis}
	counters := make(map[LockKind][]*slot)
	for _, k := range kinds {
		for range perKind {
			counters[k] = append(counters[k], &slot{})
		}
	}
	const goroutines, rounds = 1000, 20
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for range rounds {
				keys := make([]LockKey, 3)
				for i := range keys {
					k := kinds[rng.IntN(len(kinds))]
					keys[i] = LockKey{Kind: k, NS: 1, Key: []byte{byte(rng.IntN(perKind))}}
				}
				unlock := s.Lock(keys)
				seen := map[*slot]bool{}
				for _, k := range keys {
					c := counters[k.Kind][k.Key[0]]
					if !seen[c] {
						seen[c] = true
						c.n++
					}
				}
				unlock()
				unlock() // idempotent
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: lock set acquisition did not finish")
	}
	total := 0
	for _, k := range kinds {
		for _, c := range counters[k] {
			total += c.n
		}
	}
	if total == 0 {
		t.Fatal("no counter moved")
	}
}

func TestLockCollSharedExclusive(t *testing.T) {
	s := newLockStore()
	read := []LockKey{{Kind: LockColl, NS: 5, Key: []byte{'r'}}}
	write := []LockKey{{Kind: LockColl, NS: 5, Key: []byte{'w'}}}

	// Shared holders run together.
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := s.Lock(read)
			defer unlock()
			n := active.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-start
			active.Add(-1)
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for active.Load() != 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 8 {
		t.Fatalf("only %d of 8 shared holders got in together", active.Load())
	}

	// The exclusive holder waits for every shared holder.
	gotW := make(chan struct{})
	go func() {
		unlock := s.Lock(write)
		if n := active.Load(); n != 0 {
			t.Errorf("exclusive lock taken with %d shared holders", n)
		}
		close(gotW)
		unlock()
	}()
	select {
	case <-gotW:
		t.Fatal("exclusive lock granted while shared holders are inside")
	case <-time.After(50 * time.Millisecond):
	}
	close(start)
	wg.Wait()
	<-gotW

	// A different collection is independent, and 'r' plus 'w' on one
	// collection in one call deduplicates to exclusive.
	unlockOther := s.Lock([]LockKey{{Kind: LockColl, NS: 6, Key: []byte{'w'}}})
	unlockBoth := s.Lock(append(append([]LockKey{}, read...), write...))
	blocked := make(chan struct{})
	go func() {
		u := s.Lock(read)
		close(blocked)
		u()
	}()
	select {
	case <-blocked:
		t.Fatal("'r'+'w' in one call did not take the lock exclusive")
	case <-time.After(50 * time.Millisecond):
	}
	unlockBoth()
	<-blocked
	unlockOther()
}

func TestLockTwoPhase(t *testing.T) {
	s := newLockStore()
	coll := LockKey{Kind: LockColl, NS: 1, Key: []byte{'r'}}
	doc := LockKey{Kind: LockDoc, NS: 1, Key: []byte("id")}
	uniq := LockKey{Kind: LockUniq, NS: 9, Key: []byte("v")}

	held := []LockKey{coll, doc}
	unlock1 := s.Lock(held)
	unlock2 := s.LockAfter(held, []LockKey{uniq})
	unlock2()
	unlock1()

	for _, tc := range []struct {
		name string
		held []LockKey
		more []LockKey
	}{
		{"same kind", []LockKey{doc}, []LockKey{{Kind: LockDoc, NS: 1, Key: []byte("other")}}},
		{"lower kind", []LockKey{uniq}, []LockKey{doc}},
		{"collection after doc", []LockKey{coll, doc}, []LockKey{coll}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: LockAfter did not panic", tc.name)
				}
			}()
			s.LockAfter(tc.held, tc.more)()
		}()
	}
	if u := s.Lock(nil); u == nil {
		t.Fatal("Lock(nil) returned a nil unlock")
	} else {
		u()
	}
}

func TestStripeSpread(t *testing.T) {
	seen := map[uint32]bool{}
	for i := range 10000 {
		st := stripeOf(uint32(i%16), []byte{byte(i), byte(i >> 8)})
		if st >= lockStripes {
			t.Fatalf("stripe %d out of range", st)
		}
		seen[st] = true
	}
	if len(seen) < lockStripes*9/10 {
		t.Fatalf("10,000 keys used only %d of %d stripes", len(seen), lockStripes)
	}
	if stripeOf(1, []byte("k")) == stripeOf(2, []byte("k")) && stripeOf(3, []byte("k")) == stripeOf(4, []byte("k")) {
		t.Fatal("NS does not affect the stripe")
	}
}
