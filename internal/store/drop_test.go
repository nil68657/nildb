package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

func TestDeleteRangesThenRead(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	for coll := uint32(1); coll <= 3; coll++ {
		for i := range 20 {
			txn.Put(CFDoc, layout.DocKey(nil, coll, fmt.Appendf(nil, "id%02d", i)), []byte("doc"))
			txn.Put(CFIdx, layout.IdxKey(nil, coll+100, fmt.Appendf(nil, "v%02d", i)), nil)
		}
	}
	txn.Put(CFDefault, layout.CollKey("db.two"), []byte("record"))
	txn.Put(CFDefault, layout.CollKey("db.one"), []byte("record"))
	commit(t, txn)
	if err := s.Flush(CFDoc); err != nil { // half on disk, half in the memtable
		t.Fatal(err)
	}

	docLo, docHi := layout.IDBounds(2)
	idxLo, idxHi := layout.IDBounds(102)
	err := s.DeleteRanges(
		[]RangeDelete{{CF: CFDoc, Lo: docLo, Hi: docHi}, {CF: CFIdx, Lo: idxLo, Hi: idxHi}},
		[]PointDelete{{CF: CFDefault, Key: layout.CollKey("db.two")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	count := func(cf CF, id uint32) int {
		lo, hi := layout.IDBounds(id)
		it := s.Iter(cf, lo, hi, IterOpts{FillCache: true})
		defer it.Close()
		it.SeekToFirst()
		return len(collect(t, it, true))
	}
	for _, c := range []struct {
		cf   CF
		id   uint32
		want int
	}{{CFDoc, 1, 20}, {CFDoc, 2, 0}, {CFDoc, 3, 20}, {CFIdx, 101, 20}, {CFIdx, 102, 0}, {CFIdx, 103, 20}} {
		if got := count(c.cf, c.id); got != c.want {
			t.Errorf("%s id %d: %d keys, want %d", c.cf, c.id, got, c.want)
		}
	}
	wantAbsent(t, s, CFDefault, layout.CollKey("db.two"))
	wantValue(t, s, CFDefault, layout.CollKey("db.one"), []byte("record"))

	if err := s.DeleteRanges([]RangeDelete{{CF: CFDoc, Lo: []byte{1}}}, nil); err == nil {
		t.Fatal("DeleteRanges without an upper bound accepted")
	}
	if err := s.DeleteRanges(nil, nil); err != nil {
		t.Fatalf("empty DeleteRanges: %v", err)
	}
}

// setCompactHook installs a hook that records scheduled compactions and
// restores the scheduling knobs at cleanup.
func setCompactHook(t *testing.T, delay time.Duration, burst int) (ran func() []CF) {
	t.Helper()
	oldDelay, oldBurst, oldHook := compactDelay, compactBurst, compactHook
	var mu sync.Mutex
	var got []CF
	compactDelay, compactBurst = delay, burst
	compactHook = func(cf CF, lo, hi []byte) {
		mu.Lock()
		got = append(got, cf)
		mu.Unlock()
	}
	t.Cleanup(func() { compactDelay, compactBurst, compactHook = oldDelay, oldBurst, oldHook })
	return func() []CF {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestScheduleCompactDelay(t *testing.T) {
	ran := setCompactHook(t, 30*time.Millisecond, 32)
	s := openTest(t, nil)
	lo, hi := layout.IDBounds(1)
	s.ScheduleCompact(CFDoc, lo, hi)
	if n := s.PendingCompactions(); n != 1 {
		t.Fatalf("pending = %d, want 1", n)
	}
	waitFor(t, "the delayed compaction", func() bool { return len(ran()) == 1 })
	if got := ran(); got[0] != CFDoc || s.PendingCompactions() != 0 {
		t.Fatalf("ran %v, pending %d", got, s.PendingCompactions())
	}
}

func TestScheduleCompactBurst(t *testing.T) {
	ran := setCompactHook(t, time.Hour, 4)
	s := openTest(t, nil)
	for i := range 3 {
		s.ScheduleCompact(CFIdx, layout.IDPrefix(nil, uint32(i)), layout.IDPrefix(nil, uint32(i+1)))
	}
	time.Sleep(20 * time.Millisecond)
	if len(ran()) != 0 || s.PendingCompactions() != 3 {
		t.Fatalf("before the burst: ran %v, pending %d", ran(), s.PendingCompactions())
	}
	s.ScheduleCompact(CFIdx, nil, nil) // the fourth outstanding one runs all four now
	waitFor(t, "the burst", func() bool { return len(ran()) == 4 })
	if s.PendingCompactions() != 0 {
		t.Fatalf("pending after the burst = %d", s.PendingCompactions())
	}
}

func TestCloseCancelsScheduledCompactions(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	ran := setCompactHook(t, 100*time.Millisecond, 32)
	s, err := Open(testConfig(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		s.ScheduleCompact(CFDoc, layout.IDPrefix(nil, uint32(i)), nil)
	}
	start := time.Now()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Close took %v", d)
	}
	time.Sleep(200 * time.Millisecond)
	if got := ran(); len(got) != 0 {
		t.Fatalf("compactions ran after Close: %v", got)
	}
	s.ScheduleCompact(CFDoc, nil, nil) // a no-op after Close
}
