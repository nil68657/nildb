package store

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// The sweeper stands in for compaction filters on the Rust engines, but it
// only uses the store's own API, so these tests run it on every engine.

func strMeta(expire int64) []byte {
	return layout.Meta{Type: layout.TString, ExpireMS: expire, Payload: []byte("p")}.Encode(nil)
}

func sweepAll(t *testing.T, s *Store) sweepStats {
	t.Helper()
	st, err := s.sweepRound(math.MaxInt64)
	if err != nil {
		t.Fatalf("sweepRound: %v", err)
	}
	return st
}

func TestSweeperExpiredMetaAndDeadVersions(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	now := clock.Now()
	nowMS := now.UnixMilli()
	grace := metaGrace.Milliseconds()
	old := now.Add(-2 * time.Minute)

	txn := s.Begin()
	txn.Put(CFMeta, layout.MetaKey(nil, 0, []byte("persistent")), strMeta(0))
	txn.Put(CFMeta, layout.MetaKey(nil, 0, []byte("in-grace")), strMeta(nowMS-grace+1000))
	txn.Put(CFMeta, layout.MetaKey(nil, 1, []byte("past-grace")), strMeta(nowMS-grace-1))
	txn.Put(CFMeta, []byte{0x20, 'x'}, strMeta(nowMS-2*grace)) // above the dbs: not judged
	// An expired hash: metadata, version map entry and fields.
	vExpired := versionAt(old, 1)
	txn.Put(CFMeta, layout.MetaKey(nil, 2, []byte("h")),
		layout.Meta{Type: layout.THash, ExpireMS: nowMS - 2*grace, Version: vExpired, Size: 3}.Encode(nil))
	txn.Put(CFMeta, layout.VersionKey(nil, vExpired), layout.VersionEntry{ExpireMS: nowMS - 2*grace, DB: 2, Key: []byte("h")}.Encode(nil))
	// A deleted collection with more fields than the sweeper deletes one by
	// one, a live one, a young dead one and a small deleted one.
	vBig, vLive, vYoung, vSmall := versionAt(old, 2), versionAt(old, 3), versionAt(now.Add(-10*time.Second), 4), versionAt(old, 5)
	txn.Put(CFMeta, layout.VersionKey(nil, vLive), layout.VersionEntry{Key: []byte("live")}.Encode(nil))
	for i := range 3 {
		txn.Put(CFSub, layout.SubKey(nil, vExpired, fmt.Appendf(nil, "f%d", i)), []byte("x"))
		txn.Put(CFZScore, layout.ScoreKey(nil, vSmall, float64(i), []byte{'m', byte(i)}), nil)
	}
	for _, v := range []uint64{vLive, vYoung, vSmall} {
		txn.Put(CFSub, layout.SubKey(nil, v, []byte("f")), []byte("x"))
	}
	commit(t, txn)
	txn = s.Begin()
	for i := range sweepPointDeletes + 50 {
		txn.Put(CFSub, layout.SubKey(nil, vBig, fmt.Appendf(nil, "f%05d", i)), []byte("x"))
	}
	commit(t, txn)

	st := sweepAll(t, s)
	// Expired: past-grace, h and h's version-map entry. Dead versions:
	// vExpired, vBig and vSmall in sub, vSmall in zscore.
	if st.expired != 3 || st.versions != 4 {
		t.Fatalf("sweep stats: %d expired, %d dead versions; want 3 and 4 (stats %+v)", st.expired, st.versions, st)
	}
	wantValue(t, s, CFMeta, layout.MetaKey(nil, 0, []byte("persistent")), strMeta(0))
	wantValue(t, s, CFMeta, layout.MetaKey(nil, 0, []byte("in-grace")), strMeta(nowMS-grace+1000))
	wantValue(t, s, CFMeta, []byte{0x20, 'x'}, strMeta(nowMS-2*grace))
	wantAbsent(t, s, CFMeta, layout.MetaKey(nil, 1, []byte("past-grace")))
	wantAbsent(t, s, CFMeta, layout.MetaKey(nil, 2, []byte("h")))
	wantAbsent(t, s, CFMeta, layout.VersionKey(nil, vExpired))
	count := func(cf CF, v uint64) int {
		lo := layout.VersionPrefix(nil, v)
		it := s.Iter(cf, lo, layout.VersionPrefix(nil, v+1), IterOpts{})
		defer it.Close()
		it.SeekToFirst()
		return len(collect(t, it, true))
	}
	for _, c := range []struct {
		name string
		cf   CF
		v    uint64
		want int
	}{
		{"expired hash", CFSub, vExpired, 0},
		{"big deleted", CFSub, vBig, 0},
		{"small deleted", CFZScore, vSmall, 0},
		{"live", CFSub, vLive, 1},
		{"young deleted", CFSub, vYoung, 1},
		{"small deleted in sub", CFSub, vSmall, 0},
	} {
		if got := count(c.cf, c.v); got != c.want {
			t.Errorf("%s: %d entries left in %s, want %d", c.name, got, c.cf, c.want)
		}
	}
	if len(st.ranges) != 1 || st.ranges[0].CF != CFSub {
		t.Errorf("range deletes = %+v, want one on sub for the big collection", st.ranges)
	}
	// A second round finds nothing.
	if st := sweepAll(t, s); st.expired+st.versions+st.ids != 0 {
		t.Errorf("second round deleted something: %+v", st)
	}
}

func TestSweeperDroppedIDs(t *testing.T) {
	s := openTest(t, nil)
	cfs := []CF{CFDoc, CFIdx, CFGeo, CFCol}
	txn := s.Begin()
	for _, cf := range cfs {
		for id := uint32(1); id <= 3; id++ {
			for i := range 5 {
				txn.Put(cf, append(layout.IDPrefix(nil, id), byte(i)), []byte("v"))
			}
		}
		txn.Put(cf, []byte{9}, []byte("short key"))
	}
	commit(t, txn)
	// No LiveSet: everything stays.
	if st := sweepAll(t, s); st.ids != 0 {
		t.Fatalf("without a LiveSet the sweeper dropped %d ids", st.ids)
	}
	s.SetLiveSet(LiveFunc(func(id uint32) bool { return id != 2 }))
	if st := sweepAll(t, s); st.ids != len(cfs) {
		t.Fatalf("dropped %d ids, want %d", st.ids, len(cfs))
	}
	for _, cf := range cfs {
		for id := uint32(1); id <= 3; id++ {
			lo, hi := layout.IDBounds(id)
			it := s.Iter(cf, lo, hi, IterOpts{})
			it.SeekToFirst()
			n := len(collect(t, it, true))
			it.Close()
			if want := map[bool]int{true: 5, false: 0}[id != 2]; n != want {
				t.Errorf("%s id %d: %d entries, want %d", cf, id, n, want)
			}
		}
		wantValue(t, s, cf, []byte{9}, []byte("short key"))
	}
}

func TestSweeperBudgetResumes(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	expired := clock.Now().UnixMilli() - 2*metaGrace.Milliseconds()
	const n = 500
	txn := s.Begin()
	for i := range n {
		txn.Put(CFMeta, layout.MetaKey(nil, 0, fmt.Appendf(nil, "k%04d", i)), strMeta(expired))
	}
	commit(t, txn)
	total, rounds := 0, 0
	for total < n {
		if rounds++; rounds > 100 {
			t.Fatalf("%d rounds deleted %d of %d keys", rounds, total, n)
		}
		st, err := s.sweepRound(2000)
		if err != nil {
			t.Fatal(err)
		}
		if st.scanned > 2000+200 {
			t.Fatalf("a 2000-byte round read %d bytes", st.scanned)
		}
		total += st.expired
	}
	// Each entry is 16 bytes, so a round takes about 125 keys.
	if rounds < 4 {
		t.Fatalf("%d keys went in %d rounds of 2000 bytes", n, rounds)
	}
	it := s.Iter(CFMeta, []byte{0}, []byte{1}, IterOpts{})
	defer it.Close()
	if it.SeekToFirst(); it.Valid() {
		t.Fatalf("key %q survived", it.Key())
	}
}

// TestSweeperYieldsToWriters has writers re-create expired keys under
// their locks while rounds run. A key a writer made live must survive:
// the sweeper re-reads each key under its lock before deleting it.
func TestSweeperYieldsToWriters(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	expired := clock.Now().UnixMilli() - 2*metaGrace.Milliseconds()
	const n = 2000
	key := func(i int) []byte { return fmt.Appendf(nil, "k%05d", i) }
	txn := s.Begin()
	for i := range n {
		txn.Put(CFMeta, layout.MetaKey(nil, 0, key(i)), strMeta(expired))
	}
	commit(t, txn)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i += 2 {
			unlock := s.Lock([]LockKey{{Kind: LockRedis, NS: 0, Key: key(i)}})
			w := s.Begin()
			w.Put(CFMeta, layout.MetaKey(nil, 0, key(i)), strMeta(0))
			if err := w.Commit(); err != nil {
				t.Error(err)
			}
			unlock()
		}
	}()
	for range 4 {
		if _, err := s.sweepRound(20_000); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	sweepAll(t, s)
	for i := range n {
		_, ok := mustGet(t, s, CFMeta, layout.MetaKey(nil, 0, key(i)))
		if ok != (i%2 == 0) {
			t.Fatalf("key %d: present %v; the writer re-created the even keys", i, ok)
		}
	}
}

func TestSweeperRunsOnItsOwn(t *testing.T) {
	rustOnly(t, "RocksDB's compaction filters do this work")
	old := sweepInterval
	sweepInterval = 20 * time.Millisecond
	defer func() { sweepInterval = old }()
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	txn := s.Begin()
	txn.Put(CFMeta, layout.MetaKey(nil, 0, []byte("old")), strMeta(clock.Now().UnixMilli()-2*metaGrace.Milliseconds()))
	commit(t, txn)
	waitFor(t, "the background sweeper", func() bool {
		_, ok := mustGet(t, s, CFMeta, layout.MetaKey(nil, 0, []byte("old")))
		return !ok
	})
}
