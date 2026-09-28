package store

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// flushCompact forces every entry of cf through a compaction.
func flushCompact(t testing.TB, s *Store, cfs ...CF) {
	t.Helper()
	for _, cf := range cfs {
		if err := s.Flush(cf); err != nil {
			t.Fatalf("Flush(%s): %v", cf, err)
		}
		if err := s.Compact(cf, nil, nil); err != nil {
			t.Fatalf("Compact(%s): %v", cf, err)
		}
	}
}

// versionAt returns a collection version minted at t.
func versionAt(t time.Time, counter uint64) uint64 {
	return uint64(t.UnixMicro())<<layout.VersionCounterBits | counter
}

func TestMetaFilter(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	now := clock.Now().UnixMilli()
	grace := metaGrace.Milliseconds()

	str := func(expire int64) []byte {
		return layout.Meta{Type: layout.TString, ExpireMS: expire, Payload: []byte("p")}.Encode(nil)
	}
	hash := func(expire int64) []byte {
		return layout.Meta{Type: layout.THash, ExpireMS: expire, Version: 1, Size: 1}.Encode(nil)
	}
	ventry := func(expire int64) []byte { return layout.VersionEntry{ExpireMS: expire, Key: []byte("h")}.Encode(nil) }
	cases := []struct {
		key  []byte
		val  []byte
		drop bool
	}{
		{layout.MetaKey(nil, 0, []byte("persistent")), str(0), false},
		{layout.MetaKey(nil, 0, []byte("future")), str(now + 1000), false},
		{layout.MetaKey(nil, 1, []byte("in-grace")), str(now - grace + 1000), false},
		{layout.MetaKey(nil, 1, []byte("past-grace")), str(now - grace - 1), true},
		{layout.MetaKey(nil, 2, []byte("hash-past-grace")), hash(now - 2*grace), true},
		{layout.MetaKey(nil, 2, []byte("far-future")), str(1<<63 - 1), false},
		{layout.MetaKey(nil, 3, []byte("negative")), str(-5), true},
		{layout.VersionKey(nil, 10), ventry(0), false},
		{layout.VersionKey(nil, 11), ventry(now - grace/2), false},
		{layout.VersionKey(nil, 12), ventry(now - grace - 1), true},
		// Unknown layout version and short values are kept, not judged.
		{layout.MetaKey(nil, 4, []byte("layout-v1")), append([]byte{0x11}, str(now - 2*grace)[1:]...), false},
		{layout.MetaKey(nil, 4, []byte("short")), []byte{1, 2}, false},
		{[]byte{0x20, 'x'}, str(now - 2*grace), false}, // a byte reserved above the dbs
	}
	txn := s.Begin()
	for _, c := range cases {
		txn.Put(CFMeta, c.key, c.val)
	}
	commit(t, txn)
	flushCompact(t, s, CFMeta)
	for _, c := range cases {
		_, ok := mustGet(t, s, CFMeta, c.key)
		if ok == c.drop {
			t.Errorf("key %q: present=%v after compaction, want drop=%v", c.key, ok, c.drop)
		}
	}
}

func TestMetaFilterKeepsAllWhenUnbound(t *testing.T) {
	f := &metaFilter{now: func() int64 { return 1 << 40 }}
	val := layout.Meta{Type: layout.TString, ExpireMS: 1}.Encode(nil)
	if drop, _ := f.Filter(1, layout.MetaKey(nil, 0, []byte("k")), val); drop {
		t.Fatal("unbound metaFilter dropped an entry")
	}
	f.bound.Store(true)
	if drop, _ := f.Filter(1, layout.MetaKey(nil, 0, []byte("k")), val); !drop {
		t.Fatal("bound metaFilter kept an entry expired for decades")
	}
}

func TestSubFilter(t *testing.T) {
	clock := newFakeClock()
	s := openTest(t, func(c *Config) { c.Clock = clock.Now })
	now := clock.Now()
	grace := metaGrace.Milliseconds()
	old := now.Add(-2 * time.Minute)

	type ver struct {
		name   string
		v      uint64
		entry  []byte // version-map value; nil = no entry
		remove bool
	}
	vs := []ver{
		{"old live", versionAt(old, 1), layout.VersionEntry{Key: []byte("a")}.Encode(nil), false},
		{"old deleted", versionAt(old, 2), nil, true},
		{"old expired past grace", versionAt(old, 3), layout.VersionEntry{ExpireMS: now.UnixMilli() - grace - 1, Key: []byte("c")}.Encode(nil), true},
		{"old expired in grace", versionAt(old, 4), layout.VersionEntry{ExpireMS: now.UnixMilli() - 1000, Key: []byte("d")}.Encode(nil), false},
		{"young deleted", versionAt(now.Add(-10*time.Second), 5), nil, false},
		{"future deleted", versionAt(now.Add(time.Hour), 6), nil, false},
	}
	txn := s.Begin()
	for _, v := range vs {
		if v.entry != nil {
			txn.Put(CFMeta, layout.VersionKey(nil, v.v), v.entry)
		}
		for i := range 50 {
			m := []byte{byte(i), 'm'}
			txn.Put(CFSub, layout.SubKey(nil, v.v, m), []byte("val"))
			txn.Put(CFZScore, layout.ScoreKey(nil, v.v, float64(i), m), nil)
		}
	}
	commit(t, txn)
	flushCompact(t, s, CFSub, CFZScore)
	for _, v := range vs {
		for i := range 50 {
			m := []byte{byte(i), 'm'}
			_, okSub := mustGet(t, s, CFSub, layout.SubKey(nil, v.v, m))
			_, okZ := mustGet(t, s, CFZScore, layout.ScoreKey(nil, v.v, float64(i), m))
			if okSub == v.remove || okZ == v.remove {
				t.Fatalf("%s: sub present=%v zscore present=%v, want removed=%v", v.name, okSub, okZ, v.remove)
			}
		}
	}

	// A version that dies later is reclaimed by a later compaction, and a
	// drop verdict stays a drop.
	txn = s.Begin()
	txn.Delete(CFMeta, layout.VersionKey(nil, vs[0].v))
	txn.Put(CFSub, layout.SubKey(nil, vs[1].v, []byte("late")), []byte("x"))
	commit(t, txn)
	for i := range 64 { // push every cache slot to another version
		s.subF.cache[i].mu.Lock()
		s.subF.cache[i].valid = false
		s.subF.cache[i].mu.Unlock()
	}
	flushCompact(t, s, CFMeta, CFSub)
	wantAbsent(t, s, CFSub, layout.SubKey(nil, vs[0].v, []byte{0, 'm'}))
	wantAbsent(t, s, CFSub, layout.SubKey(nil, vs[1].v, []byte("late")))
}

func TestSubFilterUnboundKeepsAll(t *testing.T) {
	f := &subFilter{now: func() int64 { return 1 << 42 }}
	key := binary.BigEndian.AppendUint64(nil, 1)
	if drop, _ := f.Filter(1, key, nil); drop {
		t.Fatal("unbound subFilter dropped an entry")
	}
}

func TestIDFilter(t *testing.T) {
	s := openTest(t, nil)
	cfs := []CF{CFDoc, CFIdx, CFGeo, CFCol}
	key := func(id uint32, suffix string) []byte {
		k := layout.IDPrefix(nil, id)
		if suffix != "" {
			k = append(k, make([]byte, 8)...) // room for a geo cell id
			k = append(k, suffix...)
		}
		return k
	}
	write := func() {
		txn := s.Begin()
		for _, cf := range cfs {
			for id := uint32(1); id <= 3; id++ {
				txn.Put(cf, key(id, "row"), []byte("v"))
			}
			txn.Put(cf, []byte{9}, []byte("short key")) // shorter than an id
		}
		commit(t, txn)
	}
	write()

	// No LiveSet published: everything stays.
	flushCompact(t, s, cfs...)
	for _, cf := range cfs {
		for id := uint32(1); id <= 3; id++ {
			wantValue(t, s, cf, key(id, "row"), []byte("v"))
		}
	}

	s.SetLiveSet(LiveFunc(func(id uint32) bool { return id != 2 }))
	write()
	flushCompact(t, s, cfs...)
	for _, cf := range cfs {
		wantValue(t, s, cf, key(1, "row"), []byte("v"))
		wantAbsent(t, s, cf, key(2, "row"))
		wantValue(t, s, cf, key(3, "row"), []byte("v"))
		wantValue(t, s, cf, []byte{9}, []byte("short key"))
	}

	// Publishing nil keeps everything again.
	s.SetLiveSet(nil)
	write()
	flushCompact(t, s, cfs...)
	for _, cf := range cfs {
		wantValue(t, s, cf, key(2, "row"), []byte("v"))
	}
}
