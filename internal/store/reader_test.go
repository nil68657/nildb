package store

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

func TestReaderGetCopiesAndPinned(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	txn.Put(CFMeta, []byte("\x00a"), []byte("alpha"))
	txn.Put(CFMeta, []byte("\x00empty"), nil)
	commit(t, txn)

	v, ok := mustGet(t, s, CFMeta, []byte("\x00a"))
	if !ok || string(v) != "alpha" {
		t.Fatalf("Get = %q, %v", v, ok)
	}
	v[0] = 'X' // a Go-owned copy: changing it changes nothing stored
	wantValue(t, s, CFMeta, []byte("\x00a"), []byte("alpha"))
	v, ok = mustGet(t, s, CFMeta, []byte("\x00empty"))
	if !ok || v == nil || len(v) != 0 {
		t.Fatalf("empty value: %q, %v", v, ok)
	}

	calls := 0
	ok, err := s.GetPinned(CFMeta, []byte("\x00missing"), func([]byte) error { calls++; return nil })
	if ok || err != nil || calls != 0 {
		t.Fatalf("GetPinned(missing) = %v, %v, %d calls", ok, err, calls)
	}
	boom := errors.New("boom")
	ok, err = s.GetPinned(CFMeta, []byte("\x00a"), func(val []byte) error {
		if string(val) != "alpha" {
			t.Errorf("pinned view = %q", val)
		}
		return boom
	})
	if !ok || err != boom {
		t.Fatalf("GetPinned = %v, %v; want true, boom", ok, err)
	}

	vals, err := s.MultiGet(CFMeta, [][]byte{[]byte("\x00a"), []byte("\x00zz"), []byte("\x00empty")})
	if err != nil || len(vals) != 3 || string(vals[0]) != "alpha" || vals[1] != nil || vals[2] == nil {
		t.Fatalf("MultiGet = %q, %v", vals, err)
	}
	if vals, err := s.MultiGet(CFMeta, nil); err != nil || len(vals) != 0 {
		t.Fatalf("MultiGet(nil) = %v, %v", vals, err)
	}
}

func TestIteratorBounds(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	for i := range 10 {
		txn.Put(CFDoc, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i))
	}
	commit(t, txn)

	lo, hi := []byte("k2"), []byte("k6")
	it := s.Iter(CFDoc, lo, hi, IterOpts{FillCache: true})
	defer it.Close()
	lo[1], hi[1] = 'z', 'z' // the iterator copied its bounds
	it.SeekToFirst()
	if got := collect(t, it, true); !slices.Equal(got, []string{"k2", "k3", "k4", "k5"}) {
		t.Fatalf("forward [k2, k6) = %v", got)
	}
	it.SeekToLast()
	if got := collect(t, it, false); !slices.Equal(got, []string{"k5", "k4", "k3", "k2"}) {
		t.Fatalf("reverse [k2, k6) = %v", got)
	}
	it.Seek([]byte("k4"))
	if !it.Valid() || string(it.Key()) != "k4" || string(it.Value()) != "v4" {
		t.Fatalf("Seek(k4) at %q=%q", it.Key(), it.Value())
	}
	it.SeekForPrev([]byte("k9"))
	if !it.Valid() || string(it.Key()) != "k5" {
		t.Fatalf("SeekForPrev(k9) at %q", it.Key())
	}
	it.Close()
	it.Close() // idempotent
	if it.Valid() || it.Err() != nil {
		t.Fatal("closed iterator is valid or has an error")
	}

	open := s.Iter(CFDoc, nil, []byte("k3"), IterOpts{})
	open.SeekToFirst()
	if got := collect(t, open, true); !slices.Equal(got, []string{"k0", "k1", "k2"}) {
		t.Fatalf("[nil, k3) = %v", got)
	}
	open.Close()
}

func TestIteratorTotalOrderSeekOnGeo(t *testing.T) {
	s := openTest(t, nil)
	// geo keys: | idx_id (4) | cell_id (8) | ks(_id) |; the extractor takes
	// the first 12 bytes, so each cell is its own prefix.
	txn := s.Begin()
	var keys []string
	for cell := uint64(1); cell <= 5; cell++ {
		for id := byte(0); id < 3; id++ {
			k := layout.GeoKey(nil, 7, cell<<40, []byte{100, id})
			keys = append(keys, string(k))
			txn.Put(CFGeo, k, layout.GeoPointValue(nil, 1, 2))
		}
	}
	commit(t, txn)
	if err := s.Flush(CFGeo); err != nil {
		t.Fatal(err)
	}

	// A range across cells 2..4 needs total order.
	lo := layout.GeoCellPrefix(nil, 7, 2<<40)
	hi := layout.GeoCellPrefix(nil, 7, 5<<40)
	it := s.Iter(CFGeo, lo, hi, IterOpts{TotalOrderSeek: true})
	it.SeekToFirst()
	got := collect(t, it, true)
	it.Close()
	if want := keys[3:12]; !slices.Equal(got, want) {
		t.Fatalf("TotalOrderSeek over cells 2-4: %d keys, want %d", len(got), len(want))
	}

	// PrefixSameAsStart stays inside the cell it seeks into.
	it = s.Iter(CFGeo, nil, nil, IterOpts{PrefixSameAsStart: true})
	it.Seek(layout.GeoCellPrefix(nil, 7, 3<<40))
	got = collect(t, it, true)
	it.Close()
	if want := keys[6:9]; !slices.Equal(got, want) {
		t.Fatalf("PrefixSameAsStart in cell 3: %d keys, want %d", len(got), len(want))
	}
}

func TestIteratorOptions(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	for i := range 100 {
		txn.Put(CFDoc, fmt.Appendf(nil, "k%03d", i), []byte("value"))
	}
	commit(t, txn)
	if err := s.Flush(CFDoc); err != nil {
		t.Fatal(err)
	}
	it := s.Iter(CFDoc, []byte("k"), []byte("l"), IterOpts{
		Readahead: 2 << 20, AsyncIO: true, LowPriority: true, TotalOrderSeek: true,
		Deadline: time.Minute,
	})
	it.SeekToFirst()
	if got := collect(t, it, true); len(got) != 100 {
		t.Fatalf("analytical iterator read %d keys, want 100", len(got))
	}
	it.Close()
}

func TestSnapshotIsolation(t *testing.T) {
	s := openTest(t, nil)
	put := func(k, v string) {
		txn := s.Begin()
		txn.Put(CFDoc, []byte(k), []byte(v))
		commit(t, txn)
	}
	put("a", "1")
	put("b", "1")
	snap := s.Snapshot()
	if snap.Seq != s.LatestSeq() || !snap.Expires.IsZero() {
		t.Fatalf("snapshot = %+v, latest seq %d", snap, s.LatestSeq())
	}
	put("a", "2")
	put("c", "2")
	txn := s.Begin()
	txn.Delete(CFDoc, []byte("b"))
	commit(t, txn)

	r := s.At(snap)
	wantValue(t, r, CFDoc, []byte("a"), []byte("1"))
	wantValue(t, r, CFDoc, []byte("b"), []byte("1"))
	wantAbsent(t, r, CFDoc, []byte("c"))
	wantValue(t, s, CFDoc, []byte("a"), []byte("2"))
	vals, err := r.MultiGet(CFDoc, [][]byte{[]byte("a"), []byte("c")})
	if err != nil || string(vals[0]) != "1" || vals[1] != nil {
		t.Fatalf("snapshot MultiGet = %q, %v", vals, err)
	}
	var pinned string
	if ok, err := r.GetPinned(CFDoc, []byte("b"), func(v []byte) error { pinned = string(v); return nil }); !ok || err != nil || pinned != "1" {
		t.Fatalf("snapshot GetPinned = %v, %v, %q", ok, err, pinned)
	}

	it := r.Iter(CFDoc, nil, nil, IterOpts{})
	// Releasing the snapshot does not disturb an open iterator.
	s.Release(snap)
	it.SeekToFirst()
	if got := collect(t, it, true); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("snapshot iterator = %v", got)
	}
	it.Close()

	if _, _, err := r.Get(CFDoc, []byte("a")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Get on a released snapshot: %v", err)
	}
	it = r.Iter(CFDoc, nil, nil, IterOpts{})
	if it.Valid() || !errors.Is(it.Err(), ErrLeaseExpired) {
		t.Fatalf("Iter on a released snapshot: valid %v, err %v", it.Valid(), it.Err())
	}
	it.Close()
	s.Release(snap) // idempotent
	s.Release(nil)
	if s.At(nil) != Reader(s) {
		t.Fatal("At(nil) is not the live store")
	}
}
