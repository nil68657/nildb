package store

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/layout"
)

func TestBatchAtomicAcrossCFs(t *testing.T) {
	s := openTest(t, nil)
	key := []byte("\x00\x00\x00\x01atomic")
	txn := s.Begin()
	for cf := range CF(NumCFs) {
		txn.Put(cf, key, []byte(cf.String()))
	}
	if txn.Len() != NumCFs {
		t.Fatalf("Len = %d, want %d", txn.Len(), NumCFs)
	}
	// A plain Txn reads committed state: nothing is visible yet, through
	// the Txn or the store.
	for cf := range CF(NumCFs) {
		wantAbsent(t, txn, cf, key)
		wantAbsent(t, s, cf, key)
	}
	commit(t, txn)
	for cf := range CF(NumCFs) {
		wantValue(t, s, cf, key, []byte(cf.String()))
	}

	// A discarded Txn writes nothing.
	txn = s.Begin()
	for cf := range CF(NumCFs) {
		txn.Delete(cf, key)
	}
	txn.Discard()
	for cf := range CF(NumCFs) {
		wantValue(t, s, cf, key, []byte(cf.String()))
	}
	if err := txn.Commit(); err == nil {
		t.Fatal("Commit after Discard succeeded")
	}

	// One commit deletes the key from every CF at once.
	txn = s.Begin()
	for cf := range CF(NumCFs) {
		txn.Delete(cf, key)
	}
	commit(t, txn)
	for cf := range CF(NumCFs) {
		wantAbsent(t, s, cf, key)
	}
}

func TestEmptyTxnCommits(t *testing.T) {
	s := openTest(t, nil)
	seq := s.LatestSeq()
	for _, txn := range []Txn{s.Begin(), s.BeginIndexed()} {
		if txn.Len() != 0 {
			t.Fatalf("Len = %d", txn.Len())
		}
		commit(t, txn)
	}
	if got := s.LatestSeq(); got != seq {
		t.Fatalf("empty commits moved the sequence from %d to %d", seq, got)
	}
}

func TestTxnMisuse(t *testing.T) {
	s := openTest(t, nil)
	for name, txn := range map[string]Txn{"plain": s.Begin(), "indexed": s.BeginIndexed()} {
		txn.Put(CFMeta, []byte("\x00a"), []byte("1"))
		txn.Merge(CFMeta, []byte("\x00a"), layout.AppendCount(nil, 1))
		err := txn.Commit()
		if err == nil || !strings.Contains(err.Error(), "merge operator") {
			t.Errorf("%s: Commit after a Merge on meta: %v", name, err)
		}
		wantAbsent(t, s, CFMeta, []byte("\x00a"))
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: Put after Commit did not panic", name)
				}
			}()
			txn.Put(CFMeta, []byte("\x00b"), nil)
		}()
	}
	txn := s.Begin()
	if err := txn.DeleteRange(CFDoc, []byte("b"), []byte("a")); err == nil {
		t.Error("DeleteRange with lo > hi accepted")
	}
	if err := txn.DeleteRange(CFDoc, []byte("a"), nil); err == nil {
		t.Error("DeleteRange without an upper bound accepted")
	}
	if err := txn.DeleteRange(CFDoc, []byte("a"), []byte("a")); err != nil || txn.Len() != 0 {
		t.Errorf("empty DeleteRange: %v, Len %d", err, txn.Len())
	}
	txn.Discard()
}

func TestPlainTxnDeleteRange(t *testing.T) {
	s := openTest(t, nil)
	txn := s.Begin()
	for _, k := range []string{"a1", "a2", "b1", "c1"} {
		txn.Put(CFDoc, []byte(k), []byte("v"))
	}
	commit(t, txn)
	txn = s.Begin()
	if err := txn.DeleteRange(CFDoc, []byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	commit(t, txn)
	it := s.Iter(CFDoc, nil, nil, IterOpts{FillCache: true})
	defer it.Close()
	it.SeekToFirst()
	if got := collect(t, it, true); !slices.Equal(got, []string{"c1"}) {
		t.Fatalf("after DeleteRange [a, c): %v", got)
	}
}

func TestIndexedReadYourWrites(t *testing.T) {
	s := openTest(t, nil)
	base := s.Begin()
	for _, k := range []string{"k1", "k2", "k3", "k5"} {
		base.Put(CFSub, []byte("\x00\x00\x00\x00\x00\x00\x00\x07"+k), []byte("db-"+k))
	}
	base.Put(CFDefault, layout.CountKey(1), layout.AppendCount(nil, 10))
	commit(t, base)
	prefix := "\x00\x00\x00\x00\x00\x00\x00\x07"
	key := func(k string) []byte { return []byte(prefix + k) }

	txn := s.BeginIndexed()
	txn.Put(CFSub, key("k2"), []byte("tx-k2"))
	txn.Put(CFSub, key("k4"), []byte("tx-k4"))
	txn.Delete(CFSub, key("k3"))
	txn.Put(CFSub, key("k9"), []byte("outside"))
	txn.Merge(CFDefault, layout.CountKey(1), layout.AppendCount(nil, 5))
	txn.Merge(CFDefault, layout.CountKey(1), layout.AppendCount(nil, -2))

	wantValue(t, txn, CFSub, key("k1"), []byte("db-k1"))
	wantValue(t, txn, CFSub, key("k2"), []byte("tx-k2"))
	wantAbsent(t, txn, CFSub, key("k3"))
	wantValue(t, txn, CFSub, key("k4"), []byte("tx-k4"))
	wantValue(t, txn, CFDefault, layout.CountKey(1), layout.AppendCount(nil, 13))
	// The store does not see the batch.
	wantValue(t, s, CFSub, key("k2"), []byte("db-k2"))
	wantAbsent(t, s, CFSub, key("k4"))

	var pinned string
	ok, err := txn.GetPinned(CFSub, key("k4"), func(v []byte) error { pinned = string(v); return nil })
	if !ok || err != nil || pinned != "tx-k4" {
		t.Fatalf("GetPinned = %v, %v, %q", ok, err, pinned)
	}
	vals, err := txn.MultiGet(CFSub, [][]byte{key("k1"), key("k3"), key("k4")})
	if err != nil || string(vals[0]) != "db-k1" || vals[1] != nil || string(vals[2]) != "tx-k4" {
		t.Fatalf("MultiGet = %q, %v", vals, err)
	}

	// The merged iterator shows batch writes over the database, hides the
	// deleted key and keeps to [k1, k6) even though k9 sits in the batch.
	it := txn.Iter(CFSub, key("k1"), key("k6"), IterOpts{FillCache: true})
	it.SeekToFirst()
	fwd := collect(t, it, true)
	it.SeekToLast()
	rev := collect(t, it, false)
	it.Seek(key("k0"))
	clamped := it.Valid() && string(it.Key()) == prefix+"k1"
	it.SeekForPrev(key("k8"))
	lastBelow := it.Valid() && string(it.Key()) == prefix+"k5"
	it.Close()
	want := []string{prefix + "k1", prefix + "k2", prefix + "k4", prefix + "k5"}
	if !slices.Equal(fwd, want) {
		t.Fatalf("forward = %q, want %q", fwd, want)
	}
	slices.Reverse(want)
	if !slices.Equal(rev, want) {
		t.Fatalf("reverse = %q, want %q", rev, want)
	}
	if !clamped || !lastBelow {
		t.Fatalf("bound clamping: Seek below lo %v, SeekForPrev above hi %v", clamped, lastBelow)
	}

	if err := txn.DeleteRange(CFSub, key("k1"), key("k2")); !errors.Is(err, ErrRangeInIndexedTxn) {
		t.Fatalf("DeleteRange in indexed Txn: %v", err)
	}
	if txn.Len() != 6 {
		t.Fatalf("Len = %d, want 6", txn.Len())
	}
	commit(t, txn)
	wantValue(t, s, CFSub, key("k2"), []byte("tx-k2"))
	wantAbsent(t, s, CFSub, key("k3"))
	wantValue(t, s, CFDefault, layout.CountKey(1), layout.AppendCount(nil, 13))
}

func TestIndexedTxnWithoutWritesReadsLive(t *testing.T) {
	s := openTest(t, nil)
	w := s.Begin()
	w.Put(CFMeta, []byte("\x00k"), []byte("v"))
	commit(t, w)
	txn := s.BeginIndexed()
	wantValue(t, txn, CFMeta, []byte("\x00k"), []byte("v"))
	it := txn.Iter(CFMeta, []byte{0}, []byte{1}, IterOpts{})
	it.SeekToFirst()
	if got := collect(t, it, true); !slices.Equal(got, []string{"\x00k"}) {
		t.Fatalf("iterator = %q", got)
	}
	it.Close()
	txn.Discard()
}
