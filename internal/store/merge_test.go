package store

import (
	"testing"

	"github.com/nil68657/nildb/internal/layout"
)

func TestMergeI64Add(t *testing.T) {
	s := openTest(t, nil)
	key := layout.CountKey(42)
	wantCount := func(want int64) {
		t.Helper()
		v, ok := mustGet(t, s, CFDefault, key)
		if !ok {
			t.Fatalf("count absent, want %d", want)
		}
		n, err := layout.DecodeCount(v)
		if err != nil || n != want {
			t.Fatalf("count = %d, %v; want %d", n, err, want)
		}
	}
	for _, d := range []int64{5, 7, -2} {
		txn := s.Begin()
		txn.Merge(CFDefault, key, layout.AppendCount(nil, d))
		commit(t, txn)
	}
	wantCount(10)
	// Several operands in one batch, then through flush and compaction
	// (PartialMerge and FullMerge over SST input).
	txn := s.Begin()
	for range 100 {
		txn.Merge(CFDefault, key, layout.AppendCount(nil, 1))
	}
	commit(t, txn)
	wantCount(110)
	flushCompact(t, s, CFDefault)
	wantCount(110)

	// Merge onto a Put base.
	txn = s.Begin()
	txn.Put(CFDefault, key, layout.AppendCount(nil, 1000))
	txn.Merge(CFDefault, key, layout.AppendCount(nil, -1))
	commit(t, txn)
	wantCount(999)

	// A malformed operand fails the read instead of producing a number.
	bad := layout.CountKey(43)
	txn = s.Begin()
	txn.Merge(CFDefault, bad, []byte{1, 2, 3})
	commit(t, txn)
	if _, _, err := s.Get(CFDefault, bad); err == nil {
		t.Fatal("Get over a malformed merge operand succeeded")
	}
}

func TestI64AddOperator(t *testing.T) {
	var op i64Add
	v, ok := op.FullMerge(nil, nil, [][]byte{layout.AppendCount(nil, 3), layout.AppendCount(nil, 4)})
	if n, _ := layout.DecodeCount(v); !ok || n != 7 {
		t.Fatalf("FullMerge = %d, %v", n, ok)
	}
	v, ok = op.PartialMerge(nil, layout.AppendCount(nil, -9), layout.AppendCount(nil, 4))
	if n, _ := layout.DecodeCount(v); !ok || n != -5 {
		t.Fatalf("PartialMerge = %d, %v", n, ok)
	}
	if _, ok := op.FullMerge(nil, []byte{1}, nil); ok {
		t.Fatal("FullMerge accepted a 1-byte base")
	}
	if _, ok := op.PartialMerge(nil, []byte{1}, layout.AppendCount(nil, 1)); ok {
		t.Fatal("PartialMerge accepted a 1-byte operand")
	}
	if op.Name() != "nildb.i64add" {
		t.Fatalf("Name = %q", op.Name())
	}
}
