package store

import (
	"bytes"
	"errors"
	"fmt"
)

// Txn is the unit of atomicity. Begin wraps a plain write batch whose reads
// see committed state; BeginIndexed returns a transaction whose reads see
// its own writes: a WriteBatchWI(overwrite_key=true) on RocksDB, the
// store's overlay on the Rust engines. Only EXEC uses BeginIndexed.
//
// Put, Delete and Merge copy key and value into the batch, so callers may
// reuse their buffers. A Txn is not safe for concurrent use. After Commit or
// Discard a write panics and Commit returns an error.
//
// Inside an indexed Txn, do not write while an iterator from that Txn is
// open: RocksDB invalidates the iterator's current entry on every batch
// write. Collect the keys, Close the iterator, then write.
type Txn interface {
	Reader
	Put(cf CF, key, val []byte)
	Delete(cf CF, key []byte)
	Merge(cf CF, key, operand []byte)       // CFDefault only (nildb.i64add)
	DeleteRange(cf CF, lo, hi []byte) error // ErrRangeInIndexedTxn under BeginIndexed
	Len() int
	Commit() error // one engine write with the configured sync
	Discard()
}

var errTxnDone = errors.New("store: Txn already committed or discarded")

// Begin returns an autocommit transaction over a plain write batch. Its
// reads go to live committed state and never see its own writes. The
// engine batch is created on the first write, so a read-only command pays
// nothing.
func (s *Store) Begin() Txn { return &batchTxn{s: s} }

// BeginIndexed returns a transaction whose reads merge its own writes over
// live committed state.
func (s *Store) BeginIndexed() Txn { return s.kv.beginIndexed(s) }

// syncCommits reports whether commits sync (Fsync "always").
func (s *Store) syncCommits() bool { return s.cfg.Fsync == FsyncAlways }

// mergeErr checks that Merge targets the one CF with a merge operator.
func mergeErr(cf CF) error {
	if cf == CFDefault {
		return nil
	}
	return fmt.Errorf("store: Merge on %s: only %s has a merge operator", cf, CFDefault)
}

// rangeErr validates a DeleteRange; skip is true for an empty range.
func rangeErr(cf CF, lo, hi []byte) (skip bool, err error) {
	if len(hi) == 0 {
		return false, fmt.Errorf("store: DeleteRange on %s needs an upper bound", cf)
	}
	switch c := bytes.Compare(lo, hi); {
	case c == 0:
		return true, nil
	case c > 0:
		return false, fmt.Errorf("store: DeleteRange on %s: lower bound %x above upper bound %x", cf, lo, hi)
	}
	return false, nil
}

type batchTxn struct {
	s    *Store
	b    kvBatch
	n    int
	err  error // first Merge misuse; Commit returns it and writes nothing
	done bool
}

func (t *batchTxn) batch() kvBatch {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	if t.b == nil {
		t.b = t.s.kv.newBatch()
	}
	return t.b
}

func (t *batchTxn) Get(cf CF, key []byte) ([]byte, bool, error) { return t.s.Get(cf, key) }

func (t *batchTxn) GetPinned(cf CF, key []byte, fn func([]byte) error) (bool, error) {
	return t.s.GetPinned(cf, key, fn)
}

func (t *batchTxn) MultiGet(cf CF, keys [][]byte) ([][]byte, error) { return t.s.MultiGet(cf, keys) }

func (t *batchTxn) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator { return t.s.Iter(cf, lo, hi, o) }

func (t *batchTxn) Put(cf CF, key, val []byte) {
	t.batch().put(cf, key, val)
	t.n++
}

func (t *batchTxn) Delete(cf CF, key []byte) {
	t.batch().delete(cf, key)
	t.n++
}

func (t *batchTxn) Merge(cf CF, key, operand []byte) {
	b := t.batch()
	if err := mergeErr(cf); err != nil {
		if t.err == nil {
			t.err = err
		}
		return
	}
	b.merge(cf, key, operand)
	t.n++
}

func (t *batchTxn) DeleteRange(cf CF, lo, hi []byte) error {
	b := t.batch()
	skip, err := rangeErr(cf, lo, hi)
	if err != nil || skip {
		return err
	}
	b.deleteRange(cf, lo, hi)
	t.n++
	return nil
}

func (t *batchTxn) Len() int { return t.n }

func (t *batchTxn) Commit() error {
	if t.done {
		return errTxnDone
	}
	defer t.Discard()
	switch {
	case t.err != nil:
		return t.err
	case t.n == 0:
		return nil
	case t.s.closed.Load():
		return ErrClosed
	}
	return t.s.kv.write(t.b, t.s.syncCommits())
}

func (t *batchTxn) Discard() {
	t.done = true
	if t.b != nil {
		t.b.destroy()
		t.b = nil
	}
}
