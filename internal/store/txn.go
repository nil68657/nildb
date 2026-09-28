package store

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/linxGnu/grocksdb"
)

// Txn is the unit of atomicity. Begin wraps a plain WriteBatch whose reads
// see committed state; BeginIndexed wraps a WriteBatchWI(overwrite_key=true)
// whose reads see its own writes. Only EXEC uses BeginIndexed.
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
	Commit() error // db.Write / db.WriteWI with the configured WriteOptions
	Discard()
}

var errTxnDone = errors.New("store: Txn already committed or discarded")

// Begin returns an autocommit transaction over a plain WriteBatch. Its
// reads go to live committed state and never see its own writes. The C
// batch is created on the first write, so a read-only command pays
// nothing.
func (s *Store) Begin() Txn { return &batchTxn{s: s} }

// BeginIndexed returns a transaction over a WriteBatchWI(overwrite_key=true)
// whose reads merge its own writes over live committed state.
func (s *Store) BeginIndexed() Txn { return &indexedTxn{s: s} }

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
	wb   *grocksdb.WriteBatch
	n    int
	err  error // first Merge misuse; Commit returns it and writes nothing
	done bool
}

func (t *batchTxn) batch() *grocksdb.WriteBatch {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	if t.wb == nil {
		t.wb = grocksdb.NewWriteBatch()
	}
	return t.wb
}

func (t *batchTxn) Get(cf CF, key []byte) ([]byte, bool, error) { return t.s.Get(cf, key) }

func (t *batchTxn) GetPinned(cf CF, key []byte, fn func([]byte) error) (bool, error) {
	return t.s.GetPinned(cf, key, fn)
}

func (t *batchTxn) MultiGet(cf CF, keys [][]byte) ([][]byte, error) { return t.s.MultiGet(cf, keys) }

func (t *batchTxn) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator { return t.s.Iter(cf, lo, hi, o) }

func (t *batchTxn) Put(cf CF, key, val []byte) {
	t.batch().PutCF(t.s.handle(cf), key, val)
	t.n++
}

func (t *batchTxn) Delete(cf CF, key []byte) {
	t.batch().DeleteCF(t.s.handle(cf), key)
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
	b.MergeCF(t.s.handle(cf), key, operand)
	t.n++
}

func (t *batchTxn) DeleteRange(cf CF, lo, hi []byte) error {
	b := t.batch()
	skip, err := rangeErr(cf, lo, hi)
	if err != nil || skip {
		return err
	}
	b.DeleteRangeCF(t.s.handle(cf), lo, hi)
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
	return t.s.db.Write(t.s.wo, t.wb)
}

func (t *batchTxn) Discard() {
	t.done = true
	if t.wb != nil {
		t.wb.Destroy()
		t.wb = nil
	}
}

type indexedTxn struct {
	s    *Store
	wb   *grocksdb.WriteBatchWI
	n    int
	err  error
	done bool
}

func (t *indexedTxn) batch() *grocksdb.WriteBatchWI {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	if t.wb == nil {
		t.wb = grocksdb.NewWriteBatchWI(0, true)
	}
	return t.wb
}

// pinned reads key from the batch merged over live state.
func (t *indexedTxn) pinned(cf CF, key []byte) (*grocksdb.PinnableSlice, error) {
	if t.s.closed.Load() {
		return nil, ErrClosed
	}
	return t.wb.GetPinnableFromDBWithCF(t.s.db, t.s.ro, t.s.handle(cf), key)
}

func (t *indexedTxn) Get(cf CF, key []byte) ([]byte, bool, error) {
	if t.wb == nil {
		return t.s.Get(cf, key)
	}
	h, err := t.pinned(cf, key)
	if err != nil {
		return nil, false, err
	}
	defer h.Destroy()
	if !h.Exists() {
		return nil, false, nil
	}
	return clone(h.Data()), true, nil
}

func (t *indexedTxn) GetPinned(cf CF, key []byte, fn func([]byte) error) (bool, error) {
	if t.wb == nil {
		return t.s.GetPinned(cf, key, fn)
	}
	h, err := t.pinned(cf, key)
	return callPinned(h, err, fn)
}

func (t *indexedTxn) MultiGet(cf CF, keys [][]byte) ([][]byte, error) {
	if t.wb == nil {
		return t.s.MultiGet(cf, keys)
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		v, ok, err := t.Get(cf, k)
		if err != nil {
			return nil, err
		}
		if ok {
			out[i] = v
		}
	}
	return out, nil
}

func (t *indexedTxn) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator {
	if t.wb == nil {
		return t.s.Iter(cf, lo, hi, o)
	}
	if t.s.closed.Load() {
		return &iterator{err: ErrClosed}
	}
	return t.s.newIter(cf, lo, hi, o, nil, t.wb)
}

func (t *indexedTxn) Put(cf CF, key, val []byte) {
	t.batch().PutCF(t.s.handle(cf), key, val)
	t.n++
}

func (t *indexedTxn) Delete(cf CF, key []byte) {
	t.batch().DeleteCF(t.s.handle(cf), key)
	t.n++
}

func (t *indexedTxn) Merge(cf CF, key, operand []byte) {
	b := t.batch()
	if err := mergeErr(cf); err != nil {
		if t.err == nil {
			t.err = err
		}
		return
	}
	b.MergeCF(t.s.handle(cf), key, operand)
	t.n++
}

// DeleteRange always fails: the C++ WriteBatchWithIndex rejects range
// deletes. Commands that need one are NoMulti.
func (t *indexedTxn) DeleteRange(cf CF, lo, hi []byte) error {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	return ErrRangeInIndexedTxn
}

func (t *indexedTxn) Len() int { return t.n }

func (t *indexedTxn) Commit() error {
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
	return t.s.db.WriteWI(t.s.wo, t.wb)
}

func (t *indexedTxn) Discard() {
	t.done = true
	if t.wb != nil {
		t.wb.Destroy()
		t.wb = nil
	}
}
