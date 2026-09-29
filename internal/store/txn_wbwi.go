package store

import (
	"github.com/linxGnu/grocksdb"
)

// indexedTxn is RocksDB's read-your-writes transaction: a
// WriteBatchWI(overwrite_key=true) whose reads merge the batch over live
// committed state.
type indexedTxn struct {
	s    *Store
	k    *rocksKV
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
	return t.wb.GetPinnableFromDBWithCF(t.k.db, t.k.ro, t.k.h(cf), key)
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
	if err != nil {
		return false, err
	}
	return callPinned(h, nil, fn)
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
		return &failedIter{err: ErrClosed}
	}
	return t.k.newIterator(cf, lo, hi, o, nil, t.wb)
}

func (t *indexedTxn) Put(cf CF, key, val []byte) {
	t.batch().PutCF(t.k.h(cf), key, val)
	t.n++
}

func (t *indexedTxn) Delete(cf CF, key []byte) {
	t.batch().DeleteCF(t.k.h(cf), key)
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
	b.MergeCF(t.k.h(cf), key, operand)
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
	return t.k.db.WriteWI(t.k.writeOpts(t.s.syncCommits()), t.wb)
}

func (t *indexedTxn) Discard() {
	t.done = true
	if t.wb != nil {
		t.wb.Destroy()
		t.wb = nil
	}
}
