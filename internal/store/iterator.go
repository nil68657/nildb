package store

import (
	"bytes"
	"time"

	"github.com/linxGnu/grocksdb"
)

// Iterator walks one column family over [lo, hi). Key and Value return
// views into C memory (grocksdb KeySlice/ValueSlice) that stay valid only
// until the next move or Close; copy what must outlive that. Close is
// idempotent and must be called, or the iterator pins memtables and SST
// files.
type Iterator interface {
	Seek(key []byte)
	SeekForPrev(key []byte)
	SeekToFirst()
	SeekToLast()
	Valid() bool
	Next()
	Prev()
	Key() []byte   // view into C memory, valid until the next move (KeySlice)
	Value() []byte // same (ValueSlice)
	Err() error
	Close()
}

// iterator wraps a grocksdb iterator with the read options and bound
// slices it depends on: RocksDB keeps pointers to lo and hi, so they live
// here until Close.
type iterator struct {
	it     *grocksdb.Iterator
	ro     *grocksdb.ReadOptions
	lo, hi []byte
	err    error // set for an iterator that failed before it opened
	// check enforces [lo, hi) in Go. Iterators over an indexed Txn merge
	// the batch with the database, and the batch side is checked here.
	check bool
}

// newIter builds an iterator over live state, over the snapshot h when it
// is non-nil, or over wb merged with live state when wb is non-nil.
func (s *Store) newIter(cf CF, lo, hi []byte, o IterOpts, h *snapHandle, wb *grocksdb.WriteBatchWI) Iterator {
	cfh := s.handle(cf)
	i := &iterator{lo: cloneBound(lo), hi: cloneBound(hi), check: wb != nil}
	ro := grocksdb.NewDefaultReadOptions()
	i.ro = ro
	if i.lo != nil {
		ro.SetIterateLowerBound(i.lo)
	}
	if i.hi != nil {
		ro.SetIterateUpperBound(i.hi)
	}
	ro.SetFillCache(o.FillCache)
	if o.TotalOrderSeek {
		ro.SetTotalOrderSeek(true)
	}
	if o.PrefixSameAsStart {
		ro.SetPrefixSameAsStart(true)
	}
	if o.Readahead > 0 {
		ro.SetReadaheadSize(uint64(o.Readahead))
	}
	if o.AsyncIO {
		ro.SetAsyncIO(true)
	}
	if o.Deadline > 0 {
		// RocksDB compares the deadline with env->NowMicros(), the wall
		// clock, so this uses time.Now and not the store clock.
		ro.SetDeadline(uint64(time.Now().UnixMicro() + o.Deadline.Microseconds()))
	}
	if o.LowPriority {
		setIOLow(ro)
	}

	switch {
	case h != nil:
		h.mu.RLock()
		if h.released {
			h.mu.RUnlock()
			i.err = ErrLeaseExpired
			i.Close()
			return i
		}
		// The iterator reads the sequence number at creation, so the
		// snapshot may be released while it stays open.
		ro.SetSnapshot(h.snap)
		i.it = s.db.NewIteratorCF(ro, cfh)
		h.mu.RUnlock()
	case wb != nil:
		base := s.db.NewIteratorCF(ro, cfh)
		// The returned iterator owns base and destroys it on Close.
		i.it = wb.NewIteratorWithBaseCFReadOpts(s.db, base, cfh, ro)
	default:
		i.it = s.db.NewIteratorCF(ro, cfh)
	}
	return i
}

func (i *iterator) inBounds() bool {
	k := i.it.KeySlice().Data()
	if i.lo != nil && bytes.Compare(k, i.lo) < 0 {
		return false
	}
	return i.hi == nil || bytes.Compare(k, i.hi) < 0
}

// Valid reports whether the iterator is positioned on an entry in range.
func (i *iterator) Valid() bool {
	if i.it == nil || !i.it.Valid() {
		return false
	}
	return !i.check || i.inBounds()
}

// Seek positions at the first key >= key (and >= lo).
func (i *iterator) Seek(key []byte) {
	if i.it == nil {
		return
	}
	if i.check && i.lo != nil && bytes.Compare(key, i.lo) < 0 {
		key = i.lo
	}
	i.it.Seek(key)
}

// SeekForPrev positions at the last key <= key (and < hi).
func (i *iterator) SeekForPrev(key []byte) {
	if i.it == nil {
		return
	}
	if i.check && i.hi != nil && bytes.Compare(key, i.hi) >= 0 {
		i.SeekToLast()
		return
	}
	i.it.SeekForPrev(key)
}

// SeekToFirst positions at the first key >= lo.
func (i *iterator) SeekToFirst() {
	if i.it == nil {
		return
	}
	if i.check && i.lo != nil {
		i.it.Seek(i.lo)
		return
	}
	i.it.SeekToFirst()
}

// SeekToLast positions at the last key < hi.
func (i *iterator) SeekToLast() {
	if i.it == nil {
		return
	}
	if i.check && i.hi != nil {
		i.it.SeekForPrev(i.hi)
		if i.it.Valid() && bytes.Compare(i.it.KeySlice().Data(), i.hi) >= 0 {
			i.it.Prev()
		}
		return
	}
	i.it.SeekToLast()
}

// Next moves forward.
func (i *iterator) Next() {
	if i.it != nil {
		i.it.Next()
	}
}

// Prev moves backward.
func (i *iterator) Prev() {
	if i.it != nil {
		i.it.Prev()
	}
}

// Key returns a view of the current key, valid until the next move.
func (i *iterator) Key() []byte {
	if i.it == nil {
		return nil
	}
	return i.it.KeySlice().Data()
}

// Value returns a view of the current value, valid until the next move.
func (i *iterator) Value() []byte {
	if i.it == nil {
		return nil
	}
	return i.it.ValueSlice().Data()
}

// Err returns the iterator's error: RocksDB's (TimedOut after Deadline,
// corruption, I/O) or ErrLeaseExpired / ErrClosed for an iterator that
// could not open.
func (i *iterator) Err() error {
	if i.err != nil {
		return i.err
	}
	if i.it == nil {
		return nil
	}
	return i.it.Err()
}

// Close frees the iterator and its read options. It is idempotent; Err
// keeps returning a pre-open error after Close.
func (i *iterator) Close() {
	if i.it != nil {
		i.it.Close()
		i.it = nil
	}
	if i.ro != nil {
		i.ro.Destroy()
		i.ro = nil
	}
	i.lo, i.hi = nil, nil
}
