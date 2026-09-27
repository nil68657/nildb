package store

import (
	"time"

	"github.com/linxGnu/grocksdb"
)

// Reader is implemented by *Store (live committed state), by the value of
// Store.At(snap), and by every Txn.
type Reader interface {
	// Get returns a Go-owned copy of the value; ok is false when the key
	// is absent.
	Get(cf CF, key []byte) (val []byte, ok bool, err error)
	// GetPinned calls fn with a zero-copy view of the value, valid only
	// for the duration of fn, and returns fn's error. fn is not called
	// when the key is absent.
	GetPinned(cf CF, key []byte, fn func(val []byte) error) (ok bool, err error)
	// MultiGet returns one Go-owned value per key, nil where absent.
	MultiGet(cf CF, keys [][]byte) ([][]byte, error)
	// Iter returns an iterator over [lo, hi) through iterate_lower_bound
	// and iterate_upper_bound. A nil or empty bound is open.
	Iter(cf CF, lo, hi []byte, o IterOpts) Iterator
}

// IterOpts tunes one iterator. The zero value is the analytical profile
// (no block-cache fill); OLTP callers set FillCache.
type IterOpts struct {
	FillCache         bool          // false on every analytical iterator
	TotalOrderSeek    bool          // required whenever [lo, hi) may cross a prefix on a CF with an extractor
	PrefixSameAsStart bool          // geo ancestor probes only
	Readahead         int           // bytes; 0 = RocksDB auto
	AsyncIO           bool          // async_io prefetching
	Deadline          time.Duration // 0 = none; converted to an absolute micros timestamp for SetDeadline
	LowPriority       bool          // rate_limiter_priority = IO_LOW via the cgo shim
}

var (
	_ Reader = (*Store)(nil)
	_ Reader = (*snapReader)(nil)
)

// Get implements Reader on live committed state.
func (s *Store) Get(cf CF, key []byte) ([]byte, bool, error) {
	if s.closed.Load() {
		return nil, false, ErrClosed
	}
	return getCopy(s.db, s.ro, s.handle(cf), key)
}

// GetPinned implements Reader on live committed state.
func (s *Store) GetPinned(cf CF, key []byte, fn func(val []byte) error) (bool, error) {
	if s.closed.Load() {
		return false, ErrClosed
	}
	h, err := s.db.GetPinnedCF(s.ro, s.handle(cf), key)
	return callPinned(h, err, fn)
}

// MultiGet implements Reader on live committed state through RocksDB's
// batched MultiGet (one cgo call).
func (s *Store) MultiGet(cf CF, keys [][]byte) ([][]byte, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	return multiGet(s.db, s.ro, s.handle(cf), keys)
}

// Iter implements Reader on live committed state.
func (s *Store) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator {
	if s.closed.Load() {
		return &iterator{err: ErrClosed}
	}
	return s.newIter(cf, lo, hi, o, nil, nil)
}

// At returns a Reader over the snapshot. After the snapshot is released
// (Release, the janitor, or Close) every read returns ErrLeaseExpired;
// iterators opened before the release keep working. At(nil) is the live
// store.
func (s *Store) At(snap *Snapshot) Reader {
	if snap == nil {
		return s
	}
	return &snapReader{s: s, h: snap.h}
}

type snapReader struct {
	s *Store
	h *snapHandle
}

// with runs fn under the snapshot's read lock with its read options, so a
// concurrent Release waits until the C call that dereferences the
// snapshot has returned.
func (r *snapReader) with(fn func(ro *grocksdb.ReadOptions) error) error {
	r.h.mu.RLock()
	defer r.h.mu.RUnlock()
	if r.h.released {
		return ErrLeaseExpired
	}
	if r.s.closed.Load() {
		return ErrClosed
	}
	return fn(r.h.ro)
}

func (r *snapReader) Get(cf CF, key []byte) (val []byte, ok bool, err error) {
	err = r.with(func(ro *grocksdb.ReadOptions) error {
		var e error
		val, ok, e = getCopy(r.s.db, ro, r.s.handle(cf), key)
		return e
	})
	return val, ok, err
}

func (r *snapReader) GetPinned(cf CF, key []byte, fn func(val []byte) error) (bool, error) {
	var h *grocksdb.PinnableSlice
	var gerr error
	if err := r.with(func(ro *grocksdb.ReadOptions) error {
		h, gerr = r.s.db.GetPinnedCF(ro, r.s.handle(cf), key)
		return nil
	}); err != nil {
		return false, err
	}
	// The pinned value holds its own references; fn runs unlocked.
	return callPinned(h, gerr, fn)
}

func (r *snapReader) MultiGet(cf CF, keys [][]byte) (vals [][]byte, err error) {
	err = r.with(func(ro *grocksdb.ReadOptions) error {
		var e error
		vals, e = multiGet(r.s.db, ro, r.s.handle(cf), keys)
		return e
	})
	return vals, err
}

func (r *snapReader) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator {
	if r.s.closed.Load() {
		return &iterator{err: ErrClosed}
	}
	return r.s.newIter(cf, lo, hi, o, r.h, nil)
}

// getCopy reads one value through a pinnable slice and copies it into Go
// memory: one copy instead of GetCF's C malloc plus a Go copy.
func getCopy(db *grocksdb.DB, ro *grocksdb.ReadOptions, cf *grocksdb.ColumnFamilyHandle, key []byte) ([]byte, bool, error) {
	h, err := db.GetPinnedCF(ro, cf, key)
	if err != nil {
		return nil, false, err
	}
	defer h.Destroy()
	if !h.Exists() {
		return nil, false, nil
	}
	return clone(h.Data()), true, nil
}

// callPinned hands a pinned value to fn and destroys it.
func callPinned(h *grocksdb.PinnableSlice, err error, fn func([]byte) error) (bool, error) {
	if err != nil {
		return false, err
	}
	defer h.Destroy()
	if !h.Exists() {
		return false, nil
	}
	return true, fn(h.Data())
}

func multiGet(db *grocksdb.DB, ro *grocksdb.ReadOptions, cf *grocksdb.ColumnFamilyHandle, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	ps, err := db.BatchedMultiGetCF(ro, cf, false, keys...)
	if err != nil {
		return nil, err
	}
	defer ps.Destroy()
	for i, p := range ps {
		if p.Exists() {
			out[i] = clone(p.Data())
		}
	}
	return out, nil
}

// clone returns a Go-owned copy of b, non-nil even when b is empty.
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// cloneBound copies an iterator bound; nil or empty means open.
func cloneBound(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return clone(b)
}
