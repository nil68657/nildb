//go:build nilengine

package nilengine

/*
#cgo CFLAGS: -I${SRCDIR}/../../rust/include
#cgo LDFLAGS: ${SRCDIR}/../../rust/target/release/libnilengine.a
#include <stdlib.h>
#include "nilengine.h"
*/
import "C"

import (
	"strings"
	"sync"
	"unsafe"
)

// libStamp is set with -ldflags -X to a hash of libnilengine.a, so a
// rebuilt library changes the link inputs and go test relinks.
var libStamp string

// Kind selects an engine at Open.
type Kind int

// The three engines.
const (
	LSM    Kind = C.NIL_ENGINE_LSM
	BTree  Kind = C.NIL_ENGINE_BTREE
	PgHeap Kind = C.NIL_ENGINE_PGHEAP
)

// String returns "lsm", "btree" or "pgheap".
func (k Kind) String() string {
	switch k {
	case LSM:
		return "lsm"
	case BTree:
		return "btree"
	case PgHeap:
		return "pgheap"
	}
	return "unknown"
}

// Error is an engine error. Class is the prefix of the C message: "io",
// "corruption", "invalid-argument", "not-found", "busy", "closed",
// "unsupported" or "panic".
type Error struct {
	Class string
	Msg   string
}

func (e *Error) Error() string {
	if e.Msg == "" {
		return "nilengine: " + e.Class
	}
	return "nilengine: " + e.Class + ": " + e.Msg
}

// Is matches another *Error of the same class with an empty Msg, so
// errors.Is(err, ErrBusy) works.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Msg == "" && t.Class == e.Class
}

// Sentinels for errors.Is.
var (
	ErrIO              = &Error{Class: "io"}
	ErrCorruption      = &Error{Class: "corruption"}
	ErrInvalidArgument = &Error{Class: "invalid-argument"}
	ErrNotFound        = &Error{Class: "not-found"}
	ErrBusy            = &Error{Class: "busy"}
	ErrClosed          = &Error{Class: "closed"}
	ErrUnsupported     = &Error{Class: "unsupported"}
	ErrPanic           = &Error{Class: "panic"}
)

// takeErr converts and frees a C error message; nil means success.
func takeErr(cerr *C.char) error {
	if cerr == nil {
		return nil
	}
	s := C.GoString(cerr)
	C.nil_free(unsafe.Pointer(cerr))
	class, msg, _ := strings.Cut(s, ": ")
	return &Error{Class: class, Msg: msg}
}

// cbytes passes a Go byte slice to C for the duration of one call. Empty
// slices become NULL, which the C interface reads as an empty key or, for
// bounds, as an open bound.
func cbytes(b []byte) (*C.char, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(unsafe.SliceData(b))), C.size_t(len(b))
}

// Version returns the engine library version.
func Version() string { return C.GoString(C.nil_version()) }

// DB is an open database. Its methods are safe for concurrent use. After
// Close they return ErrClosed; iterators and snapshots opened before Close
// stay valid until released.
type DB struct {
	mu   sync.RWMutex
	c    *C.nil_db
	kind Kind
	cfs  []string
}

// Open opens (creating if needed) the database in dir with the column
// families cfs; column family n is cfs[n]. opts is a list of name=value
// pairs as described in nilengine.h, or "" for the defaults.
func Open(kind Kind, dir string, cfs []string, opts string) (*DB, error) {
	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))
	names := C.malloc(C.size_t(len(cfs)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	defer C.free(names)
	arr := unsafe.Slice((**C.char)(names), len(cfs))
	for i, n := range cfs {
		arr[i] = C.CString(n)
	}
	defer func() {
		for _, p := range arr {
			C.free(unsafe.Pointer(p))
		}
	}()
	var copts *C.char
	if opts != "" {
		copts = C.CString(opts)
		defer C.free(unsafe.Pointer(copts))
	}
	var cerr *C.char
	c := C.nil_open(C.int(kind), cdir, (**C.char)(names), C.size_t(len(cfs)), copts, &cerr)
	if err := takeErr(cerr); err != nil {
		return nil, err
	}
	return &DB{c: c, kind: kind, cfs: append([]string(nil), cfs...)}, nil
}

// Close syncs and closes the database. A second Close returns nil.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.c == nil {
		return nil
	}
	var cerr *C.char
	C.nil_close(db.c, &cerr)
	db.c = nil
	return takeErr(cerr)
}

// with runs fn with the C handle under the read lock.
func (db *DB) with(fn func(c *C.nil_db) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.c == nil {
		return ErrClosed
	}
	return fn(db.c)
}

// Kind returns the engine kind the database was opened with.
func (db *DB) Kind() Kind { return db.kind }

// ColumnFamilies returns the column family names in index order.
func (db *DB) ColumnFamilies() []string { return append([]string(nil), db.cfs...) }

// MaxKeyLen returns the longest key the engine accepts.
func (db *DB) MaxKeyLen() int {
	n := 0
	_ = db.with(func(c *C.nil_db) error {
		n = int(C.nil_max_key_len(c))
		return nil
	})
	return n
}

// Batch collects writes that Write applies atomically and in order. A Batch
// is not safe for concurrent use; Destroy frees it.
type Batch struct {
	c *C.nil_batch
}

// NewBatch returns an empty batch.
func NewBatch() *Batch { return &Batch{c: C.nil_batch_new()} }

// Put sets key to val in column family cf.
func (b *Batch) Put(cf int, key, val []byte) {
	k, kl := cbytes(key)
	v, vl := cbytes(val)
	C.nil_batch_put(b.c, C.uint32_t(cf), k, kl, v, vl)
}

// Delete removes key from column family cf.
func (b *Batch) Delete(cf int, key []byte) {
	k, kl := cbytes(key)
	C.nil_batch_delete(b.c, C.uint32_t(cf), k, kl)
}

// DeleteRange removes every key in [start, end) of column family cf.
func (b *Batch) DeleteRange(cf int, start, end []byte) {
	s, sl := cbytes(start)
	e, el := cbytes(end)
	C.nil_batch_delete_range(b.c, C.uint32_t(cf), s, sl, e, el)
}

// Merge adds the 8-byte little-endian operand to the value at key with the
// i64add operator (the store's nildb.i64add).
func (b *Batch) Merge(cf int, key, operand []byte) {
	k, kl := cbytes(key)
	o, ol := cbytes(operand)
	C.nil_batch_merge(b.c, C.uint32_t(cf), k, kl, o, ol)
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int { return int(C.nil_batch_count(b.c)) }

// Clear empties the batch for reuse.
func (b *Batch) Clear() { C.nil_batch_clear(b.c) }

// Destroy frees the batch.
func (b *Batch) Destroy() {
	if b.c != nil {
		C.nil_batch_destroy(b.c)
		b.c = nil
	}
}

// Write applies the batch atomically; with sync it is durable on return.
func (db *DB) Write(b *Batch, sync bool) error {
	cs := C.int(0)
	if sync {
		cs = 1
	}
	return db.with(func(c *C.nil_db) error {
		var cerr *C.char
		C.nil_write(c, b.c, cs, &cerr)
		return takeErr(cerr)
	})
}

// Get reads key from the latest state and returns a Go-owned copy; ok is
// false when the key is absent.
func (db *DB) Get(cf int, key []byte) (val []byte, ok bool, err error) {
	return db.GetAt(nil, cf, key)
}

// GetAt reads key from snap, or from the latest state when snap is nil.
func (db *DB) GetAt(snap *Snapshot, cf int, key []byte) (val []byte, ok bool, err error) {
	err = db.with(func(c *C.nil_db) error {
		k, kl := cbytes(key)
		var vlen C.size_t
		var cerr *C.char
		p := C.nil_get(c, snap.handle(), C.uint32_t(cf), k, kl, &vlen, &cerr)
		if e := takeErr(cerr); e != nil {
			return e
		}
		if p == nil {
			return nil
		}
		val, ok = C.GoBytes(unsafe.Pointer(p), C.int(vlen)), true
		C.nil_free(unsafe.Pointer(p))
		return nil
	})
	return val, ok, err
}

// MultiGet reads keys from one consistent view (snap, or a view taken for
// the call) and returns one Go-owned value per key, nil where absent.
func (db *DB) MultiGet(snap *Snapshot, cf int, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	err := db.with(func(c *C.nil_db) error {
		// cgo forbids passing Go memory that holds Go pointers, so the keys
		// and the pointer arrays live in C memory for the call.
		total := 0
		for _, k := range keys {
			total += len(k)
		}
		n := C.size_t(len(keys))
		ptrSize := C.size_t(unsafe.Sizeof(uintptr(0)))
		buf := C.malloc(C.size_t(total) + 1)
		ptrs := C.malloc(n * ptrSize)
		lens := C.malloc(n * C.size_t(unsafe.Sizeof(C.size_t(0))))
		vals := C.malloc(n * ptrSize)
		vlens := C.malloc(n * C.size_t(unsafe.Sizeof(C.size_t(0))))
		defer func() {
			for _, p := range []unsafe.Pointer{buf, ptrs, lens, vals, vlens} {
				C.free(p)
			}
		}()
		b := unsafe.Slice((*byte)(buf), total+1)
		pa := unsafe.Slice((**C.char)(ptrs), len(keys))
		la := unsafe.Slice((*C.size_t)(lens), len(keys))
		off := 0
		for i, k := range keys {
			copy(b[off:], k)
			pa[i] = (*C.char)(unsafe.Add(buf, off))
			la[i] = C.size_t(len(k))
			off += len(k)
		}
		var cerr *C.char
		C.nil_multi_get(c, snap.handle(), C.uint32_t(cf), n, (**C.char)(ptrs), (*C.size_t)(lens),
			(**C.char)(vals), (*C.size_t)(vlens), &cerr)
		if e := takeErr(cerr); e != nil {
			return e
		}
		va := unsafe.Slice((**C.char)(vals), len(keys))
		vl := unsafe.Slice((*C.size_t)(vlens), len(keys))
		for i := range keys {
			if va[i] != nil {
				out[i] = C.GoBytes(unsafe.Pointer(va[i]), C.int(vl[i]))
				C.nil_free(unsafe.Pointer(va[i]))
			}
		}
		return nil
	})
	return out, err
}

// Snapshot pins the state at creation. Release it when done.
type Snapshot struct {
	c   *C.nil_snapshot
	seq uint64
}

// NewSnapshot pins the current state.
func (db *DB) NewSnapshot() (*Snapshot, error) {
	var s *Snapshot
	err := db.with(func(c *C.nil_db) error {
		var cerr *C.char
		cs := C.nil_snapshot_new(c, &cerr)
		if e := takeErr(cerr); e != nil {
			return e
		}
		s = &Snapshot{c: cs, seq: uint64(C.nil_snapshot_seq(cs))}
		return nil
	})
	return s, err
}

func (s *Snapshot) handle() *C.nil_snapshot {
	if s == nil {
		return nil
	}
	return s.c
}

// Seq returns the snapshot's sequence number (LSM), transaction id (B+
// tree) or last committed xid (pgheap).
func (s *Snapshot) Seq() uint64 { return s.seq }

// Release frees the snapshot. Iterators opened on it keep working.
func (s *Snapshot) Release() {
	if s != nil && s.c != nil {
		C.nil_snapshot_release(s.c)
		s.c = nil
	}
}

// IterOptions configures NewIterator. A nil or empty bound is open, the
// store's contract for Iter.
type IterOptions struct {
	Snapshot  *Snapshot
	Lower     []byte // inclusive
	Upper     []byte // exclusive
	FillCache bool
}

// Iterator walks one column family over [Lower, Upper). It is not safe for
// concurrent use; Close frees it.
type Iterator struct {
	c *C.nil_iter
}

// NewIterator opens an iterator; it starts unpositioned.
func (db *DB) NewIterator(cf int, o IterOptions) (*Iterator, error) {
	var it *Iterator
	err := db.with(func(c *C.nil_db) error {
		lo, lol := cbytes(o.Lower)
		hi, hil := cbytes(o.Upper)
		fill := C.int(0)
		if o.FillCache {
			fill = 1
		}
		var cerr *C.char
		ci := C.nil_iter_new(c, o.Snapshot.handle(), C.uint32_t(cf), lo, lol, hi, hil, fill, &cerr)
		if e := takeErr(cerr); e != nil {
			return e
		}
		it = &Iterator{c: ci}
		return nil
	})
	return it, err
}

// SeekToFirst positions at the first key >= Lower.
func (it *Iterator) SeekToFirst() { C.nil_iter_seek_to_first(it.c) }

// SeekToLast positions at the last key < Upper.
func (it *Iterator) SeekToLast() { C.nil_iter_seek_to_last(it.c) }

// Seek positions at the first key >= key (and >= Lower).
func (it *Iterator) Seek(key []byte) {
	k, kl := cbytes(key)
	C.nil_iter_seek(it.c, k, kl)
}

// SeekForPrev positions at the last key <= key (and < Upper).
func (it *Iterator) SeekForPrev(key []byte) {
	k, kl := cbytes(key)
	C.nil_iter_seek_for_prev(it.c, k, kl)
}

// Valid reports whether the iterator is on an entry.
func (it *Iterator) Valid() bool { return C.nil_iter_valid(it.c) != 0 }

// Next moves forward; it requires a valid iterator.
func (it *Iterator) Next() { C.nil_iter_next(it.c) }

// Prev moves backward; it requires a valid iterator.
func (it *Iterator) Prev() { C.nil_iter_prev(it.c) }

// Key returns a view of the current key in C memory, valid until the next
// move or Close. Copy what must outlive that.
func (it *Iterator) Key() []byte {
	var n C.size_t
	p := C.nil_iter_key(it.c, &n)
	if p == nil || n == 0 {
		return []byte{}
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}

// Value returns a view of the current value, with Key's lifetime. A value
// the engine fails to read comes back empty and Err reports why.
func (it *Iterator) Value() []byte {
	var n C.size_t
	p := C.nil_iter_value(it.c, &n)
	if p == nil || n == 0 {
		return []byte{}
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}

// Err returns the error that made the iterator invalid, if any.
func (it *Iterator) Err() error {
	var cerr *C.char
	C.nil_iter_status(it.c, &cerr)
	return takeErr(cerr)
}

// Close frees the iterator. It is idempotent.
func (it *Iterator) Close() {
	if it.c != nil {
		C.nil_iter_destroy(it.c)
		it.c = nil
	}
}

// Flush makes every committed write durable.
func (db *DB) Flush() error {
	return db.with(func(c *C.nil_db) error {
		var cerr *C.char
		C.nil_flush(c, &cerr)
		return takeErr(cerr)
	})
}

// FlushWAL syncs the log (LSM, pgheap) or data file (B+ tree) when sync is
// true.
func (db *DB) FlushWAL(sync bool) error {
	cs := C.int(0)
	if sync {
		cs = 1
	}
	return db.with(func(c *C.nil_db) error {
		var cerr *C.char
		C.nil_flush_wal(c, cs, &cerr)
		return takeErr(cerr)
	})
}

// CompactRange compacts [lo, hi) of cf; a nil or empty bound is open. On
// pgheap it runs VACUUM on the whole column family.
func (db *DB) CompactRange(cf int, lo, hi []byte) error {
	return db.with(func(c *C.nil_db) error {
		l, ll := cbytes(lo)
		h, hl := cbytes(hi)
		var cerr *C.char
		C.nil_compact_range(c, C.uint32_t(cf), l, ll, h, hl, &cerr)
		return takeErr(cerr)
	})
}

// Property reads a named property; ok is false for an unknown name.
func (db *DB) Property(cf int, name string) (val string, ok bool) {
	_ = db.with(func(c *C.nil_db) error {
		cname := C.CString(name)
		defer C.free(unsafe.Pointer(cname))
		p := C.nil_property(c, C.uint32_t(cf), cname)
		if p != nil {
			val, ok = C.GoString(p), true
			C.nil_free(unsafe.Pointer(p))
		}
		return nil
	})
	return val, ok
}

// LatestSeq returns the sequence number (LSM), transaction id (B+ tree) or
// xid (pgheap) of the last commit.
func (db *DB) LatestSeq() uint64 {
	var s uint64
	_ = db.with(func(c *C.nil_db) error {
		s = uint64(C.nil_latest_sequence(c))
		return nil
	})
	return s
}
