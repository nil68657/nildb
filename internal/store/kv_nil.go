//go:build nilengine

package store

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nil68657/nildb/internal/nilengine"
)

// errReadOnly is what writes return on a store opened read-only over a Rust
// engine, which has no read-only open of its own.
var errReadOnly = errors.New("store: opened read-only")

// errTimedOut ends an iterator whose IterOpts.Deadline passed; query's
// readErr maps "timed out" to ErrTimeout, as it does RocksDB's TimedOut.
var errTimedOut = errors.New("store: iterator deadline passed: operation timed out")

// nilKV is one of the Rust engines through internal/nilengine: the LSM
// tree, the B+ tree or pgheap. Engine column family n is cfNames[n]. One
// cache and one memtable budget serve all eight families.
type nilKV struct {
	db       *nilengine.DB
	kind     nilengine.Kind
	readOnly bool
}

// engineAvailable reports whether this build has the named engine: this
// one, built with the nilengine tag, has all four.
func engineAvailable(string) error { return nil }

func openNil(cfg Config, readOnly bool) (kv, error) {
	var kind nilengine.Kind
	switch cfg.Engine {
	case EngineLSM:
		kind = nilengine.LSM
	case EngineBTree:
		kind = nilengine.BTree
	case EnginePgHeap:
		kind = nilengine.PgHeap
	default:
		return nil, checkEngine(cfg.Engine)
	}
	opts := fmt.Sprintf("cache_bytes=%d;write_buffer_size=%d", cfg.BlockCacheBytes, cfg.WriteBufferBytes)
	if readOnly {
		opts += ";create_if_missing=false"
	}
	db, err := nilengine.Open(kind, cfg.Dir, cfNames[:], opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %s with engine %s: %w", cfg.Dir, cfg.Engine, err)
	}
	return &nilKV{db: db, kind: kind, readOnly: readOnly}, nil
}

func (k *nilKV) name() string    { return k.kind.String() }
func (k *nilKV) version() string { return nilengine.Version() }

func snapOf(s kvSnap) *nilengine.Snapshot {
	if s == nil {
		return nil
	}
	return s.(*nilengine.Snapshot)
}

func (k *nilKV) get(s kvSnap, cf CF, key []byte) ([]byte, bool, error) {
	checkCF(cf)
	return k.db.GetAt(snapOf(s), int(cf), key)
}

func (k *nilKV) getPinned(s kvSnap, cf CF, key []byte) (pinned, error) {
	v, ok, err := k.get(s, cf, key)
	if err != nil {
		return nil, err
	}
	return &goPinned{v: v, ok: ok}, nil
}

func (k *nilKV) multiGet(s kvSnap, cf CF, keys [][]byte) ([][]byte, error) {
	checkCF(cf)
	return k.db.MultiGet(snapOf(s), int(cf), keys)
}

// prefixLen is the prefix RocksDB's extractor gives cf, which
// PrefixSameAsStart keeps an iterator inside.
func prefixLen(cf CF) int {
	switch cf {
	case CFSub, CFZScore:
		return 8
	case CFGeo:
		return 12
	}
	return 0
}

func (k *nilKV) iter(s kvSnap, cf CF, lo, hi []byte, o IterOpts) Iterator {
	checkCF(cf)
	it, err := k.db.NewIterator(int(cf), nilengine.IterOptions{
		Snapshot: snapOf(s), Lower: lo, Upper: hi, FillCache: o.FillCache,
	})
	if err != nil {
		return &failedIter{err: err}
	}
	ni := &nilIter{it: it}
	if o.Deadline > 0 {
		ni.deadline = time.Now().Add(o.Deadline)
	}
	if o.PrefixSameAsStart {
		ni.plen = prefixLen(cf)
	}
	return ni
}

// nilIter is a Rust engine iterator with the IterOpts the engines lack: a
// deadline checked every 64 moves, and PrefixSameAsStart as a check that
// the key keeps the prefix of the last seek target.
type nilIter struct {
	it       *nilengine.Iterator
	deadline time.Time
	moves    int
	err      error
	plen     int
	prefix   []byte
	closed   bool
}

func (i *nilIter) moved() {
	if i.deadline.IsZero() || i.err != nil {
		return
	}
	i.moves++
	if i.moves%64 == 1 && time.Now().After(i.deadline) {
		i.err = errTimedOut
	}
}

func (i *nilIter) target(k []byte) {
	if i.plen == 0 {
		return
	}
	if len(k) >= i.plen {
		i.prefix = append(i.prefix[:0], k[:i.plen]...)
	} else {
		i.prefix = nil
	}
}

func (i *nilIter) Seek(k []byte)        { i.target(k); i.it.Seek(k); i.moved() }
func (i *nilIter) SeekForPrev(k []byte) { i.target(k); i.it.SeekForPrev(k); i.moved() }
func (i *nilIter) SeekToFirst()         { i.prefix = nil; i.it.SeekToFirst(); i.moved() }
func (i *nilIter) SeekToLast()          { i.prefix = nil; i.it.SeekToLast(); i.moved() }

func (i *nilIter) Valid() bool {
	if i.closed || i.err != nil || !i.it.Valid() {
		return false
	}
	return i.prefix == nil || bytes.HasPrefix(i.it.Key(), i.prefix)
}

func (i *nilIter) Next() {
	if i.Valid() {
		i.it.Next()
		i.moved()
	}
}

func (i *nilIter) Prev() {
	if i.Valid() {
		i.it.Prev()
		i.moved()
	}
}

func (i *nilIter) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.it.Key()
}

func (i *nilIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.it.Value()
}

func (i *nilIter) Err() error {
	if i.err != nil || i.closed {
		return i.err
	}
	return i.it.Err()
}

// Close frees the engine iterator; it is idempotent, and Err keeps
// returning a deadline error after it.
func (i *nilIter) Close() {
	i.closed = true
	i.it.Close()
}

// nilBatch is an engine write batch.
type nilBatch struct{ b *nilengine.Batch }

func (k *nilKV) newBatch() kvBatch { return &nilBatch{b: nilengine.NewBatch()} }

func (b *nilBatch) put(cf CF, key, val []byte) { checkCF(cf); b.b.Put(int(cf), key, val) }
func (b *nilBatch) delete(cf CF, key []byte)   { checkCF(cf); b.b.Delete(int(cf), key) }
func (b *nilBatch) merge(cf CF, key, op []byte) {
	checkCF(cf)
	b.b.Merge(int(cf), key, op)
}
func (b *nilBatch) deleteRange(cf CF, lo, hi []byte) { checkCF(cf); b.b.DeleteRange(int(cf), lo, hi) }
func (b *nilBatch) destroy()                         { b.b.Destroy() }

func (k *nilKV) write(b kvBatch, sync bool) error {
	if k.readOnly {
		return errReadOnly
	}
	return k.db.Write(b.(*nilBatch).b, sync)
}

func (k *nilKV) beginIndexed(s *Store) Txn { return newOverlayTxn(s) }

func (k *nilKV) snapshot() (kvSnap, uint64, error) {
	sn, err := k.db.NewSnapshot()
	if err != nil {
		return nil, 0, err
	}
	return sn, sn.Seq(), nil
}

func (k *nilKV) release(s kvSnap) { snapOf(s).Release() }

func (k *nilKV) property(cf CF, name string) (string, bool) {
	checkCF(cf)
	return k.db.Property(int(cf), name)
}

func (k *nilKV) intProperty(cf CF, name string) (uint64, bool) {
	v, ok := k.property(cf, name)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return n, err == nil
}

func (k *nilKV) approxSizes(cf CF, ranges [][2][]byte) ([]uint64, error) {
	checkCF(cf)
	return k.db.ApproximateSizes(int(cf), ranges)
}

func (k *nilKV) compact(cf CF, lo, hi []byte) error {
	checkCF(cf)
	if k.readOnly {
		return errReadOnly
	}
	return k.db.CompactRange(int(cf), lo, hi)
}

func (k *nilKV) flush(cf CF) error {
	checkCF(cf)
	if k.readOnly {
		return errReadOnly
	}
	return k.db.Flush()
}

func (k *nilKV) flushWAL(sync bool) error    { return k.db.FlushWAL(sync) }
func (k *nilKV) checkpoint(dir string) error { return k.db.Checkpoint(dir) }
func (k *nilKV) latestSeq() uint64           { return k.db.LatestSeq() }

// stats joins the nil.stats text of every family; the engines keep their
// counters without being asked. On pgheap it adds what holds VACUUM back:
// the oldest registered xmin and the number of registered snapshots.
func (k *nilKV) stats() (string, bool) {
	var b strings.Builder
	for cf := range CF(NumCFs) {
		if v, ok := k.db.Property(int(cf), "nil.stats"); ok {
			b.WriteString(v)
		}
	}
	if k.kind == nilengine.PgHeap {
		xmin, _ := k.db.Property(0, "nil.pgheap.oldest-xmin")
		snaps, _ := k.db.Property(0, "nil.pgheap.registered-snapshots")
		fmt.Fprintf(&b, "oldest xmin %s, registered snapshots %s\n", xmin, snaps)
	}
	return b.String(), true
}

func (k *nilKV) handle(cf CF) any { return int(cf) }

func (k *nilKV) shimActive() (bool, error) {
	return false, fmt.Errorf("engine %s has no rate limiter", k.name())
}

func (k *nilKV) sweeps() bool    { return !k.readOnly }
func (k *nilKV) bind(*Store)     {}
func (k *nilKV) stopBackground() {}
func (k *nilKV) close() error    { return k.db.Close() }
