package store

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/linxGnu/grocksdb"
	"github.com/nil68657/nildb/internal/layout"
)

// subBinding is what subFilter needs from the open database.
type subBinding struct {
	db   *grocksdb.DB
	meta *grocksdb.ColumnFamilyHandle
	ro   *grocksdb.ReadOptions
}

// subFilter drops sub and zscore entries whose collection version is dead.
// The leading 8 bytes of every key are the version. A version younger than
// subYoung is kept without a lookup. Otherwise the filter looks up
// VersionKey(version) in meta: missing means the collection was deleted or
// replaced, so the entry goes; present with an expiry past metaGrace also
// goes; anything else stays.
//
// Verdicts are cached in 64 slots (slot = version & 63), each behind its
// own mutex; compaction input is sorted, so one subcompaction hits the same
// slot for every element of a collection. Caching is safe because versions
// are never reused, a drop verdict is permanent, and a stale keep verdict
// only delays reclamation to a later compaction.
type subFilter struct {
	binding atomic.Pointer[subBinding]
	now     func() int64 // Unix ms
	cache   [64]struct {
		mu      sync.Mutex
		valid   bool
		version uint64
		drop    bool
	}
	roOwned *grocksdb.ReadOptions
}

func (f *subFilter) bind(db *grocksdb.DB, meta *grocksdb.ColumnFamilyHandle) {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	f.roOwned = ro
	f.binding.Store(&subBinding{db: db, meta: meta, ro: ro})
}

func (f *subFilter) unbind() { f.binding.Store(nil) }

// destroy frees the lookup read options; call it after the database has
// closed, when no compaction can still hold the binding.
func (f *subFilter) destroy() {
	if f.roOwned != nil {
		f.roOwned.Destroy()
		f.roOwned = nil
	}
}

// Filter implements grocksdb.CompactionFilter.
func (f *subFilter) Filter(level int, key, val []byte) (bool, []byte) {
	b := f.binding.Load()
	if b == nil || len(key) < 8 {
		return false, nil
	}
	version := binary.BigEndian.Uint64(key)
	nowMS := f.now()
	if nowMS*1000-layout.VersionMicros(version) < subYoung.Microseconds() {
		return false, nil
	}
	slot := &f.cache[version&63]
	slot.mu.Lock()
	if slot.valid && slot.version == version {
		drop := slot.drop
		slot.mu.Unlock()
		return drop, nil
	}
	slot.mu.Unlock()

	drop, ok := f.lookup(b, version, nowMS)
	if !ok {
		return false, nil
	}
	slot.mu.Lock()
	slot.valid, slot.version, slot.drop = true, version, drop
	slot.mu.Unlock()
	return drop, nil
}

// lookup decides a version from its version-map entry. ok is false when
// the read failed; the entry is then kept and nothing is cached.
func (f *subFilter) lookup(b *subBinding, version uint64, nowMS int64) (drop, ok bool) {
	var k [9]byte
	h, err := b.db.GetPinnedCF(b.ro, b.meta, layout.VersionKey(k[:0], version))
	if err != nil {
		return false, false
	}
	defer h.Destroy()
	if !h.Exists() {
		return true, true
	}
	e, err := layout.DecodeVersionEntry(h.Data())
	if err != nil {
		return false, true
	}
	return expiredPastGrace(e.ExpireMS, nowMS), true
}

// Name implements grocksdb.CompactionFilter.
func (f *subFilter) Name() string { return "nildb.sub" }

// SetIgnoreSnapshots implements grocksdb.CompactionFilter; see metaFilter.
func (f *subFilter) SetIgnoreSnapshots(bool) {}

// Destroy implements grocksdb.CompactionFilter.
func (f *subFilter) Destroy() {}
