package store

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// Grace periods of the compaction filters (architecture.md section 5).
const (
	// metaGrace keeps an expired metadata or version-map entry for five
	// minutes past its expiry, covering a command that read the metadata
	// and is still iterating the collection (Kvrocks's window).
	metaGrace = 300 * time.Second
	// subYoung skips the version lookup for sub-keys whose version is
	// younger than a minute: fresh data costs nothing to keep.
	subYoung = 60 * time.Second
)

// A compaction filter shared by every subcompaction thread of its CFs.
// grocksdb has no filter factory, so each filter is a pure function of
// (key, value, now) plus fixed-size synchronised state. Filters keep every
// entry until the store binds them after Open, and again after Close
// unbinds them. RocksDB (>= 6.0) ignores snapshots when filtering, so an
// expired key can vanish from a held lease; lazy expiry hides it anyway.

// metaFilter drops expired entries of the meta CF once metaGrace has
// passed: Redis metadata (| db | key |, expire_ms in value bytes 1-8) and
// version-map entries (| 0x76 | version |, expire_ms in value bytes 0-7).
// It never reads the database.
type metaFilter struct {
	now   func() int64 // Unix ms
	bound atomic.Bool
}

// Filter implements grocksdb.CompactionFilter.
func (f *metaFilter) Filter(level int, key, val []byte) (bool, []byte) {
	if !f.bound.Load() || len(key) == 0 {
		return false, nil
	}
	var expire int64
	switch {
	case key[0] < layout.NumDBs:
		// | flags (1) | expire_ms (8) | ...; refuse to judge an unknown
		// layout version.
		if len(val) < layout.StringHeaderLen || val[0]>>4 != layout.LayoutVersion {
			return false, nil
		}
		expire = int64(binary.BigEndian.Uint64(val[1:9]))
	case key[0] == layout.VersionMapPrefix && len(key) == 9:
		if len(val) < 9 {
			return false, nil
		}
		expire = int64(binary.BigEndian.Uint64(val[:8]))
	default:
		return false, nil
	}
	return expiredPastGrace(expire, f.now()), nil
}

// expiredPastGrace reports whether expire_ms + metaGrace < now, written so
// that a far-future expire_ms cannot overflow into the past.
func expiredPastGrace(expireMS, nowMS int64) bool {
	return expireMS != 0 && expireMS < nowMS-metaGrace.Milliseconds()
}

// Name implements grocksdb.CompactionFilter.
func (f *metaFilter) Name() string { return "nildb.meta" }

// SetIgnoreSnapshots implements grocksdb.CompactionFilter. RocksDB ignores
// snapshots during filtering regardless, so it does nothing.
func (f *metaFilter) SetIgnoreSnapshots(bool) {}

// Destroy implements grocksdb.CompactionFilter.
func (f *metaFilter) Destroy() {}
