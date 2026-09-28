package store

import (
	"encoding/binary"
	"sync/atomic"
)

// idFilter drops doc, idx, geo and col entries whose leading 4-byte id
// (coll_id on doc, idx_id elsewhere) the published LiveSet reports as not
// live. With no LiveSet published it keeps everything, as it does before
// Open binds it. It never reads the database. One instance serves all four
// CFs.
type idFilter struct {
	live  atomic.Pointer[liveSetBox]
	bound atomic.Bool
}

// Filter implements grocksdb.CompactionFilter.
func (f *idFilter) Filter(level int, key, val []byte) (bool, []byte) {
	if !f.bound.Load() || len(key) < 4 {
		return false, nil
	}
	box := f.live.Load()
	if box == nil || box.ls == nil {
		return false, nil
	}
	return !box.ls.Live(binary.BigEndian.Uint32(key)), nil
}

// Name implements grocksdb.CompactionFilter.
func (f *idFilter) Name() string { return "nildb.id" }

// SetIgnoreSnapshots implements grocksdb.CompactionFilter; see metaFilter.
func (f *idFilter) SetIgnoreSnapshots(bool) {}

// Destroy implements grocksdb.CompactionFilter.
func (f *idFilter) Destroy() {}
