package store

// LiveSet backs idFilter on doc, idx, geo and col: an entry whose leading
// 4-byte id is not Live is dropped at its next compaction, or by the
// sweeper on the Rust engines. Live is called from RocksDB compaction
// threads concurrently, so an implementation must be safe for concurrent
// use; an immutable set replaced wholesale through SetLiveSet is the
// intended shape.
//
// Ordering rule (architecture.md section 5): publish a new coll_id or
// idx_id before the first batch that writes under it, and remove an id
// only after the drop batch has committed.
type LiveSet interface{ Live(id uint32) bool }

// LiveFunc adapts a function to LiveSet.
type LiveFunc func(id uint32) bool

// Live implements LiveSet.
func (f LiveFunc) Live(id uint32) bool { return f(id) }

type liveSetBox struct{ ls LiveSet }

// SetLiveSet publishes the set of live collection and index ids. nil means
// keep everything.
func (s *Store) SetLiveSet(ls LiveSet) {
	s.live.Store(&liveSetBox{ls: ls})
}

// liveSet returns the published LiveSet, or nil.
func (s *Store) liveSet() LiveSet {
	if b := s.live.Load(); b != nil {
		return b.ls
	}
	return nil
}
