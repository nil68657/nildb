package catalog

import (
	"slices"
)

// LiveSet is an immutable sorted set of live coll_ids and idx_ids. It
// implements store.LiveSet; Live is safe to call from compaction threads.
type LiveSet struct {
	ids []uint32 // sorted, no duplicates
}

func newLiveSet(ids []uint32) *LiveSet {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return &LiveSet{ids: slices.Compact(ids)}
}

// Live reports whether id belongs to a live collection or index.
func (l *LiveSet) Live(id uint32) bool {
	if l == nil {
		return false
	}
	_, ok := slices.BinarySearch(l.ids, id)
	return ok
}

// IDs returns a copy of the ids in ascending order.
func (l *LiveSet) IDs() []uint32 {
	if l == nil {
		return nil
	}
	return slices.Clone(l.ids)
}

// Len returns the number of live ids.
func (l *LiveSet) Len() int {
	if l == nil {
		return 0
	}
	return len(l.ids)
}

func (l *LiveSet) with(ids ...uint32) *LiveSet {
	var cur []uint32
	if l != nil {
		cur = l.ids
	}
	return newLiveSet(append(slices.Clone(cur), ids...))
}

func (l *LiveSet) without(ids ...uint32) *LiveSet {
	if l == nil {
		return newLiveSet(nil)
	}
	out := make([]uint32, 0, len(l.ids))
	for _, id := range l.ids {
		if !slices.Contains(ids, id) {
			out = append(out, id)
		}
	}
	return &LiveSet{ids: out}
}

// publish stores ls and hands it to the store's idFilter. The caller holds
// ddl, which makes every read-modify-write of the set atomic.
func (c *Catalog) publish(ls *LiveSet) {
	c.live.Store(ls)
	c.st.SetLiveSet(ls)
	if c.hook != nil {
		c.hook(ls)
	}
}

// LiveSet returns the set currently published to the store.
func (c *Catalog) LiveSet() *LiveSet { return c.live.Load() }

// Live reports whether id is in the published LiveSet.
func (c *Catalog) Live(id uint32) bool { return c.live.Load().Live(id) }

// Unpublish removes ids from the LiveSet, after which compaction drops
// every doc, idx, geo and col entry that starts with them. Call it only
// after the batch that deleted their records and range-deleted their keys
// has committed; CommitDrop does this for drops.
func (c *Catalog) Unpublish(ids ...uint32) {
	if len(ids) == 0 {
		return
	}
	c.ddl.Lock()
	defer c.ddl.Unlock()
	c.publish(c.live.Load().without(ids...))
}

// SetPublishHook installs fn, which runs after every LiveSet publication
// with the new set, while the catalog's DDL mutex is held. It exists so
// tests can assert publication order against store contents; fn must not
// call catalog methods that change state. nil removes the hook.
func (c *Catalog) SetPublishHook(fn func(ls *LiveSet)) {
	c.ddl.Lock()
	c.hook = fn
	c.ddl.Unlock()
}
