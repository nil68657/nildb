package docstore

import (
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
)

// DropCollection deletes coll: its rows, the entries of every index, its
// catalog records and its count, in one store.DeleteRanges batch. Each
// range is then scheduled for compaction, and the ids leave the LiveSet
// only after the batch has committed. The caller holds coll's exclusive
// lock.
func (d *Store) DropCollection(coll *catalog.Collection) error {
	drop, err := d.cat.DropCollection(coll)
	if err != nil {
		return err
	}
	return d.runDrop(drop)
}

// DropIndex deletes the index called name of coll: its entries and its
// catalog record in one batch, then compaction scheduling and LiveSet
// removal as for DropCollection. The caller holds coll's exclusive lock.
func (d *Store) DropIndex(coll *catalog.Collection, name string) error {
	_, drop, err := d.cat.DropIndex(coll, name)
	if err != nil {
		return err
	}
	return d.runDrop(drop)
}

// indexRange returns the keyspace of ix's entries.
func indexRange(ix *catalog.Index) store.RangeDelete {
	lo, hi := layout.IDBounds(ix.ID)
	return store.RangeDelete{CF: cfOf(ix), Lo: lo, Hi: hi}
}

// dropRanges returns the keyspaces a drop removes. The catalog never
// issues id MaxUint32, so every range has an upper bound.
func dropRanges(drop catalog.Drop) []store.RangeDelete {
	var ranges []store.RangeDelete
	if drop.Whole {
		lo, hi := layout.IDBounds(drop.Coll.ID)
		ranges = append(ranges, store.RangeDelete{CF: store.CFDoc, Lo: lo, Hi: hi})
	}
	for _, ix := range drop.Indexes {
		ranges = append(ranges, indexRange(ix))
	}
	return ranges
}

func (d *Store) runDrop(drop catalog.Drop) error {
	ranges := dropRanges(drop)
	if err := d.st.DeleteRanges(ranges, drop.Records); err != nil {
		d.cat.AbortDrop(drop)
		return err
	}
	for _, r := range ranges {
		d.st.ScheduleCompact(r.CF, r.Lo, r.Hi)
	}
	d.cat.CommitDrop(drop)
	return nil
}
