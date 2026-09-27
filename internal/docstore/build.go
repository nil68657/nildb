package docstore

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BuildBatch is the number of rows whose entries one build batch commits.
const BuildBatch = 256

// Build fills a Building index from the collection's rows and marks it
// Ready. v1 builds synchronously: the caller holds coll's exclusive lock
// for the whole build, so no write runs concurrently, and DOC.CREATEINDEX
// returns when the index is ready.
//
// It iterates the collection's doc keyspace with the analytical read
// profile and commits the entries of every BuildBatch rows. A unique index
// that meets two documents with the same key fails with *DupKeyError; any
// failure (a duplicate, parallel arrays, a Decimal128 in an indexed field,
// an invalid geometry) drops the partial index, catalog record included,
// before Build returns the error.
func (d *Store) Build(coll *catalog.Collection, ix *catalog.Index) error {
	multikey, err := d.fill(coll, ix)
	if err == nil {
		if multikey {
			d.cat.SetMultikey(ix)
		}
		_, err = d.cat.SetIndexState(coll, ix, catalog.Ready)
		if err == nil {
			return nil
		}
	}
	if _, drop, derr := d.cat.DropIndex(coll, ix.Name); derr == nil {
		if derr := d.runDrop(drop); derr != nil {
			return errors.Join(err, fmt.Errorf("docstore: clean up index %s: %w", ix.Name, derr))
		}
	}
	return err
}

// fill writes ix's entries for every row of coll.
func (d *Store) fill(coll *catalog.Collection, ix *catalog.Index) (multikey bool, err error) {
	lo, hi := layout.IDBounds(coll.ID)
	it := d.st.Iter(store.CFDoc, lo, hi, store.IterOpts{Readahead: 2 << 20})
	defer it.Close()
	txn := d.st.Begin()
	defer func() { txn.Discard() }()
	pending := make(map[string][]byte) // unique keys written in the open batch -> ks(_id)
	rows := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		_, ksView := layout.SplitDocKey(it.Key())
		ksID := bytes.Clone(ksView)
		row := bson.Raw(it.Value())
		ents, mk, err := indexEntries(ix, row, ksID)
		if err != nil {
			return false, err
		}
		multikey = multikey || mk
		for _, e := range ents {
			if ix.Unique {
				if other, ok := pending[string(e.key)]; ok && !bytes.Equal(other, ksID) {
					return false, newDupKeyError(coll.NS, ix.Name, ix.Fields(), e.vals)
				}
				if err := checkUnique(d.st, coll, ix, e, ksID); err != nil {
					return false, err
				}
				pending[string(e.key)] = ksID
			}
			txn.Put(e.cf, e.key, e.val)
		}
		rows++
		if rows%BuildBatch == 0 {
			if err := txn.Commit(); err != nil {
				return false, err
			}
			txn = d.st.Begin()
			clear(pending)
		}
	}
	if err := it.Err(); err != nil {
		return false, err
	}
	return multikey, txn.Commit()
}

// CreateIndex records spec on coll and builds it (catalog.AddIndex, then
// Build). created is false, with the existing index and a nil error, when
// an identical index exists, which MongoDB's createIndexes also reports as
// success. The caller holds coll's exclusive lock.
func (d *Store) CreateIndex(coll *catalog.Collection, spec catalog.Index) (ix *catalog.Index, created bool, err error) {
	ix, err = d.cat.AddIndex(coll, spec)
	if errors.Is(err, catalog.ErrIndexExists) {
		return ix, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := d.Build(coll, ix); err != nil {
		return nil, false, err
	}
	ready, _ := coll.IndexByID(ix.ID)
	return ready, true, nil
}

// ResumeBuilds rebuilds every index left Building by a crash during a
// build: it takes the collection's exclusive lock, range-deletes the
// partial entries and runs Build. Call it once after catalog.Open and
// before serving. An index whose rebuild fails (for example a unique index
// over duplicates) is dropped, and the returned error lists each failure.
func (d *Store) ResumeBuilds() error {
	var errs []error
	for _, coll := range d.cat.Collections() {
		for _, ix := range coll.Indexes() {
			if ix.State != catalog.Building {
				continue
			}
			if err := d.resume(coll, ix); err != nil {
				errs = append(errs, fmt.Errorf("docstore: rebuild index %s of %s: %w", ix.Name, coll.NS, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (d *Store) resume(coll *catalog.Collection, ix *catalog.Index) error {
	unlock := d.st.Lock([]store.LockKey{catalog.CollLock(coll.ID, true)})
	defer unlock()
	if err := d.st.DeleteRanges([]store.RangeDelete{indexRange(ix)}, nil); err != nil {
		return err
	}
	return d.Build(coll, ix)
}
