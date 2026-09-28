// Package docstore is the single-document write path (architecture.md
// sections 4 and 5): it writes a row, the btree and 2dsphere entries of
// every index of its collection and the n/<coll_id> count merge in one
// store.Txn, deletes the entries computed from the old document on update
// and delete (sync-full), enforces unique indexes with E11000 errors, drops
// collections and indexes through store.DeleteRanges, and builds new
// indexes synchronously.
//
// Locking contract. docstore takes no locks on the write path; the caller
// holds them:
//
//   - point writes: catalog.CollLock(coll.ID, false), then LockKeys (the
//     document stripe) and the unique-value stripes (UniqLockKeys);
//   - DDL (Build, CreateIndex, DropIndex, DropCollection) and filter-driven
//     multi-document writes: catalog.CollLock(coll.ID, true).
//
// Compute the unique-value keys while holding the collection lock (the
// second phase of a two-phase acquisition through store.LockAfter or
// command.Ctx.Lock): the index list cannot change under the collection
// lock, while a list read before it can miss an index that DDL added in
// between.
//
// Transactions. Writes read through the Txn they are given. A plain
// store.Begin Txn reads committed state only, so two writes to the same
// collection in one plain Txn cannot see each other (a repeated _id or
// unique value inside the batch goes undetected). Commit after each
// document, or use store.BeginIndexed (MULTI/EXEC) for several documents
// in one batch. A write that fails leaves the Txn untouched. The first
// multikey write to a btree index also commits the index's multikey flag
// in a catalog write of its own (catalog.MarkMultikey), outside the Txn.
package docstore

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Store is the document write path over one store and catalog. It holds
// no per-write state and is safe for concurrent use.
type Store struct {
	st  *store.Store
	cat *catalog.Catalog
}

// New returns the write path for st and cat.
func New(st *store.Store, cat *catalog.Catalog) *Store {
	return &Store{st: st, cat: cat}
}

// Catalog returns the catalog the Store writes through.
func (d *Store) Catalog() *catalog.Catalog { return d.cat }

// IDKey returns ks(id), the encoded _id that follows coll_id in a doc key
// and ends every non-unique index key.
func IDKey(id bson.RawValue) ([]byte, error) {
	ks, err := keyenc.Encode(nil, id)
	if err != nil {
		return nil, encodeErr(err)
	}
	return ks, nil
}

// docID returns the _id of d and its encoding.
func docID(d bson.Raw) (bson.RawValue, []byte, error) {
	id, err := d.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, nil, errf(doc.CodeInvalidIDField, "document has no _id; pass it through doc.ValidateInsert first")
	}
	ks, err := IDKey(id)
	if err != nil {
		return bson.RawValue{}, nil, err
	}
	return id, ks, nil
}

// Get returns the document with _id id as r sees it. The returned bytes
// are a Go-owned copy.
func (d *Store) Get(r store.Reader, coll *catalog.Collection, id bson.RawValue) (bson.Raw, bool, error) {
	ks, err := IDKey(id)
	if err != nil {
		return nil, false, err
	}
	return d.GetKS(r, coll, ks)
}

// GetKS is Get for an already encoded _id.
func (d *Store) GetKS(r store.Reader, coll *catalog.Collection, ksID []byte) (bson.Raw, bool, error) {
	v, ok, err := r.Get(store.CFDoc, layout.DocKey(nil, coll.ID, ksID))
	if err != nil || !ok {
		return nil, false, err
	}
	return bson.Raw(v), true, nil
}

// LockKeys returns the LockDoc key of (coll, id) and, when row is not nil,
// one LockUniq key per unique-index entry of row. It does not include the
// collection lock (catalog.CollLock). For a replacement pass the old and
// the new document through UniqLockKeys as well, so a concurrent insert of
// a value being freed waits for the commit.
func (d *Store) LockKeys(coll *catalog.Collection, id bson.RawValue, row bson.Raw) []store.LockKey {
	ks, err := keyenc.Encode(nil, id)
	if err != nil {
		// The write fails on the same value; any key serialises it.
		ks = append([]byte{byte(id.Type)}, id.Value...)
	}
	keys := []store.LockKey{{Kind: store.LockDoc, NS: coll.ID, Key: ks}}
	if row != nil {
		keys = append(keys, d.UniqLockKeys(coll, row)...)
	}
	return keys
}

// UniqLockKeys returns one LockUniq key (NS idx_id, Key ks(values)) per
// unique-index entry of each document, from coll's current index list.
// Documents whose entries cannot be computed contribute nothing; the write
// itself reports the error.
func (d *Store) UniqLockKeys(coll *catalog.Collection, docs ...bson.Raw) []store.LockKey {
	var keys []store.LockKey
	for _, ix := range coll.Indexes() {
		if !ix.Unique {
			continue
		}
		for _, row := range docs {
			if row == nil {
				continue
			}
			ents, _, err := indexEntries(ix, row, nil)
			if err != nil {
				continue
			}
			for _, e := range ents {
				keys = append(keys, store.LockKey{Kind: store.LockUniq, NS: ix.ID, Key: e.uniq})
			}
		}
	}
	return keys
}

// planned is the entry set of one document across every index of a
// collection.
type planned struct {
	ix       *catalog.Index
	ents     []entry
	multikey bool
}

func planAll(indexes []*catalog.Index, d bson.Raw, ksID []byte) ([]planned, error) {
	out := make([]planned, 0, len(indexes))
	for _, ix := range indexes {
		ents, mk, err := indexEntries(ix, d, ksID)
		if err != nil {
			return nil, err
		}
		out = append(out, planned{ix: ix, ents: ents, multikey: mk})
	}
	return out, nil
}

// checkUnique fails with *DupKeyError when a unique entry's key is present
// in r for another document.
func checkUnique(r store.Reader, coll *catalog.Collection, ix *catalog.Index, e entry, ksID []byte) error {
	dup := false
	_, err := r.GetPinned(e.cf, e.key, func(v []byte) error {
		dup = !bytes.Equal(v, ksID)
		return nil
	})
	if err != nil {
		return err
	}
	if dup {
		return newDupKeyError(coll.NS, ix.Name, ix.Fields(), e.vals)
	}
	return nil
}

func countDelta(txn store.Txn, coll *catalog.Collection, n int64) {
	txn.Merge(store.CFDefault, layout.CountKey(coll.ID), layout.AppendCount(nil, n))
}

// Insert writes a new document with its index entries and a +1 count
// merge. row must start with _id, as doc.ValidateInsert and Update.Apply
// return it. A document whose _id exists fails with a *DupKeyError on
// index "_id_"; a unique violation, a Decimal128 in an indexed field,
// parallel arrays or an invalid geometry fail before anything is written.
// The caller holds the locks described in the package comment.
func (d *Store) Insert(txn store.Txn, coll *catalog.Collection, row bson.Raw) error {
	if coll.Dropped() {
		return catalog.ErrCollectionDropped
	}
	id, ksID, err := docID(row)
	if err != nil {
		return err
	}
	rowKey := layout.DocKey(nil, coll.ID, ksID)
	exists, err := txn.GetPinned(store.CFDoc, rowKey, func([]byte) error { return nil })
	if err != nil {
		return err
	}
	if exists {
		return newDupKeyError(coll.NS, IDIndexName, []string{"_id"}, []bson.RawValue{id})
	}
	plans, err := planAll(coll.Indexes(), row, ksID)
	if err != nil {
		return err
	}
	for _, p := range plans {
		if !p.ix.Unique {
			continue
		}
		for _, e := range p.ents {
			if err := checkUnique(txn, coll, p.ix, e, ksID); err != nil {
				return err
			}
		}
	}
	if err := d.markMultikey(coll, plans); err != nil {
		return err
	}
	txn.Put(store.CFDoc, rowKey, row)
	for _, p := range plans {
		for _, e := range p.ents {
			txn.Put(e.cf, e.key, e.val)
		}
	}
	countDelta(txn, coll, 1)
	return nil
}

// markMultikey persists the multikey flag of every index the document is
// the first multikey writer of, before anything goes into the Txn.
func (d *Store) markMultikey(coll *catalog.Collection, plans []planned) error {
	for _, p := range plans {
		if p.multikey {
			if err := d.cat.MarkMultikey(coll, p.ix); err != nil {
				return err
			}
		}
	}
	return nil
}

// Replace overwrites the stored document old with row, which must carry
// the same _id (code 66 otherwise). Index entries only old produces are
// deleted, entries only row produces are written after the unique checks,
// and shared entries are left alone. Identical bytes write nothing. The
// caller read old under its locks.
func (d *Store) Replace(txn store.Txn, coll *catalog.Collection, old, row bson.Raw) error {
	if coll.Dropped() {
		return catalog.ErrCollectionDropped
	}
	_, ksOld, err := docID(old)
	if err != nil {
		return err
	}
	_, ksNew, err := docID(row)
	if err != nil {
		return err
	}
	if !bytes.Equal(ksOld, ksNew) {
		return errf(66, "Performing an update on the path '_id' would modify the immutable field '_id'")
	}
	if bytes.Equal(old, row) {
		return nil
	}
	indexes := coll.Indexes()
	oldPlans, err := planAll(indexes, old, ksOld)
	if err != nil {
		return fmt.Errorf("docstore: index entries of the stored document: %w", err)
	}
	newPlans, err := planAll(indexes, row, ksNew)
	if err != nil {
		return err
	}
	type change struct {
		dels []entry
		puts []entry
	}
	changes := make([]change, len(indexes))
	for i := range indexes {
		oldSet := make(map[string][]byte, len(oldPlans[i].ents))
		for _, e := range oldPlans[i].ents {
			oldSet[string(e.key)] = e.val
		}
		newKeys := make(map[string]struct{}, len(newPlans[i].ents))
		for _, e := range newPlans[i].ents {
			newKeys[string(e.key)] = struct{}{}
			if v, ok := oldSet[string(e.key)]; ok && bytes.Equal(v, e.val) {
				continue
			}
			changes[i].puts = append(changes[i].puts, e)
		}
		for _, e := range oldPlans[i].ents {
			if _, ok := newKeys[string(e.key)]; !ok {
				changes[i].dels = append(changes[i].dels, e)
			}
		}
	}
	for i, ix := range indexes {
		if !ix.Unique {
			continue
		}
		for _, e := range changes[i].puts {
			if err := checkUnique(txn, coll, ix, e, ksNew); err != nil {
				return err
			}
		}
	}
	if err := d.markMultikey(coll, newPlans); err != nil {
		return err
	}
	txn.Put(store.CFDoc, layout.DocKey(nil, coll.ID, ksNew), row)
	for i := range indexes {
		for _, e := range changes[i].dels {
			txn.Delete(e.cf, e.key)
		}
		for _, e := range changes[i].puts {
			txn.Put(e.cf, e.key, e.val)
		}
	}
	return nil
}

// Update stores the result of applying an update to old. The caller runs
// doc's Update.Apply (which validates the result) and passes old and the
// new document; the write is a Replace.
func (d *Store) Update(txn store.Txn, coll *catalog.Collection, old, row bson.Raw) error {
	return d.Replace(txn, coll, old, row)
}

// Delete removes the stored document old, every index entry computed from
// it, and merges -1 into the count.
func (d *Store) Delete(txn store.Txn, coll *catalog.Collection, old bson.Raw) error {
	if coll.Dropped() {
		return catalog.ErrCollectionDropped
	}
	_, ksID, err := docID(old)
	if err != nil {
		return err
	}
	plans, err := planAll(coll.Indexes(), old, ksID)
	if err != nil {
		return fmt.Errorf("docstore: index entries of the stored document: %w", err)
	}
	txn.Delete(store.CFDoc, layout.DocKey(nil, coll.ID, ksID))
	for _, p := range plans {
		for _, e := range p.ents {
			txn.Delete(e.cf, e.key)
		}
	}
	countDelta(txn, coll, -1)
	return nil
}

// IsDupKey reports whether err is a unique-index violation.
func IsDupKey(err error) bool {
	var dup *DupKeyError
	return errors.As(err, &dup)
}
