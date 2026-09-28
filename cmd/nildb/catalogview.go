package main

import (
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
)

// catalogView is the command.CatalogView that reg.SetCatalog installs.
// catalog.UniqueIndexes returns bare idx_ids; KeysFuncs also need each
// index's fields to build its LockUniq keys, so this reads them from the
// collection's index records.
type catalogView struct{ cat *catalog.Catalog }

var _ command.CatalogView = catalogView{}

func (v catalogView) ResolveNS(ns string) (uint32, bool) { return v.cat.ResolveNS(ns) }

func (v catalogView) UniqueIndexes(collID uint32) []command.UniqueIndex {
	coll, ok := v.cat.ByID(collID)
	if !ok {
		return nil
	}
	var out []command.UniqueIndex
	for _, id := range v.cat.UniqueIndexes(collID) {
		ix, ok := coll.IndexByID(id)
		if !ok {
			continue
		}
		fields := make([]string, len(ix.Keys))
		for i, k := range ix.Keys {
			fields[i] = k.Field
		}
		out = append(out, command.UniqueIndex{ID: ix.ID, Fields: fields})
	}
	return out
}
