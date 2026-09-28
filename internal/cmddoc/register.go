// Package cmddoc implements the DOC.* commands of architecture.md section
// 6 with scope-v1.md's cuts: collections, btree and 2dsphere indexes,
// inserts, finds with cursors, updates, replaces, deletes, counts,
// distinct values and aggregation, over internal/docstore (writes) and
// internal/query (reads). Documents, filters, updates and pipelines arrive
// as MongoDB Extended JSON strings; documents in replies are canonical
// Extended JSON bulk strings, or raw BSON with FORMAT BSON. RESP3 replies
// are maps; RESP2 replies are the arrays section 6 gives.
//
// Locking follows architecture.md section 4. A write's KeysFunc parses its
// arguments once (the handler receives them as Ctx.Parsed), creates the
// collection when the write may insert, and returns the collection lock:
// shared, with the document stripe, for a write aimed at one _id;
// exclusive for writes a filter selects and for DDL. Unique-value stripes
// are the second phase (Ctx.Lock), computed under the collection lock.
// Inside MULTI every write goes to the EXEC batch, which reads its own
// writes; outside it, a command that writes several documents uses one
// batch of its own that reads its own writes, so a repeated _id or unique
// value inside the command is caught.
package cmddoc

import (
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/query"
)

type handlers struct {
	cat *catalog.Catalog
	ds  *docstore.Store
	q   *query.Engine
}

// Register adds the DOC.* commands to r. cat and ds are the catalog and
// the write path over the server's store; q is the query engine, whose
// cursor table DOC.CURSOR serves for DOC.* and NIL.* cursors alike, so
// internal/analytics must be given the same engine. Close q after the
// server stops and before the store closes.
func Register(r *command.Registry, cat *catalog.Catalog, ds *docstore.Store, q *query.Engine) {
	h := &handlers{cat: cat, ds: ds, q: q}
	r.Register(h.specs()...)
}

func (h *handlers) specs() []command.Spec {
	const (
		group = "document"
		since = "1.0.0"
	)
	read := command.ReadOnly
	write := command.Write
	ddl := command.Write | command.NoMulti
	return []command.Spec{
		{Name: "doc.create", Arity: 2, Flags: ddl, Group: group, Since: since,
			Summary: "Creates a collection.", Run: h.create},
		{Name: "doc.drop", Arity: 2, Flags: ddl, Keys: h.collKeys(), Group: group, Since: since,
			Summary: "Drops a collection with its documents and indexes.", Run: h.drop},
		{Name: "doc.collections", Arity: -1, Flags: read, Group: group, Since: since,
			Summary: "Lists collections, optionally of one database prefix.", Run: h.collections},
		{Name: "doc.stats", Arity: 2, Flags: read, Group: group, Since: since,
			Summary: "Returns a collection's document count and approximate sizes.", Run: h.stats},
		{Name: "doc.createindex", Arity: -3, Flags: ddl, Keys: h.createIndexKeys, Group: group, Since: since,
			Summary: "Creates and builds a btree or 2dsphere index.", Run: h.createIndex},
		{Name: "doc.dropindex", Arity: 3, Flags: ddl, Keys: h.collKeys(), Group: group, Since: since,
			Summary: "Drops an index by name, or every index with *.", Run: h.dropIndex},
		{Name: "doc.indexes", Arity: 2, Flags: read, Group: group, Since: since,
			Summary: "Lists a collection's indexes.", Run: h.indexes},
		{Name: "doc.insert", Arity: -3, Flags: write, Keys: h.insertKeys, Group: group, Since: since,
			Summary: "Inserts documents.", Run: h.insert},
		{Name: "doc.get", Arity: 3, Flags: read | command.Fast, Group: group, Since: since,
			Summary: "Returns the document with an _id.", Run: h.get},
		{Name: "doc.find", Arity: -3, Flags: read, Group: group, Since: since,
			Summary: "Finds documents and returns a cursor.", Run: h.find},
		{Name: "doc.findone", Arity: -3, Flags: read, Group: group, Since: since,
			Summary: "Returns the first matching document.", Run: h.findOne},
		{Name: "doc.count", Arity: -2, Flags: read, Group: group, Since: since,
			Summary: "Counts matching documents.", Run: h.count},
		{Name: "doc.distinct", Arity: -3, Flags: read, Group: group, Since: since,
			Summary: "Returns the distinct values of a field.", Run: h.distinct},
		{Name: "doc.update", Arity: -4, Flags: write, Keys: h.writeKeys(opUpdate), Group: group, Since: since,
			Summary: "Updates one or every matching document.", Run: h.update},
		{Name: "doc.replace", Arity: -4, Flags: write, Keys: h.writeKeys(opReplace), Group: group, Since: since,
			Summary: "Replaces one matching document.", Run: h.update},
		{Name: "doc.delete", Arity: -3, Flags: write, Keys: h.writeKeys(opDelete), Group: group, Since: since,
			Summary: "Deletes one or every matching document.", Run: h.delete},
		{Name: "doc.aggregate", Arity: -3, Flags: read, Group: group, Since: since,
			Summary: "Runs an aggregation pipeline and returns a cursor.", Run: h.aggregate},
		{Name: "doc.cursor", Flags: command.NoMulti, Group: group, Since: since,
			Summary: "Reads, closes and lists cursors.", Subcommands: []command.Spec{
				{Name: "read", Arity: -3, Flags: read | command.NoMulti, Summary: "Returns the next batch of a cursor.", Run: h.cursorRead},
				{Name: "del", Arity: 3, Flags: read | command.NoMulti, Summary: "Closes a cursor.", Run: h.cursorDel},
				{Name: "list", Arity: 2, Flags: read | command.NoMulti, Summary: "Lists open cursors.", Run: h.cursorList},
				{Name: "help", Arity: 2, Flags: read | command.NoMulti, Summary: "Shows DOC.CURSOR subcommands.", Run: h.cursorHelp},
			}},
	}
}
