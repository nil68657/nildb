package cmddoc

import (
	"strings"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	errReplaceOps = &doc.Error{Code: doc.CodeFailedToParse, Msg: "replacement document must not contain update operators"}
	errMultiRepl  = &doc.Error{Code: doc.CodeFailedToParse, Msg: "multi update is not supported for replacement-style update"}
)

// insertArgs is DOC.INSERT as its KeysFunc parsed it.
type insertArgs struct {
	coll    *catalog.Collection
	docs    []insertDoc
	ordered bool
}

type insertDoc struct {
	row bson.Raw      // _id first, from doc.ValidateInsert
	id  bson.RawValue // aliases row
	err error         // a document ValidateInsert refused
}

// insertKeys parses DOC.INSERT ns doc [doc ...] [ORDERED yes|no], creates
// the collection when it does not exist, and locks the collection shared
// plus each document's _id stripe.
func (h *handlers) insertKeys(_ *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
	rest := args[2:]
	p := &insertArgs{ordered: true}
	if n := len(rest); n >= 2 && keyword(rest[n-2]) == "ORDERED" {
		switch keyword(rest[n-1]) {
		case "YES":
		case "NO":
			p.ordered = false
		default:
			return nil, nil, command.Fail(resp.ErrSyntax)
		}
		rest = rest[:n-2]
	}
	if len(rest) == 0 {
		return nil, nil, command.Fail(resp.ErrArity("doc.insert"))
	}
	for _, a := range rest {
		raw, err := doc.ParseEJSON(a)
		if err != nil {
			return nil, nil, fail(err)
		}
		row, id, err := doc.ValidateInsert(raw)
		p.docs = append(p.docs, insertDoc{row: row, id: id, err: err})
	}
	coll, _, err := h.cat.Ensure(string(args[1]))
	if err != nil {
		return nil, nil, fail(err)
	}
	p.coll = coll
	keys := []store.LockKey{catalog.CollLock(coll.ID, false)}
	for _, d := range p.docs {
		if d.err == nil {
			keys = append(keys, h.ds.LockKeys(coll, d.id, nil)...)
		}
	}
	return keys, p, nil
}

// insert writes the documents in order. With ORDERED yes (the default)
// the first failure stops the command; with ORDERED no every document is
// tried. Each failure becomes a writeError with MongoDB's code and
// message; the documents before it stay inserted, as in MongoDB. The
// unique-value stripes are taken here, under the collection lock, and
// held until the executor has committed.
func (h *handlers) insert(c *command.Ctx, _ [][]byte) resp.Reply {
	p := c.Parsed.(*insertArgs)
	var rows []bson.Raw
	for _, d := range p.docs {
		if d.err == nil {
			rows = append(rows, d.row)
		}
	}
	c.Lock(h.ds.UniqLockKeys(p.coll, rows...))
	w := newWriter(c, len(rows) > 1)
	var ids []resp.Reply
	var errs []writeError
	for i, d := range p.docs {
		err := d.err
		if err == nil {
			err = h.ds.Insert(w.txn, p.coll, d.row)
		}
		if err != nil {
			errs = append(errs, writeError{index: i, code: docstore.Code(err), msg: docstore.Message(err)})
			if p.ordered {
				break
			}
			continue
		}
		ids = append(ids, valueReply(d.id))
	}
	if err := w.commit(); err != nil {
		return errReply(err)
	}
	return insertReply(c, len(ids), ids, errs)
}

type writeOp uint8

const (
	opUpdate writeOp = iota
	opReplace
	opDelete
)

// writeArgs is DOC.UPDATE, DOC.REPLACE or DOC.DELETE as its KeysFunc
// parsed it.
type writeArgs struct {
	coll   *catalog.Collection // nil: the namespace does not exist
	filter bson.Raw
	upd    *doc.Update // nil for DOC.DELETE
	multi  bool
	upsert bool
	id     bson.RawValue // the _id the filter pins, when byID
	byID   bool
}

// writeKeys parses DOC.UPDATE ns filter update [MULTI] [UPSERT],
// DOC.REPLACE ns filter doc [UPSERT] and DOC.DELETE ns filter [MULTI]. A
// single-document write whose filter pins _id locks the collection shared
// and the document's stripe; every other write locks the collection
// exclusive, since the documents it touches are known only after reading.
// UPSERT creates the collection.
func (h *handlers) writeKeys(op writeOp) command.KeysFunc {
	return func(_ *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
		p := &writeArgs{}
		var err error
		if p.filter, err = doc.ParseEJSON(args[2]); err != nil {
			return nil, nil, fail(err)
		}
		flags := args[3:]
		if op != opDelete {
			u, err := doc.ParseEJSON(args[3])
			if err != nil {
				return nil, nil, fail(err)
			}
			if p.upd, err = doc.CompileUpdate(u); err != nil {
				return nil, nil, fail(err)
			}
			if op == opReplace && !p.upd.IsReplacement() {
				return nil, nil, fail(errReplaceOps)
			}
			flags = args[4:]
		}
		for _, f := range flags {
			switch kw := keyword(f); {
			case kw == "MULTI" && op != opReplace:
				p.multi = true
			case kw == "UPSERT" && op != opDelete:
				p.upsert = true
			default:
				return nil, nil, command.Fail(resp.ErrSyntax)
			}
		}
		if p.multi && p.upd != nil && p.upd.IsReplacement() {
			return nil, nil, fail(errMultiRepl)
		}
		ns := string(args[1])
		if p.upsert {
			if p.coll, _, err = h.cat.Ensure(ns); err != nil {
				return nil, nil, fail(err)
			}
		} else if coll, ok := h.cat.Resolve(ns); ok {
			p.coll = coll
		} else {
			return nil, p, nil
		}
		if id, ok := idEquality(p.filter); ok && !p.multi {
			if ks, err := docstore.IDKey(id); err == nil {
				p.id, p.byID = id, true
				return []store.LockKey{catalog.CollLock(p.coll.ID, false), {Kind: store.LockDoc, NS: p.coll.ID, Key: ks}}, p, nil
			}
		}
		return []store.LockKey{catalog.CollLock(p.coll.ID, true)}, p, nil
	}
}

// idEquality reports the _id a filter pins with a top-level {_id: v} or
// {_id: {$eq: v}}, for a value an _id can hold.
func idEquality(filter bson.Raw) (bson.RawValue, bool) {
	v, err := filter.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, false
	}
	if v.Type == bson.TypeEmbeddedDocument {
		elems, err := bson.Raw(v.Value).Elements()
		if err != nil || len(elems) == 0 {
			return bson.RawValue{}, false
		}
		if strings.HasPrefix(elems[0].Key(), "$") {
			if len(elems) != 1 || elems[0].Key() != "$eq" {
				return bson.RawValue{}, false
			}
			v = elems[0].Value()
		}
	}
	switch v.Type {
	case bson.TypeArray, bson.TypeRegex, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
		return bson.RawValue{}, false
	}
	if _, err := keyenc.Encode(nil, v); err != nil {
		return bson.RawValue{}, false
	}
	return v, true
}

// selection is the documents a write touches, as _ids, with the filter
// still to check for a write that read by _id without planning.
type selection struct {
	ids   []bson.RawValue
	check *doc.Matcher
}

var idProjection = mustProjection()

func mustProjection() bson.Raw {
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	if err != nil {
		panic(err)
	}
	return b
}

// selectIDs returns the _ids a write touches: the pinned one, or the first
// (MULTI: every) match of the filter's plan. Only _ids are kept, so a
// MULTI write over many documents holds little memory; the rows are read
// again as they are written.
func (h *handlers) selectIDs(c *command.Ctx, p *writeArgs) (selection, error) {
	if p.byID {
		m, err := query.Matcher(p.filter)
		return selection{ids: []bson.RawValue{p.id}, check: m}, err
	}
	limit := int64(1)
	if p.multi {
		limit = 0
	}
	plan, err := h.q.PlanFind(p.coll, query.Find{Filter: p.filter, Limit: limit, Project: idProjection}, options(c, c.Reader(), 0))
	if err != nil {
		return selection{}, err
	}
	run, err := plan.Start()
	if err != nil {
		return selection{}, err
	}
	docs, _, err := run.Next(-1, 0)
	run.Close()
	if err != nil {
		return selection{}, err
	}
	sel := selection{ids: make([]bson.RawValue, 0, len(docs))}
	for _, d := range docs {
		if id, err := d.LookupErr("_id"); err == nil {
			sel.ids = append(sel.ids, id)
		}
	}
	return sel, nil
}

// row reads one selected document; ok is false when it is gone or, for a
// write by _id, does not match the rest of the filter.
func (h *handlers) row(c *command.Ctx, p *writeArgs, sel selection, id bson.RawValue) (bson.Raw, bool, error) {
	row, ok, err := h.ds.Get(c.Reader(), p.coll, id)
	if err != nil || !ok {
		return nil, false, err
	}
	if sel.check != nil {
		if ok, err = sel.check.Match(row); err != nil || !ok {
			return nil, false, err
		}
	}
	return row, true, nil
}

// update runs DOC.UPDATE and DOC.REPLACE. n counts matched documents and
// nModified those whose bytes changed. With UPSERT and no match it
// inserts the document doc.Update.Apply builds from the filter's
// equality fields and reports its _id. Outside MULTI a failure writes
// nothing. A write by _id takes its unique-value stripes here, once, and
// the executor releases them after the commit.
func (h *handlers) update(c *command.Ctx, _ [][]byte) resp.Reply {
	p := c.Parsed.(*writeArgs)
	if p.coll == nil {
		return updateReply(0, 0, bson.RawValue{})
	}
	sel, err := h.selectIDs(c, p)
	if err != nil {
		return errReply(err)
	}
	w := newWriter(c, len(sel.ids) > 1)
	n, modified := 0, 0
	for _, id := range sel.ids {
		old, ok, err := h.row(c, p, sel, id)
		if err == nil && ok {
			var out bson.Raw
			var changed bool
			if out, changed, err = p.upd.Apply(old, nil, c.Now); err == nil && changed {
				if p.byID {
					c.Lock(h.ds.UniqLockKeys(p.coll, old, out))
				}
				err = h.ds.Update(w.txn, p.coll, old, out)
				modified++
			}
			n++
		}
		if err != nil {
			w.discard()
			return errReply(err)
		}
	}
	if n == 0 && p.upsert {
		out, _, err := p.upd.Apply(nil, p.filter, c.Now)
		if err == nil {
			if p.byID {
				c.Lock(h.ds.UniqLockKeys(p.coll, out))
			}
			err = h.ds.Insert(w.txn, p.coll, out)
		}
		if err == nil {
			err = w.commit()
		}
		if err != nil {
			w.discard()
			return errReply(err)
		}
		id, _ := out.LookupErr("_id")
		return updateReply(1, 0, id)
	}
	if err := w.commit(); err != nil {
		return errReply(err)
	}
	return updateReply(n, modified, bson.RawValue{})
}

// delete runs DOC.DELETE: the first match, or every match with MULTI.
func (h *handlers) delete(c *command.Ctx, _ [][]byte) resp.Reply {
	p := c.Parsed.(*writeArgs)
	if p.coll == nil {
		return deleteReply(0)
	}
	sel, err := h.selectIDs(c, p)
	if err != nil {
		return errReply(err)
	}
	w := newWriter(c, len(sel.ids) > 1)
	n := 0
	for _, id := range sel.ids {
		old, ok, err := h.row(c, p, sel, id)
		if err == nil && ok {
			err = h.ds.Delete(w.txn, p.coll, old)
			n++
		}
		if err != nil {
			w.discard()
			return errReply(err)
		}
	}
	if err := w.commit(); err != nil {
		return errReply(err)
	}
	return deleteReply(n)
}
