package cmddoc

import (
	"errors"
	"fmt"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var errTTL = resp.Err("ERR TTL indexes are not supported in v1")

func nsNotFound(ns string) resp.Reply { return resp.Err("ERR ns does not exist: " + ns) }

// collKeys locks an existing collection exclusively, for DOC.DROP and
// DOC.DROPINDEX; the handler receives the collection as Ctx.Parsed.
func (h *handlers) collKeys() command.KeysFunc {
	return func(_ *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
		coll, ok := h.cat.Resolve(string(args[1]))
		if !ok {
			return nil, nil, nil
		}
		return []store.LockKey{catalog.CollLock(coll.ID, true)}, coll, nil
	}
}

func (h *handlers) create(_ *command.Ctx, args [][]byte) resp.Reply {
	if _, err := h.cat.Create(string(args[1])); err != nil {
		return errReply(err)
	}
	return resp.OK()
}

// drop is DOC.DROP ns: 1 when it dropped the collection, 0 when there was
// none.
func (h *handlers) drop(c *command.Ctx, _ [][]byte) resp.Reply {
	coll, _ := c.Parsed.(*catalog.Collection)
	if coll == nil {
		return resp.Int(0)
	}
	if err := h.ds.DropCollection(coll); err != nil {
		if doc.Code(err) == catalog.CodeNamespaceNotFound {
			return resp.Int(0)
		}
		return errReply(err)
	}
	return resp.Int(1)
}

// dropIndex is DOC.DROPINDEX ns name; the name * drops every index but
// _id_.
func (h *handlers) dropIndex(c *command.Ctx, args [][]byte) resp.Reply {
	coll, _ := c.Parsed.(*catalog.Collection)
	if coll == nil {
		return resp.Err("ERR ns not found")
	}
	name := string(args[2])
	if name != "*" {
		if err := h.ds.DropIndex(coll, name); err != nil {
			return errReply(err)
		}
		return resp.OK()
	}
	for _, ix := range coll.Indexes() {
		if err := h.ds.DropIndex(coll, ix.Name); err != nil {
			return errReply(err)
		}
	}
	return resp.OK()
}

// indexArgs is DOC.CREATEINDEX as its KeysFunc parsed it.
type indexArgs struct {
	coll *catalog.Collection
	spec catalog.Index
}

// createIndexKeys parses DOC.CREATEINDEX ns keyspec [NAME n] [UNIQUE]
// [SPARSE] [EXPIREAFTERSECONDS s] [S2 coarsest finest maxcells], creates
// the collection when needed, and locks it exclusively for the
// synchronous build.
func (h *handlers) createIndexKeys(_ *command.KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
	keys, err := doc.ParseEJSON(args[2])
	if err != nil {
		return nil, nil, fail(err)
	}
	spec, rep := parseKeySpec(keys)
	if rep != nil {
		return nil, nil, command.Fail(rep)
	}
	hasS2 := false
	for i := 3; i < len(args); i++ {
		switch keyword(args[i]) {
		case "UNIQUE":
			spec.Unique = true
		case "SPARSE":
			spec.Sparse = true
		case "NAME":
			if i+1 >= len(args) {
				return nil, nil, command.Fail(resp.ErrSyntax)
			}
			i++
			spec.Name = string(args[i])
		case "EXPIREAFTERSECONDS":
			return nil, nil, command.Fail(errTTL)
		case "S2":
			if i+3 >= len(args) {
				return nil, nil, command.Fail(resp.ErrSyntax)
			}
			var p [3]int64
			for j := range p {
				v, rep := parseCount(args[i+1+j])
				if rep != nil {
					return nil, nil, command.Fail(rep)
				}
				p[j] = v
			}
			i += 3
			spec.S2 = geo.Params{Coarsest: int(p[0]), Finest: int(p[1]), MaxCells: int(p[2])}
			hasS2 = true
		default:
			return nil, nil, command.Fail(resp.ErrSyntax)
		}
	}
	if hasS2 && spec.Kind != catalog.TwoDSphere {
		return nil, nil, command.Fail(resp.Err("ERR S2 applies to 2dsphere indexes only"))
	}
	coll, _, err := h.cat.Ensure(string(args[1]))
	if err != nil {
		return nil, nil, fail(err)
	}
	return []store.LockKey{catalog.CollLock(coll.ID, true)}, &indexArgs{coll: coll, spec: spec}, nil
}

// parseKeySpec reads {"f": 1, "g": -1} (btree) or {"loc": "2dsphere"}.
// "columnar" fields are refused, as scope-v1.md defers columnar indexes.
func parseKeySpec(keys bson.Raw) (catalog.Index, resp.Reply) {
	spec := catalog.Index{Kind: catalog.Btree, ExpireAfter: catalog.NoExpire}
	elems, err := keys.Elements()
	if err != nil {
		return spec, errReply(err)
	}
	for _, e := range elems {
		field, v := e.Key(), e.Value()
		bad := resp.Err(fmt.Sprintf(`ERR bad index key pattern: field '%s' must be 1, -1 or "2dsphere"`, field))
		desc := false
		switch v.Type {
		case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
			f, _ := v.AsFloat64OK()
			if f != 1 && f != -1 {
				return spec, bad
			}
			desc = f == -1
		case bson.TypeString:
			switch v.StringValue() {
			case "2dsphere":
				spec.Kind = catalog.TwoDSphere
			case "columnar":
				return spec, errReply(catalog.ErrColumnar)
			default:
				return spec, bad
			}
		default:
			return spec, bad
		}
		spec.Keys = append(spec.Keys, catalog.Key{Field: field, Desc: desc})
	}
	return spec, nil
}

// createIndex builds the index under the collection's exclusive lock and
// replies its name; an identical existing index is a success.
func (h *handlers) createIndex(c *command.Ctx, _ [][]byte) resp.Reply {
	p := c.Parsed.(*indexArgs)
	ix, _, err := h.ds.CreateIndex(p.coll, p.spec)
	switch {
	case errors.Is(err, catalog.ErrIDIndex):
		return resp.Str("_id_")
	case err != nil:
		return errReply(err)
	}
	return resp.Str(ix.Name)
}

// indexes is DOC.INDEXES ns: one map per index, the implicit _id_ first.
func (h *handlers) indexes(_ *command.Ctx, args [][]byte) resp.Reply {
	ns := string(args[1])
	coll, ok := h.cat.Resolve(ns)
	if !ok {
		return nsNotFound(ns)
	}
	items := []resp.Reply{resp.Map(
		resp.Str("name"), resp.Str("_id_"),
		resp.Str("key"), resp.Str(`{"_id":1}`),
		resp.Str("kind"), resp.Str("btree"),
		resp.Str("unique"), resp.Bool(true),
		resp.Str("sparse"), resp.Bool(false),
		resp.Str("multikey"), resp.Bool(false),
		resp.Str("state"), resp.Str("ready"),
	)}
	for _, ix := range coll.Indexes() {
		key, err := bson.MarshalExtJSON(ix.KeyDoc(), false, false)
		if err != nil {
			return errReply(err)
		}
		fields := []resp.Reply{
			resp.Str("name"), resp.Str(ix.Name),
			resp.Str("key"), resp.Str(string(key)),
			resp.Str("kind"), resp.Str(ix.Kind.String()),
			resp.Str("unique"), resp.Bool(ix.Unique),
			resp.Str("sparse"), resp.Bool(ix.Sparse),
			resp.Str("multikey"), resp.Bool(ix.Multikey()),
			resp.Str("state"), resp.Str(ix.State.String()),
		}
		if ix.Kind == catalog.TwoDSphere {
			fields = append(fields,
				resp.Str("coarsest"), resp.Int(int64(ix.S2.Coarsest)),
				resp.Str("finest"), resp.Int(int64(ix.S2.Finest)),
				resp.Str("maxCells"), resp.Int(int64(ix.S2.MaxCells)))
		}
		items = append(items, resp.Map(fields...))
	}
	return resp.Array(items...)
}

// collections is DOC.COLLECTIONS [database]: every namespace, or those of
// one database prefix, sorted.
func (h *handlers) collections(_ *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 2 {
		return resp.ErrSyntax
	}
	prefix := ""
	if len(args) == 2 {
		prefix = string(args[1]) + "."
	}
	names := h.cat.List(prefix)
	items := make([]resp.Reply, len(names))
	for i, n := range names {
		items[i] = resp.Str(n)
	}
	return resp.Array(items...)
}

// stats is DOC.STATS ns: the document count from n/<coll_id>, the
// approximate on-disk bytes of the rows and of each index (RocksDB's
// ApproximateSizes, which does not count data still in memtables), and the
// index count including _id_.
func (h *handlers) stats(c *command.Ctx, args [][]byte) resp.Reply {
	ns := string(args[1])
	coll, ok := h.cat.Resolve(ns)
	if !ok {
		return nsNotFound(ns)
	}
	count, err := catalog.CountAt(c.Reader(), coll)
	if err != nil {
		return errReply(err)
	}
	size := func(cf store.CF, id uint32) int64 {
		lo, hi := layout.IDBounds(id)
		sizes, err := c.Store.ApproxSizes(cf, [][2][]byte{{lo, hi}})
		if err != nil || len(sizes) != 1 {
			return 0
		}
		return int64(sizes[0])
	}
	var ixSizes []resp.Reply
	for _, ix := range coll.Indexes() {
		cf := store.CFIdx
		if ix.Kind == catalog.TwoDSphere {
			cf = store.CFGeo
		}
		ixSizes = append(ixSizes, resp.Str(ix.Name), resp.Int(size(cf, ix.ID)))
	}
	return resp.Map(
		resp.Str("ns"), resp.Str(ns),
		resp.Str("count"), resp.Int(count),
		resp.Str("size"), resp.Int(size(store.CFDoc, coll.ID)),
		resp.Str("nindexes"), resp.Int(int64(len(coll.Indexes())+1)),
		resp.Str("indexSizes"), resp.Map(ixSizes...),
		resp.Str("created_ms"), resp.Int(coll.Created),
	)
}
