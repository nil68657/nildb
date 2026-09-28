package cmddoc

import (
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// findArgs is DOC.FIND ns filter [PROJECT p] [SORT s] [SKIP n] [LIMIT n]
// [BATCH n] [HINT name|keys] [FORMAT EJSON|BSON] [AT lease] [EXPLAIN].
type findArgs struct {
	ns      string
	f       query.Find
	batch   int
	raw     bool
	at      uint64
	hasAt   bool
	explain bool
	single  bool // a negative LIMIT: one batch, no cursor
}

func parseFind(args [][]byte) (*findArgs, resp.Reply) {
	a := &findArgs{ns: string(args[1]), batch: defaultBatch}
	var err error
	if a.f.Filter, err = doc.ParseEJSON(args[2]); err != nil {
		return nil, errReply(err)
	}
	for i := 3; i < len(args); i++ {
		kw := keyword(args[i])
		if kw == "EXPLAIN" {
			a.explain = true
			continue
		}
		if i+1 >= len(args) {
			return nil, resp.ErrSyntax
		}
		i++
		v := args[i]
		var rep resp.Reply
		switch kw {
		case "PROJECT":
			a.f.Project, err = doc.ParseEJSON(v)
		case "SORT":
			a.f.Sort, err = doc.ParseEJSON(v)
		case "SKIP":
			if a.f.Skip, rep = parseCount(v); rep == nil && a.f.Skip < 0 {
				rep = resp.Err("ERR SKIP must be a non-negative integer")
			}
		case "LIMIT":
			if a.f.Limit, rep = parseCount(v); a.f.Limit < 0 {
				a.f.Limit, a.single = -a.f.Limit, true
			}
		case "BATCH":
			var n int64
			if n, rep = parseCount(v); rep == nil && n < 0 {
				rep = resp.Err("ERR BATCH must be a non-negative integer")
			}
			a.batch = int(n)
		case "HINT":
			if len(v) > 0 && v[0] == '{' {
				a.f.HintKey, err = doc.ParseEJSON(v)
			} else {
				a.f.Hint = string(v)
			}
		case "FORMAT":
			switch keyword(v) {
			case "EJSON":
				a.raw = false
			case "BSON":
				a.raw = true
			default:
				rep = resp.ErrSyntax
			}
		case "AT":
			a.at, rep = parseCursorID(v)
			a.hasAt = true
		default:
			rep = resp.ErrSyntax
		}
		if rep != nil {
			return nil, rep
		}
		if err != nil {
			return nil, errReply(err)
		}
	}
	if a.single && a.f.Limit > 0 {
		a.batch = int(a.f.Limit)
	}
	return a, nil
}

func (h *handlers) find(c *command.Ctx, args [][]byte) resp.Reply {
	a, rep := parseFind(args)
	if rep != nil {
		return rep
	}
	r, lease, rep := reader(c, a.at, a.hasAt)
	if rep != nil {
		return rep
	}
	coll, _ := h.cat.Resolve(a.ns)
	plan, err := h.q.PlanFind(coll, a.f, options(c, r, 0))
	if err != nil {
		return errReply(err)
	}
	if a.explain {
		return plan.Explain()
	}
	return h.firstBatch(c, plan, a.ns, "find", a.batch, a.raw, lease, a.single)
}

// firstBatch runs a plan's first batch (every document inside EXEC, whose
// batch no cursor can outlive) and registers the rest as a cursor pinned
// to lease, or to a fresh lease when lease is nil.
func (h *handlers) firstBatch(c *command.Ctx, plan *query.Plan, ns, kind string, batch int, raw bool, lease *store.Snapshot, single bool) resp.Reply {
	run, err := plan.Start()
	if err != nil {
		return errReply(err)
	}
	n := batch
	if c.InExec {
		n = -1
	}
	docs, done, err := run.Next(n, maxBatchBytes)
	if err != nil {
		run.Close()
		return errReply(err)
	}
	var id uint64
	if !done && !single && !c.InExec {
		next := batch
		if next <= 0 {
			next = defaultBatch
		}
		cur, err := h.q.Cursors.Open(run, query.CursorOptions{NS: ns, Kind: kind, Batch: next, Lease: lease, RawBSON: raw})
		if err != nil {
			run.Close()
			return errReply(err)
		}
		id = cur.ID
	} else {
		run.Close()
	}
	return cursorReply(c, id, ns, "firstBatch", docs, raw)
}

// findOne is DOC.FINDONE ns filter [PROJECT p] [SORT s] and the other
// DOC.FIND options: the first matching document, or null.
func (h *handlers) findOne(c *command.Ctx, args [][]byte) resp.Reply {
	a, rep := parseFind(args)
	if rep != nil {
		return rep
	}
	r, _, rep := reader(c, a.at, a.hasAt)
	if rep != nil {
		return rep
	}
	a.f.Limit = 1
	coll, _ := h.cat.Resolve(a.ns)
	plan, err := h.q.PlanFind(coll, a.f, options(c, r, 0))
	if err != nil {
		return errReply(err)
	}
	if a.explain {
		return plan.Explain()
	}
	run, err := plan.Start()
	if err != nil {
		return errReply(err)
	}
	docs, _, err := run.Next(1, 0)
	run.Close()
	switch {
	case err != nil:
		return errReply(err)
	case len(docs) == 0:
		return resp.Null()
	}
	return docReply(docs[0], a.raw)
}

// get is DOC.GET ns id, one point read by _id.
func (h *handlers) get(c *command.Ctx, args [][]byte) resp.Reply {
	coll, ok := h.cat.Resolve(string(args[1]))
	if !ok {
		return resp.Null()
	}
	d, ok, err := h.ds.Get(c.Reader(), coll, parseID(args[2]))
	switch {
	case err != nil:
		return errReply(err)
	case !ok:
		return resp.Null()
	}
	return docReply(d, false)
}

// count is DOC.COUNT ns [filter]; without a filter it reads the
// collection's document counter.
func (h *handlers) count(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 3 {
		return resp.ErrSyntax
	}
	var filter bson.Raw
	if len(args) == 3 {
		var err error
		if filter, err = doc.ParseEJSON(args[2]); err != nil {
			return errReply(err)
		}
	}
	coll, _ := h.cat.Resolve(string(args[1]))
	n, err := h.q.Count(coll, filter, options(c, c.Reader(), 0))
	if err != nil {
		return errReply(err)
	}
	return resp.Int(n)
}

// distinct is DOC.DISTINCT ns field [filter]: the distinct values in BSON
// order, each as a canonical Extended JSON bulk string.
func (h *handlers) distinct(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 4 {
		return resp.ErrSyntax
	}
	var filter bson.Raw
	if len(args) == 4 {
		var err error
		if filter, err = doc.ParseEJSON(args[3]); err != nil {
			return errReply(err)
		}
	}
	coll, _ := h.cat.Resolve(string(args[1]))
	vals, err := h.q.Distinct(coll, string(args[2]), filter, options(c, c.Reader(), 0))
	if err != nil {
		return errReply(err)
	}
	items := make([]resp.Reply, len(vals))
	for i, v := range vals {
		items[i] = valueReply(v)
	}
	return resp.Array(items...)
}

// aggregate is DOC.AGGREGATE ns pipeline [BATCH n] [MAXMEM bytes] [FORMAT
// EJSON|BSON] [EXPLAIN], on the connection with OLTP reads and a MAXMEM of
// nildb.doc-agg-mem (32 MiB) by default.
func (h *handlers) aggregate(c *command.Ctx, args [][]byte) resp.Reply {
	pipeline, rep := parseArray(args[2])
	if rep != nil {
		return rep
	}
	batch, raw, explain := defaultBatch, false, false
	var maxMem int64
	for i := 3; i < len(args); i++ {
		kw := keyword(args[i])
		if kw == "EXPLAIN" {
			explain = true
			continue
		}
		if i+1 >= len(args) {
			return resp.ErrSyntax
		}
		i++
		switch kw {
		case "BATCH":
			n, rep := parseCount(args[i])
			if rep == nil && n < 0 {
				rep = resp.Err("ERR BATCH must be a non-negative integer")
			}
			if rep != nil {
				return rep
			}
			batch = int(n)
		case "MAXMEM":
			n, rep := parseCount(args[i])
			if rep == nil && n <= 0 {
				rep = resp.Err("ERR MAXMEM must be a positive integer")
			}
			if rep != nil {
				return rep
			}
			maxMem = n
		case "FORMAT":
			switch keyword(args[i]) {
			case "EJSON":
				raw = false
			case "BSON":
				raw = true
			default:
				return resp.ErrSyntax
			}
		default:
			return resp.ErrSyntax
		}
	}
	ns := string(args[1])
	coll, _ := h.cat.Resolve(ns)
	plan, err := h.q.PlanPipeline(coll, pipeline, options(c, c.Reader(), maxMem))
	if err != nil {
		return errReply(err)
	}
	if explain {
		return plan.Explain()
	}
	return h.firstBatch(c, plan, ns, "aggregate", batch, raw, nil, false)
}

// cursorRead is DOC.CURSOR READ id [COUNT n]: the next batch (the cursor's
// batch size by default), with id 0 once the cursor is exhausted.
func (h *handlers) cursorRead(c *command.Ctx, args [][]byte) resp.Reply {
	id, rep := parseCursorID(args[2])
	if rep != nil {
		return rep
	}
	n := 0
	if len(args) > 3 {
		if len(args) != 5 || keyword(args[3]) != "COUNT" {
			return resp.ErrSyntax
		}
		v, rep := parseCount(args[4])
		if rep == nil && v <= 0 {
			rep = resp.Err("ERR COUNT must be a positive integer")
		}
		if rep != nil {
			return rep
		}
		n = int(v)
	}
	cur, docs, done, err := h.q.Cursors.Read(id, n, maxBatchBytes)
	if err != nil {
		return errReply(err)
	}
	if done {
		id = 0
	}
	return cursorReply(c, id, cur.NS, "nextBatch", docs, cur.RawBSON)
}

func (h *handlers) cursorDel(_ *command.Ctx, args [][]byte) resp.Reply {
	id, rep := parseCursorID(args[2])
	if rep != nil {
		return rep
	}
	if h.q.Cursors.Del(id) {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *handlers) cursorList(_ *command.Ctx, _ [][]byte) resp.Reply {
	infos := h.q.Cursors.List()
	items := make([]resp.Reply, len(infos))
	for i, cur := range infos {
		items[i] = resp.Map(
			resp.Str("id"), resp.Int(int64(cur.ID)),
			resp.Str("ns"), resp.Str(cur.NS),
			resp.Str("kind"), resp.Str(cur.Kind),
			resp.Str("batch"), resp.Int(int64(cur.Batch)),
			resp.Str("lease"), resp.Int(int64(cur.Lease)),
			resp.Str("rows"), resp.Int(cur.Rows),
			resp.Str("created_ms"), resp.Int(cur.Created.UnixMilli()),
			resp.Str("idle_ms"), resp.Int(cur.Idle.Milliseconds()),
		)
	}
	return resp.Array(items...)
}

var cursorHelp = []string{
	"DOC.CURSOR <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"READ <id> [COUNT <n>]",
	"    Return the next batch of a cursor; id 0 in the reply means it is exhausted.",
	"DEL <id>",
	"    Close a cursor and release its snapshot lease.",
	"LIST",
	"    List open cursors.",
	"HELP",
	"    Print this help.",
}

func (h *handlers) cursorHelp(_ *command.Ctx, _ [][]byte) resp.Reply {
	items := make([]resp.Reply, len(cursorHelp))
	for i, l := range cursorHelp {
		items[i] = resp.Status(l)
	}
	return resp.Array(items...)
}
