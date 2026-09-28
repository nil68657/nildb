package cmddoc

import (
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func resp3(c *command.Ctx) bool { return c.Conn != nil && c.Conn.Proto() == 3 }

// docReply is one document: canonical Extended JSON, or raw BSON.
func docReply(d bson.Raw, raw bool) resp.Reply {
	if raw {
		return resp.Bulk(d)
	}
	return resp.Bulk(doc.FormatEJSON(d))
}

func valueReply(v bson.RawValue) resp.Reply { return resp.Bulk(doc.FormatEJSONValue(v)) }

func cursorReply(c *command.Ctx, id uint64, ns, batch string, docs []bson.Raw, raw bool) resp.Reply {
	return query.CursorReply(resp3(c), id, ns, batch, docs, raw)
}

type writeError struct {
	index int
	code  int
	msg   string
}

// insertReply is RESP3 {n, ids: [...], writeErrors: [{index, code,
// errmsg}]}, RESP2 [n, [ids], [[index, code, errmsg]...]].
func insertReply(c *command.Ctx, n int, ids []resp.Reply, errs []writeError) resp.Reply {
	we := make([]resp.Reply, len(errs))
	for i, e := range errs {
		if resp3(c) {
			we[i] = resp.Map(resp.Str("index"), resp.Int(int64(e.index)), resp.Str("code"), resp.Int(int64(e.code)), resp.Str("errmsg"), resp.Str(e.msg))
		} else {
			we[i] = resp.Array(resp.Int(int64(e.index)), resp.Int(int64(e.code)), resp.Str(e.msg))
		}
	}
	if resp3(c) {
		return resp.Map(resp.Str("n"), resp.Int(int64(n)), resp.Str("ids"), resp.Array(ids...), resp.Str("writeErrors"), resp.Array(we...))
	}
	return resp.Array(resp.Int(int64(n)), resp.Array(ids...), resp.Array(we...))
}

// updateReply is {n, nModified, upsertedId}: a map in RESP3, the same
// pairs flat in RESP2. upsertedId is null unless the write inserted.
func updateReply(n, modified int, upserted bson.RawValue) resp.Reply {
	id := resp.Null()
	if upserted.Type != 0 {
		id = valueReply(upserted)
	}
	return resp.Map(resp.Str("n"), resp.Int(int64(n)), resp.Str("nModified"), resp.Int(int64(modified)), resp.Str("upsertedId"), id)
}

func deleteReply(n int) resp.Reply {
	return resp.Map(resp.Str("n"), resp.Int(int64(n)))
}
