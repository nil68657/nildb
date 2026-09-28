package cmddoc

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	// defaultBatch is MongoDB's first-batch size for find and aggregate.
	defaultBatch = 101
	// maxBatchBytes caps one batch at MongoDB's 16 MiB message size.
	maxBatchBytes = 16 << 20
)

var (
	errLeaseExpired = resp.Err("ERR snapshot lease expired")
	errNotArray     = resp.Err("ERR the pipeline must be a JSON array of stage documents")
)

// errReply turns an error into its reply. doc, catalog, docstore and query
// errors carry their own "ERR " (or "E11000") prefix.
func errReply(err error) resp.Reply {
	var de *doc.Error
	var dup *docstore.DupKeyError
	if errors.As(err, &de) || errors.As(err, &dup) {
		return resp.Err(err.Error())
	}
	return resp.Err("ERR " + err.Error())
}

// fail wraps an error for a KeysFunc, which must not add a second prefix.
func fail(err error) error { return command.Fail(errReply(err)) }

// parseArray parses an Extended JSON array, such as a pipeline.
func parseArray(arg []byte) (bson.Raw, resp.Reply) {
	wrapped := make([]byte, 0, len(arg)+6)
	wrapped = append(append(append(wrapped, `{"p":`...), arg...), '}')
	d, err := doc.ParseEJSON(wrapped)
	if err != nil {
		return nil, errReply(err)
	}
	v, err := d.LookupErr("p")
	if err != nil || v.Type != bson.TypeArray {
		return nil, errNotArray
	}
	return v.Value, nil
}

// parseID reads an _id argument: any Extended JSON value (5, "a",
// {"$oid": "..."}), or, when the argument is not JSON, the string itself.
func parseID(arg []byte) bson.RawValue {
	wrapped := make([]byte, 0, len(arg)+8)
	wrapped = append(append(append(wrapped, `{"_id":`...), arg...), '}')
	if d, err := doc.ParseEJSON(wrapped); err == nil {
		if v, err := d.LookupErr("_id"); err == nil {
			return v
		}
	}
	v := binary.LittleEndian.AppendUint32(nil, uint32(len(arg)+1))
	v = append(append(v, arg...), 0)
	return bson.RawValue{Type: bson.TypeString, Value: v}
}

func parseCount(arg []byte) (int64, resp.Reply) {
	n, ok := resp.ParseInt(arg)
	if !ok {
		return 0, resp.ErrNotInteger
	}
	return n, nil
}

func parseCursorID(arg []byte) (uint64, resp.Reply) {
	n, ok := resp.ParseInt(arg)
	if !ok || n < 0 {
		return 0, resp.Err("ERR invalid cursor id")
	}
	return uint64(n), nil
}

func keyword(b []byte) string { return strings.ToUpper(string(b)) }

// options returns the query options of an OLTP read through r.
func options(c *command.Ctx, r store.Reader, maxMem int64) query.Options {
	k := c.Cfg.Knobs()
	if maxMem <= 0 {
		maxMem = k.DocAggMem
	}
	return query.Options{Mode: query.OLTP, Reader: r, MaxMem: maxMem, GeoMaxCells: k.GeoQueryMaxCells}
}

// reader returns what a DOC read goes through: the lease an AT option
// names, or the command's reader (the EXEC batch inside MULTI).
func reader(c *command.Ctx, at uint64, hasAt bool) (store.Reader, *store.Snapshot, resp.Reply) {
	if !hasAt {
		return c.Reader(), nil, nil
	}
	lease, ok := c.Store.Lookup(at)
	if !ok {
		return nil, nil, errLeaseExpired
	}
	return c.Store.At(lease), lease, nil
}

// writer is the batch a DOC write goes to: the command's own Txn for one
// document or inside EXEC (whose batch already reads its own writes), and
// otherwise a batch of the command's own that reads its own writes, so a
// repeated _id or unique value within the command is caught. The command
// commits its own batch; the executor commits Ctx.Txn.
type writer struct {
	txn store.Txn
	own bool
}

func newWriter(c *command.Ctx, many bool) *writer {
	if c.InExec || !many {
		return &writer{txn: c.Txn}
	}
	return &writer{txn: c.Store.BeginIndexed(), own: true}
}

func (w *writer) commit() error {
	if w.own {
		return w.txn.Commit()
	}
	return nil
}

func (w *writer) discard() {
	if w.own {
		w.txn.Discard()
	}
}
