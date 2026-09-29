package query

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// tenv is a real store with a catalog, the docstore write path and a
// query engine, in a temporary directory.
type tenv struct {
	t   testing.TB
	st  *store.Store
	cat *catalog.Catalog
	ds  *docstore.Store
	q   *Engine
}

func newEnv(t testing.TB) *tenv {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	st, err := store.Open(store.Config{
		Dir:                 t.TempDir(),
		Engine:              store.TestEngine(),
		BlockCacheBytes:     16 << 20,
		AnalyticsCacheBytes: 4 << 20,
		WriteBufferBytes:    32 << 20,
		Fsync:               store.FsyncNo,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	cat, err := catalog.Open(st)
	if err != nil {
		st.Close()
		t.Fatalf("catalog.Open: %v", err)
	}
	e := &tenv{t: t, st: st, cat: cat, ds: docstore.New(st, cat), q: New(st, nil)}
	t.Cleanup(func() {
		e.q.Close()
		st.Close()
	})
	return e
}

func (e *tenv) coll(ns string) *catalog.Collection {
	e.t.Helper()
	c, _, err := e.cat.Ensure(ns)
	if err != nil {
		e.t.Fatalf("Ensure(%s): %v", ns, err)
	}
	return c
}

func (e *tenv) index(c *catalog.Collection, spec catalog.Index) *catalog.Index {
	e.t.Helper()
	if spec.ExpireAfter == 0 {
		spec.ExpireAfter = catalog.NoExpire
	}
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	defer unlock()
	ix, _, err := e.ds.CreateIndex(c, spec)
	if err != nil {
		e.t.Fatalf("CreateIndex(%+v): %v", spec, err)
	}
	return ix
}

func btree(fields ...string) catalog.Index {
	ix := catalog.Index{Kind: catalog.Btree}
	for _, f := range fields {
		desc := strings.HasPrefix(f, "-")
		ix.Keys = append(ix.Keys, catalog.Key{Field: strings.TrimPrefix(f, "-"), Desc: desc})
	}
	return ix
}

// insert writes docs under the collection's exclusive lock, 500 to a batch,
// through an indexed transaction so the batch sees its own writes.
func (e *tenv) insert(c *catalog.Collection, docs ...bson.Raw) {
	e.t.Helper()
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	defer unlock()
	for start := 0; start < len(docs); start += 500 {
		txn := e.st.BeginIndexed()
		for _, d := range docs[start:min(start+500, len(docs))] {
			row, _, err := doc.ValidateInsert(d)
			if err != nil {
				e.t.Fatalf("ValidateInsert: %v", err)
			}
			if err := e.ds.Insert(txn, c, row); err != nil {
				e.t.Fatalf("Insert %s: %v", doc.FormatEJSON(row), err)
			}
		}
		if err := txn.Commit(); err != nil {
			e.t.Fatalf("Commit: %v", err)
		}
	}
}

// remove deletes the stored documents drop selects and returns how many.
func (e *tenv) remove(c *catalog.Collection, drop func(d bson.Raw) bool) int {
	e.t.Helper()
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	defer unlock()
	txn := e.st.Begin()
	n := 0
	for _, d := range e.all(c) {
		if drop(d) {
			if err := e.ds.Delete(txn, c, d); err != nil {
				e.t.Fatalf("Delete: %v", err)
			}
			n++
		}
	}
	if err := txn.Commit(); err != nil {
		e.t.Fatalf("Commit: %v", err)
	}
	return n
}

// all reads every stored document of c in _id order, bypassing the planner.
func (e *tenv) all(c *catalog.Collection) []bson.Raw {
	e.t.Helper()
	lo, hi := layout.IDBounds(c.ID)
	it := e.st.Iter(store.CFDoc, lo, hi, store.IterOpts{FillCache: true})
	defer it.Close()
	var out []bson.Raw
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out = append(out, bson.Raw(clone(it.Value())))
	}
	if err := it.Err(); err != nil {
		e.t.Fatalf("iterate: %v", err)
	}
	return out
}

// find plans and runs a find to completion.
func (e *tenv) find(c *catalog.Collection, f Find, o Options) (*Plan, []bson.Raw) {
	e.t.Helper()
	p, err := e.q.PlanFind(c, f, o)
	if err != nil {
		e.t.Fatalf("PlanFind(%s): %v", ejsonOf(f.Filter), err)
	}
	return p, e.drain(p)
}

func (e *tenv) drain(p *Plan) []bson.Raw {
	e.t.Helper()
	run, err := p.Start()
	if err != nil {
		e.t.Fatalf("Start: %v", err)
	}
	defer run.Close()
	docs, done, err := run.Next(-1, 0)
	if err != nil {
		e.t.Fatalf("Next: %v", err)
	}
	if !done {
		e.t.Fatalf("Next(-1) left rows behind")
	}
	return docs
}

// aggregate plans and runs a pipeline given as Extended JSON.
func (e *tenv) aggregate(c *catalog.Collection, pipeline string, o Options) (*Plan, []bson.Raw, error) {
	e.t.Helper()
	p, err := e.q.PlanPipeline(c, pipe(e.t, pipeline), o)
	if err != nil {
		return nil, nil, err
	}
	run, err := p.Start()
	if err != nil {
		return p, nil, err
	}
	defer run.Close()
	docs, _, err := run.Next(-1, 0)
	return p, docs, err
}

// ej parses one Extended JSON document.
func ej(t testing.TB, s string) bson.Raw {
	t.Helper()
	if s == "" {
		return nil
	}
	d, err := doc.ParseEJSON([]byte(s))
	if err != nil {
		t.Fatalf("ParseEJSON(%s): %v", s, err)
	}
	return d
}

// pipe parses a pipeline given as an Extended JSON array.
func pipe(t testing.TB, s string) bson.Raw {
	t.Helper()
	v := ej(t, `{"p":`+s+`}`).Lookup("p")
	if v.Type != bson.TypeArray {
		t.Fatalf("pipeline %s is not an array", s)
	}
	return v.Value
}

func mustMarshal(t testing.TB, d any) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return b
}

func ejsonOf(d bson.Raw) string {
	if len(d) == 0 {
		return "{}"
	}
	return string(doc.FormatEJSON(d))
}

func ejsonList(docs []bson.Raw) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = ejsonOf(d)
	}
	return out
}

// idKey returns ks(_id) of d, the planner's tie-break.
func idKey(d bson.Raw) []byte {
	id, _ := lookup(d, "_id")
	k, _ := keyenc.Encode(nil, id)
	return k
}

// brute evaluates a find over docs with the reference matcher and
// doc.SortKey, ties by _id: the oracle every planner result is held to.
func brute(t testing.TB, docs []bson.Raw, f Find) []bson.Raw {
	t.Helper()
	m, err := doc.Compile(f.Filter, geoHooks{near: true})
	if err != nil {
		t.Fatalf("Compile(%s): %v", ejsonOf(f.Filter), err)
	}
	var out []bson.Raw
	for _, d := range docs {
		ok, err := m.Match(d)
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		if ok {
			out = append(out, d)
		}
	}
	spec, err := doc.ParseSort(f.Sort)
	if err != nil {
		t.Fatalf("ParseSort: %v", err)
	}
	if len(spec) > 0 {
		slices.SortStableFunc(out, func(a, b bson.Raw) int {
			ka := append(doc.SortKey(nil, a, spec), idKey(a)...)
			kb := append(doc.SortKey(nil, b, spec), idKey(b)...)
			return bytes.Compare(ka, kb)
		})
	}
	if f.Skip > 0 {
		out = out[min(int(f.Skip), len(out)):]
	}
	if f.Limit > 0 && int(f.Limit) < len(out) {
		out = out[:f.Limit]
	}
	if !emptyDoc(f.Project) {
		p, err := doc.CompileProjection(f.Project)
		if err != nil {
			t.Fatalf("CompileProjection: %v", err)
		}
		for i, d := range out {
			if out[i], err = p.Apply(d); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
	}
	return out
}

// sortKeys returns each document's sort key without the _id tie-break.
func sortKeys(docs []bson.Raw, spec doc.SortSpec) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = string(doc.SortKey(nil, d, spec))
	}
	return out
}

func sortedEJSON(docs []bson.Raw) []string {
	out := ejsonList(docs)
	slices.Sort(out)
	return out
}
