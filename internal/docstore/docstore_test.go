package docstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/golang/geo/s2"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type env struct {
	st  *store.Store
	cat *catalog.Catalog
	ds  *Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	st, err := store.Open(store.Config{
		Dir:                 t.TempDir(),
		BlockCacheBytes:     8 << 20,
		AnalyticsCacheBytes: 4 << 20,
		WriteBufferBytes:    32 << 20,
		Fsync:               store.FsyncNo,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cat, err := catalog.Open(st)
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	return &env{st: st, cat: cat, ds: New(st, cat)}
}

func (e *env) coll(t *testing.T, ns string) *catalog.Collection {
	t.Helper()
	c, _, err := e.cat.Ensure(ns)
	if err != nil {
		t.Fatalf("Ensure(%s): %v", ns, err)
	}
	return c
}

// index creates and builds an index under the collection's exclusive lock.
func (e *env) index(t *testing.T, c *catalog.Collection, spec catalog.Index) *catalog.Index {
	t.Helper()
	if spec.ExpireAfter == 0 {
		spec.ExpireAfter = catalog.NoExpire
	}
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	defer unlock()
	ix, created, err := e.ds.CreateIndex(c, spec)
	if err != nil || !created {
		t.Fatalf("CreateIndex(%+v) = %v, %v", spec, created, err)
	}
	return ix
}

func btree(keys ...catalog.Key) catalog.Index {
	return catalog.Index{Kind: catalog.Btree, Keys: keys}
}

func asc(f string) catalog.Key  { return catalog.Key{Field: f} }
func desc(f string) catalog.Key { return catalog.Key{Field: f, Desc: true} }

func parse(t testing.TB, ejson string) bson.Raw {
	t.Helper()
	raw, err := doc.ParseEJSON([]byte(ejson))
	if err != nil {
		t.Fatalf("ParseEJSON(%s): %v", ejson, err)
	}
	withID, _, err := doc.ValidateInsert(raw)
	if err != nil {
		t.Fatalf("ValidateInsert(%s): %v", ejson, err)
	}
	return withID
}

// lockWrite takes the point-write locks in two phases: the collection
// (shared) and document stripe, then the unique-value stripes computed
// under the collection lock.
func (e *env) lockWrite(c *catalog.Collection, id bson.RawValue, docs ...bson.Raw) func() {
	first := []store.LockKey{catalog.CollLock(c.ID, false)}
	first = append(first, e.ds.LockKeys(c, id, nil)...)
	u1 := e.st.Lock(first)
	u2 := e.st.LockAfter(first, e.ds.UniqLockKeys(c, docs...))
	return func() { u2(); u1() }
}

func (e *env) insertErr(c *catalog.Collection, d bson.Raw) error {
	id := d.Lookup("_id")
	unlock := e.lockWrite(c, id, d)
	defer unlock()
	txn := e.st.Begin()
	if err := e.ds.Insert(txn, c, d); err != nil {
		if txn.Len() != 0 {
			return fmt.Errorf("failed Insert left %d writes in the Txn: %w", txn.Len(), err)
		}
		txn.Discard()
		return err
	}
	return txn.Commit()
}

func (e *env) insert(t testing.TB, c *catalog.Collection, ejson string) bson.Raw {
	t.Helper()
	d := parse(t, ejson)
	if err := e.insertErr(c, d); err != nil {
		t.Fatalf("Insert(%s): %v", ejson, err)
	}
	return d
}

func (e *env) replace(c *catalog.Collection, old, nw bson.Raw) error {
	unlock := e.lockWrite(c, old.Lookup("_id"), old, nw)
	defer unlock()
	txn := e.st.Begin()
	if err := e.ds.Replace(txn, c, old, nw); err != nil {
		txn.Discard()
		return err
	}
	return txn.Commit()
}

func (e *env) delete(t *testing.T, c *catalog.Collection, old bson.Raw) {
	t.Helper()
	unlock := e.lockWrite(c, old.Lookup("_id"), old)
	defer unlock()
	txn := e.st.Begin()
	if err := e.ds.Delete(txn, c, old); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}

// kv is one stored entry with the 4-byte id prefix removed.
type kv struct{ key, val string }

// scan returns every entry of id's keyspace in cf, prefix stripped.
func (e *env) scan(t testing.TB, cf store.CF, id uint32) []kv {
	t.Helper()
	lo, hi := layout.IDBounds(id)
	it := e.st.Iter(cf, lo, hi, store.IterOpts{TotalOrderSeek: true})
	defer it.Close()
	var out []kv
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out = append(out, kv{string(it.Key()[4:]), string(it.Value())})
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func enc(t testing.TB, ejsonValue string, descending bool) []byte {
	t.Helper()
	raw, err := doc.ParseEJSON([]byte(`{"v":` + ejsonValue + `}`))
	if err != nil {
		t.Fatal(err)
	}
	v := raw.Lookup("v")
	var b []byte
	if descending {
		b, err = keyenc.EncodeDesc(nil, v)
	} else {
		b, err = keyenc.Encode(nil, v)
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func cat(parts ...[]byte) string { return string(bytes.Join(parts, nil)) }

func sortKV(in []kv) []kv {
	out := slices.Clone(in)
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func wantEntries(t *testing.T, label string, got, want []kv) {
	t.Helper()
	got, want = sortKV(got), sortKV(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s entries:\n got  %q\n want %q", label, got, want)
	}
}

func wantErrText(t *testing.T, err error, code int, text string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %q", text)
	}
	if Code(err) != code || err.Error() != text {
		t.Fatalf("error %d %q, want %d %q", Code(err), err.Error(), code, text)
	}
}

func count(t *testing.T, e *env, c *catalog.Collection) int64 {
	t.Helper()
	n, err := e.cat.Count(c)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBtreeEntries(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	single := e.index(t, c, btree(asc("a")))
	compound := e.index(t, c, btree(asc("b"), desc("c")))
	multi := e.index(t, c, btree(asc("tags")))
	uniq := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("email")}, Unique: true})
	nested := e.index(t, c, btree(asc("sub.x"), desc("sub.y")))

	e.insert(t, c, `{"_id": 1, "a": 5, "b": "x", "c": 2.5, "tags": ["p", "q", "p"], "email": "e@x", "sub": {"x": true, "y": null}}`)
	ks := enc(t, `1`, false)

	wantEntries(t, "single", e.scan(t, store.CFIdx, single.ID), []kv{{cat(enc(t, `5`, false), ks), ""}})
	wantEntries(t, "compound desc", e.scan(t, store.CFIdx, compound.ID), []kv{{cat(enc(t, `"x"`, false), enc(t, `2.5`, true), ks), ""}})
	wantEntries(t, "multikey", e.scan(t, store.CFIdx, multi.ID), []kv{
		{cat(enc(t, `"p"`, false), ks), ""},
		{cat(enc(t, `"q"`, false), ks), ""},
	})
	wantEntries(t, "unique", e.scan(t, store.CFIdx, uniq.ID), []kv{{string(enc(t, `"e@x"`, false)), string(ks)}})
	wantEntries(t, "nested", e.scan(t, store.CFIdx, nested.ID), []kv{{cat(enc(t, `true`, false), enc(t, `null`, true), ks), ""}})

	for _, ix := range []*catalog.Index{single, compound, uniq, nested} {
		if cur, _ := c.IndexByID(ix.ID); cur.Multikey() {
			t.Errorf("%s marked multikey", ix.Name)
		}
	}
	if cur, _ := c.IndexByID(multi.ID); !cur.Multikey() {
		t.Error("tags_1 not marked multikey")
	}
	got, ok, err := e.ds.Get(e.st, c, bson.RawValue{Type: bson.TypeInt32, Value: []byte{1, 0, 0, 0}})
	if err != nil || !ok || got.Lookup("email").StringValue() != "e@x" {
		t.Fatalf("Get = %v, %v, %v", got, ok, err)
	}
	// 1, int64(1) and 1.0 are the same _id.
	if _, ok, _ := e.ds.Get(e.st, c, bson.RawValue{Type: bson.TypeDouble, Value: []byte{0, 0, 0, 0, 0, 0, 0xf0, 0x3f}}); !ok {
		t.Fatal("Get(1.0) missed the document stored with _id 1")
	}
	if n := count(t, e, c); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

func TestReplaceMovesEntries(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	a := e.index(t, c, btree(asc("a")))
	tags := e.index(t, c, btree(asc("tags")))
	g := e.index(t, c, catalog.Index{Kind: catalog.TwoDSphere, Keys: []catalog.Key{asc("loc")}})
	u := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("u")}, Unique: true})

	old := e.insert(t, c, `{"_id": "k", "a": 1, "tags": ["x", "y"], "loc": {"type": "Point", "coordinates": [10, 20]}, "u": 1}`)
	other := e.insert(t, c, `{"_id": "other", "a": 7, "u": 2}`)
	ks := enc(t, `"k"`, false)
	nw := parse(t, `{"_id": "k", "a": 2, "tags": ["y", "z"], "loc": {"type": "Point", "coordinates": [11, 21]}, "u": 3}`)
	if err := e.replace(c, old, nw); err != nil {
		t.Fatal(err)
	}
	ksOther := enc(t, `"other"`, false)
	wantEntries(t, "a", e.scan(t, store.CFIdx, a.ID), []kv{
		{cat(enc(t, `2`, false), ks), ""},
		{cat(enc(t, `7`, false), ksOther), ""},
	})
	wantEntries(t, "tags", e.scan(t, store.CFIdx, tags.ID), []kv{
		{cat(enc(t, `"y"`, false), ks), ""},
		{cat(enc(t, `"z"`, false), ks), ""},
		{cat(enc(t, `null`, false), ksOther), ""},
	})
	wantEntries(t, "unique", e.scan(t, store.CFIdx, u.ID), []kv{
		{string(enc(t, `2`, false)), string(ksOther)},
		{string(enc(t, `3`, false)), string(ks)},
	})
	geoEnts := e.scan(t, store.CFGeo, g.ID)
	if len(geoEnts) != 1 {
		t.Fatalf("geo entries after the move: %q", geoEnts)
	}
	_, ll, err := geo.DecodeValue([]byte(geoEnts[0].val))
	if err != nil || ll.Lat.Degrees() < 20.99 || ll.Lng.Degrees() < 10.99 {
		t.Fatalf("geo value %v, %v; want the new position", ll, err)
	}

	// Taking another document's unique value fails and writes nothing.
	steal := parse(t, `{"_id": "k", "a": 2, "u": 2}`)
	wantErrText(t, e.replace(c, nw, steal), CodeDuplicateKey,
		`E11000 duplicate key error collection: test.c index: u_1 dup key: { u: 2 }`)
	if got, _, _ := e.ds.Get(e.st, c, nw.Lookup("_id")); !bytes.Equal(got, nw) {
		t.Fatal("a failed Replace changed the row")
	}
	// Changing _id is refused.
	moved := parse(t, `{"_id": "k2", "a": 2}`)
	err = e.replace(c, nw, moved)
	if Code(err) != 66 {
		t.Fatalf("replace with a new _id: %v", err)
	}
	if n := count(t, e, c); n != 2 {
		t.Fatalf("count = %d after replaces", n)
	}
	_ = other
}

func TestDeleteRemovesEverything(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	a := e.index(t, c, btree(asc("a"), desc("b")))
	u := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("u")}, Unique: true})
	g := e.index(t, c, catalog.Index{Kind: catalog.TwoDSphere, Keys: []catalog.Key{asc("loc")}})
	var docs []bson.Raw
	for i := range 20 {
		docs = append(docs, e.insert(t, c, fmt.Sprintf(`{"_id": %d, "a": [%d, %d], "b": "s%d", "u": %d, "loc": {"type": "Polygon", "coordinates": [[[0, 0], [%d, 0], [%d, 1], [0, 1], [0, 0]]]}}`,
			i, i, i+1, i, i, i%3+1, i%3+1)))
	}
	if n := count(t, e, c); n != 20 {
		t.Fatalf("count = %d", n)
	}
	for _, d := range docs {
		e.delete(t, c, d)
	}
	for _, r := range []struct {
		cf store.CF
		id uint32
	}{{store.CFDoc, c.ID}, {store.CFIdx, a.ID}, {store.CFIdx, u.ID}, {store.CFGeo, g.ID}} {
		if got := e.scan(t, r.cf, r.id); len(got) != 0 {
			t.Errorf("%s/%d holds %d entries after deleting every document", r.cf, r.id, len(got))
		}
	}
	if n := count(t, e, c); n != 0 {
		t.Fatalf("count = %d", n)
	}
}

func TestDuplicateID(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	e.insert(t, c, `{"_id": 1, "a": 1}`)
	err := e.insertErr(c, parse(t, `{"_id": 1.0, "a": 2}`))
	wantErrText(t, err, CodeDuplicateKey, `E11000 duplicate key error collection: test.c index: _id_ dup key: { _id: 1.0 }`)
	if n := count(t, e, c); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

// TestUniqueRace inserts the same unique value from two goroutines with
// different _ids, round after round; exactly one of each pair must fail
// with E11000.
func TestUniqueRace(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.users")
	u := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("email")}, Unique: true})
	const rounds = 100
	var dups, oks [2]int
	for r := range rounds {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for g := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d := parse(t, fmt.Sprintf(`{"_id": "r%d-g%d", "email": "user%d@x"}`, r, g, r))
				errs[g] = e.insertErr(c, d)
			}()
		}
		wg.Wait()
		n := 0
		for g, err := range errs {
			switch {
			case err == nil:
				oks[g]++
			case IsDupKey(err):
				n++
				dups[g]++
				want := fmt.Sprintf(`E11000 duplicate key error collection: test.users index: email_1 dup key: { email: "user%d@x" }`, r)
				if err.Error() != want {
					t.Fatalf("round %d: %q, want %q", r, err.Error(), want)
				}
			default:
				t.Fatalf("round %d: %v", r, err)
			}
		}
		if n != 1 {
			t.Fatalf("round %d: %d E11000 errors, want exactly 1", r, n)
		}
	}
	if got := len(e.scan(t, store.CFIdx, u.ID)); got != rounds {
		t.Fatalf("%d unique entries, want %d", got, rounds)
	}
	if n := count(t, e, c); n != rounds {
		t.Fatalf("count = %d, want %d", n, rounds)
	}
	t.Logf("ok per goroutine %v, E11000 per goroutine %v", oks, dups)
}

func TestIndexedTxnSeesItsOwnWrites(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("u")}, Unique: true})
	txn := e.st.BeginIndexed()
	if err := e.ds.Insert(txn, c, parse(t, `{"_id": 1, "u": "same"}`)); err != nil {
		t.Fatal(err)
	}
	if err := e.ds.Insert(txn, c, parse(t, `{"_id": 2, "u": "same"}`)); !IsDupKey(err) {
		t.Fatalf("second insert of the value in one indexed Txn: %v", err)
	}
	if err := e.ds.Insert(txn, c, parse(t, `{"_id": 1, "u": "other"}`)); !IsDupKey(err) {
		t.Fatalf("second insert of the _id in one indexed Txn: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, e, c); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

func TestSparseAndMissingAsNull(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	plain := e.index(t, c, btree(asc("x")))
	sparse := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("y")}, Sparse: true})
	sparse2 := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("y"), asc("z")}, Sparse: true})

	e.insert(t, c, `{"_id": 1}`)
	e.insert(t, c, `{"_id": 2, "x": null, "y": null}`)
	e.insert(t, c, `{"_id": 3, "z": 5}`)
	e.insert(t, c, `{"_id": 4, "x": [], "y": {"$undefined": true}}`)
	ks := func(i int) []byte { return enc(t, fmt.Sprint(i), false) }
	null, undef := enc(t, `null`, false), []byte{keyenc.TUndefined}

	wantEntries(t, "non-sparse", e.scan(t, store.CFIdx, plain.ID), []kv{
		{cat(null, ks(1)), ""}, {cat(null, ks(2)), ""}, {cat(null, ks(3)), ""}, {cat(undef, ks(4)), ""},
	})
	// MongoDB's sparse rule: null is a value, only a missing field skips.
	wantEntries(t, "sparse", e.scan(t, store.CFIdx, sparse.ID), []kv{
		{cat(null, ks(2)), ""}, {cat(undef, ks(4)), ""},
	})
	wantEntries(t, "compound sparse", e.scan(t, store.CFIdx, sparse2.ID), []kv{
		{cat(null, null, ks(2)), ""}, {cat(null, enc(t, `5`, false), ks(3)), ""}, {cat(undef, null, ks(4)), ""},
	})

	d := e.coll(t, "test.u")
	e.index(t, d, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("email")}, Unique: true})
	us := e.index(t, d, catalog.Index{Kind: catalog.Btree, Name: "tag_sparse", Keys: []catalog.Key{asc("tag")}, Unique: true, Sparse: true})
	e.insert(t, d, `{"_id": 1}`)
	err := e.insertErr(d, parse(t, `{"_id": 2}`))
	wantErrText(t, err, CodeDuplicateKey, `E11000 duplicate key error collection: test.u index: email_1 dup key: { email: null }`)
	e.insert(t, d, `{"_id": 3, "email": "a"}`)
	e.insert(t, d, `{"_id": 4, "email": "b"}`)
	if got := e.scan(t, store.CFIdx, us.ID); len(got) != 0 {
		t.Fatalf("unique sparse index holds %d entries for documents without the field", len(got))
	}
}

func TestParallelArrays(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	e.index(t, c, btree(asc("a"), asc("b")))
	e.index(t, c, btree(asc("s.x"), asc("s.z")))

	err := e.insertErr(c, parse(t, `{"_id": 1, "a": [1, 2], "b": [3, 4]}`))
	wantErrText(t, err, CodeCannotIndexParallelArrays, "ERR cannot index parallel arrays [b] [a]")
	e.insert(t, c, `{"_id": 2, "a": [1, 2], "b": 3}`)
	e.insert(t, c, `{"_id": 3, "a": 1, "b": [3, 4]}`)
	// Fields under one shared array are fine...
	e.insert(t, c, `{"_id": 4, "s": [{"x": 1, "z": 2}, {"x": 3, "z": 4}]}`)
	// ...until two arrays meet inside one element.
	err = e.insertErr(c, parse(t, `{"_id": 5, "s": [{"x": [1, 2], "z": [3, 4]}]}`))
	wantErrText(t, err, CodeCannotIndexParallelArrays, "ERR cannot index parallel arrays [z] [x]")
	if n := count(t, e, c); n != 3 {
		t.Fatalf("count = %d", n)
	}
	// A build over a document with parallel arrays fails and leaves no index.
	d := e.coll(t, "test.d")
	e.insert(t, d, `{"_id": 1, "p": [1], "q": [2, 3]}`)
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(d.ID, true)})
	_, _, err = e.ds.CreateIndex(d, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("p"), asc("q")}, ExpireAfter: catalog.NoExpire})
	unlock()
	if Code(err) != CodeCannotIndexParallelArrays || len(d.Indexes()) != 0 {
		t.Fatalf("build over parallel arrays: %v, indexes %v", err, d.Indexes())
	}
}

func TestDecimal128InIndexedField(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	e.index(t, c, btree(asc("a")))
	const text = "ERR Decimal128 is not supported in indexes or arithmetic in v1"
	wantErrText(t, e.insertErr(c, parse(t, `{"_id": 1, "a": {"$numberDecimal": "1.5"}}`)), doc.CodeBadValue, text)
	wantErrText(t, e.insertErr(c, parse(t, `{"_id": 2, "a": {"n": {"$numberDecimal": "2"}}}`)), doc.CodeBadValue, text)
	wantErrText(t, e.insertErr(c, parse(t, `{"_id": 3, "a": [1, {"$numberDecimal": "2"}]}`)), doc.CodeBadValue, text)
	e.insert(t, c, `{"_id": 4, "a": 1, "b": {"$numberDecimal": "3.25"}}`)
	if n := count(t, e, c); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

func TestGeoEntries(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.places")
	g := e.index(t, c, catalog.Index{Kind: catalog.TwoDSphere, Keys: []catalog.Key{asc("loc")}})
	if g.S2 != geo.DefaultParams() {
		t.Fatalf("S2 = %+v", g.S2)
	}

	e.insert(t, c, `{"_id": "pt", "loc": {"type": "Point", "coordinates": [-73.97, 40.77]}}`)
	ents := e.scan(t, store.CFGeo, g.ID)
	if len(ents) != 1 {
		t.Fatalf("point entries: %q", ents)
	}
	ll := s2.LatLngFromDegrees(40.77, -73.97)
	wantKey := string(layout.GeoKey(nil, g.ID, uint64(s2.CellIDFromLatLng(ll)), enc(t, `"pt"`, false))[4:])
	if ents[0].key != wantKey || len(ents[0].val) != 17 {
		t.Fatalf("point entry %q, want key %q with a 17-byte value", ents[0], wantKey)
	}
	kind, got, err := geo.DecodeValue([]byte(ents[0].val))
	if err != nil || kind != geo.ValuePoint || got != ll {
		t.Fatalf("point value = %d %v %v, want %v", kind, got, err, ll)
	}

	// A legacy pair lands in the same leaf cell.
	e.insert(t, c, `{"_id": "legacy", "loc": [-73.97, 40.77]}`)
	polyJSON := `{"type": "Polygon", "coordinates": [[[0, 0], [3, 0], [3, 2], [0, 2], [0, 0]]]}`
	e.insert(t, c, `{"_id": "poly", "loc": `+polyJSON+`}`)
	for _, absent := range []string{`{"_id": "m"}`, `{"_id": "n", "loc": null}`, `{"_id": "e", "loc": []}`} {
		e.insert(t, c, absent)
	}

	raw, _ := doc.ParseEJSON([]byte(`{"g": ` + polyJSON + `}`))
	pg, err := geo.Parse(raw.Lookup("g"), geo.ParseOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var want []kv
	for _, en := range g.S2.Entries(pg) {
		want = append(want, kv{string(layout.GeoKey(nil, g.ID, uint64(en.Cell), enc(t, `"poly"`, false))[4:]), "\x02"})
	}
	if len(want) < 2 {
		t.Fatalf("polygon covering has %d cells", len(want))
	}
	want = append(want, ents[0], kv{string(layout.GeoKey(nil, g.ID, uint64(s2.CellIDFromLatLng(ll)), enc(t, `"legacy"`, false))[4:]), ents[0].val})
	wantEntries(t, "2dsphere", e.scan(t, store.CFGeo, g.ID), want)

	for _, bad := range []string{
		`{"_id": "b1", "loc": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}}`,
		`{"_id": "b2", "loc": "not a place"}`,
		`{"_id": "b3", "loc": [[1, 2], [3, 4]]}`,
		`{"_id": "b4", "loc": {"type": "Point", "coordinates": [200, 0]}}`,
	} {
		err := e.insertErr(c, parse(t, bad))
		if Code(err) != CodeGeoKeys || !bytes.HasPrefix([]byte(err.Error()), []byte("ERR Can't extract geo keys: ")) {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if n := count(t, e, c); n != 6 {
		t.Fatalf("count = %d", n)
	}
}

func TestLockKeys(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	u := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("a"), desc("b")}, Unique: true})
	e.index(t, c, btree(asc("c")))
	d := parse(t, `{"_id": 9, "a": [1, 2], "b": "x", "c": 3}`)
	keys := e.ds.LockKeys(c, d.Lookup("_id"), d)
	want := []store.LockKey{
		{Kind: store.LockDoc, NS: c.ID, Key: enc(t, `9`, false)},
		{Kind: store.LockUniq, NS: u.ID, Key: []byte(cat(enc(t, `1`, false), enc(t, `"x"`, true)))},
		{Kind: store.LockUniq, NS: u.ID, Key: []byte(cat(enc(t, `2`, false), enc(t, `"x"`, true)))},
	}
	if len(keys) != len(want) {
		t.Fatalf("LockKeys = %v", keys)
	}
	for i := range want {
		if keys[i].Kind != want[i].Kind || keys[i].NS != want[i].NS || !bytes.Equal(keys[i].Key, want[i].Key) {
			t.Errorf("key %d = %+v, want %+v", i, keys[i], want[i])
		}
	}
	if k := catalog.CollLock(c.ID, true); k.Kind != store.LockColl || k.Key[0] != 'w' {
		t.Fatalf("CollLock = %+v", k)
	}
}

func TestDropCollection(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.drop")
	a := e.index(t, c, btree(asc("a")))
	u := e.index(t, c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("u")}, Unique: true})
	g := e.index(t, c, catalog.Index{Kind: catalog.TwoDSphere, Keys: []catalog.Key{asc("loc")}})
	for i := range 50 {
		e.insert(t, c, fmt.Sprintf(`{"_id": %d, "a": %d, "u": %d, "loc": [%d, 1]}`, i, i%5, i, i))
	}
	keep := e.coll(t, "test.keep")
	e.insert(t, keep, `{"_id": 1}`)

	type check struct {
		cf store.CF
		id uint32
	}
	checks := []check{{store.CFDoc, c.ID}, {store.CFIdx, a.ID}, {store.CFIdx, u.ID}, {store.CFGeo, g.ID}}
	var publishes int
	e.cat.SetPublishHook(func(ls *catalog.LiveSet) {
		publishes++
		for _, ch := range checks {
			if ls.Live(ch.id) {
				t.Errorf("id %d still live after the drop", ch.id)
			}
			if n := len(e.scan(t, ch.cf, ch.id)); n != 0 {
				t.Errorf("unpublished id %d while %s held %d entries", ch.id, ch.cf, n)
			}
		}
	})
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	err := e.ds.DropCollection(c)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.cat.SetPublishHook(nil)
	if publishes != 1 {
		t.Fatalf("drop published %d times, want 1", publishes)
	}
	for _, ch := range checks {
		if got := e.scan(t, ch.cf, ch.id); len(got) != 0 {
			t.Errorf("%s/%d: %d entries left", ch.cf, ch.id, len(got))
		}
		if e.cat.Live(ch.id) {
			t.Errorf("id %d live", ch.id)
		}
	}
	for _, k := range [][]byte{layout.CollKey("test.drop"), layout.CountKey(c.ID), layout.IndexKey(c.ID, a.Name), layout.IndexKey(c.ID, g.Name)} {
		if _, ok, _ := e.st.Get(store.CFDefault, k); ok {
			t.Errorf("catalog key %q survived", k)
		}
	}
	if _, ok := e.cat.Resolve("test.drop"); ok {
		t.Fatal("dropped namespace resolves")
	}
	if len(e.scan(t, store.CFDoc, keep.ID)) != 1 || !e.cat.Live(keep.ID) {
		t.Fatal("the drop touched another collection")
	}
	if e.st.PendingCompactions() == 0 {
		t.Fatal("no compaction scheduled for the dropped ranges")
	}
	// A writer that resolved the collection before the drop is refused.
	if err := e.insertErr(c, parse(t, `{"_id": 99}`)); !errors.Is(err, catalog.ErrCollectionDropped) {
		t.Fatalf("insert into a dropped collection: %v", err)
	}
	// The namespace can be recreated under a new id.
	c2 := e.coll(t, "test.drop")
	if c2.ID == c.ID {
		t.Fatal("recreated collection reused the coll_id")
	}
	e.insert(t, c2, `{"_id": 1}`)
	if n := count(t, e, c2); n != 1 {
		t.Fatalf("count of the recreated collection = %d", n)
	}
}

func TestDropIndex(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	a := e.index(t, c, btree(asc("a")))
	b := e.index(t, c, btree(asc("b")))
	for i := range 10 {
		e.insert(t, c, fmt.Sprintf(`{"_id": %d, "a": %d, "b": %d}`, i, i, i))
	}
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	err := e.ds.DropIndex(c, a.Name)
	errUnknown := e.ds.DropIndex(c, "nope")
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if Code(errUnknown) != catalog.CodeIndexNotFound {
		t.Fatalf("drop of an unknown index: %v", errUnknown)
	}
	if len(e.scan(t, store.CFIdx, a.ID)) != 0 || e.cat.Live(a.ID) {
		t.Fatal("dropped index keeps entries or stays live")
	}
	if len(e.scan(t, store.CFIdx, b.ID)) != 10 || !e.cat.Live(b.ID) {
		t.Fatal("the other index was touched")
	}
	e.insert(t, c, `{"_id": 100, "a": 1, "b": 1}`)
	if len(e.scan(t, store.CFIdx, a.ID)) != 0 {
		t.Fatal("a write after the drop maintained the dropped index")
	}
}

// TestBuildMatchesInserts inserts 5,000 documents into one collection whose
// indexes exist before the inserts and into another without indexes, then
// builds the same indexes on the second. Every index must hold the same
// entries in both.
func TestBuildMatchesInserts(t *testing.T) {
	e := newEnv(t)
	live := e.coll(t, "test.live")
	built := e.coll(t, "test.built")
	specs := []catalog.Index{
		btree(asc("n"), desc("s")),
		btree(asc("tags")),
		{Kind: catalog.Btree, Keys: []catalog.Key{asc("u")}, Unique: true},
		{Kind: catalog.Btree, Keys: []catalog.Key{desc("opt")}, Sparse: true},
		{Kind: catalog.TwoDSphere, Keys: []catalog.Key{asc("loc")}},
	}
	var liveIx []*catalog.Index
	for _, s := range specs {
		liveIx = append(liveIx, e.index(t, live, s))
	}
	const n = 5000
	for i := range n {
		loc := fmt.Sprintf(`{"type": "Point", "coordinates": [%d.5, %d.25]}`, i%359-179, i%170-85)
		if i%7 == 0 {
			loc = fmt.Sprintf(`{"type": "LineString", "coordinates": [[%d, 0], [%d, 1]]}`, i%100, i%100+1)
		}
		opt := ""
		if i%3 == 0 {
			opt = fmt.Sprintf(`, "opt": "o%d"`, i%11)
		}
		d := fmt.Sprintf(`{"_id": {"$oid": "%024x"}, "n": %d, "s": "s%d", "tags": ["t%d", "t%d"], "u": %d, "loc": %s%s}`,
			i+1, i%97, i%13, i%5, i%9, i, loc, opt)
		e.insert(t, live, d)
		e.insert(t, built, d)
	}
	for i, s := range specs {
		bix := e.index(t, built, s)
		if bix.State != catalog.Ready {
			t.Fatalf("%s: state %v after build", bix.Name, bix.State)
		}
		lix, _ := live.IndexByID(liveIx[i].ID)
		if bix.Multikey() != lix.Multikey() {
			t.Errorf("%s: multikey built %v, inserted %v", bix.Name, bix.Multikey(), lix.Multikey())
		}
		cf := store.CFIdx
		if s.Kind == catalog.TwoDSphere {
			cf = store.CFGeo
		}
		want := e.scan(t, cf, liveIx[i].ID)
		got := e.scan(t, cf, bix.ID)
		if len(want) == 0 {
			t.Fatalf("%s: no entries", bix.Name)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s: built %d entries, inserts wrote %d, or they differ", bix.Name, len(got), len(want))
		}
	}
	if count(t, e, built) != n {
		t.Fatalf("count = %d", count(t, e, built))
	}
}

func TestBuildUniqueDuplicatesCleansUp(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		name     string
		dupOf    int // document index that repeats document 0's value
		wantText string
	}{
		{"same batch", 10, `E11000 duplicate key error collection: test.same batch index: v_1 dup key: { v: "dup" }`},
		{"later batch", 700, `E11000 duplicate key error collection: test.later batch index: v_1 dup key: { v: "dup" }`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := e.coll(t, "test."+tc.name)
			for i := range 800 {
				v := fmt.Sprintf("v%d", i)
				if i == 0 || i == tc.dupOf {
					v = "dup"
				}
				e.insert(t, c, fmt.Sprintf(`{"_id": %d, "v": %q}`, i, v))
			}
			var newID uint32 // the largest id any publication held: the new index
			e.cat.SetPublishHook(func(ls *catalog.LiveSet) {
				if ids := ls.IDs(); len(ids) > 0 {
					newID = max(newID, ids[len(ids)-1])
				}
			})
			unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
			ix, created, err := e.ds.CreateIndex(c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("v")}, Unique: true, ExpireAfter: catalog.NoExpire})
			unlock()
			e.cat.SetPublishHook(nil)
			wantErrText(t, err, CodeDuplicateKey, tc.wantText)
			if ix != nil || created || len(c.Indexes()) != 0 {
				t.Fatalf("failed build left %v, %v, %v", ix, created, c.Indexes())
			}
			if newID <= c.ID || e.cat.Live(newID) {
				t.Fatalf("the failed index id %d is still live or was never published", newID)
			}
			if got := e.scan(t, store.CFIdx, newID); len(got) != 0 {
				t.Fatalf("failed build left %d entries", len(got))
			}
			if _, ok, _ := e.st.Get(store.CFDefault, layout.IndexKey(c.ID, "v_1")); ok {
				t.Fatal("failed build left its catalog record")
			}
		})
	}
}

func TestResumeBuilds(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	for i := range 300 {
		e.insert(t, c, fmt.Sprintf(`{"_id": %d, "a": %d}`, i, i%10))
	}
	// A crash in the middle of a build leaves a Building record and some
	// entries, one of them stale.
	unlock := e.st.Lock([]store.LockKey{catalog.CollLock(c.ID, true)})
	ix, err := e.cat.AddIndex(c, catalog.Index{Kind: catalog.Btree, Keys: []catalog.Key{asc("a")}, ExpireAfter: catalog.NoExpire})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	txn := e.st.Begin()
	txn.Put(store.CFIdx, layout.IdxKey(nil, ix.ID, enc(t, `"stale"`, false), enc(t, `12345`, false)), nil)
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := e.ds.ResumeBuilds(); err != nil {
		t.Fatal(err)
	}
	cur, _ := c.IndexByID(ix.ID)
	if cur.State != catalog.Ready {
		t.Fatalf("state %v after ResumeBuilds", cur.State)
	}
	ents := e.scan(t, store.CFIdx, ix.ID)
	if len(ents) != 300 {
		t.Fatalf("%d entries after the rebuild, want 300", len(ents))
	}
	for _, en := range ents {
		if bytes.Contains([]byte(en.key), []byte("stale")) {
			t.Fatal("the rebuild kept a stale entry")
		}
	}
}

func TestDupKeyErrorFormat(t *testing.T) {
	raw, err := doc.ParseEJSON([]byte(`{"s": "a\"b", "i": 7, "l": {"$numberLong": "9"}, "d": 13.0, "f": 2.5, "o": {"$oid": "5f1d7a0e2c3b4a5d6e7f8091"},
		"t": {"$date": {"$numberLong": "1000"}}, "b": true, "doc": {"x": 1, "y": [1, "z"]}, "arr": [], "empty": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	var fields []string
	var vals []bson.RawValue
	elems, _ := raw.Elements()
	for _, el := range elems {
		fields = append(fields, el.Key())
		vals = append(vals, el.Value())
	}
	fields = append(fields, "missing")
	vals = append(vals, bson.RawValue{})
	e := newDupKeyError("db.c", "ix", fields, vals)
	want := `E11000 duplicate key error collection: db.c index: ix dup key: { s: "a\"b", i: 7, l: 9, d: 13.0, f: 2.5, ` +
		`o: ObjectId('5f1d7a0e2c3b4a5d6e7f8091'), t: new Date(1000), b: true, doc: { x: 1, y: [ 1, "z" ] }, arr: [], empty: {}, missing: null }`
	if got := e.Error(); got != want {
		t.Fatalf("\n got  %s\n want %s", got, want)
	}
	if Code(e) != CodeDuplicateKey || Message(e) != want {
		t.Fatal("Code/Message")
	}
	kvDoc := e.KeyValue()
	if kvDoc.Lookup("missing").Type != bson.TypeNull || kvDoc.Lookup("i").Int32() != 7 {
		t.Fatalf("KeyValue = %s", kvDoc)
	}
	if Message(catalog.ErrColumnar) != "columnar indexes are not supported in v1" || Code(catalog.ErrColumnar) != catalog.CodeCannotCreateIndex {
		t.Fatal("Message/Code of a doc.Error")
	}
}

// TestMultikeyFlagPersisted checks that the first multikey write persists
// the index's multikey flag on its own, so a discarded Txn cannot leave
// multikey documents behind an index recorded as not multikey.
func TestMultikeyFlagPersisted(t *testing.T) {
	e := newEnv(t)
	c := e.coll(t, "test.c")
	ix := e.index(t, c, btree(asc("tags")))
	flag := func() bool {
		v, ok, err := e.st.Get(store.CFDefault, layout.IndexKey(c.ID, ix.Name))
		if err != nil || !ok {
			t.Fatalf("index record: %v, %v", ok, err)
		}
		return bson.Raw(v).Lookup("multikey").Boolean()
	}
	e.insert(t, c, `{"_id": 1, "tags": "scalar"}`)
	if flag() {
		t.Fatal("a scalar value marked the index multikey")
	}
	txn := e.st.Begin()
	if err := e.ds.Insert(txn, c, parse(t, `{"_id": 2, "tags": ["a", "b"]}`)); err != nil {
		t.Fatal(err)
	}
	txn.Discard()
	if !flag() {
		t.Fatal("the multikey flag is not on disk before the writer's Txn commits")
	}
	if cur, _ := c.IndexByID(ix.ID); !cur.Multikey() || cur.State != catalog.Ready {
		t.Fatalf("in-memory record = %+v, multikey %v", cur, cur.Multikey())
	}
}
