package catalog

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
)

func testConfig(dir string) store.Config {
	return store.Config{
		Dir:                 dir,
		Engine:              store.TestEngine(),
		BlockCacheBytes:     8 << 20,
		AnalyticsCacheBytes: 4 << 20,
		WriteBufferBytes:    32 << 20,
		Fsync:               store.FsyncNo,
	}
}

// openAt opens a store in dir and its catalog; the store closes at cleanup
// unless the test closes it first.
func openAt(t *testing.T, dir string) (*store.Store, *Catalog) {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	st, err := store.Open(testConfig(dir))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cat, err := Open(st)
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	return st, cat
}

func open(t *testing.T) (*store.Store, *Catalog) { return openAt(t, t.TempDir()) }

func mustCreate(t *testing.T, cat *Catalog, ns string) *Collection {
	t.Helper()
	coll, err := cat.Create(ns)
	if err != nil {
		t.Fatalf("Create(%q): %v", ns, err)
	}
	return coll
}

func btree(name string, keys ...Key) Index {
	return Index{Name: name, Kind: Btree, Keys: keys, ExpireAfter: NoExpire}
}

func mustAdd(t *testing.T, cat *Catalog, coll *Collection, spec Index) *Index {
	t.Helper()
	ix, err := cat.AddIndex(coll, spec)
	if err != nil {
		t.Fatalf("AddIndex(%s): %v", spec.Name, err)
	}
	return ix
}

func wantCode(t *testing.T, err error, code int, msg string) {
	t.Helper()
	var de *doc.Error
	if !errors.As(err, &de) {
		t.Fatalf("error %v (%T), want *doc.Error code %d", err, err, code)
	}
	if de.Code != code || (msg != "" && de.Msg != msg) {
		t.Fatalf("error {%d %q}, want {%d %q}", de.Code, de.Msg, code, msg)
	}
}

// runDrop does what docstore does with a drop plan: one DeleteRanges batch
// with the catalog records, then CommitDrop.
func runDrop(t *testing.T, st *store.Store, cat *Catalog, d Drop) {
	t.Helper()
	var ranges []store.RangeDelete
	if d.Whole {
		lo, hi := layout.IDBounds(d.Coll.ID)
		ranges = append(ranges, store.RangeDelete{CF: store.CFDoc, Lo: lo, Hi: hi})
	}
	for _, ix := range d.Indexes {
		lo, hi := layout.IDBounds(ix.ID)
		ranges = append(ranges, store.RangeDelete{CF: store.CFIdx, Lo: lo, Hi: hi})
	}
	if err := st.DeleteRanges(ranges, d.Records); err != nil {
		t.Fatalf("DeleteRanges: %v", err)
	}
	cat.CommitDrop(d)
}

func TestCreateResolveList(t *testing.T) {
	_, cat := open(t)
	a := mustCreate(t, cat, "shop.orders")
	b := mustCreate(t, cat, "shop.users")
	mustCreate(t, cat, "other.x")
	mustCreate(t, cat, "plain")

	if got, ok := cat.Resolve("shop.orders"); !ok || got != a {
		t.Fatalf("Resolve(shop.orders) = %v, %v", got, ok)
	}
	if got, ok := cat.ByID(b.ID); !ok || got != b {
		t.Fatalf("ByID(%d) = %v, %v", b.ID, got, ok)
	}
	if _, ok := cat.Resolve("shop.missing"); ok {
		t.Fatal("Resolve of an unknown namespace succeeded")
	}
	if got := cat.List("shop."); !slices.Equal(got, []string{"shop.orders", "shop.users"}) {
		t.Fatalf("List(shop.) = %q", got)
	}
	if got := cat.List(""); len(got) != 4 || got[0] != "other.x" {
		t.Fatalf("List() = %q", got)
	}
	if id, ok := cat.ResolveNS("shop.users"); !ok || id != b.ID {
		t.Fatalf("ResolveNS = %d, %v", id, ok)
	}

	_, err := cat.Create("shop.orders")
	wantCode(t, err, CodeNamespaceExists, "Collection already exists. NS: shop.orders")
	for _, bad := range []string{"", ".x", "x.", "a\x00b", string([]byte{0xff})} {
		_, err := cat.Create(bad)
		wantCode(t, err, CodeInvalidNamespace, "")
	}

	got, created, err := cat.Ensure("shop.orders")
	if err != nil || created || got != a {
		t.Fatalf("Ensure(existing) = %v, %v, %v", got, created, err)
	}
	got, created, err = cat.Ensure("shop.new")
	if err != nil || !created || got.NS != "shop.new" {
		t.Fatalf("Ensure(new) = %v, %v, %v", got, created, err)
	}
}

func TestEnsureConcurrent(t *testing.T) {
	_, cat := open(t)
	const n = 16
	colls := make([]*Collection, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _, err := cat.Ensure("race.c")
			if err != nil {
				t.Errorf("Ensure: %v", err)
			}
			colls[i] = c
		}()
	}
	wg.Wait()
	for _, c := range colls[1:] {
		if c != colls[0] {
			t.Fatal("concurrent Ensure calls returned different collections")
		}
	}
}

func TestIDsDisjointAndNeverReused(t *testing.T) {
	dir := t.TempDir()
	st, cat := openAt(t, dir)
	a := mustCreate(t, cat, "db.a")
	ixA := mustAdd(t, cat, a, btree("", Key{Field: "x"}))
	b := mustCreate(t, cat, "db.b")
	ids := []uint32{a.ID, ixA.ID, b.ID}
	if !slices.IsSorted(ids) || a.ID == ixA.ID || ixA.ID == b.ID {
		t.Fatalf("ids %v are not increasing and distinct", ids)
	}

	d, err := cat.DropCollection(b)
	if err != nil {
		t.Fatal(err)
	}
	runDrop(t, st, cat, d)
	c := mustCreate(t, cat, "db.b")
	if c.ID <= b.ID {
		t.Fatalf("recreated collection got id %d, not above dropped id %d", c.ID, b.ID)
	}
	_, dIx, err := cat.DropIndex(a, ixA.Name)
	if err != nil {
		t.Fatal(err)
	}
	runDrop(t, st, cat, dIx)
	ix2 := mustAdd(t, cat, a, btree("", Key{Field: "x"}))
	if ix2.ID <= c.ID {
		t.Fatalf("new index id %d not above %d", ix2.ID, c.ID)
	}
	last := ix2.ID
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, cat2 := openAt(t, dir)
	_ = st2
	e := mustCreate(t, cat2, "db.e")
	if e.ID <= last {
		t.Fatalf("after reopen: id %d reuses the space below %d", e.ID, last)
	}
	for _, dropped := range []uint32{b.ID, ixA.ID} {
		if cat2.Live(dropped) {
			t.Fatalf("dropped id %d is live after reopen", dropped)
		}
	}
}

func TestAddIndexValidation(t *testing.T) {
	_, cat := open(t)
	coll := mustCreate(t, cat, "db.c")

	_, err := cat.AddIndex(coll, Index{Kind: Columnar, Keys: []Key{{Field: "amount"}}, ExpireAfter: NoExpire})
	if err == nil || err.Error() != "ERR columnar indexes are not supported in v1" {
		t.Fatalf("columnar: %v", err)
	}
	if _, err := cat.AddIndex(coll, btree("", Key{Field: "_id"})); !errors.Is(err, ErrIDIndex) {
		t.Fatalf("{_id: 1}: %v", err)
	}

	ab := mustAdd(t, cat, coll, btree("", Key{Field: "a"}, Key{Field: "b", Desc: true}))
	if ab.Name != "a_1_b_-1" || ab.State != Building || ab.Kind != Btree {
		t.Fatalf("index = %+v", ab)
	}
	if got := ab.KeyDoc().String(); got != `{"a": {"$numberInt":"1"},"b": {"$numberInt":"-1"}}` {
		t.Fatalf("KeyDoc = %s", got)
	}
	again, err := cat.AddIndex(coll, btree("", Key{Field: "a"}, Key{Field: "b", Desc: true}))
	if !errors.Is(err, ErrIndexExists) || again != ab {
		t.Fatalf("identical AddIndex = %v, %v", again, err)
	}
	_, err = cat.AddIndex(coll, btree("a_1_b_-1", Key{Field: "a"}))
	wantCode(t, err, CodeIndexKeySpecsConflict, "")
	_, err = cat.AddIndex(coll, btree("other", Key{Field: "a"}, Key{Field: "b", Desc: true}))
	wantCode(t, err, CodeIndexOptionsConflict, "Index already exists with a different name: a_1_b_-1")

	geoIx := mustAdd(t, cat, coll, Index{Kind: TwoDSphere, Keys: []Key{{Field: "loc"}}, ExpireAfter: NoExpire})
	if geoIx.Name != "loc_2dsphere" || geoIx.S2 != geo.DefaultParams() {
		t.Fatalf("2dsphere index = %+v", geoIx)
	}
	for name, spec := range map[string]Index{
		"compound 2dsphere": {Kind: TwoDSphere, Keys: []Key{{Field: "a"}, {Field: "b"}}, ExpireAfter: NoExpire},
		"unique 2dsphere":   {Kind: TwoDSphere, Keys: []Key{{Field: "g"}}, Unique: true, ExpireAfter: NoExpire},
		"bad s2":            {Kind: TwoDSphere, Keys: []Key{{Field: "h"}}, S2: geo.Params{Coarsest: 10, Finest: 5, MaxCells: 8}, ExpireAfter: NoExpire},
		"no keys":           {Kind: Btree, ExpireAfter: NoExpire},
		"dollar field":      btree("", Key{Field: "$x"}),
		"repeated field":    btree("", Key{Field: "q"}, Key{Field: "q", Desc: true}),
		"empty component":   btree("", Key{Field: "a..b"}),
		"compound ttl":      {Kind: Btree, Keys: []Key{{Field: "t"}, {Field: "u"}}, ExpireAfter: 10},
		"reserved name":     btree("_id_", Key{Field: "z"}),
	} {
		if _, err := cat.AddIndex(coll, spec); err == nil {
			t.Errorf("%s: AddIndex succeeded", name)
		} else {
			wantCode(t, err, CodeCannotCreateIndex, "")
		}
	}
	ttl := mustAdd(t, cat, coll, Index{Kind: Btree, Keys: []Key{{Field: "ts"}}, ExpireAfter: 3600})
	if ttl.ExpireAfter != 3600 {
		t.Fatalf("ExpireAfter = %d", ttl.ExpireAfter)
	}

	// 64 indexes including _id: 61 more fit next to the three above.
	for i := len(coll.Indexes()); i < MaxIndexes-1; i++ {
		mustAdd(t, cat, coll, btree("", Key{Field: "f" + string(rune('A'+i%26)) + string(rune('a'+i/26))}))
	}
	_, err = cat.AddIndex(coll, btree("", Key{Field: "one_too_many"}))
	wantCode(t, err, CodeCannotCreateIndex, "")

	if got := cat.UniqueIndexes(coll.ID); len(got) != 0 {
		t.Fatalf("UniqueIndexes = %v", got)
	}
}

func TestIndexStateMultikeyAndReopen(t *testing.T) {
	dir := t.TempDir()
	st, cat := openAt(t, dir)
	coll := mustCreate(t, cat, "shop.orders")
	u := mustAdd(t, cat, coll, Index{Kind: Btree, Name: "email_u", Keys: []Key{{Field: "email"}}, Unique: true, Sparse: true, ExpireAfter: NoExpire})
	m := mustAdd(t, cat, coll, btree("", Key{Field: "tags"}, Key{Field: "n", Desc: true}))
	g := mustAdd(t, cat, coll, Index{Kind: TwoDSphere, Keys: []Key{{Field: "loc"}}, S2: geo.Params{Coarsest: 3, Finest: 14, MaxCells: 12}, ExpireAfter: NoExpire})

	ready, err := cat.SetIndexState(coll, u, Ready)
	if err != nil {
		t.Fatal(err)
	}
	if ready == u || ready.State != Ready || u.State != Building {
		t.Fatalf("SetIndexState: new %+v, old %+v", ready, u)
	}
	if cur, _ := coll.Index("email_u"); cur != ready {
		t.Fatal("collection list does not hold the Ready record")
	}
	if _, err := cat.SetIndexState(coll, m, Ready); err != nil {
		t.Fatal(err)
	}

	if err := cat.MarkMultikey(coll, m); err != nil {
		t.Fatal(err)
	}
	txn := st.Begin()
	txn.Merge(store.CFDefault, layout.CountKey(coll.ID), layout.AppendCount(nil, 7))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if !m.Multikey() {
		t.Fatal("MarkMultikey did not set the flag")
	}
	if cur, _ := coll.Index(m.Name); !cur.Multikey() {
		t.Fatal("the Ready copy does not share the multikey flag")
	}
	if n, err := cat.Count(coll); err != nil || n != 7 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if got := cat.UniqueIndexes(coll.ID); !slices.Equal(got, []uint32{u.ID}) {
		t.Fatalf("UniqueIndexes = %v", got)
	}
	before := coll.Indexes()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	_, cat2 := openAt(t, dir)
	c2, ok := cat2.Resolve("shop.orders")
	if !ok || c2.ID != coll.ID || c2.Created != coll.Created {
		t.Fatalf("reopened collection = %+v", c2)
	}
	after := c2.Indexes()
	if len(after) != len(before) {
		t.Fatalf("reopened %d indexes, want %d", len(after), len(before))
	}
	for i, want := range before {
		got := after[i]
		if got.ID != want.ID || got.Name != want.Name || got.Kind != want.Kind || !got.sameSpec(want) ||
			got.State != want.State || got.Multikey() != want.Multikey() {
			t.Errorf("index %d: reopened %+v (multikey %v), want %+v (multikey %v)", i, got, got.Multikey(), want, want.Multikey())
		}
	}
	if gi, _ := c2.Index(g.Name); gi.S2 != (geo.Params{Coarsest: 3, Finest: 14, MaxCells: 12}) || gi.State != Building {
		t.Fatalf("2dsphere after reopen = %+v", gi)
	}
	if n, err := cat2.Count(c2); err != nil || n != 7 {
		t.Fatalf("Count after reopen = %d, %v", n, err)
	}
	for _, id := range []uint32{coll.ID, u.ID, m.ID, g.ID} {
		if !cat2.Live(id) {
			t.Errorf("id %d not live after reopen", id)
		}
	}
}

// TestLiveSetOrdering asserts, through the publish hook, that a new id is
// live before the first write under it and that a dropped id stays live
// until the drop batch has committed.
func TestLiveSetOrdering(t *testing.T) {
	st, cat := open(t)
	docKeys := func(id uint32) int {
		lo, hi := layout.IDBounds(id)
		it := st.Iter(store.CFDoc, lo, hi, store.IterOpts{})
		defer it.Close()
		n := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			n++
		}
		return n
	}
	type event struct {
		live []uint32
		rows map[uint32]int // doc rows present per watched id at publish time
	}
	var events []event
	var watch []uint32
	cat.SetPublishHook(func(ls *LiveSet) {
		ev := event{live: ls.IDs(), rows: map[uint32]int{}}
		for _, id := range watch {
			ev.rows[id] = docKeys(id)
		}
		events = append(events, ev)
	})

	coll := mustCreate(t, cat, "db.c")
	if len(events) != 1 || !slices.Contains(events[0].live, coll.ID) {
		t.Fatalf("Create published %v, want a set with %d", events, coll.ID)
	}
	if docKeys(coll.ID) != 0 {
		t.Fatal("rows exist before the first write")
	}
	// The first write under the id comes after the publication above.
	txn := st.Begin()
	txn.Put(store.CFDoc, layout.DocKey(nil, coll.ID, []byte{0x1e, 0x05, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0}), []byte("row"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	ix := mustAdd(t, cat, coll, btree("", Key{Field: "a"}))
	if len(events) != 2 || !slices.Contains(events[1].live, ix.ID) {
		t.Fatalf("AddIndex did not publish its id: %v", events)
	}

	watch = []uint32{coll.ID}
	d, err := cat.DropCollection(coll)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatal("DropCollection published before the drop batch")
	}
	if !cat.Live(coll.ID) || !cat.Live(ix.ID) {
		t.Fatal("ids left the LiveSet before the drop batch committed")
	}
	if _, ok := cat.Resolve("db.c"); ok || !coll.Dropped() {
		t.Fatal("a dropped collection still resolves")
	}
	runDrop(t, st, cat, d)
	if len(events) != 3 {
		t.Fatalf("CommitDrop published %d times, want once", len(events)-2)
	}
	last := events[2]
	if slices.Contains(last.live, coll.ID) || slices.Contains(last.live, ix.ID) {
		t.Fatalf("CommitDrop left %v live", last.live)
	}
	if last.rows[coll.ID] != 0 {
		t.Fatalf("the LiveSet dropped id %d while %d rows were still present", coll.ID, last.rows[coll.ID])
	}
	for _, k := range [][]byte{layout.CollKey("db.c"), layout.IndexKey(coll.ID, ix.Name), layout.CountKey(coll.ID)} {
		if _, ok, _ := st.Get(store.CFDefault, k); ok {
			t.Errorf("catalog record %q survived the drop", k)
		}
	}
}

func TestDropIndexErrorsAndAbort(t *testing.T) {
	_, cat := open(t)
	coll := mustCreate(t, cat, "db.c")
	a := mustAdd(t, cat, coll, btree("", Key{Field: "a"}))
	b := mustAdd(t, cat, coll, btree("", Key{Field: "b"}))

	_, _, err := cat.DropIndex(coll, "_id_")
	wantCode(t, err, CodeInvalidOptions, "cannot drop _id index")
	_, _, err = cat.DropIndex(coll, "nope")
	wantCode(t, err, CodeIndexNotFound, "index not found with name [nope]")

	got, d, err := cat.DropIndex(coll, a.Name)
	if err != nil || got != a {
		t.Fatalf("DropIndex = %v, %v", got, err)
	}
	if _, ok := coll.Index(a.Name); ok {
		t.Fatal("dropped index still listed")
	}
	cat.AbortDrop(d)
	if list := coll.Indexes(); len(list) != 2 || list[0] != a || list[1] != b {
		t.Fatalf("AbortDrop restored %v", list)
	}

	dc, err := cat.DropCollection(coll)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.DropCollection(coll); err == nil {
		t.Fatal("second DropCollection succeeded")
	}
	if _, err := cat.AddIndex(coll, btree("", Key{Field: "c"})); !errors.Is(err, ErrCollectionDropped) {
		t.Fatalf("AddIndex on a dropped collection: %v", err)
	}
	cat.AbortDrop(dc)
	if got, ok := cat.Resolve("db.c"); !ok || got != coll || coll.Dropped() {
		t.Fatal("AbortDrop did not restore the collection")
	}
}

func TestVersionSeed(t *testing.T) {
	_, cat := open(t)
	if v, err := cat.NextVersionSeed(); err != nil || v != 1 {
		t.Fatalf("NextVersionSeed on an empty store = %d, %v", v, err)
	}
	if err := cat.SaveVersion(500); err != nil {
		t.Fatal(err)
	}
	if err := cat.SaveVersion(100); err != nil {
		t.Fatal(err)
	}
	if v, err := cat.NextVersionSeed(); err != nil || v != 501 {
		t.Fatalf("NextVersionSeed = %d, %v, want 501", v, err)
	}
}

func TestLiveSetType(t *testing.T) {
	var _ store.LiveSet = (*LiveSet)(nil)
	ls := newLiveSet([]uint32{9, 3, 3, 7})
	if !slices.Equal(ls.IDs(), []uint32{3, 7, 9}) || ls.Len() != 3 {
		t.Fatalf("IDs = %v", ls.IDs())
	}
	if !ls.Live(7) || ls.Live(8) {
		t.Fatal("Live")
	}
	if got := ls.with(8).without(3, 9).IDs(); !slices.Equal(got, []uint32{7, 8}) {
		t.Fatalf("with/without = %v", got)
	}
	var nilSet *LiveSet
	if nilSet.Live(1) || nilSet.Len() != 0 {
		t.Fatal("nil LiveSet")
	}
}

func TestDefaultIndexName(t *testing.T) {
	for _, tc := range []struct {
		kind IndexKind
		keys []Key
		want string
	}{
		{Btree, []Key{{Field: "a"}}, "a_1"},
		{Btree, []Key{{Field: "a.b"}, {Field: "c", Desc: true}}, "a.b_1_c_-1"},
		{TwoDSphere, []Key{{Field: "loc"}}, "loc_2dsphere"},
	} {
		if got := DefaultIndexName(tc.kind, tc.keys); got != tc.want {
			t.Errorf("DefaultIndexName(%v) = %q, want %q", tc.keys, got, tc.want)
		}
	}
	spec := doc.SortSpec([]Key{{Field: "a", Desc: true}})
	if len(spec) != 1 || !spec[0].Desc {
		t.Fatal("[]Key does not convert to doc.SortSpec")
	}
}
