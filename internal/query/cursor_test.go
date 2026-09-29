package query

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func cursorEnv(t *testing.T, n int) (*tenv, *catalog.Collection) {
	e := newEnv(t)
	c := e.coll("test.cursor")
	e.index(c, btree("n"))
	var docs []bson.Raw
	for i := range n {
		docs = append(docs, mustMarshal(t, bson.D{{Key: "_id", Value: int32(i)}, {Key: "n", Value: int32(i % 17)}}))
	}
	e.insert(c, docs...)
	return e, c
}

func ids(t *testing.T, docs []bson.Raw) []int32 {
	t.Helper()
	out := make([]int32, len(docs))
	for i, d := range docs {
		out[i] = d.Lookup("_id").Int32()
	}
	return out
}

func mustSort(t *testing.T, s bson.Raw) doc.SortSpec {
	t.Helper()
	spec, err := doc.ParseSort(s)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// openCursor runs a plan's first batch and registers the rest as a cursor.
func openCursor(t *testing.T, e *tenv, c *catalog.Collection, f Find, batch int) (*Cursor, []bson.Raw) {
	t.Helper()
	p, err := e.q.PlanFind(c, f, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	first, done, err := run.Next(batch, 0)
	if err != nil || done {
		t.Fatalf("first batch: done %v, err %v", done, err)
	}
	cur, err := e.q.Cursors.Open(run, CursorOptions{NS: c.NS, Kind: "find", Batch: batch})
	if err != nil {
		t.Fatal(err)
	}
	return cur, first
}

// readAll reads a cursor to exhaustion.
func readAll(t *testing.T, e *tenv, id uint64) (docs []bson.Raw, reads int) {
	t.Helper()
	for {
		_, batch, done, err := e.q.Cursors.Read(id, 0, 0)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		docs = append(docs, batch...)
		reads++
		if done {
			return docs, reads
		}
	}
}

func TestCursorPagingReleasesLease(t *testing.T) {
	e, c := cursorEnv(t, 95)
	cur, first := openCursor(t, e, c, Find{}, 10)
	if len(first) != 10 || len(e.st.Leases()) != 1 {
		t.Fatalf("first batch %d documents, %d leases; want 10 and 1", len(first), len(e.st.Leases()))
	}
	rest, reads := readAll(t, e, cur.ID)
	all := append(first, rest...)
	want := make([]int32, 95)
	for i := range want {
		want[i] = int32(i)
	}
	if got := ids(t, all); !slices.Equal(got, want) {
		t.Fatalf("paged ids = %v", got)
	}
	if reads != 9 {
		t.Errorf("read %d batches, want 9 (8 full, 1 of 5)", reads)
	}
	if e.q.Cursors.Len() != 0 || len(e.st.Leases()) != 0 {
		t.Fatalf("after exhaustion: %d cursors, %d leases", e.q.Cursors.Len(), len(e.st.Leases()))
	}
	want404 := "ERR cursor id " + strconv.FormatUint(cur.ID, 10) + " not found"
	if _, _, _, err := e.q.Cursors.Read(cur.ID, 0, 0); err == nil || err.Error() != want404 {
		t.Fatalf("read after exhaustion: %v, want %q", err, want404)
	}
}

// TestCursorReadsItsLease checks that the batches after the first read the
// cursor's lease: documents inserted after Open are not seen.
func TestCursorReadsItsLease(t *testing.T) {
	e, c := cursorEnv(t, 50)
	cur, first := openCursor(t, e, c, Find{}, 5)
	var late []bson.Raw
	for i := 50; i < 60; i++ {
		late = append(late, mustMarshal(t, bson.D{{Key: "_id", Value: int32(i)}, {Key: "n", Value: int32(1)}}))
	}
	e.insert(c, late...)
	rest, _ := readAll(t, e, cur.ID)
	if got := len(first) + len(rest); got != 50 {
		t.Fatalf("cursor returned %d documents, want the 50 committed before Open", got)
	}
}

func TestCursorIndexAndReverseResume(t *testing.T) {
	e, c := cursorEnv(t, 200)
	for _, tc := range []struct {
		f     Find
		batch int
	}{
		{Find{Filter: ej(t, `{"n": {"$gte": 10}}`)}, 7},
		{Find{Sort: ej(t, `{"_id": -1}`)}, 9},
		{Find{Filter: ej(t, `{"n": {"$in": [3, 4, 11]}}`), Sort: ej(t, `{"n": -1}`)}, 4},
	} {
		cur, first := openCursor(t, e, c, tc.f, tc.batch)
		rest, _ := readAll(t, e, cur.ID)
		got := append(first, rest...)
		want := brute(t, e.all(c), tc.f)
		if g, w := len(got), len(want); g != w {
			t.Fatalf("%s: %d documents, want %d", ejsonOf(tc.f.Filter), g, w)
		}
		if len(tc.f.Sort) > 0 {
			spec := mustSort(t, tc.f.Sort)
			if !slices.Equal(sortKeys(got, spec), sortKeys(want, spec)) {
				t.Fatalf("%s: order differs", ejsonOf(tc.f.Filter))
			}
		}
		if !slices.Equal(sortedEJSON(got), sortedEJSON(want)) {
			t.Fatalf("%s: documents differ", ejsonOf(tc.f.Filter))
		}
	}
}

func TestCursorDelAndIdleReap(t *testing.T) {
	e, c := cursorEnv(t, 30)
	a, _ := openCursor(t, e, c, Find{}, 3)
	b, _ := openCursor(t, e, c, Find{}, 3)
	if len(e.st.Leases()) != 2 {
		t.Fatalf("%d leases, want 2", len(e.st.Leases()))
	}
	if !e.q.Cursors.Del(a.ID) || e.q.Cursors.Del(a.ID) {
		t.Fatalf("Del should succeed once")
	}
	if len(e.st.Leases()) != 1 {
		t.Fatalf("Del left %d leases", len(e.st.Leases()))
	}
	list := e.q.Cursors.List()
	if len(list) != 1 || list[0].ID != b.ID || list[0].Kind != "find" || list[0].Batch != 3 {
		t.Fatalf("List = %+v", list)
	}
	later := time.Now().Add(2 * e.st.Config().LeaseTTL)
	e.q.Cursors.now = func() time.Time { return later }
	if n := len(e.q.Cursors.List()); n != 0 {
		t.Fatalf("idle cursor survived the reaper: %d open", n)
	}
	if len(e.st.Leases()) != 0 {
		t.Fatalf("reaped cursor kept its lease")
	}
}

// TestCursorLeaseRenewal: an OLTP cursor whose lease expired takes a fresh
// one and keeps reading.
func TestCursorLeaseRenewal(t *testing.T) {
	e, c := cursorEnv(t, 40)
	cur, first := openCursor(t, e, c, Find{}, 10)
	e.st.Release(e.st.Leases()[0])
	rest, _ := readAll(t, e, cur.ID)
	if len(first)+len(rest) != 40 {
		t.Fatalf("renewed cursor returned %d documents", len(first)+len(rest))
	}
	if len(e.st.Leases()) != 0 {
		t.Fatalf("renewed lease leaked")
	}
}

// TestCursorFixedLeaseExpires: a cursor pinned to a lease it may not renew
// (NIL.AGGREGATE's) fails once the lease is gone and closes.
func TestCursorFixedLeaseExpires(t *testing.T) {
	e, c := cursorEnv(t, 40)
	lease, err := e.st.Lease("test", 0)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.q.PlanFind(c, Find{}, Options{Mode: Analytic, Reader: e.st.At(lease)})
	if err != nil {
		t.Fatal(err)
	}
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	if _, done, err := run.Next(10, 0); err != nil || done {
		t.Fatalf("first batch: %v %v", done, err)
	}
	cur, err := e.q.Cursors.Open(run, CursorOptions{NS: c.NS, Kind: "nil.aggregate", Batch: 10, Lease: lease, Own: true})
	if err != nil {
		t.Fatal(err)
	}
	e.st.Release(lease)
	if _, _, _, err := e.q.Cursors.Read(cur.ID, 0, 0); err == nil || err.Error() != "ERR snapshot lease expired" {
		t.Fatalf("read after the lease expired: %v", err)
	}
	if e.q.Cursors.Len() != 0 {
		t.Fatalf("failed cursor stayed open")
	}
}

// TestCursorGateRefusalKeepsCursor: a READ refused by its Gate (a full
// analytics semaphore) fails without closing the cursor, and a retry
// continues where the first batch ended.
func TestCursorGateRefusalKeepsCursor(t *testing.T) {
	e, c := cursorEnv(t, 30)
	p, err := e.q.PlanFind(c, Find{}, Options{Mode: Analytic, Reader: e.st})
	if err != nil {
		t.Fatal(err)
	}
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	if _, done, err := run.Next(10, 0); err != nil || done {
		t.Fatalf("first batch: %v %v", done, err)
	}
	busy := errors.New("ERR too many analytical queries running")
	refuse, held := true, 0
	gate := func() (func(), error) {
		if refuse {
			return nil, busy
		}
		held++
		return func() { held-- }, nil
	}
	cur, err := e.q.Cursors.Open(run, CursorOptions{NS: c.NS, Kind: "nil.aggregate", Batch: 10, Gate: gate})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, done, err := e.q.Cursors.Read(cur.ID, 0, 0); !errors.Is(err, busy) || done || e.q.Cursors.Len() != 1 {
		t.Fatalf("refused read: done %v, err %v, %d cursors open", done, err, e.q.Cursors.Len())
	}
	refuse = false
	_, docs, done, err := e.q.Cursors.Read(cur.ID, 0, 0)
	if err != nil || done || len(docs) != 10 || ids(t, docs)[0] != 10 {
		t.Fatalf("retry: %d documents, done %v, err %v", len(docs), done, err)
	}
	if held != 0 {
		t.Fatalf("gate slot not released after the read")
	}
	e.q.Cursors.Del(cur.ID)
}

func TestCursorSnapshotCap(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	st, err := store.Open(store.Config{Dir: t.TempDir(), Engine: store.TestEngine(), BlockCacheBytes: 8 << 20, AnalyticsCacheBytes: 4 << 20,
		WriteBufferBytes: 16 << 20, Fsync: store.FsyncNo, MaxSnapshots: 2})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(st)
	if err != nil {
		t.Fatal(err)
	}
	e := &tenv{t: t, st: st, cat: cat, ds: docstore.New(st, cat), q: New(st, nil)}
	t.Cleanup(func() { e.q.Close(); st.Close() })
	c := e.coll("test.cap")
	var docs []bson.Raw
	for i := range 10 {
		docs = append(docs, mustMarshal(t, bson.D{{Key: "_id", Value: int32(i)}}))
	}
	e.insert(c, docs...)
	openCursor(t, e, c, Find{}, 2)
	openCursor(t, e, c, Find{}, 2)
	p, err := e.q.PlanFind(c, Find{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run, err := p.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	if _, _, err := run.Next(2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.q.Cursors.Open(run, CursorOptions{NS: c.NS}); err == nil || err.Error() != "ERR too many open snapshots" {
		t.Fatalf("third cursor: %v", err)
	}
}
