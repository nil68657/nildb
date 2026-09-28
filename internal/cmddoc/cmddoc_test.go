package cmddoc_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/cmddoc"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/testutil"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var ctx = context.Background()

// spawn starts a server with the DOC.* commands. The query engine closes
// before the store: t.Cleanup runs last-in first-out, and Spawn registered
// its own Stop first.
func spawn(t *testing.T, mutate func(*config.Config)) *testutil.Env {
	t.Helper()
	var q *query.Engine
	e := testutil.Spawn(t, mutate, func(r *command.Registry, e *testutil.Env) error {
		cat, err := catalog.Open(e.Store)
		if err != nil {
			return err
		}
		ds := docstore.New(e.Store, cat)
		if err := ds.ResumeBuilds(); err != nil {
			return err
		}
		q = query.New(e.Store, e.Cfg)
		cmddoc.Register(r, cat, ds, q)
		return nil
	})
	t.Cleanup(q.Close)
	return e
}

func do(t *testing.T, c *redis.Client, args ...any) any {
	t.Helper()
	v, err := c.Do(ctx, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

func doErr(t *testing.T, c *redis.Client, args ...any) string {
	t.Helper()
	_, err := c.Do(ctx, args...).Result()
	if err == nil {
		t.Fatalf("%v: no error", args)
	}
	return err.Error()
}

func field(t *testing.T, m any, key string) any {
	t.Helper()
	mm, ok := m.(map[any]any)
	if !ok {
		t.Fatalf("reply %v (%T) is not a map", m, m)
	}
	v, ok := mm[key]
	if !ok {
		t.Fatalf("map %v has no %q", m, key)
	}
	return v
}

// cursor splits a RESP3 cursor reply into its id and batch.
func cursor(t *testing.T, reply any) (int64, []string) {
	t.Helper()
	cur := field(t, reply, "cursor")
	batch, ok := cur.(map[any]any)["firstBatch"]
	if !ok {
		batch = field(t, cur, "nextBatch")
	}
	var docs []string
	for _, d := range batch.([]any) {
		docs = append(docs, d.(string))
	}
	return field(t, cur, "id").(int64), docs
}

func bulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

func TestReplyShapesRESP2(t *testing.T) {
	e := spawn(t, nil)
	c := e.TCP(t)
	id := `{"$numberInt":"1"}`
	row := `{"_id":{"$numberInt":"1"},"x":{"$numberInt":"1"}}`
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"DOC.INSERT", "t.a", `{"_id": 1, "x": 1}`}, "*3\r\n:1\r\n*1\r\n" + bulk(id) + "*0\r\n"},
		{[]string{"DOC.INSERT", "t.a", `{"_id": 1}`}, "*3\r\n:0\r\n*0\r\n*1\r\n*3\r\n:0\r\n:11000\r\n" +
			bulk("E11000 duplicate key error collection: t.a index: _id_ dup key: { _id: 1 }")},
		{[]string{"DOC.FIND", "t.a", `{}`}, "*3\r\n:0\r\n" + bulk("t.a") + "*1\r\n" + bulk(row)},
		{[]string{"DOC.GET", "t.a", "1"}, bulk(row)},
		{[]string{"DOC.GET", "t.a", "2"}, "$-1\r\n"},
		{[]string{"DOC.FINDONE", "t.nope", `{}`}, "$-1\r\n"},
		{[]string{"DOC.UPDATE", "t.a", `{"_id": 1}`, `{"$set": {"x": 1}}`}, "*6\r\n" + bulk("n") + ":1\r\n" + bulk("nModified") + ":0\r\n" + bulk("upsertedId") + "$-1\r\n"},
		{[]string{"DOC.UPDATE", "t.a", `{"_id": 9}`, `{"$set": {"x": 2}}`, "UPSERT"}, "*6\r\n" + bulk("n") + ":1\r\n" + bulk("nModified") + ":0\r\n" + bulk("upsertedId") + bulk(`{"$numberInt":"9"}`)},
		{[]string{"DOC.COUNT", "t.a"}, ":2\r\n"},
		{[]string{"DOC.DELETE", "t.a", `{"_id": 9}`}, "*2\r\n" + bulk("n") + ":1\r\n"},
		{[]string{"DOC.DISTINCT", "t.a", "x"}, "*1\r\n" + bulk(`{"$numberInt":"1"}`)},
		{[]string{"DOC.FIND", "t.a", `{"$bad": 1}`}, "-ERR unknown top level operator: $bad\r\n"},
		{[]string{"DOC.INSERT", "t.a", `{"a": `}, "-ERR invalid Extended JSON: unexpected end of JSON input\r\n"},
	}
	for _, tc := range cases {
		if got := string(c.Do(tc.args...)); got != tc.want {
			t.Errorf("%q:\n got %q\nwant %q", tc.args, got, tc.want)
		}
	}
}

func TestReplyShapesRESP3(t *testing.T) {
	e := spawn(t, nil)
	c := e.TCP(t)
	c.Do("HELLO", "3")
	id := `{"$numberInt":"1"}`
	row := `{"_id":{"$numberInt":"1"},"x":{"$numberInt":"1"}}`
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"DOC.INSERT", "t.a", `{"_id": 1, "x": 1}`}, "%3\r\n" + bulk("n") + ":1\r\n" + bulk("ids") + "*1\r\n" + bulk(id) + bulk("writeErrors") + "*0\r\n"},
		{[]string{"DOC.INSERT", "t.a", `{"_id": 1}`}, "%3\r\n" + bulk("n") + ":0\r\n" + bulk("ids") + "*0\r\n" + bulk("writeErrors") + "*1\r\n%3\r\n" +
			bulk("index") + ":0\r\n" + bulk("code") + ":11000\r\n" + bulk("errmsg") + bulk("E11000 duplicate key error collection: t.a index: _id_ dup key: { _id: 1 }")},
		{[]string{"DOC.FIND", "t.a", `{}`}, "%1\r\n" + bulk("cursor") + "%3\r\n" + bulk("id") + ":0\r\n" + bulk("ns") + bulk("t.a") + bulk("firstBatch") + "*1\r\n" + bulk(row)},
		{[]string{"DOC.GET", "t.a", "2"}, "_\r\n"},
		{[]string{"DOC.UPDATE", "t.a", `{"_id": 1}`, `{"$inc": {"x": 1}}`}, "%3\r\n" + bulk("n") + ":1\r\n" + bulk("nModified") + ":1\r\n" + bulk("upsertedId") + "_\r\n"},
		{[]string{"DOC.DELETE", "t.a", `{}`, "MULTI"}, "%1\r\n" + bulk("n") + ":1\r\n"},
	}
	for _, tc := range cases {
		if got := string(c.Do(tc.args...)); got != tc.want {
			t.Errorf("%q:\n got %q\nwant %q", tc.args, got, tc.want)
		}
	}
}

func TestInsertOrdered(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	do(t, c, "DOC.INSERT", "t.o", `{"_id": 1}`)
	r := do(t, c, "DOC.INSERT", "t.o", `{"_id": 2}`, `{"_id": 1}`, `{"_id": 3}`)
	if n := field(t, r, "n"); n != int64(1) {
		t.Fatalf("ordered insert n = %v", n)
	}
	we := field(t, r, "writeErrors").([]any)
	if len(we) != 1 || field(t, we[0], "index") != int64(1) || field(t, we[0], "code") != int64(11000) {
		t.Fatalf("writeErrors = %v", we)
	}
	r = do(t, c, "DOC.INSERT", "t.o", `{"_id": 4}`, `{"_id": 1}`, `{"$bad": 1}`, `{"_id": 5}`, "ORDERED", "no")
	if n := field(t, r, "n"); n != int64(2) {
		t.Fatalf("unordered insert n = %v", n)
	}
	we = field(t, r, "writeErrors").([]any)
	if len(we) != 2 || field(t, we[0], "index") != int64(1) || field(t, we[1], "index") != int64(2) || field(t, we[1], "code") != int64(2) {
		t.Fatalf("writeErrors = %v", we)
	}
	if got := do(t, c, "DOC.COUNT", "t.o"); got != int64(4) {
		t.Fatalf("count = %v, want 4 (ids 1, 2, 4, 5)", got)
	}
	// A duplicate inside one command is caught by the command's own batch.
	r = do(t, c, "DOC.INSERT", "t.o", `{"_id": 6}`, `{"_id": 6}`, "ORDERED", "no")
	if n := field(t, r, "n"); n != int64(1) {
		t.Fatalf("in-batch duplicate: n = %v", n)
	}
}

func TestUpdateSemantics(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	do(t, c, "DOC.INSERT", "t.u", `{"_id": 1, "a": 1, "g": "x"}`, `{"_id": 2, "a": 2, "g": "x"}`, `{"_id": 3, "a": 1, "g": "y"}`)
	r := do(t, c, "DOC.UPDATE", "t.u", `{"g": "x"}`, `{"$set": {"a": 1}}`, "MULTI")
	if field(t, r, "n") != int64(2) || field(t, r, "nModified") != int64(1) {
		t.Fatalf("no-op update: %v, want n 2 nModified 1", r)
	}
	// Upsert with an _id in the filter inserts that _id and the equality
	// fields of the filter.
	r = do(t, c, "DOC.UPDATE", "t.u", `{"_id": 7, "k": "v"}`, `{"$set": {"y": 2}}`, "UPSERT")
	if field(t, r, "upsertedId") != `{"$numberInt":"7"}` {
		t.Fatalf("upsert by _id: %v", r)
	}
	if got := do(t, c, "DOC.GET", "t.u", "7"); got != `{"_id":{"$numberInt":"7"},"k":"v","y":{"$numberInt":"2"}}` {
		t.Fatalf("upserted document = %v", got)
	}
	// Upsert without an _id generates an ObjectId.
	r = do(t, c, "DOC.UPDATE", "t.u", `{"k": "w"}`, `{"$set": {"y": 3}}`, "UPSERT")
	if id := field(t, r, "upsertedId").(string); !strings.HasPrefix(id, `{"$oid":`) {
		t.Fatalf("generated upsertedId = %v", id)
	}
	// Replace keeps _id.
	r = do(t, c, "DOC.REPLACE", "t.u", `{"_id": 3}`, `{"z": true}`)
	if field(t, r, "nModified") != int64(1) {
		t.Fatalf("replace: %v", r)
	}
	if got := do(t, c, "DOC.GET", "t.u", "3"); got != `{"_id":{"$numberInt":"3"},"z":true}` {
		t.Fatalf("replaced = %v", got)
	}
	if msg := doErr(t, c, "DOC.REPLACE", "t.u", `{"_id": 3}`, `{"$set": {"z": 1}}`); msg != "ERR replacement document must not contain update operators" {
		t.Fatalf("replace with operators: %q", msg)
	}
	if msg := doErr(t, c, "DOC.UPDATE", "t.u", `{}`, `{"z": 1}`, "MULTI"); msg != "ERR multi update is not supported for replacement-style update" {
		t.Fatalf("multi replacement: %q", msg)
	}
	if msg := doErr(t, c, "DOC.UPDATE", "t.u", `{"_id": 1}`, `{"$set": {"_id": 5}}`); msg != "ERR Performing an update on the path '_id' would modify the immutable field '_id'" {
		t.Fatalf("_id change: %q", msg)
	}
	r = do(t, c, "DOC.DELETE", "t.u", `{"a": 1}`, "MULTI")
	if field(t, r, "n") != int64(2) {
		t.Fatalf("delete multi: %v", r)
	}
	r = do(t, c, "DOC.UPDATE", "t.none", `{}`, `{"$set": {"a": 1}}`)
	if field(t, r, "n") != int64(0) {
		t.Fatalf("update on a missing namespace: %v", r)
	}
}

func TestUniqueThroughWire(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	if got := do(t, c, "DOC.CREATEINDEX", "t.q", `{"u": 1}`, "UNIQUE"); got != "u_1" {
		t.Fatalf("CREATEINDEX = %v", got)
	}
	do(t, c, "DOC.INSERT", "t.q", `{"_id": 1, "u": 5}`, `{"_id": 2, "u": 6}`)
	r := do(t, c, "DOC.INSERT", "t.q", `{"_id": 3, "u": 5}`)
	we := field(t, r, "writeErrors").([]any)
	if len(we) != 1 || field(t, we[0], "errmsg") != "E11000 duplicate key error collection: t.q index: u_1 dup key: { u: 5 }" {
		t.Fatalf("writeErrors = %v", we)
	}
	if msg := doErr(t, c, "DOC.UPDATE", "t.q", `{"_id": 2}`, `{"$set": {"u": 5}}`); msg != "E11000 duplicate key error collection: t.q index: u_1 dup key: { u: 5 }" {
		t.Fatalf("update to a duplicate: %q", msg)
	}
	// A MULTI update that would create a duplicate writes nothing.
	if msg := doErr(t, c, "DOC.UPDATE", "t.q", `{}`, `{"$set": {"u": 9}}`, "MULTI"); !strings.HasPrefix(msg, "E11000") {
		t.Fatalf("multi update to duplicates: %q", msg)
	}
	if got := do(t, c, "DOC.FINDONE", "t.q", `{"_id": 1}`); got != `{"_id":{"$numberInt":"1"},"u":{"$numberInt":"5"}}` {
		t.Fatalf("failed multi update changed a document: %v", got)
	}
	// Concurrent inserts of one unique value: exactly one wins.
	var wg sync.WaitGroup
	wins := make(chan int, 16)
	clients := make([]*redis.Client, 16)
	for i := range clients {
		clients[i] = e.Client(t, 2)
	}
	for i, cl := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cl.Do(ctx, "DOC.INSERT", "t.q", fmt.Sprintf(`{"_id": %d, "u": 77}`, 100+i)).Slice()
			if err == nil && res[0] == int64(1) {
				wins <- i
			}
		}()
	}
	wg.Wait()
	close(wins)
	if n := len(wins); n != 1 {
		t.Fatalf("%d concurrent inserts of one unique value succeeded, want 1", n)
	}
}

func TestCursorPaging(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	var docs []any
	for i := range 250 {
		docs = append(docs, fmt.Sprintf(`{"_id": %d}`, i))
	}
	do(t, c, append([]any{"DOC.INSERT", "t.p"}, docs...)...)
	id, first := cursor(t, do(t, c, "DOC.FIND", "t.p", `{}`, "BATCH", "100"))
	if id == 0 || len(first) != 100 {
		t.Fatalf("first batch: id %d, %d documents", id, len(first))
	}
	next, second := cursor(t, do(t, c, "DOC.CURSOR", "READ", id, "COUNT", "100"))
	if next != id || len(second) != 100 {
		t.Fatalf("second batch: id %d, %d documents", next, len(second))
	}
	list := do(t, c, "DOC.CURSOR", "LIST").([]any)
	if len(list) != 1 || field(t, list[0], "id") != id || field(t, list[0], "kind") != "find" {
		t.Fatalf("LIST = %v", list)
	}
	next, third := cursor(t, do(t, c, "DOC.CURSOR", "READ", id))
	if next != 0 || len(third) != 50 {
		t.Fatalf("last batch: id %d, %d documents", next, len(third))
	}
	all := append(append(first, second...), third...)
	for i, d := range all {
		if want := fmt.Sprintf(`{"_id":{"$numberInt":"%d"}}`, i); d != want {
			t.Fatalf("document %d = %s", i, d)
		}
	}
	if msg := doErr(t, c, "DOC.CURSOR", "READ", id); msg != fmt.Sprintf("ERR cursor id %d not found", id) {
		t.Fatalf("read after exhaustion: %q", msg)
	}
	id, _ = cursor(t, do(t, c, "DOC.FIND", "t.p", `{}`, "BATCH", "10"))
	if do(t, c, "DOC.CURSOR", "DEL", id) != int64(1) || do(t, c, "DOC.CURSOR", "DEL", id) != int64(0) {
		t.Fatalf("DEL")
	}
	// A negative LIMIT returns one batch and no cursor.
	if id, got := cursor(t, do(t, c, "DOC.FIND", "t.p", `{}`, "LIMIT", "-7")); id != 0 || len(got) != 7 {
		t.Fatalf("LIMIT -7: id %d, %d documents", id, len(got))
	}
	if msg := doErr(t, c, "DOC.CURSOR", "NOPE"); msg != "ERR unknown subcommand 'NOPE'. Try DOC.CURSOR HELP." {
		t.Fatalf("unknown subcommand: %q", msg)
	}
}

func TestMultiExec(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	var cmds []*redis.Cmd
	_, err := c.TxPipelined(ctx, func(p redis.Pipeliner) error {
		cmds = append(cmds,
			p.Do(ctx, "DOC.INSERT", "t.m", `{"_id": 1, "a": 1}`, `{"_id": 2, "a": 2}`),
			p.Do(ctx, "DOC.UPDATE", "t.m", `{"_id": 1}`, `{"$inc": {"a": 10}}`),
			p.Do(ctx, "DOC.FIND", "t.m", `{"a": {"$gt": 5}}`),
			p.Do(ctx, "DOC.COUNT", "t.m"),
			p.Do(ctx, "DOC.DELETE", "t.m", `{"a": 2}`),
		)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := field(t, cmds[0].Val(), "n"); n != int64(2) {
		t.Fatalf("insert inside EXEC: %v", cmds[0].Val())
	}
	if m := field(t, cmds[1].Val(), "nModified"); m != int64(1) {
		t.Fatalf("update inside EXEC saw no document: %v", cmds[1].Val())
	}
	if _, docs := cursor(t, cmds[2].Val()); len(docs) != 1 || docs[0] != `{"_id":{"$numberInt":"1"},"a":{"$numberInt":"11"}}` {
		t.Fatalf("find inside EXEC did not read its own writes: %v", docs)
	}
	if cmds[3].Val() != int64(2) {
		t.Fatalf("count inside EXEC = %v", cmds[3].Val())
	}
	if do(t, c, "DOC.COUNT", "t.m") != int64(1) {
		t.Fatalf("EXEC did not commit")
	}
	raw := e.TCP(t)
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"MULTI"}, "+OK\r\n"},
		{[]string{"DOC.CREATEINDEX", "t.m", `{"a": 1}`}, "-ERR Command not allowed inside a transaction\r\n"},
		{[]string{"EXEC"}, "-EXECABORT Transaction discarded because of previous errors.\r\n"},
	} {
		if got := string(raw.Do(step.args...)); got != step.want {
			t.Fatalf("%q = %q, want %q", step.args, got, step.want)
		}
	}
}

func TestExplainUsesIndex(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	for i := range 50 {
		do(t, c, "DOC.INSERT", "t.e", fmt.Sprintf(`{"_id": %d, "a": %d}`, i, i%10))
	}
	if got := do(t, c, "DOC.CREATEINDEX", "t.e", `{"a": 1}`); got != "a_1" {
		t.Fatalf("CREATEINDEX = %v", got)
	}
	ex := do(t, c, "DOC.FIND", "t.e", `{"a": 5}`, "EXPLAIN")
	if field(t, ex, "plan") != "index" || field(t, ex, "index") != "a_1" {
		t.Fatalf("EXPLAIN = %v", ex)
	}
	if b := field(t, ex, "bounds").([]any); len(b) != 1 || b[0] != "a: [5, 5]" {
		t.Fatalf("bounds = %v", b)
	}
	if _, docs := cursor(t, do(t, c, "DOC.FIND", "t.e", `{"a": 5}`)); len(docs) != 5 {
		t.Fatalf("index find returned %d documents", len(docs))
	}
	ex = do(t, c, "DOC.AGGREGATE", "t.e", `[{"$match": {"a": {"$gt": 7}}}, {"$group": {"_id": "$a"}}]`, "EXPLAIN")
	if field(t, ex, "index") != "a_1" {
		t.Fatalf("aggregate EXPLAIN = %v", ex)
	}
	ex = do(t, c, "DOC.FIND", "t.e", `{"b": 5}`, "EXPLAIN")
	if field(t, ex, "plan") != "rowscan" || field(t, ex, "estimatedRows") != int64(50) {
		t.Fatalf("rowscan EXPLAIN = %v", ex)
	}
}

func TestNearThroughWire(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	if got := do(t, c, "DOC.CREATEINDEX", "t.g", `{"loc": "2dsphere"}`); got != "loc_2dsphere" {
		t.Fatalf("CREATEINDEX = %v", got)
	}
	for i, p := range [][2]float64{{0, 0}, {0.5, 0}, {0.1, 0}, {2, 0}, {0.3, 0.01}} {
		do(t, c, "DOC.INSERT", "t.g", fmt.Sprintf(`{"_id": %d, "loc": {"type": "Point", "coordinates": [%v, %v]}}`, i, p[0], p[1]))
	}
	_, docs := cursor(t, do(t, c, "DOC.FIND", "t.g", `{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}, "$maxDistance": 100000}}}`, "PROJECT", `{"_id": 1}`))
	want := []string{`{"_id":{"$numberInt":"0"}}`, `{"_id":{"$numberInt":"2"}}`, `{"_id":{"$numberInt":"4"}}`, `{"_id":{"$numberInt":"1"}}`}
	if !slices.Equal(docs, want) {
		t.Fatalf("$near = %v, want %v", docs, want)
	}
	if msg := doErr(t, c, "DOC.FIND", "t.none", `{"x": {"$near": [0, 0]}}`); !strings.Contains(msg, "2d index") {
		t.Fatalf("legacy $near: %q", msg)
	}
	_, docs = cursor(t, do(t, c, "DOC.AGGREGATE", "t.g", `[{"$geoNear": {"near": {"type": "Point", "coordinates": [0, 0]}, "distanceField": "d", "maxDistance": 20000}}, {"$project": {"d": 1}}]`))
	if len(docs) != 2 || docs[0] != `{"_id":{"$numberInt":"0"},"d":{"$numberDouble":"0.0"}}` {
		t.Fatalf("$geoNear = %v", docs)
	}
}

func TestAggregateGroup(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 2)
	for i := range 30 {
		do(t, c, "DOC.INSERT", "t.agg", fmt.Sprintf(`{"_id": %d, "k": "k%d", "v": %d}`, i, i%3, i))
	}
	res := do(t, c, "DOC.AGGREGATE", "t.agg", `[{"$group": {"_id": "$k", "s": {"$sum": "$v"}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]`).([]any)
	docs := res[2].([]any)
	want := []any{
		`{"_id":"k0","s":{"$numberInt":"135"},"n":{"$numberInt":"10"}}`,
		`{"_id":"k1","s":{"$numberInt":"145"},"n":{"$numberInt":"10"}}`,
		`{"_id":"k2","s":{"$numberInt":"155"},"n":{"$numberInt":"10"}}`,
	}
	if res[0] != int64(0) || res[1] != "t.agg" || !slices.Equal(docs, want) {
		t.Fatalf("aggregate = %v", res)
	}
	msg := doErr(t, c, "DOC.AGGREGATE", "t.agg", `[{"$group": {"_id": "$_id", "all": {"$push": "$$ROOT"}}}]`, "MAXMEM", "2000")
	if msg != "ERR $group exceeded memory limit of 2000 bytes (allowDiskUse is not supported)" {
		t.Fatalf("MAXMEM: %q", msg)
	}
	if msg := doErr(t, c, "DOC.AGGREGATE", "t.agg", `{"$match": {}}`); msg != "ERR the pipeline must be a JSON array of stage documents" {
		t.Fatalf("pipeline not an array: %q", msg)
	}
}

func TestDDL(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	if do(t, c, "DOC.CREATE", "shop.orders") != "OK" {
		t.Fatalf("CREATE")
	}
	if msg := doErr(t, c, "DOC.CREATE", "shop.orders"); msg != "ERR Collection already exists. NS: shop.orders" {
		t.Fatalf("CREATE twice: %q", msg)
	}
	do(t, c, "DOC.INSERT", "shop.items", `{"_id": 1}`)
	do(t, c, "DOC.INSERT", "other", `{"_id": 1}`)
	if got := do(t, c, "DOC.COLLECTIONS", "shop"); !slices.Equal(got.([]any), []any{"shop.items", "shop.orders"}) {
		t.Fatalf("COLLECTIONS shop = %v", got)
	}
	if got := do(t, c, "DOC.COLLECTIONS"); len(got.([]any)) != 3 {
		t.Fatalf("COLLECTIONS = %v", got)
	}
	for _, tc := range []struct {
		args []any
		want string
	}{
		{[]any{"DOC.CREATEINDEX", "shop.orders", `{"amount": "columnar"}`}, "ERR columnar indexes are not supported in v1"},
		{[]any{"DOC.CREATEINDEX", "shop.orders", `{"at": 1}`, "EXPIREAFTERSECONDS", "60"}, "ERR TTL indexes are not supported in v1"},
		{[]any{"DOC.CREATEINDEX", "shop.orders", `{"a": 2}`}, `ERR bad index key pattern: field 'a' must be 1, -1 or "2dsphere"`},
		{[]any{"DOC.CREATEINDEX", "shop.orders", `{"a": 1}`, "S2", "1", "2", "3"}, "ERR S2 applies to 2dsphere indexes only"},
		{[]any{"DOC.CREATEINDEX", "shop.orders", `{"loc": "2dsphere", "a": 1}`}, "ERR compound 2dsphere indexes are not supported in v1"},
		{[]any{"DOC.DROPINDEX", "shop.orders", "_id_"}, "ERR cannot drop _id index"},
		{[]any{"DOC.DROPINDEX", "shop.orders", "nope"}, "ERR index not found with name [nope]"},
		{[]any{"DOC.INDEXES", "shop.none"}, "ERR ns does not exist: shop.none"},
		{[]any{"DOC.STATS", "shop.none"}, "ERR ns does not exist: shop.none"},
	} {
		if msg := doErr(t, c, tc.args...); msg != tc.want {
			t.Errorf("%v: %q, want %q", tc.args, msg, tc.want)
		}
	}
	if got := do(t, c, "DOC.CREATEINDEX", "shop.orders", `{"a": 1, "b": -1}`, "NAME", "ab", "SPARSE"); got != "ab" {
		t.Fatalf("CREATEINDEX NAME = %v", got)
	}
	if got := do(t, c, "DOC.CREATEINDEX", "shop.orders", `{"loc": "2dsphere"}`, "S2", "3", "15", "12"); got != "loc_2dsphere" {
		t.Fatalf("CREATEINDEX S2 = %v", got)
	}
	if got := do(t, c, "DOC.CREATEINDEX", "shop.orders", `{"_id": 1}`); got != "_id_" {
		t.Fatalf("CREATEINDEX _id = %v", got)
	}
	idx := do(t, c, "DOC.INDEXES", "shop.orders").([]any)
	if len(idx) != 3 || field(t, idx[0], "name") != "_id_" || field(t, idx[1], "key") != `{"a":1,"b":-1}` ||
		field(t, idx[1], "sparse") != true || field(t, idx[2], "finest") != int64(15) || field(t, idx[2], "state") != "ready" {
		t.Fatalf("INDEXES = %v", idx)
	}
	st := do(t, c, "DOC.STATS", "shop.orders")
	if field(t, st, "count") != int64(0) || field(t, st, "nindexes") != int64(3) {
		t.Fatalf("STATS = %v", st)
	}
	if do(t, c, "DOC.DROPINDEX", "shop.orders", "*") != "OK" || len(do(t, c, "DOC.INDEXES", "shop.orders").([]any)) != 1 {
		t.Fatalf("DROPINDEX *")
	}
	if do(t, c, "DOC.DROP", "shop.orders") != int64(1) || do(t, c, "DOC.DROP", "shop.orders") != int64(0) {
		t.Fatalf("DROP")
	}
}

func TestReadsAndFormats(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	do(t, c, "DOC.INSERT", "t.r", `{"_id": {"$oid": "65f0a1b2c3d4e5f607182930"}, "n": 1, "tags": ["a", "b"]}`,
		`{"_id": "abc", "n": 2, "tags": ["b", "c"]}`, `{"_id": 3, "n": 2.0}`)
	if got := do(t, c, "DOC.GET", "t.r", `{"$oid": "65f0a1b2c3d4e5f607182930"}`); !strings.HasPrefix(got.(string), `{"_id":{"$oid":"65f0a1b2c3d4e5f607182930"}`) {
		t.Fatalf("GET by ObjectId = %v", got)
	}
	if got := do(t, c, "DOC.GET", "t.r", "abc"); !strings.HasPrefix(got.(string), `{"_id":"abc"`) {
		t.Fatalf("GET by bare string = %v", got)
	}
	if got := do(t, c, "DOC.COUNT", "t.r", `{"n": 2}`); got != int64(2) {
		t.Fatalf("COUNT {n: 2} = %v", got)
	}
	if got := do(t, c, "DOC.DISTINCT", "t.r", "tags"); !slices.Equal(got.([]any), []any{`"a"`, `"b"`, `"c"`}) {
		t.Fatalf("DISTINCT tags = %v", got)
	}
	if got := do(t, c, "DOC.DISTINCT", "t.r", "n", `{"_id": {"$ne": "abc"}}`); len(got.([]any)) != 2 {
		t.Fatalf("DISTINCT n = %v", got)
	}
	_, docs := cursor(t, do(t, c, "DOC.FIND", "t.r", `{"_id": 3}`, "FORMAT", "BSON"))
	var d bson.D
	if err := bson.Unmarshal([]byte(docs[0]), &d); err != nil || d[0].Value != int32(3) {
		t.Fatalf("FORMAT BSON returned %q (%v)", docs[0], err)
	}
	if got := do(t, c, "DOC.FINDONE", "t.r", `{}`, "SORT", `{"n": -1, "_id": 1}`, "PROJECT", `{"n": 1}`); got != `{"_id":{"$numberInt":"3"},"n":{"$numberDouble":"2.0"}}` {
		t.Fatalf("FINDONE sorted = %v", got)
	}
	if msg := doErr(t, c, "DOC.FIND", "t.r", `{}`, "AT", "999"); msg != "ERR snapshot lease expired" {
		t.Fatalf("AT unknown lease: %q", msg)
	}
	if msg := doErr(t, c, "DOC.FIND", "t.r", `{}`, "BOGUS", "1"); msg != "ERR syntax error" {
		t.Fatalf("unknown option: %q", msg)
	}
}
