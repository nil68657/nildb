package analytics_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/analytics"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/cmddoc"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"github.com/nil68657/nildb/internal/testutil"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var ctx = context.Background()

// spawn starts a server with DOC.*, NIL.* and the test SET and GET. The
// query engine closes before the store: t.Cleanup runs last-in first-out,
// and Spawn registered its own Stop first.
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
		analytics.Register(r, e.Server, e.Cfg, cat, q)
		r.Register(kvSpecs()...)
		return nil
	})
	t.Cleanup(q.Close)
	return e
}

// kvSpecs are a minimal SET key value [PX ms] and GET over layout's string
// metadata, so these tests time point operations and fill the meta column
// family without the Redis command package.
func kvSpecs() []command.Spec {
	return []command.Spec{
		{Name: "set", Arity: -3, Flags: command.Write, FirstKey: 1, LastKey: 1, KeyStep: 1,
			Run: func(c *command.Ctx, args [][]byte) resp.Reply {
				m := layout.Meta{Type: layout.TString, Payload: args[2]}
				if len(args) == 5 {
					ms, err := strconv.ParseInt(string(args[4]), 10, 64)
					if err != nil {
						return resp.ErrNotInteger
					}
					m.ExpireMS = c.NowMS() + ms
				}
				c.Txn.Put(store.CFMeta, layout.MetaKey(nil, c.DB, args[1]), m.Encode(nil))
				return resp.OK()
			}},
		{Name: "get", Arity: 2, Flags: command.ReadOnly | command.Fast, FirstKey: 1, LastKey: 1, KeyStep: 1,
			Run: func(c *command.Ctx, args [][]byte) resp.Reply {
				v, ok, err := c.Reader().Get(store.CFMeta, layout.MetaKey(nil, c.DB, args[1]))
				if err != nil {
					return resp.Err("ERR " + err.Error())
				}
				if !ok {
					return resp.Null()
				}
				m, err := layout.DecodeMeta(v)
				if err != nil {
					return resp.Err("ERR " + err.Error())
				}
				return resp.Bulk(m.Payload)
			}},
	}
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

// drain runs a command that returns a cursor and reads it to the end.
func drain(t *testing.T, c *redis.Client, args ...any) []string {
	t.Helper()
	id, docs := cursor(t, do(t, c, args...))
	for id != 0 {
		var more []string
		id, more = cursor(t, do(t, c, "DOC.CURSOR", "READ", id))
		docs = append(docs, more...)
	}
	return docs
}

var pad = strings.Repeat("p", 200)

// row is document i of the test collections: about 280 bytes of BSON.
func row(i int) string {
	return fmt.Sprintf(`{"_id": %d, "k": "k%d", "v": %d, "tags": ["t%d", "t%d"], "pad": %q}`, i, i%7, i%100, i%3, i%5, pad)
}

// load inserts documents from through from+n-1 with DOC.INSERT, 1000 per
// command.
func load(t *testing.T, c *redis.Client, ns string, from, n int, gen func(int) string) {
	t.Helper()
	for lo := from; lo < from+n; lo += 1000 {
		args := []any{"DOC.INSERT", ns}
		for i := lo; i < min(lo+1000, from+n); i++ {
			args = append(args, gen(i))
		}
		if we := field(t, do(t, c, args...), "writeErrors").([]any); len(we) != 0 {
			t.Fatalf("load %s: %v", ns, we[0])
		}
	}
}

func infoField(t *testing.T, c *redis.Client, name string) int64 {
	t.Helper()
	s := c.Info(ctx, "analytics").Val()
	for line := range strings.SplitSeq(s, "\r\n") {
		if v, ok := strings.CutPrefix(line, name+":"); ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatalf("INFO %s = %q", name, v)
			}
			return n
		}
	}
	t.Fatalf("INFO analytics has no %s:\n%s", name, s)
	return 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestMatchesDocAggregate(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.x", 0, 3000, row)
	for _, p := range []string{
		`[{"$group": {"_id": "$k", "n": {"$sum": 1}, "s": {"$sum": "$v"}, "avg": {"$avg": "$v"}}}, {"$sort": {"_id": 1}}]`,
		`[{"$match": {"v": {"$gte": 90}}}, {"$sort": {"v": -1, "_id": 1}}, {"$limit": 25}]`,
		`[{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]`,
		`[{"$project": {"k": 1, "twice": {"$multiply": ["$v", 2]}}}, {"$skip": 2900}]`,
		`[{"$match": {"k": "k3"}}, {"$count": "n"}]`,
		`[{"$match": {"tags": "t4"}}]`,
	} {
		want := drain(t, c, "DOC.AGGREGATE", "t.x", p, "BATCH", "40")
		got := drain(t, c, "NIL.AGGREGATE", "t.x", p, "BATCH", "40")
		if len(want) == 0 || !slices.Equal(got, want) {
			t.Errorf("%s:\nNIL.AGGREGATE %d documents, DOC.AGGREGATE %d; first %v vs %v", p, len(got), len(want), got[:min(len(got), 1)], want[:min(len(want), 1)])
		}
	}
	for _, f := range []string{`{}`, `{"v": {"$lt": 10}}`, `{"tags": "t2"}`} {
		if got, want := do(t, c, "NIL.COUNT", "t.x", f), do(t, c, "DOC.COUNT", "t.x", f); got != want {
			t.Errorf("NIL.COUNT %s = %v, DOC.COUNT %v", f, got, want)
		}
	}
	if got := do(t, c, "NIL.COUNT", "t.x"); got != int64(3000) {
		t.Errorf("NIL.COUNT without a filter = %v", got)
	}
	for _, args := range [][]any{{"k"}, {"tags", `{"v": {"$gt": 50}}`}} {
		got := do(t, c, append([]any{"NIL.DISTINCT", "t.x"}, args...)...)
		want := do(t, c, append([]any{"DOC.DISTINCT", "t.x"}, args...)...)
		if !reflect.DeepEqual(got, want) || len(want.([]any)) == 0 {
			t.Errorf("NIL.DISTINCT %v = %v, DOC.DISTINCT %v", args, got, want)
		}
	}
	_, docs := cursor(t, do(t, c, "NIL.AGGREGATE", "t.x", `[{"$match": {"_id": 5}}]`, "FORMAT", "BSON"))
	var d bson.D
	if err := bson.Unmarshal([]byte(docs[0]), &d); err != nil || d[0].Value != int32(5) {
		t.Errorf("FORMAT BSON returned %q (%v)", docs[0], err)
	}
	if id, docs := cursor(t, do(t, c, "NIL.AGGREGATE", "t.none", `[]`)); id != 0 || len(docs) != 0 {
		t.Errorf("missing collection: id %d, %d documents", id, len(docs))
	}
	raw := e.TCP(t)
	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"MULTI"}, "+OK\r\n"},
		{[]string{"NIL.COUNT", "t.x"}, "-ERR Command not allowed inside a transaction\r\n"},
		{[]string{"DISCARD"}, "+OK\r\n"},
	} {
		if got := string(raw.Do(step.args...)); got != step.want {
			t.Errorf("%q = %q, want %q", step.args, got, step.want)
		}
	}
}

func TestFreshness(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	for i := 1; i <= 20; i++ {
		do(t, c, "DOC.INSERT", "t.f", fmt.Sprintf(`{"_id": %d, "marker": true}`, i))
		_, docs := cursor(t, do(t, c, "NIL.AGGREGATE", "t.f", `[{"$match": {"marker": true}}, {"$count": "n"}]`))
		if want := fmt.Sprintf(`{"n":{"$numberInt":"%d"}}`, i); len(docs) != 1 || docs[0] != want {
			t.Fatalf("after insert %d NIL.AGGREGATE saw %v", i, docs)
		}
		if n := do(t, c, "NIL.COUNT", "t.f", `{"marker": true}`); n != int64(i) {
			t.Fatalf("after insert %d NIL.COUNT = %v", i, n)
		}
	}
}

func TestLeaseIsolation(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.l", 0, 100, row)
	l := do(t, c, "NIL.SNAPSHOT", "CREATE")
	id := field(t, l, "id").(int64)
	if owner := field(t, l, "owner").(string); !strings.HasPrefix(owner, "NIL.SNAPSHOT conn ") {
		t.Errorf("owner = %q", owner)
	}
	load(t, c, "t.l", 100, 50, row)
	do(t, c, "DOC.DELETE", "t.l", `{"_id": {"$lt": 10}}`, "MULTI")
	for _, tc := range []struct {
		args []any
		want int64
	}{
		{[]any{"NIL.COUNT", "t.l", "AT", id}, 100},
		{[]any{"NIL.COUNT", "t.l", `{}`, "AT", id}, 100},
		{[]any{"NIL.COUNT", "t.l", `{"_id": {"$lt": 10}}`, "AT", id}, 10},
		{[]any{"NIL.COUNT", "t.l"}, 140},
		{[]any{"NIL.COUNT", "t.l", `{"_id": {"$lt": 10}}`}, 0},
	} {
		if got := do(t, c, tc.args...); got != tc.want {
			t.Errorf("%v = %v, want %d", tc.args, got, tc.want)
		}
	}
	_, docs := cursor(t, do(t, c, "NIL.AGGREGATE", "t.l", `[{"$count": "n"}]`, "AT", id))
	if len(docs) != 1 || docs[0] != `{"n":{"$numberInt":"100"}}` {
		t.Errorf("NIL.AGGREGATE AT = %v", docs)
	}
	if got := drain(t, c, "DOC.FIND", "t.l", `{}`, "AT", id); len(got) != 100 {
		t.Errorf("DOC.FIND AT returned %d documents", len(got))
	}
	if got := do(t, c, "NIL.DISTINCT", "t.l", "_id", `{"_id": {"$gte": 145}}`, "AT", id); len(got.([]any)) != 0 {
		t.Errorf("NIL.DISTINCT AT saw later inserts: %v", got)
	}
	if got := do(t, c, "NIL.DISTINCT", "t.l", "_id", `{"_id": {"$gte": 145}}`); len(got.([]any)) != 5 {
		t.Errorf("NIL.DISTINCT = %v", got)
	}
	list := do(t, c, "NIL.SNAPSHOT", "LIST").([]any)
	if len(list) != 1 || field(t, list[0], "id") != id {
		t.Errorf("LIST = %v", list)
	}
	if do(t, c, "NIL.SNAPSHOT", "RELEASE", id) != int64(1) || do(t, c, "NIL.SNAPSHOT", "RELEASE", id) != int64(0) {
		t.Errorf("RELEASE")
	}
	if msg := doErr(t, c, "NIL.COUNT", "t.l", "AT", id); msg != "ERR snapshot lease expired" {
		t.Errorf("AT a released lease: %q", msg)
	}
}

func TestLeaseExpiryAndLimits(t *testing.T) {
	e := spawn(t, func(cfg *config.Config) { cfg.MaxSnapshots = 3 })
	c := e.Client(t, 3)
	load(t, c, "t.x", 0, 10, row)
	l := do(t, c, "NIL.SNAPSHOT", "CREATE", "TTL", "10")
	if d := field(t, l, "expires_ms").(int64) - field(t, l, "created_ms").(int64); d != 10_000 {
		t.Errorf("TTL 10 lease lives %d ms", d)
	}
	e.Clock.Advance(11 * time.Second)
	if msg := doErr(t, c, "NIL.AGGREGATE", "t.x", `[]`, "AT", field(t, l, "id")); msg != "ERR snapshot lease expired" {
		t.Errorf("AT an expired lease: %q", msg)
	}
	if got := do(t, c, "NIL.SNAPSHOT", "LIST").([]any); len(got) != 0 {
		t.Errorf("LIST after expiry = %v", got)
	}
	leaseMax := int64(e.Cfg.LeaseMax / time.Second)
	for _, tc := range []struct {
		args []any
		want string
	}{
		{[]any{"CREATE", "TTL", leaseMax + 1}, fmt.Sprintf("ERR TTL %d is above nildb.lease-max of %d seconds", leaseMax+1, leaseMax)},
		{[]any{"CREATE", "TTL", "0"}, "ERR TTL must be a positive number of seconds"},
		{[]any{"CREATE", "TTL"}, "ERR syntax error"},
		{[]any{"CREATE", "FOR", "5"}, "ERR syntax error"},
		{[]any{"RELEASE", "abc"}, "ERR value is not an integer or out of range"},
		{[]any{"NOPE"}, "ERR unknown subcommand 'NOPE'. Try NIL.SNAPSHOT HELP."},
	} {
		if msg := doErr(t, c, append([]any{"NIL.SNAPSHOT"}, tc.args...)...); msg != tc.want {
			t.Errorf("NIL.SNAPSHOT %v: %q, want %q", tc.args, msg, tc.want)
		}
	}
	for range 3 {
		do(t, c, "NIL.SNAPSHOT", "CREATE")
	}
	if msg := doErr(t, c, "NIL.SNAPSHOT", "CREATE"); msg != "ERR too many open snapshots" {
		t.Errorf("fourth lease: %q", msg)
	}
	if msg := doErr(t, c, "NIL.COUNT", "t.x", `{"v": 1}`); msg != "ERR too many open snapshots" {
		t.Errorf("NIL.COUNT with no lease slot: %q", msg)
	}
	if help := do(t, c, "NIL.SNAPSHOT", "HELP").([]any); len(help) == 0 || !strings.HasPrefix(help[0].(string), "NIL.SNAPSHOT <subcommand>") {
		t.Errorf("HELP = %v", help)
	}
}

func TestCursorKeepsLease(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.c", 0, 300, row)
	id, first := cursor(t, do(t, c, "NIL.AGGREGATE", "t.c", `[{"$project": {"_id": 1}}]`, "BATCH", "100"))
	if id == 0 || len(first) != 100 {
		t.Fatalf("first batch: id %d, %d documents", id, len(first))
	}
	if list := do(t, c, "DOC.CURSOR", "LIST").([]any); len(list) != 1 || field(t, list[0], "kind") != "nil.aggregate" {
		t.Fatalf("DOC.CURSOR LIST = %v", list)
	}
	leases := do(t, c, "NIL.SNAPSHOT", "LIST").([]any)
	if len(leases) != 1 || !strings.HasPrefix(field(t, leases[0], "owner").(string), "NIL.AGGREGATE conn ") {
		t.Fatalf("NIL.SNAPSHOT LIST = %v", leases)
	}
	if n := field(t, do(t, c, "DOC.DELETE", "t.c", `{}`, "MULTI"), "n"); n != int64(300) {
		t.Fatalf("delete = %v", n)
	}
	var rest []string
	for id != 0 {
		var more []string
		id, more = cursor(t, do(t, c, "DOC.CURSOR", "READ", id))
		rest = append(rest, more...)
	}
	if len(rest) != 200 || rest[0] != `{"_id":{"$numberInt":"100"}}` {
		t.Fatalf("cursor after the delete read %d documents, first %v", len(rest), rest[:min(1, len(rest))])
	}
	if got := do(t, c, "NIL.SNAPSHOT", "LIST").([]any); len(got) != 0 {
		t.Errorf("exhausted cursor kept its lease: %v", got)
	}
	if n := infoField(t, c, "analytics_open_cursors"); n != 0 {
		t.Errorf("open cursors = %d", n)
	}
}

func TestSemaphoreAndThrottle(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.s", 0, 2000, row)
	for _, kv := range [][2]string{
		{"nildb.analytics-max-concurrent", "1"},
		{"nildb.analytics-queue-timeout", "100ms"},
		{"nildb.analytics-read-bps", "300000"},
	} {
		do(t, c, "CONFIG", "SET", kv[0], kv[1])
	}
	slow := e.Client(t, 3)
	type result struct {
		v   any
		err error
		d   time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		v, err := slow.Do(ctx, "NIL.AGGREGATE", "t.s", `[{"$group": {"_id": null, "n": {"$sum": 1}}}]`).Result()
		done <- result{v, err, time.Since(start)}
	}()
	waitFor(t, "the throttled scan to start", func() bool { return infoField(t, c, "analytics_running") == 1 })
	if msg := doErr(t, c, "NIL.COUNT", "t.s", `{"v": 1}`); msg != "ERR too many analytical queries running (nildb.analytics-max-concurrent is 1)" {
		t.Fatalf("second query: %q", msg)
	}
	if n := infoField(t, c, "analytics_rejections"); n != 1 {
		t.Errorf("rejections = %d", n)
	}
	// With a long queue timeout the next query waits for the slot.
	do(t, c, "CONFIG", "SET", "nildb.analytics-queue-timeout", "30s")
	queued := e.Client(t, 3)
	waiter := make(chan any, 1)
	go func() { waiter <- queued.Do(ctx, "NIL.COUNT", "t.s", `{"v": 1}`).Val() }()
	waitFor(t, "the second query to queue", func() bool { return infoField(t, c, "analytics_queued") == 1 })
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, docs := cursor(t, r.v); len(docs) != 1 || docs[0] != `{"_id":null,"n":{"$numberInt":"2000"}}` {
		t.Fatalf("throttled scan = %v", docs)
	}
	// 2000 documents of about 280 bytes at 300,000 bytes/s.
	if r.d < time.Second {
		t.Errorf("throttled scan took %v", r.d)
	}
	if n := <-waiter; n != int64(20) {
		t.Errorf("queued NIL.COUNT = %v", n)
	}
	if n := infoField(t, c, "analytics_semaphore_waits"); n < 1 {
		t.Errorf("semaphore waits = %d", n)
	}
	if ms := infoField(t, c, "analytics_throttle_sleep_ms"); ms < 500 {
		t.Errorf("throttle sleep = %d ms", ms)
	}
}

func TestTimeoutAndRowCap(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.x", 0, 2000, row)
	do(t, c, "CONFIG", "SET", "nildb.analytics-read-bps", "20000")
	start := time.Now()
	if msg := doErr(t, c, "NIL.AGGREGATE", "t.x", `[{"$group": {"_id": "$k", "n": {"$sum": 1}}}]`, "TIMEOUT", "300"); msg != "ERR analytics timeout" {
		t.Fatalf("TIMEOUT: %q", msg)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("TIMEOUT 300 returned after %v", d)
	}
	do(t, c, "CONFIG", "SET", "nildb.analytics-read-bps", "0")
	do(t, c, "CONFIG", "SET", "nildb.analytics-max-rows", "500")
	if msg := doErr(t, c, "NIL.AGGREGATE", "t.x", `[{"$group": {"_id": null, "n": {"$sum": 1}}}]`); msg != "ERR analytics row limit reached" {
		t.Errorf("row cap: %q", msg)
	}
	// A streaming pipeline returns its first batch; the cap counts the
	// rows of the whole run, so a later DOC.CURSOR READ hits it.
	id, _ := cursor(t, do(t, c, "NIL.AGGREGATE", "t.x", `[]`, "BATCH", "100"))
	var msg string
	for range 10 {
		_, err := c.Do(ctx, "DOC.CURSOR", "READ", id).Result()
		if err != nil {
			msg = err.Error()
			break
		}
	}
	if msg != "ERR analytics row limit reached" {
		t.Errorf("row cap across cursor reads: %q", msg)
	}
	if msg := doErr(t, c, "NIL.COUNT", "t.x", `{"v": 1}`); msg != "ERR analytics row limit reached" {
		t.Errorf("row cap on NIL.COUNT: %q", msg)
	}
	if n := do(t, c, "NIL.COUNT", "t.x"); n != int64(2000) {
		t.Errorf("NIL.COUNT without a filter reads the counter: %v", n)
	}
	if n := infoField(t, c, "analytics_open_leases"); n != 0 {
		t.Errorf("failed commands left %d leases", n)
	}
}

func TestKeyStats(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	for i := range 5 {
		do(t, c, "SET", fmt.Sprintf("k%d", i), "v")
	}
	do(t, c, "SET", "tmp", "v", "PX", "1000")
	if got := do(t, c, "GET", "k0"); got != "v" {
		t.Fatalf("GET = %v", got)
	}
	txn := e.Store.Begin()
	txn.Put(store.CFMeta, layout.MetaKey(nil, 3, []byte("h")), layout.Meta{Type: layout.THash, Version: 9, Size: 2}.Encode(nil))
	txn.Put(store.CFMeta, layout.MetaKey(nil, 3, []byte("z")), layout.Meta{Type: layout.TZSet, Version: 10, Size: 1}.Encode(nil))
	txn.Put(store.CFMeta, layout.VersionKey(nil, 9), []byte{0})
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	check := func(args []any, want map[string]any) {
		t.Helper()
		got := do(t, c, append([]any{"NIL.KEYSTATS"}, args...)...)
		for k, v := range want {
			if g := field(t, got, k); g != v {
				t.Errorf("NIL.KEYSTATS %v %s = %v, want %v (%v)", args, k, g, v, got)
			}
		}
	}
	check(nil, map[string]any{"db": "all", "keys": int64(8), "expires": int64(1), "string": int64(6), "hash": int64(1), "zset": int64(1), "list": int64(0), "set": int64(0)})
	check([]any{"3"}, map[string]any{"db": int64(3), "keys": int64(2), "string": int64(0), "hash": int64(1)})
	e.Clock.Advance(2 * time.Second)
	check(nil, map[string]any{"keys": int64(7), "expires": int64(0), "string": int64(5)})
	if msg := doErr(t, c, "NIL.KEYSTATS", "16"); msg != "ERR DB index is out of range" {
		t.Errorf("db 16: %q", msg)
	}
	if msg := doErr(t, c, "NIL.KEYSTATS", "1", "2"); msg != "ERR syntax error" {
		t.Errorf("two arguments: %q", msg)
	}
}

func TestExplainAndInfo(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	load(t, c, "t.x", 0, 100, row)
	do(t, c, "DOC.CREATEINDEX", "t.x", `{"v": 1}`)
	p := `[{"$match": {"v": 5}}, {"$group": {"_id": "$k"}}]`
	ex := do(t, c, "NIL.EXPLAIN", "t.x", p)
	if field(t, ex, "plan") != "index" || field(t, ex, "index") != "v_1" {
		t.Errorf("NIL.EXPLAIN = %v", ex)
	}
	if ex2 := do(t, c, "NIL.AGGREGATE", "t.x", p, "EXPLAIN"); !reflect.DeepEqual(ex, ex2) {
		t.Errorf("NIL.AGGREGATE EXPLAIN = %v, NIL.EXPLAIN %v", ex2, ex)
	}
	if _, docs := cursor(t, do(t, c, "NIL.AGGREGATE", "t.x", p)); len(docs) != 1 {
		t.Errorf("aggregate = %v", docs)
	}
	for _, tc := range []struct {
		args []any
		want string
	}{
		{[]any{"NIL.EXPLAIN", "t.x", `{"a": 1}`}, "ERR the pipeline must be a JSON array of stage documents"},
		{[]any{"NIL.AGGREGATE", "t.x", `[{"$out": "x"}]`}, "ERR unsupported pipeline stage '$out' in v1"},
		{[]any{"NIL.AGGREGATE", "t.x", `[]`, "BOGUS"}, "ERR syntax error"},
		{[]any{"NIL.AGGREGATE", "t.x", `[]`, "TIMEOUT", "0"}, "ERR TIMEOUT must be a positive number of milliseconds"},
		{[]any{"NIL.AGGREGATE", "t.x", `[]`, "AT", "999"}, "ERR snapshot lease expired"},
		{[]any{"NIL.DISTINCT", "t.x", "k", `{"$bad": 1}`}, "ERR unknown top level operator: $bad"},
	} {
		if msg := doErr(t, c, tc.args...); msg != tc.want {
			t.Errorf("%v: %q, want %q", tc.args, msg, tc.want)
		}
	}
	info := c.Info(ctx, "analytics").Val()
	if !strings.Contains(info, "# Analytics") {
		t.Fatalf("INFO analytics = %q", info)
	}
	if n := infoField(t, c, "analytics_queries"); n < 1 {
		t.Errorf("queries = %d", n)
	}
	if n := infoField(t, c, "analytics_rows_scanned"); n < 1 {
		t.Errorf("rows scanned = %d", n)
	}
	if n := infoField(t, c, "analytics_bytes_scanned"); n < 1 {
		t.Errorf("bytes scanned = %d", n)
	}
	if n := infoField(t, c, "analytics_running"); n != 0 {
		t.Errorf("running = %d", n)
	}
}

// TestInterference is the smoke test of architecture.md section 7: eight
// clients run SET and GET while NIL.AGGREGATE groups 50,000 documents in a
// loop. Point operations must not fail, and their p99 must stay under
// 50 ms (checked without the race detector, which slows every path).
func TestInterference(t *testing.T) {
	e := spawn(t, nil)
	c := e.Client(t, 3)
	const docs = 50_000
	load(t, c, "t.big", 0, docs, func(i int) string {
		return fmt.Sprintf(`{"_id": %d, "k": "k%d", "v": %d, "pad": %q}`, i, i%10, i%1000, pad[:100])
	})
	pipeline := `[{"$group": {"_id": "$k", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]`

	stop := make(chan struct{})
	var wg sync.WaitGroup
	lat := make([][]time.Duration, 8)
	errs := make(chan error, len(lat))
	for g := range lat {
		cl := e.Client(t, 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("g%d:%d", g, n%100)
				t0 := time.Now()
				if err := cl.Do(ctx, "SET", key, "value").Err(); err != nil {
					errs <- err
					return
				}
				t1 := time.Now()
				if v, err := cl.Do(ctx, "GET", key).Text(); err != nil || v != "value" {
					errs <- fmt.Errorf("GET %s = %q, %v", key, v, err)
					return
				}
				lat[g] = append(lat[g], t1.Sub(t0), time.Since(t1))
			}
		}()
	}

	ac := e.Client(t, 3)
	start := time.Now()
	runs := 0
	for runs < 3 || time.Since(start) < 2*time.Second {
		groups := drain(t, ac, "NIL.AGGREGATE", "t.big", pipeline)
		if len(groups) != 10 || groups[0] != `{"_id":"k0","n":{"$numberInt":"5000"}}` {
			t.Fatalf("NIL.AGGREGATE run %d = %v", runs, groups)
		}
		runs++
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("point operation failed: %v", err)
	}
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	if len(all) < 1000 {
		t.Fatalf("only %d point operations ran", len(all))
	}
	slices.Sort(all)
	p50, p99 := all[len(all)/2], all[len(all)*99/100]
	t.Logf("%d point operations during %d NIL.AGGREGATE runs over %d documents in %v: p50 %v, p99 %v, max %v",
		len(all), runs, docs, time.Since(start).Round(time.Millisecond), p50, p99, all[len(all)-1])
	if !raceEnabled && p99 > 50*time.Millisecond {
		t.Errorf("point operation p99 %v is above 50ms", p99)
	}
}
