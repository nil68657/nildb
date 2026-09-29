package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/admin"
	"github.com/nil68657/nildb/internal/analytics"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/cmddoc"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/docstore"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/redis"
	"github.com/nil68657/nildb/internal/testutil"
)

// spawnNilDB starts an in-process NilDB with every command package
// cmd/nildb registers, and returns its address.
func spawnNilDB(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("opens a storage engine")
	}
	var q *query.Engine
	env := testutil.Spawn(t, nil, func(r *command.Registry, e *testutil.Env) error {
		cat, err := catalog.Open(e.Store)
		if err != nil {
			return err
		}
		if err := redis.Register(r); err != nil {
			return err
		}
		if err := admin.Register(r, e.Server, e.Store); err != nil {
			return err
		}
		q = query.New(e.Store, e.Cfg)
		cmddoc.Register(r, cat, docstore.New(e.Store, cat), q)
		analytics.Register(r, e.Server, e.Cfg, cat, q)
		return nil
	})
	// Registered after Spawn's own cleanup, so it runs first: cursors and
	// their leases end while the store is still open.
	t.Cleanup(q.Close)
	return env.Addr
}

func (hs *harness) batch(db int, cmds ...[]any) []map[string]any {
	hs.t.Helper()
	r := hs.exec(map[string]any{"cmds": cmds, "db": db})
	if r.code != http.StatusOK {
		hs.t.Fatalf("batch: HTTP %d %v", r.code, r.body)
	}
	out := make([]map[string]any, 0, len(cmds))
	for _, x := range r.body["replies"].([]any) {
		out = append(out, x.(map[string]any))
	}
	return out
}

// mapField returns the value under key in a map reply.
func mapField(m map[string]any, key string) map[string]any {
	for _, p := range asList(m["v"]) {
		kv := p.([]any)
		if kv[0].(map[string]any)["v"] == key {
			return kv[1].(map[string]any)
		}
	}
	return nil
}

func TestNilDBReplyShapes(t *testing.T) {
	hs := newHarness(t, spawnNilDB(t), false, nil)

	r := hs.do("GET", "/api/status", nil, nil)
	if r.body["connected"] != true || r.body["hello"].(map[string]any)["proto"] != float64(3) {
		t.Fatalf("status = %v", r.body)
	}

	got := hs.batch(0,
		[]any{"HSET", "h", "f1", "v1", "f2", "v2"},
		[]any{"HGETALL", "h"},
		[]any{"ZADD", "z", "1.5", "a", "2", "b"},
		[]any{"ZRANGE", "z", "0", "-1", "WITHSCORES"},
		[]any{"SADD", "s", "x"},
		[]any{"SMEMBERS", "s"},
		[]any{"INFO", "server"},
		[]any{"ZSCORE", "z", "a"},
		[]any{"GEOADD", "g", "2.3522", "48.8566", "paris"},
		[]any{"GEOPOS", "g", "paris"},
		[]any{"SET", map[string]string{"b64": "/wA="}, map[string]string{"b64": "AP8="}},
		[]any{"GET", map[string]string{"b64": "/wA="}},
	)
	wantReply(t, got[0], "int", 2)
	if got[1]["t"] != "map" || len(asList(got[1]["v"])) != 2 {
		t.Errorf("HGETALL = %v", got[1])
	}
	pairs := asList(got[3]["v"])
	if got[3]["t"] != "array" || len(pairs) != 2 || asList(pairs[0].(map[string]any)["v"])[1].(map[string]any)["t"] != "double" {
		t.Errorf("ZRANGE WITHSCORES = %v", got[3])
	}
	if got[5]["t"] != "set" {
		t.Errorf("SMEMBERS = %v", got[5])
	}
	if got[6]["t"] != "verbatim" || !strings.Contains(got[6]["v"].(string), "redis_version:") {
		t.Errorf("INFO = %v", got[6])
	}
	wantReply(t, got[7], "double", "1.5")
	coords := asList(asList(got[9]["v"])[0].(map[string]any)["v"])
	if len(coords) != 2 || coords[0].(map[string]any)["t"] != "double" || !strings.HasPrefix(coords[0].(map[string]any)["v"].(string), "2.352") {
		t.Errorf("GEOPOS = %v", got[9])
	}
	if got[11]["b64"] != "AP8=" {
		t.Errorf("binary GET = %v", got[11])
	}

	docs := hs.batch(0,
		[]any{"DOC.INSERT", "shop.cafes", `{"_id":1,"name":"Kaffeine","city":"London","rating":4.6}`, `{"_id":2,"name":"Ten Belles","city":"Paris","rating":4.4}`},
		[]any{"DOC.CREATEINDEX", "shop.cafes", `{"city":1}`},
		[]any{"DOC.FIND", "shop.cafes", `{"city":"Paris"}`, "EXPLAIN"},
		[]any{"DOC.FIND", "shop.cafes", `{}`, "BATCH", "1"},
	)
	if n := mapField(docs[0], "n"); n["v"] != float64(2) {
		t.Fatalf("DOC.INSERT = %v", docs[0])
	}
	if mapField(docs[2], "plan")["v"] != "index" {
		t.Errorf("EXPLAIN = %v", docs[2])
	}
	id := mapField(mapField(docs[3], "cursor"), "id")
	if id["t"] != "int" {
		t.Fatalf("DOC.FIND cursor = %v", docs[3])
	}
	// Cursor ids are random 63-bit numbers; they must reach the browser as
	// exact digits.
	idText := fmt.Sprint(id["v"])
	next := hs.run(0, "DOC.CURSOR", "READ", idText)
	if batch := asList(mapField(mapField(next, "cursor"), "nextBatch")["v"]); len(batch) != 1 {
		t.Errorf("DOC.CURSOR READ %s = %v", idText, next)
	}

	lease := hs.run(0, "ROCKS.SNAPSHOT", "CREATE", "TTL", "60")
	leaseID := fmt.Sprint(mapField(lease, "id")["v"])
	hs.run(0, "DOC.INSERT", "shop.cafes", `{"_id":3,"name":"Later","city":"Paris","rating":3.9}`)
	agg := hs.run(0, "NIL.AGGREGATE", "shop.cafes", `[{"$group":{"_id":"$city","n":{"$sum":1}}},{"$sort":{"_id":1}}]`, "AT", leaseID)
	rows := asList(mapField(mapField(agg, "cursor"), "firstBatch")["v"])
	if len(rows) != 2 {
		t.Fatalf("NIL.AGGREGATE AT %s = %v", leaseID, agg)
	}
	paris := rows[1].(map[string]any)["v"].(string)
	if !strings.Contains(paris, `"Paris"`) || !(strings.Contains(paris, `"$numberInt":"1"`) || strings.Contains(paris, `"$numberLong":"1"`)) {
		t.Errorf("NIL.AGGREGATE AT %s saw the later insert: %s", leaseID, paris)
	}
}

// scanAll pages through /api/scan and returns the key names in order and
// the size of every page.
func (hs *harness) scanAll(req map[string]any) ([]string, []int) {
	hs.t.Helper()
	var names []string
	var pages []int
	req["cursor"] = "0"
	for len(pages) < 100 {
		r := hs.do("POST", "/api/scan", req, nil)
		if r.code != http.StatusOK {
			hs.t.Fatalf("scan %v: HTTP %d %v", req, r.code, r.body)
		}
		keys := asList(r.body["keys"])
		pages = append(pages, len(keys))
		for _, x := range keys {
			names = append(names, x.(map[string]any)["k"].(string))
		}
		if req["cursor"] = r.body["cursor"]; req["cursor"] == "0" {
			return names, pages
		}
	}
	hs.t.Fatalf("scan %v did not finish", req)
	return nil, nil
}

func TestNilDBScanPages(t *testing.T) {
	hs := newHarness(t, spawnNilDB(t), false, nil)
	types := []string{"string", "hash", "list", "set", "zset"}
	var cmds [][]any
	want := map[string]string{}
	for i := range 1000 {
		k := fmt.Sprintf("user:%04d", i)
		typ := types[i%5]
		want[k] = typ
		switch typ {
		case "string":
			cmds = append(cmds, []any{"SET", k, "hello"})
		case "hash":
			cmds = append(cmds, []any{"HSET", k, "a", "1", "b", "2"})
		case "list":
			cmds = append(cmds, []any{"RPUSH", k, "x", "y", "z"})
		case "set":
			cmds = append(cmds, []any{"SADD", k, "m"})
		case "zset":
			cmds = append(cmds, []any{"ZADD", k, "1", "m", "2", "n"})
		}
		if i%7 == 0 {
			cmds = append(cmds, []any{"EXPIRE", k, "3600"})
		}
	}
	cmds = append(cmds, []any{"SET", "other", "1"})
	hs.batch(2, cmds...)

	// Unfiltered pages hold exactly the keys asked for.
	names, pages := hs.scanAll(map[string]any{"db": 2, "count": 150})
	if len(names) != 1001 || len(pages) != 7 || pages[0] != 150 || pages[6] != 101 {
		t.Fatalf("scan of 1001 keys: %d keys in pages %v", len(names), pages)
	}
	if !sort.StringsAreSorted(names) {
		t.Error("SCAN pages are not in key order")
	}

	lens := map[string]float64{"string": 5, "hash": 2, "list": 3, "set": 1, "zset": 2}
	seen := map[string]bool{}
	r := hs.do("POST", "/api/scan", map[string]any{"db": 2, "match": "user:*", "count": 2000}, nil)
	for _, x := range asList(r.body["keys"]) {
		k := x.(map[string]any)
		name := k["k"].(string)
		seen[name] = true
		if k["t"] != want[name] || k["n"] != lens[want[name]] {
			t.Errorf("%s = %v, want type %s", name, k, want[name])
		}
		var i int
		fmt.Sscanf(name, "user:%d", &i)
		if ttl := k["ttl"].(float64); (i%7 == 0) != (ttl > 0) {
			t.Errorf("%s ttl = %v", name, ttl)
		}
	}
	if len(seen) != 1000 || r.body["cursor"] != "0" {
		t.Errorf("MATCH user:* found %d keys, cursor %v", len(seen), r.body["cursor"])
	}

	names, _ = hs.scanAll(map[string]any{"db": 2, "match": "user:00*", "type": "hash", "count": 5})
	if len(names) != 20 || names[0] != "user:0001" || names[19] != "user:0096" {
		t.Errorf("MATCH user:00* TYPE hash = %d keys %v", len(names), names)
	}
	if r := hs.do("POST", "/api/scan", map[string]any{"type": "stream"}, nil); r.code != http.StatusBadRequest {
		t.Errorf("TYPE stream: HTTP %d, want 400", r.code)
	}
	if r := hs.do("POST", "/api/scan", map[string]any{"cursor": "12345"}, nil); r.code != http.StatusBadRequest {
		t.Errorf("stale cursor: HTTP %d %v, want 400", r.code, r.body)
	}
}

func TestNilDBReadonly(t *testing.T) {
	hs := newHarness(t, spawnNilDB(t), true, nil)
	refused := [][]any{
		{"SET", "k", "v"},
		{"DEL", "k"},
		{"FLUSHALL"},
		{"DOC.INSERT", "a.b", `{"x":1}`},
		{"DOC.CREATEINDEX", "a.b", `{"x":1}`},
		{"ROCKS.COMPACT"},
		{"ROCKS.CHECKPOINT", "/tmp/never"},
		{"CONFIG", "SET", "nildb.lease-ttl", "30"},
		{"SHUTDOWN", "NOSAVE"},
		{"GEORADIUS", "g", "0", "0", "1", "km", "STORE", "x"},
	}
	for _, args := range refused {
		if r := hs.exec(map[string]any{"args": args}); r.code != http.StatusForbidden {
			t.Errorf("%v: HTTP %d %v, want 403", args, r.code, r.body)
		}
	}
	allowed := [][]any{
		{"GET", "k"},
		{"SCAN", "0"},
		{"INFO"},
		{"CONFIG", "GET", "nildb.*"},
		{"CLIENT", "LIST"},
		{"ROCKS.INFO"},
		{"ROCKS.CF", "INFO", "default"},
		{"ROCKS.SNAPSHOT", "CREATE"},
		{"DOC.FIND", "a.b", "{}"},
		{"NIL.KEYSTATS"},
		{"GEOSEARCH", "g", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km"},
	}
	for _, args := range allowed {
		if r := hs.exec(map[string]any{"args": args}); r.code != http.StatusOK {
			t.Errorf("%v: HTTP %d %v, want 200", args, r.code, r.body)
		}
	}
	sess := func(args ...any) result {
		return hs.exec(map[string]any{"args": args, "session": "ro-tab"})
	}
	if r := sess("MULTI"); r.code != http.StatusOK {
		t.Fatalf("MULTI in a read-only session: HTTP %d", r.code)
	}
	if r := sess("SET", "k", "v"); r.code != http.StatusForbidden {
		t.Errorf("SET inside MULTI: HTTP %d, want 403", r.code)
	}
	if r := sess("EXEC"); r.code != http.StatusOK || r.body["session"].(map[string]any)["multi"] != false {
		t.Errorf("EXEC: HTTP %d %v", r.code, r.body)
	}
}
