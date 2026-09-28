package admin

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/redis"
	"github.com/nil68657/nildb/internal/store"
	"github.com/nil68657/nildb/internal/testutil"
)

func spawn(t *testing.T, mutate func(*config.Config)) *testutil.Env {
	t.Helper()
	return testutil.Spawn(t, mutate, func(r *command.Registry, e *testutil.Env) error {
		if err := redis.Register(r); err != nil {
			return err
		}
		return Register(r, e.Server, e.Store)
	})
}

func do(t *testing.T, c *goredis.Client, args ...any) any {
	t.Helper()
	v, err := c.Do(context.Background(), args...).Result()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

// wantErr runs a command that must fail with an error starting with want.
func wantErr(t *testing.T, c *goredis.Client, want string, args ...any) {
	t.Helper()
	err := c.Do(context.Background(), args...).Err()
	if err == nil || errors.Is(err, goredis.Nil) || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("%v: got error %v, want prefix %q", args, err, want)
	}
}

func wantNil(t *testing.T, c *goredis.Client, args ...any) {
	t.Helper()
	if v, err := c.Do(context.Background(), args...).Result(); !errors.Is(err, goredis.Nil) {
		t.Errorf("%v = %v, %v; want nil", args, v, err)
	}
}

// asMap reads a RESP3 map reply.
func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[any]any)
	if !ok {
		t.Fatalf("want a map, got %T %v", v, v)
	}
	out := make(map[string]any, len(m))
	for k, x := range m {
		out[k.(string)] = x
	}
	return out
}

// pairs reads a RESP2 flat array of names and values.
func pairs(t *testing.T, v any) map[string]any {
	t.Helper()
	a, ok := v.([]any)
	if !ok || len(a)%2 != 0 {
		t.Fatalf("want a flat array of pairs, got %T %v", v, v)
	}
	out := make(map[string]any, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		out[a[i].(string)] = a[i+1]
	}
	return out
}

func metaKeyHex(key string) string { return hex.EncodeToString(layout.MetaKey(nil, 0, []byte(key))) }

// userKeys decodes the db-0 entries of a ROCKS.SCAN meta reply to user
// key -> string payload.
func userKeys(t *testing.T, scan map[string]any) map[string]string {
	t.Helper()
	out := map[string]string{}
	for kh, vh := range scan {
		k, err := hex.DecodeString(kh)
		if err != nil {
			t.Fatalf("key %q is not hex: %v", kh, err)
		}
		v, err := hex.DecodeString(vh.(string))
		if err != nil {
			t.Fatalf("value %q is not hex: %v", vh, err)
		}
		db, key, ok := layout.SplitMetaKey(k)
		if !ok || db != 0 {
			continue
		}
		m, err := layout.DecodeMeta(v)
		if err != nil {
			t.Fatalf("meta of %q: %v", key, err)
		}
		out[string(key)] = string(m.Payload)
	}
	return out
}

func intProperty(t *testing.T, c *goredis.Client, name, cf string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(do(t, c, "ROCKS.PROPERTY", name, "CF", cf).(string), 10, 64)
	if err != nil {
		t.Fatalf("%s on %s: %v", name, cf, err)
	}
	return n
}

// infoFields parses one INFO section into field -> value.
func infoFields(t *testing.T, text, header string) map[string]string {
	t.Helper()
	out := map[string]string{}
	in := false
	for _, line := range strings.Split(text, "\r\n") {
		switch {
		case strings.HasPrefix(line, "# "):
			in = line == header
		case in && line != "":
			k, v, ok := strings.Cut(line, ":")
			if !ok {
				t.Fatalf("INFO line %q has no colon", line)
			}
			out[k] = v
		}
	}
	if len(out) == 0 {
		t.Fatalf("INFO has no %q section:\n%s", header, text)
	}
	return out
}

func TestRocksInfoAndProperties(t *testing.T) {
	e := spawn(t, nil)
	r2, r3 := e.Client(t, 2), e.Client(t, 3)

	info := asMap(t, do(t, r3, "ROCKS.INFO"))
	major, minor, patch := store.Version()
	if want := fmt.Sprintf("%d.%d.%d", major, minor, patch); info["version"] != want {
		t.Errorf("version = %v, want %s", info["version"], want)
	}
	cfs, _ := info["column_families"].([]any)
	if got := fmt.Sprint(cfs); got != "[default meta sub zscore doc idx geo col]" {
		t.Errorf("column_families = %s", got)
	}
	if info["fsync"] != e.Cfg.Fsync || info["statistics"] != "no" || info["readonly"] != "no" ||
		info["leases"] != int64(0) || info["max_snapshots"] != int64(32) || info["dir"] != e.Cfg.Dir {
		t.Errorf("ROCKS.INFO = %v", info)
	}
	if flat := pairs(t, do(t, r2, "ROCKS.INFO")); len(flat) != len(info) || flat["version"] != info["version"] {
		t.Errorf("RESP2 ROCKS.INFO = %v", flat)
	}
	tc := e.TCP(t)
	if got := string(tc.Do("ROCKS.INFO")); !strings.HasPrefix(got, "*28\r\n$7\r\nversion\r\n") {
		t.Errorf("RESP2 ROCKS.INFO starts %q", got[:min(len(got), 40)])
	}
	tc.Do("HELLO", "3")
	if got := string(tc.Do("ROCKS.INFO")); !strings.HasPrefix(got, "%14\r\n$7\r\nversion\r\n") {
		t.Errorf("RESP3 ROCKS.INFO starts %q", got[:min(len(got), 40)])
	}
	if got := string(tc.Do("MULTI")); got != "+OK\r\n" {
		t.Fatalf("MULTI = %q", got)
	}
	if got := string(tc.Do("ROCKS.SEQ")); got != "-ERR Command not allowed inside a transaction\r\n" {
		t.Errorf("ROCKS.SEQ in MULTI = %q", got)
	}
	tc.Do("DISCARD")

	before := do(t, r2, "ROCKS.SEQ").(int64)
	do(t, r2, "SET", "k", "v")
	if after := do(t, r2, "ROCKS.SEQ").(int64); after <= before {
		t.Errorf("ROCKS.SEQ %d after a write, %d before", after, before)
	}

	if got := fmt.Sprint(do(t, r2, "ROCKS.CF", "LIST")); got != "[default meta sub zscore doc idx geo col]" {
		t.Errorf("ROCKS.CF LIST = %s", got)
	}
	cf := asMap(t, do(t, r3, "ROCKS.CF", "INFO", "META"))
	for _, name := range []string{"rocksdb.estimate-num-keys", "rocksdb.total-sst-files-size", "rocksdb.num-files-at-level0"} {
		if _, ok := cf[name].(int64); !ok {
			t.Errorf("ROCKS.CF INFO meta lacks %s: %v", name, cf)
		}
	}
	wantErr(t, r2, "ERR unknown column family 'nope'", "ROCKS.CF", "INFO", "nope")
	wantErr(t, r2, "ERR wrong number of arguments for 'rocks.cf' command", "ROCKS.CF")
	wantErr(t, r2, "ERR unknown subcommand 'bogus'. Try ROCKS.CF HELP.", "ROCKS.CF", "bogus")

	if v := do(t, r2, "ROCKS.PROPERTY", "rocksdb.num-snapshots"); v != "0" {
		t.Errorf("rocksdb.num-snapshots = %v", v)
	}
	intProperty(t, r2, "rocksdb.estimate-num-keys", "meta")
	if v := do(t, r2, "ROCKS.PROPERTY", "rocksdb.stats").(string); !strings.Contains(v, "DB Stats") {
		t.Errorf("rocksdb.stats = %q", v)
	}
	wantNil(t, r2, "ROCKS.PROPERTY", "rocksdb.no-such-property")
	wantErr(t, r2, "ERR syntax error", "ROCKS.PROPERTY", "rocksdb.stats", "CF")
	wantErr(t, r2, "ERR syntax error", "ROCKS.PROPERTY", "rocksdb.stats", "FOO", "meta")

	wantErr(t, r2, "ERR statistics disabled, start with --rocks-stats", "ROCKS.STATS")
	for _, args := range [][]any{{"ROCKS.FLUSHWAL"}, {"ROCKS.FLUSHWAL", "sync"},
		{"ROCKS.SETOPTION", "meta", "disable_auto_compactions", "true"},
		{"ROCKS.SETOPTION", "meta", "disable_auto_compactions", "false"}} {
		if v := do(t, r2, args...); v != "OK" {
			t.Errorf("%v = %v", args, v)
		}
	}
	wantErr(t, r2, "ERR syntax error", "ROCKS.FLUSHWAL", "NOW")
	wantErr(t, r2, "ERR ", "ROCKS.SETOPTION", "meta", "no_such_option", "1")
	wantErr(t, r2, "ERR unknown column family 'x'", "ROCKS.SETOPTION", "x", "write_buffer_size", "1")
	wantErr(t, r2, "ERR ", "ROCKS.INGEST", "meta", filepath.Join(t.TempDir(), "missing.sst"))
	wantErr(t, r2, "ERR wrong number of arguments for 'rocks.ingest' command", "ROCKS.INGEST", "meta")
}

func TestRocksStatsEnabled(t *testing.T) {
	e := spawn(t, func(c *config.Config) { c.RocksStats = true })
	r2 := e.Client(t, 2)
	do(t, r2, "SET", "k", "v")
	if s := do(t, r2, "ROCKS.STATS").(string); !strings.Contains(s, "rocksdb.") {
		t.Errorf("ROCKS.STATS = %q", s)
	}
	if info := pairs(t, do(t, r2, "ROCKS.INFO")); info["statistics"] != "yes" {
		t.Errorf("statistics = %v", info["statistics"])
	}
}

func TestRocksScanGetHex(t *testing.T) {
	e := spawn(t, nil)
	r2, r3 := e.Client(t, 2), e.Client(t, 3)
	for i := 1; i <= 3; i++ {
		do(t, r2, "SET", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	do(t, r2, "HSET", "h", "f", "v")

	scan := asMap(t, do(t, r3, "ROCKS.SCAN", "meta"))
	got := userKeys(t, scan)
	if got["k1"] != "v1" || got["k2"] != "v2" || got["k3"] != "v3" {
		t.Errorf("decoded ROCKS.SCAN meta = %q", got)
	}
	if _, ok := got["h"]; !ok {
		t.Errorf("ROCKS.SCAN meta misses the hash: %q", got)
	}
	for kh, vh := range scan {
		if v := do(t, r2, "ROCKS.GET", "meta", strings.ToUpper(kh)); v != vh {
			t.Errorf("ROCKS.GET meta %s = %v, scan said %v", kh, v, vh)
		}
	}

	page := do(t, r2, "ROCKS.SCAN", "meta", "FROM", metaKeyHex("k2"), "TO", metaKeyHex("k3"), "COUNT", "10").([]any)
	if len(page) != 2 || page[0] != metaKeyHex("k2") {
		t.Errorf("ROCKS.SCAN meta FROM k2 TO k3 = %v", page)
	}
	page = do(t, r2, "ROCKS.SCAN", "meta", "COUNT", "1").([]any)
	if len(page) != 2 || page[0] != metaKeyHex("h") {
		t.Errorf("ROCKS.SCAN meta COUNT 1 = %v", page)
	}
	next := hex.EncodeToString(append(layout.MetaKey(nil, 0, []byte("h")), 0))
	page = do(t, r2, "ROCKS.SCAN", "meta", "FROM", next, "COUNT", "1").([]any)
	if len(page) != 2 || page[0] != metaKeyHex("k1") {
		t.Errorf("second page = %v", page)
	}

	tc := e.TCP(t)
	k1, v1 := metaKeyHex("k1"), scan[metaKeyHex("k1")].(string)
	want := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k1), k1, len(v1), v1)
	if got := string(tc.Do("ROCKS.SCAN", "meta", "FROM", k1, "TO", metaKeyHex("k2"))); got != want {
		t.Errorf("RESP2 ROCKS.SCAN = %q, want %q", got, want)
	}
	tc.Do("HELLO", "3")
	want = "%1" + want[2:]
	if got := string(tc.Do("ROCKS.SCAN", "meta", "FROM", k1, "TO", metaKeyHex("k2"))); got != want {
		t.Errorf("RESP3 ROCKS.SCAN = %q, want %q", got, want)
	}
	if got := string(tc.Do("ROCKS.SCAN", "meta", "FROM", "ff")); got != "%0\r\n" {
		t.Errorf("empty RESP3 ROCKS.SCAN = %q", got)
	}

	wantNil(t, r2, "ROCKS.GET", "meta", "00ff")
	wantErr(t, r2, "ERR invalid hex string 'zz'", "ROCKS.GET", "meta", "zz")
	wantErr(t, r2, "ERR invalid hex string 'abc'", "ROCKS.GET", "meta", "abc")
	wantErr(t, r2, "ERR unknown column family 'nope'", "ROCKS.SCAN", "nope")
	wantErr(t, r2, "ERR syntax error", "ROCKS.SCAN", "meta", "COUNT", "0")
	wantErr(t, r2, "ERR value is not an integer or out of range", "ROCKS.SCAN", "meta", "COUNT", "x")
	wantErr(t, r2, "ERR syntax error", "ROCKS.SCAN", "meta", "FROM")
	wantErr(t, r2, "ERR syntax error", "ROCKS.SCAN", "meta", "BOGUS", "1")
	wantErr(t, r2, "ERR snapshot lease expired", "ROCKS.SCAN", "meta", "SNAPSHOT", "999")
}

func TestRocksSnapshotLeases(t *testing.T) {
	e := spawn(t, func(c *config.Config) { c.MaxSnapshots = 2 })
	r2, r3 := e.Client(t, 2), e.Client(t, 3)
	do(t, r2, "SET", "before", "1")

	lease := asMap(t, do(t, r3, "ROCKS.SNAPSHOT", "CREATE", "TTL", "30"))
	id := lease["id"].(int64)
	if owner := lease["owner"].(string); !strings.HasPrefix(owner, "ROCKS.SNAPSHOT conn ") {
		t.Errorf("owner = %q", owner)
	}
	if d := lease["expires_ms"].(int64) - lease["created_ms"].(int64); d != 30000 {
		t.Errorf("lease lasts %d ms, want 30000", d)
	}
	do(t, r2, "SET", "after", "2")
	sid := strconv.FormatInt(id, 10)
	old := userKeys(t, asMap(t, do(t, r3, "ROCKS.SCAN", "meta", "SNAPSHOT", sid)))
	if _, ok := old["after"]; ok || old["before"] != "1" {
		t.Errorf("ROCKS.SCAN on the lease = %q", old)
	}
	if live := userKeys(t, asMap(t, do(t, r3, "ROCKS.SCAN", "meta"))); live["after"] != "2" {
		t.Errorf("live ROCKS.SCAN = %q", live)
	}
	list := do(t, r3, "ROCKS.SNAPSHOT", "LIST").([]any)
	if len(list) != 1 || asMap(t, list[0])["id"] != id {
		t.Errorf("ROCKS.SNAPSHOT LIST = %v", list)
	}
	rocks := infoFields(t, do(t, r2, "INFO", "rocksdb").(string), "# Rocksdb")
	if rocks["nildb_leases"] != "1" || rocks["num_snapshots"] != "1" || rocks["oldest_snapshot_time"] == "0" {
		t.Errorf("INFO rocksdb with one lease: %v", rocks)
	}

	do(t, r2, "ROCKS.SNAPSHOT", "CREATE")
	wantErr(t, r2, "ERR too many open snapshots", "ROCKS.SNAPSHOT", "CREATE")
	if v := do(t, r2, "ROCKS.SNAPSHOT", "RELEASE", sid); v != int64(1) {
		t.Errorf("RELEASE = %v", v)
	}
	if v := do(t, r2, "ROCKS.SNAPSHOT", "RELEASE", sid); v != int64(0) {
		t.Errorf("second RELEASE = %v", v)
	}
	wantErr(t, r2, "ERR snapshot lease expired", "ROCKS.SCAN", "meta", "SNAPSHOT", sid)
	wantErr(t, r2, "ERR value is not an integer or out of range", "ROCKS.SNAPSHOT", "RELEASE", "x")

	short := asMap(t, do(t, r3, "ROCKS.SNAPSHOT", "CREATE", "TTL", "1"))["id"].(int64)
	e.Clock.Advance(2 * time.Second)
	for _, l := range do(t, r3, "ROCKS.SNAPSHOT", "LIST").([]any) {
		if asMap(t, l)["id"] == short {
			t.Errorf("expired lease %d still listed", short)
		}
	}
	wantErr(t, r2, "ERR snapshot lease expired", "ROCKS.SCAN", "meta", "SNAPSHOT", strconv.FormatInt(short, 10))

	wantErr(t, r2, "ERR TTL must be a positive number of seconds", "ROCKS.SNAPSHOT", "CREATE", "TTL", "0")
	wantErr(t, r2, "ERR TTL 601 is above nildb.lease-max of 600 seconds", "ROCKS.SNAPSHOT", "CREATE", "TTL", "601")
	wantErr(t, r2, "ERR syntax error", "ROCKS.SNAPSHOT", "CREATE", "FOO", "1")
	wantErr(t, r2, "ERR syntax error", "ROCKS.SNAPSHOT", "CREATE", "TTL")
	help := do(t, r2, "ROCKS.SNAPSHOT", "HELP").([]any)
	if len(help) != len(snapshotHelp) || !strings.HasPrefix(help[0].(string), "ROCKS.SNAPSHOT <subcommand>") {
		t.Errorf("ROCKS.SNAPSHOT HELP = %v", help)
	}
}

func TestRocksCheckpointReadOnly(t *testing.T) {
	e := spawn(t, nil)
	r2, r3 := e.Client(t, 2), e.Client(t, 3)
	do(t, r2, "SET", "before", "kept")
	do(t, r2, "HSET", "h", "f", "v")
	seq := do(t, r2, "ROCKS.SEQ").(int64)

	dir := filepath.Join(t.TempDir(), "cp")
	res := asMap(t, do(t, r3, "ROCKS.CHECKPOINT", dir))
	if res["dir"] != dir || res["seq"].(int64) < seq {
		t.Errorf("ROCKS.CHECKPOINT = %v, want dir %s and seq >= %d", res, dir, seq)
	}
	do(t, r2, "SET", "after", "later")
	persistence := infoFields(t, do(t, r2, "INFO", "persistence").(string), "# Persistence")
	if persistence["nildb_last_checkpoint_dir"] != dir || persistence["nildb_last_checkpoint_seq"] != strconv.FormatInt(res["seq"].(int64), 10) ||
		persistence["nildb_last_checkpoint_time"] == "0" || persistence["nildb_fsync"] != e.Cfg.Fsync {
		t.Errorf("INFO persistence = %v", persistence)
	}

	ro, err := store.OpenReadOnly(store.Config{Dir: dir, BlockCacheBytes: 8 << 20, AnalyticsCacheBytes: 8 << 20, WriteBufferBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	v, ok, err := ro.Get(store.CFMeta, layout.MetaKey(nil, 0, []byte("before")))
	if err != nil || !ok {
		t.Fatalf("checkpoint Get(before) = %v, %v", ok, err)
	}
	if m, err := layout.DecodeMeta(v); err != nil || string(m.Payload) != "kept" {
		t.Errorf("checkpoint value of before = %+v, %v", m, err)
	}
	if _, ok, err := ro.Get(store.CFMeta, layout.MetaKey(nil, 0, []byte("after"))); ok || err != nil {
		t.Errorf("checkpoint has the key written after it: %v, %v", ok, err)
	}
	wantErr(t, r2, "ERR ", "ROCKS.CHECKPOINT", dir)

	if v := do(t, r2, "ROCKS.FLUSH", "CF", "meta"); v != "OK" {
		t.Fatalf("ROCKS.FLUSH = %v", v)
	}
	exp := filepath.Join(t.TempDir(), "export")
	if res := asMap(t, do(t, r3, "ROCKS.CHECKPOINT", exp, "CF", "meta")); res["cf"] != "meta" || res["dir"] != exp {
		t.Errorf("export = %v", res)
	}
	files, _ := filepath.Glob(filepath.Join(exp, "*.sst"))
	if len(files) == 0 {
		t.Fatalf("export of meta wrote no SST files")
	}
	// Default ingestion options refuse files RocksDB itself generated.
	wantErr(t, r2, "ERR ", "ROCKS.INGEST", "meta", files[0])
	if persistence := infoFields(t, do(t, r2, "INFO", "persistence").(string), "# Persistence"); persistence["nildb_last_checkpoint_cf"] != "meta" {
		t.Errorf("INFO persistence after export = %v", persistence)
	}
	wantErr(t, r2, "ERR syntax error", "ROCKS.CHECKPOINT", exp+"2", "FOO", "meta")
	wantErr(t, r2, "ERR unknown column family 'nope'", "ROCKS.CHECKPOINT", exp+"2", "CF", "nope")
}

func TestRocksCompactShrinksEstimate(t *testing.T) {
	e := spawn(t, nil)
	r2 := e.Client(t, 2)
	ctx := context.Background()
	const n = 2000
	keys := make([]string, n)
	pipe := r2.Pipeline()
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%05d", i)
		pipe.Set(ctx, keys[i], strings.Repeat("x", 100), 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	do(t, r2, "ROCKS.FLUSH", "CF", "meta")
	full := intProperty(t, r2, "rocksdb.estimate-num-keys", "meta")
	if full < n/2 {
		t.Fatalf("estimate-num-keys after %d SETs = %d", n, full)
	}
	if size := do(t, r2, "ROCKS.SIZES", "meta", "00", "01").(int64); size <= 0 {
		t.Errorf("ROCKS.SIZES over db 0 = %d", size)
	}
	for i := 0; i < n; i += 500 {
		args := []any{"DEL"}
		for _, k := range keys[i : i+500] {
			args = append(args, k)
		}
		do(t, r2, args...)
	}
	do(t, r2, "ROCKS.FLUSH", "CF", "meta")
	sstBefore := intProperty(t, r2, "rocksdb.total-sst-files-size", "meta")
	if v := do(t, r2, "ROCKS.COMPACT", "CF", "meta"); v != "OK" {
		t.Fatalf("ROCKS.COMPACT = %v", v)
	}
	if got := intProperty(t, r2, "rocksdb.estimate-num-keys", "meta"); got >= full/10 {
		t.Errorf("estimate-num-keys after deletes and compaction = %d, was %d", got, full)
	}
	if got := intProperty(t, r2, "rocksdb.total-sst-files-size", "meta"); got >= sstBefore {
		t.Errorf("total-sst-files-size %d after compaction, %d before", got, sstBefore)
	}

	for _, args := range [][]any{{"ROCKS.COMPACT"}, {"ROCKS.COMPACT", "CF", "sub", "FROM", "00", "TO", "01"},
		{"ROCKS.COMPACT", "to", "ff"}, {"ROCKS.FLUSH"}} {
		if v := do(t, r2, args...); v != "OK" {
			t.Errorf("%v = %v", args, v)
		}
	}
	wantErr(t, r2, "ERR invalid hex string 'zz'", "ROCKS.COMPACT", "FROM", "zz")
	wantErr(t, r2, "ERR syntax error", "ROCKS.COMPACT", "CF")
	wantErr(t, r2, "ERR syntax error", "ROCKS.COMPACT", "BOGUS", "x")
	wantErr(t, r2, "ERR unknown column family 'nope'", "ROCKS.FLUSH", "CF", "nope")
	wantErr(t, r2, "ERR syntax error", "ROCKS.FLUSH", "X", "meta")
	wantErr(t, r2, "ERR wrong number of arguments for 'rocks.sizes' command", "ROCKS.SIZES", "meta", "00")
}

func TestInfoRocksdbParses(t *testing.T) {
	e := spawn(t, nil)
	r2 := e.Client(t, 2)
	do(t, r2, "SET", "a", "1")
	text := do(t, r2, "INFO", "rocksdb").(string)
	f := infoFields(t, text, "# Rocksdb")
	if f["rocksdb_version"] != version() {
		t.Errorf("rocksdb_version = %q", f["rocksdb_version"])
	}
	if f["rocksdb_column_families"] != "default,meta,sub,zscore,doc,idx,geo,col" {
		t.Errorf("rocksdb_column_families = %q", f["rocksdb_column_families"])
	}
	cfLine := regexp.MustCompile(`^estimate_num_keys=\d+,total_sst_files_size=\d+$`)
	for _, name := range e.Store.CFNames() {
		if v := f["cf_"+name]; !cfLine.MatchString(v) {
			t.Errorf("cf_%s = %q", name, v)
		}
	}
	for _, name := range []string{"block_cache_usage", "block_cache_pinned_usage", "block_cache_capacity",
		"analytics_cache_usage", "analytics_cache_capacity", "cur_size_all_mem_tables", "num_snapshots",
		"oldest_snapshot_time", "estimate_pending_compaction_bytes", "num_running_compactions",
		"num_running_flushes", "latest_sequence_number", "nildb_leases", "nildb_max_snapshots",
		"nildb_scheduled_compactions"} {
		if _, err := strconv.ParseUint(f[name], 10, 64); err != nil {
			t.Errorf("%s = %q is not a count", name, f[name])
		}
	}
	if f["block_cache_capacity"] != strconv.Itoa(16<<20) || f["nildb_max_snapshots"] != "32" || f["latest_sequence_number"] == "0" {
		t.Errorf("INFO rocksdb = %v", f)
	}
	if !slices.Contains([]string{"yes", "no"}, f["nildb_rate_limiter_priority_shim"]) || f["nildb_statistics"] != "no" {
		t.Errorf("INFO rocksdb flags = %v", f)
	}
	if all := do(t, r2, "INFO").(string); !strings.Contains(all, "\r\n# Rocksdb\r\n") {
		t.Errorf("default INFO has no Rocksdb section")
	}
	p := infoFields(t, do(t, r2, "INFO", "persistence").(string), "# Persistence")
	if p["nildb_last_checkpoint_time"] != "0" || p["nildb_last_checkpoint_dir"] != "" {
		t.Errorf("INFO persistence before any checkpoint = %v", p)
	}
}
