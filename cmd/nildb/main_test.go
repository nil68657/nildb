package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nil68657/nildb/internal/store"
)

// nildbBinary returns $NILDB_BIN (bin/nildb from make build) or builds
// this package into a temporary directory with this test binary's build
// tags.
func nildbBinary(t testing.TB) string {
	t.Helper()
	if bin := os.Getenv("NILDB_BIN"); bin != "" {
		return bin
	}
	bin := filepath.Join(t.TempDir(), "nildb")
	args := append(append([]string{"build"}, buildArgs(t)...), "-o", bin, ".")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// freeAddr returns a loopback address whose port was free a moment ago.
func freeAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// proc is one running nildb process.
type proc struct {
	cmd    *exec.Cmd
	out    lockedBuffer
	exited chan struct{}
	err    error // set before exited closes
}

// start runs bin on addr with the test engine (store.TestEngine) and args,
// and waits until it accepts connections. Cleanup kills it if the test
// did not stop it.
func start(t testing.TB, bin, addr string, args ...string) *proc {
	t.Helper()
	args = append([]string{"--addr", addr, "--engine", store.TestEngine()}, args...)
	p := &proc{cmd: exec.Command(bin, args...), exited: make(chan struct{})}
	p.cmd.Stdout, p.cmd.Stderr = &p.out, &p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-p.exited:
		default:
			p.cmd.Process.Kill()
			<-p.exited
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.exited:
			t.Fatalf("nildb exited early: %v\n%s", p.err, p.out.String())
		default:
		}
		if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			c.Close()
			return p
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nildb did not listen on %s:\n%s", addr, p.out.String())
	return nil
}

// stop sends sig and waits for a zero exit status. It returns the log.
func (p *proc) stop(t testing.TB, sig os.Signal) string {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.exited:
	case <-time.After(20 * time.Second):
		t.Fatalf("nildb ignored %v:\n%s", sig, p.out.String())
	}
	if p.err != nil {
		t.Fatalf("nildb exited with %v after %v:\n%s", p.err, sig, p.out.String())
	}
	return p.out.String()
}

func client(addr string, proto int) *goredis.Client {
	return goredis.NewClient(&goredis.Options{Addr: addr, Protocol: proto, MaxRetries: -1})
}

func check[T any](t *testing.T, what string, got T, err error, want T) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", what, err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %#v, want %#v", what, got, want)
	}
}

func TestBinaryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the nildb binary")
	}
	bin := nildbBinary(t)
	dir := filepath.Join(t.TempDir(), "data")
	addr := freeAddr(t)
	p := start(t, bin, addr, "--dir", dir)
	for _, proto := range []int{2, 3} {
		t.Run(fmt.Sprintf("RESP%d", proto), func(t *testing.T) {
			rdb := client(addr, proto)
			defer rdb.Close()
			drive(t, rdb, proto)
		})
	}

	ctx := context.Background()
	rdb := client(addr, 3)
	check(t, "SET survivor", rdb.Set(ctx, "survivor", "yes", 0).Val(), nil, "OK")
	cp := filepath.Join(t.TempDir(), "cp")
	if err := rdb.Do(ctx, "ROCKS.CHECKPOINT", cp).Err(); err != nil {
		t.Fatalf("ROCKS.CHECKPOINT: %v", err)
	}
	rdb.Close()
	logs := p.stop(t, syscall.SIGTERM)
	last := -1
	for _, want := range []string{"nildb: listening on " + addr, ", engine " + store.TestEngine() + " ", "nildb: shutting down", "nildb: stopped"} {
		i := strings.Index(logs, want)
		if i <= last {
			t.Errorf("log lacks %q after the previous line:\n%s", want, logs)
		}
		last = i
	}

	p = start(t, bin, addr, "--dir", dir)
	rdb = client(addr, 2)
	v, err := rdb.Get(ctx, "survivor").Result()
	check(t, "GET survivor after restart", v, err, "yes")
	n, err := rdb.Incr(ctx, "counter").Result()
	check(t, "INCR counter after restart", n, err, int64(44))
	rdb.Close()
	p.stop(t, syscall.SIGINT)

	p = start(t, bin, addr, "--readonly", cp)
	rdb = client(addr, 2)
	v, err = rdb.Get(ctx, "survivor").Result()
	check(t, "GET on --readonly", v, err, "yes")
	if err := rdb.Set(ctx, "k", "v", 0).Err(); err == nil || !strings.HasPrefix(err.Error(), "ERR ") {
		t.Errorf("SET on --readonly: %v", err)
	}
	rdb.Close()
	p.stop(t, syscall.SIGTERM)
}

// drive runs the exit-check commands on a fresh keyspace.
func drive(t *testing.T, rdb *goredis.Client, proto int) {
	ctx := context.Background()
	check(t, "FLUSHALL", rdb.FlushAll(ctx).Val(), nil, "OK")

	check(t, "SET", rdb.Set(ctx, "greeting", "hello", 0).Val(), nil, "OK")
	s, err := rdb.Get(ctx, "greeting").Result()
	check(t, "GET", s, err, "hello")
	n, err := rdb.Incr(ctx, "counter").Result()
	check(t, "INCR", n, err, int64(1))
	n, err = rdb.IncrBy(ctx, "counter", 41).Result()
	check(t, "INCRBY", n, err, int64(42))
	ok, err := rdb.Expire(ctx, "greeting", 100*time.Second).Result()
	check(t, "EXPIRE", ok, err, true)
	if ttl, err := rdb.TTL(ctx, "greeting").Result(); err != nil || ttl <= 90*time.Second || ttl > 100*time.Second {
		t.Errorf("TTL greeting = %v, %v", ttl, err)
	}
	ttl, err := rdb.Do(ctx, "TTL", "counter").Int64()
	check(t, "TTL counter", ttl, err, int64(-1))
	if err := rdb.HIncrBy(ctx, "greeting", "f", 1).Err(); err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE ") {
		t.Errorf("HINCRBY on a string: %v", err)
	}

	n, err = rdb.HSet(ctx, "user:1", "name", "Ada", "lang", "Go").Result()
	check(t, "HSET", n, err, int64(2))
	h, err := rdb.HGetAll(ctx, "user:1").Result()
	check(t, "HGETALL", h, err, map[string]string{"name": "Ada", "lang": "Go"})

	n, err = rdb.LPush(ctx, "jobs", "a", "b", "c").Result()
	check(t, "LPUSH", n, err, int64(3))
	l, err := rdb.LRange(ctx, "jobs", 0, -1).Result()
	check(t, "LRANGE", l, err, []string{"c", "b", "a"})

	n, err = rdb.SAdd(ctx, "tags", "x", "y", "z", "x").Result()
	check(t, "SADD", n, err, int64(3))
	m, err := rdb.SMembers(ctx, "tags").Result()
	slices.Sort(m)
	check(t, "SMEMBERS", m, err, []string{"x", "y", "z"})

	n, err = rdb.ZAdd(ctx, "board", goredis.Z{Score: 1.5, Member: "a"}, goredis.Z{Score: 3, Member: "c"},
		goredis.Z{Score: 2, Member: "b"}).Result()
	check(t, "ZADD", n, err, int64(3))
	z, err := rdb.ZRangeWithScores(ctx, "board", 0, -1).Result()
	check(t, "ZRANGE WITHSCORES", z, err, []goredis.Z{{Score: 1.5, Member: "a"}, {Score: 2, Member: "b"}, {Score: 3, Member: "c"}})
	raw, err := rdb.Do(ctx, "ZRANGE", "board", "0", "1", "WITHSCORES").Result()
	wantRaw := any([]any{"a", "1.5", "b", "2"})
	if proto == 3 {
		wantRaw = []any{[]any{"a", 1.5}, []any{"b", float64(2)}}
	}
	check(t, "raw ZRANGE WITHSCORES", raw, err, wantRaw)

	// Redis's GEOADD, GEODIST and GEOSEARCH documentation examples.
	n, err = rdb.GeoAdd(ctx, "Sicily", &goredis.GeoLocation{Name: "Palermo", Longitude: 13.361389, Latitude: 38.115556},
		&goredis.GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669}).Result()
	check(t, "GEOADD", n, err, int64(2))
	n, err = rdb.Do(ctx, "GEOADD", "Sicily", "12.758489", "38.788135", "edge1", "17.241510", "38.788135", "edge2").Int64()
	check(t, "GEOADD edges", n, err, int64(2))
	s, err = rdb.Do(ctx, "GEODIST", "Sicily", "Palermo", "Catania").Text()
	check(t, "GEODIST", s, err, "166274.1516")
	raw, err = rdb.Do(ctx, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC").Result()
	check(t, "GEOSEARCH BYRADIUS", raw, err, any([]any{"Catania", "Palermo"}))
	raw, err = rdb.Do(ctx, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST").Result()
	coord := func(s string) any {
		if proto == 2 {
			return s
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	hit := func(name, dist, lon, lat string) any { return []any{name, dist, []any{coord(lon), coord(lat)}} }
	check(t, "GEOSEARCH BYBOX WITHCOORD WITHDIST", raw, err, any([]any{
		hit("Catania", "56.4413", "15.08726745843887329", "37.50266842333162032"),
		hit("Palermo", "190.4424", "13.36138933897018433", "38.11555639549629859"),
		hit("edge2", "279.7403", "17.24151045083999634", "38.78813451624225195"),
		hit("edge1", "279.7405", "12.7584877610206604", "38.78813451624225195"),
	}))

	cmds, err := rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.Incr(ctx, "counter")
		p.LPush(ctx, "jobs", "d")
		p.Get(ctx, "greeting")
		return nil
	})
	if err != nil || len(cmds) != 3 {
		t.Fatalf("MULTI/EXEC: %v, %d replies", err, len(cmds))
	}
	check(t, "EXEC INCR", cmds[0].(*goredis.IntCmd).Val(), nil, int64(43))
	check(t, "EXEC LPUSH", cmds[1].(*goredis.IntCmd).Val(), nil, int64(4))
	check(t, "EXEC GET", cmds[2].(*goredis.StringCmd).Val(), nil, "hello")

	var keys []string
	for cursor, calls := uint64(0), 0; ; calls++ {
		page, next, err := rdb.Scan(ctx, cursor, "*", 2).Result()
		if err != nil || calls > 100 {
			t.Fatalf("SCAN %d: %v after %d calls", cursor, err, calls)
		}
		keys = append(keys, page...)
		if cursor = next; cursor == 0 {
			break
		}
	}
	slices.Sort(keys)
	check(t, "SCAN keys", keys, nil, []string{"Sicily", "board", "counter", "greeting", "jobs", "tags", "user:1"})

	info, err := rdb.Do(ctx, "ROCKS.INFO").Result()
	if err != nil {
		t.Fatalf("ROCKS.INFO: %v", err)
	}
	var version any
	switch v := info.(type) {
	case map[any]any:
		if proto == 3 {
			version = v["version"]
		}
	case []any:
		if proto == 2 && len(v) > 1 && v[0] == "version" {
			version = v[1]
		}
	}
	if s, _ := version.(string); strings.Count(s, ".") != 2 {
		t.Errorf("ROCKS.INFO over RESP%d = %#v", proto, info)
	}
}

func TestBinaryFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the nildb binary")
	}
	bin := nildbBinary(t)
	out, err := exec.Command(bin, "-h").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "Usage: nildb [flags]") || !strings.Contains(string(out), "-rocks-stats") {
		t.Errorf("nildb -h: %v\n%s", err, out)
	}
	out, err = exec.Command(bin, "--no-such-flag").CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 || !strings.Contains(string(out), "no-such-flag") {
		t.Errorf("nildb --no-such-flag: %v\n%s", err, out)
	}

	conf := filepath.Join(t.TempDir(), "nildb.conf")
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(conf, []byte("# test\nnildb.dir = \""+dir+"\"\nfsync=always\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	p := start(t, bin, addr, "--config", conf)
	rdb := client(addr, 2)
	info, err := rdb.Info(context.Background(), "persistence").Result()
	rdb.Close()
	if err != nil || !strings.Contains(info, "nildb_fsync:always\r\n") {
		t.Errorf("INFO persistence = %q, %v", info, err)
	}
	if logs := p.stop(t, syscall.SIGTERM); !strings.Contains(logs, "data in "+dir+", fsync always") {
		t.Errorf("log does not name the config file's settings:\n%s", logs)
	}
}
