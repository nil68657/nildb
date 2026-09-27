package testutil_test

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/testutil"
)

func skipShort(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
}

func TestTenThousandPipelinedPings(t *testing.T) {
	skipShort(t)
	e := testutil.Spawn(t, nil)
	c := e.TCP(t)
	const n = 10000
	c.Send(bytes.Repeat([]byte("PING\r\n"), n))
	c.Expect(t, bytes.Repeat([]byte("+PONG\r\n"), n))
	c.Send(bytes.Repeat(testutil.Encode("PING", "hello"), n))
	c.Expect(t, bytes.Repeat([]byte("$5\r\nhello\r\n"), n))
	if got := string(c.Do("ECHO", "done")); got != "$4\r\ndone\r\n" {
		t.Errorf("after the batch: %q", got)
	}
}

func TestReadWholeAggregates(t *testing.T) {
	skipShort(t)
	e := testutil.Spawn(t, nil)
	c := e.TCP(t)
	id := strings.TrimSpace(strings.TrimPrefix(string(c.Do("CLIENT", "ID")), ":"))
	got := string(c.Do("HELLO", "3"))
	want := "%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:" + id +
		"\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
	if got != want {
		t.Errorf("HELLO 3\n got %q\nwant %q", got, want)
	}
	if got := string(c.Do("GET", "nothing")); got != "-ERR unknown command 'GET', with args beginning with: 'nothing' \r\n" {
		t.Errorf("no command packages registered, yet %q", got)
	}
	if got := string(c.Do("PING")); got != "+PONG\r\n" {
		t.Errorf("PING %q", got)
	}
}

func TestGoRedisHandshakeBothProtocols(t *testing.T) {
	skipShort(t)
	e := testutil.Spawn(t, nil)
	ctx := context.Background()
	for _, proto := range []int{2, 3} {
		rc := e.Client(t, proto)
		if err := rc.Ping(ctx).Err(); err != nil {
			t.Fatalf("RESP%d ping: %v", proto, err)
		}
		info, err := rc.ClientInfo(ctx).Result()
		if err != nil {
			t.Fatalf("RESP%d CLIENT INFO: %v", proto, err)
		}
		if info.Resp != proto || !strings.HasPrefix(info.LibName, "go-redis") || info.LibVer == "" {
			t.Errorf("RESP%d CLIENT INFO %+v", proto, info)
		}
		vals, err := rc.ConfigGet(ctx, "databases").Result()
		if err != nil || vals["databases"] != "16" {
			t.Errorf("RESP%d CONFIG GET databases = %v, %v", proto, vals, err)
		}
		text, err := rc.Info(ctx, "server").Result()
		if err != nil || !strings.Contains(text, "redis_version:7.2.0") {
			t.Errorf("RESP%d INFO server: %v %q", proto, err, text)
		}
		cmds, err := rc.Command(ctx).Result()
		if err != nil || cmds["ping"] == nil || cmds["ping"].Arity != -1 {
			t.Errorf("RESP%d COMMAND: %v", proto, err)
		}
	}
}

func TestRequirePass(t *testing.T) {
	skipShort(t)
	e := testutil.Spawn(t, func(c *config.Config) { c.RequirePass = "hunter2" })
	ctx := context.Background()
	for _, proto := range []int{2, 3} {
		if err := e.Client(t, proto).Ping(ctx).Err(); err != nil {
			t.Errorf("RESP%d authenticated ping: %v", proto, err)
		}
	}
	c := e.TCP(t)
	if got := string(c.Do("PING")); got != "-NOAUTH Authentication required.\r\n" {
		t.Errorf("unauthenticated PING: %q", got)
	}
}

func TestClockAndRegisterFunc(t *testing.T) {
	skipShort(t)
	var seen *testutil.Env
	e := testutil.Spawn(t, nil, func(r *command.Registry, env *testutil.Env) error {
		seen = env
		if env.Store == nil || env.Server == nil || env.Cfg == nil {
			t.Error("RegisterFunc ran before the Env was filled")
		}
		var ir command.InfoRegistrar = env.Server
		ir.RegisterInfoSection("probe", func(b *strings.Builder) { b.WriteString("probe_ok:1\r\n") })
		r.Register(command.Spec{Name: "now.ms", Arity: 1, Run: func(c *command.Ctx, _ [][]byte) resp.Reply {
			return resp.Int(c.NowMS())
		}})
		return nil
	})
	if seen != e || e.Clock == nil {
		t.Fatal("RegisterFunc got another Env, or the clock is missing")
	}
	c := e.TCP(t)
	read := func() int64 {
		t.Helper()
		n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(string(c.Do("NOW.MS")), ":")), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := read()
	e.Clock.Advance(time.Hour)
	if d := read() - before; d < 3_600_000 || d > 3_610_000 {
		t.Errorf("clock moved %d ms after Advance(1h)", d)
	}
	if got := string(c.Do("INFO", "probe")); !strings.Contains(got, "# Probe\r\nprobe_ok:1\r\n") {
		t.Errorf("INFO probe = %q", got)
	}
}

func TestStopIsIdempotentAndClosesConnections(t *testing.T) {
	skipShort(t)
	e := testutil.Spawn(t, nil)
	c := e.TCP(t)
	if got := string(c.Do("PING")); got != "+PONG\r\n" {
		t.Fatalf("PING %q", got)
	}
	e.Stop()
	e.Stop()
	c.ExpectClosed(t)
}
