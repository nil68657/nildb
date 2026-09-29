package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
)

// fakeNilDB is a small RESP server: HELLO, COMMAND (built by NilDB's own
// command table code), SELECT, GET, SET, ECHO, MULTI/EXEC, and a few
// commands that send what NilDB never does: pushes, attributes, huge or
// slow replies.
type fakeNilDB struct {
	ln  net.Listener
	reg *command.Registry

	mu    sync.Mutex
	conns map[net.Conn]bool
	data  [16]map[string]string
	log   []string
}

func fakeRegistry() *command.Registry {
	reg := command.NewRegistry()
	run := func(*command.Ctx, [][]byte) resp.Reply { return resp.OK() }
	ro, w, admin, fast := command.ReadOnly, command.Write, command.Admin, command.Fast
	reg.Register(
		command.Spec{Name: "get", Arity: 2, Flags: ro | fast, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "string", Summary: "Returns the string value of a key.", Run: run},
		command.Spec{Name: "set", Arity: -3, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "string", Summary: "Sets the string value of a key.", Run: run},
		command.Spec{Name: "echo", Arity: 2, Flags: fast, Run: run},
		command.Spec{Name: "ping", Arity: -1, Flags: fast, Run: run},
		command.Spec{Name: "select", Arity: 2, Flags: fast, Run: run},
		command.Spec{Name: "hello", Arity: -1, Flags: command.NoAuth | fast, Run: run},
		command.Spec{Name: "multi", Arity: 1, Flags: command.NoMulti | fast, Run: run},
		command.Spec{Name: "exec", Arity: 1, Run: run},
		command.Spec{Name: "shutdown", Arity: -1, Flags: admin | command.NoMulti, Run: run},
		command.Spec{Name: "config", Subcommands: []command.Spec{
			{Name: "get", Arity: -3, Flags: admin, Run: run},
			{Name: "set", Arity: -4, Flags: admin, Run: run},
		}},
		command.Spec{Name: "rocks.compact", Arity: -1, Flags: admin | command.NoMulti, Run: run},
		command.Spec{Name: "rocks.snapshot", Flags: admin | command.NoMulti | ro, Subcommands: []command.Spec{
			{Name: "create", Arity: -2, Flags: admin | command.NoMulti | ro, Run: run},
		}},
		command.Spec{Name: "pushy", Arity: 1, Flags: ro, Run: run},
		command.Spec{Name: "attr", Arity: 1, Flags: ro, Run: run},
		command.Spec{Name: "big", Arity: 1, Flags: ro, Run: run},
		command.Spec{Name: "slow", Arity: 1, Flags: ro, Run: run},
		command.Spec{Name: "binary", Arity: 1, Flags: ro, Run: run},
	)
	return reg
}

func startFake(t *testing.T) *fakeNilDB {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNilDB{ln: ln, reg: fakeRegistry(), conns: make(map[net.Conn]bool)}
	for i := range f.data {
		f.data[i] = make(map[string]string)
	}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns[nc] = true
			f.mu.Unlock()
			go f.serve(nc)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		f.dropAll()
	})
	return f
}

// dropAll closes every connection, as a NilDB restart does.
func (f *fakeNilDB) dropAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		c.Close()
		delete(f.conns, c)
	}
}

func (f *fakeNilDB) logged() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func (f *fakeNilDB) count(prefix string) int {
	n := 0
	for _, l := range f.logged() {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeNilDB) serve(nc net.Conn) {
	defer nc.Close()
	r := resp.NewReader(nc, resp.Limits{})
	r.SetAuthenticated(true)
	w := resp.NewWriter(nc)
	db := 0
	var queue [][][]byte
	multi := false
	for {
		args, err := r.Next()
		if err != nil {
			return
		}
		name := strings.ToUpper(string(args[0]))
		parts := []string{name}
		for _, a := range args[1:] {
			parts = append(parts, string(a))
		}
		f.mu.Lock()
		f.log = append(f.log, strings.Join(parts, " "))
		f.mu.Unlock()
		if multi && name != "EXEC" && name != "MULTI" {
			queue = append(queue, args)
			w.Write(resp.Status("QUEUED"))
			w.Flush()
			continue
		}
		switch name {
		case "HELLO":
			w.SetProto(3)
			w.Write(resp.Map(resp.Str("server"), resp.Str("redis"), resp.Str("version"), resp.Str("7.2.0"),
				resp.Str("proto"), resp.Int(3), resp.Str("id"), resp.Int(7), resp.Str("mode"), resp.Str("standalone"),
				resp.Str("role"), resp.Str("master"), resp.Str("modules"), resp.Array()))
		case "MULTI":
			multi = true
			w.Write(resp.OK())
		case "EXEC":
			multi = false
			out := make([]resp.Reply, len(queue))
			for i, q := range queue {
				out[i] = f.run(&db, q)
			}
			queue = nil
			w.Write(resp.Array(out...))
		case "PUSHY":
			w.Raw([]byte(">2\r\n+invalidate\r\n$1\r\nk\r\n"))
			w.Write(resp.OK())
		case "ATTR":
			w.Raw([]byte("|1\r\n+ttl\r\n:5\r\n"))
			w.Write(resp.OK())
		case "SLOW":
			time.Sleep(600 * time.Millisecond)
			w.Write(resp.OK())
		default:
			w.Write(f.run(&db, args))
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (f *fakeNilDB) run(db *int, args [][]byte) resp.Reply {
	name := strings.ToUpper(string(args[0]))
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case name == "COMMAND" && len(args) == 1:
		return command.CommandInfoReply(f.reg, nil)
	case name == "COMMAND" && strings.EqualFold(string(args[1]), "docs"):
		return command.CommandDocsReply(f.reg, nil)
	case name == "PING":
		return resp.Status("PONG")
	case name == "SELECT":
		n, _ := strconv.Atoi(string(args[1]))
		if n < 0 || n > 15 {
			return resp.ErrDBIndex
		}
		*db = n
		return resp.OK()
	case name == "SET" && len(args) == 3:
		f.data[*db][string(args[1])] = string(args[2])
		return resp.OK()
	case name == "GET" && len(args) == 2:
		v, ok := f.data[*db][string(args[1])]
		if !ok {
			return resp.Null()
		}
		return resp.Str(v)
	case name == "ECHO" && len(args) == 2:
		return resp.Bulk(args[1])
	case name == "BIG":
		return resp.Str(strings.Repeat("x", 4096))
	case name == "BINARY":
		return resp.Bulk([]byte{0xff, 'x'})
	case name == "CONFIG" || name == "SHUTDOWN" || name == "ROCKS.COMPACT" || name == "ROCKS.SNAPSHOT":
		return resp.OK()
	}
	return resp.ErrUnknown(string(args[0]), args[1:])
}

// harness is a console under test on an httptest server.
type harness struct {
	t    *testing.T
	ts   *httptest.Server
	srv  *server
	host string
}

func newHarness(t *testing.T, nildb string, readonly bool, mut func(*clientConfig)) *harness {
	t.Helper()
	cfg := &clientConfig{addr: nildb, timeout: 5 * time.Second, maxReply: maxReply}
	if mut != nil {
		mut(cfg)
	}
	ts := httptest.NewUnstartedServer(nil)
	_, port, _ := net.SplitHostPort(ts.Listener.Addr().String())
	srv, err := newServer(options{nildb: nildb, readonly: readonly}, cfg, 4, port)
	if err != nil {
		t.Fatal(err)
	}
	ts.Config.Handler = srv.handler()
	ts.Start()
	t.Cleanup(func() {
		ts.Close()
		srv.close()
	})
	return &harness{t: t, ts: ts, srv: srv, host: ts.Listener.Addr().String()}
}

type result struct {
	code int
	body map[string]any
	hdr  http.Header
}

func (hs *harness) do(method, path string, body any, hdr map[string]string) result {
	hs.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			hs.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, hs.ts.URL+path, rd)
	if err != nil {
		hs.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	res, err := hs.ts.Client().Do(req)
	if err != nil {
		hs.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := result{code: res.StatusCode, hdr: res.Header}
	if strings.HasPrefix(res.Header.Get("Content-Type"), "application/json") && len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			hs.t.Fatalf("%s %s: bad JSON %q: %v", method, path, raw, err)
		}
	}
	return out
}

func (hs *harness) exec(body map[string]any) result {
	hs.t.Helper()
	return hs.do("POST", "/api/exec", body, nil)
}

// run executes one command on db and returns its reply as {"t", "v", ...}.
func (hs *harness) run(db int, args ...any) map[string]any {
	hs.t.Helper()
	r := hs.exec(map[string]any{"args": args, "db": db})
	if r.code != http.StatusOK {
		hs.t.Fatalf("%v: HTTP %d %v", args, r.code, r.body)
	}
	return r.body["reply"].(map[string]any)
}

func wantReply(t *testing.T, got map[string]any, kind string, v any) {
	t.Helper()
	if got["t"] != kind || (v != nil && fmt.Sprint(got["v"]) != fmt.Sprint(v)) {
		t.Fatalf("reply %v, want t=%s v=%v", got, kind, v)
	}
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func TestExecAgainstFake(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)

	wantReply(t, hs.run(0, "SET", "k", "v"), "simple", "OK")
	wantReply(t, hs.run(0, "GET", "k"), "bulk", "v")
	wantReply(t, hs.run(0, "GET", "nope"), "null", nil)
	wantReply(t, hs.run(0, "ECHO", 42), "bulk", "42")
	if got := hs.run(0, "ECHO", map[string]string{"b64": "/3g="}); got["b64"] != "/3g=" {
		t.Errorf("binary ECHO = %v", got)
	}
	if got := hs.run(0, "BINARY"); got["b64"] != "/3g=" || got["v"] != nil {
		t.Errorf("binary reply = %v", got)
	}
	wantReply(t, hs.run(0, "NOSUCH", "a"), "error", "ERR unknown command 'NOSUCH', with args beginning with: 'a' ")

	for _, bad := range []map[string]any{
		{"args": []any{}},
		{"args": []any{true}},
		{"args": []any{map[string]string{"b64": "!!"}}},
		{"args": []any{"GET", "k"}, "db": 16},
		{"args": []any{"GET", "k"}, "cmds": [][]any{{"GET", "k"}}},
		{"args": []any{"GET", "k"}, "atomic": true},
		{"cmds": [][]any{}},
		{"args": []any{"GET", "k"}, "session": "not valid!"},
		{"args": []any{"GET"}, "extra": 1},
	} {
		if r := hs.exec(bad); r.code != http.StatusBadRequest {
			t.Errorf("%v: HTTP %d, want 400 (%v)", bad, r.code, r.body)
		}
	}
}

func TestBatchSelectsDatabase(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)
	r := hs.exec(map[string]any{"cmds": [][]any{{"SET", "k", "three"}, {"GET", "k"}}, "db": 3})
	if r.code != http.StatusOK {
		t.Fatalf("batch: HTTP %d %v", r.code, r.body)
	}
	replies := r.body["replies"].([]any)
	wantReply(t, replies[1].(map[string]any), "bulk", "three")
	wantReply(t, hs.run(0, "GET", "k"), "null", nil)
	wantReply(t, hs.run(3, "GET", "k"), "bulk", "three")
	if f.count("SELECT 3") == 0 {
		t.Errorf("no SELECT 3 in %q", f.logged())
	}
}

func TestReadonlyUsesCommandFlags(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), true, nil)
	blocked := [][]any{
		{"SET", "k", "v"},
		{"CONFIG", "SET", "nildb.lease-ttl", "5"},
		{"SHUTDOWN"},
		{"ROCKS.COMPACT"},
	}
	for _, args := range blocked {
		r := hs.exec(map[string]any{"args": args})
		if r.code != http.StatusForbidden || r.body["blocked"] != true {
			t.Errorf("%v: HTTP %d %v, want 403 blocked", args, r.code, r.body)
		}
		r = hs.exec(map[string]any{"args": args, "session": "s1"})
		if r.code != http.StatusForbidden {
			t.Errorf("%v in a session: HTTP %d, want 403", args, r.code)
		}
	}
	for _, args := range [][]any{{"GET", "k"}, {"CONFIG", "GET", "*"}, {"ROCKS.SNAPSHOT", "CREATE"}, {"PING"}, {"NOSUCH"}} {
		if r := hs.exec(map[string]any{"args": args}); r.code != http.StatusOK {
			t.Errorf("%v: HTTP %d %v, want 200", args, r.code, r.body)
		}
	}
	if r := hs.exec(map[string]any{"cmds": [][]any{{"GET", "k"}, {"SET", "k", "v"}}}); r.code != http.StatusForbidden {
		t.Errorf("batch with a write: HTTP %d, want 403", r.code)
	}
	if f.count("SET") != 0 {
		t.Errorf("a refused write reached the server: %q", f.logged())
	}

	r := hs.do("GET", "/api/commands", nil, nil)
	if r.code != http.StatusOK {
		t.Fatalf("/api/commands: HTTP %d", r.code)
	}
	flags := map[string]bool{}
	for _, c := range r.body["commands"].([]any) {
		m := c.(map[string]any)
		flags[m["name"].(string)] = m["blocked"] == true
		for _, s := range asList(m["subcommands"]) {
			sm := s.(map[string]any)
			flags[sm["name"].(string)] = sm["blocked"] == true
		}
	}
	for name, want := range map[string]bool{"get": false, "set": true, "config|get": false, "config|set": true, "rocks.snapshot|create": false, "shutdown": true} {
		if flags[name] != want {
			t.Errorf("%s blocked = %v, want %v", name, flags[name], want)
		}
	}
}

func TestStateCommandsNeedASession(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)
	for _, name := range []string{"MULTI", "EXEC", "HELLO", "AUTH", "WATCH", "QUIT"} {
		if r := hs.exec(map[string]any{"args": []any{name}}); r.code != http.StatusForbidden {
			t.Errorf("pooled %s: HTTP %d, want 403", name, r.code)
		}
	}

	sess := func(args ...any) (map[string]any, map[string]any) {
		r := hs.exec(map[string]any{"args": args, "session": "tab-1", "db": 0})
		if r.code != http.StatusOK {
			t.Fatalf("session %v: HTTP %d %v", args, r.code, r.body)
		}
		return r.body["reply"].(map[string]any), r.body["session"].(map[string]any)
	}
	rep, st := sess("MULTI")
	wantReply(t, rep, "simple", "OK")
	if st["multi"] != true {
		t.Fatalf("after MULTI session = %v", st)
	}
	rep, _ = sess("SET", "a", "1")
	wantReply(t, rep, "simple", "QUEUED")
	rep, st = sess("EXEC")
	if rep["t"] != "array" || st["multi"] != false {
		t.Fatalf("EXEC = %v, session %v", rep, st)
	}
	_, st = sess("SELECT", "5")
	if st["db"] != float64(5) {
		t.Fatalf("after SELECT 5 session = %v", st)
	}

	r := hs.exec(map[string]any{"cmds": [][]any{{"SET", "b", "2"}, {"GET", "b"}}, "atomic": true})
	if r.code != http.StatusOK {
		t.Fatalf("atomic: HTTP %d %v", r.code, r.body)
	}
	replies := r.body["replies"].([]any)
	wantReply(t, replies[1].(map[string]any), "bulk", "2")
	if r.body["exec"].(map[string]any)["t"] != "array" {
		t.Errorf("atomic exec = %v", r.body["exec"])
	}

	if r := hs.do("POST", "/api/session/close", map[string]any{"session": "tab-1"}, nil); r.code != http.StatusNoContent {
		t.Errorf("session close: HTTP %d", r.code)
	}
	for i := range maxSessions {
		hs.exec(map[string]any{"args": []any{"PING"}, "session": fmt.Sprintf("tab-%d", i)})
	}
	if r := hs.exec(map[string]any{"args": []any{"PING"}, "session": "one-too-many"}); r.code != http.StatusTooManyRequests {
		t.Errorf("session %d: HTTP %d, want 429", maxSessions+1, r.code)
	}
}

func TestGuardRefusesOtherOrigins(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)
	body := map[string]any{"args": []any{"PING"}}
	cases := []struct {
		name string
		hdr  map[string]string
		code int
	}{
		{"curl", nil, 200},
		{"same origin", map[string]string{"Origin": "http://" + hs.host, "Sec-Fetch-Site": "same-origin"}, 200},
		{"localhost", map[string]string{"Host": "localhost:" + hs.srv.port}, 200},
		{"other origin", map[string]string{"Origin": "http://evil.example"}, 403},
		{"cross site fetch", map[string]string{"Sec-Fetch-Site": "cross-site"}, 403},
		{"same site fetch", map[string]string{"Sec-Fetch-Site": "same-site"}, 403},
		{"rebound host", map[string]string{"Host": "evil.example:" + hs.srv.port}, 403},
		{"other port", map[string]string{"Host": "127.0.0.1:1"}, 403},
		{"form post", map[string]string{"Content-Type": "text/plain"}, 415},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := hs.do("POST", "/api/exec", body, c.hdr)
			if r.code != c.code {
				t.Errorf("HTTP %d, want %d (%v)", r.code, c.code, r.body)
			}
			if r.hdr.Get("Content-Security-Policy") == "" || r.hdr.Get("X-Frame-Options") != "DENY" {
				t.Errorf("security headers missing: %v", r.hdr)
			}
		})
	}
	if r := hs.do("GET", "/api/exec", nil, nil); r.code != http.StatusMethodNotAllowed {
		t.Errorf("GET /api/exec: HTTP %d, want 405", r.code)
	}
	if r := hs.do("GET", "/", nil, map[string]string{"Host": "evil.example:" + hs.srv.port}); r.code != http.StatusForbidden {
		t.Errorf("page under a rebound host: HTTP %d, want 403", r.code)
	}
}

// TestRetryAfterRestart drops every server connection, as a NilDB restart
// does: reads are retried on a new connection, writes are not.
func TestRetryAfterRestart(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)
	wantReply(t, hs.run(0, "SET", "k", "v1"), "simple", "OK")
	f.dropAll()
	wantReply(t, hs.run(0, "GET", "k"), "bulk", "v1")
	f.dropAll()
	if r := hs.exec(map[string]any{"args": []any{"SET", "k", "v2"}}); r.code != http.StatusBadGateway || r.body["conn"] != true {
		t.Fatalf("write on a dropped connection: HTTP %d %v, want 502", r.code, r.body)
	}
	wantReply(t, hs.run(0, "SET", "k", "v3"), "simple", "OK")
	wantReply(t, hs.run(0, "GET", "k"), "bulk", "v3")
}

func TestUnusualReplies(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, func(c *clientConfig) {
		c.maxReply = 1024
		c.timeout = 250 * time.Millisecond
	})
	wantReply(t, hs.run(0, "PUSHY"), "simple", "OK")
	rep := hs.run(0, "ATTR")
	wantReply(t, rep, "simple", "OK")
	if attrs := asList(rep["attrs"]); len(attrs) != 1 {
		t.Errorf("attrs = %v", rep["attrs"])
	}
	hellos := f.count("HELLO")
	rep = hs.run(0, "BIG")
	if rep["t"] != "error" || !strings.HasPrefix(rep["v"].(string), "NILDBUI reply is larger than") {
		t.Fatalf("BIG = %v", rep)
	}
	wantReply(t, hs.run(0, "PING"), "simple", "PONG")
	if f.count("HELLO") != hellos {
		t.Errorf("an oversized reply cost a connection: %d HELLOs, want %d", f.count("HELLO"), hellos)
	}
	if r := hs.exec(map[string]any{"args": []any{"SLOW"}}); r.code != http.StatusGatewayTimeout {
		t.Fatalf("SLOW: HTTP %d %v, want 504", r.code, r.body)
	}
	wantReply(t, hs.run(0, "PING"), "simple", "PONG")
}

func TestStatus(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), true, nil)
	r := hs.do("GET", "/api/status", nil, nil)
	if r.code != http.StatusOK || r.body["connected"] != true || r.body["readonly"] != true {
		t.Fatalf("status = %d %v", r.code, r.body)
	}
	if hello := r.body["hello"].(map[string]any); hello["version"] != "7.2.0" || hello["proto"] != float64(3) {
		t.Errorf("hello = %v", hello)
	}
	f.ln.Close()
	f.dropAll()
	r = hs.do("GET", "/api/status", nil, nil)
	if r.body["connected"] != false || r.body["error"] == "" {
		t.Errorf("status with NilDB down = %v", r.body)
	}
}

func TestStaticFiles(t *testing.T) {
	f := startFake(t)
	hs := newHarness(t, f.ln.Addr().String(), false, nil)
	r := hs.do("GET", "/", nil, nil)
	if r.code != http.StatusOK || !strings.HasPrefix(r.hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("/: HTTP %d %s", r.code, r.hdr.Get("Content-Type"))
	}
	if !strings.Contains(r.hdr.Get("Content-Security-Policy"), "script-src 'self'") {
		t.Errorf("CSP = %q", r.hdr.Get("Content-Security-Policy"))
	}
	etag := r.hdr.Get("Etag")
	if etag == "" {
		t.Fatal("no ETag")
	}
	if r := hs.do("GET", "/", nil, map[string]string{"If-None-Match": etag}); r.code != http.StatusNotModified {
		t.Errorf("revalidation: HTTP %d, want 304", r.code)
	}
	for path, ctype := range map[string]string{"/js/main.js": "javascript", "/app.css": "text/css", "/js/views/keys.js": "javascript"} {
		if r := hs.do("GET", path, nil, nil); r.code != http.StatusOK || !strings.Contains(r.hdr.Get("Content-Type"), ctype) {
			t.Errorf("%s: HTTP %d %s", path, r.code, r.hdr.Get("Content-Type"))
		}
	}
	if r := hs.do("GET", "/nope.js", nil, nil); r.code != http.StatusNotFound {
		t.Errorf("/nope.js: HTTP %d, want 404", r.code)
	}
}

func TestSessionIDs(t *testing.T) {
	for id, ok := range map[string]bool{
		"0d8f7a2c-6f7e-4d6b-9d0c-3f0b1e2a4c5d": true, "tab-1": true, "": false,
		"has space": false, strings.Repeat("a", 65): false, "semi;colon": false,
	} {
		if validSessionID(id) != ok {
			t.Errorf("validSessionID(%q) = %v", id, !ok)
		}
	}
}
