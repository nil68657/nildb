package redis

import (
	"strconv"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/testutil"
)

// newEnv starts a server with this package's commands registered.
func newEnv(t *testing.T) *testutil.Env { return newEnvWith(t, nil) }

// newEnvWith is newEnv with a configuration change.
func newEnvWith(t *testing.T, mutate func(*config.Config)) *testutil.Env {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	return testutil.Spawn(t, mutate, func(r *command.Registry, _ *testutil.Env) error { return Register(r) })
}

// conn is a raw RESP client speaking one protocol version.
type conn struct {
	t     *testing.T
	c     *testutil.TCP
	proto int
}

// dial opens a raw connection and switches it to proto with HELLO.
func dial(t *testing.T, e *testutil.Env, proto int) *conn {
	t.Helper()
	c := &conn{t: t, c: e.TCP(t), proto: proto}
	if proto == 3 {
		c.c.Do("HELLO", "3")
	}
	return c
}

// is sends args and checks the reply byte for byte.
func (c *conn) is(want string, args ...string) {
	c.t.Helper()
	if got := string(c.c.Do(args...)); got != want {
		c.t.Errorf("RESP%d %q\n got %q\nwant %q", c.proto, strings.Join(args, " "), got, want)
	}
}

// do sends args and returns the raw reply.
func (c *conn) do(args ...string) string {
	c.t.Helper()
	return string(c.c.Do(args...))
}

// hasPrefix sends args and checks that the reply starts with prefix.
func (c *conn) hasPrefix(prefix string, args ...string) {
	c.t.Helper()
	if got := string(c.c.Do(args...)); !strings.HasPrefix(got, prefix) {
		c.t.Errorf("RESP%d %q\n got %q\nwant prefix %q", c.proto, strings.Join(args, " "), got, prefix)
	}
}

// Encoded replies.
const okr = "+OK\r\n"

func bs(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
func in(n int64) string  { return ":" + strconv.FormatInt(n, 10) + "\r\n" }
func er(s string) string { return "-" + s + "\r\n" }
func st(s string) string { return "+" + s + "\r\n" }
func ar(items ...string) string {
	return "*" + strconv.Itoa(len(items)) + "\r\n" + strings.Join(items, "")
}

// bulks encodes an array of bulk strings.
func bulks(items ...string) string {
	enc := make([]string, len(items))
	for i, s := range items {
		enc[i] = bs(s)
	}
	return ar(enc...)
}

// null is the null bulk string of the connection's protocol.
func (c *conn) null() string {
	if c.proto == 3 {
		return "_\r\n"
	}
	return "$-1\r\n"
}

// nullArr is the null array of the connection's protocol.
func (c *conn) nullArr() string {
	if c.proto == 3 {
		return "_\r\n"
	}
	return "*-1\r\n"
}

// dbl is a double reply printed as s.
func (c *conn) dbl(s string) string {
	if c.proto == 3 {
		return "," + s + "\r\n"
	}
	return bs(s)
}

// mapOf encodes a map of already encoded keys and values.
func (c *conn) mapOf(kv ...string) string {
	if c.proto == 3 {
		return "%" + strconv.Itoa(len(kv)/2) + "\r\n" + strings.Join(kv, "")
	}
	return ar(kv...)
}

// setOf encodes a set of already encoded items.
func (c *conn) setOf(items ...string) string {
	if c.proto == 3 {
		return "~" + strconv.Itoa(len(items)) + "\r\n" + strings.Join(items, "")
	}
	return ar(items...)
}

// bothProtos runs fn once per protocol, each on a fresh connection after
// FLUSHALL.
func bothProtos(t *testing.T, e *testutil.Env, fn func(c *conn)) {
	for _, proto := range []int{2, 3} {
		c := dial(t, e, proto)
		c.is(okr, "FLUSHALL")
		fn(c)
	}
}
