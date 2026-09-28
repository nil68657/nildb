package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Cases ported from the SCAN part of Redis 7.2 tests/unit/scan.tcl.

// populate writes key:0 .. key:n-1 as strings, like the TCL helper.
func populate(t *testing.T, rc *goredis.Client, n int) {
	t.Helper()
	ctx := context.Background()
	pipe := rc.Pipeline()
	for i := range n {
		pipe.Set(ctx, "key:"+strconv.Itoa(i), i, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

// scanAll runs SCAN to completion and returns the distinct keys and the
// number of calls.
func scanAll(t *testing.T, rc *goredis.Client, match string, count int64, typ string) (map[string]bool, int) {
	t.Helper()
	ctx := context.Background()
	keys := map[string]bool{}
	var cur uint64
	calls := 0
	for {
		var page []string
		var err error
		if typ != "" {
			page, cur, err = rc.ScanType(ctx, cur, match, count, typ).Result()
		} else {
			page, cur, err = rc.Scan(ctx, cur, match, count).Result()
		}
		if err != nil {
			t.Fatalf("SCAN: %v", err)
		}
		calls++
		for _, k := range page {
			keys[k] = true
		}
		if cur == 0 {
			return keys, calls
		}
	}
}

func TestScanBasics(t *testing.T) {
	e := newEnv(t)
	for _, proto := range []int{2, 3} {
		rc := e.Client(t, proto)
		rc.FlushAll(context.Background())
		populate(t, rc, 1000)
		if keys, calls := scanAll(t, rc, "", 0, ""); len(keys) != 1000 || calls < 100 {
			t.Errorf("RESP%d SCAN basic: %d keys in %d calls", proto, len(keys), calls)
		}
		if keys, calls := scanAll(t, rc, "", 5, ""); len(keys) != 1000 || calls < 200 {
			t.Errorf("RESP%d SCAN COUNT 5: %d keys in %d calls", proto, len(keys), calls)
		}
		if keys, _ := scanAll(t, rc, "key:1??", 0, ""); len(keys) != 100 {
			t.Errorf("RESP%d SCAN MATCH: %d keys", proto, len(keys))
		}
		if keys, _ := scanAll(t, rc, "", 0, "list"); len(keys) != 0 {
			t.Errorf("RESP%d SCAN TYPE list: %d keys", proto, len(keys))
		}
		if keys, _ := scanAll(t, rc, "", 0, "string"); len(keys) != 1000 {
			t.Errorf("RESP%d SCAN TYPE string: %d keys", proto, len(keys))
		}
		if keys, _ := scanAll(t, rc, "key:*", 10, "STRING"); len(keys) != 1000 {
			t.Errorf("RESP%d SCAN TYPE MATCH COUNT: %d keys", proto, len(keys))
		}
		if keys, _ := scanAll(t, rc, "", 0, "string1"); len(keys) != 0 {
			t.Errorf("RESP%d SCAN unknown type: %d keys", proto, len(keys))
		}
	}
}

func TestScanExpiredKeys(t *testing.T) {
	e := newEnv(t)
	rc := e.Client(t, 2)
	populate(t, rc, 1000)
	rc.Set(context.Background(), "foo", "bar", time.Millisecond)
	e.Clock.Advance(5 * time.Millisecond)
	if keys, _ := scanAll(t, rc, "", 10, ""); len(keys) != 1000 || keys["foo"] {
		t.Errorf("SCAN with expired keys: %d keys, foo %v", len(keys), keys["foo"])
	}
	if keys, _ := scanAll(t, rc, "", 10, "string"); len(keys) != 1000 {
		t.Errorf("SCAN TYPE with expired keys: %d keys", len(keys))
	}
}

func TestScanErrors(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(er("ERR invalid cursor"), "SCAN", "abc")
		c.is(er("ERR invalid cursor"), "SCAN", " 1")
		c.is(er("ERR invalid cursor"), "SCAN", "12345")
		c.is(er("ERR syntax error"), "SCAN", "0", "COUNT", "0")
		c.is(er("ERR syntax error"), "SCAN", "0", "COUNT", "-1")
		c.is(er("ERR value is not an integer or out of range"), "SCAN", "0", "COUNT", "abc")
		c.is(er("ERR syntax error"), "SCAN", "0", "COUNT")
		c.is(er("ERR syntax error"), "SCAN", "0", "NOVALUES")
		c.is(er("ERR syntax error"), "SCAN", "0", "FOO", "bar")
		c.is(ar(bs("0"), ar()), "SCAN", "0")
		c.is(ar(bs("0"), ar()), "SCAN", "")

		c.is(okr, "MSET", "a", "1", "b", "2", "c", "3")
		reply := c.do("SCAN", "0", "COUNT", "2")
		want := "*2\r\n$"
		if len(reply) < len(want) || reply[:len(want)] != want {
			t.Fatalf("RESP%d SCAN COUNT 2 = %q", c.proto, reply)
		}
		cursor := scanCursor(t, reply)
		c.is(ar(bs("0"), bulks("c")), "SCAN", cursor)
		// The cursor belongs to db 0 and to SCAN.
		c.is(okr, "SELECT", "1")
		c.is(er("ERR invalid cursor"), "SCAN", cursor)
		c.is(okr, "SELECT", "0")
		c.is(ar(bs("0"), bulks("a", "b", "c")), "SCAN", "0", "MATCH", "*")
		c.is(ar(bs("0"), bulks("b")), "SCAN", "0", "MATCH", "b")
	})
}

// scanCursor extracts the cursor from a raw SCAN reply,
// *2\r\n$<n>\r\n<cursor>\r\n...
func scanCursor(t *testing.T, reply string) string {
	t.Helper()
	i := len("*2\r\n$")
	j := i
	for reply[j] != '\r' {
		j++
	}
	n, err := strconv.Atoi(reply[i:j])
	if err != nil {
		t.Fatalf("cursor length in %q", reply)
	}
	return reply[j+2 : j+2+n]
}

// TestScanUnderWriteLoad ports "SCAN guarantees check under write load":
// every key present for the whole iteration is returned, while other keys
// come and go.
func TestScanUnderWriteLoad(t *testing.T) {
	e := newEnv(t)
	rc := e.Client(t, 2)
	ctx := context.Background()
	populate(t, rc, 500)
	seen := map[string]bool{}
	var cur uint64
	for i := 0; ; i++ {
		page, next, err := rc.Scan(ctx, cur, "", 10).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range page {
			seen[k] = true
		}
		rc.Set(ctx, "tmp:"+strconv.Itoa(i), "x", 0)
		rc.Del(ctx, "tmp:"+strconv.Itoa(i-3))
		cur = next
		if cur == 0 {
			break
		}
	}
	for i := range 500 {
		if !seen["key:"+strconv.Itoa(i)] {
			t.Errorf("key:%d missing", i)
		}
	}
}
