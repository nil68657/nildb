package redis

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Cases ported from Redis 7.2 tests/unit/keyspace.tcl.

func TestDelExistsKeys(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "x", "foo")
		c.is(bs("foo"), "GET", "x")
		c.is(in(1), "DEL", "x")
		c.is(c.null(), "GET", "x")

		c.is(okr, "SET", "foo1", "a")
		c.is(okr, "SET", "foo2", "b")
		c.is(okr, "SET", "foo3", "c")
		c.is(in(3), "DEL", "foo1", "foo2", "foo3", "foo4")
		c.is(ar(c.null(), c.null(), c.null()), "MGET", "foo1", "foo2", "foo3")
		c.is(in(0), "DEL", "foo1")

		for _, k := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
			c.is(okr, "SET", k, "hello")
		}
		c.is(bulks("foo_a", "foo_b", "foo_c"), "KEYS", "foo*")
		c.is(bulks("foo_a", "foo_b", "foo_c", "key_x", "key_y", "key_z"), "KEYS", "*")
		c.is(bulks("key_x", "key_y"), "KEYS", "key_[xy]")
		c.is(bulks(), "KEYS", "nomatch*")
		c.is(in(6), "DBSIZE")
		c.is(in(6), "DEL", "key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c")
		c.is(in(0), "DBSIZE")

		c.is(okr, "SET", "newkey", "test")
		c.is(in(1), "EXISTS", "newkey")
		c.is(in(2), "EXISTS", "newkey", "newkey")
		c.is(in(1), "EXISTS", "newkey", "nokey")
		c.is(in(1), "DEL", "newkey")
		c.is(in(0), "EXISTS", "newkey")
		c.is(in(0), "TOUCH", "newkey")

		c.is(okr, "SET", "", "foo")
		c.is(bs("foo"), "GET", "")
		c.is(in(1), "EXISTS", "")
		c.is(in(1), "DEL", "")

		c.is(st("none"), "TYPE", "nokey")
		c.is(okr, "SET", "s", "v")
		c.is(st("string"), "TYPE", "s")
		c.is(in(2), "TOUCH", "s", "s", "nokey")
		c.is(in(1), "DEL", "s", "s")
	})
}

func TestDelExpiredKey(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "PSETEX", "foo1", "10", "a")
	c.is(okr, "SET", "foo2", "b")
	e.Clock.Advance(20 * time.Millisecond)
	c.is(in(1), "DEL", "foo1", "foo2")
	c.is(in(0), "DBSIZE")
	c.is(okr, "SET", "live", "v")
	c.is(okr, "SET", "dead", "v", "PX", "5")
	e.Clock.Advance(10 * time.Millisecond)
	c.is(bulks("live"), "KEYS", "*")
	c.is(in(1), "DBSIZE")
	c.is(in(1), "EXISTS", "live", "dead")
	c.is(bs("live"), "RANDOMKEY")
}

func TestRename(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "mykey", "hello")
		c.is(okr, "RENAME", "mykey", "mykey1")
		c.is(okr, "RENAME", "mykey1", "mykey2")
		c.is(bs("hello"), "GET", "mykey2")
		c.is(in(0), "EXISTS", "mykey")
		c.is(okr, "SET", "mykey", "a")
		c.is(okr, "SET", "mykey2", "b")
		c.is(okr, "RENAME", "mykey2", "mykey")
		c.is(bs("b"), "GET", "mykey")
		c.is(in(0), "EXISTS", "mykey2")

		c.is(in(1), "DEL", "mykey")
		c.is(okr, "SET", "mykey", "foobar")
		c.is(in(1), "RENAMENX", "mykey", "mykey2")
		c.is(bs("foobar"), "GET", "mykey2")
		c.is(in(0), "EXISTS", "mykey")
		c.is(okr, "SET", "mykey", "a")
		c.is(okr, "SET", "mykey2", "b")
		c.is(in(0), "RENAMENX", "mykey", "mykey2")
		c.is(bs("b"), "GET", "mykey2")

		c.is(er("ERR no such key"), "RENAME", "nokey", "foobar")
		c.is(okr, "RENAME", "mykey", "mykey")
		c.is(in(0), "RENAMENX", "mykey", "mykey")
		c.is(er("ERR no such key"), "RENAME", "nokey", "nokey")

		c.is(okr, "SET", "mykey", "foo")
		c.is(in(1), "EXPIRE", "mykey", "100")
		c.is(okr, "RENAME", "mykey", "mykey2")
		c.is(in(100), "TTL", "mykey2")
		c.is(in(-2), "TTL", "mykey")

		c.is(okr, "SET", "mykey", "foo")
		c.is(okr, "SET", "mykey2", "bar")
		c.is(in(1), "EXPIRE", "mykey2", "100")
		c.is(okr, "RENAME", "mykey", "mykey2")
		c.is(in(-1), "TTL", "mykey2")
	})
}

func TestMove(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SELECT", "9")
		c.is(okr, "FLUSHDB")
		c.is(okr, "SELECT", "10")
		c.is(okr, "FLUSHDB")
		c.is(okr, "SELECT", "9")
		c.is(okr, "SET", "mykey", "foobar")
		c.is(in(1), "MOVE", "mykey", "10")
		c.is(in(0), "EXISTS", "mykey")
		c.is(okr, "SELECT", "10")
		c.is(bs("foobar"), "GET", "mykey")
		c.is(okr, "SELECT", "9")
		c.is(okr, "SET", "mykey", "hello")
		c.is(in(0), "MOVE", "mykey", "10")
		c.is(er("ERR value is not an integer or out of range"), "MOVE", "mykey", "notanumber")
		c.is(er("ERR DB index is out of range"), "MOVE", "mykey", "16")
		c.is(er("ERR DB index is out of range"), "MOVE", "mykey", "-1")
		c.is(er("ERR value is out of range, value must between -2147483648 and 2147483647"), "MOVE", "mykey", "99999999999")
		c.is(er("ERR source and destination objects are the same"), "MOVE", "mykey", "9")
		c.is(in(0), "MOVE", "nokey", "11")

		c.is(okr, "SELECT", "10")
		c.is(okr, "FLUSHDB")
		c.is(okr, "SELECT", "9")
		c.is(okr, "SET", "mykey", "foo")
		c.is(in(1), "EXPIRE", "mykey", "100")
		c.is(in(1), "MOVE", "mykey", "10")
		c.is(in(-2), "TTL", "mykey")
		c.is(okr, "SELECT", "10")
		c.is(in(100), "TTL", "mykey")
		c.is(okr, "SELECT", "9")
		c.is(okr, "SET", "nottl", "v")
		c.is(in(1), "MOVE", "nottl", "10")
		c.is(okr, "SELECT", "10")
		c.is(in(-1), "TTL", "nottl")
		c.is(okr, "SELECT", "0")
	})
}

func TestKeysInDifferentDBs(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "a", "hello")
	c.is(okr, "SET", "b", "world")
	c.is(okr, "SELECT", "10")
	c.is(okr, "SET", "a", "foo")
	c.is(okr, "SET", "b", "bared")
	c.is(okr, "SELECT", "0")
	c.is(bulks("hello", "world"), "MGET", "a", "b")
	c.is(okr, "SELECT", "10")
	c.is(bulks("foo", "bared"), "MGET", "a", "b")
	c.is(okr, "FLUSHDB")
	c.is(in(0), "DBSIZE")
	c.is(okr, "SELECT", "0")
	c.is(in(2), "DBSIZE")
	c.is(okr, "FLUSHALL", "SYNC")
	c.is(in(0), "DBSIZE")
	c.is(er("ERR syntax error"), "FLUSHALL", "NOW")
	c.is(er("ERR syntax error"), "FLUSHDB", "ASYNC", "SYNC")
	c.is(okr, "FLUSHDB", "async")
	c.is(okr, "MULTI")
	c.is(er("ERR Command not allowed inside a transaction"), "FLUSHDB")
	c.is(er("EXECABORT Transaction discarded because of previous errors."), "EXEC")
}

func TestRandomkey(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(c.null(), "RANDOMKEY")
	c.is(okr, "SET", "foo", "x")
	c.is(okr, "SET", "bar", "y")
	seen := map[string]bool{}
	for range 200 {
		seen[c.do("RANDOMKEY")] = true
	}
	if !seen[bs("foo")] || !seen[bs("bar")] || len(seen) != 2 {
		t.Errorf("RANDOMKEY returned %v", seen)
	}
	c.is(okr, "FLUSHDB")
	c.is(okr, "SET", "x", "10")
	c.is(in(1), "DEL", "x")
	c.is(c.null(), "RANDOMKEY")
}

func TestKeysLongKeyAndPatterns(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "dlskeriewrioeuwqoirueioqwrueoqwrueqw", "test")
	c.is(bulks("dlskeriewrioeuwqoirueioqwrueoqwrueqw"), "KEYS", "*")
	c.is(bulks("dlskeriewrioeuwqoirueioqwrueoqwrueqw"), "KEYS", "*")
	c.is(okr, "FLUSHDB")
	c.is(okr, "SET", strings.Repeat("a", 40), "1")
	c.is(bulks(), "KEYS", "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b")
	c.is(okr, "FLUSHDB")
	c.is(okr, "SET", strings.Repeat("a", 50000), "1")
	c.is(bulks(), "KEYS", strings.Repeat("*?", 50000))
}

func TestWatchTouchRules(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	other := dial(t, e, 2)

	// DEL of an absent key touches nothing, so EXEC runs.
	c.is(okr, "WATCH", "w")
	other.is(in(0), "DEL", "w")
	c.is(okr, "MULTI")
	c.is(st("QUEUED"), "PING")
	c.is(ar(st("PONG")), "EXEC")

	// A failed SET NX touches nothing either.
	c.is(okr, "SET", "w", "1")
	c.is(okr, "WATCH", "w")
	other.is(c.null(), "SET", "w", "2", "NX")
	c.is(okr, "MULTI")
	c.is(st("QUEUED"), "GET", "w")
	c.is(ar(bs("1")), "EXEC")

	// A real write does.
	c.is(okr, "WATCH", "w")
	other.is(in(2), "INCR", "w")
	c.is(okr, "MULTI")
	c.is(st("QUEUED"), "GET", "w")
	c.is(c.nullArr(), "EXEC")

	// FLUSHDB touches watched keys that exist.
	c.is(okr, "WATCH", "w")
	other.is(okr, "FLUSHDB")
	c.is(okr, "MULTI")
	c.is(st("QUEUED"), "PING")
	c.is(c.nullArr(), "EXEC")
}

func TestMultiExecStrings(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "MULTI")
		c.is(st("QUEUED"), "SET", "k", "1")
		c.is(st("QUEUED"), "INCR", "k")
		c.is(st("QUEUED"), "INCRBY", "k", "x")
		c.is(st("QUEUED"), "APPEND", "k", "0")
		c.is(st("QUEUED"), "GET", "k")
		c.is(ar(okr, in(2), er("ERR value is not an integer or out of range"), in(2), bs("20")), "EXEC")
		c.is(okr, "MULTI")
		c.is(st("QUEUED"), "RENAME", "k", "k2")
		c.is(st("QUEUED"), "GET", "k2")
		c.is(st("QUEUED"), "EXISTS", "k")
		c.is(ar(okr, bs("20"), in(0)), "EXEC")
	})
}

func TestGenericGoRedis(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	for _, proto := range []int{2, 3} {
		rc := e.Client(t, proto)
		rc.FlushAll(ctx)
		for i := range 20 {
			rc.Set(ctx, "key:"+strconv.Itoa(i), i, 0)
		}
		keys, err := rc.Keys(ctx, "key:1*").Result()
		sort.Strings(keys)
		if err != nil || strings.Join(keys, ",") != "key:1,key:10,key:11,key:12,key:13,key:14,key:15,key:16,key:17,key:18,key:19" {
			t.Errorf("RESP%d KEYS = %v, %v", proto, keys, err)
		}
		if n, err := rc.DBSize(ctx).Result(); err != nil || n != 20 {
			t.Errorf("RESP%d DBSIZE = %d, %v", proto, n, err)
		}
		if n, err := rc.Del(ctx, "key:1", "key:2", "nokey").Result(); err != nil || n != 2 {
			t.Errorf("RESP%d DEL = %d, %v", proto, n, err)
		}
		if typ, err := rc.Type(ctx, "key:3").Result(); err != nil || typ != "string" {
			t.Errorf("RESP%d TYPE = %q, %v", proto, typ, err)
		}
		if k, err := rc.RandomKey(ctx).Result(); err != nil || !strings.HasPrefix(k, "key:") {
			t.Errorf("RESP%d RANDOMKEY = %q, %v", proto, k, err)
		}
	}
}
