package redis

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// Cases ported from Redis 7.2 tests/unit/type/hash.tcl.

func TestHashBasics(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(1), "HSET", "h", "f1", "v1")
		c.is(in(0), "HSET", "h", "f1", "v1b")
		c.is(in(2), "HSET", "h", "f2", "v2", "f3", "v3", "f2", "v2b")
		c.is(bs("v2b"), "HGET", "h", "f2")
		c.is(in(3), "HLEN", "h")
		c.is(c.null(), "HGET", "h", "nofield")
		c.is(c.null(), "HGET", "nokey", "f")
		c.is(er("ERR wrong number of arguments for 'hset' command"), "HSET", "h", "k1", "v1", "k2")
		c.is(er("ERR wrong number of arguments for 'hmset' command"), "HMSET", "h", "k1", "v1", "k2")
		c.is(okr, "HMSET", "h", "f4", "v4")
		c.is(in(0), "HSETNX", "h", "f4", "other")
		c.is(in(1), "HSETNX", "h", "f5", "v5")
		c.is(bs("v4"), "HGET", "h", "f4")
		c.is(ar(bs("v1b"), c.null(), bs("v5")), "HMGET", "h", "f1", "nofield", "f5")
		c.is(ar(c.null(), c.null()), "HMGET", "nokey", "a", "b")
		c.is(in(1), "HEXISTS", "h", "f1")
		c.is(in(0), "HEXISTS", "h", "nofield")
		c.is(in(0), "HEXISTS", "nokey", "f")
		c.is(in(3), "HSTRLEN", "h", "f1")
		c.is(in(0), "HSTRLEN", "h", "nofield")
		c.is(bulks("f1", "f2", "f3", "f4", "f5"), "HKEYS", "h")
		c.is(bulks("v1b", "v2b", "v3", "v4", "v5"), "HVALS", "h")
		c.is(c.mapOf(bs("f1"), bs("v1b"), bs("f2"), bs("v2b"), bs("f3"), bs("v3"), bs("f4"), bs("v4"), bs("f5"), bs("v5")), "HGETALL", "h")
		c.is(c.mapOf(), "HGETALL", "nokey")
		c.is(ar(), "HKEYS", "nokey")
		c.is(ar(), "HVALS", "nokey")
		c.is(in(0), "HLEN", "nokey")

		c.is(in(2), "HDEL", "h", "f1", "f2", "f2", "nofield")
		c.is(in(3), "HLEN", "h")
		c.is(in(0), "HDEL", "nokey", "f")
		c.is(in(3), "HDEL", "h", "f3", "f4", "f5")
		c.is(in(0), "EXISTS", "h")
		c.is(st("none"), "TYPE", "h")

		c.is(okr, "SET", "wrongtype", "x")
		for _, args := range [][]string{
			{"HMGET", "wrongtype", "f1", "f2"}, {"HRANDFIELD", "wrongtype"}, {"HGET", "wrongtype", "f1"},
			{"HGETALL", "wrongtype"}, {"HDEL", "wrongtype", "f1"}, {"HINCRBY", "wrongtype", "f1", "2"},
			{"HINCRBYFLOAT", "wrongtype", "f1", "2.5"}, {"HSTRLEN", "wrongtype", "f1"}, {"HVALS", "wrongtype"},
			{"HKEYS", "wrongtype"}, {"HEXISTS", "wrongtype", "f1"}, {"HSET", "wrongtype", "a", "b"},
			{"HLEN", "wrongtype"}, {"HSETNX", "wrongtype", "a", "b"}, {"HSCAN", "wrongtype", "0"},
		} {
			c.is(er("WRONGTYPE Operation against a key holding the wrong kind of value"), args...)
		}
		c.is(in(1), "HSET", "hs", "x", "1")
		c.is(er("WRONGTYPE Operation against a key holding the wrong kind of value"), "GET", "hs")
		c.is(er("WRONGTYPE Operation against a key holding the wrong kind of value"), "INCR", "hs")
		c.is(er("WRONGTYPE Operation against a key holding the wrong kind of value"), "APPEND", "hs", "x")
		c.is(ar(c.null()), "MGET", "hs")
		c.is(st("hash"), "TYPE", "hs")
	})
}

func TestHashIncr(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(2), "HINCRBY", "h", "n", "2")
		c.is(in(-1), "HINCRBY", "h", "n", "-3")
		c.is(bs("-1"), "HGET", "h", "n")
		c.is(in(17179869185), "HINCRBY", "h", "big", "17179869185")
		c.is(er("ERR value is not an integer or out of range"), "HINCRBY", "h", "n", "x")
		c.is(in(1), "HSET", "h", "str", "0")
		for _, v := range []string{" 11", "11 ", "1.5"} {
			c.is(in(0), "HSET", "h", "str", v)
			c.is(er("ERR hash value is not an integer"), "HINCRBY", "h", "str", "1")
		}
		c.is(in(0), "HSET", "h", "n", "-9223372036854775484")
		c.is(in(-9223372036854775485), "HINCRBY", "h", "n", "-1")
		c.is(er("ERR increment or decrement would overflow"), "HINCRBY", "h", "n", "-10000")

		c.is(bs("2.5"), "HINCRBYFLOAT", "hf", "foo", "2.5")
		c.is(bs("3.5"), "HINCRBYFLOAT", "hf", "foo", "1")
		c.is(bs("-0.1"), "HINCRBYFLOAT", "hf", "one", "-0.1")
		c.is(in(1), "HSET", "hf", "big", "17179869184")
		c.is(bs("34359738368"), "HINCRBYFLOAT", "hf", "big", "17179869184")
		for _, v := range []string{" 11", "11 ", "1\x002"} {
			c.is(in(0), "HSET", "hf", "big", v)
			c.is(er("ERR hash value is not a float"), "HINCRBYFLOAT", "hf", "big", "1")
		}
		c.is(er("ERR value is NaN or Infinity"), "HINCRBYFLOAT", "hf", "field", "+inf")
		c.is(er("ERR value is not a valid float"), "HINCRBYFLOAT", "hf", "field", "abc")
		c.is(in(1), "HSET", "hf", "f", "1")
		c.is(bs("-0.1"), "HINCRBYFLOAT", "hf", "f", "-1.1")
	})
}

func TestHrandfield(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(c.null(), "HRANDFIELD", "nokey")
		c.is(ar(), "HRANDFIELD", "nokey", "5")
		c.is(ar(), "HRANDFIELD", "nokey", "5", "WITHVALUES")
		c.is(okr, "HMSET", "myhash", "a", "1")
		c.is(er("ERR value is out of range"), "HRANDFIELD", "myhash", "-9223372036854770000", "withvalues")
		c.is(er("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"), "HRANDFIELD", "myhash", "-9223372036854775808", "withvalues")
		c.is(er("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"), "HRANDFIELD", "myhash", "-9223372036854775808")
		c.is(er("ERR syntax error"), "HRANDFIELD", "myhash", "1", "values")
		c.is(er("ERR syntax error"), "HRANDFIELD", "myhash", "1", "withvalues", "x")
		c.is(ar(), "HRANDFIELD", "myhash", "0")
		c.is(bs("a"), "HRANDFIELD", "myhash")
		c.is(bulks("a"), "HRANDFIELD", "myhash", "5")
		c.is(bulks("a", "a", "a"), "HRANDFIELD", "myhash", "-3")
		if c.proto == 3 {
			c.is(ar(bulks("a", "1"), bulks("a", "1")), "HRANDFIELD", "myhash", "-2", "WITHVALUES")
		} else {
			c.is(bulks("a", "1", "a", "1"), "HRANDFIELD", "myhash", "-2", "WITHVALUES")
		}
	})
	rc := e.Client(t, 3)
	ctx := context.Background()
	fields := map[string]any{}
	for i := range 100 {
		fields["f"+strconv.Itoa(i)] = i
	}
	rc.HSet(ctx, "big", fields)
	got, err := rc.HRandField(ctx, "big", 30).Result()
	if err != nil || len(got) != 30 || len(distinctStrings(got)) != 30 {
		t.Errorf("HRANDFIELD 30 = %d fields (%d distinct), %v", len(got), len(distinctStrings(got)), err)
	}
	got, err = rc.HRandField(ctx, "big", -300).Result()
	if err != nil || len(got) != 300 {
		t.Errorf("HRANDFIELD -300 = %d fields, %v", len(got), err)
	}
	got, err = rc.HRandField(ctx, "big", 1000).Result()
	if err != nil || len(got) != 100 {
		t.Errorf("HRANDFIELD 1000 = %d fields, %v", len(got), err)
	}
	pairs, err := rc.HRandFieldWithValues(ctx, "big", 5).Result()
	if err != nil || len(pairs) != 5 || fields[pairs[0].Key] == nil {
		t.Errorf("HRANDFIELD WITHVALUES = %v, %v", pairs, err)
	}
}

func distinctStrings(ss []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func TestHashExpiryAndRename(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(in(2), "HSET", "h", "a", "1", "b", "2")
	c.is(in(1), "EXPIRE", "h", "100")
	c.is(in(1), "HSET", "h", "c", "3")
	c.is(in(100), "TTL", "h")
	c.is(okr, "RENAME", "h", "h2")
	c.is(in(100), "TTL", "h2")
	c.is(bs("3"), "HGET", "h2", "c")
	c.is(in(1), "MOVE", "h2", "5")
	c.is(okr, "SELECT", "5")
	c.is(bs("1"), "HGET", "h2", "a")
	c.is(in(100), "TTL", "h2")
	e.Clock.Advance(101 * time.Second)
	c.is(c.null(), "HGET", "h2", "a")
	c.is(in(0), "HLEN", "h2")
	// A write on the expired hash starts a new, empty one.
	c.is(in(1), "HSET", "h2", "z", "26")
	c.is(ar(bs("z")), "HKEYS", "h2")
	c.is(in(-1), "TTL", "h2")
	c.is(okr, "SET", "h2", "string now")
	c.is(bs("string now"), "GET", "h2")
	c.is(in(1), "HSET", "h2b", "x", "y")
	c.is(okr, "RENAME", "h2b", "h2")
	c.is(bs("y"), "HGET", "h2", "x")
}

func TestHashMultiExec(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "MULTI")
		c.is(st("QUEUED"), "HSET", "h", "a", "1")
		c.is(st("QUEUED"), "HSET", "h", "a", "2", "b", "3")
		c.is(st("QUEUED"), "HINCRBY", "h", "a", "10")
		c.is(st("QUEUED"), "HGETALL", "h")
		c.is(st("QUEUED"), "HDEL", "h", "a", "b")
		c.is(st("QUEUED"), "EXISTS", "h")
		c.is(ar(in(1), in(1), in(12), c.mapOf(bs("a"), bs("12"), bs("b"), bs("3")), in(2), in(0)), "EXEC")
	})
}

func TestHscan(t *testing.T) {
	e := newEnv(t)
	rc := e.Client(t, 2)
	ctx := context.Background()
	fields := map[string]any{}
	for i := range 1000 {
		fields["f"+strconv.Itoa(i)] = i
	}
	rc.HSet(ctx, "h", fields)
	seen := map[string]string{}
	var cur uint64
	for {
		kv, next, err := rc.HScan(ctx, "h", cur, "", 50).Result()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i+1 < len(kv); i += 2 {
			seen[kv[i]] = kv[i+1]
		}
		if cur = next; cur == 0 {
			break
		}
	}
	if len(seen) != 1000 || seen["f7"] != "7" {
		t.Errorf("HSCAN saw %d fields, f7=%q", len(seen), seen["f7"])
	}
	c := dial(t, e, 3)
	c.is(ar(bs("0"), ar()), "HSCAN", "nokey", "0")
	c.is(ar(bs("0"), ar()), "HSCAN", "nokey", "0", "COUNT", "0")
	c.is(er("ERR invalid cursor"), "HSCAN", "h", "x")
	c.is(er("ERR syntax error"), "HSCAN", "h", "0", "TYPE", "string")
	c.is(in(3), "HSET", "small", "a", "1", "b", "2", "c", "3")
	c.is(ar(bs("0"), bulks("a", "1", "b", "2", "c", "3")), "HSCAN", "small", "0")
	c.is(ar(bs("0"), bulks("a", "b", "c")), "HSCAN", "small", "0", "NOVALUES")
	c.is(ar(bs("0"), bulks("b", "2")), "HSCAN", "small", "0", "MATCH", "b")
}
