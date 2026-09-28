package redis

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/nil68657/nildb/internal/config"
)

// Cases ported from Redis 7.2 tests/unit/type/set.tcl.

const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

func TestSetBasics(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(1), "SADD", "myset", "foo")
		c.is(in(1), "SADD", "myset", "bar")
		c.is(in(0), "SADD", "myset", "bar")
		c.is(in(2), "SADD", "myset", "a", "b", "a")
		c.is(in(4), "SCARD", "myset")
		c.is(in(1), "SISMEMBER", "myset", "foo")
		c.is(in(0), "SISMEMBER", "myset", "bla")
		c.is(in(0), "SISMEMBER", "noset", "bla")
		c.is(ar(in(1), in(0), in(1)), "SMISMEMBER", "myset", "foo", "bla", "bar")
		c.is(ar(in(0)), "SMISMEMBER", "myset1", "foo")
		c.is(ar(in(0), in(0)), "SMISMEMBER", "myset1", "foo", "bar")
		c.is(er("ERR wrong number of arguments for 'smismember' command"), "SMISMEMBER", "myset")
		c.is(c.setOf(bs("a"), bs("b"), bs("bar"), bs("foo")), "SMEMBERS", "myset")
		c.is(c.setOf(), "SMEMBERS", "myset1")
		c.is(in(0), "SCARD", "myset1")

		c.is(in(2), "SREM", "myset", "a", "b", "b", "zzz")
		c.is(in(0), "SREM", "myset", "a")
		c.is(in(0), "SREM", "noset", "a")
		c.is(in(2), "SREM", "myset", "foo", "bar")
		c.is(in(0), "EXISTS", "myset")

		c.is(okr, "SET", "str", "x")
		for _, args := range [][]string{
			{"SADD", "str", "bar"}, {"SMISMEMBER", "str", "bar"}, {"SMEMBERS", "str"}, {"SCARD", "str"},
			{"SISMEMBER", "str", "a"}, {"SREM", "str", "a"}, {"SPOP", "str"}, {"SRANDMEMBER", "str"},
		} {
			c.is(er(wrongType), args...)
		}
		c.is(in(1), "SADD", "s", "m")
		c.is(st("set"), "TYPE", "s")
		c.is(er(wrongType), "HGET", "s", "m")
	})
}

func TestSetOperations(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(4), "SADD", "set1", "a", "b", "c", "d")
		c.is(in(4), "SADD", "set2", "c", "d", "e", "f")
		c.is(in(2), "SADD", "set3", "d", "x")
		c.is(c.setOf(bs("c"), bs("d")), "SINTER", "set1", "set2")
		c.is(c.setOf(bs("d")), "SINTER", "set1", "set2", "set3")
		c.is(c.setOf(), "SINTER", "set1", "noset")
		c.is(c.setOf(bs("a"), bs("b"), bs("c"), bs("d")), "SINTER", "set1", "set1")
		c.is(c.setOf(bs("a"), bs("b"), bs("c"), bs("d"), bs("e"), bs("f"), bs("x")), "SUNION", "set1", "set2", "set3", "noset")
		c.is(c.setOf(bs("a"), bs("b")), "SDIFF", "set1", "set2")
		c.is(c.setOf(bs("a"), bs("b"), bs("c")), "SDIFF", "set1", "set3")
		c.is(c.setOf(), "SDIFF", "set1", "set1")
		c.is(c.setOf(), "SDIFF", "noset", "set1")
		c.is(c.setOf(bs("a"), bs("b"), bs("c"), bs("d")), "SDIFF", "set1", "noset")

		c.is(in(2), "SINTERSTORE", "dst", "set1", "set2")
		c.is(c.setOf(bs("c"), bs("d")), "SMEMBERS", "dst")
		c.is(in(7), "SUNIONSTORE", "dst", "set1", "set2", "set3")
		c.is(in(7), "SCARD", "dst")
		c.is(in(2), "SDIFFSTORE", "dst", "set1", "set2")
		c.is(c.setOf(bs("a"), bs("b")), "SMEMBERS", "dst")
		c.is(in(0), "SINTERSTORE", "dst", "set1", "noset")
		c.is(in(0), "EXISTS", "dst")
		c.is(okr, "SET", "dst", "string")
		c.is(in(1), "EXPIRE", "dst", "100")
		c.is(in(4), "SUNIONSTORE", "dst", "set1")
		c.is(in(-1), "TTL", "dst")
		c.is(in(2), "SINTERSTORE", "set1", "set1", "set2")
		c.is(c.setOf(bs("c"), bs("d")), "SMEMBERS", "set1")

		c.is(okr, "SET", "key1", "x")
		c.is(er(wrongType), "SINTER", "key1", "noset")
		c.is(er(wrongType), "SINTER", "noset", "key1")
		c.is(er(wrongType), "SUNION", "key1", "noset")
		c.is(er(wrongType), "SDIFF", "noset", "key1")
		c.is(er(wrongType), "SDIFFSTORE", "set9", "set1", "key1", "set2")
		c.is(er(wrongType), "SINTERSTORE", "set9", "noset", "key1")
		c.is(er(wrongType), "SUNIONSTORE", "set9", "key1", "noset")
		c.is(in(0), "EXISTS", "set9")
	})
}

func TestSintercard(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(5), "SADD", "myset", "a", "b", "c", "d", "e")
		c.is(in(5), "SADD", "myset2", "a", "b", "c", "d", "e")
		c.is(er("ERR wrong number of arguments for 'sintercard' command"), "SINTERCARD")
		c.is(er("ERR wrong number of arguments for 'sintercard' command"), "SINTERCARD", "1")
		c.is(er("ERR numkeys should be greater than 0"), "SINTERCARD", "0", "myset")
		c.is(er("ERR numkeys should be greater than 0"), "SINTERCARD", "a", "myset")
		c.is(er("ERR Number of keys can't be greater than number of args"), "SINTERCARD", "2", "myset")
		c.is(er("ERR Number of keys can't be greater than number of args"), "SINTERCARD", "3", "myset", "myset2")
		c.is(er("ERR syntax error"), "SINTERCARD", "1", "myset", "myset2")
		c.is(er("ERR syntax error"), "SINTERCARD", "1", "myset", "bar_arg")
		c.is(er("ERR syntax error"), "SINTERCARD", "1", "myset", "LIMIT")
		c.is(er("ERR LIMIT can't be negative"), "SINTERCARD", "1", "myset", "LIMIT", "-1")
		c.is(er("ERR LIMIT can't be negative"), "SINTERCARD", "1", "myset", "LIMIT", "a")
		c.is(in(5), "SINTERCARD", "2", "myset", "myset2")
		c.is(in(3), "SINTERCARD", "2", "myset", "myset2", "LIMIT", "3")
		c.is(in(5), "SINTERCARD", "2", "myset", "myset2", "LIMIT", "0")
		c.is(in(0), "SINTERCARD", "2", "myset", "noset")
		c.is(okr, "SET", "key1", "x")
		c.is(er(wrongType), "SINTERCARD", "1", "key1")
		c.is(er(wrongType), "SINTERCARD", "2", "myset", "key1")
	})
}

func TestSpopSrandmember(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(c.null(), "SPOP", "noset")
		c.is(c.setOf(), "SPOP", "noset", "3")
		c.is(ar(), "SRANDMEMBER", "noset", "3")
		c.is(c.null(), "SRANDMEMBER", "noset")
		c.is(er("ERR value is out of range, must be positive"), "SPOP", "noset", "-1")
		c.is(er("ERR value is out of range, must be positive"), "SPOP", "noset", "abc")
		c.is(er("ERR syntax error"), "SPOP", "noset", "1", "2")
		c.is(er("ERR syntax error"), "SRANDMEMBER", "noset", "1", "2")
		c.is(er("ERR value is not an integer or out of range"), "SRANDMEMBER", "noset", "x")
		c.is(in(1), "SADD", "one", "m")
		c.is(bs("m"), "SRANDMEMBER", "one")
		c.is(bulks("m", "m", "m"), "SRANDMEMBER", "one", "-3")
		c.is(bulks("m"), "SRANDMEMBER", "one", "3")
		c.is(c.setOf(), "SPOP", "one", "0")
		c.is(bs("m"), "SPOP", "one")
		c.is(in(0), "EXISTS", "one")
		c.is(in(2), "SADD", "two", "a", "b")
		reply := c.do("SPOP", "two", "5")
		if reply != c.setOf(bs("a"), bs("b")) && reply != c.setOf(bs("b"), bs("a")) {
			t.Errorf("RESP%d SPOP two 5 = %q", c.proto, reply)
		}
		c.is(in(0), "EXISTS", "two")
	})
	rc := e.Client(t, 2)
	ctx := context.Background()
	var members []any
	for i := range 200 {
		members = append(members, "m"+strconv.Itoa(i))
	}
	rc.SAdd(ctx, "big", members...)
	got, err := rc.SRandMemberN(ctx, "big", 50).Result()
	if err != nil || len(got) != 50 || len(distinctStrings(got)) != 50 {
		t.Errorf("SRANDMEMBER 50 = %d members, %v", len(got), err)
	}
	popped := map[string]bool{}
	for range 5 {
		batch, err := rc.SPopN(ctx, "big", 30).Result()
		if err != nil || len(batch) != 30 {
			t.Fatalf("SPOP 30 = %d, %v", len(batch), err)
		}
		for _, m := range batch {
			if popped[m] {
				t.Fatalf("SPOP returned %s twice", m)
			}
			popped[m] = true
		}
	}
	if n, _ := rc.SCard(ctx, "big").Result(); n != 50 {
		t.Errorf("SCARD after popping 150 of 200 = %d", n)
	}
	rest, _ := rc.SMembers(ctx, "big").Result()
	for _, m := range rest {
		if popped[m] {
			t.Errorf("%s popped but still a member", m)
		}
	}
}

func TestSmove(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(3), "SADD", "myset1", "a", "b", "c")
		c.is(in(2), "SADD", "myset2", "c", "d")
		c.is(in(1), "SMOVE", "myset1", "myset2", "a")
		c.is(c.setOf(bs("b"), bs("c")), "SMEMBERS", "myset1")
		c.is(c.setOf(bs("a"), bs("c"), bs("d")), "SMEMBERS", "myset2")
		c.is(in(1), "SMOVE", "myset1", "myset2", "c")
		c.is(c.setOf(bs("a"), bs("c"), bs("d")), "SMEMBERS", "myset2")
		c.is(in(0), "SMOVE", "myset1", "myset2", "zzz")
		c.is(in(1), "SMOVE", "myset1", "newset", "b")
		c.is(in(0), "EXISTS", "myset1")
		c.is(c.setOf(bs("b")), "SMEMBERS", "newset")
		c.is(in(0), "SMOVE", "noset", "myset2", "a")
		c.is(in(1), "SMOVE", "myset2", "myset2", "a")
		c.is(in(0), "SMOVE", "myset2", "myset2", "zzz")
		c.is(okr, "SET", "x", "10")
		c.is(er(wrongType), "SMOVE", "x", "myset2", "a")
		c.is(er(wrongType), "SMOVE", "myset2", "x", "a")
		c.is(in(0), "SMOVE", "noset", "x", "a")
	})
}

func TestSetOpLimit(t *testing.T) {
	e := newEnvWith(t, func(cfg *config.Config) { cfg.SetOpMaxMembers = 3 })
	c := dial(t, e, 2)
	c.is(in(2), "SADD", "a", "1", "2")
	c.is(in(2), "SADD", "b", "3", "4")
	c.is(er("ERR set operation exceeds nildb.setop-max-members (3)"), "SUNION", "a", "b")
	c.is(er("ERR set operation exceeds nildb.setop-max-members (3)"), "SUNIONSTORE", "d", "a", "b")
	c.is(in(0), "EXISTS", "d")
	c.is(bulks("1", "2"), "SUNION", "a", "a")
	c.is(bulks("1", "2"), "SDIFF", "a", "b")
}

func TestSscan(t *testing.T) {
	e := newEnv(t)
	rc := e.Client(t, 2)
	ctx := context.Background()
	var members []any
	for i := range 500 {
		members = append(members, strconv.Itoa(i))
	}
	rc.SAdd(ctx, "s", members...)
	var all []string
	var cur uint64
	for {
		page, next, err := rc.SScan(ctx, "s", cur, "", 20).Result()
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
		if cur = next; cur == 0 {
			break
		}
	}
	sort.Strings(all)
	if len(all) != 500 || len(distinctStrings(all)) != 500 {
		t.Errorf("SSCAN returned %d members (%d distinct)", len(all), len(distinctStrings(all)))
	}
	c := dial(t, e, 2)
	c.is(ar(bs("0"), bulks("10", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109")), "SSCAN", "s", "0", "MATCH", "10*", "COUNT", "1000")
	c.is(er(wrongType), "HSCAN", "s", "0")
}
