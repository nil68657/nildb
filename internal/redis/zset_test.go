package redis

import (
	"context"
	"strconv"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// Cases ported from Redis 7.2 tests/unit/type/zset.tcl.

// zws encodes a WITHSCORES reply: flat in RESP2, pairs in RESP3.
func (c *conn) zws(pairs ...string) string {
	if c.proto == 2 {
		enc := make([]string, len(pairs))
		for i, p := range pairs {
			enc[i] = bs(p)
		}
		return ar(enc...)
	}
	items := make([]string, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		items = append(items, ar(bs(pairs[i]), c.dbl(pairs[i+1])))
	}
	return ar(items...)
}

func TestZaddOptions(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(3), "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
		c.is(bulks("x", "y", "z"), "ZRANGE", "ztmp", "0", "-1")
		c.is(in(0), "ZADD", "ztmp", "1", "y")
		c.is(bulks("y", "x", "z"), "ZRANGE", "ztmp", "0", "-1")
		c.is(er("ERR value is not a valid float"), "ZADD", "myzset", "nan", "abc")
		c.is(er("ERR value is not a valid float"), "ZINCRBY", "myzset", "nan", "abc")
		c.is(er("ERR value is not a valid float"), "ZADD", "myzset", "", "abc")
		c.is(er("ERR syntax error"), "ZADD", "ztmp", "xx", "10", "x", "20")
		c.is(er("ERR XX and NX options at the same time are not compatible"), "ZADD", "ztmp", "xx", "nx", "10", "x")
		c.is(er("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "ztmp", "gt", "nx", "10", "x")
		c.is(er("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "ztmp", "lt", "nx", "10", "x")
		c.is(er("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "ztmp", "lt", "gt", "10", "x")
		c.is(er("ERR INCR option supports a single increment-element pair"), "ZADD", "ztmp", "INCR", "15", "x", "10", "y")
		c.is(er("ERR wrong number of arguments for 'zincrby' command"), "ZINCRBY", "myzset", "10", "a", "20", "b", "30", "c")
		c.is(er("ERR value is not a valid float"), "ZADD", "myzset", "10", "a", "20", "b", "30.badscore", "c")
		c.is(in(0), "EXISTS", "myzset")
		c.is(er("ERR syntax error"), "ZADD", "myzset", "10", "a", "20", "b", "30", "c", "40")

		c.is(in(0), "ZADD", "zxx", "XX", "10", "x")
		c.is(in(0), "EXISTS", "zxx")
		c.is(c.null(), "ZADD", "zxx", "XX", "INCR", "10", "x")
		c.is(in(1), "ZADD", "zxx", "10", "x")
		c.is(in(0), "ZADD", "zxx", "XX", "20", "x", "30", "y")
		c.is(c.dbl("20"), "ZSCORE", "zxx", "x")
		c.is(c.null(), "ZSCORE", "zxx", "y")
		c.is(in(1), "ZADD", "zxx", "NX", "25", "x", "30", "y")
		c.is(c.dbl("20"), "ZSCORE", "zxx", "x")
		c.is(c.dbl("30"), "ZSCORE", "zxx", "y")
		c.is(in(0), "ZADD", "zxx", "GT", "10", "x")
		c.is(in(1), "ZADD", "zxx", "GT", "CH", "40", "x", "20", "y")
		c.is(c.dbl("40"), "ZSCORE", "zxx", "x")
		c.is(c.dbl("30"), "ZSCORE", "zxx", "y")
		c.is(in(2), "ZADD", "zxx", "LT", "CH", "35", "x", "50", "y", "1", "new")
		c.is(c.dbl("35"), "ZSCORE", "zxx", "x")
		c.is(c.null(), "ZADD", "zxx", "GT", "INCR", "-5", "x")
		c.is(c.dbl("45"), "ZADD", "zxx", "GT", "INCR", "10", "x")
		c.is(c.dbl("46"), "ZINCRBY", "zxx", "1", "x")

		c.is(in(3), "ZADD", "zch", "10", "x", "20", "y", "30", "z")
		c.is(in(0), "ZADD", "zch", "11", "x", "21", "y", "30", "z")
		c.is(in(2), "ZADD", "zch", "ch", "12", "x", "22", "y", "30", "z")
		c.is(in(1), "ZADD", "zch", "5", "m", "6", "m")
		c.is(c.dbl("6"), "ZSCORE", "zch", "m")

		c.is(c.dbl("inf"), "ZINCRBY", "myzset", "+inf", "abc")
		c.is(er("ERR resulting score is not a number (NaN)"), "ZINCRBY", "myzset", "-inf", "abc")
		c.is(c.dbl("inf"), "ZSCORE", "myzset", "abc")
		c.is(in(1), "ZADD", "neg", "-0", "a")
		c.is(c.dbl("0"), "ZSCORE", "neg", "a")
		c.is(c.dbl("1.5"), "ZINCRBY", "neg", "1.5", "a")
		c.is(c.dbl("0.1"), "ZINCRBY", "neg2", "0.1", "x")
		c.is(c.dbl("0.30000000000000004"), "ZINCRBY", "neg2", "0.2", "x")
	})
}

func TestZsetBasics(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(3), "ZADD", "ztmp", "10", "a", "20", "b", "30", "c")
		c.is(st("zset"), "TYPE", "ztmp")
		c.is(in(3), "ZCARD", "ztmp")
		c.is(in(0), "ZCARD", "zdoesntexist")
		c.is(in(1), "ZREM", "ztmp", "b", "b", "zzz")
		c.is(in(0), "ZREM", "nokey", "a")
		c.is(c.zws("a", "10", "c", "30"), "ZRANGE", "ztmp", "0", "-1", "WITHSCORES")
		c.is(ar(c.dbl("10"), c.null(), c.dbl("30")), "ZMSCORE", "ztmp", "a", "x", "c")
		c.is(ar(c.null(), c.null()), "ZMSCORE", "nokey", "a", "b")
		c.is(er("ERR wrong number of arguments for 'zmscore' command"), "ZMSCORE", "ztmp")
		c.is(in(2), "ZREM", "ztmp", "a", "c")
		c.is(in(0), "EXISTS", "ztmp")
		c.is(okr, "SET", "str", "x")
		for _, args := range [][]string{
			{"ZADD", "str", "1", "a"}, {"ZSCORE", "str", "a"}, {"ZRANGE", "str", "0", "1"}, {"ZCARD", "str"},
			{"ZPOPMIN", "str"}, {"ZPOPMIN", "str", "0"}, {"ZPOPMAX", "str", "2"}, {"ZRANK", "str", "a"},
			{"ZUNION", "1", "str"}, {"ZRANDMEMBER", "str"}, {"ZSCAN", "str", "0"}, {"ZMSCORE", "str", "a"},
		} {
			c.is(er(wrongType), args...)
		}
	})
}

func TestZrangeForms(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(4), "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
		c.is(bulks("a", "b", "c", "d"), "ZRANGE", "z", "0", "-1")
		c.is(bulks("a", "b", "c"), "ZRANGE", "z", "0", "-2")
		c.is(bulks("b", "c", "d"), "ZRANGE", "z", "1", "-1")
		c.is(bulks("b", "c"), "ZRANGE", "z", "1", "-2")
		c.is(bulks("c", "d"), "ZRANGE", "z", "-2", "-1")
		c.is(bulks("a", "b", "c", "d"), "ZRANGE", "z", "-10", "10")
		c.is(bulks(), "ZRANGE", "z", "5", "10")
		c.is(bulks(), "ZRANGE", "z", "0", "-10")
		c.is(bulks("d", "c", "b", "a"), "ZREVRANGE", "z", "0", "-1")
		c.is(bulks("c", "b"), "ZRANGE", "z", "1", "2", "REV")
		c.is(c.zws("d", "4", "c", "3"), "ZREVRANGE", "z", "0", "1", "WITHSCORES")
		c.is(bulks(), "ZRANGE", "nokey", "0", "-1")

		c.is(bulks("b", "c"), "ZRANGEBYSCORE", "z", "2", "3")
		c.is(bulks("c"), "ZRANGEBYSCORE", "z", "(2", "3")
		c.is(bulks("b"), "ZRANGEBYSCORE", "z", "2", "(3")
		c.is(bulks(), "ZRANGEBYSCORE", "z", "(2", "(3")
		c.is(bulks("a", "b", "c", "d"), "ZRANGEBYSCORE", "z", "-inf", "+inf")
		c.is(bulks("c", "b"), "ZREVRANGEBYSCORE", "z", "3", "2")
		c.is(bulks("c", "b"), "ZRANGE", "z", "3", "2", "BYSCORE", "REV")
		c.is(bulks("b", "c"), "ZRANGE", "z", "2", "3", "BYSCORE")
		c.is(bulks("b", "c"), "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2")
		c.is(bulks("c", "b"), "ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "1", "2")
		c.is(bulks("b", "c", "d"), "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "-1")
		c.is(bulks(), "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "-1", "2")
		c.is(bulks(), "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "10", "2")
		c.is(bulks(), "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "0", "0")
		c.is(c.zws("b", "2", "c", "3"), "ZRANGEBYSCORE", "z", "2", "3", "WITHSCORES")
		c.is(bulks("a"), "ZRANGEBYSCORE", "z", " 1", "( 1.5")
		c.is(bulks(), "ZRANGEBYSCORE", "z", "", "")
		c.is(bulks("a", "b", "c", "d"), "ZRANGEBYSCORE", "z", "-1e400", "1e400")
		c.is(er("ERR min or max is not a float"), "ZRANGEBYSCORE", "z", "str", "1")
		c.is(er("ERR min or max is not a float"), "ZRANGEBYSCORE", "z", "1", "str")
		c.is(er("ERR min or max is not a float"), "ZRANGEBYSCORE", "z", "1", "NaN")
		c.is(er("ERR min or max is not a float"), "ZRANGEBYSCORE", "z", "1", "   ")

		c.is(in(4), "ZADD", "l", "0", "alpha", "0", "bar", "0", "cool", "0", "down")
		c.is(bulks("alpha", "bar", "cool", "down"), "ZRANGEBYLEX", "l", "-", "+")
		c.is(bulks("bar", "cool"), "ZRANGEBYLEX", "l", "[bar", "[cool")
		c.is(bulks("cool"), "ZRANGEBYLEX", "l", "(bar", "[cool")
		c.is(bulks("bar"), "ZRANGEBYLEX", "l", "[bar", "(cool")
		c.is(bulks("down", "cool"), "ZREVRANGEBYLEX", "l", "+", "(bar")
		c.is(bulks("cool", "bar"), "ZRANGE", "l", "[cool", "[b", "BYLEX", "REV")
		c.is(bulks("bar", "cool"), "ZRANGEBYLEX", "l", "-", "+", "LIMIT", "1", "2")
		c.is(bulks(), "ZRANGEBYLEX", "l", "+", "-")
		c.is(bulks(), "ZRANGEBYLEX", "l", "[z", "[a")
		c.is(in(4), "ZLEXCOUNT", "l", "-", "+")
		c.is(in(2), "ZLEXCOUNT", "l", "(alpha", "(down")
		c.is(in(0), "ZLEXCOUNT", "nokey", "-", "+")
		for _, bad := range [][2]string{{"foo", "bar"}, {"[foo", "bar"}, {"foo", "[bar"}, {"+x", "[bar"}, {"-x", "[bar"}} {
			c.is(er("ERR min or max not valid string range item"), "ZRANGEBYLEX", "l", bad[0], bad[1])
		}
		c.is(in(3), "ZCOUNT", "z", "2", "+inf")
		c.is(in(1), "ZCOUNT", "z", "(1", "(3")
		c.is(in(0), "ZCOUNT", "nokey", "-inf", "+inf")

		c.is(er("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"), "ZRANGE", "z", "0", "1", "LIMIT", "0", "1")
		c.is(bulks("a", "b"), "ZRANGE", "z", "0", "1", "LIMIT", "0", "-1")
		c.is(er("ERR syntax error, WITHSCORES not supported in combination with BYLEX"), "ZRANGE", "l", "-", "+", "BYLEX", "WITHSCORES")
		c.is(er("ERR syntax error"), "ZRANGE", "z", "0", "1", "REV", "REV")
		c.is(er("ERR syntax error"), "ZREVRANGE", "z", "0", "1", "REV")
		c.is(er("ERR syntax error"), "ZRANGEBYSCORE", "z", "0", "1", "BYLEX")
		c.is(er("ERR syntax error"), "ZRANGE", "z", "0", "1", "BYSCORE", "BYLEX")
		c.is(er("ERR syntax error"), "ZRANGE", "z", "0", "1", "LIMIT", "1")
		c.is(er("ERR value is not an integer or out of range"), "ZRANGE", "z", "a", "1")
	})
}

func TestZrankPopRem(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(3), "ZADD", "z", "10", "x", "20", "y", "30", "z")
		c.is(in(0), "ZRANK", "z", "x")
		c.is(in(2), "ZRANK", "z", "z")
		c.is(in(0), "ZREVRANK", "z", "z")
		c.is(in(2), "ZREVRANK", "z", "x")
		c.is(c.null(), "ZRANK", "z", "foo")
		c.is(ar(in(1), c.dbl("20")), "ZRANK", "z", "y", "WITHSCORE")
		c.is(ar(in(1), c.dbl("20")), "ZREVRANK", "z", "y", "withscore")
		c.is(c.nullArr(), "ZRANK", "z", "foo", "WITHSCORE")
		c.is(c.nullArr(), "ZRANK", "nokey", "foo", "WITHSCORE")
		c.is(er("ERR syntax error"), "ZRANK", "z", "x", "WITHSCORES")
		c.is(er("ERR wrong number of arguments for 'zrank' command"), "ZRANK", "z", "x", "WITHSCORE", "x")

		if c.proto == 2 {
			c.is(bulks("x", "10"), "ZPOPMIN", "z")
		} else {
			c.is(ar(bs("x"), c.dbl("10")), "ZPOPMIN", "z")
		}
		c.is(c.zws("z", "30"), "ZPOPMAX", "z", "1")
		c.is(ar(), "ZPOPMIN", "z", "0")
		c.is(c.zws("y", "20"), "ZPOPMIN", "z", "10")
		c.is(in(0), "EXISTS", "z")
		c.is(ar(), "ZPOPMIN", "nokey")
		c.is(er("ERR value is out of range, must be positive"), "ZPOPMIN", "nokey", "-1")
		c.is(er("ERR syntax error"), "ZPOPMIN", "nokey", "1", "2")

		c.is(in(6), "ZADD", "r", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f")
		c.is(in(2), "ZREMRANGEBYRANK", "r", "0", "1")
		c.is(bulks("c", "d", "e", "f"), "ZRANGE", "r", "0", "-1")
		c.is(in(0), "ZREMRANGEBYRANK", "r", "10", "20")
		c.is(in(2), "ZREMRANGEBYSCORE", "r", "(3", "5")
		c.is(bulks("c", "f"), "ZRANGE", "r", "0", "-1")
		c.is(er("ERR min or max is not a float"), "ZREMRANGEBYSCORE", "r", "str", "1")
		c.is(in(4), "ZADD", "rl", "0", "a", "0", "b", "0", "c", "0", "d")
		c.is(in(2), "ZREMRANGEBYLEX", "rl", "[b", "(d")
		c.is(bulks("a", "d"), "ZRANGE", "rl", "0", "-1")
		c.is(in(2), "ZREMRANGEBYRANK", "rl", "0", "-1")
		c.is(in(0), "EXISTS", "rl")
		c.is(in(0), "ZREMRANGEBYRANK", "nokey", "0", "-1")
	})
}

func TestZsetStoreOps(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(4), "ZADD", "zseta", "1", "a", "2", "b", "3", "c", "4", "d")
		c.is(in(3), "ZADD", "zsetb", "1", "b", "2", "c", "3", "d")
		c.is(in(2), "SADD", "plain", "a", "b")
		c.is(c.zws("a", "1", "b", "3", "c", "5", "d", "7"), "ZUNION", "2", "zseta", "zsetb", "WITHSCORES")
		c.is(c.zws("b", "3", "c", "5", "d", "7"), "ZINTER", "2", "zseta", "zsetb", "WITHSCORES")
		c.is(bulks("a"), "ZDIFF", "2", "zseta", "zsetb")
		c.is(c.zws("a", "1"), "ZDIFF", "2", "zseta", "zsetb", "WITHSCORES")
		c.is(bulks("a", "b", "c", "d"), "ZUNION", "2", "zseta", "zsetb", "WEIGHTS", "2", "1")
		c.is(c.zws("a", "2", "b", "5", "c", "8", "d", "11"), "ZUNION", "2", "zseta", "zsetb", "WEIGHTS", "2", "1", "WITHSCORES")
		c.is(c.zws("b", "1", "c", "2", "d", "3"), "ZINTER", "2", "zseta", "zsetb", "AGGREGATE", "MIN", "WITHSCORES")
		c.is(c.zws("b", "2", "c", "3", "d", "4"), "ZINTER", "2", "zseta", "zsetb", "AGGREGATE", "MAX", "WITHSCORES")
		c.is(c.zws("a", "2", "b", "3"), "ZINTER", "2", "zseta", "plain", "WITHSCORES")
		c.is(c.zws("a", "1", "b", "1"), "ZUNION", "1", "plain", "WITHSCORES")

		c.is(in(4), "ZUNIONSTORE", "dst", "2", "zseta", "zsetb")
		c.is(c.zws("a", "1", "b", "3", "c", "5", "d", "7"), "ZRANGE", "dst", "0", "-1", "WITHSCORES")
		c.is(in(3), "ZINTERSTORE", "dst", "2", "zseta", "zsetb", "WEIGHTS", "1", "0")
		c.is(c.zws("b", "2", "c", "3", "d", "4"), "ZRANGE", "dst", "0", "-1", "WITHSCORES")
		c.is(in(1), "ZDIFFSTORE", "dst", "2", "zseta", "zsetb")
		c.is(in(0), "ZINTERSTORE", "dst", "2", "zseta", "nokey")
		c.is(in(0), "EXISTS", "dst")
		c.is(in(3), "ZINTERCARD", "2", "zseta", "zsetb")
		c.is(in(2), "ZINTERCARD", "2", "zseta", "zsetb", "LIMIT", "2")

		c.is(er("ERR at least 1 input key is needed for 'zunion' command"), "ZUNION", "0", "key")
		c.is(er("ERR at least 1 input key is needed for 'zunionstore' command"), "ZUNIONSTORE", "dst", "0", "key")
		c.is(er("ERR at least 1 input key is needed for 'zdiff' command"), "ZDIFF", "0", "key")
		c.is(er("ERR syntax error"), "ZUNION", "3", "zseta", "zsetb")
		c.is(er("ERR syntax error"), "ZUNIONSTORE", "foo", "2", "zseta", "zsetb", "WITHSCORES")
		c.is(er("ERR syntax error"), "ZDIFF", "2", "zseta", "zsetb", "WEIGHTS", "1", "2")
		c.is(er("ERR weight value is not a float"), "ZUNION", "2", "zseta", "zsetb", "WEIGHTS", "1", "x")
		c.is(er("ERR syntax error"), "ZUNION", "2", "zseta", "zsetb", "AGGREGATE", "avg")
		c.is(er("ERR syntax error"), "ZINTERCARD", "1", "zseta", "zseta")
		c.is(er("ERR syntax error"), "ZINTERCARD", "1", "zseta", "LIMIT")
		c.is(er("ERR LIMIT can't be negative"), "ZINTERCARD", "1", "zseta", "LIMIT", "-1")
		c.is(er("ERR LIMIT can't be negative"), "ZINTERCARD", "1", "zseta", "LIMIT", "a")
		c.is(okr, "SET", "str", "x")
		c.is(er(wrongType), "ZUNION", "2", "zseta", "str", "WEIGHTS", "x", "y")

		c.is(in(2), "ZRANGESTORE", "zr", "zseta", "1", "2")
		c.is(c.zws("b", "2", "c", "3"), "ZRANGE", "zr", "0", "-1", "WITHSCORES")
		c.is(in(2), "ZRANGESTORE", "zr", "zseta", "4", "3", "BYSCORE", "REV")
		c.is(c.zws("c", "3", "d", "4"), "ZRANGE", "zr", "0", "-1", "WITHSCORES")
		c.is(in(0), "ZRANGESTORE", "zr", "nokey", "0", "-1")
		c.is(in(0), "EXISTS", "zr")
		c.is(er("ERR syntax error"), "ZRANGESTORE", "zr", "zseta", "0", "-1", "WITHSCORES")

		c.is(in(1), "ZADD", "inf", "+inf", "a")
		c.is(in(1), "ZADD", "ninf", "-inf", "a")
		c.is(c.zws("a", "0"), "ZUNION", "2", "inf", "ninf", "WITHSCORES")
		c.is(c.zws("a", "0"), "ZUNION", "1", "inf", "WEIGHTS", "0", "WITHSCORES")
	})
}

func TestZrandmemberZscan(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(c.null(), "ZRANDMEMBER", "nokey")
		c.is(ar(), "ZRANDMEMBER", "nokey", "3")
		c.is(in(1), "ZADD", "one", "1.5", "m")
		c.is(bs("m"), "ZRANDMEMBER", "one")
		c.is(bulks("m", "m"), "ZRANDMEMBER", "one", "-2")
		c.is(c.zws("m", "1.5"), "ZRANDMEMBER", "one", "1", "WITHSCORES")
		c.is(er("ERR syntax error"), "ZRANDMEMBER", "one", "1", "WITHSCORE")
		c.is(er("ERR value is out of range"), "ZRANDMEMBER", "one", "-9223372036854770000", "WITHSCORES")
		c.is(ar(bs("0"), bulks("m", "1.5")), "ZSCAN", "one", "0")
		c.is(in(4), "ZADD", "zp", "1", "foo", "2", "fab", "3", "fiz", "10", "foobar")
		c.is(ar(bs("0"), bulks("foo", "1", "foobar", "10")), "ZSCAN", "zp", "0", "MATCH", "foo*", "COUNT", "10000")
	})
	rc := e.Client(t, 3)
	ctx := context.Background()
	var zs []goredis.Z
	for i := range 300 {
		zs = append(zs, goredis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)})
	}
	rc.ZAdd(ctx, "big", zs...)
	got, err := rc.ZRandMember(ctx, "big", 40).Result()
	if err != nil || len(got) != 40 || len(distinctStrings(got)) != 40 {
		t.Errorf("ZRANDMEMBER 40 = %d, %v", len(got), err)
	}
	seen := map[string]bool{}
	var cur uint64
	for {
		kv, next, err := rc.ZScan(ctx, "big", cur, "", 25).Result()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i+1 < len(kv); i += 2 {
			seen[kv[i]] = true
		}
		if cur = next; cur == 0 {
			break
		}
	}
	if len(seen) != 300 {
		t.Errorf("ZSCAN saw %d members", len(seen))
	}
	res, err := rc.ZRangeWithScores(ctx, "big", 0, 2).Result()
	if err != nil || len(res) != 3 || res[2].Score != 2 || res[2].Member != "m2" {
		t.Errorf("ZRANGE WITHSCORES = %v, %v", res, err)
	}
}

func TestZsetMultiExec(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "MULTI")
		c.is(st("QUEUED"), "ZADD", "z", "1", "a", "2", "b")
		c.is(st("QUEUED"), "ZINCRBY", "z", "5", "a")
		c.is(st("QUEUED"), "ZRANGE", "z", "0", "-1", "WITHSCORES")
		c.is(st("QUEUED"), "ZPOPMIN", "z", "1")
		c.is(st("QUEUED"), "ZCARD", "z")
		c.is(ar(in(2), c.dbl("6"), c.zws("b", "2", "a", "6"), c.zws("b", "2"), in(1)), "EXEC")
	})
}
