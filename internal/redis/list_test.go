package redis

import (
	"context"
	"strconv"
	"testing"
)

// Cases ported from Redis 7.2 tests/unit/type/list.tcl (blocking
// commands excluded; they are not in v1).

func TestListPushPop(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(1), "LPUSH", "l", "a")
		c.is(in(3), "LPUSH", "l", "b", "c")
		c.is(bulks("c", "b", "a"), "LRANGE", "l", "0", "-1")
		c.is(in(5), "RPUSH", "l", "d", "e")
		c.is(bulks("c", "b", "a", "d", "e"), "LRANGE", "l", "0", "-1")
		c.is(in(5), "LLEN", "l")
		c.is(in(0), "LLEN", "nokey")
		c.is(in(0), "LPUSHX", "nokey", "x")
		c.is(in(0), "RPUSHX", "nokey", "x")
		c.is(in(0), "EXISTS", "nokey")
		c.is(in(6), "LPUSHX", "l", "z")
		c.is(in(8), "RPUSHX", "l", "y", "x")
		c.is(bs("z"), "LPOP", "l")
		c.is(bs("x"), "RPOP", "l")
		c.is(c.null(), "LPOP", "nokey")
		c.is(c.nullArr(), "LPOP", "nokey", "2")
		c.is(er("ERR wrong number of arguments for 'lpop' command"), "LPOP", "key", "1", "1")
		c.is(er("ERR wrong number of arguments for 'rpop' command"), "RPOP", "key", "2", "2")
		c.is(er("ERR value is out of range, must be positive"), "LPOP", "forbarqaz", "-123")
		c.is(ar(), "LPOP", "l", "0")

		c.is(in(7), "LPUSH", "listcount", "aa", "bb", "cc", "dd", "ee", "ff", "gg")
		c.is(bulks("gg"), "LPOP", "listcount", "1")
		c.is(bulks("ff", "ee"), "LPOP", "listcount", "2")
		c.is(bulks("aa", "bb"), "RPOP", "listcount", "2")
		c.is(bulks("cc"), "RPOP", "listcount", "1")
		c.is(bulks("dd"), "RPOP", "listcount", "123")
		c.is(in(0), "EXISTS", "listcount")
		c.is(st("list"), "TYPE", "l")

		c.is(okr, "SET", "str", "x")
		for _, args := range [][]string{
			{"LLEN", "str"}, {"LINDEX", "str", "0"}, {"LPUSH", "str", "0"}, {"RPUSH", "str", "0"}, {"LPOP", "str"},
			{"RPOP", "str"}, {"LRANGE", "str", "0", "-1"}, {"LSET", "str", "0", "foo"}, {"LINSERT", "str", "after", "0", "0"},
			{"LREM", "str", "0", "x"}, {"LTRIM", "str", "0", "1"}, {"LPOS", "str", "x"}, {"RPOPLPUSH", "str", "dst"},
		} {
			c.is(er(wrongType), args...)
		}
	})
}

func TestListIndexSetRange(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(5), "RPUSH", "l", "a", "b", "c", "d", "e")
		c.is(bs("a"), "LINDEX", "l", "0")
		c.is(bs("e"), "LINDEX", "l", "-1")
		c.is(bs("d"), "LINDEX", "l", "-2")
		c.is(c.null(), "LINDEX", "l", "5")
		c.is(c.null(), "LINDEX", "l", "-6")
		c.is(c.null(), "LINDEX", "nokey", "x")
		c.is(er("ERR value is not an integer or out of range"), "LINDEX", "l", "x")
		c.is(okr, "LSET", "l", "1", "B")
		c.is(okr, "LSET", "l", "-1", "E")
		c.is(bulks("a", "B", "c", "d", "E"), "LRANGE", "l", "0", "-1")
		c.is(er("ERR index out of range"), "LSET", "l", "10", "foo")
		c.is(er("ERR no such key"), "LSET", "nosuchkey", "10", "foo")
		c.is(bulks("B", "c"), "LRANGE", "l", "1", "2")
		c.is(bulks("d", "E"), "LRANGE", "l", "-2", "-1")
		c.is(bulks("a", "B", "c", "d", "E"), "LRANGE", "l", "-10", "10")
		c.is(bulks(), "LRANGE", "l", "10", "20")
		c.is(bulks(), "LRANGE", "l", "3", "1")
		c.is(bulks(), "LRANGE", "nokey", "0", "-1")
		c.is(er("ERR value is not an integer or out of range"), "LRANGE", "l", "a", "1")

		c.is(okr, "LTRIM", "l", "1", "-2")
		c.is(bulks("B", "c", "d"), "LRANGE", "l", "0", "-1")
		c.is(okr, "LTRIM", "l", "0", "0")
		c.is(bulks("B"), "LRANGE", "l", "0", "-1")
		c.is(okr, "LTRIM", "l", "5", "10")
		c.is(in(0), "EXISTS", "l")
		c.is(okr, "LTRIM", "nokey", "0", "1")
	})
}

func TestListInsertRemPos(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(4), "RPUSH", "l", "a", "b", "c", "d")
		c.is(in(5), "LINSERT", "l", "before", "c", "zz")
		c.is(bulks("a", "b", "zz", "c", "d"), "LRANGE", "l", "0", "-1")
		c.is(in(6), "LINSERT", "l", "after", "c", "yy")
		c.is(bulks("a", "b", "zz", "c", "yy", "d"), "LRANGE", "l", "0", "-1")
		c.is(in(7), "LINSERT", "l", "after", "d", "dd")
		c.is(in(8), "LINSERT", "l", "before", "a", "aa")
		c.is(bulks("aa", "a", "b", "zz", "c", "yy", "d", "dd"), "LRANGE", "l", "0", "-1")
		c.is(in(-1), "LINSERT", "l", "before", "bad", "x")
		c.is(in(0), "LINSERT", "nokey", "before", "a", "x")
		c.is(er("ERR syntax error"), "LINSERT", "l", "middle", "a", "x")

		c.is(in(8), "RPUSH", "r", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo")
		c.is(in(2), "LREM", "r", "0", "bar")
		c.is(bulks("foo", "foobar", "foobared", "zap", "test", "foo"), "LRANGE", "r", "0", "-1")
		c.is(in(1), "LREM", "r", "-1", "foo")
		c.is(bulks("foo", "foobar", "foobared", "zap", "test"), "LRANGE", "r", "0", "-1")
		c.is(in(0), "LREM", "r", "0", "nothing")
		c.is(in(1), "LREM", "r", "1", "foo")
		c.is(bulks("foobar", "foobared", "zap", "test"), "LRANGE", "r", "0", "-1")
		c.is(in(8), "RPUSH", "r", "x", "y", "x", "x")
		c.is(in(2), "LREM", "r", "2", "x")
		c.is(bulks("foobar", "foobared", "zap", "test", "y", "x"), "LRANGE", "r", "0", "-1")
		c.is(in(0), "LREM", "nokey", "1", "a")
		c.is(er("ERR value is not an integer or out of range"), "LREM", "r", "x", "a")
		c.is(in(3), "RPUSH", "only", "a", "a", "a")
		c.is(in(3), "LREM", "only", "0", "a")
		c.is(in(0), "EXISTS", "only")

		large := "large value, longer than a listpack entry would hold inline"
		c.is(in(8), "RPUSH", "mylist", "a", "b", "c", large, "2", "3", "c", "c")
		c.is(in(0), "LPOS", "mylist", "a")
		c.is(in(2), "LPOS", "mylist", "c")
		c.is(in(2), "LPOS", "mylist", "c", "RANK", "1")
		c.is(in(6), "LPOS", "mylist", "c", "RANK", "2")
		c.is(c.null(), "LPOS", "mylist", "c", "RANK", "4")
		c.is(in(7), "LPOS", "mylist", "c", "RANK", "-1")
		c.is(in(6), "LPOS", "mylist", "c", "RANK", "-2")
		c.is(er("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"), "LPOS", "mylist", "c", "RANK", "0")
		c.is(er("ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"), "LPOS", "mylist", "c", "RANK", "-9223372036854775808")
		c.is(ar(in(2), in(6), in(7)), "LPOS", "mylist", "c", "COUNT", "0")
		c.is(ar(in(2)), "LPOS", "mylist", "c", "COUNT", "1")
		c.is(ar(in(2), in(6)), "LPOS", "mylist", "c", "COUNT", "2")
		c.is(ar(in(2), in(6), in(7)), "LPOS", "mylist", "c", "COUNT", "100")
		c.is(ar(in(6), in(7)), "LPOS", "mylist", "c", "COUNT", "0", "RANK", "2")
		c.is(ar(in(7), in(6)), "LPOS", "mylist", "c", "COUNT", "2", "RANK", "-1")
		c.is(ar(), "LPOS", "mylist", "x", "COUNT", "2", "RANK", "-1")
		c.is(c.null(), "LPOS", "mylist", "x")
		c.is(ar(), "LPOS", "nokey", "x", "COUNT", "0")
		c.is(c.null(), "LPOS", "nokey", "x")
		c.is(ar(in(2)), "LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "6")
		c.is(ar(in(7), in(6)), "LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "2", "RANK", "-1")
		c.is(er("ERR COUNT can't be negative"), "LPOS", "mylist", "c", "COUNT", "-1")
		c.is(er("ERR MAXLEN can't be negative"), "LPOS", "mylist", "c", "MAXLEN", "-5")
		c.is(er("ERR syntax error"), "LPOS", "mylist", "c", "RANK")
	})
}

func TestListMove(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(4), "RPUSH", "src", "a", "b", "c", "d")
		c.is(bs("d"), "RPOPLPUSH", "src", "dst")
		c.is(bs("c"), "RPOPLPUSH", "src", "dst")
		c.is(bulks("a", "b"), "LRANGE", "src", "0", "-1")
		c.is(bulks("c", "d"), "LRANGE", "dst", "0", "-1")
		c.is(bs("a"), "LMOVE", "src", "dst", "LEFT", "RIGHT")
		c.is(bulks("c", "d", "a"), "LRANGE", "dst", "0", "-1")
		c.is(bs("b"), "LMOVE", "src", "dst", "left", "left")
		c.is(in(0), "EXISTS", "src")
		c.is(bulks("b", "c", "d", "a"), "LRANGE", "dst", "0", "-1")
		c.is(c.null(), "RPOPLPUSH", "src", "dst")
		c.is(er("ERR syntax error"), "LMOVE", "dst", "x", "up", "left")

		c.is(bs("a"), "RPOPLPUSH", "dst", "dst")
		c.is(bulks("a", "b", "c", "d"), "LRANGE", "dst", "0", "-1")
		c.is(bs("a"), "LMOVE", "dst", "dst", "LEFT", "RIGHT")
		c.is(bulks("b", "c", "d", "a"), "LRANGE", "dst", "0", "-1")
		c.is(bs("b"), "LMOVE", "dst", "dst", "LEFT", "LEFT")
		c.is(bulks("b", "c", "d", "a"), "LRANGE", "dst", "0", "-1")
		c.is(in(1), "RPUSH", "one", "x")
		c.is(bs("x"), "RPOPLPUSH", "one", "one")
		c.is(bulks("x"), "LRANGE", "one", "0", "-1")

		c.is(okr, "SET", "str", "v")
		c.is(er(wrongType), "RPOPLPUSH", "dst", "str")
		c.is(er(wrongType), "RPOPLPUSH", "str", "dst")
		c.is(c.null(), "RPOPLPUSH", "nokey", "str")
		c.is(bulks("b", "c", "d", "a"), "LRANGE", "dst", "0", "-1")
	})
}

func TestListMultiExecAndLarge(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "MULTI")
		c.is(st("QUEUED"), "RPUSH", "l", "a", "b", "c")
		c.is(st("QUEUED"), "LPOP", "l")
		c.is(st("QUEUED"), "LINSERT", "l", "after", "b", "x")
		c.is(st("QUEUED"), "LRANGE", "l", "0", "-1")
		c.is(st("QUEUED"), "RPOPLPUSH", "l", "l")
		c.is(st("QUEUED"), "LRANGE", "l", "0", "-1")
		c.is(ar(in(3), bs("a"), in(3), bulks("b", "x", "c"), bs("c"), bulks("c", "b", "x")), "EXEC")
	})
	rc := e.Client(t, 2)
	ctx := context.Background()
	var vals []any
	for i := range 3000 {
		vals = append(vals, strconv.Itoa(i))
	}
	rc.RPush(ctx, "big", vals...)
	if err := rc.LTrim(ctx, "big", 1500, 2999).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := rc.LRange(ctx, "big", 0, 2).Result()
	if err != nil || len(got) != 3 || got[0] != "1500" {
		t.Errorf("LRANGE after LTRIM = %v, %v", got, err)
	}
	if n, _ := rc.LLen(ctx, "big").Result(); n != 1500 {
		t.Errorf("LLEN after LTRIM = %d", n)
	}
	popped, err := rc.LPopCount(ctx, "big", 1400).Result()
	if err != nil || len(popped) != 1400 || popped[0] != "1500" {
		t.Errorf("LPOP 1400 = %d elements, %v", len(popped), err)
	}
	if v, _ := rc.LIndex(ctx, "big", 0).Result(); v != "2900" {
		t.Errorf("LINDEX 0 = %q", v)
	}
	if n, _ := rc.LInsertBefore(ctx, "big", "2950", "new").Result(); n != 101 {
		t.Errorf("LINSERT = %d", n)
	}
	if v, _ := rc.LIndex(ctx, "big", 50).Result(); v != "new" {
		t.Errorf("LINDEX 50 after LINSERT = %q", v)
	}
	if v, _ := rc.LIndex(ctx, "big", 51).Result(); v != "2950" {
		t.Errorf("LINDEX 51 after LINSERT = %q", v)
	}
}
