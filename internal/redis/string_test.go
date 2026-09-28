package redis

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Cases ported from Redis 7.2 tests/unit/type/string.tcl and incr.tcl.
// Wrong-type cases that need a collection live in the collection tests.

func TestStringBasics(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "x", "foobar")
		c.is(bs("foobar"), "GET", "x")
		c.is(okr, "SET", "x", "")
		c.is(bs(""), "GET", "x")
		c.is(c.null(), "GET", "missing")

		c.is(in(1), "SETNX", "novar", "foobared")
		c.is(bs("foobared"), "GET", "novar")
		c.is(in(0), "SETNX", "novar", "blabla")
		c.is(bs("foobared"), "GET", "novar")

		c.is(c.null(), "GETSET", "gs", "xyz")
		c.is(bs("xyz"), "GET", "gs")
		c.is(bs("xyz"), "GETSET", "gs", "bar")
		c.is(bs("bar"), "GET", "gs")

		c.is(okr, "SET", "foo", "bar")
		c.is(bs("bar"), "GETDEL", "foo")
		c.is(c.null(), "GETDEL", "foo")

		c.is(okr, "SET", "foo", "BAR")
		c.is(okr, "SET", "bar", "FOO")
		c.is(bulks("BAR", "FOO"), "MGET", "foo", "bar")
		c.is(ar(bs("BAR"), c.null(), bs("FOO")), "MGET", "foo", "baazz", "bar")

		c.is(okr, "MSET", "x", "10", "y", "foo bar", "z", "x x x x x x x\n\n\r\n")
		c.is(bulks("10", "foo bar", "x x x x x x x\n\n\r\n"), "MGET", "x", "y", "z")
		c.is(er("ERR wrong number of arguments for 'mset' command"), "MSET", "x", "10", "y", "foo bar", "z")
		c.is(er("ERR wrong number of arguments for 'msetnx' command"), "MSETNX", "x", "20", "y", "foo bar", "z")
		c.is(okr, "SET", "x", "x")
		c.is(okr, "MSET", "x", "xxx", "x", "yyy")
		c.is(bs("yyy"), "GET", "x")
		c.is(in(0), "MSETNX", "x1", "xxx", "y2", "yyy", "x", "20")
		c.is(in(0), "EXISTS", "x1", "y2")
		c.is(in(1), "MSETNX", "x1", "xxx", "y2", "yyy")
		c.is(bulks("xxx", "yyy"), "MGET", "x1", "y2")
		c.is(in(1), "DEL", "x1")
		c.is(in(1), "MSETNX", "x1", "xxx", "x1", "yyy")
		c.is(bs("yyy"), "GET", "x1")
		c.is(in(0), "MSETNX", "x1", "xxx", "x1", "zzz")
		c.is(bs("yyy"), "GET", "x1")

		c.is(in(0), "STRLEN", "notakey")
		c.is(okr, "SET", "myinteger", "-555")
		c.is(in(4), "STRLEN", "myinteger")
		c.is(okr, "SET", "mystring", "foozzz0123456789 baz")
		c.is(in(20), "STRLEN", "mystring")

		c.is(in(3), "APPEND", "ap", "foo")
		c.is(in(6), "APPEND", "ap", "bar")
		c.is(bs("foobar"), "GET", "ap")
	})
}

func TestSetOptions(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "non-existing-option")
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "NX", "XX")
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "EX")
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "EX", "10", "PX", "100")
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "KEEPTTL", "EX", "10")
		c.is(er("ERR syntax error"), "SET", "foo", "bar", "PERSIST")

		c.is(c.null(), "SET", "foo", "bar", "XX")
		c.is(c.null(), "GET", "foo")
		c.is(okr, "SET", "foo", "bar", "NX")
		c.is(c.null(), "SET", "foo", "bar", "NX")
		c.is(okr, "SET", "foo", "bared", "XX")
		c.is(bs("bared"), "GET", "foo")

		c.is(okr, "SET", "foo", "bar")
		c.is(bs("bar"), "SET", "foo", "bar2", "GET")
		c.is(bs("bar2"), "GET", "foo")
		c.is(c.null(), "SET", "nokey", "bar", "GET")
		c.is(bs("bar"), "GET", "nokey")

		c.is(in(1), "DEL", "foo")
		c.is(c.null(), "SET", "foo", "bar", "XX", "GET")
		c.is(c.null(), "GET", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(bs("bar"), "SET", "foo", "baz", "XX", "GET")
		c.is(bs("baz"), "GET", "foo")
		c.is(bs("baz"), "SET", "foo", "qux", "NX", "GET")
		c.is(bs("baz"), "GET", "foo")
		c.is(c.null(), "SET", "nx2", "v", "NX", "GET")
		c.is(bs("v"), "GET", "nx2")

		c.is(okr, "SET", "foo", "bar", "EX", "10")
		c.hasPrefix(":", "TTL", "foo")
		c.is(in(10), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar", "PX", "10000")
		c.is(in(10), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar", "KEEPTTL")
		c.is(in(10), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(-1), "TTL", "foo")
		c.is(bs("bar"), "SET", "foo", "bar", "NX", "GET", "GET", "EX", "5", "EX", "7")
		c.is(c.null(), "SET", "fresh", "v", "NX", "GET", "GET", "EX", "5", "EX", "7")
		c.is(in(7), "TTL", "fresh")
	})
}

func TestSetExpireErrors(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(er("ERR invalid expire time in 'set' command"), "SET", "foo", "bar", "EX", "0")
		c.is(er("ERR invalid expire time in 'set' command"), "SET", "foo", "bar", "EX", "10000000000000000")
		c.is(er("ERR invalid expire time in 'set' command"), "SET", "foo", "bar", "EX", "-9999999999999999")
		c.is(er("ERR invalid expire time in 'set' command"), "SET", "foo", "bar", "PX", "-1")
		c.is(er("ERR value is not an integer or out of range"), "SET", "foo", "bar", "EX", "abc")
		c.is(er("ERR invalid expire time in 'setex' command"), "SETEX", "foo", "-10", "bar")
		c.is(er("ERR invalid expire time in 'psetex' command"), "PSETEX", "foo", "0", "bar")
		c.is(er("ERR value is not an integer or out of range"), "SETEX", "foo", "x", "bar")
		c.is(okr, "SET", "foo", "bar")
		c.is(er("ERR invalid expire time in 'getex' command"), "GETEX", "foo", "EX", "10000000000000000")
		c.is(er("ERR invalid expire time in 'getex' command"), "GETEX", "foo", "EX", "-9999999999999999")
		c.is(er("ERR syntax error"), "GETEX", "foo", "non-existent-option")
		c.is(er("ERR syntax error"), "GETEX", "foo", "PERSIST", "EX", "10")
		c.is(er("ERR wrong number of arguments for 'getex' command"), "GETEX")
		c.is(bs("bar"), "GET", "foo")
	})
}

func TestSetExpiryWithClock(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SETEX", "k", "100", "v")
	c.is(in(100), "TTL", "k")
	c.is(bs("v"), "GET", "k")
	c.is(in(0), "SETNX", "k", "w")
	e.Clock.Advance(101 * time.Second)
	c.is(c.null(), "GET", "k")
	c.is(in(1), "SETNX", "k", "w")
	c.is(bs("w"), "GET", "k")
	c.is(in(-1), "TTL", "k")

	c.is(okr, "PSETEX", "p", "1500", "v")
	if ttl := intReply(t, c.do("TTL", "p")); ttl < 1 || ttl > 2 {
		t.Errorf("TTL after PSETEX 1500 = %d", ttl)
	}
	e.Clock.Advance(1300 * time.Millisecond)
	c.is(bs("v"), "GET", "p")
	e.Clock.Advance(300 * time.Millisecond)
	c.is(c.null(), "GET", "p")

	// SET ... EXAT in the past stores a key that reads as absent.
	c.is(okr, "SET", "past", "v", "EXAT", "1")
	c.is(c.null(), "GET", "past")
	c.is(in(0), "EXISTS", "past")
}

func TestGetex(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "foo", "bar")
	c.is(bs("bar"), "GETEX", "foo", "EX", "10")
	c.is(in(10), "TTL", "foo")
	c.is(bs("bar"), "GETEX", "foo", "PX", "20000")
	c.is(in(20), "TTL", "foo")
	now := time.Now().Add(e.Clock.Offset())
	c.is(bs("bar"), "GETEX", "foo", "EXAT", strconv.FormatInt(now.Unix()+30, 10))
	c.hasPrefix(":", "TTL", "foo")
	c.is(bs("bar"), "GETEX", "foo", "PXAT", strconv.FormatInt(now.UnixMilli()+40000, 10))
	c.is(in(40), "TTL", "foo")
	c.is(bs("bar"), "GETEX", "foo", "PERSIST")
	c.is(in(-1), "TTL", "foo")
	c.is(bs("bar"), "GETEX", "foo")
	c.is(in(-1), "TTL", "foo")
	c.is(bs("bar"), "GETEX", "foo", "PXAT", "1")
	c.is(in(0), "EXISTS", "foo")
	c.is(c.null(), "GETEX", "foo")
	c.is(okr, "SET", "foo", "bar", "PX", "1")
	e.Clock.Advance(2 * time.Millisecond)
	c.is(c.null(), "GETEX", "foo")
	c.is(c.null(), "GET", "foo")
}

func TestSetrangeGetrange(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(3), "SETRANGE", "mykey", "0", "foo")
		c.is(bs("foo"), "GET", "mykey")
		c.is(in(1), "DEL", "mykey")
		c.is(in(0), "SETRANGE", "mykey", "0", "")
		c.is(in(0), "EXISTS", "mykey")
		c.is(in(4), "SETRANGE", "mykey", "1", "foo")
		c.is(bs("\x00foo"), "GET", "mykey")

		c.is(okr, "SET", "mykey", "foo")
		c.is(in(3), "SETRANGE", "mykey", "0", "b")
		c.is(bs("boo"), "GET", "mykey")
		c.is(okr, "SET", "mykey", "foo")
		c.is(in(3), "SETRANGE", "mykey", "0", "")
		c.is(bs("foo"), "GET", "mykey")
		c.is(okr, "SET", "mykey", "foo")
		c.is(in(3), "SETRANGE", "mykey", "1", "b")
		c.is(bs("fbo"), "GET", "mykey")
		c.is(okr, "SET", "mykey", "foo")
		c.is(in(7), "SETRANGE", "mykey", "4", "bar")
		c.is(bs("foo\x00bar"), "GET", "mykey")
		c.is(okr, "SET", "mykey", "1234")
		c.is(in(6), "SETRANGE", "mykey", "5", "2")
		c.is(bs("1234\x002"), "GET", "mykey")

		c.is(in(1), "DEL", "mykey")
		c.is(er("ERR string exceeds maximum allowed size (proto-max-bulk-len)"), "SETRANGE", "mykey", strconv.Itoa(512*1024*1024-4), "world")
		c.is(okr, "SET", "mykey", "hello")
		c.is(er("ERR offset is out of range"), "SETRANGE", "mykey", "-1", "world")
		c.is(er("ERR string exceeds maximum allowed size (proto-max-bulk-len)"), "SETRANGE", "mykey", strconv.Itoa(512*1024*1024-4), "world")
		c.is(er("ERR string exceeds maximum allowed size (proto-max-bulk-len)"), "SETRANGE", "mykey", "9223372036854775807", "x")
		c.is(er("ERR value is not an integer or out of range"), "SETRANGE", "mykey", "x", "world")

		c.is(bs(""), "GETRANGE", "nokey", "0", "-1")
		c.is(okr, "SET", "mykey", "Hello World")
		c.is(bs("Hell"), "GETRANGE", "mykey", "0", "3")
		c.is(bs("Hello World"), "GETRANGE", "mykey", "0", "-1")
		c.is(bs("orld"), "GETRANGE", "mykey", "-4", "-1")
		c.is(bs(""), "GETRANGE", "mykey", "5", "3")
		c.is(bs(" World"), "GETRANGE", "mykey", "5", "5000")
		c.is(bs("Hello World"), "GETRANGE", "mykey", "-5000", "10000")
		c.is(bs("H"), "GETRANGE", "mykey", "0", "-100") // Redis 7.2 clamps end to 0
		c.is(bs("H"), "GETRANGE", "mykey", "-100", "0")
		c.is(er("ERR value is not an integer or out of range"), "GETRANGE", "mykey", "a", "1")
		c.is(bs("Hello World"), "GETRANGE", "mykey", "0", "9223372036854775807")

		c.is(okr, "SET", "key", "abcde")
		c.is(bs("a"), "SUBSTR", "key", "0", "0")
		c.is(bs("abcd"), "SUBSTR", "key", "0", "3")
		c.is(bs("bcde"), "SUBSTR", "key", "-4", "-1")
		c.is(bs(""), "SUBSTR", "key", "-1", "-3")
		c.is(bs(""), "SUBSTR", "key", "7", "8")
		c.is(bs(""), "SUBSTR", "nokey", "0", "1")
	})
}

func TestGetrangeFuzz(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	val := "The quick brown fox jumps over the lazy dog"
	c.is(okr, "SET", "bin", val)
	for start := -60; start <= 60; start += 7 {
		for end := -60; end <= 60; end += 5 {
			c.is(bs(tclRange(val, start, end)), "GETRANGE", "bin", strconv.Itoa(start), strconv.Itoa(end))
		}
	}
}

// tclRange mirrors getrangeCommand's index rules for the fuzz test.
func tclRange(s string, start, end int) string {
	n := len(s)
	if start < 0 && end < 0 && start > end {
		return ""
	}
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start, end = max(start, 0), max(end, 0)
	if end >= n {
		end = n - 1
	}
	if start > end || n == 0 {
		return ""
	}
	return s[start : end+1]
}

func TestIncr(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(in(1), "INCR", "novar")
		c.is(bs("1"), "GET", "novar")
		c.is(in(2), "INCR", "novar")
		c.is(in(1), "DECR", "novar")
		c.is(in(-1), "DECR", "novar_not_exist")
		c.is(in(0), "INCR", "novar_not_exist")
		c.is(okr, "SET", "novar", "100")
		c.is(in(101), "INCR", "novar")
		c.is(okr, "SET", "novar", "17179869184")
		c.is(in(17179869185), "INCR", "novar")
		c.is(okr, "SET", "novar", "17179869184")
		c.is(in(34359738368), "INCRBY", "novar", "17179869184")
		for _, v := range []string{"    11", "11    ", "    11    ", "011", "+1", "1.5", ""} {
			c.is(okr, "SET", "novar", v)
			c.is(er("ERR value is not an integer or out of range"), "INCR", "novar")
		}
		c.is(okr, "SET", "x", "0")
		c.is(er("ERR decrement would overflow"), "DECRBY", "x", "-9223372036854775808")
		c.is(okr, "SET", "novar", "17179869184")
		c.is(in(-1), "DECRBY", "novar", "17179869185")
		c.is(in(-1), "DECRBY", "key_not_exist", "1")
		c.is(okr, "SET", "big", "9223372036854775807")
		c.is(er("ERR increment or decrement would overflow"), "INCR", "big")
		c.is(okr, "SET", "small", "-9223372036854775808")
		c.is(er("ERR increment or decrement would overflow"), "DECR", "small")
		c.is(er("ERR value is not an integer or out of range"), "INCRBY", "x", "1x")
		c.is(bs("0"), "GET", "x")
	})
}

func TestIncrKeepsTTL(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "n", "1", "EX", "100")
	c.is(in(2), "INCR", "n")
	c.is(in(100), "TTL", "n")
	c.is(bs("2.5"), "INCRBYFLOAT", "n", "0.5")
	c.is(in(100), "TTL", "n")
	c.is(in(6), "APPEND", "n", "000")
	c.is(in(100), "TTL", "n")
	c.is(in(6), "SETRANGE", "n", "0", "3")
	c.is(in(100), "TTL", "n")
	e.Clock.Advance(101 * time.Second)
	c.is(in(1), "INCR", "n")
	c.is(in(-1), "TTL", "n")
}

func TestIncrbyfloat(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(bs("1"), "INCRBYFLOAT", "novar", "1")
		c.is(bs("1"), "GET", "novar")
		c.is(bs("1.25"), "INCRBYFLOAT", "novar", "0.25")
		c.is(bs("1.25"), "GET", "novar")
		c.is(okr, "SET", "novar", "1.5")
		c.is(bs("3"), "INCRBYFLOAT", "novar", "1.5")
		c.is(okr, "SET", "novar", "17179869184")
		c.is(bs("17179869185.5"), "INCRBYFLOAT", "novar", "1.5")
		c.is(okr, "SET", "novar", "17179869184")
		c.is(bs("34359738368"), "INCRBYFLOAT", "novar", "17179869184")
		for _, v := range []string{"    11", "11    ", " 11 "} {
			c.is(okr, "SET", "novar", v)
			c.is(er("ERR value is not a valid float"), "INCRBYFLOAT", "novar", "1.0")
		}
		c.is(okr, "SET", "foo", "0")
		c.is(er("ERR increment would produce NaN or Infinity"), "INCRBYFLOAT", "foo", "+inf")
		c.is(er("ERR value is not a valid float"), "INCRBYFLOAT", "foo", "nan")
		c.is(okr, "SET", "foo", "1")
		c.is(bs("-0.1"), "INCRBYFLOAT", "foo", "-1.1")
		c.is(okr, "SET", "foo", "1")
		c.is(in(3), "SETRANGE", "foo", "2", "2")
		c.is(er("ERR value is not a valid float"), "INCRBYFLOAT", "foo", "1")
		c.is(bs("10.5"), "INCRBYFLOAT", "f2", "10.5")
		c.is(bs("10.6"), "INCRBYFLOAT", "f2", "0.1")
		c.is(okr, "SET", "f3", "5.0e3")
		c.is(bs("5001"), "INCRBYFLOAT", "f3", "1")
	})
}

func TestIncrByFloatLongDouble(t *testing.T) {
	cases := []struct{ cur, by, want string }{
		{"1", "-1.1", "-0.1"},
		{"10.5", "0.1", "10.6"},
		{"0.1", "0.2", "0.3"},
		{"1", "1", "2"},
		{"17179869184", "1.5", "17179869185.5"},
		{"1.5", "1.5", "3"},
		{"5.0e3", "1", "5001"},
		{"0", "1e-20", "0"},
		{"-1", "0.99999999999999999999", "0"},
		// %.17Lf prints the exact expansion of the 80-bit sum.
		{"1e308", "1e308", "199999999999999999993371759311691291321120199694831134415594095989843469737676123744200253843777078640893494450108026446304269499187921167194841628860392837535918200039206381557326219209014213335878306791577877829121087126122536729803237260434173178506889763247582601711514636284849020905456510092687857156096"},
	}
	for _, tc := range cases {
		got, rep := incrByFloat([]byte(tc.cur), true, []byte(tc.by))
		if rep != nil || got != tc.want {
			t.Errorf("%s + %s = %q, %v; want %q", tc.cur, tc.by, got, rep, tc.want)
		}
	}
	// incr.tcl "No negative zero".
	s, _ := incrByFloat(nil, false, []byte("0.024390243902439025"))
	s, _ = incrByFloat([]byte(s), true, []byte("-0.024390243902439025"))
	if s != "0" {
		t.Errorf("1/41 - 1/41 = %q, want 0", s)
	}
	for _, bad := range [][2]string{{"1", "inf"}, {"inf", "1"}, {"1", "nan"}, {" 1", "1"}, {"1", "1e999999999"}} {
		if _, rep := incrByFloat([]byte(bad[0]), true, []byte(bad[1])); rep == nil {
			t.Errorf("%q + %q: no error", bad[0], bad[1])
		}
	}
}

func TestStringGoRedis(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	for _, proto := range []int{2, 3} {
		rc := e.Client(t, proto)
		if err := rc.FlushAll(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rc.Set(ctx, "k", "v", 10*time.Second).Err(); err != nil {
			t.Fatalf("RESP%d SET: %v", proto, err)
		}
		if v, err := rc.Get(ctx, "k").Result(); err != nil || v != "v" {
			t.Errorf("RESP%d GET = %q, %v", proto, v, err)
		}
		if d, err := rc.TTL(ctx, "k").Result(); err != nil || d != 10*time.Second {
			t.Errorf("RESP%d TTL = %v, %v", proto, d, err)
		}
		if n, err := rc.Incr(ctx, "n").Result(); err != nil || n != 1 {
			t.Errorf("RESP%d INCR = %d, %v", proto, n, err)
		}
		if f, err := rc.IncrByFloat(ctx, "f", 2.5).Result(); err != nil || f != 2.5 {
			t.Errorf("RESP%d INCRBYFLOAT = %v, %v", proto, f, err)
		}
		vals, err := rc.MGet(ctx, "k", "missing", "n").Result()
		if err != nil || len(vals) != 3 || vals[0] != "v" || vals[1] != nil || vals[2] != "1" {
			t.Errorf("RESP%d MGET = %v, %v", proto, vals, err)
		}
		if ok, err := rc.SetNX(ctx, "k", "w", 0).Result(); err != nil || ok {
			t.Errorf("RESP%d SETNX = %v, %v", proto, ok, err)
		}
		if old, err := rc.SetArgs(ctx, "k", "w", goredis.SetArgs{Get: true}).Result(); err != nil || old != "v" {
			t.Errorf("RESP%d SET GET = %q, %v", proto, old, err)
		}
		if s, err := rc.GetRange(ctx, "k", 0, -1).Result(); err != nil || !strings.EqualFold(s, "w") {
			t.Errorf("RESP%d GETRANGE = %q, %v", proto, s, err)
		}
	}
}
