package redis

import (
	"strconv"
	"testing"
	"time"
)

// Cases ported from Redis 7.2 tests/unit/expire.tcl, with the server's
// offset clock standing in for sleeps.

func TestExpireBasics(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "x", "foobar")
	c.is(in(1), "EXPIRE", "x", "5")
	c.is(in(5), "TTL", "x")
	c.is(in(1), "EXPIRE", "x", "10")
	c.is(in(10), "TTL", "x")
	c.is(in(1), "EXPIRE", "x", "2")
	c.is(bs("foobar"), "GET", "x")
	e.Clock.Advance(2100 * time.Millisecond)
	c.is(c.null(), "GET", "x")
	c.is(in(0), "EXISTS", "x")

	// A write on an expired key starts from scratch.
	c.is(okr, "SET", "x", "foo")
	c.is(in(1), "EXPIRE", "x", "1")
	e.Clock.Advance(2 * time.Second)
	c.is(in(3), "APPEND", "x", "bar")
	c.is(bs("bar"), "GET", "x")
	c.is(in(-1), "TTL", "x")

	c.is(okr, "SET", "x", "foo")
	at := time.Now().Add(e.Clock.Offset()).Unix() + 2
	c.is(in(1), "EXPIREAT", "x", strconv.FormatInt(at, 10))
	c.hasPrefix(":", "TTL", "x")
	e.Clock.Advance(3 * time.Second)
	c.is(in(0), "EXISTS", "x")

	c.is(okr, "SETEX", "x", "12", "test")
	c.is(in(12), "TTL", "x")
	c.is(bs("test"), "GET", "x")
	c.is(okr, "SETEX", "x", "12", "overwrite")
	c.is(bs("overwrite"), "GET", "x")

	c.is(okr, "SET", "x", "foo")
	c.is(in(1), "EXPIRE", "x", "100")
	c.is(in(100), "TTL", "x")
	c.is(in(1), "PERSIST", "x")
	c.is(in(-1), "TTL", "x")
	c.is(bs("foo"), "GET", "x")
	c.is(in(0), "PERSIST", "nokeyatall")
	c.is(in(0), "PERSIST", "x")
}

func TestExpireMilliseconds(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SET", "x", "somevalue")
	c.is(in(1), "PEXPIRE", "x", "100")
	e.Clock.Advance(80 * time.Millisecond)
	c.is(bs("somevalue"), "GET", "x")
	e.Clock.Advance(40 * time.Millisecond)
	c.is(c.null(), "GET", "x")

	c.is(okr, "SET", "x", "somevalue")
	at := time.Now().Add(e.Clock.Offset()).UnixMilli() + 100
	c.is(in(1), "PEXPIREAT", "x", strconv.FormatInt(at, 10))
	c.hasPrefix(":", "PTTL", "x")
	e.Clock.Advance(200 * time.Millisecond)
	c.is(c.null(), "GET", "x")

	c.is(okr, "SET", "x", "somevalue")
	c.is(in(1), "EXPIRE", "x", "1")
	c.is(in(1), "TTL", "x")
	c.is(in(1), "PEXPIRE", "x", "1000000")
	c.is(in(1000), "TTL", "x")
	if ms := intReply(t, c.do("PTTL", "x")); ms < 999000 || ms > 1000000 {
		t.Errorf("PTTL = %d", ms)
	}
}

// intReply parses a raw integer reply.
func intReply(t *testing.T, reply string) int64 {
	t.Helper()
	if len(reply) < 4 || reply[0] != ':' {
		t.Fatalf("not an integer reply: %q", reply)
	}
	n, err := strconv.ParseInt(reply[1:len(reply)-2], 10, 64)
	if err != nil {
		t.Fatalf("integer reply %q: %v", reply, err)
	}
	return n
}

func TestTTLForms(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "foo", "bar")
		for _, cmd := range []string{"TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"} {
			c.is(in(-1), cmd, "foo")
			c.is(in(-2), cmd, "nokey")
		}
		abs := time.Now().Add(e.Clock.Offset()).Unix() + 100
		c.is(in(1), "EXPIREAT", "foo", strconv.FormatInt(abs, 10))
		c.is(in(abs), "EXPIRETIME", "foo")
		c.is(in(abs*1000), "PEXPIRETIME", "foo")
		c.is(in(1), "PEXPIREAT", "foo", strconv.FormatInt(abs*1000+123, 10))
		c.is(in(abs*1000+123), "PEXPIRETIME", "foo")
		c.is(in(abs), "EXPIRETIME", "foo")
	})
}

func TestExpireErrors(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "foo", "bar")
		c.is(er("ERR value is not an integer or out of range"), "EXPIRE", "foo", "")
		for _, v := range []string{"9223370399119966", "9223372036854776", "10000000000000000", "18446744073709561", "-9223372036854776", "-9999999999999999"} {
			c.is(er("ERR invalid expire time in 'expire' command"), "EXPIRE", "foo", v)
		}
		c.is(in(-1), "TTL", "foo")
		c.is(er("ERR invalid expire time in 'pexpire' command"), "PEXPIRE", "foo", "9223372036854770000")
		c.is(in(1), "PEXPIREAT", "foo", "9223372036854770000")
		c.is(in(1), "PEXPIREAT", "foo", "-9223372036854770000")
		c.is(in(-2), "TTL", "foo")

		c.is(er("ERR GT and LT options at the same time are not compatible"), "EXPIRE", "foo", "200", "LT", "GT")
		c.is(er("ERR NX and XX, GT or LT options at the same time are not compatible"), "EXPIRE", "foo", "200", "NX", "GT")
		c.is(er("ERR NX and XX, GT or LT options at the same time are not compatible"), "EXPIRE", "foo", "200", "NX", "LT")
		c.is(er("ERR NX and XX, GT or LT options at the same time are not compatible"), "EXPIRE", "foo", "200", "NX", "XX")
		c.is(er("ERR Unsupported option AB"), "EXPIRE", "foo", "200", "AB")
		c.is(er("ERR Unsupported option AB"), "EXPIRE", "foo", "200", "XX", "AB")
	})
}

func TestExpireConditions(t *testing.T) {
	e := newEnv(t)
	bothProtos(t, e, func(c *conn) {
		c.is(okr, "SET", "foo", "bar", "EX", "100")
		c.is(in(0), "EXPIRE", "foo", "200", "NX")
		c.is(in(100), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(1), "EXPIRE", "foo", "200", "NX")
		c.is(in(200), "TTL", "foo")

		c.is(okr, "SET", "foo", "bar", "EX", "100")
		c.is(in(1), "EXPIRE", "foo", "200", "XX")
		c.is(in(200), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(0), "EXPIRE", "foo", "200", "XX")
		c.is(in(-1), "TTL", "foo")

		c.is(okr, "SET", "foo", "bar", "EX", "100")
		c.is(in(1), "EXPIRE", "foo", "200", "GT")
		c.is(in(200), "TTL", "foo")
		c.is(in(0), "EXPIRE", "foo", "100", "GT")
		c.is(in(200), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(0), "EXPIRE", "foo", "200", "GT")
		c.is(in(-1), "TTL", "foo")

		c.is(okr, "SET", "foo", "bar", "EX", "200")
		c.is(in(1), "EXPIRE", "foo", "100", "LT")
		c.is(in(100), "TTL", "foo")
		c.is(in(0), "EXPIRE", "foo", "200", "LT")
		c.is(in(100), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(1), "EXPIRE", "foo", "100", "LT")
		c.is(in(100), "TTL", "foo")

		c.is(okr, "SET", "foo", "bar", "EX", "200")
		c.is(in(1), "EXPIRE", "foo", "100", "LT", "XX")
		c.is(in(100), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(0), "EXPIRE", "foo", "200", "LT", "XX")
		c.is(in(-1), "TTL", "foo")

		c.is(okr, "SET", "foo", "bar", "EX", "100")
		c.is(in(1), "EXPIRE", "foo", "-10", "LT")
		c.is(in(-2), "TTL", "foo")
		c.is(okr, "SET", "foo", "bar")
		c.is(in(1), "EXPIRE", "foo", "-10", "LT")
		c.is(in(-2), "TTL", "foo")

		for _, cond := range []string{"NX", "XX", "GT", "LT"} {
			c.is(in(0), "EXPIRE", "none", "100", cond)
		}
		c.is(okr, "SET", "foo", "bar")
		c.is(in(1), "EXPIRE", "foo", "0")
		c.is(in(0), "EXISTS", "foo")
	})
}

func TestSetClearsAndKeepsTTL(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	c.is(okr, "SETEX", "foo", "100", "bar")
	c.is(okr, "SET", "foo", "bar")
	c.is(in(-1), "TTL", "foo")
	c.is(okr, "SET", "foo", "bar", "EX", "100")
	c.is(okr, "SET", "foo", "bar", "KEEPTTL")
	c.is(in(100), "TTL", "foo")
	c.is(bs("bar"), "GETSET", "foo", "baz")
	c.is(in(-1), "TTL", "foo")
	c.is(okr, "SET", "foo", "bar", "EX", "100")
	c.is(bs("bar"), "GETEX", "foo", "PERSIST")
	c.is(in(-1), "TTL", "foo")
}

func TestExpiredKeysLeaveReadsClean(t *testing.T) {
	e := newEnv(t)
	c := dial(t, e, 2)
	for i := range 5 {
		c.is(okr, "SET", "key"+strconv.Itoa(i), "v", "PX", "10")
	}
	c.is(okr, "SET", "keep", "v")
	e.Clock.Advance(20 * time.Millisecond)
	c.is(in(1), "DBSIZE")
	c.is(bulks("keep"), "KEYS", "*")
	c.is(ar(bs("0"), bulks("keep")), "SCAN", "0")
	c.is(in(-2), "TTL", "key0")
	c.is(st("none"), "TYPE", "key0")
}
