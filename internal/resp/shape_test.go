package resp

import "testing"

// TestShapes is the RESP2 versus RESP3 table: the reply a handler builds
// once and the bytes each protocol puts on the wire. The expected bytes
// follow Redis 7.2's addReply* functions and Kvrocks's protocol_test.go.
func TestShapes(t *testing.T) {
	info := "# Server\r\nredis_version:7.2.0\r\n"
	cases := []struct {
		name       string
		r          Reply
		resp2, re3 string
	}{
		{"double", Double(3.141), "$5\r\n3.141\r\n", ",3.141\r\n"},
		{"integral double", Double(2), "$1\r\n2\r\n", ",2\r\n"},
		{"null", Null(), "$-1\r\n", "_\r\n"},
		{"null array", NullArray(), "*-1\r\n", "_\r\n"},
		{"true", Bool(true), ":1\r\n", "#t\r\n"},
		{"false", Bool(false), ":0\r\n", "#f\r\n"},
		{"verbatim", Verbatim("txt", "verbatim string"), "$15\r\nverbatim string\r\n", "=19\r\ntxt:verbatim string\r\n"},
		{"set", Set(Str("a"), Str("b"), Str("c")),
			"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
			"~3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"},
		{"map", Map(Str("a"), Int(1), Str("b"), Int(2), Str("c"), Int(3)),
			"*6\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n$1\r\nc\r\n:3\r\n",
			"%3\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n$1\r\nc\r\n:3\r\n"},
		{"empty map", Map(), "*0\r\n", "%0\r\n"},
		{"empty set", Set(), "*0\r\n", "~0\r\n"},
		{"HGETALL", Map(Str("f1"), Str("v1"), Str("f2"), Str("v2")),
			"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
			"%2\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"},
		{"SMEMBERS", Set(Str("m1"), Str("m2")),
			"*2\r\n$2\r\nm1\r\n$2\r\nm2\r\n",
			"~2\r\n$2\r\nm1\r\n$2\r\nm2\r\n"},
		{"ZSCORE", Double(1.5), "$3\r\n1.5\r\n", ",1.5\r\n"},
		{"ZSCORE missing member", Null(), "$-1\r\n", "_\r\n"},
		{"MGET with a missing key", Array(Str("v"), Null()), "*2\r\n$1\r\nv\r\n$-1\r\n", "*2\r\n$1\r\nv\r\n_\r\n"},
		{"EXEC aborted by WATCH", NullArray(), "*-1\r\n", "_\r\n"},
		{"INFO", Verbatim("txt", info),
			"$31\r\n" + info + "\r\n",
			"=35\r\ntxt:" + info + "\r\n"},
		{"HELLO modules", Map(Str("proto"), Int(3), Str("modules"), Array()),
			"*4\r\n$5\r\nproto\r\n:3\r\n$7\r\nmodules\r\n*0\r\n",
			"%2\r\n$5\r\nproto\r\n:3\r\n$7\r\nmodules\r\n*0\r\n"},
		{"map of sets", Map(Str("k"), Set(Str("x"))),
			"*2\r\n$1\r\nk\r\n*1\r\n$1\r\nx\r\n",
			"%1\r\n$1\r\nk\r\n~1\r\n$1\r\nx\r\n"},
	}
	for _, c := range cases {
		if got := string(Encode(c.r, 2)); got != c.resp2 {
			t.Errorf("%s RESP2:\n got %q\nwant %q", c.name, got, c.resp2)
		}
		if got := string(Encode(c.r, 3)); got != c.re3 {
			t.Errorf("%s RESP3:\n got %q\nwant %q", c.name, got, c.re3)
		}
	}
}

// TestZRangeWithScoresShape shows the one shape a handler must build per
// protocol, as t_zset.c does: RESP2 is a flat member, score, member, score
// array with scores as bulk strings; RESP3 is an array of [member, double]
// pairs.
func TestZRangeWithScoresShape(t *testing.T) {
	type pair struct {
		m string
		s float64
	}
	items := []pair{{"a", 1}, {"b", 2.5}}
	build := func(proto int) Reply {
		out := make([]Reply, 0, 2*len(items))
		for _, p := range items {
			if proto == 3 {
				out = append(out, Array(Str(p.m), Double(p.s)))
			} else {
				out = append(out, Str(p.m), Double(p.s))
			}
		}
		return Array(out...)
	}
	if got, want := string(Encode(build(2), 2)), "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n2.5\r\n"; got != want {
		t.Errorf("RESP2 %q, want %q", got, want)
	}
	if got, want := string(Encode(build(3), 3)), "*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n"; got != want {
		t.Errorf("RESP3 %q, want %q", got, want)
	}
}
