package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/resp"
)

func decoder(b []byte) *Decoder {
	return NewDecoder(bufio.NewReaderSize(bytes.NewReader(b), 64<<10))
}

func jsonOf(v Value) string { return string(AppendJSON(nil, &v)) }

// decodeOne decodes exactly one reply from b and fails unless the input
// ends right after it.
func decodeOne(t *testing.T, b []byte) Value {
	t.Helper()
	d := decoder(b)
	v, err := d.Read(1 << 20)
	if err != nil {
		t.Fatalf("Read(%q): %v", b, err)
	}
	if _, err := d.Read(1 << 20); err != io.EOF {
		t.Fatalf("Read(%q) left input behind: %v", b, err)
	}
	return v
}

// TestDecodeServerShapes decodes the bytes NilDB's own reply writer
// produces for every reply constructor, in RESP3 and RESP2.
func TestDecodeServerShapes(t *testing.T) {
	cases := []struct {
		name  string
		reply resp.Reply
		resp3 string
		resp2 string // "" when it matches resp3
	}{
		{"status", resp.OK(), `{"t":"simple","v":"OK"}`, ""},
		{"error", resp.Err("WRONGTYPE Operation against a key holding the wrong kind of value"), `{"t":"error","v":"WRONGTYPE Operation against a key holding the wrong kind of value"}`, ""},
		{"int", resp.Int(-42), `{"t":"int","v":-42}`, ""},
		{"safe int edge", resp.Int(1<<53 - 1), `{"t":"int","v":9007199254740991}`, ""},
		{"int beyond 2^53", resp.Int(1 << 62), `{"t":"int","v":"4611686018427387904"}`, ""},
		{"negative int beyond 2^53", resp.Int(math.MinInt64), `{"t":"int","v":"-9223372036854775808"}`, ""},
		{"bulk", resp.Str("héllo\n\"q\""), `{"t":"bulk","v":"héllo\n\"q\""}`, ""},
		{"empty bulk", resp.Str(""), `{"t":"bulk","v":""}`, ""},
		{"binary bulk", resp.Bulk([]byte{0xff, 0x00, 'a'}), `{"t":"bulk","b64":"/wBh"}`, ""},
		{"control bytes", resp.Str("a\x01\x7f"), `{"t":"bulk","v":"a\u0001\u007f"}`, ""},
		{"null", resp.Null(), `{"t":"null"}`, ""},
		{"null array", resp.NullArray(), `{"t":"null"}`, ""},
		{"double", resp.Double(3.5), `{"t":"double","v":"3.5"}`, `{"t":"bulk","v":"3.5"}`},
		{"double inf", resp.Double(math.Inf(-1)), `{"t":"double","v":"-inf"}`, `{"t":"bulk","v":"-inf"}`},
		{"bool", resp.Bool(true), `{"t":"bool","v":true}`, `{"t":"int","v":1}`},
		{"verbatim", resp.Verbatim("txt", "# Server\r\nredis_version:7.2.0\r\n"), `{"t":"verbatim","f":"txt","v":"# Server\r\nredis_version:7.2.0\r\n"}`, `{"t":"bulk","v":"# Server\r\nredis_version:7.2.0\r\n"}`},
		{"map", resp.Map(resp.Str("id"), resp.Int(7), resp.Str("ns"), resp.Str("a.b")),
			`{"t":"map","v":[[{"t":"bulk","v":"id"},{"t":"int","v":7}],[{"t":"bulk","v":"ns"},{"t":"bulk","v":"a.b"}]]}`,
			`{"t":"array","v":[{"t":"bulk","v":"id"},{"t":"int","v":7},{"t":"bulk","v":"ns"},{"t":"bulk","v":"a.b"}]}`},
		{"set", resp.Set(resp.Str("a"), resp.Str("b")), `{"t":"set","v":[{"t":"bulk","v":"a"},{"t":"bulk","v":"b"}]}`,
			`{"t":"array","v":[{"t":"bulk","v":"a"},{"t":"bulk","v":"b"}]}`},
		{"empty array", resp.Array(), `{"t":"array","v":[]}`, ""},
		{"nested", resp.Array(resp.Array(resp.Str("m"), resp.Double(1.25)), resp.Map()),
			`{"t":"array","v":[{"t":"array","v":[{"t":"bulk","v":"m"},{"t":"double","v":"1.25"}]},{"t":"map","v":[]}]}`,
			`{"t":"array","v":[{"t":"array","v":[{"t":"bulk","v":"m"},{"t":"bulk","v":"1.25"}]},{"t":"array","v":[]}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := jsonOf(decodeOne(t, resp.Encode(c.reply, 3))); got != c.resp3 {
				t.Errorf("RESP3\n got %s\nwant %s", got, c.resp3)
			}
			want2 := c.resp2
			if want2 == "" {
				want2 = c.resp3
			}
			if got := jsonOf(decodeOne(t, resp.Encode(c.reply, 2))); got != want2 {
				t.Errorf("RESP2\n got %s\nwant %s", got, want2)
			}
		})
	}
}

// TestDecodeRESP3Types covers the RESP3 types NilDB does not send today:
// big numbers, blob errors, push, attributes and streamed values.
func TestDecodeRESP3Types(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"null", "_\r\n", `{"t":"null"}`},
		{"false", "#f\r\n", `{"t":"bool","v":false}`},
		{"nan", ",nan\r\n", `{"t":"double","v":"nan"}`},
		{"exponent", ",1.5e+300\r\n", `{"t":"double","v":"1.5e+300"}`},
		{"big number", "(3492890328409238509324850943850943825024385\r\n", `{"t":"big","v":"3492890328409238509324850943850943825024385"}`},
		{"blob error", "!21\r\nSYNTAX invalid syntax\r\n", `{"t":"error","v":"SYNTAX invalid syntax"}`},
		{"markdown verbatim", "=8\r\nmkd:# hi\r\n", `{"t":"verbatim","f":"mkd","v":"# hi"}`},
		{"push", ">3\r\n+message\r\n+news\r\n$2\r\nhi\r\n", `{"t":"push","v":[{"t":"simple","v":"message"},{"t":"simple","v":"news"},{"t":"bulk","v":"hi"}]}`},
		{"attribute", "|1\r\n+key-popularity\r\n%1\r\n$1\r\na\r\n,0.1923\r\n*1\r\n:2039123\r\n",
			`{"t":"array","v":[{"t":"int","v":2039123}],"attrs":[[{"t":"simple","v":"key-popularity"},{"t":"map","v":[[{"t":"bulk","v":"a"},{"t":"double","v":"0.1923"}]]}]]}`},
		{"streamed string", "$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n", `{"t":"bulk","v":"Hello world"}`},
		{"streamed array", "*?\r\n:1\r\n:2\r\n.\r\n", `{"t":"array","v":[{"t":"int","v":1},{"t":"int","v":2}]}`},
		{"streamed map", "%?\r\n+a\r\n:1\r\n.\r\n", `{"t":"map","v":[[{"t":"simple","v":"a"},{"t":"int","v":1}]]}`},
		{"invalid utf-8 in a status", "+ok\xff\r\n", `{"t":"simple","v":"ok\ufffd"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := jsonOf(decodeOne(t, []byte(c.in))); got != c.want {
				t.Errorf("\n got %s\nwant %s", got, c.want)
			}
		})
	}
}

func TestDecodeBrokenStreams(t *testing.T) {
	cases := []struct {
		name, in string
		want     error // nil: any *ProtocolError
	}{
		{"truncated array", "*2\r\n:1\r\n", io.ErrUnexpectedEOF},
		{"truncated bulk", "$5\r\nab", io.ErrUnexpectedEOF},
		{"bulk without CRLF", "$3\r\nabcd\r\n", errNoCRLF},
		{"line without CR", "+OK\n", errNoCRLF},
		{"unknown type", "?x\r\n", nil},
		{"bad integer", ":12a\r\n", nil},
		{"bad length", "$-2\r\n", nil},
		{"bad boolean", "#x\r\n", nil},
		{"null map", "%-1\r\n", nil},
		{"streamed map ends mid pair", "%?\r\n+a\r\n.\r\n", nil},
		{"streamed verbatim", "=?\r\n", nil},
		{"verbatim without format", "=3\r\nabc\r\n", nil},
		{"too deep", strings.Repeat("*1\r\n", maxDepth+2) + ":1\r\n", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := decoder([]byte(c.in)).Read(1 << 20)
			var pe *ProtocolError
			switch {
			case c.want != nil && !errors.Is(err, c.want):
				t.Errorf("err = %v, want %v", err, c.want)
			case c.want == nil && !errors.As(err, &pe):
				t.Errorf("err = %v, want a *ProtocolError", err)
			}
		})
	}
}

// TestDecodeOverBudget checks that a reply over the byte limit turns into
// an error value and that the next reply still decodes.
func TestDecodeOverBudget(t *testing.T) {
	big := strings.Repeat("x", 600)
	in := append(resp.Encode(resp.Array(resp.Str(big), resp.Str(big), resp.Map(resp.Str("k"), resp.Str(big))), 3), "+OK\r\n"...)
	d := decoder(in)
	v, err := d.Read(1000)
	if err != nil {
		t.Fatal(err)
	}
	if !v.IsErr() || !strings.HasPrefix(v.Text(), "NILDBUI reply is larger than") {
		t.Fatalf("over-budget reply = %s", jsonOf(v))
	}
	v, err = d.Read(1000)
	if err != nil || !v.IsOK() {
		t.Fatalf("next reply = %s, %v; want +OK", jsonOf(v), err)
	}
	many := resp.Encode(resp.Array(make([]resp.Reply, 200)...), 3)
	if v, err := decoder(many).Read(1000); err != nil || !v.IsErr() {
		t.Fatalf("200 nulls under a 1000-byte budget = %s, %v; want the size error", jsonOf(v), err)
	}
}

// TestAppendCommand encodes requests and parses them back with NilDB's
// own request reader.
func TestAppendCommand(t *testing.T) {
	cmds := [][][]byte{
		{[]byte("SET"), []byte("k"), []byte("v")},
		{[]byte("SET"), []byte(""), []byte("a\r\nb\x00c")},
		{[]byte("PING")},
		{[]byte("HSET"), {0xff, 0xfe}, []byte(strings.Repeat("z", 70<<10))},
	}
	var wire []byte
	for _, c := range cmds {
		wire = AppendCommand(wire, c)
	}
	r := resp.NewReader(bytes.NewReader(wire), resp.Limits{})
	r.SetAuthenticated(true)
	for i, want := range cmds {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if len(got) != len(want) {
			t.Fatalf("command %d: %d args, want %d", i, len(got), len(want))
		}
		for j := range want {
			if !bytes.Equal(got[j], want[j]) {
				t.Errorf("command %d arg %d = %q, want %q", i, j, got[j], want[j])
			}
		}
	}
}

func TestValueLookup(t *testing.T) {
	m := decodeOne(t, resp.Encode(resp.Map(resp.Str("proto"), resp.Int(3), resp.Str("server"), resp.Str("redis")), 3))
	if v, ok := m.Lookup("proto"); !ok || v.Int != 3 {
		t.Errorf("Lookup(proto) = %v, %v", v, ok)
	}
	flat := decodeOne(t, resp.Encode(resp.Map(resp.Str("proto"), resp.Int(2)), 2))
	if v, ok := flat.Lookup("proto"); !ok || v.Int != 2 {
		t.Errorf("RESP2 Lookup(proto) = %v, %v", v, ok)
	}
	if _, ok := m.Lookup("missing"); ok {
		t.Error("Lookup(missing) found something")
	}
	if got := Kind(99).String(); got != "kind(99)" {
		t.Errorf("Kind(99) = %q", got)
	}
}
