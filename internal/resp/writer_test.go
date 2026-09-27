package resp

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

// countWriter records each Write call.
type countWriter struct {
	bytes.Buffer
	writes []int
	fail   error
}

func (c *countWriter) Write(p []byte) (int, error) {
	if c.fail != nil {
		return 0, c.fail
	}
	c.writes = append(c.writes, len(p))
	return c.Buffer.Write(p)
}

func TestEncodePrimitives(t *testing.T) {
	cases := []struct {
		name       string
		r          Reply
		resp2, re3 string
	}{
		{"ok", OK(), "+OK\r\n", "+OK\r\n"},
		{"status", Status("PONG"), "+PONG\r\n", "+PONG\r\n"},
		{"int", Int(1000), ":1000\r\n", ":1000\r\n"},
		{"negative int", Int(math.MinInt64), ":-9223372036854775808\r\n", ":-9223372036854775808\r\n"},
		{"bulk", Bulk([]byte("hello")), "$5\r\nhello\r\n", "$5\r\nhello\r\n"},
		{"empty bulk", Bulk([]byte{}), "$0\r\n\r\n", "$0\r\n\r\n"},
		{"nil bulk is empty", Bulk(nil), "$0\r\n\r\n", "$0\r\n\r\n"},
		{"binary bulk", Bulk([]byte("a\r\nb\x00")), "$5\r\na\r\nb\x00\r\n", "$5\r\na\r\nb\x00\r\n"},
		{"str", Str("world"), "$5\r\nworld\r\n", "$5\r\nworld\r\n"},
		{"error", Err("ERR syntax error"), "-ERR syntax error\r\n", "-ERR syntax error\r\n"},
		{"error with dash", Err("-ERR x"), "-ERR x\r\n", "-ERR x\r\n"},
		{"error newlines", Err("ERR a\r\nb\nc"), "-ERR a  b c\r\n", "-ERR a  b c\r\n"},
		{"status newlines", Status("a\nb"), "+a b\r\n", "+a b\r\n"},
		{"errorf", Errorf("ERR %s is %d", "x", 3), "-ERR x is 3\r\n", "-ERR x is 3\r\n"},
		{"raw", Raw([]byte(":1\r\n:2\r\n")), ":1\r\n:2\r\n", ":1\r\n:2\r\n"},
		{"empty array", Array(), "*0\r\n", "*0\r\n"},
		{"nil item", Array(nil, Int(1)), "*2\r\n$-1\r\n:1\r\n", "*2\r\n_\r\n:1\r\n"},
		{"nested", Array(Array(Int(1)), Str("a")), "*2\r\n*1\r\n:1\r\n$1\r\na\r\n", "*2\r\n*1\r\n:1\r\n$1\r\na\r\n"},
		{"verbatim padded", Verbatim("tx", "hi"), "$2\r\nhi\r\n", "=6\r\ntx :hi\r\n"},
		{"verbatim cut", Verbatim("text", "hi"), "$2\r\nhi\r\n", "=6\r\ntex:hi\r\n"},
	}
	for _, c := range cases {
		if got := string(Encode(c.r, 2)); got != c.resp2 {
			t.Errorf("%s RESP2 = %q, want %q", c.name, got, c.resp2)
		}
		if got := string(Encode(c.r, 3)); got != c.re3 {
			t.Errorf("%s RESP3 = %q, want %q", c.name, got, c.re3)
		}
	}
	if got := string(Encode(nil, 3)); got != "_\r\n" {
		t.Errorf("nil reply = %q", got)
	}
}

func TestErrorTexts(t *testing.T) {
	cases := []struct {
		r    Reply
		want string
	}{
		{ErrArity("get"), "ERR wrong number of arguments for 'get' command"},
		{ErrArity("PING"), "ERR wrong number of arguments for 'ping' command"},
		{ErrArity("client|setname"), "ERR wrong number of arguments for 'client|setname' command"},
		{ErrUnknown("foo", nil), "ERR unknown command 'foo', with args beginning with: "},
		{ErrUnknown("FOO", [][]byte{[]byte("a"), []byte("b c")}), "ERR unknown command 'FOO', with args beginning with: 'a' 'b c' "},
		{ErrUnknown(strings.Repeat("n", 200), nil), "ERR unknown command '" + strings.Repeat("n", 128) + "', with args beginning with: "},
		{ErrUnknown("x", [][]byte{[]byte(strings.Repeat("a", 200)), []byte("b")}),
			"ERR unknown command 'x', with args beginning with: '" + strings.Repeat("a", 128) + "' "},
		{ErrUnknown("x", [][]byte{[]byte(strings.Repeat("a", 120)), []byte(strings.Repeat("b", 20)), []byte("c")}),
			"ERR unknown command 'x', with args beginning with: '" + strings.Repeat("a", 120) + "' '" + strings.Repeat("b", 5) + "' "},
		{ErrUnknown("x\x00y", [][]byte{[]byte("a\x00b")}), "ERR unknown command 'x', with args beginning with: 'a' "},
		{ErrUnknownSubcommand("foo", "client"), "ERR unknown subcommand 'foo'. Try CLIENT HELP."},
		{ErrUnknownSubcommand(strings.Repeat("s", 130), "object"), "ERR unknown subcommand '" + strings.Repeat("s", 128) + "'. Try OBJECT HELP."},
		{ErrWrongType, "WRONGTYPE Operation against a key holding the wrong kind of value"},
		{ErrNoAuth, "NOAUTH Authentication required."},
		{ErrExecAbort, "EXECABORT Transaction discarded because of previous errors."},
		{ErrNoProto, "NOPROTO unsupported protocol version"},
	}
	for _, c := range cases {
		got, ok := ErrorText(c.r)
		if !ok || got != c.want {
			t.Errorf("ErrorText = %q, %v; want %q", got, ok, c.want)
		}
		if enc := string(Encode(c.r, 2)); enc != "-"+c.want+"\r\n" {
			t.Errorf("encoded %q", enc)
		}
	}
	// The newline in an unknown command's argument must not end the line.
	if got := string(Encode(ErrUnknown("x", [][]byte{[]byte("a\r\nb")}), 2)); got != "-ERR unknown command 'x', with args beginning with: 'a  b' \r\n" {
		t.Errorf("sanitised = %q", got)
	}
}

func TestIsErr(t *testing.T) {
	yes := []Reply{Err("ERR x"), ErrSyntax, Errorf("ERR %d", 1), Raw([]byte("-ERR raw\r\n")), ErrArity("get")}
	no := []Reply{OK(), Status("ERR x"), Str("-ERR x"), Bulk([]byte("-x")), Int(-1), Null(), Array(Err("ERR x")), Raw([]byte(":1\r\n")), Raw(nil)}
	for _, r := range yes {
		if !IsErr(r) {
			t.Errorf("IsErr(%q) = false", Encode(r, 2))
		}
	}
	for _, r := range no {
		if IsErr(r) {
			t.Errorf("IsErr(%q) = true", Encode(r, 2))
		}
	}
	if s, _ := ErrorText(Raw([]byte("-ERR raw\r\n:1\r\n"))); s != "ERR raw" {
		t.Errorf("raw error text = %q", s)
	}
	if ErrSyntax != Err("ERR syntax error") {
		t.Error("error replies do not compare equal by value")
	}
}

func TestMapOddPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Map with three arguments did not panic")
		}
	}()
	Map(Str("a"), Str("b"), Str("c"))
}

func TestWriterProto(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	if w.Proto() != 2 {
		t.Errorf("new writer proto %d", w.Proto())
	}
	w.SetProto(3)
	if w.Proto() != 3 {
		t.Errorf("after SetProto(3): %d", w.Proto())
	}
	w.SetProto(4)
	if w.Proto() != 2 {
		t.Errorf("after SetProto(4): %d", w.Proto())
	}
}

// TestPipelinedFlush checks that many small replies leave in one write.
func TestPipelinedFlush(t *testing.T) {
	var cw countWriter
	w := NewWriter(&cw)
	for range 1000 {
		w.Write(Status("PONG"))
	}
	if len(cw.writes) != 0 {
		t.Fatalf("%d writes before Flush", len(cw.writes))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(cw.writes) != 1 || cw.Len() != 7000 {
		t.Errorf("writes %v, %d bytes", cw.writes, cw.Len())
	}
	if w.Buffered() != 0 {
		t.Errorf("Buffered() = %d after Flush", w.Buffered())
	}
	if err := w.Flush(); err != nil || len(cw.writes) != 1 {
		t.Errorf("empty Flush wrote: %v %v", cw.writes, err)
	}
}

func TestAutoFlushAt64KiB(t *testing.T) {
	var cw countWriter
	w := NewWriter(&cw)
	item := Str(strings.Repeat("x", 1000))
	for range 70 {
		w.Write(item)
	}
	if len(cw.writes) != 1 || cw.writes[0] < writeBufSize {
		t.Errorf("writes before Flush = %v, want one of at least %d bytes", cw.writes, writeBufSize)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if want := 70 * len(Encode(item, 2)); cw.Len() != want {
		t.Errorf("%d bytes written, want %d", cw.Len(), want)
	}
}

func TestLargeBulkWrittenDirectly(t *testing.T) {
	var cw countWriter
	w := NewWriter(&cw)
	payload := bytes.Repeat([]byte("y"), 100<<10)
	w.Write(Int(1))
	w.Write(Bulk(payload))
	w.Write(Int(2))
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := ":1\r\n$102400\r\n" + string(payload) + "\r\n:2\r\n"
	if cw.String() != want {
		t.Errorf("output differs (%d bytes, want %d)", cw.Len(), len(want))
	}
	if len(cw.writes) != 3 || cw.writes[1] != len(payload) {
		t.Errorf("writes = %v, want the payload as its own write", cw.writes)
	}
	// Encode has no connection and keeps the payload inline.
	if got := Encode(Bulk(payload), 2); len(got) != len(payload)+11 {
		t.Errorf("Encode length %d", len(got))
	}
}

func TestStickyWriteError(t *testing.T) {
	boom := errors.New("broken pipe")
	cw := countWriter{fail: boom}
	w := NewWriter(&cw)
	w.Write(OK())
	if err := w.Flush(); err != boom {
		t.Fatalf("Flush = %v", err)
	}
	cw.fail = nil
	w.Write(OK())
	if err := w.Flush(); err != boom {
		t.Errorf("second Flush = %v, want the first error", err)
	}
	if cw.Len() != 0 {
		t.Errorf("wrote %q after an error", cw.String())
	}
}

func TestStreamingMethods(t *testing.T) {
	for _, proto := range []int{2, 3} {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.SetProto(proto)
		w.MapHeader(2)
		w.Str("a")
		w.Int(1)
		w.Bulk([]byte("b"))
		w.Double(2.5)
		w.SetHeader(2)
		w.Status("x")
		w.Error("ERR y")
		w.ArrayHeader(3)
		w.Null()
		w.NullArray()
		w.Bool(true)
		w.Verbatim("txt", "v")
		w.Raw([]byte(":7\r\n"))
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		want := map[int]string{
			2: "*4\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n$3\r\n2.5\r\n*2\r\n+x\r\n-ERR y\r\n*3\r\n$-1\r\n*-1\r\n:1\r\n$1\r\nv\r\n:7\r\n",
			3: "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n,2.5\r\n~2\r\n+x\r\n-ERR y\r\n*3\r\n_\r\n_\r\n#t\r\n=5\r\ntxt:v\r\n:7\r\n",
		}[proto]
		if buf.String() != want {
			t.Errorf("RESP%d:\n got %q\nwant %q", proto, buf.String(), want)
		}
	}
}

func TestStreamReply(t *testing.T) {
	members := []string{"a", "b", "c"}
	r := Stream(func(w *Writer) {
		w.SetHeader(len(members))
		for _, m := range members {
			w.Str(m)
		}
	})
	if got := string(Encode(Array(r, Int(1)), 2)); got != "*2\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n:1\r\n" {
		t.Errorf("RESP2 %q", got)
	}
	if got := string(Encode(r, 3)); got != "~3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n" {
		t.Errorf("RESP3 %q", got)
	}
}

// TestFormatFloat holds outputs of Redis 7.2's d2string compiled from its
// sources, including values where Grisu2 is not the shortest form.
func TestFormatFloat(t *testing.T) {
	a, b := 0.1, 0.2
	c, d := 10.5, 0.1
	cases := []struct {
		f    float64
		want string
	}{
		{3.14, "3.14"},
		{1e21, "1e+21"},
		{a + b, "0.30000000000000004"},
		{math.Copysign(0, -1), "-0"},
		{0, "0"},
		{math.Inf(1), "inf"},
		{math.Inf(-1), "-inf"},
		{math.NaN(), "nan"},
		{1, "1"},
		{-2.5, "-2.5"},
		{0.5, "0.5"},
		{c + d, "10.6"},
		{4.35, "4.35"},
		{12345678.5, "12345678.5"},
		{1.05262, "1.0526199999999999"},
		{9317.81304, "9317.813039999999"},
		{1e23, "99999999999999990000000"},
		{90836761923877.63, "90836761923877.63"},
		{123456789012345678901234.0, "123456789012345690000000"},
		{1.5e-7, "1.5e-7"},
		{1e-7, "1e-7"},
		{1e-5, "0.00001"},
		{0.000123, "0.000123"},
		{1.2345e-6, "1.2345e-6"},
		{1e100, "1e+100"},
		{5e-324, "5e-324"},
		{2.2250738585072014e-308, "2.2250738585072014e-308"},
		{math.MaxFloat64, "1.7976931348623157e+308"},
		{1 << 62, "4611686018427387904"},
		{-(1 << 62), "-4611686018427387904"},
		{1 << 63, "9223372036854776000"},
		{1e15, "1000000000000000"},
		{1e19, "1e+19"},
	}
	for _, tc := range cases {
		if got := FormatFloat(tc.f); got != tc.want {
			t.Errorf("FormatFloat(%v) = %q, want %q", tc.f, got, tc.want)
		}
	}
}

// TestFormatFloatRoundTrip checks that every printed double parses back
// to the same bits.
func TestFormatFloatRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 200000 {
		var f float64
		switch i % 3 {
		case 0:
			f = math.Float64frombits(r.Uint64())
		case 1:
			f = r.NormFloat64() * math.Pow(10, float64(r.IntN(60)-30))
		default:
			f = float64(r.IntN(10000000)) / math.Pow(10, float64(r.IntN(9)))
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			continue
		}
		s := FormatFloat(f)
		g, ok := ParseFloat([]byte(s))
		if !ok || math.Float64bits(g) != math.Float64bits(f) {
			t.Fatalf("FormatFloat(%v) = %q parses back to %v, %v", f, s, g, ok)
		}
	}
}

func TestDoubleEncoding(t *testing.T) {
	cases := []struct {
		f          float64
		resp2, re3 string
	}{
		{3.141, "$5\r\n3.141\r\n", ",3.141\r\n"},
		{math.Inf(1), "$3\r\ninf\r\n", ",inf\r\n"},
		{math.Inf(-1), "$4\r\n-inf\r\n", ",-inf\r\n"},
		{math.NaN(), "$3\r\nnan\r\n", ",nan\r\n"},
		{10, "$2\r\n10\r\n", ",10\r\n"},
		{1e21, "$5\r\n1e+21\r\n", ",1e+21\r\n"},
	}
	for _, c := range cases {
		if got := string(Encode(Double(c.f), 2)); got != c.resp2 {
			t.Errorf("Double(%v) RESP2 = %q, want %q", c.f, got, c.resp2)
		}
		if got := string(Encode(Double(c.f), 3)); got != c.re3 {
			t.Errorf("Double(%v) RESP3 = %q, want %q", c.f, got, c.re3)
		}
	}
}

func BenchmarkWritePipeline(b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	b.ReportAllocs()
	for i := range b.N {
		w.Write(Str("value"))
		w.Write(Int(int64(i)))
		if i%16 == 15 {
			_ = w.Flush()
			buf.Reset()
		}
	}
}

func BenchmarkReadPipeline(b *testing.B) {
	cmd := []byte(mbulk("SET", "key:000000000001", "value"))
	in := bytes.Repeat(cmd, 16)
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	for range b.N {
		r := NewReader(bytes.NewReader(in), DefaultLimits())
		r.SetAuthenticated(true)
		for range 16 {
			if _, err := r.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestFormatFloatHuman(t *testing.T) {
	c, d := 10.5, 0.1
	cases := []struct {
		f    float64
		want string
	}{
		{3, "3"},
		{c + d, "10.6"},
		{1.5, "1.5"},
		{-2.25, "-2.25"},
		{17179869185.5, "17179869185.5"},
		{1e21, "1000000000000000000000"},
		{1.5e-7, "0.00000015"},
		{1e-20, "0"},
		{-1e-20, "0"},
		{1.23456789e-15, "0.00000000000000123"},
		{math.Copysign(0, -1), "0"},
		{math.Inf(1), "inf"},
		{math.Inf(-1), "-inf"},
	}
	for _, tc := range cases {
		if got := FormatFloatHuman(tc.f); got != tc.want {
			t.Errorf("FormatFloatHuman(%v) = %q, want %q", tc.f, got, tc.want)
		}
	}
}
