package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"testing/iotest"
)

// readAll calls Next until it fails and returns every argument vector as
// strings together with the final error.
func readAll(r *Reader) ([][]string, error) {
	var out [][]string
	for {
		args, err := r.Next()
		if err != nil {
			return out, err
		}
		cmd := make([]string, len(args))
		for i, a := range args {
			cmd[i] = string(a)
		}
		out = append(out, cmd)
	}
}

// chunkReader returns at most n bytes per Read.
type chunkReader struct {
	b []byte
	n int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.b) == 0 {
		return 0, io.EOF
	}
	k := min(len(p), c.n, len(c.b))
	copy(p, c.b[:k])
	c.b = c.b[k:]
	return k, nil
}

// splitReader returns its input in the pieces given, one piece per Read.
type splitReader struct{ parts [][]byte }

func (s *splitReader) Read(p []byte) (int, error) {
	for len(s.parts) > 0 && len(s.parts[0]) == 0 {
		s.parts = s.parts[1:]
	}
	if len(s.parts) == 0 {
		return 0, io.EOF
	}
	k := copy(p, s.parts[0])
	s.parts[0] = s.parts[0][k:]
	return k, nil
}

var errTimeout = errors.New("i/o timeout")

// stallReader returns errTimeout with no data before every chunk of n
// bytes, the way a connection with a short read deadline behaves.
type stallReader struct {
	b       []byte
	n       int
	stalled bool
}

func (s *stallReader) Read(p []byte) (int, error) {
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	if !s.stalled {
		s.stalled = true
		return 0, errTimeout
	}
	s.stalled = false
	k := min(len(p), s.n, len(s.b))
	copy(p, s.b[:k])
	s.b = s.b[k:]
	return k, nil
}

func mbulk(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

const ping = "*1\r\n$4\r\nPING\r\n"

func pingCmds(n int) [][]string {
	out := make([][]string, n)
	for i := range out {
		out[i] = []string{"PING"}
	}
	return out
}

func repeatTo(prefix, fill string, n int) string {
	var b strings.Builder
	b.WriteString(prefix)
	for b.Len() < n {
		b.WriteString(fill)
	}
	return b.String()
}

func errText(err error) string {
	var pe *ProtocolError
	switch {
	case err == nil:
		return "<nil>"
	case errors.As(err, &pe):
		return pe.Text
	}
	return err.Error()
}

// protocolCases is tests/unit/protocol.tcl from Redis 7.2 plus the other
// branches of processInlineBuffer and processMultibulkBuffer. err is the
// Protocol error text, or the io error that ends the input.
var protocolCases = []struct {
	name   string
	in     string
	authed bool // SetAuthenticated(true) before reading
	want   [][]string
	err    string
}{
	{name: "empty query", in: "\r\n" + ping, authed: true, want: pingCmds(1), err: "EOF"},
	{name: "negative multibulk length", in: "*-10\r\n" + ping, authed: true, want: pingCmds(1), err: "EOF"},
	{name: "zero multibulk length", in: "*0\r\n" + ping, authed: true, want: pingCmds(1), err: "EOF"},
	{name: "out of range multibulk length", in: "*3000000000\r\n", authed: true,
		err: "Protocol error: invalid multibulk length"},
	{name: "multibulk length above INT_MAX", in: "*2147483648\r\n", authed: true,
		err: "Protocol error: invalid multibulk length"},
	{name: "wrong multibulk payload header", in: "*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n", authed: true,
		err: "Protocol error: expected '$', got 'f'"},
	{name: "negative multibulk payload length", in: "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n", authed: true,
		err: "Protocol error: invalid bulk length"},
	{name: "out of range multibulk payload length", in: "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n", authed: true,
		err: "Protocol error: invalid bulk length"},
	{name: "non-number multibulk payload length", in: "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$blabla\r\n", authed: true,
		err: "Protocol error: invalid bulk length"},
	{name: "multibulk not followed by bulk arguments", in: "*1\r\nfoo\r\n", authed: true,
		err: "Protocol error: expected '$', got 'f'"},
	{name: "unbalanced number of quotes", in: "set \"\"\"test-key\"\"\" test-value\r\nping\r\n", authed: true,
		err: "Protocol error: unbalanced quotes in request"},
	{name: "desync NUL", in: repeatTo("\x00", "payload", 70<<10), authed: true,
		err: "Protocol error: too big inline request"},
	{name: "desync star NUL", in: repeatTo("*\x00", "payload", 70<<10), authed: true,
		err: "Protocol error: too big mbulk count string"},
	{name: "desync dollar NUL", in: repeatTo("$\x00", "payload", 70<<10), authed: true,
		err: "Protocol error: too big inline request"},
	{name: "NUL hides a later newline", in: repeatTo("PING\x00\n", "A\n", 70<<10), authed: true,
		err: "Protocol error: too big inline request"},
	{name: "too big inline request", in: strings.Repeat("a", 70<<10), authed: true,
		err: "Protocol error: too big inline request"},
	{name: "too big mbulk count string", in: "*" + strings.Repeat("1", 70<<10), authed: true,
		err: "Protocol error: too big mbulk count string"},
	{name: "too big bulk count string", in: "*1\r\n$" + strings.Repeat("1", 70<<10), authed: true,
		err: "Protocol error: too big bulk count string"},
	{name: "NUL hides bulk count terminator", in: repeatTo("*1\r\n$4\x00\r\n", "A\r\n", 70<<10), authed: true,
		err: "Protocol error: too big bulk count string"},
	{name: "unauthenticated multibulk length", in: "*11\r\n", err: "Protocol error: unauthenticated multibulk length"},
	{name: "ten arguments before auth", in: mbulk("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"),
		want: [][]string{{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}, err: "EOF"},
	{name: "eleven arguments after auth", in: mbulk("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"), authed: true,
		want: [][]string{{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}}, err: "EOF"},
	{name: "invalid count wins over unauthenticated", in: "*3000000000\r\n", err: "Protocol error: invalid multibulk length"},
	{name: "unauthenticated bulk length", in: "*1\r\n$16385\r\n", err: "Protocol error: unauthenticated bulk length"},
	{name: "16384 byte bulk before auth", in: mbulk(strings.Repeat("x", 16384)),
		want: [][]string{{strings.Repeat("x", 16384)}}, err: "EOF"},
	{name: "invalid bulk wins over unauthenticated", in: "*1\r\n$-1\r\n", err: "Protocol error: invalid bulk length"},
	{name: "unauthenticated inline has no cap", in: "a b c d e f g h i j k l\r\n",
		want: [][]string{{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}}, err: "EOF"},
	{name: "empty bulk header line", in: "*1\r\n\r\n", authed: true, err: "Protocol error: expected '$', got ' '"},
	{name: "LF where dollar belongs", in: "*1\r\n\n\r\n", authed: true, err: "Protocol error: expected '$', got ' '"},
	{name: "plus sign in count", in: "*+1\r\n", authed: true, err: "Protocol error: invalid multibulk length"},
	{name: "leading zero in count", in: "*01\r\n", authed: true, err: "Protocol error: invalid multibulk length"},
	{name: "space in count", in: "*1 \r\n", authed: true, err: "Protocol error: invalid multibulk length"},
	{name: "empty count", in: "*\r\n", authed: true, err: "Protocol error: invalid multibulk length"},
	{name: "leading zero in bulk length", in: "*1\r\n$04\r\nPING\r\n", authed: true, err: "Protocol error: invalid bulk length"},
	{name: "empty bulk length", in: "*1\r\n$\r\n", authed: true, err: "Protocol error: invalid bulk length"},
	{name: "bulk above proto-max-bulk-len", in: "*1\r\n$536870913\r\n", authed: true, err: "Protocol error: invalid bulk length"},
	{name: "empty bulk", in: "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", authed: true, want: [][]string{{"ECHO", ""}}, err: "EOF"},
	{name: "bulk trailer is not checked", in: "*1\r\n$4\r\nPINGxx" + ping, authed: true, want: pingCmds(2), err: "EOF"},
	{name: "count line needs one byte after CR only", in: "*1\rX$4\r\nPING\r\n", authed: true, want: pingCmds(1), err: "EOF"},
	{name: "inline LF only", in: "PING\nPING\r\n", authed: true, want: pingCmds(2), err: "EOF"},
	{name: "inline blank lines", in: "\n \t \r\n\r\nPING\r\n", authed: true, want: pingCmds(1), err: "EOF"},
	{name: "inline strips one CR", in: "a\r\r\n", authed: true, want: [][]string{{"a"}}, err: "EOF"},
	{name: "inline quoting", in: "set \"a b\" 'c d'\r\n", authed: true, want: [][]string{{"set", "a b", "c d"}}, err: "EOF"},
	{name: "inline and multibulk mixed", in: "set k v\r\n" + mbulk("GET", "k") + "DEL k\n", authed: true,
		want: [][]string{{"set", "k", "v"}, {"GET", "k"}, {"DEL", "k"}}, err: "EOF"},
	{name: "binary bulk", in: mbulk("SET", "k", "a\r\n\x00b"), authed: true,
		want: [][]string{{"SET", "k", "a\r\n\x00b"}}, err: "EOF"},
	{name: "EOF inside multibulk", in: "*2\r\n$3\r\nGET\r\n", authed: true, err: "unexpected EOF"},
	{name: "EOF inside count line", in: "*2", authed: true, err: "unexpected EOF"},
	{name: "EOF inside bulk trailer", in: "*1\r\n$4\r\nPING\r", authed: true, err: "unexpected EOF"},
	{name: "EOF inside inline line", in: "PING", authed: true, err: "unexpected EOF"},
	{name: "CR without LF ends no inline line", in: "PING\r", authed: true, err: "unexpected EOF"},
	{name: "empty input", in: "", authed: true, err: "EOF"},
}

func TestProtocolTable(t *testing.T) {
	readers := []struct {
		name string
		wrap func([]byte) io.Reader
	}{
		{"whole", func(b []byte) io.Reader { return bytes.NewReader(b) }},
		{"one byte", func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) }},
		{"7 bytes", func(b []byte) io.Reader { return &chunkReader{b: b, n: 7} }},
		{"data with EOF", func(b []byte) io.Reader { return iotest.DataErrReader(bytes.NewReader(b)) }},
	}
	for _, tc := range protocolCases {
		for _, rd := range readers {
			t.Run(tc.name+"/"+rd.name, func(t *testing.T) {
				r := NewReader(rd.wrap([]byte(tc.in)), DefaultLimits())
				r.SetAuthenticated(tc.authed)
				got, err := readAll(r)
				if fmt.Sprint(got) != fmt.Sprint(tc.want) {
					t.Errorf("commands = %q, want %q", got, tc.want)
				}
				if errText(err) != tc.err {
					t.Errorf("error = %q, want %q", errText(err), tc.err)
				}
			})
		}
	}
}

func TestProtocolErrorReply(t *testing.T) {
	r := NewReader(strings.NewReader("*3000000000\r\n"), DefaultLimits())
	_, err := r.Next()
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v, want *ProtocolError", err)
	}
	if got, want := string(Encode(pe.Reply(), 2)), "-ERR Protocol error: invalid multibulk length\r\n"; got != want {
		t.Errorf("reply = %q, want %q", got, want)
	}
	if got, want := string(Encode(pe.Reply(), 3)), "-ERR Protocol error: invalid multibulk length\r\n"; got != want {
		t.Errorf("RESP3 reply = %q, want %q", got, want)
	}
}

func TestExpectedDollarRawByte(t *testing.T) {
	for _, c := range []byte{'f', '+', ':', 0x7f, 0xff} {
		r := NewReader(strings.NewReader("*1\r\n"+string([]byte{c})+"x\r\n"), DefaultLimits())
		r.SetAuthenticated(true)
		_, err := r.Next()
		want := "Protocol error: expected '$', got '" + string([]byte{c}) + "'"
		if errText(err) != want {
			t.Errorf("byte %#x: error = %q, want %q", c, errText(err), want)
		}
	}
}

func TestPipelinedPings(t *testing.T) {
	const n = 10000
	var buf bytes.Buffer
	for range n {
		buf.WriteString(ping)
	}
	r := NewReader(&buf, DefaultLimits())
	r.SetAuthenticated(true)
	for i := range n {
		args, err := r.Next()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if len(args) != 1 || string(args[0]) != "PING" {
			t.Fatalf("command %d = %q", i, args)
		}
		if i == 0 && r.Buffered() == 0 {
			t.Errorf("Buffered() = 0 after the first of %d pipelined commands", n)
		}
	}
	if r.Buffered() != 0 {
		t.Errorf("Buffered() = %d after the last command", r.Buffered())
	}
	if _, err := r.Next(); err != io.EOF {
		t.Errorf("after the last command: %v, want EOF", err)
	}
}

// mixedStream holds every request shape, including arguments on both
// sides of bigArg.
func mixedStream() string {
	return strings.Join([]string{
		ping,
		"set \"a b\" 'c\\'d' \"\\x41\\n\"\r\n",
		mbulk("SET", "key", "value"),
		"\r\n",
		"*0\r\n",
		mbulk("SET", "small", strings.Repeat("s", 1000)),
		mbulk("SET", "big", strings.Repeat("b", 40000)),
		"GET big\n",
		mbulk("MSET", "k1", "v1", "k2", "v2"),
		mbulk(""),
		ping,
	}, "")
}

func TestByteAtATime(t *testing.T) {
	in := mixedStream()
	want, wantErr := readAll(authed(NewReader(strings.NewReader(in), DefaultLimits())))
	if wantErr != io.EOF || len(want) != 9 {
		t.Fatalf("whole read: %d commands, %v", len(want), wantErr)
	}
	got, err := readAll(authed(NewReader(iotest.OneByteReader(strings.NewReader(in)), DefaultLimits())))
	if err != io.EOF {
		t.Fatalf("byte at a time: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("byte at a time parsed differently")
	}
	if got[1][2] != "c'd" || got[1][3] != "A\n" {
		t.Errorf("inline quoting: %q", got[1])
	}
	if len(got[4][2]) != 40000 {
		t.Errorf("big argument length %d", len(got[4][2]))
	}
}

func authed(r *Reader) *Reader {
	r.SetAuthenticated(true)
	return r
}

// TestEverySplit feeds a stream in two reads split at every offset, which
// catches a frame whose terminator arrives in the second read.
func TestEverySplit(t *testing.T) {
	in := []byte(strings.Join([]string{
		ping, "echo \"x y\"\r\n", mbulk("SET", "k", "v"), "\r\n", "*-1\r\n", mbulk("ECHO", ""), "PING\n",
	}, ""))
	want, _ := readAll(authed(NewReader(bytes.NewReader(in), DefaultLimits())))
	for k := range len(in) + 1 {
		r := authed(NewReader(&splitReader{parts: [][]byte{bytes.Clone(in[:k]), bytes.Clone(in[k:])}}, DefaultLimits()))
		got, err := readAll(r)
		if err != io.EOF || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("split at %d: %q, %v", k, got, err)
		}
	}
}

// TestBigArgSplits splits around the edges of an argument read straight
// into its own slice.
func TestBigArgSplits(t *testing.T) {
	payload := strings.Repeat("x", bigArg+100)
	in := []byte(mbulk("SET", "k", payload) + ping)
	head := len(in) - len(ping) - len(payload) - 2
	var splits []int
	for _, base := range []int{head, head + len(payload), len(in) - len(ping)} {
		for d := -3; d <= 3; d++ {
			splits = append(splits, base+d)
		}
	}
	for _, k := range splits {
		for _, k2 := range []int{k + 1, k + 2, k + 1000} {
			k2 = min(k2, len(in))
			parts := [][]byte{bytes.Clone(in[:k]), bytes.Clone(in[k:k2]), bytes.Clone(in[k2:])}
			got, err := readAll(authed(NewReader(&splitReader{parts: parts}, DefaultLimits())))
			if err != io.EOF || len(got) != 2 || got[0][2] != payload || got[1][0] != "PING" {
				t.Fatalf("splits %d,%d: %d commands, %v", k, k2, len(got), err)
			}
		}
	}
}

func TestBigArgGrows(t *testing.T) {
	payload := strings.Repeat("abcdefgh", (3<<20)/8+1)
	in := mbulk("SET", "k", payload) + ping
	for _, n := range []int{4096, 1 << 20, len(in)} {
		got, err := readAll(authed(NewReader(&chunkReader{b: []byte(in), n: n}, DefaultLimits())))
		if err != io.EOF || len(got) != 2 || got[0][2] != payload || got[1][0] != "PING" {
			t.Fatalf("chunk %d: %d commands, %v", n, len(got), err)
		}
	}
}

// TestBigArgAnnouncedNotSent checks that a length line alone does not
// allocate the announced size.
func TestBigArgAnnouncedNotSent(t *testing.T) {
	r := authed(NewReader(strings.NewReader("*1\r\n$536870912\r\nabc"), DefaultLimits()))
	if _, err := r.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("error = %v, want unexpected EOF", err)
	}
	if cap(r.bulk) > bigArgInitial {
		t.Errorf("allocated %d bytes for 3 received", cap(r.bulk))
	}
}

// TestResumeAfterTimeout checks that a read error in the middle of a
// request loses nothing: the next call picks up where the last stopped.
func TestResumeAfterTimeout(t *testing.T) {
	in := mixedStream()
	want, _ := readAll(authed(NewReader(strings.NewReader(in), DefaultLimits())))
	for _, n := range []int{1, 3, 1000, 20000} {
		r := authed(NewReader(&stallReader{b: []byte(in), n: n}, DefaultLimits()))
		var got [][]string
		timeouts := 0
		for {
			args, err := r.Next()
			if err == errTimeout {
				timeouts++
				continue
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("chunk %d: %v", n, err)
			}
			cmd := make([]string, len(args))
			for i, a := range args {
				cmd[i] = string(a)
			}
			got = append(got, cmd)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("chunk %d: parsed differently after %d timeouts", n, timeouts)
		}
		if timeouts == 0 {
			t.Errorf("chunk %d: no timeouts seen", n)
		}
	}
}

func TestArgsAreCopies(t *testing.T) {
	const n = 5000
	var b strings.Builder
	for i := range n {
		b.WriteString(mbulk("SET", fmt.Sprintf("k%d", i), "v"))
	}
	r := authed(NewReader(&chunkReader{b: []byte(b.String()), n: 100}, DefaultLimits()))
	var kept [][][]byte
	for {
		args, err := r.Next()
		if err != nil {
			break
		}
		kept = append(kept, args)
	}
	if len(kept) != n {
		t.Fatalf("parsed %d commands, want %d", len(kept), n)
	}
	for i, args := range kept {
		if string(args[1]) != fmt.Sprintf("k%d", i) {
			t.Fatalf("command %d key = %q after later reads", i, args[1])
		}
	}
}

func TestInlineMaxBoundary(t *testing.T) {
	l := Limits{InlineMax: 16}
	// One byte at a time, the check runs after every byte: a line of
	// exactly InlineMax bytes is accepted, one more byte is refused.
	ok := strings.Repeat("a", 16) + "\n"
	got, err := readAll(authed(NewReader(iotest.OneByteReader(strings.NewReader(ok)), l)))
	if err != io.EOF || len(got) != 1 {
		t.Errorf("16-byte line: %q, %v", got, err)
	}
	bad := strings.Repeat("a", 17) + "\n"
	_, err = readAll(authed(NewReader(iotest.OneByteReader(strings.NewReader(bad)), l)))
	if errText(err) != errTooBigInline {
		t.Errorf("17-byte line: %v", err)
	}
}

// TestInlineReadChunks checks the 16 KiB read size: as in Redis, the size
// check runs only when a read brings no newline, so with the default 64
// KiB limit a line whose newline arrives in the fifth 16 KiB read passes.
func TestInlineReadChunks(t *testing.T) {
	pass := strings.Repeat("a", 5*readChunk-1) + "\n"
	got, err := readAll(authed(NewReader(strings.NewReader(pass), DefaultLimits())))
	if err != io.EOF || len(got) != 1 || len(got[0][0]) != 5*readChunk-1 {
		t.Errorf("newline in the fifth read: %d commands, %v", len(got), err)
	}
	fail := strings.Repeat("a", 5*readChunk) + "\n"
	_, err = readAll(authed(NewReader(strings.NewReader(fail), DefaultLimits())))
	if errText(err) != errTooBigInline {
		t.Errorf("newline in the sixth read: %v", err)
	}
}

func TestCustomBulkMax(t *testing.T) {
	l := Limits{BulkMax: 10}
	got, err := readAll(authed(NewReader(strings.NewReader(mbulk("0123456789")), l)))
	if err != io.EOF || len(got) != 1 {
		t.Errorf("10-byte bulk: %q, %v", got, err)
	}
	_, err = readAll(authed(NewReader(strings.NewReader(mbulk("0123456789a")), l)))
	if errText(err) != errInvalidBulk {
		t.Errorf("11-byte bulk: %v", err)
	}
}

func TestAuthenticateMidStream(t *testing.T) {
	in := mbulk("AUTH", "pw") + mbulk("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
	r := NewReader(strings.NewReader(in), DefaultLimits())
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	r.SetAuthenticated(true)
	args, err := r.Next()
	if err != nil || len(args) != 11 {
		t.Fatalf("after auth: %d args, %v", len(args), err)
	}
}

func TestBufferShrinksWhenDrained(t *testing.T) {
	in := strings.Repeat("a", 70<<10) + "\n" + ping
	r := authed(NewReader(strings.NewReader(in), DefaultLimits()))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatal(err)
	}
	if len(r.buf) != readChunk {
		t.Errorf("buffer is %d bytes after draining, want %d", len(r.buf), readChunk)
	}
}

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		in   string
		want []string // nil with ok false means unbalanced
		ok   bool
	}{
		{"", nil, true},
		{"   ", nil, true},
		{"a", []string{"a"}, true},
		{"a b\tc", []string{"a", "b", "c"}, true},
		{"\v a", []string{"a"}, true},
		{"a\vb", []string{"a\vb"}, true},
		{"a\fb c", []string{"a\fb", "c"}, true},
		{`""`, []string{""}, true},
		{`''`, []string{""}, true},
		{`"a" "b"`, []string{"a", "b"}, true},
		{"\"a\"\t\"b\"", []string{"a", "b"}, true},
		{"\"a\"\v\"b\"", []string{"a", "b"}, true},
		{`ab"cd ef"`, []string{"abcd ef"}, true},
		{`ab'cd'`, []string{"abcd"}, true},
		{`"\x41\x4a\x4A"`, []string{"AJJ"}, true},
		{`"\x4"`, []string{"x4"}, true},
		{`"\xZZ"`, []string{"xZZ"}, true},
		{`"\n\r\t\b\a\"\\\q"`, []string{"\n\r\t\b\a\"\\q"}, true},
		{`'\n'`, []string{`\n`}, true},
		{`'it\'s'`, []string{"it's"}, true},
		{`'a\\b'`, []string{`a\\b`}, true},
		{`\x41`, []string{`\x41`}, true},
		{`"a b`, nil, false},
		{`'a b`, nil, false},
		{`"a"b`, nil, false},
		{`'a'b`, nil, false},
		{`"a\"`, nil, false},
		{`"a\`, nil, false},
		{`'a\'`, nil, false},
		{`"a"`, []string{"a"}, true},
	}
	for _, c := range cases {
		got, ok := splitArgs([]byte(c.in))
		var gs []string
		for _, g := range got {
			gs = append(gs, string(g))
		}
		if ok != c.ok || fmt.Sprintf("%q", gs) != fmt.Sprintf("%q", c.want) {
			t.Errorf("splitArgs(%q) = %q, %v; want %q, %v", c.in, gs, ok, c.want, c.ok)
		}
	}
}

func TestParseInt(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"-1", -1, true},
		{"42", 42, true},
		{"9223372036854775807", math.MaxInt64, true},
		{"-9223372036854775808", math.MinInt64, true},
		{"9223372036854775808", 0, false},
		{"-9223372036854775809", 0, false},
		{"18446744073709551615", 0, false},
		{"18446744073709551616", 0, false},
		{"99999999999999999999", 0, false},
		{"-99999999999999999999", 0, false},
		{"123456789012345678901", 0, false},
		{"-0", 0, false},
		{"+1", 0, false},
		{"007", 0, false},
		{"00", 0, false},
		{"-01", 0, false},
		{"", 0, false},
		{"-", 0, false},
		{" 1", 0, false},
		{"1 ", 0, false},
		{"1e3", 0, false},
		{"1.0", 0, false},
		{"0x10", 0, false},
		{"12a", 0, false},
	}
	for _, c := range cases {
		got, ok := ParseInt([]byte(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("ParseInt(%q) = %d, %v; want %d, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestParseFloat(t *testing.T) {
	inf := math.Inf(1)
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"1", 1, true},
		{"1e3", 1000, true},
		{"1E3", 1000, true},
		{"1e+3", 1000, true},
		{"1e-3", 0.001, true},
		{"3.14", 3.14, true},
		{"+1.5", 1.5, true},
		{"-1.5", -1.5, true},
		{".5", 0.5, true},
		{"5.", 5, true},
		{"1.e2", 100, true},
		{"007", 7, true},
		{"0", 0, true},
		{"0e0", 0, true},
		{"0.000", 0, true},
		{"inf", inf, true},
		{"+inf", inf, true},
		{"-inf", -inf, true},
		{"Inf", inf, true},
		{"INFINITY", inf, true},
		{"-Infinity", -inf, true},
		{"0x10", 16, true},
		{"0X1p4", 16, true},
		{"0x1.8p1", 3, true},
		{"-0x.8", -0.5, true},
		{"4.9e-324", 5e-324, true},
		{"1e-320", 1e-320, true},
		{"1.7976931348623157e308", math.MaxFloat64, true},
		{"nan", 0, false},
		{"NaN", 0, false},
		{"-nan", 0, false},
		{"nan(1)", 0, false},
		{"infin", 0, false},
		{"infinityx", 0, false},
		{"", 0, false},
		{" 1", 0, false},
		{"1 ", 0, false},
		{"\t1", 0, false},
		{"1e400", 0, false},
		{"-1e400", 0, false},
		{"1e-400", 0, false},
		{"0x1p-1100", 0, false},
		{"0x1p2000", 0, false},
		{".", 0, false},
		{"-", 0, false},
		{"+", 0, false},
		{"e5", 0, false},
		{"1e", 0, false},
		{"1e+", 0, false},
		{"0x", 0, false},
		{"0x.p1", 0, false},
		{"0x1p", 0, false},
		{"1_000", 0, false},
		{"0x_1p0", 0, false},
		{"1,5", 0, false},
		{"--1", 0, false},
		{"+-1", 0, false},
		{"1.5.5", 0, false},
		{"1e5.5", 0, false},
		{"one", 0, false},
		{"1\x00", 0, false},
		{strings.Repeat("1", maxFloatLen), 0, false},
	}
	for _, c := range cases {
		got, ok := ParseFloat([]byte(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("ParseFloat(%q) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
	if f, ok := ParseFloat([]byte("-0")); !ok || f != 0 || !math.Signbit(f) {
		t.Errorf("ParseFloat(-0) = %v, %v; want negative zero", f, ok)
	}
	long := strings.Repeat("0", maxFloatLen-2) + "1"
	if f, ok := ParseFloat([]byte(long)); !ok || f != 1 {
		t.Errorf("ParseFloat of a %d-byte number = %v, %v", len(long), f, ok)
	}
}
