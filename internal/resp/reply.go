package resp

import (
	"bytes"
	"fmt"
)

// Reply is a value a command handler returns; the connection loop encodes
// it with Writer.Write for the client's protocol version. Only this
// package implements Reply; build one with the constructors below.
//
// Replies built from strings, integers, floats and booleans are
// comparable with ==, so a test can write reply == resp.ErrSyntax.
// Replies that hold slices (Bulk, Array, Map, Set, Verbatim, Raw, Stream)
// are pointers and compare by identity.
type Reply interface{ emit(w *Writer) }

type (
	statusReply string
	errReply    string
	intReply    int64
	strReply    string
	doubleReply float64
	boolReply   bool
	nullReply   struct{}
	nullArray   struct{}
	bulkReply   struct{ b []byte }
	rawReply    struct{ b []byte }
	verbatim    struct{ format, s string }
	aggReply    struct {
		kind  byte // '*' array, '%' map, '~' set
		items []Reply
	}
	streamReply struct{ fn func(w *Writer) }
)

func (r statusReply) emit(w *Writer) { w.Status(string(r)) }
func (r errReply) emit(w *Writer)    { w.Error(string(r)) }
func (r intReply) emit(w *Writer)    { w.Int(int64(r)) }
func (r strReply) emit(w *Writer)    { w.Str(string(r)) }
func (r doubleReply) emit(w *Writer) { w.Double(float64(r)) }
func (r boolReply) emit(w *Writer)   { w.Bool(bool(r)) }
func (nullReply) emit(w *Writer)     { w.Null() }
func (nullArray) emit(w *Writer)     { w.NullArray() }
func (r *bulkReply) emit(w *Writer)  { w.Bulk(r.b) }
func (r *rawReply) emit(w *Writer)   { w.Raw(r.b) }
func (r *verbatim) emit(w *Writer)   { w.Verbatim(r.format, r.s) }
func (r *streamReply) emit(w *Writer) {
	r.fn(w)
}

func (r *aggReply) emit(w *Writer) {
	switch r.kind {
	case '%':
		w.MapHeader(len(r.items) / 2)
	case '~':
		w.SetHeader(len(r.items))
	default:
		w.ArrayHeader(len(r.items))
	}
	for _, it := range r.items {
		w.Write(it)
	}
}

// OK is "+OK".
func OK() Reply { return statusReply("OK") }

// Status is a simple string, "+s". CR and LF in s are written as spaces.
func Status(s string) Reply { return statusReply(s) }

// Int is an integer reply, ":n".
func Int(n int64) Reply { return intReply(n) }

// Bulk is a bulk string holding b. The reply keeps b, so the caller must
// not change it afterwards. A nil b is an empty string, not a null.
func Bulk(b []byte) Reply { return &bulkReply{b: b} }

// Str is a bulk string holding s.
func Str(s string) Reply { return strReply(s) }

// Null is the null bulk string: "$-1" in RESP2, "_" in RESP3.
func Null() Reply { return nullReply{} }

// NullArray is the null array: "*-1" in RESP2, "_" in RESP3.
func NullArray() Reply { return nullArray{} }

// Array is "*n" followed by the items. A nil item is written as Null().
func Array(items ...Reply) Reply { return &aggReply{kind: '*', items: items} }

// Map takes keys and values alternately (k1, v1, k2, v2, ...) and is
// "%n" in RESP3 and a flat "*2n" array in RESP2. It panics on an odd
// number of arguments.
func Map(pairs ...Reply) Reply {
	if len(pairs)%2 != 0 {
		panic("resp: Map needs an even number of arguments, got " + fmt.Sprint(len(pairs)))
	}
	return &aggReply{kind: '%', items: pairs}
}

// Set is "~n" in RESP3 and "*n" in RESP2.
func Set(items ...Reply) Reply { return &aggReply{kind: '~', items: items} }

// Double is a bulk string in RESP2 and ",<f>" in RESP3, printed by
// FormatFloat in both.
func Double(f float64) Reply { return doubleReply(f) }

// Bool is ":1" or ":0" in RESP2 and "#t" or "#f" in RESP3.
func Bool(b bool) Reply { return boolReply(b) }

// Verbatim is a plain bulk string in RESP2 and "=<len>\r\n<format>:<s>"
// in RESP3. format is "txt" or "mkd".
func Verbatim(format, s string) Reply { return &verbatim{format: format, s: s} }

// Err is an error reply. text starts with the error code, as in
// "ERR syntax error" or "WRONGTYPE Operation against ...". CR and LF are
// written as spaces.
func Err(text string) Reply { return errReply(text) }

// Errorf is Err(fmt.Sprintf(format, a...)).
func Errorf(format string, a ...any) Reply { return errReply(fmt.Sprintf(format, a...)) }

// Raw is pre-encoded RESP, written unchanged in either protocol. EXEC uses
// it to replay replies encoded with Encode.
func Raw(b []byte) Reply { return &rawReply{b: b} }

// Stream defers a reply to encoding time: fn receives the Writer and must
// write exactly one complete value with the Writer's methods. It lets a
// handler return a large reply (LRANGE, HGETALL, ZRANGE) without building
// one Reply per element. fn runs after the handler has returned, when the
// connection loop writes the reply, so it must not depend on locks,
// snapshots or iterators the handler releases; read the data inside the
// handler and let fn only encode it.
func Stream(fn func(w *Writer)) Reply { return &streamReply{fn: fn} }

// Encode renders r for protocol version proto (3 for RESP3, anything else
// for RESP2) and returns the bytes. The server uses it to hold EXEC
// results and tests use it to compare output.
func Encode(r Reply, proto int) []byte {
	w := Writer{proto: proto}
	w.Write(r)
	return w.buf
}

// IsErr reports whether r is an error reply: one built by Err, Errorf or
// an error variable, or a Raw reply whose bytes start with '-'.
func IsErr(r Reply) bool {
	_, ok := ErrorText(r)
	return ok
}

// ErrorText returns the text of an error reply without the leading '-',
// such as "ERR syntax error", and false for any other reply. For a Raw
// reply it returns the first line after the '-'.
func ErrorText(r Reply) (string, bool) {
	switch v := r.(type) {
	case errReply:
		s := string(v)
		if len(s) > 0 && s[0] == '-' {
			s = s[1:]
		}
		return s, true
	case *rawReply:
		if len(v.b) == 0 || v.b[0] != '-' {
			return "", false
		}
		line := v.b[1:]
		if i := bytes.Index(line, []byte("\r\n")); i >= 0 {
			line = line[:i]
		}
		return string(line), true
	}
	return "", false
}
