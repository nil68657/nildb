package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/nil68657/nildb/internal/resp"
)

// Kind is the RESP type of a decoded reply.
type Kind uint8

// Reply kinds. RESP2's null bulk string and null array decode to KindNull,
// as RESP3's "_" does; a blob error ("!") decodes to KindError.
const (
	KindSimple   Kind = iota + 1 // +
	KindError                    // - and !
	KindInt                      // :
	KindBulk                     // $
	KindNull                     // _, $-1, *-1
	KindDouble                   // ,
	KindBool                     // #
	KindBig                      // (
	KindVerbatim                 // =
	KindArray                    // *
	KindMap                      // %
	KindSet                      // ~
	KindPush                     // >
)

var kindNames = [...]string{
	KindSimple: "simple", KindError: "error", KindInt: "int", KindBulk: "bulk",
	KindNull: "null", KindDouble: "double", KindBool: "bool", KindBig: "big",
	KindVerbatim: "verbatim", KindArray: "array", KindMap: "map", KindSet: "set",
	KindPush: "push",
}

func (k Kind) String() string {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return kindNames[k]
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Value is one decoded reply.
type Value struct {
	Kind Kind
	// Str holds the text of simple strings, errors, bulk and verbatim
	// strings, and the digits of doubles ("3.14", "inf") and big numbers
	// exactly as the server sent them.
	Str    []byte
	Int    int64
	Bool   bool
	Format string  // verbatim format, "txt" or "mkd"
	Elems  []Value // array, set and push elements; map keys and values alternate
	Attrs  []Value // an attribute map ("|") sent before the value, keys and values alternating
}

// Text returns Str as a string.
func (v Value) Text() string { return string(v.Str) }

// IsErr reports whether v is an error reply.
func (v Value) IsErr() bool { return v.Kind == KindError }

// IsOK reports whether v is the status reply OK.
func (v Value) IsOK() bool { return v.Kind == KindSimple && string(v.Str) == "OK" }

// Lookup returns the value stored under key in a map reply, or in a RESP2
// flat array of alternating keys and values.
func (v Value) Lookup(key string) (Value, bool) {
	if v.Kind != KindMap && v.Kind != KindArray {
		return Value{}, false
	}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		if string(v.Elems[i].Str) == key {
			return v.Elems[i+1], true
		}
	}
	return Value{}, false
}

// Decoder limits.
const (
	// maxLine bounds a header line: a type byte, a length or a simple
	// string. NilDB's longest simple strings are HELP lines.
	maxLine = 64 << 10
	// maxDepth bounds how deeply aggregates nest.
	maxDepth = 64
	// maxPrealloc caps the first allocation for an aggregate, so a count
	// line announcing 2^31 elements costs nothing up front.
	maxPrealloc = 1024
	// elemCost is what one element costs against the byte budget, so a
	// reply of a million empty strings is refused like a large one.
	elemCost = 16
)

// Protocol errors: the stream can no longer be trusted and the connection
// must be closed.
var (
	errMaxLine = errors.New("resp3: header line longer than 64 KiB")
	errNoCRLF  = errors.New("resp3: line does not end in CRLF")
)

// ProtocolError reports a reply the decoder cannot parse.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "resp3: " + e.Msg }

func protoErrf(format string, a ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, a...)}
}

// tooLarge is the error value that replaces a reply over the byte budget.
// The decoder still reads the whole reply, so the connection stays in step.
func tooLarge(limit int64) Value {
	return Value{Kind: KindError, Str: []byte(fmt.Sprintf(
		"NILDBUI reply is larger than %d MiB; ask for less (SCAN instead of KEYS, a smaller LRANGE range)", limit>>20))}
}

// Decoder reads replies from a server connection. It is not safe for
// concurrent use.
type Decoder struct {
	br     *bufio.Reader
	budget int64 // payload bytes the current reply may still keep
	over   bool  // the current reply went over budget; the rest is read and dropped
}

// NewDecoder returns a Decoder over br.
func NewDecoder(br *bufio.Reader) *Decoder { return &Decoder{br: br} }

// Read decodes one complete reply. A reply whose strings and elements add
// up to more than limit bytes is read to its end and returned as an error
// value (tooLarge), so the caller can keep using the connection. io.EOF
// means the stream ended between replies; any returned error means the
// stream is broken.
func (d *Decoder) Read(limit int64) (Value, error) {
	if _, err := d.br.Peek(1); err != nil {
		return Value{}, err
	}
	d.budget, d.over = limit, false
	v, err := d.value(0)
	if err != nil {
		return Value{}, err
	}
	if d.over {
		return tooLarge(limit), nil
	}
	return v, nil
}

// spend charges n bytes to the budget and reports whether the payload
// should still be kept.
func (d *Decoder) spend(n int64) bool {
	if d.over {
		return false
	}
	d.budget -= n
	if d.budget < 0 {
		d.over = true
		return false
	}
	return true
}

// line returns the next line without its CRLF. The slice is valid until
// the next read.
func (d *Decoder) line() ([]byte, error) {
	b, err := d.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		acc := append([]byte(nil), b...)
		for err == bufio.ErrBufferFull {
			if len(acc) > maxLine {
				return nil, errMaxLine
			}
			b, err = d.br.ReadSlice('\n')
			acc = append(acc, b...)
		}
		b = acc
	}
	if err != nil {
		return nil, eofMidReply(err)
	}
	if len(b) > maxLine+2 {
		return nil, errMaxLine
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return nil, errNoCRLF
	}
	return b[:len(b)-2], nil
}

// eofMidReply turns io.EOF inside a reply into io.ErrUnexpectedEOF.
func eofMidReply(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// keep copies b when the budget allows, and returns nil otherwise.
func (d *Decoder) keep(b []byte) []byte {
	if !d.spend(int64(len(b)) + elemCost) {
		return nil
	}
	return append([]byte(nil), b...)
}

// length parses the count or length after a type byte. It accepts -1 (a
// RESP2 null) and reports "?" (a streamed RESP3 value) as stream.
func length(b []byte) (n int64, stream bool, err error) {
	if len(b) == 1 && b[0] == '?' {
		return 0, true, nil
	}
	n, ok := resp.ParseInt(b)
	if !ok || n < -1 {
		return 0, false, protoErrf("bad length %q", b)
	}
	return n, false, nil
}

// payload reads n bytes and the CRLF after them. Over budget the bytes
// are skipped and nil is returned.
func (d *Decoder) payload(n int64) ([]byte, error) {
	var out []byte
	if d.spend(n + elemCost) {
		out = make([]byte, n)
		if _, err := io.ReadFull(d.br, out); err != nil {
			return nil, eofMidReply(err)
		}
	} else if _, err := d.br.Discard(int(n)); err != nil {
		return nil, eofMidReply(err)
	}
	var crlf [2]byte
	if _, err := io.ReadFull(d.br, crlf[:]); err != nil {
		return nil, eofMidReply(err)
	}
	if crlf != [2]byte{'\r', '\n'} {
		return nil, errNoCRLF
	}
	return out, nil
}

// streamed reads the chunks of a "$?" string until the empty chunk.
func (d *Decoder) streamed() ([]byte, error) {
	var out []byte
	for {
		b, err := d.line()
		if err != nil {
			return nil, err
		}
		if len(b) == 0 || b[0] != ';' {
			return nil, protoErrf("expected a ';' chunk in a streamed string, got %q", b)
		}
		n, ok := resp.ParseInt(b[1:])
		if !ok || n < 0 {
			return nil, protoErrf("bad chunk length %q", b)
		}
		if n == 0 {
			return out, nil
		}
		chunk, err := d.payload(n)
		if err != nil {
			return nil, err
		}
		if !d.over {
			out = append(out, chunk...)
		}
	}
}

func (d *Decoder) value(depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, protoErrf("aggregates nested deeper than %d", maxDepth)
	}
	b, err := d.line()
	if err != nil {
		return Value{}, err
	}
	if len(b) == 0 {
		return Value{}, protoErrf("empty line")
	}
	body := b[1:]
	switch b[0] {
	case '+':
		return Value{Kind: KindSimple, Str: d.keep(body)}, nil
	case '-':
		return Value{Kind: KindError, Str: d.keep(body)}, nil
	case ':':
		n, ok := resp.ParseInt(body)
		if !ok {
			return Value{}, protoErrf("bad integer %q", body)
		}
		return Value{Kind: KindInt, Int: n}, nil
	case '_':
		return Value{Kind: KindNull}, nil
	case ',':
		if len(body) == 0 {
			return Value{}, protoErrf("empty double")
		}
		return Value{Kind: KindDouble, Str: d.keep(body)}, nil
	case '(':
		if len(body) == 0 {
			return Value{}, protoErrf("empty big number")
		}
		return Value{Kind: KindBig, Str: d.keep(body)}, nil
	case '#':
		switch string(body) {
		case "t":
			return Value{Kind: KindBool, Bool: true}, nil
		case "f":
			return Value{Kind: KindBool}, nil
		}
		return Value{}, protoErrf("bad boolean %q", body)
	case '$', '!', '=':
		return d.blob(b[0], body)
	case '*', '~', '>', '%':
		return d.aggregate(b[0], body, depth)
	case '|':
		n, _, err := length(body)
		if err != nil || n < 0 {
			return Value{}, protoErrf("bad attribute count %q", body)
		}
		attrs, err := d.elems(2*n, depth)
		if err != nil {
			return Value{}, err
		}
		v, err := d.value(depth)
		if err != nil {
			return Value{}, err
		}
		v.Attrs = attrs
		return v, nil
	}
	return Value{}, protoErrf("unknown type byte %q", b[0])
}

// blob reads a bulk string, blob error or verbatim string.
func (d *Decoder) blob(t byte, body []byte) (Value, error) {
	n, stream, err := length(body)
	if err != nil {
		return Value{}, err
	}
	var data []byte
	switch {
	case stream && t == '$':
		if data, err = d.streamed(); err != nil {
			return Value{}, err
		}
	case stream:
		return Value{}, protoErrf("%q cannot be streamed", t)
	case n == -1 && t == '$':
		return Value{Kind: KindNull}, nil
	case n == -1:
		return Value{}, protoErrf("bad length %q", body)
	default:
		if data, err = d.payload(n); err != nil {
			return Value{}, err
		}
	}
	switch t {
	case '!':
		return Value{Kind: KindError, Str: data}, nil
	case '=':
		v := Value{Kind: KindVerbatim, Str: data}
		if d.over {
			return v, nil
		}
		if len(data) < 4 || data[3] != ':' {
			return Value{}, protoErrf("verbatim string without a format prefix")
		}
		v.Format, v.Str = string(data[:3]), data[4:]
		return v, nil
	}
	return Value{Kind: KindBulk, Str: data}, nil
}

var aggKinds = map[byte]Kind{'*': KindArray, '~': KindSet, '>': KindPush, '%': KindMap}

// aggregate reads an array, set, push or map, counted or streamed.
func (d *Decoder) aggregate(t byte, body []byte, depth int) (Value, error) {
	n, stream, err := length(body)
	if err != nil {
		return Value{}, err
	}
	kind := aggKinds[t]
	if n == -1 {
		if t != '*' {
			return Value{}, protoErrf("bad count %q", body)
		}
		return Value{Kind: KindNull}, nil
	}
	if stream {
		elems, err := d.streamedElems(t == '%', depth)
		return Value{Kind: kind, Elems: elems}, err
	}
	if t == '%' {
		n *= 2
	}
	elems, err := d.elems(n, depth)
	return Value{Kind: kind, Elems: elems}, err
}

func (d *Decoder) elems(n int64, depth int) ([]Value, error) {
	out := make([]Value, 0, min(n, maxPrealloc))
	for range n {
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		if d.spend(elemCost) {
			out = append(out, v)
		}
	}
	return out, nil
}

// streamedElems reads elements until the "." end marker; a map's end
// marker must fall between pairs.
func (d *Decoder) streamedElems(pairs bool, depth int) ([]Value, error) {
	var out []Value
	for i := 0; ; i++ {
		c, err := d.br.Peek(1)
		if err != nil {
			return nil, eofMidReply(err)
		}
		if c[0] == '.' {
			b, err := d.line()
			if err != nil {
				return nil, err
			}
			if len(b) != 1 || (pairs && i%2 != 0) {
				return nil, protoErrf("bad end of a streamed aggregate %q", b)
			}
			return out, nil
		}
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		if d.spend(elemCost) {
			out = append(out, v)
		}
	}
}

// AppendCommand appends args as a RESP multibulk request.
func AppendCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// maxSafeInt is Number.MAX_SAFE_INTEGER: integers beyond it are sent to
// the browser as strings so ids such as DOC.FIND cursor ids survive.
const maxSafeInt = 1<<53 - 1

// AppendJSON appends v in the console's JSON form: {"t": kind, "v": ...}.
// Strings that are not valid UTF-8 carry "b64" instead of "v"; integers
// outside ±(2^53-1) are strings; maps are [[key, value], ...]; attributes
// are "attrs" in the map form.
func AppendJSON(dst []byte, v *Value) []byte {
	dst = append(dst, `{"t":"`...)
	dst = append(dst, v.Kind.String()...)
	dst = append(dst, '"')
	switch v.Kind {
	case KindSimple, KindError, KindDouble, KindBig:
		dst = append(dst, `,"v":`...)
		dst = appendJSONString(dst, v.Str)
	case KindBulk, KindVerbatim:
		if v.Kind == KindVerbatim {
			dst = append(dst, `,"f":`...)
			dst = appendJSONString(dst, []byte(v.Format))
		}
		if utf8.Valid(v.Str) {
			dst = append(dst, `,"v":`...)
			dst = appendJSONString(dst, v.Str)
		} else {
			dst = append(dst, `,"b64":"`...)
			dst = base64.StdEncoding.AppendEncode(dst, v.Str)
			dst = append(dst, '"')
		}
	case KindInt:
		dst = append(dst, `,"v":`...)
		if v.Int >= -maxSafeInt && v.Int <= maxSafeInt {
			dst = strconv.AppendInt(dst, v.Int, 10)
		} else {
			dst = append(dst, '"')
			dst = strconv.AppendInt(dst, v.Int, 10)
			dst = append(dst, '"')
		}
	case KindBool:
		dst = append(dst, `,"v":`...)
		dst = strconv.AppendBool(dst, v.Bool)
	case KindArray, KindSet, KindPush:
		dst = append(dst, `,"v":`...)
		dst = appendJSONList(dst, v.Elems)
	case KindMap:
		dst = append(dst, `,"v":`...)
		dst = appendJSONPairs(dst, v.Elems)
	}
	if len(v.Attrs) > 0 {
		dst = append(dst, `,"attrs":`...)
		dst = appendJSONPairs(dst, v.Attrs)
	}
	return append(dst, '}')
}

func appendJSONList(dst []byte, vs []Value) []byte {
	dst = append(dst, '[')
	for i := range vs {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = AppendJSON(dst, &vs[i])
	}
	return append(dst, ']')
}

func appendJSONPairs(dst []byte, vs []Value) []byte {
	dst = append(dst, '[')
	for i := 0; i+1 < len(vs); i += 2 {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '[')
		dst = AppendJSON(dst, &vs[i])
		dst = append(dst, ',')
		dst = AppendJSON(dst, &vs[i+1])
		dst = append(dst, ']')
	}
	return append(dst, ']')
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string. Invalid UTF-8 becomes
// U+FFFD; callers that must keep the bytes send base64 instead.
func appendJSONString(dst, s []byte) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				dst = append(dst, '\\', c)
			case c == '\n':
				dst = append(dst, '\\', 'n')
			case c == '\r':
				dst = append(dst, '\\', 'r')
			case c == '\t':
				dst = append(dst, '\\', 't')
			case c < 0x20 || c == 0x7f:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			default:
				dst = append(dst, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRune(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, `\ufffd`...)
		} else {
			dst = append(dst, s[i:i+size]...)
		}
		i += size
	}
	return append(dst, '"')
}
