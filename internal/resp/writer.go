package resp

import (
	"io"
	"strconv"
	"strings"
)

const (
	// writeBufSize is how many encoded bytes the Writer holds before it
	// writes them out on its own.
	writeBufSize = 64 << 10
	// directWriteMin is the payload size from which Bulk and Raw write the
	// payload straight to the connection instead of copying it into the
	// buffer.
	directWriteMin = 16 << 10
)

// Writer encodes replies for one connection. It buffers up to 64 KiB and
// writes to the underlying io.Writer when that fills or on Flush, so a
// pipelined batch of small replies leaves in one write. The protocol
// version starts at 2 and changes with SetProto after HELLO.
//
// Write errors are sticky: the first one is kept, later output is
// dropped, and Flush returns it. A Writer is not safe for concurrent use.
type Writer struct {
	w     io.Writer // nil for the Writer inside Encode
	buf   []byte
	proto int
	err   error
}

// NewWriter returns a RESP2 Writer over w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, buf: make([]byte, 0, writeBufSize), proto: 2}
}

// SetProto selects RESP3 when v is 3 and RESP2 for any other value.
func (w *Writer) SetProto(v int) {
	if v == 3 {
		w.proto = 3
	} else {
		w.proto = 2
	}
}

// Proto returns 2 or 3.
func (w *Writer) Proto() int {
	if w.proto == 3 {
		return 3
	}
	return 2
}

// Buffered returns how many encoded bytes wait for the next write.
func (w *Writer) Buffered() int { return len(w.buf) }

// Write encodes one reply. A nil Reply is written as Null().
func (w *Writer) Write(r Reply) {
	if r == nil {
		w.Null()
		return
	}
	r.emit(w)
}

// Flush writes the buffered bytes and returns the first write error seen
// since the Writer was created.
func (w *Writer) Flush() error {
	w.drain()
	return w.err
}

// drain hands the buffer to the underlying writer. The Encode writer has
// none and keeps everything.
func (w *Writer) drain() {
	if w.w == nil {
		return
	}
	if len(w.buf) > 0 && w.err == nil {
		_, w.err = w.w.Write(w.buf)
	}
	w.buf = w.buf[:0]
}

// done writes the buffer out once it holds writeBufSize bytes.
func (w *Writer) done() {
	if len(w.buf) >= writeBufSize && w.w != nil {
		w.drain()
	}
}

func (w *Writer) crlf() { w.buf = append(w.buf, '\r', '\n') }

// header writes "<prefix><n>\r\n".
func (w *Writer) header(prefix byte, n int64) {
	w.buf = append(w.buf, prefix)
	w.buf = strconv.AppendInt(w.buf, n, 10)
	w.crlf()
}

// appendLine writes s with CR and LF mapped to spaces, as Redis does for
// error texts built from client input; a raw CR or LF would end the line
// early and desynchronise the client.
func (w *Writer) appendLine(s string) {
	if strings.IndexAny(s, "\r\n") < 0 {
		w.buf = append(w.buf, s...)
		return
	}
	for i := range len(s) {
		c := s[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		w.buf = append(w.buf, c)
	}
}

// Status writes a simple string, "+s\r\n", in both protocols.
func (w *Writer) Status(s string) {
	w.buf = append(w.buf, '+')
	w.appendLine(s)
	w.crlf()
	w.done()
}

// Error writes "-text\r\n" in both protocols. text carries its own code
// ("ERR ...", "WRONGTYPE ..."); a leading '-' is dropped so it is never
// doubled.
func (w *Writer) Error(text string) {
	text = strings.TrimPrefix(text, "-")
	w.buf = append(w.buf, '-')
	w.appendLine(text)
	w.crlf()
	w.done()
}

// Int writes ":n\r\n".
func (w *Writer) Int(n int64) {
	w.header(':', n)
	w.done()
}

// ArrayHeader starts an array of n elements; the caller writes the n
// elements next.
func (w *Writer) ArrayHeader(n int) {
	w.header('*', int64(n))
	w.done()
}

// MapHeader starts a map of n key-value pairs: "%n" in RESP3 and a flat
// array "*2n" in RESP2. The caller writes 2n elements next, key first.
func (w *Writer) MapHeader(n int) {
	if w.proto == 3 {
		w.header('%', int64(n))
	} else {
		w.header('*', 2*int64(n))
	}
	w.done()
}

// SetHeader starts a set of n elements: "~n" in RESP3, "*n" in RESP2.
func (w *Writer) SetHeader(n int) {
	if w.proto == 3 {
		w.header('~', int64(n))
	} else {
		w.header('*', int64(n))
	}
	w.done()
}

// Bulk writes a bulk string. A nil slice is an empty string, not a null;
// use Null for that.
func (w *Writer) Bulk(b []byte) {
	w.header('$', int64(len(b)))
	w.payload(b)
	w.crlf()
	w.done()
}

// Str writes s as a bulk string.
func (w *Writer) Str(s string) {
	w.header('$', int64(len(s)))
	w.buf = append(w.buf, s...)
	w.crlf()
	w.done()
}

// payload appends b, or writes it straight through when it is large and
// there is a connection to write to.
func (w *Writer) payload(b []byte) {
	if len(b) < directWriteMin || w.w == nil {
		w.buf = append(w.buf, b...)
		return
	}
	w.drain()
	if w.err == nil {
		_, w.err = w.w.Write(b)
	}
}

// Null writes the null bulk string: "$-1\r\n" in RESP2, "_\r\n" in RESP3.
func (w *Writer) Null() {
	if w.proto == 3 {
		w.buf = append(w.buf, '_', '\r', '\n')
	} else {
		w.buf = append(w.buf, "$-1\r\n"...)
	}
	w.done()
}

// NullArray writes the null array: "*-1\r\n" in RESP2, "_\r\n" in RESP3.
func (w *Writer) NullArray() {
	if w.proto == 3 {
		w.buf = append(w.buf, '_', '\r', '\n')
	} else {
		w.buf = append(w.buf, "*-1\r\n"...)
	}
	w.done()
}

// Double writes f as FormatFloat prints it: a bulk string in RESP2 and
// ",<text>\r\n" in RESP3.
func (w *Writer) Double(f float64) {
	var tmp [32]byte
	s := AppendFloat(tmp[:0], f)
	if w.proto == 3 {
		w.buf = append(w.buf, ',')
	} else {
		w.header('$', int64(len(s)))
	}
	w.buf = append(w.buf, s...)
	w.crlf()
	w.done()
}

// Bool writes ":1" or ":0" in RESP2 and "#t" or "#f" in RESP3.
func (w *Writer) Bool(b bool) {
	switch {
	case w.proto == 3 && b:
		w.buf = append(w.buf, "#t\r\n"...)
	case w.proto == 3:
		w.buf = append(w.buf, "#f\r\n"...)
	case b:
		w.buf = append(w.buf, ":1\r\n"...)
	default:
		w.buf = append(w.buf, ":0\r\n"...)
	}
	w.done()
}

// Verbatim writes s as a plain bulk string in RESP2 and as
// "=<len>\r\n<fmt>:<s>\r\n" in RESP3. format is the three-byte type,
// "txt" or "mkd"; as in addReplyVerbatim, a shorter one is padded with
// spaces and a longer one cut to three bytes.
func (w *Writer) Verbatim(format, s string) {
	if w.proto != 3 {
		w.Str(s)
		return
	}
	w.header('=', int64(len(s))+4)
	for i := range 3 {
		c := byte(' ')
		if i < len(format) {
			c = format[i]
		}
		w.buf = append(w.buf, c)
	}
	w.buf = append(w.buf, ':')
	w.buf = append(w.buf, s...)
	w.crlf()
	w.done()
}

// Raw writes b unchanged. b must be one or more complete RESP values.
func (w *Writer) Raw(b []byte) {
	w.payload(b)
	w.done()
}
