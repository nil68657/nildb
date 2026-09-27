// Package resp speaks the Redis serialization protocol the way Redis 7.2
// does on a client connection. Reader reproduces the request parser of
// processInlineBuffer and processMultibulkBuffer (networking.c) together
// with its size caps and its "Protocol error" texts; Writer produces the
// RESP2 and RESP3 encodings of the addReply* family in the same file. A
// Reply value lets a command handler describe its answer once and have the
// server encode it for whichever protocol version the client picked with
// HELLO.
package resp

import (
	"bytes"
	"io"
	"math"
)

// Limits holds the request size caps of Redis 7.2. NewReader replaces a
// zero or negative field with its DefaultLimits value.
type Limits struct {
	// InlineMax is PROTO_INLINE_MAX_SIZE: how many bytes of an inline
	// request, a multibulk count line or a bulk length line may be
	// buffered without the line terminator before the request is refused.
	InlineMax int
	// BulkMax is proto-max-bulk-len, the longest bulk argument.
	BulkMax int64
	// PreAuthArgs is the largest multibulk count accepted before AUTH.
	PreAuthArgs int
	// PreAuthBulk is the longest bulk argument accepted before AUTH.
	PreAuthBulk int
}

// DefaultLimits returns the caps Redis 7.2 ships with: 64 KiB lines,
// 512 MiB bulks, and 10 arguments of at most 16384 bytes before AUTH.
func DefaultLimits() Limits {
	return Limits{InlineMax: 64 << 10, BulkMax: 512 << 20, PreAuthArgs: 10, PreAuthBulk: 16384}
}

const (
	// readChunk is PROTO_IOBUF_LEN. Each read asks for at most this many
	// bytes, as readQueryFromClient does, so the inline size check sees
	// the same amount of input Redis would.
	readChunk = 16 << 10
	// idleBufMax is the buffer size above which a drained buffer is
	// dropped and reallocated at readChunk, so one long line does not pin
	// a large buffer for the life of the connection.
	idleBufMax = 64 << 10
	// bigArg is PROTO_MBULK_BIG_ARG. A bulk payload at least this long
	// that is not already buffered is read straight into its own slice.
	bigArg = 32 << 10
	// bigArgInitial is the first allocation for such a payload.
	bigArgInitial = 1 << 20
	// intMax is INT_MAX, the ceiling Redis puts on a multibulk count.
	intMax = math.MaxInt32
	// argvInitial caps the first argv allocation, like the min(count, 1024)
	// in processMultibulkBuffer, so a count of 2^31-1 costs nothing up front.
	argvInitial = 1024
	// maxEmptyReads is how many (0, nil) reads in a row count as a stuck
	// reader, the same bound bufio uses.
	maxEmptyReads = 100
)

const (
	errTooBigInline = "Protocol error: too big inline request"
	errTooBigMbulk  = "Protocol error: too big mbulk count string"
	errTooBigBulk   = "Protocol error: too big bulk count string"
	errUnbalanced   = "Protocol error: unbalanced quotes in request"
	errInvalidMbulk = "Protocol error: invalid multibulk length"
	errUnauthMbulk  = "Protocol error: unauthenticated multibulk length"
	errInvalidBulk  = "Protocol error: invalid bulk length"
	errUnauthBulk   = "Protocol error: unauthenticated bulk length"
)

// Reader parses requests from a connection. It keeps its own input buffer
// (the querybuf of a Redis client) and the state of a multibulk frame in
// progress, so a request split across reads is assembled and a transient
// read error, such as a deadline, loses nothing already parsed. A Reader
// is not safe for concurrent use.
type Reader struct {
	rd     io.Reader
	buf    []byte // buf[pos:end] is input read but not yet consumed
	pos    int
	end    int
	rerr   error // error returned by rd together with data, delivered by the next fill
	limits Limits
	authed bool

	want    int      // argument count of the multibulk in progress, 0 outside one
	argv    [][]byte // arguments parsed so far
	bulklen int64    // length of the argument being read, -1 before its length line
	bulk    []byte   // payload of a big argument being read straight from rd; len is the bytes filled
}

// NewReader returns a Reader over r with the given limits. The pre-AUTH
// caps apply until SetAuthenticated(true); a server without a password
// calls that as soon as the connection opens.
func NewReader(r io.Reader, l Limits) *Reader {
	d := DefaultLimits()
	if l.InlineMax <= 0 {
		l.InlineMax = d.InlineMax
	}
	if l.BulkMax <= 0 {
		l.BulkMax = d.BulkMax
	}
	if l.PreAuthArgs <= 0 {
		l.PreAuthArgs = d.PreAuthArgs
	}
	if l.PreAuthBulk <= 0 {
		l.PreAuthBulk = d.PreAuthBulk
	}
	return &Reader{rd: r, buf: make([]byte, readChunk), limits: l, bulklen: -1}
}

// SetAuthenticated lifts (true) or restores (false) the PreAuthArgs and
// PreAuthBulk caps.
func (r *Reader) SetAuthenticated(ok bool) { r.authed = ok }

// Buffered returns how many bytes are read but not yet consumed. A value
// above zero after Next means more pipelined input is already waiting, so
// the caller can hold its replies back and flush them in one write.
func (r *Reader) Buffered() int { return r.end - r.pos }

// Next returns the argument vector of the next request; args[0] is the
// command name as the client sent it. A request whose first byte is '*' is
// multibulk, anything else is inline. Empty inline lines and multibulk
// counts of zero or less are skipped, as Redis skips them.
//
// Every returned slice is a fresh copy that the caller owns and may keep
// after the next call.
//
// A *ProtocolError carries the text Redis sends after "-ERR " before it
// closes the connection; the caller replies and closes. io.EOF is returned
// only between requests; end of input inside a request is
// io.ErrUnexpectedEOF. Any other error from the underlying reader (a read
// deadline, for example) is returned as is, and a later call resumes the
// request where it stopped.
func (r *Reader) Next() ([][]byte, error) {
	for {
		if r.want == 0 {
			if r.pos == r.end {
				if err := r.fill(); err != nil {
					return nil, err
				}
			}
			if r.buf[r.pos] != '*' {
				args, err := r.readInline()
				if err != nil {
					return nil, err
				}
				if len(args) > 0 {
					return args, nil
				}
				continue
			}
			ok, err := r.readCount()
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		if err := r.readArgs(); err != nil {
			return nil, err
		}
		args := r.argv
		r.argv, r.want = nil, 0
		return args, nil
	}
}

// fill reads once from rd, at most readChunk bytes, into the free tail of
// buf. When the tail has less than readChunk free it first moves the
// unconsumed bytes to the front if that frees enough room, and grows the
// buffer otherwise. It returns an error only when the read delivered no
// bytes.
func (r *Reader) fill() error {
	if r.rerr != nil {
		err := r.rerr
		r.rerr = nil
		return err
	}
	if r.pos == r.end {
		r.pos, r.end = 0, 0
		if len(r.buf) > idleBufMax {
			r.buf = make([]byte, readChunk)
		}
	}
	if len(r.buf)-r.end < readChunk {
		live := r.end - r.pos
		if live+readChunk <= len(r.buf) {
			copy(r.buf, r.buf[r.pos:r.end])
		} else {
			nb := make([]byte, max(2*len(r.buf), live+readChunk))
			copy(nb, r.buf[r.pos:r.end])
			r.buf = nb
		}
		r.pos, r.end = 0, live
	}
	for range maxEmptyReads {
		n, err := r.rd.Read(r.buf[r.end : r.end+readChunk])
		if n < 0 {
			panic("resp: reader returned a negative count")
		}
		r.end += n
		if n > 0 {
			r.rerr = err
			return nil
		}
		if err != nil {
			return err
		}
	}
	return io.ErrNoProgress
}

// more reads more input for a request already under way, where end of
// input means a truncated frame.
func (r *Reader) more() error {
	err := r.fill()
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// protoErr drops the frame in progress and returns the error the server
// sends before it closes the connection.
func (r *Reader) protoErr(text string) error {
	r.want, r.argv, r.bulklen, r.bulk = 0, nil, -1, nil
	return &ProtocolError{Text: text}
}

// lineScan finds a line terminator the way strchr does on the NUL
// terminated querybuf in Redis: the first c counts only when no NUL byte
// comes before it; a NUL hides the terminator for good and the line can
// then only end in a size error. The scan remembers how far it has looked
// so input arriving a few bytes at a time is not searched again from the
// start.
type lineScan struct {
	from   int
	hidden bool
}

func (s *lineScan) find(b []byte, c byte) int {
	if s.hidden {
		return -1
	}
	rest := b[s.from:]
	i := bytes.IndexByte(rest, c)
	if i < 0 {
		if bytes.IndexByte(rest, 0) >= 0 {
			s.hidden = true
		} else {
			s.from = len(b)
		}
		return -1
	}
	if bytes.IndexByte(rest[:i], 0) >= 0 {
		s.hidden = true
		return -1
	}
	return s.from + i
}

// readInline consumes one inline request line, terminated by "\n" or
// "\r\n", and splits it into arguments. The returned slices are copies.
func (r *Reader) readInline() ([][]byte, error) {
	var scan lineScan
	var line []byte
	for {
		avail := r.buf[r.pos:r.end]
		if i := scan.find(avail, '\n'); i >= 0 {
			line = avail[:i]
			r.pos += i + 1
			break
		}
		if len(avail) > r.limits.InlineMax {
			return nil, r.protoErr(errTooBigInline)
		}
		if err := r.more(); err != nil {
			return nil, err
		}
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	args, ok := splitArgs(line)
	if !ok {
		return nil, r.protoErr(errUnbalanced)
	}
	return args, nil
}

// crLine returns the bytes before the next '\r' and consumes them, the
// '\r' and the byte after it, which Redis never checks is '\n'. The result
// aliases the buffer and is only valid until the next fill.
func (r *Reader) crLine(tooBig string) ([]byte, error) {
	var scan lineScan
	for {
		avail := r.buf[r.pos:r.end]
		i := scan.find(avail, '\r')
		if i >= 0 && i+2 <= len(avail) {
			r.pos += i + 2
			return avail[:i], nil
		}
		if i < 0 && len(avail) > r.limits.InlineMax {
			return nil, r.protoErr(tooBig)
		}
		if err := r.more(); err != nil {
			return nil, err
		}
	}
}

// readCount parses the "*<n>" line that opens a multibulk request and
// reports false when Redis treats the frame as a no-op (n <= 0).
func (r *Reader) readCount() (bool, error) {
	line, err := r.crLine(errTooBigMbulk)
	if err != nil {
		return false, err
	}
	n, ok := ParseInt(line[1:])
	if !ok || n > intMax {
		return false, r.protoErr(errInvalidMbulk)
	}
	if n > int64(r.limits.PreAuthArgs) && !r.authed {
		return false, r.protoErr(errUnauthMbulk)
	}
	if n <= 0 {
		return false, nil
	}
	r.want = int(n)
	r.argv = make([][]byte, 0, min(n, argvInitial))
	r.bulklen = -1
	return true, nil
}

// readArgs reads bulk arguments until the frame has all of them.
func (r *Reader) readArgs() error {
	for len(r.argv) < r.want {
		if r.bulklen < 0 {
			line, err := r.crLine(errTooBigBulk)
			if err != nil {
				return err
			}
			// An empty line means the '\r' itself sits where '$' belongs,
			// and that '\r' is what Redis prints.
			first := byte('\r')
			if len(line) > 0 {
				first = line[0]
			}
			if first != '$' {
				return r.protoErr(expectedDollar(first))
			}
			n, ok := ParseInt(line[1:])
			if !ok || n < 0 || n > r.limits.BulkMax {
				return r.protoErr(errInvalidBulk)
			}
			if n > int64(r.limits.PreAuthBulk) && !r.authed {
				return r.protoErr(errUnauthBulk)
			}
			r.bulklen = n
		}
		arg, err := r.readBulk()
		if err != nil {
			return err
		}
		r.argv = append(r.argv, arg)
		r.bulklen = -1
	}
	return nil
}

// readBulk returns a copy of the current argument's payload and skips the
// two bytes after it, which Redis never checks are "\r\n". A payload of
// bigArg or more that is not yet buffered is read from rd straight into
// the argument instead of growing the buffer to hold it. That slice starts
// at bigArgInitial bytes and doubles as data arrives, so a length line
// announcing 512 MiB costs memory only for the bytes actually sent.
func (r *Reader) readBulk() ([]byte, error) {
	n := int(r.bulklen)
	if r.bulk == nil {
		for r.end-r.pos < n+2 {
			if n >= bigArg {
				have := min(r.end-r.pos, n)
				r.bulk = make([]byte, have, max(have, min(n, bigArgInitial)))
				copy(r.bulk, r.buf[r.pos:r.pos+have])
				r.pos += have
				break
			}
			if err := r.more(); err != nil {
				return nil, err
			}
		}
		if r.bulk == nil {
			arg := make([]byte, n)
			copy(arg, r.buf[r.pos:])
			r.pos += n + 2
			return arg, nil
		}
	}
	for stalls := 0; len(r.bulk) < n; {
		if r.rerr != nil {
			err := r.rerr
			r.rerr = nil
			return nil, eofMidFrame(err)
		}
		if len(r.bulk) == cap(r.bulk) {
			grown := make([]byte, len(r.bulk), min(2*cap(r.bulk), n))
			copy(grown, r.bulk)
			r.bulk = grown
		}
		m, err := r.rd.Read(r.bulk[len(r.bulk):cap(r.bulk)])
		if m < 0 {
			panic("resp: reader returned a negative count")
		}
		r.bulk = r.bulk[:len(r.bulk)+m]
		switch {
		case err != nil && len(r.bulk) < n:
			return nil, eofMidFrame(err)
		case err != nil:
			r.rerr = err
		case m == 0:
			if stalls++; stalls == maxEmptyReads {
				return nil, io.ErrNoProgress
			}
		default:
			stalls = 0
		}
	}
	for r.end-r.pos < 2 {
		if err := r.more(); err != nil {
			return nil, err
		}
	}
	r.pos += 2
	arg := r.bulk
	r.bulk = nil
	return arg, nil
}

// eofMidFrame turns io.EOF into io.ErrUnexpectedEOF for reads inside a
// request and passes every other error through.
func eofMidFrame(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// expectedDollar builds the "expected '$', got 'c'" text with the raw
// byte, then maps CR and LF to spaces as addReplyErrorFormat does.
func expectedDollar(c byte) string {
	if c == '\r' || c == '\n' {
		c = ' '
	}
	return "Protocol error: expected '$', got '" + string([]byte{c}) + "'"
}

// isSpace is isspace in the C locale.
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexVal(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// splitArgs is sdssplitargs from sds.c. Arguments are separated by
// whitespace. Double quotes take the escapes \xHH \n \r \t \a \b and
// \<c> for c itself; single quotes take only \'. A closing quote must be
// followed by whitespace or the end of the line. An unquoted token ends
// at space, tab, CR or LF but not at \v or \f, which only count as
// whitespace between tokens. It reports false for unbalanced quotes. The
// line must not contain a NUL byte; the caller guarantees that because a
// NUL hides the line terminator.
func splitArgs(line []byte) ([][]byte, bool) {
	var argv [][]byte
	n := len(line)
	p := 0
	for {
		for p < n && isSpace(line[p]) {
			p++
		}
		if p >= n {
			return argv, true
		}
		cur := []byte{}
		inq, insq, done := false, false, false
		for !done {
			switch {
			case inq:
				switch {
				case p+3 < n && line[p] == '\\' && line[p+1] == 'x' && isHex(line[p+2]) && isHex(line[p+3]):
					cur = append(cur, hexVal(line[p+2])<<4|hexVal(line[p+3]))
					p += 3
				case p+1 < n && line[p] == '\\':
					p++
					c := line[p]
					switch c {
					case 'n':
						c = '\n'
					case 'r':
						c = '\r'
					case 't':
						c = '\t'
					case 'b':
						c = '\b'
					case 'a':
						c = '\a'
					}
					cur = append(cur, c)
				case p < n && line[p] == '"':
					if p+1 < n && !isSpace(line[p+1]) {
						return nil, false
					}
					done = true
				case p >= n:
					return nil, false
				default:
					cur = append(cur, line[p])
				}
			case insq:
				switch {
				case p+1 < n && line[p] == '\\' && line[p+1] == '\'':
					p++
					cur = append(cur, '\'')
				case p < n && line[p] == '\'':
					if p+1 < n && !isSpace(line[p+1]) {
						return nil, false
					}
					done = true
				case p >= n:
					return nil, false
				default:
					cur = append(cur, line[p])
				}
			default:
				if p >= n {
					done = true
					break
				}
				switch line[p] {
				case ' ', '\n', '\r', '\t':
					done = true
				case '"':
					inq = true
				case '\'':
					insq = true
				default:
					cur = append(cur, line[p])
				}
			}
			if p < n {
				p++
			}
		}
		argv = append(argv, cur)
	}
}
