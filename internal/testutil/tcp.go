package testutil

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// ioTimeout bounds every read and write of the raw client, so a missing
// reply fails the test instead of hanging it.
const ioTimeout = 10 * time.Second

// TCP is a raw RESP client for byte-exact tests. Its methods report
// failures through the testing.TB it was created with, so call them from
// the test goroutine.
type TCP struct {
	t    testing.TB
	conn net.Conn
	r    *bufio.Reader
}

// TCP dials the server. The connection is closed on t.Cleanup.
func (e *Env) TCP(t testing.TB) *TCP {
	t.Helper()
	nc, err := net.DialTimeout("tcp", e.Addr, ioTimeout)
	if err != nil {
		t.Fatalf("testutil: dial %s: %v", e.Addr, err)
	}
	c := &TCP{t: t, conn: nc, r: bufio.NewReaderSize(nc, 64<<10)}
	t.Cleanup(func() { nc.Close() })
	return c
}

// Conn returns the underlying connection.
func (c *TCP) Conn() net.Conn { return c.conn }

// Close closes the connection.
func (c *TCP) Close() { c.conn.Close() }

// Encode returns args as a RESP multibulk request.
func Encode(args ...string) []byte {
	b := []byte("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, "\r\n"...)
		b = append(b, a...)
		b = append(b, "\r\n"...)
	}
	return b
}

// Send writes b as is.
func (c *TCP) Send(b []byte) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatalf("testutil: send: %v", err)
	}
}

// Do sends args as one multibulk request and returns the raw bytes of
// exactly one reply.
func (c *TCP) Do(args ...string) []byte {
	c.t.Helper()
	c.Send(Encode(args...))
	return c.Read()
}

// Read returns the raw bytes of exactly one reply, of any RESP2 or RESP3
// type, including nested aggregates.
func (c *TCP) Read() []byte {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	var out []byte
	if err := readValue(c.r, &out); err != nil {
		c.t.Fatalf("testutil: read reply (got %q so far): %v", out, err)
	}
	return out
}

// Expect reads exactly len(want) bytes and fails the test unless they
// equal want. It suits pipelined batches whose replies are known in full.
func (c *TCP) Expect(t testing.TB, want []byte) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	got := make([]byte, len(want))
	n, err := io.ReadFull(c.r, got)
	if err != nil {
		t.Fatalf("testutil: read %d bytes: got %d (%s) before %v", len(want), n, shorten(got[:n]), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("testutil: reply mismatch\n got %s\nwant %s", shorten(got), shorten(want))
	}
}

// ExpectClosed fails the test unless the server closes the connection
// (after any bytes still buffered, which it returns).
func (c *TCP) ExpectClosed(t testing.TB) []byte {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(ioTimeout))
	rest, err := io.ReadAll(c.r)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("testutil: connection still open after %s (read %q)", ioTimeout, rest)
	}
	return rest
}

func shorten(b []byte) string {
	if len(b) > 300 {
		return fmt.Sprintf("%q...(%d bytes)", b[:300], len(b))
	}
	return fmt.Sprintf("%q", b)
}

// readValue appends one RESP value from r to out.
func readValue(r *bufio.Reader, out *[]byte) error {
	line, err := r.ReadBytes('\n')
	*out = append(*out, line...)
	if err != nil {
		return err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return fmt.Errorf("malformed line %q", line)
	}
	body := string(line[1 : len(line)-2])
	switch line[0] {
	case '+', '-', ':', ',', '#', '_', '(':
		return nil
	case '$', '=', '!':
		n, err := strconv.Atoi(body)
		if err != nil {
			return fmt.Errorf("bad length in %q", line)
		}
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		*out = append(*out, buf...)
		return nil
	case '*', '~', '>', '%':
		n, err := strconv.Atoi(body)
		if err != nil {
			return fmt.Errorf("bad count in %q", line)
		}
		if line[0] == '%' {
			n *= 2
		}
		for range max(n, 0) {
			if err := readValue(r, out); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unknown type byte in %q", line)
}
