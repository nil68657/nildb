package server

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/store"
)

// newTestServer opens a store in a temporary directory and builds a server
// with the fake string commands registered. It does not listen; tests
// drive it through pipe().
func newTestServer(t *testing.T, mutate func(*config.Config)) *Server {
	t.Helper()
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	cfg := config.Default()
	cfg.Dir = t.TempDir()
	cfg.Addr = "127.0.0.1:0"
	cfg.Clock = &config.OffsetClock{}
	cfg.EnableDebugCommands = true
	cfg.BlockCacheMB = 8
	cfg.AnalyticsCacheMB = 8
	cfg.WriteBufferMB = 32
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(store.Config{
		Dir: cfg.Dir, Engine: store.TestEngine(), BlockCacheBytes: cfg.BlockCacheBytes(), AnalyticsCacheBytes: cfg.AnalyticsCacheBytes(),
		WriteBufferBytes: cfg.WriteBufferBytes(), Fsync: cfg.Fsync, MaxSnapshots: cfg.MaxSnapshots,
		LeaseTTL: cfg.LeaseTTL, LeaseMax: cfg.LeaseMax, Clock: cfg.ClockFunc(),
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := command.NewRegistry()
	reg.Register(FakeStringCommands()...)
	s := New(&cfg, st, reg)
	t.Cleanup(func() {
		s.Shutdown()
		s.wg.Wait()
		st.Close()
	})
	return s
}

// countConn counts the Write calls the server makes.
type countConn struct {
	net.Conn
	writes atomic.Int64
}

func (c *countConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

// pipeClient is the client end of a net.Pipe served by ServeConn.
type pipeClient struct {
	t      *testing.T
	nc     net.Conn
	r      *bufio.Reader
	server *countConn
	done   chan struct{}
}

func (s *Server) pipe(t *testing.T) *pipeClient {
	t.Helper()
	cli, srv := net.Pipe()
	cc := &countConn{Conn: srv}
	p := &pipeClient{t: t, nc: cli, r: bufio.NewReader(cli), server: cc, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		s.ServeConn(cc)
	}()
	t.Cleanup(func() {
		cli.Close()
		<-p.done
	})
	return p
}

func encodeArgs(args ...string) []byte {
	var b bytes.Buffer
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	return b.Bytes()
}

// send writes raw bytes. net.Pipe blocks a write until the server reads
// it, so the write runs on its own goroutine.
func (p *pipeClient) send(b []byte) {
	go func() { _, _ = p.nc.Write(b) }()
}

// expect reads exactly len(want) bytes and compares them.
func (p *pipeClient) expect(want string) {
	p.t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	got := make([]byte, len(want))
	if n, err := io.ReadFull(p.r, got); err != nil {
		p.t.Fatalf("read %d bytes: got %q, %v", len(want), got[:n], err)
	}
	if string(got) != want {
		p.t.Fatalf("reply\n got %q\nwant %q", got, want)
	}
}

// do sends one multibulk request and checks the reply bytes.
func (p *pipeClient) do(want string, args ...string) {
	p.t.Helper()
	p.send(encodeArgs(args...))
	p.expect(want)
}

// expectClosed checks that the server closed the pipe.
func (p *pipeClient) expectClosed() {
	p.t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	if b, err := p.r.ReadByte(); err == nil {
		p.t.Fatalf("connection still open, read %q", b)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.t.Fatal("server goroutine did not exit")
	}
}
