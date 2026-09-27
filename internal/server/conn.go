package server

import (
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
)

// conn is one client connection. Fields under "connection goroutine" are
// touched only by the goroutine running handle; the atomics and the
// mutex-guarded fields are also read by CLIENT LIST from other
// connections and by the WATCH table.
type conn struct {
	srv     *Server
	nc      net.Conn
	id      uint64
	addr    string
	laddr   string
	created time.Time
	r       *resp.Reader
	w       *resp.Writer

	db         atomic.Uint32
	proto      atomic.Int32
	authed     atomic.Bool
	closing    atomic.Bool
	watchDirty atomic.Bool  // set by WatchTable.Touch; EXEC then replies a null array
	multiLen   atomic.Int32 // queued commands, -1 outside MULTI
	lastSpec   atomic.Pointer[command.Spec]
	lastActive atomic.Int64 // Unix nanoseconds

	mu      sync.Mutex
	name    string
	libName string
	libVer  string

	// connection goroutine
	inMulti    bool
	multiDirty bool // a command failed at queue time; EXEC replies EXECABORT
	queue      []queued
	watched    []string // WatchTable keys this connection watches
	noReply    bool     // the current command sends no reply (SHUTDOWN)
}

// queued is one command waiting in a MULTI queue.
type queued struct {
	spec *command.Spec
	args [][]byte
}

var _ command.Conn = (*conn)(nil)

func (s *Server) newConn(nc net.Conn) *conn {
	c := &conn{
		srv:     s,
		nc:      nc,
		id:      s.nextID.Add(1),
		created: time.Now(),
	}
	if a := nc.RemoteAddr(); a != nil {
		c.addr = a.String()
	}
	if a := nc.LocalAddr(); a != nil {
		c.laddr = a.String()
	}
	c.r = resp.NewReader(countReader{nc, &s.stats.netIn}, resp.DefaultLimits())
	c.w = resp.NewWriter(countWriter{nc, &s.stats.netOut})
	c.proto.Store(2)
	c.multiLen.Store(-1)
	c.lastActive.Store(c.created.UnixNano())
	if s.cfg.RequirePass == "" {
		c.SetAuthenticated(true)
	}
	return c
}

// handle runs the request loop of one connection: read a request, run it,
// buffer the reply, and flush once no pipelined input is waiting, so a
// pipelined batch leaves in one write.
func (s *Server) handle(nc net.Conn) {
	c := s.newConn(nc)
	if !s.track(c) {
		nc.Close()
		return
	}
	defer func() {
		s.watch.remove(c)
		s.untrack(c)
		nc.Close()
	}()
	for {
		args, err := c.r.Next()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				c.w.Write(pe.Reply())
				_ = c.w.Flush()
			}
			return
		}
		reply := s.dispatch(c, args)
		if c.noReply {
			c.noReply = false
		} else {
			c.w.Write(reply)
		}
		if c.closing.Load() {
			_ = c.w.Flush()
			return
		}
		if c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
		}
	}
}

func (c *conn) ID() uint64          { return c.id }
func (c *conn) Addr() string        { return c.addr }
func (c *conn) DB() uint8           { return uint8(c.db.Load()) }
func (c *conn) SetDB(db uint8)      { c.db.Store(uint32(db)) }
func (c *conn) Proto() int          { return int(c.proto.Load()) }
func (c *conn) InMulti() bool       { return c.inMulti }
func (c *conn) Close()              { c.closing.Store(true) }
func (c *conn) Unwatch()            { c.srv.watch.remove(c) }
func (c *conn) Authenticated() bool { return c.authed.Load() }

// SetProto switches the reply encoding; the reply of the running command
// already uses the new protocol, as HELLO 3 requires.
func (c *conn) SetProto(v int) {
	if v != 3 {
		v = 2
	}
	c.proto.Store(int32(v))
	c.w.SetProto(v)
}

func (c *conn) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

func (c *conn) SetName(name string) {
	c.mu.Lock()
	c.name = name
	c.mu.Unlock()
}

func (c *conn) LibInfo() (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.libName, c.libVer
}

func (c *conn) SetLibInfo(name, ver string) {
	c.mu.Lock()
	c.libName, c.libVer = name, ver
	c.mu.Unlock()
}

// SetAuthenticated also lifts or restores the reader's pre-AUTH caps.
func (c *conn) SetAuthenticated(ok bool) {
	c.authed.Store(ok)
	c.r.SetAuthenticated(ok)
}

func (c *conn) Watch(db uint8, keys [][]byte, stale []bool) {
	c.srv.watch.add(c, db, keys, stale)
}

// resetMulti leaves MULTI state and drops the queue.
func (c *conn) resetMulti() {
	c.inMulti = false
	c.multiDirty = false
	c.queue = nil
	c.multiLen.Store(-1)
}

func sortConns(cs []*conn) {
	slices.SortFunc(cs, func(a, b *conn) int {
		switch {
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	})
}

// countReader and countWriter feed total_net_input_bytes and
// total_net_output_bytes.
type countReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (c countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(uint64(n))
	return n, err
}

type countWriter struct {
	w io.Writer
	n *atomic.Uint64
}

func (c countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(uint64(n))
	return n, err
}
