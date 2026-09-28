// Package server accepts RESP connections and runs commands from a
// command.Registry against the store. Each connection has one goroutine
// that reads a request, executes it and writes the reply, flushing only
// when no pipelined input is waiting. The executor (exec.go) is the one
// place that takes key locks, begins and commits transactions, takes the
// snapshots of MultiRead commands and touches the WATCH table. The
// package also owns the connection commands (PING, HELLO, AUTH, CLIENT,
// CONFIG, INFO, ...), MULTI/EXEC/WATCH, the SCAN cursor ring and the
// per-command latency histograms behind INFO commandstats.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/store"
)

// shutdownGrace is how long Serve waits for connections to finish their
// current command after shutdown starts before it closes their sockets.
const shutdownGrace = 5 * time.Second

// Server serves one store to RESP clients.
type Server struct {
	cfg     *config.Config
	st      *store.Store
	reg     *command.Registry
	watch   *WatchTable
	cursors *CursorRing
	stats   *stats
	started time.Time
	runID   string

	// immediate holds the specs that run at once inside MULTI instead of
	// being queued: MULTI, EXEC, DISCARD, WATCH, QUIT.
	immediate  map[*command.Spec]bool
	selectSpec *command.Spec

	nextID atomic.Uint64

	mu    sync.Mutex
	ln    net.Listener
	conns map[*conn]struct{}

	wg       sync.WaitGroup
	stop     chan struct{}
	stopOnce sync.Once
}

// New returns a server for st that runs the commands in reg. It registers
// the server's own commands (connection, transaction, CONFIG, INFO,
// COMMAND, and NIL.DEBUG when cfg.EnableDebugCommands is set) into reg, so
// callers must not register those names themselves. Register the other
// command packages into reg before Serve starts accepting.
func New(cfg *config.Config, st *store.Store, reg *command.Registry) *Server {
	var id [20]byte
	_, _ = rand.Read(id[:])
	s := &Server{
		cfg:       cfg,
		st:        st,
		reg:       reg,
		watch:     newWatchTable(),
		cursors:   NewCursorRing(),
		stats:     newStats(),
		started:   time.Now(),
		runID:     hex.EncodeToString(id[:]),
		immediate: make(map[*command.Spec]bool),
		conns:     make(map[*conn]struct{}),
		stop:      make(chan struct{}),
	}
	s.registerCommands()
	for _, name := range []string{"multi", "exec", "discard", "watch", "quit"} {
		spec, _ := reg.Lookup([]byte(name))
		s.immediate[spec] = true
	}
	s.selectSpec, _ = reg.Lookup([]byte("select"))
	return s
}

func (s *Server) registerCommands() {
	s.reg.Register(s.connectionSpecs()...)
	s.reg.Register(s.transactionSpecs()...)
	s.reg.Register(s.infoSpec())
	if s.cfg.EnableDebugCommands {
		s.reg.Register(s.debugSpec())
	}
}

// Registry returns the command registry the server runs.
func (s *Server) Registry() *command.Registry { return s.reg }

// Cursors returns the SCAN cursor ring.
func (s *Server) Cursors() *CursorRing { return s.cursors }

// Watch returns the WATCH table.
func (s *Server) Watch() *WatchTable { return s.watch }

// RegisterInfoSection adds an INFO section; it forwards to the registry,
// see command.Registry.RegisterInfoSection.
func (s *Server) RegisterInfoSection(name string, fn func(b *strings.Builder)) {
	s.reg.RegisterInfoSection(name, fn)
}

var _ command.InfoRegistrar = (*Server)(nil)

// Listen opens the listener on cfg.Addr and returns its address, which
// carries the real port when cfg.Addr asks for port 0. Serve calls it when
// it has not been called yet.
func (s *Server) Listen() (net.Addr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Addr(), nil
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, err
	}
	s.ln = ln
	return ln.Addr(), nil
}

// Addr returns the listener's address, or nil before Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections until ctx is done, Shutdown is called or
// SHUTDOWN runs. It then stops accepting, lets every connection finish its
// current command (closing sockets that take longer than five seconds),
// waits for all connection goroutines and returns. It does not close the
// store; the caller does that after Serve returns. The error is nil after
// a requested shutdown.
func (s *Server) Serve(ctx context.Context) error {
	if _, err := s.Listen(); err != nil {
		return err
	}
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		s.Shutdown()
		ln.Close()
	}()
	go s.stats.sampleOps(s.stop)

	var err error
	for {
		nc, aerr := ln.Accept()
		if aerr != nil {
			if s.stopping() {
				break
			}
			var ne net.Error
			if errors.As(aerr, &ne) && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			err = aerr
			s.Shutdown()
			ln.Close()
			break
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(nc)
		}()
	}
	s.drainConns()
	s.wg.Wait()
	return err
}

// ServeConn runs the connection loop on nc and returns when the client
// disconnects or the server shuts down. Tests use it with net.Pipe.
func (s *Server) ServeConn(nc net.Conn) {
	s.wg.Add(1)
	defer s.wg.Done()
	s.handle(nc)
}

// Shutdown makes Serve return after in-flight commands finish. It is
// idempotent.
func (s *Server) Shutdown() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// Done is closed once shutdown has started (Shutdown, SHUTDOWN or the
// Serve context).
func (s *Server) Done() <-chan struct{} { return s.stop }

func (s *Server) stopping() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// track adds c to the connection table; it refuses once shutdown began.
func (s *Server) track(c *conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping() {
		return false
	}
	s.conns[c] = struct{}{}
	s.stats.connsTotal.Add(1)
	return true
}

func (s *Server) untrack(c *conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// connList returns the tracked connections ordered by id.
func (s *Server) connList() []*conn {
	s.mu.Lock()
	out := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		out = append(out, c)
	}
	s.mu.Unlock()
	sortConns(out)
	return out
}

// drainConns wakes every connection blocked in a read so it exits after
// its current command, and closes the sockets of those still busy after
// shutdownGrace. Serve calls it once its accept loop has ended, so no
// connection goroutine starts while it waits.
func (s *Server) drainConns() {
	for _, c := range s.connList() {
		_ = c.nc.SetReadDeadline(time.Now())
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownGrace):
		for _, c := range s.connList() {
			c.nc.Close()
		}
	}
}
