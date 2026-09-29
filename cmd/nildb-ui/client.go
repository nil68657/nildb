package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// clientConfig is how the console reaches NilDB.
type clientConfig struct {
	addr     string
	password string
	timeout  time.Duration // one command, or one pipeline, end to end
	maxReply int64         // decoded bytes one reply may hold
}

const (
	dialTimeout = 3 * time.Second
	// idleMax closes pooled connections nobody used for this long, so a
	// NilDB restart costs at most one failed attempt per connection.
	idleMax = 2 * time.Minute
)

// errTimeout is returned when NilDB does not answer within the timeout.
var errTimeout = errors.New("NilDB did not reply in time")

// helloInfo is what HELLO replied when the connection opened.
type helloInfo struct {
	Server  string `json:"server"`
	Version string `json:"version"`
	Proto   int    `json:"proto"`
	ID      int64  `json:"id"`
	Mode    string `json:"mode"`
	Role    string `json:"role"`
}

// conn is one connection to NilDB.
type conn struct {
	nc     net.Conn
	dec    *Decoder
	out    []byte
	cfg    *clientConfig
	hello  helloInfo
	db     int  // selected database, -1 when unknown
	proto  int  // 2 or 3
	multi  bool // inside MULTI (console sessions only)
	closed bool // QUIT ran or the connection failed
	used   time.Time
}

// dial opens a connection and runs HELLO 3 with AUTH when a password is
// set. name is sent with SETNAME, so CLIENT LIST shows the console.
func dial(ctx context.Context, cfg *clientConfig, name string) (*conn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", cfg.addr)
	if err != nil {
		return nil, err
	}
	c := &conn{nc: nc, dec: NewDecoder(bufio.NewReaderSize(nc, 64<<10)), cfg: cfg, proto: 2, used: time.Now()}
	hello := [][]byte{[]byte("HELLO"), []byte("3")}
	if cfg.password != "" {
		hello = append(hello, []byte("AUTH"), []byte("default"), []byte(cfg.password))
	}
	hello = append(hello, []byte("SETNAME"), []byte(name))
	vals, err := c.roundTrip(ctx, [][][]byte{hello})
	if err != nil {
		nc.Close()
		return nil, err
	}
	v := vals[0]
	if v.IsErr() {
		nc.Close()
		msg := v.Text()
		if strings.HasPrefix(msg, "NOAUTH") || strings.HasPrefix(msg, "WRONGPASS") {
			return nil, fmt.Errorf("%s (start nildb-ui with --password or NILDB_PASSWORD)", msg)
		}
		return nil, errors.New(msg)
	}
	c.proto = 3
	c.hello = parseHello(v)
	return c, nil
}

func parseHello(v Value) helloInfo {
	str := func(k string) string {
		x, _ := v.Lookup(k)
		return x.Text()
	}
	h := helloInfo{Server: str("server"), Version: str("version"), Mode: str("mode"), Role: str("role")}
	if x, ok := v.Lookup("proto"); ok {
		h.Proto = int(x.Int)
	}
	if x, ok := v.Lookup("id"); ok {
		h.ID = x.Int
	}
	return h
}

// roundTrip writes cmds in one write and reads one reply per command.
// RESP3 push messages that arrive in between are dropped: NilDB sends
// none, and a console request has nobody to deliver them to. Any error
// leaves the connection unusable.
func (c *conn) roundTrip(ctx context.Context, cmds [][][]byte) ([]Value, error) {
	deadline := time.Now().Add(c.cfg.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.nc.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	c.out = c.out[:0]
	for _, args := range cmds {
		c.out = AppendCommand(c.out, args)
	}
	_, err := c.nc.Write(c.out)
	if cap(c.out) > 1<<20 {
		c.out = nil
	}
	if err != nil {
		return nil, c.ioErr(ctx, err)
	}
	vals := make([]Value, 0, len(cmds))
	for len(vals) < len(cmds) {
		v, err := c.dec.Read(c.cfg.maxReply)
		if err != nil {
			return nil, c.ioErr(ctx, err)
		}
		if v.Kind == KindPush {
			continue
		}
		vals = append(vals, v)
	}
	c.used = time.Now()
	return vals, nil
}

func (c *conn) ioErr(ctx context.Context, err error) error {
	c.closed = true
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return errTimeout
	}
	return err
}

// selectCmd is SELECT db.
func selectCmd(db int) [][]byte {
	return [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))}
}

// run sends cmds on database db, prefixing SELECT when the connection is
// on another one.
func (c *conn) run(ctx context.Context, db int, cmds [][][]byte) ([]Value, error) {
	if db == c.db {
		return c.roundTrip(ctx, cmds)
	}
	vals, err := c.roundTrip(ctx, append([][][]byte{selectCmd(db)}, cmds...))
	if err != nil {
		return nil, err
	}
	if !vals[0].IsOK() {
		return nil, &selectError{db: db, reply: vals[0].Text()}
	}
	c.db = db
	return vals[1:], nil
}

// selectError is a SELECT the server refused; the connection is fine.
type selectError struct {
	db    int
	reply string
}

func (e *selectError) Error() string { return fmt.Sprintf("SELECT %d: %s", e.db, e.reply) }

func (c *conn) close() {
	c.closed = true
	c.nc.Close()
}

// pool shares connections between the console's views. Every request
// names its database; a connection is switched with SELECT when needed.
// Connection state commands (MULTI, WATCH, HELLO, AUTH) never run on a
// pooled connection; the console sessions below take those.
type pool struct {
	cfg *clientConfig
	sem chan struct{}

	mu   sync.Mutex
	idle []*conn

	// onDial runs after every new connection, outside any lock.
	onDial func(*conn)
}

func newPool(cfg *clientConfig, size int) *pool {
	return &pool{cfg: cfg, sem: make(chan struct{}, size)}
}

// get returns an idle connection or dials one; reused reports which.
func (p *pool) get(ctx context.Context) (c *conn, reused bool, err error) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	p.mu.Lock()
	for len(p.idle) > 0 {
		c = p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if time.Since(c.used) < idleMax {
			p.mu.Unlock()
			return c, true, nil
		}
		c.close()
	}
	p.mu.Unlock()
	c, err = dial(ctx, p.cfg, "nildb-ui")
	if err != nil {
		<-p.sem
		return nil, false, err
	}
	if p.onDial != nil {
		p.onDial(c)
	}
	return c, false, nil
}

// put returns c to the pool, or closes it when it failed.
func (p *pool) put(c *conn) {
	if c.closed {
		c.close()
	} else {
		p.mu.Lock()
		p.idle = append(p.idle, c)
		p.mu.Unlock()
	}
	<-p.sem
}

// do runs cmds on database db on one connection. When a connection taken
// from the idle list fails before replying, which is what a NilDB restart
// looks like, and retry is true, the commands run once more on a new
// connection. Callers pass retry only for commands that do not write.
func (p *pool) do(ctx context.Context, db int, cmds [][][]byte, retry bool) ([]Value, error) {
	for attempt := 0; ; attempt++ {
		c, reused, err := p.get(ctx)
		if err != nil {
			return nil, err
		}
		vals, err := c.run(ctx, db, cmds)
		failed := c.closed
		p.put(c)
		if err == nil || !failed || !reused || !retry || attempt > 0 || ctx.Err() != nil || errors.Is(err, errTimeout) {
			return vals, err
		}
	}
}

// close closes the idle connections.
func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idle {
		c.close()
	}
	p.idle = nil
}

// Console sessions. Each Console tab owns one connection, so MULTI,
// WATCH, SELECT, HELLO and AUTH behave as they do in redis-cli.
const (
	maxSessions = 16
	sessionIdle = 15 * time.Minute
)

type session struct {
	mu   sync.Mutex // one command at a time
	c    *conn      // nil until the first command and after a failure
	used time.Time
}

// sessionState is what the Console shows in its prompt.
type sessionState struct {
	DB    int  `json:"db"`
	Multi bool `json:"multi"`
	Proto int  `json:"proto"`
}

type sessions struct {
	cfg *clientConfig
	mu  sync.Mutex
	m   map[string]*session
}

func newSessions(cfg *clientConfig) *sessions {
	return &sessions{cfg: cfg, m: make(map[string]*session)}
}

var errTooManySessions = fmt.Errorf("more than %d Console tabs are open; close one", maxSessions)

// validSessionID accepts what crypto.randomUUID() produces, and similar.
func validSessionID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

func (s *sessions) get(id string) (*session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ss := s.m[id]; ss != nil {
		return ss, nil
	}
	s.reapLocked()
	if len(s.m) >= maxSessions {
		return nil, errTooManySessions
	}
	ss := &session{used: time.Now()}
	s.m[id] = ss
	return ss, nil
}

// reapLocked closes sessions idle for sessionIdle. A session busy with a
// command holds its own lock and is skipped.
func (s *sessions) reapLocked() {
	for id, ss := range s.m {
		if !ss.mu.TryLock() {
			continue
		}
		if time.Since(ss.used) > sessionIdle {
			if ss.c != nil {
				ss.c.close()
			}
			delete(s.m, id)
		}
		ss.mu.Unlock()
	}
}

// end closes one session.
func (s *sessions) end(id string) {
	s.mu.Lock()
	ss := s.m[id]
	delete(s.m, id)
	s.mu.Unlock()
	if ss == nil {
		return
	}
	ss.mu.Lock()
	if ss.c != nil {
		ss.c.close()
		ss.c = nil
	}
	ss.mu.Unlock()
}

func (s *sessions) closeAll() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.m))
	for id := range s.m {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.end(id)
	}
}

// do runs one command on the session's connection. db is the database the
// Console shows; outside MULTI the connection is moved there first. The
// returned state reflects SELECT, MULTI, EXEC, DISCARD and HELLO.
func (s *sessions) do(ctx context.Context, id string, db int, args [][]byte) (Value, sessionState, error) {
	ss, err := s.get(id)
	if err != nil {
		return Value{}, sessionState{}, err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.used = time.Now()
	fresh := sessionState{DB: db, Proto: 3}
	if ss.c == nil {
		if ss.c, err = dial(ctx, s.cfg, "nildb-ui-console"); err != nil {
			return Value{}, fresh, err
		}
	}
	c := ss.c
	cmds := [][][]byte{args}
	selecting := !c.multi && c.db != db
	if selecting {
		cmds = append([][][]byte{selectCmd(db)}, cmds...)
	}
	vals, err := c.roundTrip(ctx, cmds)
	if err != nil {
		c.close()
		ss.c = nil
		return Value{}, fresh, err
	}
	if selecting {
		if !vals[0].IsOK() {
			return vals[0], c.state(), nil
		}
		c.db = db
		vals = vals[1:]
	}
	v := vals[0]
	c.track(args, v)
	st := c.state()
	if c.closed {
		c.close()
		ss.c = nil
	}
	return v, st, nil
}

func (c *conn) state() sessionState { return sessionState{DB: c.db, Multi: c.multi, Proto: c.proto} }

// track updates the connection state a command's reply implies.
func (c *conn) track(args [][]byte, v Value) {
	name := strings.ToLower(string(args[0]))
	switch {
	case v.IsErr() && name != "exec":
		return
	case name == "select" && len(args) == 2 && !c.multi:
		if n, err := strconv.Atoi(string(args[1])); err == nil {
			c.db = n
		}
	case name == "select":
		// Queued inside MULTI: EXEC applies it, so the database is
		// unknown until the next request selects one again.
		c.db = -1
	case name == "multi":
		c.multi = true
	case name == "exec" || name == "discard":
		c.multi = false
	case name == "hello":
		if p, ok := v.Lookup("proto"); ok && (p.Int == 2 || p.Int == 3) {
			c.proto = int(p.Int)
		}
	case name == "quit":
		c.closed = true
	}
}
