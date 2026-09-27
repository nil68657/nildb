package command

import "sync"

// Conn is the client connection a command runs for. The server's
// connection type implements it; LocalConn is a detached implementation
// for tests that build a Ctx by hand.
type Conn interface {
	ID() uint64
	Addr() string // remote address, "127.0.0.1:52341"
	DB() uint8
	SetDB(db uint8)
	Proto() int // 2 or 3
	SetProto(v int)
	Name() string
	SetName(name string)
	LibInfo() (name, ver string)
	SetLibInfo(name, ver string)
	Authenticated() bool
	SetAuthenticated(ok bool)
	InMulti() bool
	// Watch adds keys of db to the connection's watched set. stale[i]
	// records whether keys[i] was missing or logically expired when WATCH
	// ran; EXEC skips its expiry re-check for such keys.
	Watch(db uint8, keys [][]byte, stale []bool)
	// Unwatch forgets every watched key and clears the dirty flag.
	Unwatch()
	// Close closes the connection once the current reply is written.
	Close()
}

// LocalConn is a Conn with no network behind it. It keeps its state in
// memory and is safe for concurrent use. Tests use it to build a Ctx
// without a server.
type LocalConn struct {
	mu      sync.Mutex
	id      uint64
	db      uint8
	proto   int
	name    string
	libName string
	libVer  string
	authed  bool
	closed  bool
	watched []WatchedKey
}

// WatchedKey is one entry LocalConn.Watched reports.
type WatchedKey struct {
	DB    uint8
	Key   []byte
	Stale bool
}

// NewLocalConn returns an authenticated RESP2 LocalConn on db 0.
func NewLocalConn(id uint64) *LocalConn {
	return &LocalConn{id: id, proto: 2, authed: true}
}

var _ Conn = (*LocalConn)(nil)

func (c *LocalConn) ID() uint64   { return c.id }
func (c *LocalConn) Addr() string { return "local" }

func (c *LocalConn) DB() uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db
}

func (c *LocalConn) SetDB(db uint8) {
	c.mu.Lock()
	c.db = db
	c.mu.Unlock()
}

func (c *LocalConn) Proto() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proto
}

func (c *LocalConn) SetProto(v int) {
	c.mu.Lock()
	c.proto = v
	c.mu.Unlock()
}

func (c *LocalConn) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

func (c *LocalConn) SetName(name string) {
	c.mu.Lock()
	c.name = name
	c.mu.Unlock()
}

func (c *LocalConn) LibInfo() (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.libName, c.libVer
}

func (c *LocalConn) SetLibInfo(name, ver string) {
	c.mu.Lock()
	c.libName, c.libVer = name, ver
	c.mu.Unlock()
}

func (c *LocalConn) Authenticated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authed
}

func (c *LocalConn) SetAuthenticated(ok bool) {
	c.mu.Lock()
	c.authed = ok
	c.mu.Unlock()
}

// InMulti is always false: a LocalConn never queues.
func (c *LocalConn) InMulti() bool { return false }

func (c *LocalConn) Watch(db uint8, keys [][]byte, stale []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, k := range keys {
		c.watched = append(c.watched, WatchedKey{DB: db, Key: append([]byte(nil), k...), Stale: i < len(stale) && stale[i]})
	}
}

func (c *LocalConn) Unwatch() {
	c.mu.Lock()
	c.watched = nil
	c.mu.Unlock()
}

// Watched returns the keys recorded by Watch.
func (c *LocalConn) Watched() []WatchedKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]WatchedKey(nil), c.watched...)
}

func (c *LocalConn) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// Closed reports whether Close was called.
func (c *LocalConn) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
