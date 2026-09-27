package command

import (
	"strings"
	"time"

	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/store"
)

// Ctx is what a handler receives. The executor builds one per command.
type Ctx struct {
	Conn  Conn
	Store *store.Store
	// Txn is store.Begin() for an autocommit command and the EXEC-wide
	// store.BeginIndexed() inside EXEC. The executor commits it once after
	// the handler returns when Len() > 0; handlers never call Commit.
	Txn store.Txn
	// Snap is the command-scoped snapshot of a MultiRead command, nil
	// otherwise (and always nil inside EXEC).
	Snap *store.Snapshot
	// Now is the clock reading for the whole command.
	Now time.Time
	// DB is the database the command runs in.
	DB     uint8
	Parsed any // what the KeysFunc returned
	Cfg    *config.Config
	// Spec is the running command (the subcommand for a container).
	Spec *Spec
	// Cursors is the SCAN cursor ring.
	Cursors Cursors
	// Watch is the WATCH table, for FLUSHDB and FLUSHALL.
	Watch Watcher
	// InExec is true while the command runs as part of EXEC.
	InExec bool

	reader  store.Reader
	held    []store.LockKey
	phases  []*lockPhase
	touched []TouchKey
	touch   bool
}

// Reader returns what the handler reads through: the EXEC transaction
// inside EXEC (read-your-writes), the snapshot when Snap is set, otherwise
// the live store.
func (c *Ctx) Reader() store.Reader {
	if c.reader != nil {
		return c.reader
	}
	switch {
	case c.InExec && c.Txn != nil:
		c.reader = c.Txn
	case c.Snap != nil && c.Store != nil:
		c.reader = c.Store.At(c.Snap)
	case c.Store != nil:
		c.reader = c.Store
	default:
		c.reader = c.Txn
	}
	return c.reader
}

// NowMS is Now in Unix milliseconds, the unit of Redis expiry times.
func (c *Ctx) NowMS() int64 { return c.Now.UnixMilli() }

// TouchKey names one key the executor touches in the WATCH table.
type TouchKey struct {
	DB  uint8
	Key []byte
}

// Touch records keys of c.DB to touch in the WATCH table once the command
// has committed. By default the executor touches every LockRedis key of a
// Write command whose commit wrote something; once a handler records any
// key with Touch or TouchDB, the executor touches exactly the recorded
// keys instead, even when nothing was written through Txn. Use it when
// the keys a command changed differ from its lock set, for example DEL
// k1 k2 where only k1 existed. The keys are copied.
func (c *Ctx) Touch(keys ...[]byte) { c.TouchDB(c.DB, keys...) }

// TouchDB is Touch for keys of another database (MOVE's target).
func (c *Ctx) TouchDB(db uint8, keys ...[]byte) {
	c.touch = true
	for _, k := range keys {
		c.touched = append(c.touched, TouchKey{DB: db, Key: append([]byte(nil), k...)})
	}
}

// TouchedKeys returns the keys recorded with Touch and TouchDB, and
// whether the handler recorded any (it may have recorded an empty set on
// purpose by calling Touch with no keys). The executor calls it.
func (c *Ctx) TouchedKeys() ([]TouchKey, bool) { return c.touched, c.touch }

// Lock takes a second-phase set of locks, for DOC.* commands that learn
// their unique-value stripes only after reading the old document. Every
// key must be of a strictly higher kind than the locks the command holds
// at that moment: the executor's set plus earlier Lock sets not yet
// released (store.LockAfter panics otherwise). The locks are released when
// the returned function is called or, at the latest, when the command
// ends.
//
// Inside EXEC, Lock takes nothing and returns a no-op: EXEC holds every
// queued collection lock exclusive, which already shuts out every other
// writer of the collection, so a DOC write KeysFunc must include its
// LockColl key.
func (c *Ctx) Lock(keys []store.LockKey) func() {
	if c.InExec || len(keys) == 0 || c.Store == nil {
		return func() {}
	}
	held := c.held
	for _, p := range c.phases {
		if !p.released {
			held = append(held[:len(held):len(held)], p.keys...)
		}
	}
	u := c.Store.LockAfter(held, keys)
	p := &lockPhase{keys: keys, unlock: u}
	c.phases = append(c.phases, p)
	return p.release
}

// lockPhase is one set of locks taken with Ctx.Lock.
type lockPhase struct {
	keys     []store.LockKey
	unlock   func()
	released bool
}

func (p *lockPhase) release() {
	if !p.released {
		p.released = true
		p.unlock()
	}
}

// SetHeld tells Lock which locks the executor took for the command. The
// executor calls it.
func (c *Ctx) SetHeld(keys []store.LockKey) { c.held = keys[:len(keys):len(keys)] }

// ReleaseLocks releases every lock taken with Lock that the handler did
// not release itself. The executor calls it when the command ends.
func (c *Ctx) ReleaseLocks() {
	for i := len(c.phases) - 1; i >= 0; i-- {
		c.phases[i].release()
	}
	c.phases = nil
}

// CursorKind separates the four SCAN families in the cursor ring.
type CursorKind uint8

// Cursor kinds. The zero value is not a valid kind.
const (
	CursorScan CursorKind = 1 + iota
	CursorHScan
	CursorSScan
	CursorZScan
)

// Position is where a SCAN-family iteration stopped.
type Position struct {
	DB      uint8
	Kind    CursorKind
	Version uint64 // collection version for HSCAN/SSCAN/ZSCAN; 0 for SCAN
	Key     []byte // SCAN: last returned user key; HSCAN/SSCAN/ZSCAN: the collection's key
	Sub     []byte // HSCAN/SSCAN/ZSCAN: last returned field or member
}

// Cursors is the SCAN cursor ring (server.CursorRing implements it).
type Cursors interface {
	// Put stores p and returns a non-zero cursor for it. The ring copies
	// p.Key and p.Sub.
	Put(p Position) uint64
	// Get returns the position of a cursor Put returned, as a copy. It
	// reports false for 0, an evicted cursor, a cursor from another
	// process, and a cursor issued for another db or kind; SCAN then
	// replies resp.ErrInvalidCursor.
	Get(cursor uint64, db uint8, kind CursorKind) (Position, bool)
}

// Watcher is the WATCH table (server.WatchTable implements it). The
// executor touches the keys of ordinary writes itself; FLUSHDB and
// FLUSHALL use TouchExisting.
type Watcher interface {
	// Touch marks every connection watching one of keys in db as dirty,
	// so its next EXEC replies a null array.
	Touch(db uint8, keys ...[]byte)
	// TouchExisting marks dirty the watchers of every watched key of db
	// for which exists returns true, except watchers that saw the key
	// already missing or expired at WATCH time. FLUSHDB calls it for its
	// db (FLUSHALL once per db) before deleting anything, with exists
	// reporting whether the key has a meta entry. It does not hold the
	// table lock while exists runs.
	TouchExisting(db uint8, exists func(key []byte) bool)
}

// InfoRegistrar adds a section to INFO. *Registry and *server.Server
// implement it; see Registry.RegisterInfoSection for the rules.
type InfoRegistrar interface {
	RegisterInfoSection(name string, fn func(b *strings.Builder))
}

var _ InfoRegistrar = (*Registry)(nil)
