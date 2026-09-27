package server

import (
	"sync"
	"sync/atomic"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
)

// WatchTable maps (db, key) to the connections watching it, with each
// watcher's stale bit: whether the key was missing or logically expired
// when that connection ran WATCH. A write that commits touches its keys,
// which sets the dirty flag of every watcher; EXEC on a dirty connection
// replies a null array. When nothing is watched, Touch costs one atomic
// load.
type WatchTable struct {
	n  atomic.Int64 // (key, connection) pairs
	mu sync.Mutex
	m  map[string]map[*conn]bool // watchKey -> watcher -> stale
}

var _ command.Watcher = (*WatchTable)(nil)

func newWatchTable() *WatchTable {
	return &WatchTable{m: make(map[string]map[*conn]bool)}
}

// watchKey is the table key: the db byte followed by the user key.
func watchKey(db uint8, key []byte) string {
	b := make([]byte, 1+len(key))
	b[0] = db
	copy(b[1:], key)
	return string(b)
}

// Len returns the number of (key, connection) pairs watched.
func (t *WatchTable) Len() int { return int(t.n.Load()) }

// Touch marks every connection watching one of keys in db as dirty.
func (t *WatchTable) Touch(db uint8, keys ...[]byte) {
	if t.n.Load() == 0 {
		return
	}
	var buf []byte
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range keys {
		buf = append(append(buf[:0], db), k...)
		for c := range t.m[string(buf)] {
			c.watchDirty.Store(true)
		}
	}
}

// TouchExisting marks dirty the non-stale watchers of every watched key
// of db for which exists returns true. A watcher that saw the key missing
// or expired at WATCH time is left alone: deleting such a key changes
// nothing it could observe, and EXEC skips the expiry re-check for it.
// exists runs without the table lock held.
func (t *WatchTable) TouchExisting(db uint8, exists func(key []byte) bool) {
	if t.n.Load() == 0 {
		return
	}
	t.mu.Lock()
	var cands []string
	for k := range t.m {
		if k[0] == db {
			cands = append(cands, k)
		}
	}
	t.mu.Unlock()
	for _, k := range cands {
		if !exists([]byte(k[1:])) {
			continue
		}
		t.mu.Lock()
		for c, stale := range t.m[k] {
			if !stale {
				c.watchDirty.Store(true)
			}
		}
		t.mu.Unlock()
	}
}

// add records that c watches keys of db. Keys c already watches keep
// their original stale bit, as in Redis.
func (t *WatchTable) add(c *conn, db uint8, keys [][]byte, stale []bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, k := range keys {
		wk := watchKey(db, k)
		ws := t.m[wk]
		if ws == nil {
			ws = make(map[*conn]bool)
			t.m[wk] = ws
		}
		if _, dup := ws[c]; dup {
			continue
		}
		ws[c] = i < len(stale) && stale[i]
		c.watched = append(c.watched, wk)
		t.n.Add(1)
	}
}

// remove forgets every key c watches and clears its dirty flag. EXEC,
// DISCARD, UNWATCH and disconnect call it.
func (t *WatchTable) remove(c *conn) {
	if len(c.watched) > 0 {
		t.mu.Lock()
		for _, wk := range c.watched {
			ws := t.m[wk]
			if _, ok := ws[c]; !ok {
				continue
			}
			delete(ws, c)
			if len(ws) == 0 {
				delete(t.m, wk)
			}
			t.n.Add(-1)
		}
		t.mu.Unlock()
		c.watched = nil
	}
	c.watchDirty.Store(false)
}

// watchEntry is one key a connection watches.
type watchEntry struct {
	db    uint8
	key   []byte
	stale bool
}

// entries returns the keys c watches with their stale bits.
func (t *WatchTable) entries(c *conn) []watchEntry {
	if len(c.watched) == 0 {
		return nil
	}
	out := make([]watchEntry, 0, len(c.watched))
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, wk := range c.watched {
		stale := t.m[wk][c]
		out = append(out, watchEntry{db: wk[0], key: []byte(wk[1:]), stale: stale})
	}
	return out
}

// cmdWatch is WATCH key [key ...]. The executor holds the keys' locks
// while it runs, so no write can commit between the liveness read and the
// registration.
func (s *Server) cmdWatch(ctx *command.Ctx, args [][]byte) resp.Reply {
	c := ctx.Conn.(*conn)
	if c.inMulti {
		return resp.ErrWatchInMulti
	}
	keys := args[1:]
	stale := make([]bool, len(keys))
	nowMS := ctx.NowMS()
	var mk []byte
	for i, k := range keys {
		mk = layout.MetaKey(mk[:0], ctx.DB, k)
		live, err := liveMeta(s.st, mk, nowMS)
		if err != nil {
			return resp.Err("ERR " + err.Error())
		}
		stale[i] = !live
	}
	c.Watch(ctx.DB, keys, stale)
	return resp.OK()
}

// cmdUnwatch is UNWATCH.
func (s *Server) cmdUnwatch(ctx *command.Ctx, _ [][]byte) resp.Reply {
	ctx.Conn.Unwatch()
	return resp.OK()
}
