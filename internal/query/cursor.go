package query

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Cursor errors.
var (
	errCursorsClosed = &doc.Error{Code: 91, Msg: "server is shutting down"}
	// ErrTooManySnapshots is store's lease cap as the client sees it.
	ErrTooManySnapshots = &doc.Error{Code: doc.CodeBadValue, Msg: "too many open snapshots"}
)

func errCursorNotFound(id uint64) error {
	return &doc.Error{Code: 43, Msg: fmt.Sprintf("cursor id %d not found", id)}
}

// CursorReply is the reply of every command that returns a cursor batch
// (DOC.FIND, DOC.AGGREGATE, NIL.AGGREGATE, DOC.CURSOR READ): RESP3
// {cursor: {id, ns, <batch>: [...]}}, RESP2 [id, ns, [...]]. batch is
// "firstBatch" or "nextBatch"; id 0 means the cursor is exhausted.
// Documents are canonical Extended JSON bulk strings, or raw BSON.
func CursorReply(resp3 bool, id uint64, ns, batch string, docs []bson.Raw, raw bool) resp.Reply {
	items := make([]resp.Reply, len(docs))
	for i, d := range docs {
		if raw {
			items[i] = resp.Bulk(d)
		} else {
			items[i] = resp.Bulk(doc.FormatEJSON(d))
		}
	}
	if resp3 {
		return resp.Map(resp.Str("cursor"), resp.Map(
			resp.Str("id"), resp.Int(int64(id)),
			resp.Str("ns"), resp.Str(ns),
			resp.Str(batch), resp.Array(items...)))
	}
	return resp.Array(resp.Int(int64(id)), resp.Str(ns), resp.Array(items...))
}

// Cursors is the table of open cursors: runs that returned a first batch
// and have more to give. Every cursor is pinned to a store lease, which
// counts against --max-snapshots with every other lease, and holds no
// RocksDB iterator between reads (its run is suspended). A cursor idle for
// longer than the idle timeout (nildb.lease-ttl) is closed and its lease
// released on the next table operation.
type Cursors struct {
	st   *store.Store
	idle func() time.Duration
	now  func() time.Time

	mu     sync.Mutex
	m      map[uint64]*Cursor
	closed bool
}

func newCursors(st *store.Store, idle func() time.Duration) *Cursors {
	return &Cursors{st: st, idle: idle, now: time.Now, m: make(map[uint64]*Cursor)}
}

// Cursor is one open cursor.
type Cursor struct {
	ID      uint64
	NS      string
	Kind    string // "find", "aggregate" or "nil.aggregate"
	Batch   int    // documents per read when READ gives no COUNT
	Created time.Time
	// RawBSON records FORMAT BSON: reads reply raw BSON documents rather
	// than Extended JSON. DOC.CURSOR serves DOC.* and NIL.* cursors alike.
	RawBSON bool

	run   *Run
	lease *store.Snapshot
	own   bool // release lease when the cursor ends
	renew bool // take a fresh lease when the old one expired
	gate  func() (release func(), err error)
	last  time.Time
	busy  bool
}

// CursorOptions describe a cursor for Open.
type CursorOptions struct {
	NS    string
	Kind  string
	Batch int
	// Lease is the lease the run already reads through (NIL.AGGREGATE's,
	// or an AT lease). When nil, Open takes a new lease, points the run
	// at it, and renews it if it expires while the cursor is still read:
	// an OLTP cursor sees each batch at one sequence number.
	Lease *store.Snapshot
	// Own releases Lease when the cursor ends; an AT lease belongs to the
	// client and stays.
	Own bool
	// Gate runs before each read and returns the function that ends it:
	// NIL cursors take an analytics semaphore slot through it.
	Gate    func() (release func(), err error)
	RawBSON bool
}

// CursorInfo is one DOC.CURSOR LIST entry.
type CursorInfo struct {
	ID      uint64
	NS      string
	Kind    string
	Batch   int
	Created time.Time
	Idle    time.Duration
	Lease   uint64
	Rows    int64
}

// Open registers run as a cursor. The run must be suspended or about to
// be: Open suspends it.
func (c *Cursors) Open(run *Run, o CursorOptions) (*Cursor, error) {
	cur := &Cursor{NS: o.NS, Kind: o.Kind, Batch: o.Batch, RawBSON: o.RawBSON, run: run,
		lease: o.Lease, own: o.Own, gate: o.Gate}
	if cur.lease == nil {
		lease, err := c.st.Lease("cursor "+o.NS, 0)
		if err != nil {
			return nil, leaseErr(err)
		}
		cur.lease, cur.own, cur.renew = lease, true, true
		run.SetReader(c.st.At(lease))
	}
	run.Suspend()
	now := c.now()
	cur.Created, cur.last = now, now
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapLocked(now)
	if c.closed {
		c.endLocked(cur)
		return nil, errCursorsClosed
	}
	for cur.ID == 0 || c.m[cur.ID] != nil {
		cur.ID = rand.Uint64() >> 1
	}
	c.m[cur.ID] = cur
	return cur, nil
}

func leaseErr(err error) error {
	switch {
	case errors.Is(err, store.ErrTooManySnapshots):
		return ErrTooManySnapshots
	case errors.Is(err, store.ErrLeaseExpired):
		return ErrLeaseExpired
	}
	return err
}

// Read returns up to n more documents of cursor id (the cursor's batch
// size when n <= 0), within maxBytes. done reports that the cursor is
// exhausted and gone. A read that fails closes the cursor.
func (c *Cursors) Read(id uint64, n, maxBytes int) (cur *Cursor, docs []bson.Raw, done bool, err error) {
	now := c.now()
	c.mu.Lock()
	c.reapLocked(now)
	cur = c.m[id]
	switch {
	case cur == nil:
		c.mu.Unlock()
		return nil, nil, true, errCursorNotFound(id)
	case cur.busy:
		c.mu.Unlock()
		return nil, nil, false, &doc.Error{Code: 292, Msg: fmt.Sprintf("cursor id %d is already in use", id)}
	}
	cur.busy = true
	c.mu.Unlock()

	// A gate refusal (a full analytics semaphore) leaves the cursor open
	// for a retry.
	if cur.gate != nil {
		release, gerr := cur.gate()
		if gerr != nil {
			c.mu.Lock()
			cur.busy = false
			c.mu.Unlock()
			return cur, nil, false, gerr
		}
		defer release()
	}

	docs, done, err = c.read(cur, n, maxBytes)

	c.mu.Lock()
	defer c.mu.Unlock()
	cur.busy = false
	cur.last = c.now()
	if err != nil || done || c.closed {
		delete(c.m, id)
		c.endLocked(cur)
		done = true
	}
	return cur, docs, done, err
}

func (c *Cursors) read(cur *Cursor, n, maxBytes int) ([]bson.Raw, bool, error) {
	if cur.own {
		if _, live := c.st.Lookup(cur.lease.ID); !live {
			if !cur.renew {
				return nil, false, ErrLeaseExpired
			}
			lease, err := c.st.Lease("cursor "+cur.NS, 0)
			if err != nil {
				return nil, false, leaseErr(err)
			}
			cur.lease = lease
			cur.run.SetReader(c.st.At(lease))
		}
	}
	if n <= 0 {
		n = max(cur.Batch, 1)
	}
	docs, done, err := cur.run.Next(n, maxBytes)
	if err == nil && !done {
		cur.run.Suspend()
	}
	return docs, done, err
}

// Del closes cursor id and reports whether it existed.
func (c *Cursors) Del(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.m[id]
	if cur == nil || cur.busy {
		return false
	}
	delete(c.m, id)
	c.endLocked(cur)
	return true
}

// List returns the open cursors ordered by id.
func (c *Cursors) List() []CursorInfo {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reapLocked(now)
	out := make([]CursorInfo, 0, len(c.m))
	for _, cur := range c.m {
		out = append(out, CursorInfo{ID: cur.ID, NS: cur.NS, Kind: cur.Kind, Batch: cur.Batch, Created: cur.Created,
			Idle: now.Sub(cur.last), Lease: cur.lease.ID, Rows: cur.run.Rows()})
	}
	slices.SortFunc(out, func(a, b CursorInfo) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return out
}

// Len returns the number of open cursors.
func (c *Cursors) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// Close closes every cursor and refuses new ones. Call it before the store
// closes.
func (c *Cursors) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for id, cur := range c.m {
		if !cur.busy {
			delete(c.m, id)
			c.endLocked(cur)
		}
	}
}

// reapLocked closes cursors idle for longer than the idle timeout.
func (c *Cursors) reapLocked(now time.Time) {
	idle := c.idle()
	if idle <= 0 {
		return
	}
	for id, cur := range c.m {
		if !cur.busy && now.Sub(cur.last) > idle {
			delete(c.m, id)
			c.endLocked(cur)
		}
	}
}

func (c *Cursors) endLocked(cur *Cursor) {
	cur.run.Close()
	if cur.own {
		c.st.Release(cur.lease)
	}
}
