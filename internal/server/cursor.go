package server

import (
	"crypto/rand"
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/nil68657/nildb/internal/command"
)

// CursorKind and Position are defined in command so that the SCAN
// handlers reach the ring without importing this package.
type (
	CursorKind = command.CursorKind
	Position   = command.Position
)

// Cursor kinds, re-exported from command.
const (
	CursorScan  = command.CursorScan
	CursorHScan = command.CursorHScan
	CursorSScan = command.CursorSScan
	CursorZScan = command.CursorZScan
)

// RingSize is the number of positions the ring keeps; the oldest is
// evicted once more cursors are issued.
const RingSize = 16384

// Cursor bit layout, from the top bit down (architecture.md section 3):
// | kind (3) | db (4) | hash25(position) (25) | epoch (16) | counter (16) |.
const (
	kindShift  = 61
	dbShift    = 57
	hashShift  = 32
	epochShift = 16
	hashBits   = 25
)

// CursorRing maps SCAN cursors to positions. A cursor is a non-zero
// uint64 that clients parse with strtoull; the ring slot is its low
// counter bits, the epoch is random per ring so a cursor from an earlier
// process is refused, and the kind and db fields make a cursor replayed
// against another SCAN family or after SELECT fail. It is safe for
// concurrent use.
type CursorRing struct {
	mu      sync.Mutex
	counter uint64
	epoch   uint16
	entries [RingSize]ringEntry
}

type ringEntry struct {
	cursor uint64
	pos    Position
}

var _ command.Cursors = (*CursorRing)(nil)

// NewCursorRing returns an empty ring with a random epoch.
func NewCursorRing() *CursorRing {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return newCursorRingEpoch(binary.BigEndian.Uint16(b[:]))
}

func newCursorRingEpoch(epoch uint16) *CursorRing {
	return &CursorRing{epoch: epoch}
}

// hash25 is the 25-bit fingerprint of a position stored in its cursor.
func hash25(p Position) uint64 {
	var d xxhash.Digest
	d.Reset()
	var v [9]byte
	v[0] = p.DB
	binary.BigEndian.PutUint64(v[1:], p.Version)
	d.Write(v[:])
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(p.Key)))
	d.Write(n[:])
	d.Write(p.Key)
	d.Write(p.Sub)
	return d.Sum64() >> (64 - hashBits)
}

func clonePos(p Position) Position {
	p.Key = append([]byte(nil), p.Key...)
	p.Sub = append([]byte(nil), p.Sub...)
	return p
}

// Put stores p and returns its cursor, never 0. p.Kind must be one of the
// Cursor kinds and p.DB below 16; anything else is a programming error
// and panics.
func (r *CursorRing) Put(p Position) uint64 {
	if p.Kind == 0 || p.Kind > 7 || p.DB > 15 {
		panic("server: cursor position with invalid kind or db")
	}
	h := hash25(p)
	p = clonePos(p)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counter++
	ctr := r.counter & 0xFFFF
	cur := uint64(p.Kind)<<kindShift | uint64(p.DB)<<dbShift | h<<hashShift | uint64(r.epoch)<<epochShift | ctr
	r.entries[ctr&(RingSize-1)] = ringEntry{cursor: cur, pos: p}
	return cur
}

// Get returns a copy of the position behind cursor. It reports false for
// 0, for a cursor issued for another db or kind, for a cursor of another
// epoch, and for one whose slot has since been reused.
func (r *CursorRing) Get(cursor uint64, db uint8, kind CursorKind) (Position, bool) {
	if cursor == 0 ||
		CursorKind(cursor>>kindShift) != kind ||
		uint8(cursor>>dbShift&0xF) != db ||
		uint16(cursor>>epochShift) != r.epoch {
		return Position{}, false
	}
	r.mu.Lock()
	e := r.entries[cursor&0xFFFF&(RingSize-1)]
	r.mu.Unlock()
	if e.cursor != cursor {
		return Position{}, false
	}
	return clonePos(e.pos), true
}
