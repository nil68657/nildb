package store

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// LockKind orders the lock arrays. Locks are always taken in ascending
// kind order.
type LockKind uint8

// Lock kinds, in acquisition order.
const (
	LockColl  LockKind = iota // NS = coll_id; Key[0] = 'r' shared, 'w' exclusive
	LockDoc                   // NS = coll_id; Key = ks(_id)
	LockUniq                  // NS = idx_id;  Key = ks(values)
	LockRedis                 // NS = db;      Key = user key
)

// LockKey names one lock. For LockColl, Key[0] selects the mode: 'r' takes
// the collection lock shared and anything else (normally 'w') takes it
// exclusive. For the other kinds, (NS, Key) hashes to one of 4096 stripes.
type LockKey struct {
	Kind LockKind
	NS   uint32
	Key  []byte
}

// lockStripes is the size of each striped mutex array.
const lockStripes = 4096

// lockManager holds one 4096-stripe mutex array per striped kind
// (LockDoc, LockUniq, LockRedis) and a registry of per-collection
// RWMutexes for LockColl. Collection locks are created on first use and
// never removed; coll_ids are never reused, so the registry grows by one
// small entry per collection ever locked.
type lockManager struct {
	stripes [3][lockStripes]sync.Mutex
	colls   sync.Map // uint32 coll_id -> *sync.RWMutex
}

// lockEnt is one resolved lock: a stripe index for striped kinds, the
// coll_id for LockColl.
type lockEnt struct {
	kind LockKind
	idx  uint32
	excl bool          // LockColl only
	coll *sync.RWMutex // LockColl only, resolved when locking
}

func stripeOf(ns uint32, key []byte) uint32 {
	var nsb [4]byte
	binary.BigEndian.PutUint32(nsb[:], ns)
	var d xxhash.Digest
	d.Reset()
	d.Write(nsb[:])
	d.Write(key)
	return uint32(d.Sum64() & (lockStripes - 1))
}

func (m *lockManager) coll(id uint32) *sync.RWMutex {
	if v, ok := m.colls.Load(id); ok {
		return v.(*sync.RWMutex)
	}
	v, _ := m.colls.LoadOrStore(id, new(sync.RWMutex))
	return v.(*sync.RWMutex)
}

// Lock takes every lock in keys and returns the function that releases
// them. It resolves each key to its stripe (or collection lock), sorts by
// (kind, stripe), deduplicates (a shared and an exclusive request for the
// same collection become one exclusive lock) and locks in ascending order,
// so any two Lock calls are deadlock-free against each other. The returned
// unlock releases in reverse order; calling it more than once is a no-op.
//
// Two-phase rule: a caller that already holds locks may call Lock again
// only with keys of a strictly higher kind than any it holds (LockColl <
// LockDoc < LockUniq < LockRedis). Use LockAfter, which checks the rule, for
// a second phase. Reads take no locks.
func (s *Store) Lock(keys []LockKey) (unlock func()) {
	return s.locks.lock(keys)
}

// LockAfter is the second phase of a two-phase acquisition: held is what
// the caller already holds. It panics unless every kind in keys is
// strictly above every kind in held, which is the rule that keeps
// two-phase acquisition deadlock-free across the arrays.
func (s *Store) LockAfter(held, keys []LockKey) (unlock func()) {
	if len(held) > 0 {
		top := held[0].Kind
		for _, k := range held[1:] {
			top = max(top, k.Kind)
		}
		for _, k := range keys {
			if k.Kind <= top {
				panic(fmt.Sprintf("store: two-phase lock order violated: kind %d requested while holding kind %d", k.Kind, top))
			}
		}
	}
	return s.locks.lock(keys)
}

func (m *lockManager) lock(keys []LockKey) func() {
	if len(keys) == 0 {
		return func() {}
	}
	ents := make([]lockEnt, 0, len(keys))
	for _, k := range keys {
		switch {
		case k.Kind == LockColl:
			ents = append(ents, lockEnt{kind: LockColl, idx: k.NS, excl: len(k.Key) == 0 || k.Key[0] != 'r'})
		case k.Kind <= LockRedis:
			ents = append(ents, lockEnt{kind: k.Kind, idx: stripeOf(k.NS, k.Key)})
		default:
			panic(fmt.Sprintf("store: unknown lock kind %d", k.Kind))
		}
	}
	slices.SortFunc(ents, func(a, b lockEnt) int {
		if a.kind != b.kind {
			return int(a.kind) - int(b.kind)
		}
		switch {
		case a.idx < b.idx:
			return -1
		case a.idx > b.idx:
			return 1
		}
		return 0
	})
	out := ents[:0]
	for _, e := range ents {
		if n := len(out); n > 0 && out[n-1].kind == e.kind && out[n-1].idx == e.idx {
			out[n-1].excl = out[n-1].excl || e.excl
			continue
		}
		out = append(out, e)
	}
	ents = out

	for i := range ents {
		e := &ents[i]
		if e.kind == LockColl {
			e.coll = m.coll(e.idx)
			if e.excl {
				e.coll.Lock()
			} else {
				e.coll.RLock()
			}
			continue
		}
		m.stripes[e.kind-LockDoc][e.idx].Lock()
	}
	done := false
	return func() {
		if done {
			return
		}
		done = true
		for i := len(ents) - 1; i >= 0; i-- {
			e := ents[i]
			if e.kind == LockColl {
				if e.excl {
					e.coll.Unlock()
				} else {
					e.coll.RUnlock()
				}
				continue
			}
			m.stripes[e.kind-LockDoc][e.idx].Unlock()
		}
	}
}
