package store

import (
	"slices"
	"sync"
	"time"
)

// Snapshot is an engine snapshot NilDB tracks. Snapshot() creates a
// command-scoped one (uncounted, Expires zero); Lease() creates a counted
// one that the janitor releases after its TTL. The exported fields are a
// record of creation: the store decides expiry from its own copy, so
// changing them changes nothing.
type Snapshot struct {
	ID      uint64
	Seq     uint64
	Owner   string
	Created time.Time
	Expires time.Time // zero for command-scoped snapshots

	h *snapHandle
}

// snapHandle is the mutable part of a Snapshot. Reads hold mu.RLock
// across the C call that dereferences snap; Release takes mu.Lock.
type snapHandle struct {
	mu       sync.RWMutex
	snap     kvSnap
	lease    bool
	expires  time.Time // leases only
	released bool
}

type snapRegistry struct {
	mu     sync.Mutex
	nextID uint64
	all    map[uint64]*Snapshot // every unreleased snapshot, both kinds
	leases int                  // unreleased or reserved leases
}

func (r *snapRegistry) init() { r.all = make(map[uint64]*Snapshot) }

// newSnapshot creates an engine snapshot and registers it.
func (s *Store) newSnapshot(owner string, lease bool, ttl time.Duration) (*Snapshot, error) {
	snap, seq, err := s.kv.snapshot()
	if err != nil {
		return nil, err
	}
	now := s.cfg.Clock()
	sn := &Snapshot{
		Seq:     seq,
		Owner:   owner,
		Created: now,
		h:       &snapHandle{snap: snap, lease: lease},
	}
	if lease {
		sn.Expires = now.Add(ttl)
		sn.h.expires = sn.Expires
	}
	s.snaps.mu.Lock()
	s.snaps.nextID++
	sn.ID = s.snaps.nextID
	s.snaps.all[sn.ID] = sn
	s.snaps.mu.Unlock()
	return sn, nil
}

// Snapshot takes a command-scoped snapshot. It is not counted against
// MaxSnapshots; release it when the command ends. It returns nil after
// Close.
func (s *Store) Snapshot() *Snapshot {
	if s.closed.Load() {
		return nil
	}
	sn, err := s.newSnapshot("", false, 0)
	if err != nil {
		return nil
	}
	return sn
}

// Lease takes a snapshot counted against MaxSnapshots that the janitor
// releases once its TTL has passed. ttl <= 0 means Config.LeaseTTL; a ttl
// above Config.LeaseMax is cut to LeaseMax. At the cap Lease first
// releases expired leases, then fails with ErrTooManySnapshots.
func (s *Store) Lease(owner string, ttl time.Duration) (*Snapshot, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if ttl <= 0 {
		ttl = s.cfg.LeaseTTL
	}
	ttl = min(ttl, s.cfg.LeaseMax)
	if !s.reserveLease() {
		s.ReleaseExpired()
		if !s.reserveLease() {
			return nil, ErrTooManySnapshots
		}
	}
	sn, err := s.newSnapshot(owner, true, ttl)
	if err != nil {
		s.snaps.mu.Lock()
		s.snaps.leases--
		s.snaps.mu.Unlock()
		return nil, err
	}
	return sn, nil
}

// reserveLease takes a lease slot if one is free.
func (s *Store) reserveLease() bool {
	s.snaps.mu.Lock()
	defer s.snaps.mu.Unlock()
	if s.snaps.leases >= s.cfg.MaxSnapshots {
		return false
	}
	s.snaps.leases++
	return true
}

// Lookup returns the unreleased lease with the given id. A lease whose TTL
// has passed is released here and reported absent. Command-scoped
// snapshots are never returned.
func (s *Store) Lookup(id uint64) (*Snapshot, bool) {
	s.snaps.mu.Lock()
	sn, ok := s.snaps.all[id]
	s.snaps.mu.Unlock()
	if !ok || !sn.h.lease {
		return nil, false
	}
	if !s.cfg.Clock().Before(sn.h.expires) {
		s.Release(sn)
		return nil, false
	}
	return sn, true
}

// Release releases the snapshot. It is idempotent and accepts nil. Reads
// through At(snap) afterwards return ErrLeaseExpired.
func (s *Store) Release(snap *Snapshot) {
	if snap == nil || snap.h == nil {
		return
	}
	s.snaps.mu.Lock()
	_, ok := s.snaps.all[snap.ID]
	if ok {
		delete(s.snaps.all, snap.ID)
		if snap.h.lease {
			s.snaps.leases--
		}
	}
	s.snaps.mu.Unlock()
	if ok {
		s.freeSnapshot(snap.h)
	}
}

// freeSnapshot waits for in-flight reads, then frees the engine snapshot.
func (s *Store) freeSnapshot(h *snapHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return
	}
	h.released = true
	s.kv.release(h.snap)
	h.snap = nil
}

// oldestSnapshotTime is the creation time, in Unix seconds, of the oldest
// snapshot the store has not released, or 0: rocksdb.oldest-snapshot-time
// for engines that do not report it.
func (s *Store) oldestSnapshotTime() uint64 {
	s.snaps.mu.Lock()
	defer s.snaps.mu.Unlock()
	var oldest time.Time
	for _, sn := range s.snaps.all {
		if oldest.IsZero() || sn.Created.Before(oldest) {
			oldest = sn.Created
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return uint64(oldest.Unix())
}

// Leases returns the unreleased leases ordered by ID.
func (s *Store) Leases() []*Snapshot {
	s.snaps.mu.Lock()
	out := make([]*Snapshot, 0, s.snaps.leases)
	for _, sn := range s.snaps.all {
		if sn.h.lease {
			out = append(out, sn)
		}
	}
	s.snaps.mu.Unlock()
	slices.SortFunc(out, func(a, b *Snapshot) int {
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

// ReleaseExpired releases every lease whose Expires is not after the
// store clock's now and returns how many it released. The janitor calls
// it once a second.
func (s *Store) ReleaseExpired() int {
	now := s.cfg.Clock()
	var expired []*Snapshot
	s.snaps.mu.Lock()
	for id, sn := range s.snaps.all {
		if sn.h.lease && !now.Before(sn.h.expires) {
			delete(s.snaps.all, id)
			s.snaps.leases--
			expired = append(expired, sn)
		}
	}
	s.snaps.mu.Unlock()
	for _, sn := range expired {
		s.freeSnapshot(sn.h)
	}
	return len(expired)
}

// releaseAll releases every snapshot, both kinds; Close calls it.
func (s *Store) releaseAll() {
	s.snaps.mu.Lock()
	all := s.snaps.all
	s.snaps.all = make(map[uint64]*Snapshot)
	s.snaps.leases = 0
	s.snaps.mu.Unlock()
	for _, sn := range all {
		s.freeSnapshot(sn.h)
	}
}

func (s *Store) runJanitor(every time.Duration) {
	defer s.janitor.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.ReleaseExpired()
		case <-s.stop:
			return
		}
	}
}
