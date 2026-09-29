package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// Sweeper tuning. Tests in this package change them before Open.
var (
	// sweepInterval is the time between two sweep rounds.
	sweepInterval = 10 * time.Minute
	// sweepBudget is how many bytes of keys and values one round reads.
	sweepBudget int64 = 256 << 20
)

// sweepPointDeletes is the most entries of one dead collection version the
// sweeper deletes one by one; a larger one gets a single range delete. The
// LSM keeps range tombstones in its manifest until a compaction covers
// them, so one tombstone per small collection would pile up.
const sweepPointDeletes = 1024

// sweepChunk is how many deletes the sweeper commits in one batch.
const sweepChunk = 1024

// sweepOrder is the order in which rounds visit the column families; the
// catalog in CFDefault holds no garbage.
var sweepOrder = [...]CF{CFMeta, CFSub, CFZScore, CFDoc, CFIdx, CFGeo, CFCol}

// sweeper does the work of RocksDB's three compaction filters on the Rust
// engines, which have none:
//
//   - meta: metadata and version-map entries expired for longer than
//     metaGrace are deleted one by one, each under its LockRedis stripe
//     after a re-read, because a writer may have recreated the key since
//     the scan (a filter sees old versions; a delete would hit the new one).
//   - sub and zscore: the scan skips from one 8-byte version prefix to the
//     next and looks each version up in the version map with subFilter's
//     rules; a dead version's entries go. No lock is needed, since a
//     version is never reused.
//   - doc, idx, geo and col: the scan skips from one 4-byte id to the next
//     and range-deletes the ids the published LiveSet reports dead, the
//     backstop idFilter is today.
//
// Each round reads at most sweepBudget bytes and remembers where it
// stopped. On pgheap every deleted row is a dead tuple, so a round ends
// with VACUUM on each family it deleted from; on the LSM a range delete
// schedules a compaction of its range, as drops do.
type sweeper struct {
	mu     sync.Mutex // one pass at a time
	next   int        // index in sweepOrder where the next round starts
	resume []byte     // key in that family where it starts; nil is the first
}

// sweepStats counts what one pass did.
type sweepStats struct {
	scanned  int64       // bytes of keys and values read
	expired  int         // metadata and version-map entries deleted
	versions int         // dead collection versions whose entries were deleted
	ids      int         // dropped ids whose entries were deleted
	deleted  [NumCFs]int // point and range deletes per family
	ranges   []RangeDelete
}

func (s *Store) runSweeper() {
	defer s.janitor.Done()
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// A failed round leaves its position; the next one retries.
			_, _ = s.sweepRound(sweepBudget)
		case <-s.stop:
			return
		}
	}
}

// sweepRound runs one round: from where the last one stopped it visits the
// families in sweepOrder until it has read budget bytes or visited each
// family once.
func (s *Store) sweepRound(budget int64) (sweepStats, error) {
	sw := &s.sweep
	sw.mu.Lock()
	defer sw.mu.Unlock()
	var st sweepStats
	nowMS := s.cfg.Clock().UnixMilli()
	left := budget
	var err error
	for range len(sweepOrder) {
		cf := sweepOrder[sw.next]
		var resume []byte
		resume, err = s.sweepCF(cf, sw.resume, nil, &left, nowMS, &st)
		if err != nil {
			break
		}
		if resume != nil {
			sw.resume = resume
			break
		}
		sw.resume = nil
		sw.next = (sw.next + 1) % len(sweepOrder)
	}
	if aerr := s.afterSweep(&st, true); err == nil {
		err = aerr
	}
	return st, err
}

// sweepRange sweeps [lo, hi) of cf with no budget; Compact runs it first
// on the Rust engines, so ROCKS.COMPACT drops what RocksDB's filters would.
func (s *Store) sweepRange(cf CF, lo, hi []byte) (sweepStats, error) {
	s.sweep.mu.Lock()
	defer s.sweep.mu.Unlock()
	var st sweepStats
	left := int64(math.MaxInt64)
	_, err := s.sweepCF(cf, cloneBound(lo), cloneBound(hi), &left, s.cfg.Clock().UnixMilli(), &st)
	if aerr := s.afterSweep(&st, false); err == nil {
		err = aerr
	}
	return st, err
}

// afterSweep schedules compactions of the range deletes and, on pgheap
// when vacuum is set, vacuums every family the pass deleted from.
func (s *Store) afterSweep(st *sweepStats, vacuum bool) error {
	if s.cfg.Engine == EnginePgHeap {
		if !vacuum {
			return nil
		}
		for cf := range CF(NumCFs) {
			if st.deleted[cf] > 0 {
				if err := s.kv.compact(cf, nil, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, r := range st.ranges {
		s.ScheduleCompact(r.CF, r.Lo, r.Hi)
	}
	return nil
}

// sweepCF sweeps [from, to) of cf until *left runs out; it returns the key
// to resume at, or nil once it reached the end.
func (s *Store) sweepCF(cf CF, from, to []byte, left *int64, nowMS int64, st *sweepStats) ([]byte, error) {
	d := &deleter{s: s, cf: cf, st: st}
	var resume []byte
	var err error
	switch cf {
	case CFMeta:
		resume, err = s.sweepMeta(from, to, left, nowMS, d)
	case CFSub, CFZScore:
		resume, err = s.sweepVersions(cf, from, to, left, nowMS, d)
	case CFDoc, CFIdx, CFGeo, CFCol:
		resume, err = s.sweepIDs(cf, from, to, left, d)
	}
	if ferr := d.flush(); err == nil {
		err = ferr
	}
	return resume, err
}

// deleter batches the sweeper's deletes on one family.
type deleter struct {
	s  *Store
	cf CF
	st *sweepStats
	b  kvBatch
	n  int
}

func (d *deleter) delete(key []byte) error {
	if d.b == nil {
		d.b = d.s.kv.newBatch()
	}
	d.b.delete(d.cf, key)
	return d.count()
}

func (d *deleter) deleteRange(lo, hi []byte) error {
	if d.b == nil {
		d.b = d.s.kv.newBatch()
	}
	d.b.deleteRange(d.cf, lo, hi)
	d.st.ranges = append(d.st.ranges, RangeDelete{CF: d.cf, Lo: lo, Hi: hi})
	return d.count()
}

func (d *deleter) count() error {
	d.n++
	d.st.deleted[d.cf]++
	if d.n >= sweepChunk {
		return d.flush()
	}
	return nil
}

func (d *deleter) flush() error {
	if d.b == nil {
		return nil
	}
	b, n := d.b, d.n
	d.b, d.n = nil, 0
	defer b.destroy()
	if n == 0 || d.s.closed.Load() {
		return nil
	}
	return d.s.kv.write(b, d.s.syncCommits())
}

// spend charges one entry to the budget; false means stop here.
func spend(left *int64, st *sweepStats, it Iterator) bool {
	if *left <= 0 {
		return false
	}
	n := int64(len(it.Key()) + len(it.Value()))
	*left -= n
	st.scanned += n
	return true
}

// sweepOpts are the sweeper's iterator options: its scans skip across the
// prefixes of sub, zscore and geo, which on RocksDB needs total order.
var sweepOpts = IterOpts{TotalOrderSeek: true}

func seekFrom(it Iterator, from []byte) {
	if from == nil {
		it.SeekToFirst()
	} else {
		it.Seek(from)
	}
}

// sweepVersions deletes the entries of dead collection versions in sub or
// zscore.
func (s *Store) sweepVersions(cf CF, from, to []byte, left *int64, nowMS int64, d *deleter) ([]byte, error) {
	it := s.Iter(cf, from, to, sweepOpts)
	defer it.Close()
	for seekFrom(it, from); it.Valid(); {
		if s.closed.Load() {
			return nil, ErrClosed
		}
		if !spend(left, d.st, it) {
			return clone(it.Key()), nil
		}
		k := it.Key()
		if len(k) < 8 {
			it.Next()
			continue
		}
		version := binary.BigEndian.Uint64(k)
		dead, err := s.versionDead(version, nowMS)
		if err != nil {
			return nil, err
		}
		if !dead {
			if version == math.MaxUint64 {
				break
			}
			it.Seek(binary.BigEndian.AppendUint64(nil, version+1))
			continue
		}
		prefix := clone(k[:8])
		var keys [][]byte
		for it.Valid() && bytes.HasPrefix(it.Key(), prefix) && len(keys) <= sweepPointDeletes {
			keys = append(keys, clone(it.Key()))
			spend(left, d.st, it)
			it.Next()
		}
		d.st.versions++
		if len(keys) > sweepPointDeletes && version < math.MaxUint64 {
			hi := binary.BigEndian.AppendUint64(nil, version+1)
			if err := d.deleteRange(prefix, hi); err != nil {
				return nil, err
			}
			it.Seek(hi)
			continue
		}
		for _, key := range keys {
			if err := d.delete(key); err != nil {
				return nil, err
			}
		}
	}
	return nil, it.Err()
}

// versionDead applies subFilter's rules to one collection version.
func (s *Store) versionDead(version uint64, nowMS int64) (bool, error) {
	if nowMS*1000-layout.VersionMicros(version) < subYoung.Microseconds() {
		return false, nil
	}
	v, ok, err := s.Get(CFMeta, layout.VersionKey(nil, version))
	if err != nil || !ok {
		return !ok && err == nil, err
	}
	e, err := layout.DecodeVersionEntry(v)
	if err != nil {
		return false, nil
	}
	return expiredPastGrace(e.ExpireMS, nowMS), nil
}

// sweepMeta deletes metadata and version-map entries expired past
// metaGrace, re-reading each under its key's lock.
func (s *Store) sweepMeta(from, to []byte, left *int64, nowMS int64, d *deleter) ([]byte, error) {
	it := s.Iter(CFMeta, from, to, sweepOpts)
	defer it.Close()
	var keys [][]byte
	var locks []LockKey
	drain := func() error {
		if len(keys) == 0 {
			return nil
		}
		defer func() { keys, locks = keys[:0], locks[:0] }()
		unlock := s.Lock(locks)
		defer unlock()
		for _, k := range keys {
			v, ok, err := s.Get(CFMeta, k)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if _, still := expiredMeta(k, v, nowMS); !still {
				continue
			}
			if err := d.delete(k); err != nil {
				return err
			}
			d.st.expired++
		}
		// Commit before the locks go, so no writer sees the key between
		// the re-read and the delete.
		return d.flush()
	}
	for seekFrom(it, from); it.Valid(); it.Next() {
		if s.closed.Load() {
			return nil, ErrClosed
		}
		if !spend(left, d.st, it) {
			resume := clone(it.Key())
			return resume, drain()
		}
		if lk, ok := expiredMeta(it.Key(), it.Value(), nowMS); ok {
			keys = append(keys, clone(it.Key()))
			locks = append(locks, lk)
			if len(keys) >= 256 {
				if err := drain(); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return nil, drain()
}

// expiredMeta applies metaFilter's rule to one meta entry and returns the
// lock of the Redis key it belongs to.
func expiredMeta(k, v []byte, nowMS int64) (LockKey, bool) {
	switch {
	case len(k) > 0 && k[0] < layout.NumDBs:
		if len(v) < layout.StringHeaderLen || v[0]>>4 != layout.LayoutVersion {
			return LockKey{}, false
		}
		if !expiredPastGrace(int64(binary.BigEndian.Uint64(v[1:9])), nowMS) {
			return LockKey{}, false
		}
		return LockKey{Kind: LockRedis, NS: uint32(k[0]), Key: clone(k[1:])}, true
	case len(k) == 9 && k[0] == layout.VersionMapPrefix:
		e, err := layout.DecodeVersionEntry(v)
		if err != nil || !expiredPastGrace(e.ExpireMS, nowMS) {
			return LockKey{}, false
		}
		return LockKey{Kind: LockRedis, NS: uint32(e.DB), Key: clone(e.Key)}, true
	}
	return LockKey{}, false
}

// sweepIDs range-deletes the entries of ids the LiveSet reports dead.
// Without a LiveSet it keeps everything, as idFilter does.
func (s *Store) sweepIDs(cf CF, from, to []byte, left *int64, d *deleter) ([]byte, error) {
	live := s.liveSet()
	if live == nil {
		return nil, nil
	}
	it := s.Iter(cf, from, to, sweepOpts)
	defer it.Close()
	for seekFrom(it, from); it.Valid(); {
		if s.closed.Load() {
			return nil, ErrClosed
		}
		if !spend(left, d.st, it) {
			return clone(it.Key()), nil
		}
		k := it.Key()
		if len(k) < 4 {
			it.Next()
			continue
		}
		id := binary.BigEndian.Uint32(k)
		if id == math.MaxUint32 {
			break
		}
		lo, hi := layout.IDBounds(id)
		if !live.Live(id) {
			if err := d.deleteRange(lo, hi); err != nil {
				return nil, err
			}
			d.st.ids++
		}
		it.Seek(hi)
	}
	return nil, it.Err()
}
