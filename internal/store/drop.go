package store

import (
	"sync"
	"time"
)

// RangeDelete is one [Lo, Hi) range tombstone for DeleteRanges. Hi must be
// set; layout.IDBounds returns a nil Hi only for id MaxUint32.
type RangeDelete struct {
	CF     CF
	Lo, Hi []byte
}

// PointDelete names the struct type of DeleteRanges' point deletes, so
// callers can write []store.PointDelete{{CF: ..., Key: ...}}.
type PointDelete = struct {
	CF  CF
	Key []byte
}

// DeleteRanges drops keyspaces: one plain write batch holding a range
// delete per range plus the caller's point deletes (catalog records,
// version-map entries), committed once, so the drop is atomic. It never
// runs inside an indexed Txn. Follow it with ScheduleCompact on each range
// so the range tombstones get compacted away (on pgheap, so VACUUM removes
// the rows the range delete marked dead).
func (s *Store) DeleteRanges(ranges []RangeDelete, alsoDelete []struct {
	CF  CF
	Key []byte
}) error {
	if s.closed.Load() {
		return ErrClosed
	}
	for _, r := range ranges {
		if _, err := rangeErr(r.CF, r.Lo, r.Hi); err != nil {
			return err
		}
	}
	b := s.kv.newBatch()
	defer b.destroy()
	n := 0
	for _, r := range ranges {
		if skip, _ := rangeErr(r.CF, r.Lo, r.Hi); !skip {
			b.deleteRange(r.CF, r.Lo, r.Hi)
			n++
		}
	}
	for _, p := range alsoDelete {
		b.delete(p.CF, p.Key)
		n++
	}
	if n == 0 {
		return nil
	}
	return s.kv.write(b, s.syncCommits())
}

// Scheduling of post-drop compactions. Tests in this package change the
// delay and burst before calling ScheduleCompact.
var (
	compactDelay = 60 * time.Second
	compactBurst = 32
	// compactHook, when set, runs after each scheduled compaction.
	compactHook func(cf CF, lo, hi []byte)
)

// ScheduleCompact runs a non-exclusive CompactRangeCFOpt over [lo, hi) of
// cf 60 s from now, so a drop's range tombstones do not pile up. When 32
// compactions are outstanding, all of them run at once. Close cancels
// pending ones and waits for running ones. lo and hi are copied; nil is an
// open bound.
func (s *Store) ScheduleCompact(cf CF, lo, hi []byte) {
	if s.closed.Load() {
		return
	}
	checkCF(cf) // reject an unknown CF now, not in the timer
	s.compact.schedule(cf, clonePtr(lo), clonePtr(hi))
}

// clonePtr copies b, keeping nil as nil (an open compaction bound).
func clonePtr(b []byte) []byte {
	if b == nil {
		return nil
	}
	return clone(b)
}

func (s *Store) runCompact(cf CF, lo, hi []byte) {
	if s.closed.Load() {
		return
	}
	_ = s.kv.compact(cf, lo, hi)
	if compactHook != nil {
		compactHook(cf, lo, hi)
	}
}

type compactJob struct {
	cf     CF
	lo, hi []byte
	timer  *time.Timer
}

// compactor holds the timers of scheduled compactions.
type compactor struct {
	run func(cf CF, lo, hi []byte)

	mu     sync.Mutex
	jobs   map[*compactJob]struct{}
	closed bool
	wg     sync.WaitGroup // running compactions
}

func (c *compactor) init(run func(cf CF, lo, hi []byte)) {
	c.run = run
	c.jobs = make(map[*compactJob]struct{})
}

func (c *compactor) schedule(cf CF, lo, hi []byte) {
	j := &compactJob{cf: cf, lo: lo, hi: hi}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.jobs[j] = struct{}{}
	if len(c.jobs) >= compactBurst {
		batch := make([]*compactJob, 0, len(c.jobs))
		for p := range c.jobs {
			if p.timer != nil {
				p.timer.Stop()
			}
			batch = append(batch, p)
		}
		clear(c.jobs)
		c.wg.Add(1)
		go c.runBatch(batch)
		return
	}
	j.timer = time.AfterFunc(compactDelay, func() { c.fire(j) })
}

func (c *compactor) fire(j *compactJob) {
	c.mu.Lock()
	if _, ok := c.jobs[j]; !ok || c.closed {
		c.mu.Unlock()
		return
	}
	delete(c.jobs, j)
	c.wg.Add(1)
	c.mu.Unlock()
	c.runBatch([]*compactJob{j})
}

func (c *compactor) runBatch(batch []*compactJob) {
	defer c.wg.Done()
	for _, j := range batch {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		c.run(j.cf, j.lo, j.hi)
	}
}

func (c *compactor) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

// close cancels every pending timer and waits for running compactions.
func (c *compactor) close() {
	c.mu.Lock()
	c.closed = true
	for j := range c.jobs {
		if j.timer != nil {
			j.timer.Stop()
		}
	}
	clear(c.jobs)
	c.mu.Unlock()
	c.wg.Wait()
}
