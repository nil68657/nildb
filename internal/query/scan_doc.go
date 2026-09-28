package query

import (
	"bytes"
	"slices"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
)

// keyRange is [lo, hi) over absolute column-family keys.
type keyRange struct{ lo, hi []byte }

// docScan reads a collection's rows from the doc column family in ks(_id)
// order, or in reverse: the whole collection for a rowscan, or the ranges
// that predicates on _id bound. The filter runs on the iterator's view of
// each row, so only matching rows are copied.
type docScan struct {
	x       *Exec
	ranges  []keyRange // sorted, disjoint
	reverse bool
	filter  *doc.Matcher

	it   store.Iterator
	ri   int    // ranges consumed, in scan order
	last []byte // last key examined in the current range; the resume point
	done bool
}

func newDocScan(x *Exec, coll uint32, ranges []keyRange, reverse bool, filter *doc.Matcher) *docScan {
	if ranges == nil {
		lo, hi := layout.IDBounds(coll)
		ranges = []keyRange{{lo, hi}}
	}
	return &docScan{x: x, ranges: ranges, reverse: reverse, filter: filter}
}

// open positions an iterator on the current range, after the resume point.
func (s *docScan) open() error {
	for s.ri < len(s.ranges) {
		r := s.ranges[s.ri]
		if s.reverse {
			r = s.ranges[len(s.ranges)-1-s.ri]
		}
		lo, hi := r.lo, r.hi
		if s.last != nil {
			if s.reverse {
				hi = s.last
			} else if next := plusByte(s.last, 0); bytes.Compare(next, lo) > 0 {
				lo = next
			}
		}
		if hi != nil && bytes.Compare(lo, hi) >= 0 {
			s.ri++
			s.last = nil
			continue
		}
		it, err := s.x.iter(store.CFDoc, lo, hi, false, false)
		if err != nil {
			return err
		}
		s.it = it
		if s.reverse {
			it.SeekToLast()
		} else {
			it.SeekToFirst()
		}
		return nil
	}
	s.done = true
	return nil
}

func (s *docScan) Next() (Row, bool, error) {
	for !s.done {
		if s.it == nil {
			if err := s.open(); err != nil {
				return Row{}, false, err
			}
			continue
		}
		if !s.it.Valid() {
			err := s.it.Err()
			s.it.Close()
			s.it = nil
			if err != nil {
				return Row{}, false, s.x.readErr(err)
			}
			s.ri++
			s.last = nil
			continue
		}
		k, v := s.it.Key(), s.it.Value()
		if err := s.x.row(len(k) + len(v)); err != nil {
			return Row{}, false, err
		}
		s.last = append(s.last[:0], k...)
		ok := true
		if s.filter != nil {
			var err error
			if ok, err = s.filter.Match(v); err != nil {
				return Row{}, false, err
			}
		}
		var row Row
		if ok {
			row = Row{Doc: clone(v), ID: clone(k[4:])}
		}
		if s.reverse {
			s.it.Prev()
		} else {
			s.it.Next()
		}
		if ok {
			return row, true, nil
		}
	}
	return Row{}, false, nil
}

func (s *docScan) Suspend() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
}

func (s *docScan) Close() {
	s.Suspend()
	s.done = true
}

// docGet reads rows by exact _id, fetchBatch at a time: the plan for
// {_id: v} and {_id: {$in: [...]}}.
type docGet struct {
	x      *Exec
	coll   uint32
	ids    [][]byte // ks(_id), in output order
	filter *doc.Matcher
	pos    int
	buf    []Row
	bi     int
}

func newDocGet(x *Exec, coll uint32, ids [][]byte, reverse bool, filter *doc.Matcher) *docGet {
	ids = slices.Clone(ids)
	slices.SortFunc(ids, bytes.Compare)
	ids = slices.CompactFunc(ids, bytes.Equal)
	if reverse {
		slices.Reverse(ids)
	}
	return &docGet{x: x, coll: coll, ids: ids, filter: filter}
}

func (g *docGet) Next() (Row, bool, error) {
	for {
		if g.bi < len(g.buf) {
			r := g.buf[g.bi]
			g.buf[g.bi] = Row{}
			g.bi++
			return r, true, nil
		}
		if g.pos >= len(g.ids) {
			return Row{}, false, nil
		}
		batch := g.ids[g.pos:min(g.pos+fetchBatch, len(g.ids))]
		g.pos += len(batch)
		rows, err := fetchRows(g.x, g.coll, batch, g.filter)
		if err != nil {
			return Row{}, false, err
		}
		g.buf, g.bi = rows, 0
	}
}

func (g *docGet) Suspend() {}
func (g *docGet) Close()   { g.pos, g.buf = len(g.ids), nil }

// fetchRows reads the rows of ids in one MultiGet and keeps those that
// exist and pass filter, in ids order.
func fetchRows(x *Exec, coll uint32, ids [][]byte, filter *doc.Matcher) ([]Row, error) {
	keys := make([][]byte, len(ids))
	for i, id := range ids {
		keys[i] = layout.DocKey(nil, coll, id)
	}
	vals, err := x.multiGet(store.CFDoc, keys)
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		if err := x.row(len(keys[i]) + len(v)); err != nil {
			return nil, err
		}
		if filter != nil {
			ok, err := filter.Match(v)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		out = append(out, Row{Doc: v, ID: ids[i]})
	}
	return out, nil
}
