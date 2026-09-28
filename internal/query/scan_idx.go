package query

import (
	"bytes"
	"fmt"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/store"
)

// idxScan reads a btree index over sorted, disjoint key ranges, collects
// fetchBatch _ids at a time, reads their rows with one MultiGet and
// re-checks the full filter on each, because index bounds are a superset
// of the matches. Rows come out in index order (reverse order for a
// reverse scan). A multikey index can hold several entries per document,
// so its scan remembers the _ids it has emitted.
type idxScan struct {
	x       *Exec
	coll    uint32
	ix      *catalog.Index
	ranges  []keyRange
	reverse bool
	filter  *doc.Matcher
	seen    map[string]struct{} // multikey only

	it   store.Iterator
	ri   int
	last []byte
	ids  [][]byte
	buf  []Row
	bi   int
	done bool // the index side is exhausted
}

func newIdxScan(x *Exec, coll uint32, ix *catalog.Index, ranges []keyRange, reverse bool, filter *doc.Matcher) *idxScan {
	s := &idxScan{x: x, coll: coll, ix: ix, ranges: ranges, reverse: reverse, filter: filter}
	if ix.Multikey() {
		s.seen = make(map[string]struct{})
	}
	return s
}

func (s *idxScan) open() error {
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
		if bytes.Compare(lo, hi) >= 0 {
			s.ri++
			s.last = nil
			continue
		}
		it, err := s.x.iter(store.CFIdx, lo, hi, false, false)
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

// idOf returns a copy of ks(_id) from one index entry: the value of a
// unique index, the tail after the k field encodings otherwise.
func (s *idxScan) idOf(k, v []byte) ([]byte, error) {
	if s.ix.Unique {
		return clone(v), nil
	}
	rest := k[4:]
	for _, key := range s.ix.Keys {
		var err error
		if key.Desc {
			rest, err = keyenc.SkipDesc(rest)
		} else {
			rest, err = keyenc.Skip(rest)
		}
		if err != nil {
			return nil, fmt.Errorf("query: index %s entry %x: %w", s.ix.Name, k, err)
		}
	}
	return clone(rest), nil
}

// fill reads index entries until fetchBatch new _ids are waiting or the
// ranges end.
func (s *idxScan) fill() error {
	for len(s.ids) < fetchBatch && !s.done {
		if s.it == nil {
			if err := s.open(); err != nil {
				return err
			}
			continue
		}
		if !s.it.Valid() {
			err := s.it.Err()
			s.it.Close()
			s.it = nil
			if err != nil {
				return s.x.readErr(err)
			}
			s.ri++
			s.last = nil
			continue
		}
		k, v := s.it.Key(), s.it.Value()
		s.x.step(len(k) + len(v))
		s.last = append(s.last[:0], k...)
		id, err := s.idOf(k, v)
		if err != nil {
			return err
		}
		if s.reverse {
			s.it.Prev()
		} else {
			s.it.Next()
		}
		if s.seen != nil {
			if _, dup := s.seen[string(id)]; dup {
				continue
			}
			s.seen[string(id)] = struct{}{}
			if err := s.x.mem.Grow("index scan", int64(len(id))+48); err != nil {
				return err
			}
		}
		s.ids = append(s.ids, id)
	}
	return nil
}

func (s *idxScan) Next() (Row, bool, error) {
	for {
		if s.bi < len(s.buf) {
			r := s.buf[s.bi]
			s.buf[s.bi] = Row{}
			s.bi++
			return r, true, nil
		}
		if err := s.fill(); err != nil {
			return Row{}, false, err
		}
		if len(s.ids) == 0 {
			return Row{}, false, nil
		}
		rows, err := fetchRows(s.x, s.coll, s.ids, s.filter)
		if err != nil {
			return Row{}, false, err
		}
		s.ids = s.ids[:0]
		s.buf, s.bi = rows, 0
	}
}

func (s *idxScan) Suspend() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
}

func (s *idxScan) Close() {
	s.Suspend()
	s.done = true
	s.ids, s.buf = nil, nil
}
