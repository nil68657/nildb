package query

import (
	"bytes"
	"container/heap"
	"slices"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
)

// sortItemOverhead approximates the bookkeeping bytes of one held row.
const sortItemOverhead = 64

// sortOp sorts its input by doc.SortKey with ks(_id) appended, so equal
// keys order by _id and the output is deterministic; rows with the same
// key and _id (from $unwind) keep their input order. With topK > 0 it
// keeps only the first topK rows in a bounded max-heap, which is what
// $sort followed by $limit (and $skip) needs; otherwise it holds every row.
// Either way every held byte is charged to the run's Accountant.
type sortOp struct {
	x    *Exec
	in   Operator
	spec doc.SortSpec
	topK int64

	items   []sortItem
	pos     int
	ready   bool
	charged int64
}

type sortItem struct {
	key []byte
	seq uint64
	row Row
}

func newSortOp(x *Exec, in Operator, spec doc.SortSpec, topK int64) *sortOp {
	return &sortOp{x: x, in: in, spec: spec, topK: topK}
}

func itemLess(a, b *sortItem) bool {
	if c := bytes.Compare(a.key, b.key); c != 0 {
		return c < 0
	}
	return a.seq < b.seq
}

// sortKey is doc.SortKey plus ks(_id): the row's source _id, or the _id
// field of a document an earlier stage built.
func sortKey(spec doc.SortSpec, r Row) []byte {
	k := doc.SortKey(nil, r.Doc, spec)
	if r.ID != nil {
		return append(k, r.ID...)
	}
	id, _ := lookup(r.Doc, "_id")
	if enc, err := keyenc.Encode(nil, id); err == nil {
		return append(k, enc...)
	}
	return keyenc.EncodeMissing(k)
}

func (s *sortOp) cost(it *sortItem) int64 {
	return int64(len(it.key)+len(it.row.Doc)+len(it.row.ID)) + sortItemOverhead
}

func (s *sortOp) fill() error {
	defer func() { s.in.Close() }()
	var seq uint64
	h := (*maxHeap)(&s.items)
	for {
		r, ok, err := s.in.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if seq++; seq&1023 == 0 {
			if err := s.x.checkDeadline(); err != nil {
				return err
			}
		}
		it := sortItem{key: sortKey(s.spec, r), seq: seq, row: r}
		if s.topK > 0 && int64(len(s.items)) >= s.topK {
			if !itemLess(&it, &s.items[0]) {
				continue
			}
			old := s.cost(&s.items[0])
			s.items[0] = it
			heap.Fix(h, 0)
			s.x.mem.Shrink(old)
			s.charged -= old
		} else if s.topK > 0 {
			heap.Push(h, it)
		} else {
			s.items = append(s.items, it)
		}
		c := s.cost(&it)
		s.charged += c
		if err := s.x.mem.Grow("$sort", c); err != nil {
			return err
		}
	}
	slices.SortFunc(s.items, func(a, b sortItem) int {
		if itemLess(&a, &b) {
			return -1
		}
		if itemLess(&b, &a) {
			return 1
		}
		return 0
	})
	return nil
}

func (s *sortOp) Next() (Row, bool, error) {
	if !s.ready {
		s.ready = true
		if err := s.fill(); err != nil {
			return Row{}, false, err
		}
	}
	if s.pos >= len(s.items) {
		s.release()
		return Row{}, false, nil
	}
	it := &s.items[s.pos]
	s.pos++
	r := it.row
	c := s.cost(it)
	s.x.mem.Shrink(c)
	s.charged -= c
	*it = sortItem{}
	return r, true, nil
}

func (s *sortOp) release() {
	s.x.mem.Shrink(s.charged)
	s.charged = 0
	s.items = nil
}

func (s *sortOp) Suspend() {
	if !s.ready {
		s.in.Suspend()
	}
}

func (s *sortOp) Close() {
	s.in.Close()
	s.release()
	s.ready = true
}

// maxHeap keeps the largest held item on top, the one a smaller newcomer
// replaces.
type maxHeap []sortItem

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return itemLess(&h[j], &h[i]) }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(sortItem)) }
func (h *maxHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}
