package query

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
)

// geoIndex returns the ready 2dsphere index on field.
func geoIndex(coll *catalog.Collection, field string) *catalog.Index {
	for _, ix := range coll.Indexes() {
		if ix.Kind == catalog.TwoDSphere && ix.State == catalog.Ready && ix.Keys[0].Field == field {
			return ix
		}
	}
	return nil
}

// geoWithinSource plans the first $geoWithin or $geoIntersects predicate
// whose field has a ready 2dsphere index (or the hinted one): the index's
// frozen S2 parameters turn the query region into descendant ranges and
// ancestor probes (geo.Params.PlanRegion). It returns nil when no geo
// predicate can use an index.
func (e *Engine) geoWithinSource(coll *catalog.Collection, preds []doc.Predicate, hinted *catalog.Index, o Options) (*srcPlan, error) {
	for _, p := range preds {
		if p.Op != doc.OpGeoWithin && p.Op != doc.OpGeoIntersects {
			continue
		}
		ix := hinted
		if ix == nil {
			ix = geoIndex(coll, p.Field)
		} else if ix.Keys[0].Field != p.Field {
			continue
		}
		if ix == nil {
			continue
		}
		q, err := parseGeoQuery(p.Op.String(), p.Values[0].Value)
		if err != nil {
			return nil, err
		}
		plan := ix.S2.PlanRegion(q.region(), o.GeoMaxCells)
		id := coll.ID
		return &srcPlan{
			kind:   "index",
			index:  ix.Name,
			bounds: []string{fmt.Sprintf("%s: %s, %d ranges, %d probes", p.Field, p.Op, len(plan.Ranges), len(plan.Probes))},
			est:    e.count(o, coll),
			open: func(x *Exec, f *doc.Matcher) Operator {
				return &geoWithinScan{x: x, coll: id, ix: ix, plan: plan, filter: f}
			},
		}, nil
	}
	return nil, nil
}

// geoWithinScan reads a 2dsphere plan: every descendant range as one
// iterator with TotalOrderSeek (a range crosses the geo CF's 12-byte
// prefix) and every ancestor probe as one prefix read, collecting the
// distinct _ids into a set charged to the Accountant. It sorts them, then
// fetches rows fetchBatch at a time and applies the full filter, whose geo
// hook refines each candidate exactly. Results come out in _id order.
type geoWithinScan struct {
	x      *Exec
	coll   uint32
	ix     *catalog.Index
	plan   geo.Plan
	filter *doc.Matcher

	ids     [][]byte
	pos     int
	ready   bool
	buf     []Row
	bi      int
	charged int64
}

func (s *geoWithinScan) collect() error {
	seen := make(map[string]struct{})
	add := func(ks []byte) error {
		if _, ok := seen[string(ks)]; ok {
			return nil
		}
		seen[string(ks)] = struct{}{}
		c := int64(len(ks)) + 48
		s.charged += c
		return s.x.mem.Grow("$geoWithin", c)
	}
	for _, r := range s.plan.Ranges {
		lo := layout.GeoCellPrefix(nil, s.ix.ID, uint64(r.Lo))
		hi := layout.GeoCellPrefix(nil, s.ix.ID, uint64(r.Hi)+1)
		if err := geoScan(s.x, lo, hi, true, func(_ uint64, ks, _ []byte) (bool, error) { return true, add(ks) }); err != nil {
			return err
		}
	}
	for _, c := range s.plan.Probes {
		lo := layout.GeoCellPrefix(nil, s.ix.ID, uint64(c))
		hi := layout.GeoCellPrefix(nil, s.ix.ID, uint64(c)+1)
		if err := geoScan(s.x, lo, hi, false, func(_ uint64, ks, _ []byte) (bool, error) { return true, add(ks) }); err != nil {
			return err
		}
	}
	s.ids = make([][]byte, 0, len(seen))
	for k := range seen {
		s.ids = append(s.ids, []byte(k))
	}
	slices.SortFunc(s.ids, bytes.Compare)
	return nil
}

// geoScan iterates [lo, hi) of the geo CF and calls visit with each
// entry's cell, ks(_id) and value, which are iterator views. rangeScan
// selects TotalOrderSeek for descendant ranges; otherwise the read is an
// ancestor probe with PrefixSameAsStart.
func geoScan(x *Exec, lo, hi []byte, rangeScan bool, visit func(cell uint64, ks, val []byte) (bool, error)) error {
	it, err := x.iter(store.CFGeo, lo, hi, rangeScan, !rangeScan)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Seek(lo); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		x.step(len(k) + len(v))
		_, cell, ks := layout.SplitGeoKey(k)
		more, err := visit(cell, ks, v)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return x.readErr(it.Err())
}

func (s *geoWithinScan) Next() (Row, bool, error) {
	if !s.ready {
		s.ready = true
		if err := s.collect(); err != nil {
			return Row{}, false, err
		}
	}
	for {
		if s.bi < len(s.buf) {
			r := s.buf[s.bi]
			s.buf[s.bi] = Row{}
			s.bi++
			return r, true, nil
		}
		if s.pos >= len(s.ids) {
			return Row{}, false, nil
		}
		batch := s.ids[s.pos:min(s.pos+fetchBatch, len(s.ids))]
		s.pos += len(batch)
		rows, err := fetchRows(s.x, s.coll, batch, s.filter)
		if err != nil {
			return Row{}, false, err
		}
		s.buf, s.bi = rows, 0
	}
}

func (s *geoWithinScan) Suspend() {}

func (s *geoWithinScan) Close() {
	s.x.mem.Shrink(s.charged)
	s.charged = 0
	s.ids, s.buf, s.ready = nil, nil, true
}
