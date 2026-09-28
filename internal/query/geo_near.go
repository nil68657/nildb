package query

import (
	"strconv"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// nearFindSource plans a find whose filter holds $near or $nearSphere: the
// field needs a ready 2dsphere index, rows come out by distance, and the
// rest of the filter (the filter without the near clause) is applied to
// each candidate inside the distance bounds.
func (e *Engine) nearFindSource(coll *catalog.Collection, filter bson.Raw, p doc.Predicate, limit int64, o Options) (*srcPlan, error) {
	op := p.Op.String()
	ns, err := parseNear(op, p.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if coll == nil {
		return emptySource(), nil
	}
	ix := geoIndex(coll, p.Field)
	if ix == nil {
		return nil, errNoGeoIndex
	}
	rest, err := compileRest(withoutNear(filter, p.Field))
	if err != nil {
		return nil, err
	}
	id, field := coll.ID, p.Field
	return &srcPlan{
		kind:   "index",
		index:  ix.Name,
		bounds: []string{ns.text(field, op)},
		est:    e.count(o, coll),
		open: func(x *Exec, _ *doc.Matcher) Operator {
			return &nearOp{x: x, coll: id, ix: ix, ns: ns, rest: rest, limit: limit, field: field}
		},
	}, nil
}

// Matcher compiles a filter with the geo hooks, so $geoWithin and
// $geoIntersects evaluate on any row; $near and $nearSphere are refused,
// as outside a find. DOC writes use it to check a document they read by
// _id.
func Matcher(filter bson.Raw) (*doc.Matcher, error) { return doc.Compile(filter, geoHooks{}) }

// compileRest compiles the filter a near source applies to its candidates;
// nil when it matches everything.
func compileRest(filter bson.Raw) (*doc.Matcher, error) {
	m, err := doc.Compile(filter, geoHooks{})
	if err != nil {
		return nil, err
	}
	if m.MatchAll() {
		return nil, nil
	}
	return m, nil
}

// withoutNear returns filter without the $near or $nearSphere clause on
// field (with its $minDistance and $maxDistance), at the top level or
// inside top-level $and, where doc allows the near operators.
func withoutNear(filter bson.Raw, field string) bson.Raw {
	var w builder
	w.begin()
	copyWithoutNear(&w, filter, field)
	return w.end()
}

func copyWithoutNear(w *builder, d []byte, field string) {
	it := iterDoc(d)
	for e, ok := it.next(); ok; e, ok = it.next() {
		name := string(e.name)
		switch {
		case name == field && e.typ == bson.TypeEmbeddedDocument && hasNear(e.val):
			var sub builder
			sub.begin()
			n := 0
			fit := iterDoc(e.val)
			for f, ok := fit.next(); ok; f, ok = fit.next() {
				switch string(f.name) {
				case "$near", "$nearSphere", "$minDistance", "$maxDistance":
					continue
				}
				sub.rawElem(f.raw)
				n++
			}
			if n > 0 {
				w.elem(name, docV(sub.end()))
			}
		case name == "$and" && e.typ == bson.TypeArray:
			var arr builder
			arr.begin()
			n := 0
			for _, v := range arrayValues(e.val) {
				if v.Type == bson.TypeEmbeddedDocument {
					var part builder
					part.begin()
					copyWithoutNear(&part, v.Value, field)
					if out := part.end(); !emptyDoc(out) {
						arr.elem(strconv.Itoa(n), docV(out))
						n++
					}
					continue
				}
				arr.elem(strconv.Itoa(n), v)
				n++
			}
			if n > 0 {
				w.elem(name, bson.RawValue{Type: bson.TypeArray, Value: arr.end()})
			}
		default:
			w.rawElem(e.raw)
		}
	}
}

func hasNear(ops []byte) bool {
	_, a := lookup(ops, "$near")
	_, b := lookup(ops, "$nearSphere")
	return a || b
}

// nearOp runs geo.Near, MongoDB's expanding-annulus kNN search, over the
// geo column family: descendant ranges with TotalOrderSeek, ancestor
// probes with PrefixSameAsStart, point distances from the index value and
// shape distances from the row. The first Next runs the whole search and
// keeps each hit's _id and distance (charged to the Accountant); rows are
// then read fetchBatch at a time, re-checked against the rest of the
// filter, and returned in ascending distance with Row.Dist set.
type nearOp struct {
	x     *Exec
	coll  uint32
	ix    *catalog.Index
	ns    *nearSpec
	rest  *doc.Matcher
	limit int64
	field string

	hits    []nearHit
	pos     int
	ran     bool
	buf     []Row
	bi      int
	charged int64
	err     error
}

type nearHit struct {
	id []byte
	d  float64
}

func (o *nearOp) run() error {
	opts := geo.NearOpts{Min: o.ns.min, Max: o.ns.max, Limit: int(o.limit), QueryMaxCells: o.x.geoCells}
	if o.rest != nil {
		opts.Filter = func(ks []byte) bool {
			if o.err != nil {
				return false
			}
			row, ok, err := o.x.get(store.CFDoc, layout.DocKey(nil, o.coll, ks))
			if err == nil && ok {
				err = o.x.row(len(row))
			}
			if err != nil {
				o.err = err
				return false
			}
			if !ok {
				return false
			}
			match, err := o.rest.Match(row)
			if err != nil {
				o.err = err
			}
			return match
		}
	}
	scan := func(r geo.Range, visit func(cell s2.CellID, ksID, val []byte) bool) error {
		lo := layout.GeoCellPrefix(nil, o.ix.ID, uint64(r.Lo))
		hi := layout.GeoCellPrefix(nil, o.ix.ID, uint64(r.Hi)+1)
		return geoScan(o.x, lo, hi, true, func(cell uint64, ks, val []byte) (bool, error) {
			return visit(s2.CellID(cell), ks, val), nil
		})
	}
	probe := func(c s2.CellID, visit func(ksID, val []byte) bool) error {
		lo := layout.GeoCellPrefix(nil, o.ix.ID, uint64(c))
		hi := layout.GeoCellPrefix(nil, o.ix.ID, uint64(c)+1)
		return geoScan(o.x, lo, hi, false, func(_ uint64, ks, val []byte) (bool, error) {
			return visit(ks, val), nil
		})
	}
	load := func(ks []byte) (*geo.Geometry, error) {
		row, ok, err := o.x.get(store.CFDoc, layout.DocKey(nil, o.coll, ks))
		if err != nil || !ok {
			return nil, err
		}
		if err := o.x.row(len(row)); err != nil {
			return nil, err
		}
		return fieldGeometry(row, o.field), nil
	}
	emit := func(ks []byte, d s1.Angle) bool {
		h := nearHit{id: clone(ks), d: o.ns.distance(d)}
		o.hits = append(o.hits, h)
		c := int64(len(h.id)) + 40
		o.charged += c
		if err := o.x.mem.Grow("$near", c); err != nil {
			o.err = err
			return false
		}
		return true
	}
	err := geo.Near(o.ix.S2, o.ns.center, opts, scan, probe, load, emit)
	if err == nil {
		err = o.err
	}
	return err
}

func (o *nearOp) Next() (Row, bool, error) {
	if !o.ran {
		o.ran = true
		if err := o.run(); err != nil {
			return Row{}, false, err
		}
	}
	for {
		if o.bi < len(o.buf) {
			r := o.buf[o.bi]
			o.buf[o.bi] = Row{}
			o.bi++
			return r, true, nil
		}
		if o.pos >= len(o.hits) {
			return Row{}, false, nil
		}
		batch := o.hits[o.pos:min(o.pos+fetchBatch, len(o.hits))]
		o.pos += len(batch)
		if err := o.fetch(batch); err != nil {
			return Row{}, false, err
		}
	}
}

func (o *nearOp) fetch(batch []nearHit) error {
	keys := make([][]byte, len(batch))
	for i, h := range batch {
		keys[i] = layout.DocKey(nil, o.coll, h.id)
	}
	vals, err := o.x.multiGet(store.CFDoc, keys)
	if err != nil {
		return err
	}
	o.buf, o.bi = o.buf[:0], 0
	for i, v := range vals {
		if v == nil {
			continue
		}
		if err := o.x.row(len(v)); err != nil {
			return err
		}
		if o.rest != nil {
			ok, err := o.rest.Match(v)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
		}
		o.buf = append(o.buf, Row{Doc: v, ID: batch[i].id, Dist: batch[i].d})
	}
	return nil
}

func (o *nearOp) Suspend() {}

func (o *nearOp) Close() {
	o.x.mem.Shrink(o.charged)
	o.charged = 0
	o.hits, o.buf, o.ran = nil, nil, true
	o.pos = 0
}
