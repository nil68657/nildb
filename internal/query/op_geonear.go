package query

import (
	"math"
	"strings"

	"github.com/golang/geo/s2"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() { registerStage("$geoNear", parseGeoNearStage) }

// geoNearSpec is a parsed $geoNear stage.
type geoNearSpec struct {
	near      *nearSpec
	distField string
	query     bson.Raw
	mult      float64
	locs      string
	key       string
}

func parseGeoNearStage(arg bson.RawValue) (*stage, error) {
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, exprErr(10065, "$geoNear must take an object, got %s", typeName(arg.Type))
	}
	g := &geoNearSpec{mult: 1}
	var nearV, minV, maxV bson.RawValue
	var hasNear, hasMin, hasMax bool
	spherical := false
	it := iterDoc(arg.Value)
	for e, ok := it.next(); ok; e, ok = it.next() {
		v := e.value()
		switch name := string(e.name); name {
		case "near":
			nearV, hasNear = v, true
		case "distanceField":
			s, ok := strOf(v)
			if !ok || s == "" || strings.HasPrefix(s, "$") {
				return nil, exprErr(16606, "$geoNear requires a 'distanceField' option as a String")
			}
			g.distField = s
		case "spherical":
			spherical = truthy(v)
		case "minDistance":
			minV, hasMin = v, true
		case "maxDistance":
			maxV, hasMax = v, true
		case "query":
			if v.Type != bson.TypeEmbeddedDocument {
				return nil, exprErr(doc.CodeBadValue, "query must be an object")
			}
			g.query = v.Value
		case "distanceMultiplier":
			n, ok := numOf(v)
			if !ok || n.float() < 0 || math.IsNaN(n.float()) {
				return nil, exprErr(doc.CodeBadValue, "distanceMultiplier must be a non-negative number")
			}
			g.mult = n.float()
		case "includeLocs":
			s, ok := strOf(v)
			if !ok || s == "" || strings.HasPrefix(s, "$") {
				return nil, exprErr(doc.CodeBadValue, "includeLocs must be a field name")
			}
			g.locs = s
		case "key":
			s, ok := strOf(v)
			if !ok || s == "" {
				return nil, exprErr(doc.CodeBadValue, "$geoNear parameter 'key' must be a non-empty string")
			}
			g.key = s
		case "limit", "num":
			return nil, exprErr(doc.CodeBadValue, "$geoNear no longer supports the '%s' parameter; use a $limit stage", name)
		default:
			return nil, exprErr(doc.CodeBadValue, "unknown argument to $geoNear: %s", name)
		}
	}
	if !hasNear {
		return nil, exprErr(doc.CodeBadValue, "$geoNear requires a 'near' option")
	}
	if g.distField == "" {
		return nil, exprErr(16606, "$geoNear requires a 'distanceField' option as a String")
	}
	ns := &nearSpec{}
	if _, isGeoJSON := lookup(nearV.Value, "type"); nearV.Type == bson.TypeEmbeddedDocument && isGeoJSON {
		pt, err := parseNear("$near", oneField("$near", docV(oneField("$geometry", nearV))))
		if err != nil {
			return nil, err
		}
		ns.center = pt.center
	} else {
		if !spherical {
			return nil, exprErr(doc.CodeBadValue, "$geoNear with legacy coordinates needs spherical: true (v1 has no 2d index)")
		}
		ll, err := legacyPoint(nearV)
		if err != nil {
			return nil, err
		}
		ns.center, ns.radians = s2.PointFromLatLng(ll), true
	}
	if err := ns.bounds(minV, hasMin, maxV, hasMax, "minDistance", "maxDistance"); err != nil {
		return nil, err
	}
	g.near = ns
	if _, err := doc.Compile(g.query, geoHooks{}); err != nil {
		return nil, err
	}
	return &stage{label: "$geoNear", geoNear: g}, nil
}

// geoNearSource plans a leading $geoNear: the 2dsphere index named by key,
// or the collection's only one, read through nearOp with limit pushed down
// from a following $limit.
func (e *Engine) geoNearSource(coll *catalog.Collection, g *geoNearSpec, limit int64, o Options) (*srcPlan, error) {
	if coll == nil {
		return emptySource(), nil
	}
	var ix *catalog.Index
	if g.key != "" {
		if ix = geoIndex(coll, g.key); ix == nil {
			return nil, exprErr(291, "$geoNear found no 2dsphere index on field '%s'", g.key)
		}
	} else {
		for _, c := range coll.Indexes() {
			if c.Kind != catalog.TwoDSphere || c.State != catalog.Ready {
				continue
			}
			if ix != nil {
				return nil, exprErr(doc.CodeBadValue, "There is more than one 2dsphere index on %s; unsure which to use for $geoNear", coll.NS)
			}
			ix = c
		}
		if ix == nil {
			return nil, exprErr(291, "$geoNear requires a 2d or 2dsphere index, but none were found")
		}
	}
	rest, err := compileRest(g.query)
	if err != nil {
		return nil, err
	}
	id, field := coll.ID, ix.Keys[0].Field
	return &srcPlan{
		kind:   "index",
		index:  ix.Name,
		bounds: []string{g.near.text(field, "$geoNear")},
		est:    e.count(o, coll),
		open: func(x *Exec, _ *doc.Matcher) Operator {
			op := &geoNearOp{in: &nearOp{x: x, coll: id, ix: ix, ns: g.near, rest: rest, limit: limit, field: field},
				spec: g, field: field, dist: newFieldSetter(g.distField)}
			if g.locs != "" {
				op.locs = newFieldSetter(g.locs)
			}
			return op
		},
	}, nil
}

// geoNearOp writes distanceField (the distance times distanceMultiplier)
// and includeLocs (the indexed location) into the rows nearOp returns.
type geoNearOp struct {
	in    Operator
	spec  *geoNearSpec
	field string
	dist  *fieldSetter
	locs  *fieldSetter
}

func (o *geoNearOp) Next() (Row, bool, error) {
	r, ok, err := o.in.Next()
	if err != nil || !ok {
		return r, ok, err
	}
	loc, hasLoc := doc.Lookup(r.Doc, o.field)
	d := o.dist.set(r.Doc, f64(r.Dist*o.spec.mult))
	if o.locs != nil && hasLoc {
		d = o.locs.set(d, bson.RawValue{Type: loc.Type, Value: clone(loc.Value)})
	}
	r.Doc = d
	return r, true, nil
}

func (o *geoNearOp) Suspend() { o.in.Suspend() }
func (o *geoNearOp) Close()   { o.in.Close() }
