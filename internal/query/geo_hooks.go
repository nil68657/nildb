package query

import (
	"fmt"
	"math"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	errNearContext = &doc.Error{Code: doc.CodeBadValue, Msg: "$geoNear, $near, and $nearSphere are not allowed in this context"}
	errNoGeoIndex  = &doc.Error{Code: 291, Msg: "unable to find index for $geoNear query"}
)

func geoErr(format string, a ...any) error {
	return &doc.Error{Code: doc.CodeBadValue, Msg: fmt.Sprintf(format, a...)}
}

// geoHooks implements doc.GeoHooks with internal/geo, so a filter's geo
// operators evaluate on any row: $geoWithin and $geoIntersects parse the
// document's field and refine exactly (geo.Refine, geo.WithinCap), which
// is how a rowscan answers them and how an index plan checks its
// candidates. $near and $nearSphere check their distance bounds; near is
// false where MongoDB refuses them (an aggregation $match, a count).
//
// A compiled hook holds a parsed query geometry, which golang/geo indexes
// lazily and so must not be shared across goroutines; every run compiles
// its own filter.
type geoHooks struct{ near bool }

func (h geoHooks) Compile(field, op string, spec bson.Raw) (func(bson.Raw) (bool, error), error) {
	if op == "$near" || op == "$nearSphere" {
		if !h.near {
			return nil, errNearContext
		}
		ns, err := parseNear(op, spec)
		if err != nil {
			return nil, err
		}
		return func(d bson.Raw) (bool, error) {
			g := fieldGeometry(d, field)
			if g == nil {
				return false, nil
			}
			dist := geo.Distance(ns.center, g)
			return dist >= ns.min && (ns.max <= 0 || dist <= ns.max), nil
		}, nil
	}
	q, err := parseGeoQuery(op, spec)
	if err != nil {
		return nil, err
	}
	return func(d bson.Raw) (bool, error) {
		g := fieldGeometry(d, field)
		return g != nil && q.match(g), nil
	}, nil
}

// fieldGeometry parses the geometry at path the way a 2dsphere index reads
// it (a strict path; GeoJSON or a legacy pair), or returns nil when the
// field is absent or holds no valid geometry.
func fieldGeometry(d bson.Raw, path string) *geo.Geometry {
	v, ok := doc.Lookup(d, path)
	if !ok || geo.Absent(v) {
		return nil
	}
	g, err := geo.Parse(v, geo.ParseOpts{AllowLegacyPair: true})
	if err != nil {
		return nil
	}
	return g
}

// geoQuery is a parsed $geoWithin or $geoIntersects argument.
type geoQuery struct {
	op    string
	g     *geo.Geometry // $geometry
	cap   s2.Cap        // $centerSphere
	isCap bool
}

func parseGeoQuery(op string, spec bson.Raw) (*geoQuery, error) {
	arg, _ := lookup(spec, op)
	if arg.Type != bson.TypeEmbeddedDocument {
		return nil, geoErr("%s needs an object holding $geometry", op)
	}
	it := iterDoc(arg.Value)
	e, ok := it.next()
	if !ok {
		return nil, geoErr("%s needs a $geometry", op)
	}
	if _, more := it.next(); more {
		return nil, geoErr("%s takes exactly one shape", op)
	}
	switch name := string(e.name); name {
	case "$geometry":
		g, err := geo.Parse(e.value(), geo.ParseOpts{})
		if err != nil {
			return nil, geoErr("%s", err.Error())
		}
		if op == "$geoWithin" && g.Kind != geo.Polygon && g.Kind != geo.MultiPolygon {
			return nil, geoErr("$geoWithin not supported with provided geometry: %s", g.Kind)
		}
		return &geoQuery{op: op, g: g}, nil
	case "$centerSphere":
		if op != "$geoWithin" {
			return nil, geoErr("$centerSphere is only valid with $geoWithin")
		}
		c, err := parseCenterSphere(e.value())
		if err != nil {
			return nil, err
		}
		return &geoQuery{op: op, cap: c, isCap: true}, nil
	case "$box", "$polygon", "$center":
		return nil, geoErr("$geoWithin legacy shapes require a 2d index (not supported)")
	default:
		return nil, geoErr("unknown geo specifier: %s", name)
	}
}

// parseCenterSphere parses [[lng, lat], radius], the radius in radians.
func parseCenterSphere(v bson.RawValue) (s2.Cap, error) {
	bad := geoErr("$centerSphere needs [[lng, lat], radius in radians]")
	if v.Type != bson.TypeArray {
		return s2.Cap{}, bad
	}
	parts := arrayValues(v.Value)
	if len(parts) != 2 {
		return s2.Cap{}, bad
	}
	g, err := geo.Parse(parts[0], geo.ParseOpts{AllowLegacyPair: true})
	if err != nil {
		return s2.Cap{}, geoErr("%s", err.Error())
	}
	ll, ok := g.LatLng()
	r, isNum := numOf(parts[1])
	if !ok || !isNum || r.float() < 0 || math.IsNaN(r.float()) {
		return s2.Cap{}, bad
	}
	return s2.CapFromCenterAngle(s2.PointFromLatLng(ll), s1.Angle(r.float())), nil
}

func (q *geoQuery) region() s2.Region {
	if q.isCap {
		return q.cap
	}
	return q.g.Region()
}

func (q *geoQuery) match(d *geo.Geometry) bool {
	switch {
	case q.isCap:
		return geo.WithinCap(q.cap, d)
	case q.op == "$geoWithin":
		return geo.Refine(geo.Within, q.g, d)
	}
	return geo.Refine(geo.Intersects, q.g, d)
}

// nearSpec is a parsed $near, $nearSphere or $geoNear center with its
// distance bounds.
type nearSpec struct {
	center  s2.Point
	radians bool     // a legacy coordinate pair: distances in radians, not metres
	min     s1.Angle // inclusive
	max     s1.Angle // inclusive; 0 means unbounded
}

// parseNear parses the spec GeoHooks receive for $near and $nearSphere:
// {op: point, $minDistance, $maxDistance}, where point is {$geometry:
// GeoJSON Point, $minDistance, $maxDistance} (metres) or a legacy pair
// (radians, $nearSphere only: legacy $near needs a 2d index).
func parseNear(op string, spec bson.Raw) (*nearSpec, error) {
	arg, _ := lookup(spec, op)
	minV, hasMin := lookup(spec, "$minDistance")
	maxV, hasMax := lookup(spec, "$maxDistance")
	ns := &nearSpec{}
	geometry, isGeoJSON := bson.RawValue{}, false
	if arg.Type == bson.TypeEmbeddedDocument {
		if g, ok := lookup(arg.Value, "$geometry"); ok {
			geometry, isGeoJSON = g, true
			if v, ok := lookup(arg.Value, "$minDistance"); ok {
				minV, hasMin = v, true
			}
			if v, ok := lookup(arg.Value, "$maxDistance"); ok {
				maxV, hasMax = v, true
			}
		} else if _, ok := lookup(arg.Value, "type"); ok {
			geometry, isGeoJSON = arg, true
		}
	}
	if isGeoJSON {
		g, err := geo.Parse(geometry, geo.ParseOpts{})
		if err != nil {
			return nil, geoErr("%s", err.Error())
		}
		ll, ok := g.LatLng()
		if !ok {
			return nil, geoErr("%s requires a GeoJSON Point, got %s", op, g.Kind)
		}
		ns.center = s2.PointFromLatLng(ll)
	} else {
		if arg.Type != bson.TypeArray && arg.Type != bson.TypeEmbeddedDocument {
			return nil, geoErr("%s needs a point", op)
		}
		if op == "$near" {
			return nil, geoErr("$near with legacy coordinates needs a 2d index, which v1 does not have; use $nearSphere or a GeoJSON point")
		}
		ll, err := legacyPoint(arg)
		if err != nil {
			return nil, err
		}
		ns.center, ns.radians = s2.PointFromLatLng(ll), true
	}
	if err := ns.bounds(minV, hasMin, maxV, hasMax, "$minDistance", "$maxDistance"); err != nil {
		return nil, err
	}
	return ns, nil
}

func legacyPoint(v bson.RawValue) (s2.LatLng, error) {
	g, err := geo.Parse(v, geo.ParseOpts{AllowLegacyPair: true})
	if err != nil {
		return s2.LatLng{}, geoErr("%s", err.Error())
	}
	ll, ok := g.LatLng()
	if !ok {
		return s2.LatLng{}, geoErr("near needs a point, got %s", g.Kind)
	}
	return ll, nil
}

// bounds converts the distance bounds to angles: metres through
// EarthRadiusMetres, or radians for a legacy pair. A $maxDistance of 0
// keeps only exact hits, which geo.Near spells as the smallest angle.
func (ns *nearSpec) bounds(minV bson.RawValue, hasMin bool, maxV bson.RawValue, hasMax bool, minName, maxName string) error {
	conv := func(v bson.RawValue, name string) (s1.Angle, error) {
		n, ok := numOf(v)
		if !ok {
			return 0, geoErr("%s must be a number", name)
		}
		f := n.float()
		if f < 0 || math.IsNaN(f) {
			return 0, geoErr("%s must be non-negative", name)
		}
		if ns.radians {
			return s1.Angle(f), nil
		}
		return geo.Angle(f), nil
	}
	var err error
	if hasMin {
		if ns.min, err = conv(minV, minName); err != nil {
			return err
		}
	}
	if hasMax {
		if ns.max, err = conv(maxV, maxName); err != nil {
			return err
		}
		if ns.max == 0 {
			ns.max = s1.Angle(math.SmallestNonzeroFloat64)
		}
	}
	return nil
}

// distance converts an angle to the unit the query used.
func (ns *nearSpec) distance(a s1.Angle) float64 {
	if ns.radians {
		return a.Radians()
	}
	return geo.Metres(a)
}

func (ns *nearSpec) text(field, op string) string {
	ll := s2.LatLngFromPoint(ns.center)
	unit, lo, hi := "m", ns.distance(ns.min), "inf"
	if ns.radians {
		unit = "rad"
	}
	if ns.max > 0 {
		hi = fmt.Sprintf("%g", ns.distance(ns.max))
	}
	return fmt.Sprintf("%s: %s [%g, %g] within [%g, %s] %s", field, op, ll.Lng.Degrees(), ll.Lat.Degrees(), lo, hi, unit)
}
