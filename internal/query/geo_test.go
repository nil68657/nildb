package query

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func geoPoint(lng, lat float64) bson.D {
	return bson.D{{Key: "type", Value: "Point"}, {Key: "coordinates", Value: bson.A{lng, lat}}}
}

func square(x, y, s float64) bson.A {
	return bson.A{bson.A{x - s, y - s}, bson.A{x + s, y - s}, bson.A{x + s, y + s}, bson.A{x - s, y + s}, bson.A{x - s, y - s}}
}

// geoFixture is 400 points, 40 squares (every third with a square hole),
// 25 polylines, 10 legacy pairs and one document whose loc is null.
func geoFixture(t *testing.T) []bson.Raw {
	r := rand.New(rand.NewPCG(7, 8))
	var out []bson.Raw
	add := func(loc any, kind string) {
		id := int32(len(out))
		out = append(out, mustMarshal(t, bson.D{{Key: "_id", Value: id}, {Key: "loc", Value: loc}, {Key: "kind", Value: kind}, {Key: "w", Value: id % 5}}))
	}
	for range 400 {
		add(geoPoint(-10+r.Float64()*20, 40+r.Float64()*15), "pt")
	}
	for i := range 40 {
		x, y, s := -9+r.Float64()*18, 41+r.Float64()*13, 0.2+r.Float64()*1.2
		rings := bson.A{square(x, y, s)}
		if i%3 == 0 {
			rings = append(rings, square(x, y, s/3))
		}
		add(bson.D{{Key: "type", Value: "Polygon"}, {Key: "coordinates", Value: rings}}, "poly")
	}
	for range 25 {
		x, y := -9+r.Float64()*18, 41+r.Float64()*13
		add(bson.D{{Key: "type", Value: "LineString"}, {Key: "coordinates", Value: bson.A{bson.A{x, y}, bson.A{x + 0.3, y + 0.2}, bson.A{x + 0.1, y + 0.6}}}}, "line")
	}
	for range 10 {
		add(bson.A{-10 + r.Float64()*20, 40 + r.Float64()*15}, "legacy")
	}
	add(nil, "none")
	return out
}

func geoEnv(t *testing.T) (*tenv, *catalog.Collection, []bson.Raw) {
	e := newEnv(t)
	c := e.coll("test.geo")
	e.index(c, catalog.Index{Kind: catalog.TwoDSphere, Keys: []catalog.Key{{Field: "loc"}}})
	docs := geoFixture(t)
	e.insert(c, docs...)
	return e, c, docs
}

func docIDs(docs []bson.Raw) []int32 {
	out := make([]int32, len(docs))
	for i, d := range docs {
		out[i] = d.Lookup("_id").Int32()
	}
	return out
}

// bruteGeo evaluates a $geoWithin or $geoIntersects predicate on every
// document with geo.Refine or geo.WithinCap.
func bruteGeo(t *testing.T, docs []bson.Raw, match func(g *geo.Geometry) bool, extra func(bson.Raw) bool) []int32 {
	var out []int32
	for _, d := range docs {
		v, ok := doc.Lookup(d, "loc")
		if !ok || geo.Absent(v) {
			continue
		}
		g, err := geo.Parse(v, geo.ParseOpts{AllowLegacyPair: true})
		if err != nil {
			t.Fatalf("fixture geometry: %v", err)
		}
		if match(g) && (extra == nil || extra(d)) {
			out = append(out, d.Lookup("_id").Int32())
		}
	}
	slices.Sort(out)
	return out
}

type nearHitT struct {
	id int32
	ks []byte
	d  s1.Angle
}

// bruteNear sorts every document within [lo, hi] (hi 0 = unbounded) of
// center by (geo.Distance, ks(_id)).
func bruteNear(t *testing.T, docs []bson.Raw, center s2.Point, lo, hi s1.Angle, extra func(bson.Raw) bool, limit int) []nearHitT {
	var hits []nearHitT
	for _, d := range docs {
		v, ok := doc.Lookup(d, "loc")
		if !ok || geo.Absent(v) {
			continue
		}
		g, err := geo.Parse(v, geo.ParseOpts{AllowLegacyPair: true})
		if err != nil {
			t.Fatal(err)
		}
		dist := geo.Distance(center, g)
		if dist < lo || (hi > 0 && dist > hi) || (extra != nil && !extra(d)) {
			continue
		}
		hits = append(hits, nearHitT{d.Lookup("_id").Int32(), idKey(d), dist})
	}
	slices.SortFunc(hits, func(a, b nearHitT) int {
		if a.d != b.d {
			if a.d < b.d {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.ks, b.ks)
	})
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

func mustGeo(t *testing.T, s string) *geo.Geometry {
	g, err := geo.Parse(ej(t, `{"g":`+s+`}`).Lookup("g"), geo.ParseOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func ll(lng, lat float64) s2.Point { return s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng)) }

// ringCenter returns the centre of a fixture square, from its first and
// third vertices.
func ringCenter(t *testing.T, d bson.Raw) [2]float64 {
	rings, err := d.Lookup("loc", "coordinates").Array().Values()
	if err != nil {
		t.Fatal(err)
	}
	pts, _ := rings[0].Array().Values()
	p0, _ := pts[0].Array().Values()
	p2, _ := pts[2].Array().Values()
	return [2]float64{(p0[0].Double() + p2[0].Double()) / 2, (p0[1].Double() + p2[1].Double()) / 2}
}

// runGeoChecks compares every geo query with the oracle. first is false
// after documents were deleted, when the two point probes may hit nothing.
func runGeoChecks(t *testing.T, e *tenv, c *catalog.Collection, fixture []bson.Raw, first bool) {
	docs := e.all(c)
	big := `{"type": "Polygon", "coordinates": [[[-5, 45], [5, 45], [5, 50], [-5, 50], [-5, 45]]]}`
	mid := `{"type": "Polygon", "coordinates": [[[-2, 44], [3, 44], [3, 49], [-2, 49], [-2, 44]]]}`
	multi := `{"type": "MultiPolygon", "coordinates": [[[[-9, 41], [-6, 41], [-6, 44], [-9, 44], [-9, 41]]], [[[6, 50], [9, 50], [9, 53], [6, 53], [6, 50]]]]}`
	line := `{"type": "LineString", "coordinates": [[-10, 40], [10, 55]]}`
	all := `{"type": "Polygon", "coordinates": [[[-11, 39], [11, 39], [11, 56], [-11, 56], [-11, 39]]]}`
	// A point inside polygon 401 (no hole) and one inside the hole of 400.
	pt := func(p [2]float64) string {
		return fmt.Sprintf(`{"type": "Point", "coordinates": [%v, %v]}`, p[0], p[1])
	}
	inPoly, inHole := pt(ringCenter(t, fixture[401])), pt(ringCenter(t, fixture[400]))
	sphere := s2.CapFromCenterAngle(ll(0, 47), s1.Angle(0.05))
	isPoly := func(d bson.Raw) bool { return d.Lookup("kind").StringValue() == "poly" }
	refine := func(pred geo.Predicate, q string) func(*geo.Geometry) bool {
		qg := mustGeo(t, q)
		return func(g *geo.Geometry) bool { return geo.Refine(pred, qg, g) }
	}
	within := []struct {
		filter string
		match  func(g *geo.Geometry) bool
		extra  func(bson.Raw) bool
	}{
		{`{"loc": {"$geoWithin": {"$geometry": ` + big + `}}}`, refine(geo.Within, big), nil},
		{`{"loc": {"$geoWithin": {"$centerSphere": [[0, 47], 0.05]}}}`, func(g *geo.Geometry) bool { return geo.WithinCap(sphere, g) }, nil},
		{`{"loc": {"$geoWithin": {"$geometry": ` + multi + `}}}`, refine(geo.Within, multi), nil},
		{`{"loc": {"$geoIntersects": {"$geometry": ` + mid + `}}}`, refine(geo.Intersects, mid), nil},
		{`{"loc": {"$geoIntersects": {"$geometry": ` + line + `}}}`, refine(geo.Intersects, line), nil},
		{`{"loc": {"$geoIntersects": {"$geometry": ` + inPoly + `}}}`, refine(geo.Intersects, inPoly), nil},
		{`{"loc": {"$geoIntersects": {"$geometry": ` + inHole + `}}}`, refine(geo.Intersects, inHole), nil},
		{`{"kind": "poly", "loc": {"$geoWithin": {"$geometry": ` + all + `}}}`, refine(geo.Within, all), isPoly},
	}
	for i, tc := range within {
		want := bruteGeo(t, docs, tc.match, tc.extra)
		if len(want) == 0 && i != 6 && (first || i != 5) {
			t.Fatalf("%s: the oracle matches nothing, so the case checks nothing", tc.filter)
		}
		p, got := e.find(c, Find{Filter: ej(t, tc.filter)}, Options{})
		if p.Source != "index" || p.Index != "loc_2dsphere" {
			t.Errorf("%s: plan %s %s, want the 2dsphere index", tc.filter, p.Source, p.Index)
		}
		g := docIDs(got)
		slices.Sort(g)
		if !slices.Equal(g, want) {
			t.Fatalf("%s: index plan %d ids %v\nwant %d %v", tc.filter, len(g), g, len(want), want)
		}
		_, scanned := e.find(c, Find{Filter: ej(t, tc.filter), Hint: "_id_"}, Options{})
		s := docIDs(scanned)
		slices.Sort(s)
		if !slices.Equal(s, want) {
			t.Fatalf("%s: rowscan %v\nwant %v", tc.filter, s, want)
		}
	}
	if len(bruteGeo(t, fixture, refine(geo.Intersects, inPoly), nil)) == 0 {
		t.Fatalf("fixture: the point in polygon 401 intersects nothing")
	}

	metres := func(m float64) s1.Angle { return geo.Angle(m) }
	nears := []struct {
		filter string
		limit  int64
		center s2.Point
		lo, hi s1.Angle
		extra  func(bson.Raw) bool
	}{
		{`{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [1, 47]}, "$maxDistance": 200000}}}`, 0, ll(1, 47), 0, metres(200000), nil},
		{`{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [1, 47]}, "$minDistance": 50000, "$maxDistance": 300000}}}`, 0, ll(1, 47), metres(50000), metres(300000), nil},
		{`{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [-3, 50]}}}}`, 25, ll(-3, 50), 0, 0, nil},
		{`{"loc": {"$nearSphere": [2, 48], "$maxDistance": 0.02}}`, 0, ll(2, 48), 0, s1.Angle(0.02), nil},
		{`{"w": 1, "loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 45]}, "$maxDistance": 400000}}}`, 0, ll(0, 45), 0, metres(400000),
			func(d bson.Raw) bool { return d.Lookup("w").Int32() == 1 }},
	}
	for _, tc := range nears {
		want := bruteNear(t, docs, tc.center, tc.lo, tc.hi, tc.extra, int(tc.limit))
		if len(want) < 3 {
			t.Fatalf("%s: the oracle finds %d hits, too few to check the order", tc.filter, len(want))
		}
		p, got := e.find(c, Find{Filter: ej(t, tc.filter), Limit: tc.limit}, Options{})
		if p.Index != "loc_2dsphere" {
			t.Errorf("%s: plan index %q", tc.filter, p.Index)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d hits, want %d", tc.filter, len(got), len(want))
		}
		for i := range want {
			if got[i].Lookup("_id").Int32() != want[i].id {
				t.Fatalf("%s: hit %d = %d, want %d", tc.filter, i, got[i].Lookup("_id").Int32(), want[i].id)
			}
		}
	}
	if _, err := e.q.PlanFind(c, Find{Filter: ej(t, nears[0].filter), Sort: ej(t, `{"w": 1}`)}, Options{}); err == nil {
		t.Errorf("$near with SORT planned")
	}

	// $geoNear with a dotted distanceField, a query, includeLocs and a
	// multiplier that turns metres into kilometres.
	wantNear := bruteNear(t, docs, ll(1, 47), 0, metres(300000), func(d bson.Raw) bool { return d.Lookup("w").Int32() >= 2 }, 30)
	_, got, err := e.aggregate(c, `[{"$geoNear": {"near": {"type": "Point", "coordinates": [1, 47]}, "distanceField": "calc.dist", "maxDistance": 300000,
		"query": {"w": {"$gte": 2}}, "includeLocs": "where", "distanceMultiplier": 0.001, "spherical": true}}, {"$limit": 30}]`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(wantNear) || len(wantNear) < 3 {
		t.Fatalf("$geoNear: %d hits, want %d (at least 3)", len(got), len(wantNear))
	}
	for i, h := range wantNear {
		d := got[i]
		if d.Lookup("_id").Int32() != h.id {
			t.Fatalf("$geoNear hit %d = %s, want _id %d", i, ejsonOf(d), h.id)
		}
		dist := d.Lookup("calc", "dist").Double()
		if w := geo.Metres(h.d) * 0.001; math.Abs(dist-w) > 1e-9*math.Max(1, w) {
			t.Fatalf("$geoNear hit %d distance %v, want %v", i, dist, w)
		}
		if loc, _ := doc.Lookup(d, "where"); !bytes.Equal(loc.Value, d.Lookup("loc").Value) {
			t.Fatalf("$geoNear hit %d includeLocs = %v", i, loc)
		}
	}
	// A legacy pair: distances in radians.
	wantRad := bruteNear(t, docs, ll(2, 48), 0, s1.Angle(0.03), nil, 0)
	_, got, err = e.aggregate(c, `[{"$geoNear": {"near": [2, 48], "distanceField": "d", "maxDistance": 0.03, "spherical": true}}]`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(wantRad) || len(wantRad) < 3 {
		t.Fatalf("legacy $geoNear: %d hits, want %d (at least 3)", len(got), len(wantRad))
	}
	for i, h := range wantRad {
		if got[i].Lookup("_id").Int32() != h.id || math.Abs(got[i].Lookup("d").Double()-h.d.Radians()) > 1e-12 {
			t.Fatalf("legacy $geoNear hit %d = %s, want %d at %v rad", i, ejsonOf(got[i]), h.id, h.d.Radians())
		}
	}
}

func TestGeoQueriesMatchBruteForce(t *testing.T) {
	e, c, fixture := geoEnv(t)
	runGeoChecks(t, e, c, fixture, true)
	n := e.remove(c, func(d bson.Raw) bool { return d.Lookup("_id").Int32()%10 < 3 })
	if n < 100 {
		t.Fatalf("removed only %d documents", n)
	}
	runGeoChecks(t, e, c, fixture, false)
}

func TestGeoErrors(t *testing.T) {
	e, c, _ := geoEnv(t)
	cases := []struct{ filter, want string }{
		{`{"loc": {"$geoWithin": {"$box": [[0, 0], [1, 1]]}}}`, "ERR $geoWithin legacy shapes require a 2d index (not supported)"},
		{`{"loc": {"$geoWithin": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}}`, "ERR $geoWithin not supported with provided geometry: Point"},
		{`{"loc": {"$near": [0, 0]}}`, "ERR $near with legacy coordinates needs a 2d index, which v1 does not have; use $nearSphere or a GeoJSON point"},
		{`{"other": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}}`, "ERR unable to find index for $geoNear query"},
		{`{"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}, "$maxDistance": -1}}}`, "ERR $maxDistance must be non-negative"},
	}
	for _, tc := range cases {
		_, err := e.q.PlanFind(c, Find{Filter: ej(t, tc.filter)}, Options{})
		if err == nil || err.Error() != tc.want {
			t.Errorf("%s: %v, want %q", tc.filter, err, tc.want)
		}
	}
	if _, err := e.q.Count(c, ej(t, `{"loc": {"$near": [0, 0]}}`), Options{}); err == nil || err.Error() != "ERR $geoNear, $near, and $nearSphere are not allowed in this context" {
		t.Errorf("count with $near: %v", err)
	}
}
