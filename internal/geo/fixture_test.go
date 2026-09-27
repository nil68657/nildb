package geo

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// The fixture indexes 3,000 points (1,000 of them in a 50 km cluster, so
// some kNN rounds emit hundreds of documents), 200 polygons (a third
// with holes, one across the antimeridian, one over the north pole, one
// 2,500 km wide), 100 polylines and 20 multi-geometries in a sorted slice
// keyed by (cell, _id), the same order the geo column family uses. Queries
// run through PlanRegion+Refine and Near over Scan/Probe/Load callbacks and
// are compared with a brute-force oracle over every document.

type fixtureDoc struct {
	id    []byte
	g     *Geometry
	point bool
}

type fixtureKey struct {
	cell s2.CellID
	id   []byte
	val  []byte
}

type fixture struct {
	p          Params
	docs       []*fixtureDoc
	byID       map[string]*fixtureDoc
	keys       []fixtureKey
	loads      int
	pointLoads int
}

func (f *fixture) add(t *testing.T, g *Geometry) {
	t.Helper()
	d := &fixtureDoc{id: fmt.Appendf(nil, "d%05d", len(f.docs)), g: g}
	_, d.point = g.LatLng()
	f.docs = append(f.docs, d)
	f.byID[string(d.id)] = d
	es := f.p.Entries(g)
	if len(es) == 0 {
		t.Fatalf("document %s has no entries", d.id)
	}
	for _, e := range es {
		f.keys = append(f.keys, fixtureKey{cell: e.Cell, id: d.id, val: e.AppendValue(nil)})
	}
}

func (f *fixture) sortKeys() {
	slices.SortFunc(f.keys, func(a, b fixtureKey) int {
		switch {
		case a.cell < b.cell:
			return -1
		case a.cell > b.cell:
			return 1
		}
		return bytes.Compare(a.id, b.id)
	})
}

// scan and probe hand out ksID and val through one reused buffer each, the
// way a RocksDB iterator's KeySlice changes on every move, so a caller
// that keeps a slice without copying it fails the comparison.
func (f *fixture) scan(r Range, visit func(cell s2.CellID, ksID, val []byte) bool) error {
	var idBuf, valBuf []byte
	i := sort.Search(len(f.keys), func(i int) bool { return f.keys[i].cell >= r.Lo })
	for ; i < len(f.keys) && f.keys[i].cell <= r.Hi; i++ {
		k := f.keys[i]
		idBuf = append(idBuf[:0], k.id...)
		valBuf = append(valBuf[:0], k.val...)
		if !visit(k.cell, idBuf, valBuf) {
			break
		}
		clear(idBuf)
	}
	return nil
}

func (f *fixture) probe(c s2.CellID, visit func(ksID, val []byte) bool) error {
	var idBuf, valBuf []byte
	i := sort.Search(len(f.keys), func(i int) bool { return f.keys[i].cell >= c })
	for ; i < len(f.keys) && f.keys[i].cell == c; i++ {
		idBuf = append(idBuf[:0], f.keys[i].id...)
		valBuf = append(valBuf[:0], f.keys[i].val...)
		if !visit(idBuf, valBuf) {
			break
		}
		clear(idBuf)
	}
	return nil
}

func (f *fixture) load(id []byte) (*Geometry, error) {
	d, ok := f.byID[string(id)]
	if !ok {
		return nil, nil
	}
	f.loads++
	if d.point {
		f.pointLoads++
	}
	return d.g, nil
}

// destination moves from (lat, lng) in degrees along bearing (radians,
// clockwise from north) by dist radians and returns degrees.
func destination(lat, lng, bearing, dist float64) (float64, float64) {
	φ1, λ1 := lat*math.Pi/180, lng*math.Pi/180
	φ2 := math.Asin(math.Sin(φ1)*math.Cos(dist) + math.Cos(φ1)*math.Sin(dist)*math.Cos(bearing))
	λ2 := λ1 + math.Atan2(math.Sin(bearing)*math.Sin(dist)*math.Cos(φ1), math.Cos(dist)-math.Sin(φ1)*math.Sin(φ2))
	return φ2 * 180 / math.Pi, math.Remainder(λ2*180/math.Pi, 360)
}

func randLatLng(rng *rand.Rand) (float64, float64) {
	return math.Asin(2*rng.Float64()-1) * 180 / math.Pi, 360*rng.Float64() - 180
}

// star returns a closed ring of k vertices at increasing bearings around
// (lat, lng), each at a distance in [lo*radius, radius]. Consecutive
// bearings differ by less than 180 degrees, so the ring is simple.
func star(rng *rand.Rand, lat, lng, radius float64, k int, lo float64) [][2]float64 {
	base := rng.Float64() * 2 * math.Pi
	ring := make([][2]float64, 0, k+1)
	for i := 0; i < k; i++ {
		theta := base + (float64(i)+0.4*(rng.Float64()-0.5))*2*math.Pi/float64(k)
		vlat, vlng := destination(lat, lng, theta, radius*(lo+(1-lo)*rng.Float64()))
		ring = append(ring, [2]float64{vlng, vlat})
	}
	return append(ring, ring[0])
}

func ringBSON(ring [][2]float64) bson.A {
	a := make(bson.A, len(ring))
	for i, p := range ring {
		a[i] = bson.A{p[0], p[1]}
	}
	return a
}

func docValue(t *testing.T, d bson.D) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw}
}

// starPolygon parses a star polygon around (lat, lng), with a hole when
// holed. It retries with fresh draws if Parse rejects a ring.
func starPolygon(t *testing.T, rng *rand.Rand, lat, lng, radius float64, holed bool) (*Geometry, bson.A) {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		k := 3 + rng.IntN(10)
		if holed && k < 4 {
			k = 4
		}
		coords := bson.A{ringBSON(star(rng, lat, lng, radius, k, 0.4))}
		if holed {
			coords = append(coords, ringBSON(star(rng, lat, lng, radius*0.12, 3+rng.IntN(4), 0.5)))
		}
		g, err := Parse(docValue(t, bson.D{{Key: "type", Value: "Polygon"}, {Key: "coordinates", Value: coords}}), ParseOpts{})
		if err == nil {
			return g, coords
		}
	}
	t.Fatalf("no valid star polygon around %v, %v", lat, lng)
	return nil, nil
}

func buildFixture(t *testing.T, rng *rand.Rand) (*fixture, [][3]float64) {
	f := &fixture{p: DefaultParams(), byID: make(map[string]*fixtureDoc)}
	km := func(x float64) float64 { return x * 1000 / EarthRadiusMetres }

	// 2,000 uniform points and 1,000 within 50 km of Paris; every tenth
	// point is a legacy pair.
	for i := 0; i < 3000; i++ {
		lat, lng := randLatLng(rng)
		if i >= 2000 {
			lat, lng = destination(48.85, 2.35, rng.Float64()*2*math.Pi, km(50)*math.Sqrt(rng.Float64()))
		}
		var v bson.RawValue
		if i%10 == 0 {
			raw, _ := bson.Marshal(bson.D{{Key: "v", Value: bson.A{lng, lat}}})
			v = bson.Raw(raw).Lookup("v")
		} else {
			v = docValue(t, bson.D{{Key: "type", Value: "Point"}, {Key: "coordinates", Value: bson.A{lng, lat}}})
		}
		g, err := Parse(v, ParseOpts{AllowLegacyPair: true})
		if err != nil {
			t.Fatalf("point %d: %v", i, err)
		}
		f.add(t, g)
	}

	// 200 polygons; their centres and radii seed some queries.
	var centres [][3]float64
	for i := 0; i < 200; i++ {
		lat, lng := randLatLng(rng)
		radius := km(20 + 780*rng.Float64())
		switch i {
		case 0: // across the antimeridian
			lat, lng, radius = 10, 179.7, km(300)
		case 1: // over the north pole
			lat, lng, radius = 88, 30, km(500)
		case 2: // indexed at coarse cells
			lat, lng, radius = 20, 60, km(2500)
		}
		g, _ := starPolygon(t, rng, lat, lng, radius, i%3 == 0)
		f.add(t, g)
		centres = append(centres, [3]float64{lat, lng, radius})
	}
	if !f.docs[3000].g.Polys[0].ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(10, 180))) {
		t.Fatal("antimeridian polygon does not straddle longitude 180")
	}
	if !f.docs[3001].g.Polys[0].ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(90, 0))) {
		t.Fatal("polar polygon does not contain the pole")
	}

	// 100 polylines.
	for i := 0; i < 100; i++ {
		lat, lng := randLatLng(rng)
		bearing := rng.Float64() * 2 * math.Pi
		line := bson.A{bson.A{lng, lat}}
		for n := 1 + rng.IntN(7); n > 0; n-- {
			bearing += rng.Float64() - 0.5
			lat, lng = destination(lat, lng, bearing, km(20+380*rng.Float64()))
			line = append(line, bson.A{lng, lat})
		}
		g, err := Parse(docValue(t, bson.D{{Key: "type", Value: "LineString"}, {Key: "coordinates", Value: line}}), ParseOpts{})
		if err != nil {
			t.Fatalf("polyline %d: %v", i, err)
		}
		f.add(t, g)
	}

	// 20 multi-geometries.
	for i := 0; i < 20; i++ {
		lat, lng := randLatLng(rng)
		var d bson.D
		switch i % 4 {
		case 0, 1:
			mp := bson.A{}
			for n := 2 + rng.IntN(3); n > 0; n-- {
				plat, plng := destination(lat, lng, rng.Float64()*2*math.Pi, km(300)*rng.Float64())
				mp = append(mp, bson.A{plng, plat})
			}
			d = bson.D{{Key: "type", Value: "MultiPoint"}, {Key: "coordinates", Value: mp}}
		case 2:
			_, a := starPolygon(t, rng, lat, lng, km(50+150*rng.Float64()), false)
			blat, blng := destination(lat, lng, rng.Float64()*2*math.Pi, km(500))
			_, b := starPolygon(t, rng, blat, blng, km(50+150*rng.Float64()), false)
			d = bson.D{{Key: "type", Value: "MultiPolygon"}, {Key: "coordinates", Value: bson.A{a, b}}}
		default:
			elat, elng := destination(lat, lng, rng.Float64()*2*math.Pi, km(200))
			d = bson.D{{Key: "type", Value: "GeometryCollection"}, {Key: "geometries", Value: bson.A{
				bson.D{{Key: "type", Value: "Point"}, {Key: "coordinates", Value: bson.A{lng, lat}}},
				bson.D{{Key: "type", Value: "LineString"}, {Key: "coordinates", Value: bson.A{bson.A{lng, lat}, bson.A{elng, elat}}}},
			}}}
		}
		g, err := Parse(docValue(t, d), ParseOpts{})
		if err != nil {
			t.Fatalf("multi %d: %v", i, err)
		}
		f.add(t, g)
	}
	f.sortKeys()
	return f, centres
}

func sortedIDs(set map[string]bool) []string {
	var out []string
	for id, ok := range set {
		if ok {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func TestFixtureRegionQueries(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 927))
	f, centres := buildFixture(t, rng)
	km := func(x float64) float64 { return x * 1000 / EarthRadiusMetres }

	var queries []*Geometry
	add := func(g *Geometry) { queries = append(queries, g) }
	polyQuery := func(lat, lng, radius float64, holed bool) {
		g, _ := starPolygon(t, rng, lat, lng, radius, holed)
		add(g)
	}
	polyQuery(12, -179.8, km(800), false) // across the antimeridian, near polygon 0
	polyQuery(-87, 0, km(700), false)     // over the south pole
	polyQuery(86, 100, km(400), false)    // overlapping the polar polygon
	for i := 0; i < 5; i++ {              // small queries inside indexed polygons: probes
		c := centres[2+i*37]
		polyQuery(c[0], c[1], km(5+25*rng.Float64()), false)
	}
	polyQuery(48.85, 2.35, km(30), true) // inside the Paris cluster
	for len(queries) < 36 {
		lat, lng := randLatLng(rng)
		polyQuery(lat, lng, km(100+2900*rng.Float64()), len(queries)%4 == 0)
	}
	for len(queries) < 40 {
		lat, lng := randLatLng(rng)
		_, a := starPolygon(t, rng, lat, lng, km(300+700*rng.Float64()), false)
		blat, blng := destination(lat, lng, rng.Float64()*2*math.Pi, km(3000))
		_, b := starPolygon(t, rng, blat, blng, km(300+700*rng.Float64()), true)
		g, err := Parse(docValue(t, bson.D{{Key: "type", Value: "MultiPolygon"}, {Key: "coordinates", Value: bson.A{a, b}}}), ParseOpts{})
		if err != nil {
			t.Fatalf("multipolygon query: %v", err)
		}
		add(g)
	}

	var totalWithin, totalInter, viaProbeOnly int
	for qi, q := range queries {
		plan := f.p.PlanRegion(q.Region(), DefaultQueryMaxCells)
		fromRange := make(map[string]bool)
		fromProbe := make(map[string]bool)
		for _, r := range plan.Ranges {
			_ = f.scan(r, func(_ s2.CellID, id, _ []byte) bool { fromRange[string(id)] = true; return true })
		}
		for _, c := range plan.Probes {
			_ = f.probe(c, func(id, _ []byte) bool { fromProbe[string(id)] = true; return true })
		}
		idxWithin, idxInter := map[string]bool{}, map[string]bool{}
		for _, set := range []map[string]bool{fromRange, fromProbe} {
			for id := range set {
				d := f.byID[id]
				idxWithin[id] = Refine(Within, q, d.g)
				idxInter[id] = Refine(Intersects, q, d.g)
			}
		}
		oraWithin, oraInter := map[string]bool{}, map[string]bool{}
		for _, d := range f.docs {
			id := string(d.id)
			oraWithin[id] = Refine(Within, q, d.g)
			oraInter[id] = Refine(Intersects, q, d.g)
		}
		gw, ow := sortedIDs(idxWithin), sortedIDs(oraWithin)
		gi, oi := sortedIDs(idxInter), sortedIDs(oraInter)
		if !slices.Equal(gw, ow) {
			t.Errorf("query %d (%v): $geoWithin index %d docs, oracle %d: missing %v",
				qi, q.Kind, len(gw), len(ow), missing(ow, gw))
		}
		if !slices.Equal(gi, oi) {
			t.Errorf("query %d (%v): $geoIntersects index %d docs, oracle %d: missing %v",
				qi, q.Kind, len(gi), len(oi), missing(oi, gi))
		}
		totalWithin += len(ow)
		totalInter += len(oi)
		for _, id := range oi {
			if fromProbe[id] && !fromRange[id] {
				viaProbeOnly++
			}
		}
	}
	t.Logf("40 region queries: %d within, %d intersecting, %d found only by ancestor probes", totalWithin, totalInter, viaProbeOnly)
	if totalWithin == 0 || totalInter <= totalWithin || viaProbeOnly == 0 {
		t.Fatalf("fixture too weak: within %d, intersects %d, probe-only %d", totalWithin, totalInter, viaProbeOnly)
	}
}

func missing(want, got []string) []string {
	var out []string
	for _, id := range want {
		if _, ok := slices.BinarySearch(got, id); !ok {
			out = append(out, id)
		}
	}
	return out
}

type nearCase struct {
	lat, lng float64
	opts     NearOpts
	filtered bool
}

type nearHit struct {
	id string
	d  s1.Angle
}

func TestFixtureNear(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 928))
	f, centres := buildFixture(t, rng)
	km := func(x float64) s1.Angle { return Angle(x * 1000) }
	even := func(id []byte) bool { return id[len(id)-1]%2 == 0 }

	cases := []nearCase{
		{48.85, 2.35, NearOpts{Max: km(100)}, false},               // the whole 50 km cluster, no limit
		{centres[2][0], centres[2][1], NearOpts{Limit: 30}, false}, // inside the 2,500 km polygon: distance 0
		{10, 179.9, NearOpts{Limit: 40}, false},                    // at the antimeridian polygon
		{90, 0, NearOpts{Limit: 40, Max: km(3000)}, false},         // north pole
		{-90, 0, NearOpts{Limit: 20}, false},                       // south pole
		{0, 0, NearOpts{}, false},                                  // everything: whole-sphere stop
		{48.9, 2.4, NearOpts{Min: km(20), Max: km(40)}, false},     // an annulus inside the cluster
		{48.9, 2.4, NearOpts{Min: km(5000), Limit: 50}, true},      // skip the inner cap
		{centres[40][0], centres[40][1], NearOpts{Limit: 5, Max: km(1)}, false},
	}
	for len(cases) < 40 {
		lat, lng := randLatLng(rng)
		c := nearCase{lat: lat, lng: lng}
		switch len(cases) % 4 {
		case 0:
			c.opts.Limit = 10
		case 1:
			c.opts.Limit = 50
			c.opts.Min = km(800 * rng.Float64())
			c.opts.Max = c.opts.Min + km(500+2500*rng.Float64())
		case 2:
			c.opts.Max = km(300 + 1700*rng.Float64())
		case 3:
			c.opts.Limit = 25
			c.opts.Min = km(3000 * rng.Float64())
			c.filtered = true
		}
		cases = append(cases, c)
	}

	var emittedTotal int
	for ci, c := range cases {
		center := s2.PointFromLatLng(s2.LatLngFromDegrees(c.lat, c.lng))
		opts := c.opts
		filterCalls := map[string]int{}
		if c.filtered {
			opts.Filter = func(id []byte) bool { filterCalls[string(id)]++; return even(id) }
		}
		var got []nearHit
		err := Near(f.p, center, opts, f.scan, f.probe, f.load, func(id []byte, d s1.Angle) bool {
			got = append(got, nearHit{string(id), d})
			return true
		})
		if err != nil {
			t.Fatalf("case %d: %v", ci, err)
		}
		for id, n := range filterCalls {
			if n > 1 {
				t.Fatalf("case %d: Filter called %d times for %s", ci, n, id)
			}
		}

		maxA := c.opts.Max
		if maxA <= 0 {
			maxA = math.Pi
		}
		var want []nearHit
		for _, d := range f.docs {
			dist := Distance(center, d.g)
			if dist < c.opts.Min || dist > maxA || (c.filtered && !even(d.id)) {
				continue
			}
			want = append(want, nearHit{string(d.id), dist})
		}
		slices.SortFunc(want, func(a, b nearHit) int {
			switch {
			case a.d < b.d:
				return -1
			case a.d > b.d:
				return 1
			}
			return bytes.Compare([]byte(a.id), []byte(b.id))
		})
		if c.opts.Limit > 0 && len(want) > c.opts.Limit {
			want = want[:c.opts.Limit]
		}

		if len(got) != len(want) {
			t.Errorf("case %d (%+v): Near returned %d docs, oracle %d", ci, c.opts, len(got), len(want))
			continue
		}
		for i := range want {
			g, w := got[i], want[i]
			if g.id != w.id || !closeRel(float64(g.d), float64(w.d), 1e-9) {
				t.Errorf("case %d: result %d is %s at %.3f m, oracle %s at %.3f m", ci, i, g.id, Metres(g.d), w.id, Metres(w.d))
				break
			}
		}
		emittedTotal += len(got)
	}
	if f.pointLoads != 0 {
		t.Fatalf("Near loaded %d point documents; points must be served from the index value", f.pointLoads)
	}
	if emittedTotal < len(f.docs) {
		t.Fatalf("fixture too weak: %d results in total", emittedTotal)
	}
	t.Logf("40 kNN queries: %d results, %d shape loads", emittedTotal, f.loads)
}

func closeRel(a, b, tol float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= tol*math.Max(math.Abs(a), math.Abs(b))
}
