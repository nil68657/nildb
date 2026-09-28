package geo

import (
	"errors"
	"math"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

func pointAt(lat, lng float64) *Geometry { return pointGeometry(s2.LatLngFromDegrees(lat, lng)) }

func smallFixture(t *testing.T, geoms ...*Geometry) *fixture {
	f := &fixture{p: DefaultParams(), byID: make(map[string]*fixtureDoc)}
	for _, g := range geoms {
		f.add(t, g)
	}
	f.sortKeys()
	return f
}

// TestNearRoundSchedule places 700 points 2 km from the centre and 100 at
// 5 km, then checks each round's radius: the width starts at 1 km, halves
// after the round that emits 700 and doubles after every round below 300.
func TestNearRoundSchedule(t *testing.T) {
	const lat, lng = -33.9, 151.2
	var geoms []*Geometry
	for i := 0; i < 800; i++ {
		dist := 2e3
		if i >= 700 {
			dist = 5e3
		}
		plat, plng := destination(lat, lng, float64(i)*2*math.Pi/800, dist/EarthRadiusMetres)
		geoms = append(geoms, pointAt(plat, plng))
	}
	f := smallFixture(t, geoms...)
	type round struct {
		hi      float64 // km
		emitted int
	}
	var got []round
	n := 0
	err := near(f.p, s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng)), NearOpts{Max: Angle(12e3)},
		f.scan, f.probe, f.load,
		func([]byte, s1.Angle) bool { n++; return true },
		func(hi s1.Angle, emitted int) { got = append(got, round{Metres(hi) / 1e3, emitted}) })
	if err != nil {
		t.Fatal(err)
	}
	want := []round{{1, 0}, {3, 700}, {4, 0}, {6, 100}, {10, 0}, {12, 0}}
	if len(got) != len(want) || n != 800 {
		t.Fatalf("rounds %v (%d emitted), want %v", got, n, want)
	}
	for i := range want {
		if got[i].emitted != want[i].emitted || math.Abs(got[i].hi-want[i].hi) > 1e-9 {
			t.Fatalf("rounds %v, want %v", got, want)
		}
	}
}

func TestNearStopsAndBounds(t *testing.T) {
	var geoms []*Geometry
	for i := 0; i < 10; i++ {
		geoms = append(geoms, pointAt(0, float64(i)))
	}
	f := smallFixture(t, geoms...)
	origin := s2.PointFromLatLng(s2.LatLngFromDegrees(0, 0))
	collect := func(o NearOpts, stopAfter int) []string {
		var ids []string
		err := Near(f.p, origin, o, f.scan, f.probe, f.load, func(id []byte, _ s1.Angle) bool {
			ids = append(ids, string(id))
			return stopAfter == 0 || len(ids) < stopAfter
		})
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if ids := collect(NearOpts{}, 3); len(ids) != 3 || ids[0] != "d00000" || ids[2] != "d00002" {
		t.Fatalf("emit stop: %v", ids)
	}
	if ids := collect(NearOpts{Limit: 4}, 0); len(ids) != 4 || ids[3] != "d00003" {
		t.Fatalf("limit: %v", ids)
	}
	deg := func(d float64) s1.Angle { return s1.Angle(d * math.Pi / 180) }
	// Min and Max are inclusive; the point at exactly 3 degrees counts.
	if ids := collect(NearOpts{Min: pointDistance(origin, geoms[3].Points[0]), Max: deg(6.5)}, 0); len(ids) != 4 || ids[0] != "d00003" || ids[3] != "d00006" {
		t.Fatalf("annulus: %v", ids)
	}
	if ids := collect(NearOpts{Min: deg(5), Max: deg(4)}, 0); len(ids) != 0 {
		t.Fatalf("min above max: %v", ids)
	}
	if ids := collect(NearOpts{Max: s1.Angle(math.SmallestNonzeroFloat64)}, 0); len(ids) != 1 || ids[0] != "d00000" {
		t.Fatalf("$maxDistance 0: %v", ids)
	}
}

func TestNearSkipsAndErrors(t *testing.T) {
	poly := mustParse(t, `{"type":"Polygon","coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]]}`, ParseOpts{})
	f := smallFixture(t, pointAt(0, 0.5), poly, pointAt(0, 3))
	origin := s2.PointFromLatLng(s2.LatLngFromDegrees(0, 0))
	emitAll := func(ids *[]string) func([]byte, s1.Angle) bool {
		return func(id []byte, _ s1.Angle) bool { *ids = append(*ids, string(id)); return true }
	}

	// A document Load no longer finds is skipped.
	var ids []string
	gone := func([]byte) (*Geometry, error) { return nil, nil }
	if err := Near(f.p, origin, NearOpts{}, f.scan, f.probe, gone, emitAll(&ids)); err != nil || len(ids) != 2 {
		t.Fatalf("missing shape: %v %v", ids, err)
	}

	boom := errors.New("boom")
	failLoad := func([]byte) (*Geometry, error) { return nil, boom }
	if err := Near(f.p, origin, NearOpts{}, f.scan, f.probe, failLoad, emitAll(&ids)); !errors.Is(err, boom) {
		t.Fatalf("load error: %v", err)
	}
	failScan := func(Range, func(s2.CellID, []byte, []byte) bool) error { return boom }
	if err := Near(f.p, origin, NearOpts{}, failScan, f.probe, f.load, emitAll(&ids)); !errors.Is(err, boom) {
		t.Fatalf("scan error: %v", err)
	}
	failProbe := func(s2.CellID, func([]byte, []byte) bool) error { return boom }
	big := smallFixture(t, mustParse(t, `{"type":"Polygon","coordinates":[[[-40,-40],[40,-40],[40,40],[-40,40],[-40,-40]]]}`, ParseOpts{}))
	if err := Near(big.p, origin, NearOpts{}, big.scan, failProbe, big.load, emitAll(&ids)); !errors.Is(err, boom) {
		t.Fatalf("probe error: %v", err)
	}
	badValue := func(r Range, visit func(s2.CellID, []byte, []byte) bool) error {
		return f.scan(r, func(c s2.CellID, id, _ []byte) bool { return visit(c, id, []byte{0x09}) })
	}
	if err := Near(f.p, origin, NearOpts{}, badValue, f.probe, f.load, emitAll(&ids)); !errors.Is(err, ErrBadValue) {
		t.Fatalf("bad value: %v", err)
	}
}
