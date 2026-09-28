package geo

import (
	"math"
	"reflect"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

func TestDefaultParams(t *testing.T) {
	p := DefaultParams()
	if p.Coarsest != 2 || p.Finest != 16 || p.MaxCells != 20 {
		t.Fatalf("DefaultParams = %+v, want {2 16 20}", p)
	}
}

func TestMetresAngle(t *testing.T) {
	if got := Metres(s1.Angle(1)); got != EarthRadiusMetres {
		t.Fatalf("Metres(1 rad) = %v", got)
	}
	for _, m := range []float64{0, 1, 110, 2e6, math.Pi * EarthRadiusMetres} {
		if got := Metres(Angle(m)); math.Abs(got-m) > 1e-9*math.Max(1, m) {
			t.Fatalf("Metres(Angle(%v)) = %v", m, got)
		}
	}
}

func TestValueRoundTrip(t *testing.T) {
	ll := s2.LatLngFromDegrees(40.77, -73.97)
	v := Entry{Kind: ValuePoint, LatLng: ll}.AppendValue([]byte{0xAA})
	if len(v) != 18 || v[0] != 0xAA || v[1] != ValuePoint {
		t.Fatalf("point value % x", v)
	}
	kind, got, err := DecodeValue(v[1:])
	if err != nil || kind != ValuePoint || got != ll {
		t.Fatalf("DecodeValue = %v %v %v", kind, got, err)
	}
	v = Entry{Kind: ValueShape}.AppendValue(nil)
	if kind, _, err := DecodeValue(v); err != nil || kind != ValueShape || len(v) != 1 {
		t.Fatalf("shape value % x: %v %v", v, kind, err)
	}
	for _, bad := range [][]byte{nil, {0x03}, {0x01, 1, 2}, {0x02, 0}} {
		if _, _, err := DecodeValue(bad); err != ErrBadValue {
			t.Fatalf("DecodeValue(% x) err %v", bad, err)
		}
	}
}

func TestEntriesPoint(t *testing.T) {
	g := mustParse(t, `{"type":"Point","coordinates":[2.35,48.85]}`, ParseOpts{})
	es := DefaultParams().Entries(g)
	ll, _ := g.LatLng()
	if len(es) != 1 || es[0].Kind != ValuePoint || es[0].LatLng != ll || !es[0].Cell.IsLeaf() {
		t.Fatalf("entries %+v", es)
	}
	if es[0].Cell != s2.CellIDFromLatLng(ll) {
		t.Fatal("point not indexed at its leaf cell")
	}
}

func TestEntriesShapes(t *testing.T) {
	p := DefaultParams()
	shapes := []string{
		`{"type":"LineString","coordinates":[[0,0],[3,1],[6,0]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[4,4],[6,4],[6,6],[4,6],[4,4]]]}`,
		`{"type":"Polygon","coordinates":[[[-0.001,51.5],[0.001,51.5],[0.001,51.501],[-0.001,51.5]]]}`,
		`{"type":"Polygon","coordinates":[[[-60,-60],[60,-60],[60,60],[-60,60],[-60,-60]]]}`,
		`{"type":"MultiPoint","coordinates":[[0,0],[100,10],[-100,-10]]}`,
		`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[120,5],[121,5],[121,6],[120,5]]]]}`,
		`{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[30,30]},{"type":"LineString","coordinates":[[-30,0],[-31,1]]}]}`,
	}
	for _, js := range shapes {
		g := mustParse(t, js, ParseOpts{})
		es := p.Entries(g)
		if len(es) == 0 {
			t.Fatalf("%s: no entries", js)
		}
		var cu s2.CellUnion
		for i, e := range es {
			if e.Kind != ValueShape {
				t.Fatalf("%s: entry kind %x", js, e.Kind)
			}
			if l := e.Cell.Level(); l < p.Coarsest || l > p.Finest {
				t.Fatalf("%s: level %d outside [%d, %d]", js, l, p.Coarsest, p.Finest)
			}
			if i > 0 && es[i-1].Cell >= e.Cell {
				t.Fatalf("%s: entries not sorted", js)
			}
			cu = append(cu, e.Cell)
		}
		cu.Normalize()
		// Every vertex and point of the shape lies in an indexed cell.
		for _, a := range g.atoms(nil) {
			var pts []s2.Point
			switch {
			case a.poly != nil:
				for _, l := range a.poly.Loops() {
					pts = append(pts, l.Vertices()...)
				}
			case a.line != nil:
				pts = *a.line
			default:
				pts = []s2.Point{a.pt}
			}
			for _, pt := range pts {
				if !cu.ContainsPoint(pt) {
					t.Fatalf("%s: vertex %v not covered", js, s2.LatLngFromPoint(pt))
				}
			}
		}
		if !reflect.DeepEqual(es, p.Entries(mustParse(t, js, ParseOpts{}))) {
			t.Fatalf("%s: entries differ between two parses", js)
		}
	}
}

func TestCoveringDeterminism(t *testing.T) {
	p := DefaultParams()
	js := `{"type":"Polygon","coordinates":[[[179,-1],[-179,-1],[-179,1],[179,1],[179,-1]]]}`
	first := p.Entries(mustParse(t, js, ParseOpts{}))
	firstPlan := p.PlanRegion(mustParse(t, js, ParseOpts{}).Region(), 0)
	for i := 0; i < 20; i++ {
		if got := p.Entries(mustParse(t, js, ParseOpts{})); !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d: entries changed", i)
		}
		if got := p.PlanRegion(mustParse(t, js, ParseOpts{}).Region(), 0); !reflect.DeepEqual(got, firstPlan) {
			t.Fatalf("run %d: plan changed", i)
		}
	}
	// The antimeridian square straddles faces; its entries must sit on
	// both sides of longitude 180.
	var east, west bool
	for _, e := range first {
		lng := e.Cell.LatLng().Lng.Degrees()
		east = east || lng > 0
		west = west || lng < 0
	}
	if !east || !west {
		t.Fatalf("antimeridian covering one-sided: east %v west %v", east, west)
	}
}
