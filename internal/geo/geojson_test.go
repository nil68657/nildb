package geo

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/golang/geo/s2"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// val parses relaxed Extended JSON into a RawValue: integers become int32,
// decimals doubles.
func val(t testing.TB, js string) bson.RawValue {
	t.Helper()
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(`{"v":`+js+`}`), false, &raw); err != nil {
		t.Fatalf("bad test JSON %s: %v", js, err)
	}
	return raw.Lookup("v")
}

func mustParse(t testing.TB, js string, o ParseOpts) *Geometry {
	t.Helper()
	g, err := Parse(val(t, js), o)
	if err != nil {
		t.Fatalf("Parse(%s): %v", js, err)
	}
	return g
}

var legacy = ParseOpts{AllowLegacyPair: true}

func TestParseAccepts(t *testing.T) {
	cases := []struct {
		name   string
		js     string
		opts   ParseOpts
		kind   Kind
		points int
		lines  int
		polys  int
		parts  int
	}{
		{"point", `{"type":"Point","coordinates":[-73.97,40.77]}`, ParseOpts{}, Point, 1, 0, 0, 0},
		{"point with altitude", `{"type":"Point","coordinates":[1,2,300]}`, ParseOpts{}, Point, 1, 0, 0, 0},
		{"point int64 and decimal", `{"type":"Point","coordinates":[{"$numberLong":"5"},{"$numberDecimal":"1.5"}]}`, ParseOpts{}, Point, 1, 0, 0, 0},
		{"point on the antimeridian", `{"type":"Point","coordinates":[180,-90]}`, ParseOpts{}, Point, 1, 0, 0, 0},
		{"linestring", `{"type":"LineString","coordinates":[[0,0],[0,0],[1,1],[2,1]]}`, ParseOpts{}, LineString, 0, 1, 0, 0},
		{"polygon", `{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}`, ParseOpts{}, Polygon, 0, 0, 1, 0},
		{"polygon with hole", `{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[4,4],[6,4],[6,6],[4,6],[4,4]]]}`, ParseOpts{}, Polygon, 0, 0, 1, 0},
		{"polygon crs EPSG:4326", `{"type":"Polygon","crs":{"type":"name","properties":{"name":"EPSG:4326"}},"coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}`, ParseOpts{}, Polygon, 0, 0, 1, 0},
		{"multipoint", `{"type":"MultiPoint","coordinates":[[0,0],[1,1],[2,2]]}`, ParseOpts{}, MultiPoint, 3, 0, 0, 0},
		{"multilinestring", `{"type":"MultiLineString","coordinates":[[[0,0],[1,1]],[[2,2],[3,3]]]}`, ParseOpts{}, MultiLineString, 0, 2, 0, 0},
		{"multipolygon", `{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[5,5],[6,5],[6,6],[5,5]]]]}`, ParseOpts{}, MultiPolygon, 0, 0, 2, 0},
		{"collection", `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0,0]},{"type":"LineString","coordinates":[[0,0],[1,1]]}]}`, ParseOpts{}, Collection, 0, 0, 0, 2},
		{"legacy array", `[-73.97,40.77]`, legacy, Point, 1, 0, 0, 0},
		{"legacy x y", `{"x":1,"y":2}`, legacy, Point, 1, 0, 0, 0},
		{"legacy lng lat", `{"lng":{"$numberLong":"3"},"lat":4.5}`, legacy, Point, 1, 0, 0, 0},
		{"geojson wins over legacy", `{"type":"Point","coordinates":[1,2]}`, legacy, Point, 1, 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := mustParse(t, c.js, c.opts)
			if g.Kind != c.kind || len(g.Points) != c.points || len(g.Lines) != c.lines || len(g.Polys) != c.polys || len(g.Parts) != c.parts {
				t.Fatalf("got kind %v points %d lines %d polys %d parts %d", g.Kind, len(g.Points), len(g.Lines), len(g.Polys), len(g.Parts))
			}
		})
	}
}

func TestParsePositionsExact(t *testing.T) {
	g := mustParse(t, `{"type":"Point","coordinates":[-73.97,40.77]}`, ParseOpts{})
	ll, ok := g.LatLng()
	if !ok || ll != s2.LatLngFromDegrees(40.77, -73.97) {
		t.Fatalf("LatLng = %v, %v", ll, ok)
	}
	if g.Points[0] != s2.PointFromLatLng(ll) {
		t.Fatal("point does not match its LatLng")
	}
	l := mustParse(t, `{"x":1,"y":2}`, legacy)
	if ll, _ := l.LatLng(); ll != s2.LatLngFromDegrees(2, 1) {
		t.Fatalf("legacy {x,y} parsed as %v, want lng 1 lat 2", ll)
	}
	line := mustParse(t, `{"type":"LineString","coordinates":[[0,0],[0,0],[1,1],[2,1]]}`, ParseOpts{})
	if n := len(*line.Lines[0]); n != 3 {
		t.Fatalf("consecutive duplicate kept: %d vertices", n)
	}
	if _, ok := line.LatLng(); ok {
		t.Fatal("LatLng reported true for a LineString")
	}
	if _, ok := mustParse(t, `{"type":"MultiPoint","coordinates":[[0,0]]}`, ParseOpts{}).LatLng(); ok {
		t.Fatal("LatLng reported true for a MultiPoint")
	}
}

// TestParseWinding checks that without a crs both windings of a ring mean
// the small area, and that the strict-winding crs keeps the winding.
func TestParseWinding(t *testing.T) {
	ccw := `[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]`
	cw := `[[[-10,-10],[-10,10],[10,10],[10,-10],[-10,-10]]]`
	strict := `"crs":{"type":"name","properties":{"name":"urn:x-mongodb:crs:strictwinding:EPSG:4326"}}`
	area := func(js string) float64 { return mustParse(t, js, ParseOpts{}).Polys[0].Area() }

	small := area(`{"type":"Polygon","coordinates":` + ccw + `}`)
	if small <= 0 || small >= 2*math.Pi {
		t.Fatalf("ccw area %v", small)
	}
	if a := area(`{"type":"Polygon","coordinates":` + cw + `}`); math.Abs(a-small) > 1e-12 {
		t.Fatalf("cw ring not normalised: area %v, want %v", a, small)
	}
	if a := area(`{"type":"Polygon",` + strict + `,"coordinates":` + ccw + `}`); math.Abs(a-small) > 1e-12 {
		t.Fatalf("strict ccw area %v, want %v", a, small)
	}
	big := mustParse(t, `{"type":"Polygon",`+strict+`,"coordinates":`+cw+`}`, ParseOpts{})
	if a := big.Polys[0].Area(); math.Abs(a-(4*math.Pi-small)) > 1e-9 {
		t.Fatalf("strict cw area %v, want %v", a, 4*math.Pi-small)
	}
	if !big.StrictWinding() {
		t.Fatal("StrictWinding false")
	}
	if !big.Polys[0].ContainsPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(0, 180))) {
		t.Fatal("big polygon does not contain the far side")
	}
}

func TestParseRejects(t *testing.T) {
	const strictCRS = `"crs":{"type":"name","properties":{"name":"urn:x-mongodb:crs:strictwinding:EPSG:4326"}}`
	square := `[[0,0],[10,0],[10,10],[0,10],[0,0]]`
	cases := []struct {
		name string
		js   string
		opts ParseOpts
		want string // exact text, or a prefix when it ends in "..."
	}{
		{"number", `5`, legacy, "geo element must be an array or object"},
		{"string", `"x"`, ParseOpts{}, "geo element must be an array or object"},
		{"legacy array not allowed", `[1,2]`, ParseOpts{}, "legacy coordinate pairs are not accepted here, use a GeoJSON object"},
		{"legacy object not allowed", `{"x":1,"y":2}`, ParseOpts{}, `unknown GeoJSON type: object has no "type" field`},
		{"legacy object not numeric", `{"x":"a","y":2}`, legacy, `unknown GeoJSON type: object has no "type" field`},
		{"legacy three numbers", `[1,2,3]`, legacy, "Point must only contain two numeric elements"},
		{"legacy one number", `[1]`, legacy, "Point must only contain numeric elements"},
		{"legacy out of range", `[181,0]`, legacy, "longitude/latitude is out of bounds, lng: 181 lat: 0"},
		{"unknown type", `{"type":"Circle","coordinates":[0,0]}`, ParseOpts{}, "unknown GeoJSON type: Circle"},
		{"type not a string", `{"type":5}`, ParseOpts{}, `unknown GeoJSON type: "type" must be a string`},
		{"point coords missing", `{"type":"Point"}`, ParseOpts{}, "GeoJSON coordinates must be an array"},
		{"point coords object", `{"type":"Point","coordinates":{"a":1}}`, ParseOpts{}, "GeoJSON coordinates must be an array"},
		{"point one number", `{"type":"Point","coordinates":[1]}`, ParseOpts{}, "Point must only contain numeric elements"},
		{"point string", `{"type":"Point","coordinates":["a",1]}`, ParseOpts{}, "Point must only contain numeric elements"},
		{"point lng", `{"type":"Point","coordinates":[200,0]}`, ParseOpts{}, "longitude/latitude is out of bounds, lng: 200 lat: 0"},
		{"point lat", `{"type":"Point","coordinates":[0,90.5]}`, ParseOpts{}, "longitude/latitude is out of bounds, lng: 0 lat: 90.5"},
		{"point NaN", `{"type":"Point","coordinates":[{"$numberDouble":"NaN"},0]}`, ParseOpts{}, "longitude/latitude is out of bounds, lng: NaN lat: 0"},
		{"line one vertex", `{"type":"LineString","coordinates":[[0,0]]}`, ParseOpts{}, "GeoJSON LineString must have at least 2 vertices"},
		{"line repeated vertex", `{"type":"LineString","coordinates":[[0,0],[0,0]]}`, ParseOpts{}, "GeoJSON LineString must have at least 2 vertices"},
		{"line coords not array", `{"type":"LineString","coordinates":"abc"}`, ParseOpts{}, "GeoJSON coordinates must be an array of coordinates"},
		{"line element not array", `{"type":"LineString","coordinates":[1,2]}`, ParseOpts{}, "GeoJSON coordinates must be an array"},
		{"polygon coords not array", `{"type":"Polygon","coordinates":5}`, ParseOpts{}, "Polygon coordinates must be an array"},
		{"polygon no loops", `{"type":"Polygon","coordinates":[]}`, ParseOpts{}, "Polygon has no loops."},
		{"polygon empty ring", `{"type":"Polygon","coordinates":[[]]}`, ParseOpts{}, "Loop has no vertices"},
		{"ring not closed", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1]]]}`, ParseOpts{}, "Loop is not closed, first vertex does not equal last vertex"},
		{"ring three positions", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[0,0]]]}`, ParseOpts{}, "Loop must have at least 3 different vertices"},
		{"ring repeated vertex", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,0],[0,0]]]}`, ParseOpts{}, "Loop must have at least 3 different vertices"},
		{"bowtie", `{"type":"Polygon","coordinates":[[[0,0],[1,1],[1,0],[0,1],[0,0]]]}`, ParseOpts{},
			"Loop is not valid: Edges 0 and 2 cross. Edge locations in degrees: [0.0000000, 0.0000000]-[1.0000000, 1.0000000] and [0.0000000, 1.0000000]-[1.0000000, 0.0000000]"},
		{"duplicate vertex", `{"type":"Polygon","coordinates":[[[0,0],[2,0],[1,1],[2,2],[2,0],[0,0]]]}`, ParseOpts{}, "Loop is not valid: Duplicate vertices: 1 and 4"},
		{"hole outside", `{"type":"Polygon","coordinates":[` + square + `,[[20,20],[21,20],[21,21],[20,20]]]}`, ParseOpts{},
			"Secondary loops not contained by first exterior loop - secondary loops must be holes"},
		{"hole crossing shell", `{"type":"Polygon","coordinates":[` + square + `,[[8,8],[12,8],[12,9],[8,9],[8,8]]]}`, ParseOpts{},
			"Secondary loops not contained by first exterior loop - secondary loops must be holes"},
		{"nested holes", `{"type":"Polygon","coordinates":[` + square + `,[[1,1],[9,1],[9,9],[1,9],[1,1]],[[2,2],[8,2],[8,8],[2,8],[2,2]]]}`, ParseOpts{},
			"Polygon interior loops cannot be nested"},
		{"crossing holes", `{"type":"Polygon","coordinates":[` + square + `,[[1,1],[5,1],[5,5],[1,5],[1,1]],[[3,3],[7,3],[7,7],[3,7],[3,3]]]}`, ParseOpts{},
			"Polygon isn't valid: Loop 1 crosses loop 2"},
		{"holes sharing an edge", `{"type":"Polygon","coordinates":[` + square + `,[[1,1],[3,1],[3,3],[1,3],[1,1]],[[3,1],[5,1],[5,3],[3,3],[3,1]]]}`, ParseOpts{},
			"Polygon isn't valid: Duplicate edge: ..."},
		{"crs not object", `{"type":"Polygon","crs":5,"coordinates":[` + square + `]}`, ParseOpts{}, "GeoJSON CRS must be an object"},
		{"crs type", `{"type":"Polygon","crs":{"type":"link"},"coordinates":[` + square + `]}`, ParseOpts{}, `GeoJSON CRS must have field "type": "name"`},
		{"crs properties", `{"type":"Polygon","crs":{"type":"name"},"coordinates":[` + square + `]}`, ParseOpts{}, `GeoJSON CRS must have field "properties" which is an object`},
		{"crs name", `{"type":"Polygon","crs":{"type":"name","properties":{}},"coordinates":[` + square + `]}`, ParseOpts{}, `GeoJSON CRS must have field "properties.name" which is a string`},
		{"crs unknown", `{"type":"Polygon","crs":{"type":"name","properties":{"name":"EPSG:3857"}},"coordinates":[` + square + `]}`, ParseOpts{}, "Unknown CRS name: EPSG:3857"},
		{"strict point", `{"type":"Point",` + strictCRS + `,"coordinates":[0,0]}`, ParseOpts{}, "Strict winding order is only supported by Polygon"},
		{"strict with hole", `{"type":"Polygon",` + strictCRS + `,"coordinates":[` + square + `,[[4,4],[6,4],[6,6],[4,6],[4,4]]]}`, ParseOpts{}, "Only one simple loop is allowed in a big polygon"},
		{"multipoint empty", `{"type":"MultiPoint","coordinates":[]}`, ParseOpts{}, "MultiPoint coordinates must have at least 1 element"},
		{"multilinestring not array", `{"type":"MultiLineString","coordinates":1}`, ParseOpts{}, "MultiLineString coordinates must be an array"},
		{"multipolygon bad member", `{"type":"MultiPolygon","coordinates":[[[[0,0],[1,1],[1,0],[0,1],[0,0]]]]}`, ParseOpts{}, "Loop is not valid: Edges 0 and 2 cross. ..."},
		{"multipolygon empty", `{"type":"MultiPolygon","coordinates":[]}`, ParseOpts{}, "MultiPolygon coordinates must have at least 1 element"},
		{"collection no geometries", `{"type":"GeometryCollection"}`, ParseOpts{}, "GeometryCollection geometries must be an array"},
		{"collection empty", `{"type":"GeometryCollection","geometries":[]}`, ParseOpts{}, "GeometryCollection geometries must have at least 1 element"},
		{"collection member not object", `{"type":"GeometryCollection","geometries":[5]}`, ParseOpts{}, `Element 0 of "geometries" is not an object`},
		{"collection nested", `{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[]}]}`, ParseOpts{}, "GeometryCollections cannot be nested"},
		{"collection bad member", `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0,100]}]}`, ParseOpts{}, "longitude/latitude is out of bounds, lng: 0 lat: 100"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, err := Parse(val(t, c.js), c.opts)
			if err == nil {
				t.Fatalf("accepted: %+v", g)
			}
			var ge *Error
			if !errors.As(err, &ge) {
				t.Fatalf("error %T is not *geo.Error", err)
			}
			got := err.Error()
			if prefix, ok := strings.CutSuffix(c.want, "..."); ok {
				if !strings.HasPrefix(got, prefix) {
					t.Fatalf("error %q, want prefix %q", got, prefix)
				}
			} else if got != c.want {
				t.Fatalf("error %q, want %q", got, c.want)
			}
		})
	}
}

func TestAbsent(t *testing.T) {
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(`{"a":1}`), false, &raw); err != nil {
		t.Fatal(err)
	}
	if !Absent(raw.Lookup("missing")) {
		t.Fatal("missing field not absent")
	}
	for _, js := range []string{`null`, `[]`, `{"$undefined":true}`} {
		if !Absent(val(t, js)) {
			t.Errorf("%s not absent", js)
		}
	}
	for _, js := range []string{`{}`, `[1,2]`, `0`, `{"type":"Point","coordinates":[0,0]}`} {
		if Absent(val(t, js)) {
			t.Errorf("%s absent", js)
		}
	}
}
