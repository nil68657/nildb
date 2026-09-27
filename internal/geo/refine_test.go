package geo

import (
	"math"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

func pt(lng, lat float64) string {
	return `{"type":"Point","coordinates":[` + fmtNum(lng) + `,` + fmtNum(lat) + `]}`
}

func TestRefinePairs(t *testing.T) {
	// Q is the square [0,10]x[0,10] with the hole [4,6]x[4,6].
	const q = `{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[4,4],[6,4],[6,6],[4,6],[4,4]]]}`
	// M is Q's outer square plus the square [20,30]x[0,10].
	const m = `{"type":"MultiPolygon","coordinates":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[20,0],[30,0],[30,10],[20,10],[20,0]]]]}`
	const strict = `{"type":"Polygon","crs":{"type":"name","properties":{"name":"urn:x-mongodb:crs:strictwinding:EPSG:4326"}},` +
		`"coordinates":[[[95,-5],[95,5],[105,5],[105,-5],[95,-5]]]}` // everything except a square around (100, 0)

	cases := []struct {
		name          string
		query, doc    string
		within, inter bool
	}{
		// point documents
		{"point inside", q, pt(2, 2), true, true},
		{"point in hole", q, pt(5, 5), false, false},
		{"point outside", q, pt(20, 20), false, false},
		{"point on shell vertex", q, pt(0, 0), true, true},
		{"point on hole vertex", q, pt(4, 4), true, true},
		// polyline documents
		{"line inside", q, `{"type":"LineString","coordinates":[[1,1],[3,1],[3,3]]}`, true, true},
		{"line across hole", q, `{"type":"LineString","coordinates":[[3,5],[7,5]]}`, false, true},
		{"line leaving shell", q, `{"type":"LineString","coordinates":[[5,1],[15,1]]}`, false, true},
		{"line spanning shell", q, `{"type":"LineString","coordinates":[[-5,2],[15,2]]}`, false, true},
		{"line inside hole", q, `{"type":"LineString","coordinates":[[4.5,4.5],[5.5,5.5]]}`, false, false},
		{"line outside", q, `{"type":"LineString","coordinates":[[20,20],[21,21]]}`, false, false},
		// polygon documents
		{"polygon inside", q, `{"type":"Polygon","coordinates":[[[1,1],[3,1],[3,3],[1,3],[1,1]]]}`, true, true},
		{"polygon over hole", q, `{"type":"Polygon","coordinates":[[[3,3],[7,3],[7,7],[3,7],[3,3]]]}`, false, true},
		{"polygon in hole", q, `{"type":"Polygon","coordinates":[[[4.5,4.5],[5.5,4.5],[5.5,5.5],[4.5,5.5],[4.5,4.5]]]}`, false, false},
		{"polygon overlapping edge", q, `{"type":"Polygon","coordinates":[[[8,8],[12,8],[12,12],[8,12],[8,8]]]}`, false, true},
		{"polygon disjoint", q, `{"type":"Polygon","coordinates":[[[20,20],[22,20],[22,22],[20,22],[20,20]]]}`, false, false},
		{"polygon containing query", q, `{"type":"Polygon","coordinates":[[[-5,-5],[15,-5],[15,15],[-5,15],[-5,-5]]]}`, false, true},
		{"polygon with hole inside", q, `{"type":"Polygon","coordinates":[[[0.5,0.5],[3.5,0.5],[3.5,3.5],[0.5,3.5],[0.5,0.5]],[[1,1],[2,1],[2,2],[1,2],[1,1]]]}`, true, true},
		{"holed polygon around query hole", q, `{"type":"Polygon","coordinates":[[[3,3],[7,3],[7,7],[3,7],[3,3]],[[3.5,3.5],[6.5,3.5],[6.5,6.5],[3.5,6.5],[3.5,3.5]]]}`, true, true},
		{"holed polygon over query hole", q, `{"type":"Polygon","coordinates":[[[3,3],[7,3],[7,7],[3,7],[3,3]],[[4.5,4.5],[5.5,4.5],[5.5,5.5],[4.5,5.5],[4.5,4.5]]]}`, false, true},
		// multi documents
		{"multipoint all inside", q, `{"type":"MultiPoint","coordinates":[[1,1],[9,9]]}`, true, true},
		{"multipoint one outside", q, `{"type":"MultiPoint","coordinates":[[1,1],[19,19]]}`, false, true},
		{"multipoint all outside", q, `{"type":"MultiPoint","coordinates":[[-1,-1],[5,5]]}`, false, false},
		{"multilinestring half out", q, `{"type":"MultiLineString","coordinates":[[[1,1],[2,2]],[[20,20],[21,21]]]}`, false, true},
		{"multipolygon inside", q, `{"type":"MultiPolygon","coordinates":[[[[1,1],[2,1],[2,2],[1,1]]],[[[8,8],[9,8],[9,9],[8,8]]]]}`, true, true},
		{"collection inside", q, `{"type":"GeometryCollection","geometries":[` + pt(2, 2) + `,{"type":"LineString","coordinates":[[7,7],[8,8]]}]}`, true, true},
		{"collection half out", q, `{"type":"GeometryCollection","geometries":[` + pt(2, 2) + `,{"type":"Polygon","coordinates":[[[20,20],[21,20],[21,21],[20,20]]]}]}`, false, true},
		// multipolygon queries: each part needs one containing polygon
		{"multi query point in second", m, pt(25, 5), true, true},
		{"multi query points split", m, `{"type":"MultiPoint","coordinates":[[5,5],[25,5]]}`, true, true},
		{"multi query line bridging", m, `{"type":"LineString","coordinates":[[5,5],[25,5]]}`, false, true},
		{"multi query polygon in gap", m, `{"type":"Polygon","coordinates":[[[12,2],[18,2],[18,8],[12,8],[12,2]]]}`, false, false},
		// big polygon query
		{"big polygon far point", strict, pt(0, 0), true, true},
		{"big polygon excluded point", strict, pt(100, 0), false, false},
		{"big polygon doc polygon", strict, `{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,0]]]}`, true, true},
		{"big polygon straddling doc", strict, `{"type":"Polygon","coordinates":[[[90,-1],[100,-1],[100,1],[90,1],[90,-1]]]}`, false, true},
		// non-polygon queries: within never holds
		{"point query in polygon", pt(2, 2), q, false, true},
		{"point query in hole", pt(5, 5), q, false, false},
		{"point query same point", pt(2, 2), pt(2, 2), false, true},
		{"point query other point", pt(2, 2), pt(2.001, 2), false, false},
		{"point query on meridian line", pt(2, 2), `{"type":"LineString","coordinates":[[2,-1],[2,3]]}`, false, true},
		{"point query beside line", pt(2.01, 2), `{"type":"LineString","coordinates":[[2,-1],[2,3]]}`, false, false},
		{"line query crossing line", `{"type":"LineString","coordinates":[[0,0],[2,2]]}`, `{"type":"LineString","coordinates":[[0,2],[2,0]]}`, false, true},
		{"line query parallel line", `{"type":"LineString","coordinates":[[0,0],[2,0]]}`, `{"type":"LineString","coordinates":[[0,1],[2,1]]}`, false, false},
		{"line query into polygon", `{"type":"LineString","coordinates":[[-5,5],[2,5]]}`, q, false, true},
		{"line query inside hole", `{"type":"LineString","coordinates":[[4.5,5],[5.5,5]]}`, q, false, false},
		{"collection query", `{"type":"GeometryCollection","geometries":[` + pt(50, 50) + `,` + pt(2, 2) + `]}`, q, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			query := mustParse(t, c.query, ParseOpts{})
			doc := mustParse(t, c.doc, ParseOpts{})
			if got := Refine(Within, query, doc); got != c.within {
				t.Errorf("Within = %v, want %v", got, c.within)
			}
			if got := Refine(Intersects, query, doc); got != c.inter {
				t.Errorf("Intersects = %v, want %v", got, c.inter)
			}
			// Intersects is symmetric.
			if got := Refine(Intersects, doc, query); got != c.inter {
				t.Errorf("reversed Intersects = %v, want %v", got, c.inter)
			}
		})
	}
	if Refine(Within, nil, mustParse(t, pt(0, 0), ParseOpts{})) || Refine(Intersects, &Geometry{Kind: MultiPoint}, mustParse(t, pt(0, 0), ParseOpts{})) {
		t.Fatal("empty geometry matched")
	}
}

func deg(a s1.Angle) float64 { return a.Degrees() }

func TestDistance(t *testing.T) {
	from := func(lng, lat float64) s2.Point { return s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng)) }
	holed := mustParse(t, `{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[4,4],[6,4],[6,6],[4,6],[4,4]]]}`, ParseOpts{})

	if d := Distance(from(2, 2), holed); d != 0 {
		t.Fatalf("inside polygon: %v", d)
	}
	// From the hole's centre the nearest boundary is the hole's meridian
	// edge, 1 degree of longitude at latitude 5.
	want := math.Asin(math.Cos(5*math.Pi/180)*math.Sin(1*math.Pi/180)) * 180 / math.Pi
	if d := deg(Distance(from(5, 5), holed)); math.Abs(d-want) > 1e-9 {
		t.Fatalf("in hole: %v deg, want %v", d, want)
	}
	if d := deg(Distance(from(20, 5), holed)); d < 9.9 || d > 10 {
		t.Fatalf("outside: %v deg", d)
	}

	p := mustParse(t, pt(1, 0), ParseOpts{})
	if d := deg(Distance(from(0, 0), p)); math.Abs(d-1) > 1e-12 {
		t.Fatalf("point: %v deg", d)
	}
	line := mustParse(t, `{"type":"LineString","coordinates":[[0,-1],[0,3]]}`, ParseOpts{})
	want = math.Asin(math.Cos(1*math.Pi/180)*math.Sin(2*math.Pi/180)) * 180 / math.Pi
	if d := deg(Distance(from(2, 1), line)); math.Abs(d-want) > 1e-9 {
		t.Fatalf("line: %v deg, want %v", d, want)
	}
	mp := mustParse(t, `{"type":"MultiPoint","coordinates":[[10,0],[0,3],[-50,0]]}`, ParseOpts{})
	if d := deg(Distance(from(0, 0), mp)); math.Abs(d-3) > 1e-9 {
		t.Fatalf("multipoint: %v deg", d)
	}
	coll := mustParse(t, `{"type":"GeometryCollection","geometries":[`+pt(40, 0)+`,{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}]}`, ParseOpts{})
	if d := Distance(from(0, 0), coll); d != 0 {
		t.Fatalf("collection containing the point: %v", d)
	}
	if d := Distance(from(0, 0), nil); !math.IsInf(float64(d), 1) {
		t.Fatalf("nil geometry: %v", d)
	}
}

func TestWithinCap(t *testing.T) {
	c := s2.CapFromCenterAngle(s2.PointFromLatLng(s2.LatLngFromDegrees(0, 0)), s1.Angle(10*math.Pi/180))
	cases := []struct {
		doc  string
		want bool
	}{
		{pt(5, 0), true},
		{pt(15, 0), false},
		{`{"type":"LineString","coordinates":[[-5,0],[5,1]]}`, true},
		{`{"type":"LineString","coordinates":[[-5,0],[15,1]]}`, false},
		{`{"type":"Polygon","coordinates":[[[-2,-2],[2,-2],[2,2],[-2,2],[-2,-2]]]}`, true},
		{`{"type":"MultiPoint","coordinates":[[1,1],[20,0]]}`, false},
		// Same ring, strict winding the other way: everything but the square.
		{`{"type":"Polygon","crs":{"type":"name","properties":{"name":"urn:x-mongodb:crs:strictwinding:EPSG:4326"}},"coordinates":[[[-2,-2],[-2,2],[2,2],[2,-2],[-2,-2]]]}`, false},
	}
	for _, tc := range cases {
		if got := WithinCap(c, mustParse(t, tc.doc, ParseOpts{})); got != tc.want {
			t.Errorf("WithinCap(10 deg, %s) = %v, want %v", tc.doc, got, tc.want)
		}
	}
	// A cap of 170 degrees excludes 10 degrees around (180, 0). Both
	// vertices of this line lie in the cap, its edge crosses the gap.
	big := s2.CapFromCenterAngle(s2.PointFromLatLng(s2.LatLngFromDegrees(0, 0)), s1.Angle(170*math.Pi/180))
	gapLine := mustParse(t, `{"type":"LineString","coordinates":[[170,5],[-170,5]]}`, ParseOpts{})
	if WithinCap(big, gapLine) {
		t.Error("line through the excluded cap counted as within")
	}
	if !WithinCap(big, mustParse(t, `{"type":"LineString","coordinates":[[170,15],[160,15]]}`, ParseOpts{})) {
		t.Error("line clear of the excluded cap not within")
	}
	if !WithinCap(s2.FullCap(), gapLine) || WithinCap(s2.EmptyCap(), gapLine) {
		t.Error("full or empty cap wrong")
	}
}
