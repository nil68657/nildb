package geo

import (
	"fmt"
	"strconv"

	"github.com/golang/geo/s2"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ParseOpts controls which encodings Parse accepts besides GeoJSON.
type ParseOpts struct {
	// AllowLegacyPair accepts a bare [lon, lat] array or an embedded
	// document whose first two fields are numbers ({x, y}, {lng, lat}).
	// GeoJSON is tried first, as in 2dsphere index version 4. Index
	// maintenance sets it; $geometry parsing does not.
	AllowLegacyPair bool
}

// CRS names accepted in a GeoJSON "crs" member.
const (
	CRSEPSG4326      = "EPSG:4326"
	CRSOGC84         = "urn:ogc:def:crs:OGC:1.3:CRS84"
	CRSStrictWinding = "urn:x-mongodb:crs:strictwinding:EPSG:4326"
)

// Error is a GeoJSON validation failure. Its text is MongoDB's reason for
// the same input (error code 2, BadValue) without the dump of the offending
// BSON element that MongoDB appends. Callers reply "ERR " + Error().
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

func errorf(format string, a ...any) error { return &Error{msg: fmt.Sprintf(format, a...)} }

// Absent reports whether v is missing, null, undefined or an empty array.
// A 2dsphere index is always sparse and indexes nothing for such a value;
// Parse rejects them.
func Absent(v bson.RawValue) bool {
	switch v.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return true
	case bson.TypeArray:
		arr, ok := v.ArrayOK()
		if !ok {
			return false
		}
		vals, err := arr.Values()
		return err == nil && len(vals) == 0
	}
	return false
}

// Parse converts a GeoJSON object, or with o.AllowLegacyPair a legacy
// coordinate pair, into a Geometry. Positions are [lon, lat] in degrees
// with lon in [-180, 180] and lat in [-90, 90]; GeoJSON positions may carry
// extra elements such as altitude, which are ignored. Every error is a
// *Error carrying MongoDB's text, such as "Loop is not valid: Edges 0 and 2
// cross. ...".
//
// Polygon rings must be closed and have at least three distinct vertices
// (four positions); consecutive duplicate positions are removed first. Each
// ring must not self-intersect, holes must lie inside the first ring and
// must not cross or nest. Without a crs every ring is normalised to the
// smaller of the two areas it bounds; with crs
// urn:x-mongodb:crs:strictwinding:EPSG:4326 (single-ring Polygon only) the
// interior is on the left of the ring, which can exceed a hemisphere.
func Parse(v bson.RawValue, o ParseOpts) (*Geometry, error) {
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		doc, ok := v.DocumentOK()
		if !ok {
			return nil, errorf("geo element must be an array or object")
		}
		if _, err := doc.LookupErr("type"); err == nil {
			return parseGeoJSON(doc, true)
		}
		if o.AllowLegacyPair {
			vals, err := doc.Values()
			if err != nil {
				return nil, errorf("geo element must be an array or object")
			}
			if len(vals) > 0 && isNumber(vals[0]) {
				return parseLegacyPair(vals)
			}
		}
		return nil, errorf(`unknown GeoJSON type: object has no "type" field`)
	case bson.TypeArray:
		if !o.AllowLegacyPair {
			return nil, errorf("legacy coordinate pairs are not accepted here, use a GeoJSON object")
		}
		arr, ok := v.ArrayOK()
		if !ok {
			return nil, errorf("geo element must be an array or object")
		}
		vals, err := arr.Values()
		if err != nil {
			return nil, errorf("geo element must be an array or object")
		}
		return parseLegacyPair(vals)
	default:
		return nil, errorf("geo element must be an array or object")
	}
}

func parseLegacyPair(vals []bson.RawValue) (*Geometry, error) {
	if len(vals) < 2 {
		return nil, errorf("Point must only contain numeric elements")
	}
	x, okx := number(vals[0])
	y, oky := number(vals[1])
	if !okx || !oky {
		return nil, errorf("Point must only contain numeric elements")
	}
	if len(vals) > 2 {
		return nil, errorf("Point must only contain two numeric elements")
	}
	ll, err := lngLat(x, y)
	if err != nil {
		return nil, err
	}
	return pointGeometry(ll), nil
}

func pointGeometry(ll s2.LatLng) *Geometry {
	return &Geometry{Kind: Point, Points: []s2.Point{s2.PointFromLatLng(ll)}, lls: []s2.LatLng{ll}}
}

func parseGeoJSON(doc bson.Raw, top bool) (*Geometry, error) {
	name, ok := doc.Lookup("type").StringValueOK()
	if !ok {
		return nil, errorf(`unknown GeoJSON type: "type" must be a string`)
	}
	switch name {
	case "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection":
	default:
		return nil, errorf("unknown GeoJSON type: %s", name)
	}

	strict := false
	if top {
		if crs, err := doc.LookupErr("crs"); err == nil {
			if strict, err = parseCRS(crs); err != nil {
				return nil, err
			}
		}
	}
	if strict && name != "Polygon" {
		return nil, errorf("Strict winding order is only supported by Polygon")
	}

	coords := doc.Lookup("coordinates")
	switch name {
	case "Point":
		ll, err := parsePosition(coords)
		if err != nil {
			return nil, err
		}
		return pointGeometry(ll), nil

	case "LineString":
		pl, err := parseLine(coords)
		if err != nil {
			return nil, err
		}
		return &Geometry{Kind: LineString, Lines: []*s2.Polyline{pl}}, nil

	case "Polygon":
		p, err := parsePolygon(coords, strict)
		if err != nil {
			return nil, err
		}
		return &Geometry{Kind: Polygon, Polys: []*s2.Polygon{p}, strict: strict}, nil

	case "MultiPoint":
		elems, err := multiCoords(coords, "MultiPoint")
		if err != nil {
			return nil, err
		}
		g := &Geometry{Kind: MultiPoint}
		for _, e := range elems {
			ll, err := parsePosition(e)
			if err != nil {
				return nil, err
			}
			g.lls = append(g.lls, ll)
			g.Points = append(g.Points, s2.PointFromLatLng(ll))
		}
		return g, nil

	case "MultiLineString":
		elems, err := multiCoords(coords, "MultiLineString")
		if err != nil {
			return nil, err
		}
		g := &Geometry{Kind: MultiLineString}
		for _, e := range elems {
			pl, err := parseLine(e)
			if err != nil {
				return nil, err
			}
			g.Lines = append(g.Lines, pl)
		}
		return g, nil

	case "MultiPolygon":
		elems, err := multiCoords(coords, "MultiPolygon")
		if err != nil {
			return nil, err
		}
		g := &Geometry{Kind: MultiPolygon}
		for _, e := range elems {
			p, err := parsePolygon(e, false)
			if err != nil {
				return nil, err
			}
			g.Polys = append(g.Polys, p)
		}
		return g, nil

	default: // GeometryCollection
		arr, ok := doc.Lookup("geometries").ArrayOK()
		if !ok {
			return nil, errorf("GeometryCollection geometries must be an array")
		}
		vals, err := arr.Values()
		if err != nil {
			return nil, errorf("GeometryCollection geometries must be an array")
		}
		if len(vals) == 0 {
			return nil, errorf("GeometryCollection geometries must have at least 1 element")
		}
		g := &Geometry{Kind: Collection}
		for i, v := range vals {
			d, ok := v.DocumentOK()
			if !ok {
				return nil, errorf(`Element %d of "geometries" is not an object`, i)
			}
			if t, _ := d.Lookup("type").StringValueOK(); t == "GeometryCollection" {
				return nil, errorf("GeometryCollections cannot be nested")
			}
			part, err := parseGeoJSON(d, false)
			if err != nil {
				return nil, err
			}
			g.Parts = append(g.Parts, part)
		}
		return g, nil
	}
}

// parseCRS reports whether crs names the strict-winding CRS.
func parseCRS(v bson.RawValue) (bool, error) {
	doc, ok := v.DocumentOK()
	if !ok {
		return false, errorf("GeoJSON CRS must be an object")
	}
	if t, ok := doc.Lookup("type").StringValueOK(); !ok || t != "name" {
		return false, errorf(`GeoJSON CRS must have field "type": "name"`)
	}
	props, ok := doc.Lookup("properties").DocumentOK()
	if !ok {
		return false, errorf(`GeoJSON CRS must have field "properties" which is an object`)
	}
	name, ok := props.Lookup("name").StringValueOK()
	if !ok {
		return false, errorf(`GeoJSON CRS must have field "properties.name" which is a string`)
	}
	switch name {
	case CRSEPSG4326, CRSOGC84:
		return false, nil
	case CRSStrictWinding:
		return true, nil
	}
	return false, errorf("Unknown CRS name: %s", name)
}

func multiCoords(v bson.RawValue, typ string) ([]bson.RawValue, error) {
	arr, ok := v.ArrayOK()
	if !ok {
		return nil, errorf("%s coordinates must be an array", typ)
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, errorf("%s coordinates must be an array", typ)
	}
	if len(vals) == 0 {
		return nil, errorf("%s coordinates must have at least 1 element", typ)
	}
	return vals, nil
}

// parsePosition reads one GeoJSON position [lon, lat, ...].
func parsePosition(v bson.RawValue) (s2.LatLng, error) {
	arr, ok := v.ArrayOK()
	if !ok {
		return s2.LatLng{}, errorf("GeoJSON coordinates must be an array")
	}
	vals, err := arr.Values()
	if err != nil || len(vals) < 2 {
		return s2.LatLng{}, errorf("Point must only contain numeric elements")
	}
	x, okx := number(vals[0])
	y, oky := number(vals[1])
	if !okx || !oky {
		return s2.LatLng{}, errorf("Point must only contain numeric elements")
	}
	return lngLat(x, y)
}

// parsePositions reads an array of positions.
func parsePositions(v bson.RawValue) ([]s2.LatLng, error) {
	arr, ok := v.ArrayOK()
	if !ok {
		return nil, errorf("GeoJSON coordinates must be an array of coordinates")
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, errorf("GeoJSON coordinates must be an array of coordinates")
	}
	out := make([]s2.LatLng, 0, len(vals))
	for _, e := range vals {
		ll, err := parsePosition(e)
		if err != nil {
			return nil, err
		}
		out = append(out, ll)
	}
	return out, nil
}

func parseLine(v bson.RawValue) (*s2.Polyline, error) {
	lls, err := parsePositions(v)
	if err != nil {
		return nil, err
	}
	pts := dedupeConsecutive(toPoints(lls))
	if len(pts) < 2 {
		return nil, errorf("GeoJSON LineString must have at least 2 vertices")
	}
	pl := s2.Polyline(pts)
	if err := pl.Validate(); err != nil {
		return nil, errorf("GeoJSON LineString is not valid: %v", err)
	}
	return &pl, nil
}

func parsePolygon(v bson.RawValue, strict bool) (*s2.Polygon, error) {
	arr, ok := v.ArrayOK()
	if !ok {
		return nil, errorf("Polygon coordinates must be an array")
	}
	rings, err := arr.Values()
	if err != nil {
		return nil, errorf("Polygon coordinates must be an array")
	}
	if len(rings) == 0 {
		return nil, errorf("Polygon has no loops.")
	}
	if strict && len(rings) != 1 {
		return nil, errorf("Only one simple loop is allowed in a big polygon")
	}
	loops := make([]*s2.Loop, 0, len(rings))
	for i, ring := range rings {
		loop, err := parseRing(ring)
		if err != nil {
			return nil, err
		}
		if !strict {
			loop.Normalize()
		}
		if i > 0 && !loops[0].Contains(loop) {
			return nil, errorf("Secondary loops not contained by first exterior loop - secondary loops must be holes")
		}
		loops = append(loops, loop)
	}
	if err := checkHoles(loops); err != nil {
		return nil, err
	}
	p := s2.PolygonFromLoops(loops)
	if err := p.Validate(); err != nil {
		return nil, errorf("Polygon isn't valid: %v", err)
	}
	return p, nil
}

// parseRing turns a closed GeoJSON ring into a loop without its closing
// vertex and checks it with loopError.
func parseRing(v bson.RawValue) (*s2.Loop, error) {
	lls, err := parsePositions(v)
	if err != nil {
		return nil, err
	}
	if len(lls) == 0 {
		return nil, errorf("Loop has no vertices")
	}
	pts := toPoints(lls)
	if pts[0] != pts[len(pts)-1] {
		return nil, errorf("Loop is not closed, first vertex does not equal last vertex")
	}
	pts = dedupeConsecutive(pts)
	pts = pts[:len(pts)-1]
	if len(pts) < 3 {
		return nil, errorf("Loop must have at least 3 different vertices")
	}
	loop := s2.LoopFromPoints(pts)
	if reason := loopError(loop, pts); reason != "" {
		return nil, errorf("Loop is not valid: %s", reason)
	}
	return loop, nil
}

// loopError returns why a loop is invalid, or "". golang/geo's
// Loop.Validate does not look for self-intersections (its findAnyCrossing
// is a TODO), so duplicate vertices and crossing edges are checked here
// with a CrossingEdgeQuery over the loop's edges.
func loopError(loop *s2.Loop, pts []s2.Point) string {
	if err := loop.Validate(); err != nil {
		return err.Error()
	}
	seen := make(map[s2.Point]int, len(pts))
	for i, p := range pts {
		if j, ok := seen[p]; ok {
			return fmt.Sprintf("Duplicate vertices: %d and %d", j, i)
		}
		seen[p] = i
	}
	n := len(pts)
	idx := s2.NewShapeIndex()
	idx.Add(loop)
	q := s2.NewCrossingEdgeQuery(idx)
	for i := 0; i < n; i++ {
		a, b := pts[i], pts[(i+1)%n]
		for _, j := range q.Crossings(a, b, loop, s2.CrossingTypeAll) {
			if j <= i || j == (i+1)%n || j == (i+n-1)%n {
				continue
			}
			c, d := pts[j], pts[(j+1)%n]
			return fmt.Sprintf("Edges %d and %d cross. Edge locations in degrees: %s-%s and %s-%s",
				i, j, degrees(a), degrees(b), degrees(c), degrees(d))
		}
	}
	return ""
}

// checkHoles rejects edges shared between loops, holes nested in holes and
// holes that cross each other. loops[0] is the shell and already contains
// every hole.
func checkHoles(loops []*s2.Loop) error {
	if len(loops) < 2 {
		return nil
	}
	type edgeRef struct{ loop, edge int }
	edges := make(map[[2]s2.Point]edgeRef)
	for li, l := range loops {
		n := l.NumVertices()
		for k := 0; k < n; k++ {
			a, b := l.Vertex(k), l.Vertex(k+1)
			if prev, ok := edges[[2]s2.Point{a, b}]; ok && prev.loop != li {
				return errorf("Polygon isn't valid: Duplicate edge: loop %d, edge %d and loop %d, edge %d", prev.loop, prev.edge, li, k)
			}
			if prev, ok := edges[[2]s2.Point{b, a}]; ok && prev.loop != li {
				return errorf("Polygon isn't valid: Duplicate edge: loop %d, edge %d and loop %d, edge %d", prev.loop, prev.edge, li, k)
			}
			edges[[2]s2.Point{a, b}] = edgeRef{li, k}
		}
	}
	for i := 1; i < len(loops); i++ {
		for j := i + 1; j < len(loops); j++ {
			if loops[i].Contains(loops[j]) || loops[j].Contains(loops[i]) {
				return errorf("Polygon interior loops cannot be nested")
			}
			if loops[i].Intersects(loops[j]) {
				return errorf("Polygon isn't valid: Loop %d crosses loop %d", i, j)
			}
		}
	}
	return nil
}

func degrees(p s2.Point) string {
	ll := s2.LatLngFromPoint(p)
	return fmt.Sprintf("[%.7f, %.7f]", ll.Lat.Degrees(), ll.Lng.Degrees())
}

func toPoints(lls []s2.LatLng) []s2.Point {
	pts := make([]s2.Point, len(lls))
	for i, ll := range lls {
		pts[i] = s2.PointFromLatLng(ll)
	}
	return pts
}

func dedupeConsecutive(pts []s2.Point) []s2.Point {
	if len(pts) < 2 {
		return pts
	}
	out := pts[:1]
	for _, p := range pts[1:] {
		if p != out[len(out)-1] {
			out = append(out, p)
		}
	}
	return out
}

// lngLat validates a GeoJSON position and converts it. NaN fails the range
// test.
func lngLat(lng, lat float64) (s2.LatLng, error) {
	if !(lng >= -180 && lng <= 180 && lat >= -90 && lat <= 90) {
		return s2.LatLng{}, errorf("longitude/latitude is out of bounds, lng: %s lat: %s", fmtNum(lng), fmtNum(lat))
	}
	return s2.LatLngFromDegrees(lat, lng), nil
}

func fmtNum(x float64) string { return strconv.FormatFloat(x, 'g', -1, 64) }

func isNumber(v bson.RawValue) bool {
	_, ok := number(v)
	return ok
}

// number reads a BSON double, int32, int64 or Decimal128 as a float64.
func number(v bson.RawValue) (float64, bool) {
	if v.Type == bson.TypeDecimal128 {
		d, ok := v.Decimal128OK()
		if !ok {
			return 0, false
		}
		f, err := strconv.ParseFloat(d.String(), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return v.AsFloat64OK()
}
