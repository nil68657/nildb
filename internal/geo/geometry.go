// Package geo implements NilDB's 2dsphere index: GeoJSON parsing with
// MongoDB's validation rules, S2 cell coverings for index entries, query
// plans made of descendant ranges and ancestor probes, exact refinement of
// $geoWithin and $geoIntersects candidates, distances, and a kNN driver
// for $near that runs over caller-supplied scan callbacks. The package
// imports no other NilDB package, so every piece is testable in memory.
//
// Distances use MongoDB's Earth radius, 6378100 m, not golang/geo's
// earth.Radius.
package geo

import (
	"github.com/golang/geo/s2"
)

// Kind names the GeoJSON type a Geometry was parsed from.
type Kind uint8

// GeoJSON types. A legacy coordinate pair parses as Point.
const (
	Point Kind = iota
	MultiPoint
	LineString
	MultiLineString
	Polygon
	MultiPolygon
	Collection
)

var kindNames = [...]string{
	Point:           "Point",
	MultiPoint:      "MultiPoint",
	LineString:      "LineString",
	MultiLineString: "MultiLineString",
	Polygon:         "Polygon",
	MultiPolygon:    "MultiPolygon",
	Collection:      "GeometryCollection",
}

// String returns the GeoJSON type name.
func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "Kind(?)"
}

// Geometry is a parsed GeoJSON value on the unit sphere.
//
// Point and MultiPoint fill Points, LineString and MultiLineString fill
// Lines, Polygon and MultiPolygon fill Polys, and Collection fills Parts.
// Kind matters only for a single Point, which is indexed at its leaf cell
// and measured without an edge query; everything else reads all four
// slices whatever the Kind, so a hand-built Geometry works as long as its
// parts are valid S2 values.
//
// A Geometry is not safe for concurrent use: golang/geo builds polygon and
// loop indexes lazily on the first query, and ShapeIndex is built lazily
// here too. Parse one per goroutine.
type Geometry struct {
	Kind   Kind
	Points []s2.Point
	Lines  []*s2.Polyline
	Polys  []*s2.Polygon
	Parts  []*Geometry // Collection

	lls    []s2.LatLng // exact parsed positions of Points, when parsed
	strict bool        // polygon parsed with the strict-winding CRS
	index  *s2.ShapeIndex
}

// LatLng returns the position of a single Point. It reports false for every
// other kind, including a MultiPoint with one member.
func (g *Geometry) LatLng() (s2.LatLng, bool) {
	if g == nil || g.Kind != Point || len(g.Points) != 1 {
		return s2.LatLng{}, false
	}
	if len(g.lls) == 1 {
		return g.lls[0], true
	}
	return s2.LatLngFromPoint(g.Points[0]), true
}

// StrictWinding reports whether the geometry is a polygon parsed with the
// crs urn:x-mongodb:crs:strictwinding:EPSG:4326, which keeps the ring's
// winding and so can describe an area larger than a hemisphere.
func (g *Geometry) StrictWinding() bool { return g != nil && g.strict }

// Region returns the geometry as an s2.Region for a RegionCoverer: the part
// itself when there is one, otherwise an s2.RegionUnion of every part
// (nested collections flattened).
func (g *Geometry) Region() s2.Region {
	var regs []s2.Region
	for _, a := range g.atoms(nil) {
		regs = append(regs, a.region())
	}
	if len(regs) == 1 {
		return regs[0]
	}
	return s2.RegionUnion(regs)
}

// ShapeIndex returns an index over every part of the geometry: one
// s2.PointVector holding all points, then each polyline and polygon. It is
// built on first use and kept.
func (g *Geometry) ShapeIndex() *s2.ShapeIndex {
	if g.index != nil {
		return g.index
	}
	idx := s2.NewShapeIndex()
	var pts s2.PointVector
	for _, a := range g.atoms(nil) {
		switch {
		case a.poly != nil:
			idx.Add(a.poly)
		case a.line != nil:
			idx.Add(a.line)
		default:
			pts = append(pts, a.pt)
		}
	}
	if len(pts) > 0 {
		idx.Add(&pts)
	}
	idx.Build()
	g.index = idx
	return idx
}

// atom is one primitive part of a geometry: a point, a polyline or a
// polygon. owner is the top-level geometry whose ShapeIndex holds it.
type atom struct {
	pt    s2.Point
	line  *s2.Polyline
	poly  *s2.Polygon
	owner *Geometry
}

func (a atom) region() s2.Region {
	switch {
	case a.poly != nil:
		return a.poly
	case a.line != nil:
		return a.line
	default:
		return a.pt
	}
}

// atoms appends every primitive part of g, descending into Parts. Nested
// parts keep g as their owner so one ShapeIndex serves the whole tree.
func (g *Geometry) atoms(dst []atom) []atom {
	return g.atomsOwned(dst, g)
}

func (g *Geometry) atomsOwned(dst []atom, owner *Geometry) []atom {
	if g == nil {
		return dst
	}
	for _, p := range g.Points {
		dst = append(dst, atom{pt: p, owner: owner})
	}
	for _, l := range g.Lines {
		dst = append(dst, atom{line: l, owner: owner})
	}
	for _, p := range g.Polys {
		dst = append(dst, atom{poly: p, owner: owner})
	}
	for _, part := range g.Parts {
		dst = part.atomsOwned(dst, owner)
	}
	return dst
}
