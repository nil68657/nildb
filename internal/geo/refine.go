package geo

import (
	"math"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// Predicate selects the relation Refine tests.
type Predicate uint8

// Predicates of $geoWithin and $geoIntersects.
const (
	Within Predicate = iota
	Intersects
)

// Refine reports whether doc satisfies pred against query exactly. Index
// candidates are a superset, so every candidate goes through Refine.
//
// Within ($geoWithin $geometry) uses only the polygon parts of query and
// asks that every part of doc lies inside one query polygon: a point is
// contained (a point on the boundary counts, as in MongoDB, through its
// leaf cell), a polyline has every vertex contained and no edge crossing
// the polygon boundary, a polygon is contained by Polygon.Contains. A query
// without polygons contains nothing.
//
// Intersects ($geoIntersects) holds when any part of query meets any part
// of doc: point and point share a leaf cell, point and polyline when the
// polyline meets the point's leaf cell, point and polygon when the polygon
// contains the point, polyline and polyline by Polyline.Intersects,
// polyline and polygon when a vertex lies inside or an edge touches the
// boundary, polygon and polygon by Polygon.Intersects.
func Refine(pred Predicate, query, doc *Geometry) bool {
	if query == nil || doc == nil {
		return false
	}
	qs := query.atoms(nil)
	ds := doc.atoms(nil)
	if len(qs) == 0 || len(ds) == 0 {
		return false
	}
	if pred == Within {
		var polys []atom
		for _, q := range qs {
			if q.poly != nil {
				polys = append(polys, q)
			}
		}
		if len(polys) == 0 {
			return false
		}
		for _, d := range ds {
			if !withinAny(d, polys) {
				return false
			}
		}
		return true
	}
	for _, q := range qs {
		for _, d := range ds {
			if intersects(q, d) {
				return true
			}
		}
	}
	return false
}

func withinAny(d atom, polys []atom) bool {
	for _, q := range polys {
		if within(d, q) {
			return true
		}
	}
	return false
}

// within reports whether atom d lies inside polygon atom q.
func within(d, q atom) bool {
	switch {
	case d.poly != nil:
		return q.poly.Contains(d.poly)
	case d.line != nil:
		return lineWithin(*d.line, q)
	default:
		return polyContainsPoint(q.poly, d.pt)
	}
}

func lineWithin(line s2.Polyline, q atom) bool {
	for _, v := range line {
		if !polyContainsPoint(q.poly, v) {
			return false
		}
	}
	cq := s2.NewCrossingEdgeQuery(q.owner.ShapeIndex())
	for i := 1; i < len(line); i++ {
		if len(cq.Crossings(line[i-1], line[i], q.poly, s2.CrossingTypeInterior)) > 0 {
			return false
		}
	}
	return true
}

// intersects is symmetric in its two atoms.
func intersects(a, b atom) bool {
	// Order the pair so that a has the lower dimension.
	if dim(a) > dim(b) {
		a, b = b, a
	}
	switch {
	case a.poly != nil: // polygon, polygon
		return a.poly.Intersects(b.poly)
	case a.line != nil && b.line != nil:
		return a.line.Intersects(b.line)
	case a.line != nil: // polyline, polygon
		return lineIntersectsPoly(*a.line, b)
	case b.poly != nil: // point, polygon
		return polyContainsPoint(b.poly, a.pt)
	case b.line != nil: // point, polyline
		return b.line.IntersectsCell(s2.CellFromPoint(a.pt))
	default: // point, point
		return s2.CellFromPoint(a.pt).ID() == s2.CellFromPoint(b.pt).ID()
	}
}

func dim(a atom) int {
	switch {
	case a.poly != nil:
		return 2
	case a.line != nil:
		return 1
	}
	return 0
}

func lineIntersectsPoly(line s2.Polyline, q atom) bool {
	if !q.poly.RectBound().Intersects(line.RectBound()) {
		return false
	}
	for _, v := range line {
		if polyContainsPoint(q.poly, v) {
			return true
		}
	}
	cq := s2.NewCrossingEdgeQuery(q.owner.ShapeIndex())
	for i := 1; i < len(line); i++ {
		if len(cq.Crossings(line[i-1], line[i], q.poly, s2.CrossingTypeAll)) > 0 {
			return true
		}
	}
	return false
}

// polyContainsPoint is Polygon.ContainsPoint with MongoDB's boundary rule:
// a point whose leaf cell touches the polygon is inside, so a vertex or a
// point on an edge is contained whichever way S2's semi-open model would
// assign it.
func polyContainsPoint(p *s2.Polygon, pt s2.Point) bool {
	return p.ContainsPoint(pt) || p.IntersectsCell(s2.CellFromPoint(pt))
}

// WithinCap reports whether every part of doc lies inside c, for
// $geoWithin $centerSphere: every point and every vertex inside the cap, no
// polygon containing the cap's antipode (which rules out a strict-winding
// polygon wrapped around the outside), and for a cap larger than a
// hemisphere no edge passing through the excluded small cap. A cap up to a
// hemisphere is convex, so its edges need no further test.
func WithinCap(c s2.Cap, doc *Geometry) bool {
	ds := doc.atoms(nil)
	if len(ds) == 0 || c.IsEmpty() {
		return false
	}
	if c.IsFull() {
		return true
	}
	big := c.Radius() > math.Pi/2
	hole := c.Complement() // centred on the antipode of c
	edgeOK := func(a, b s2.Point) bool {
		if !c.ContainsPoint(a) || !c.ContainsPoint(b) {
			return false
		}
		return !big || s2.DistanceFromSegment(hole.Center(), a, b) >= hole.Radius()
	}
	for _, d := range ds {
		switch {
		case d.poly != nil:
			if d.poly.ContainsPoint(hole.Center()) {
				return false
			}
			for _, l := range d.poly.Loops() {
				n := l.NumVertices()
				for i := 0; i < n; i++ {
					if !edgeOK(l.Vertex(i), l.Vertex(i+1)) {
						return false
					}
				}
			}
		case d.line != nil:
			line := *d.line
			for i := range line {
				if !c.ContainsPoint(line[i]) {
					return false
				}
				if i > 0 && !edgeOK(line[i-1], line[i]) {
					return false
				}
			}
		default:
			if !c.ContainsPoint(d.pt) {
				return false
			}
		}
	}
	return true
}

// Distance returns the angle from `from` to the nearest point of doc: the
// point distance for a single Point, otherwise the result of an s2
// ClosestEdgeQuery over doc's ShapeIndex, which counts polygon interiors,
// so the distance is 0 when a polygon of doc contains from. golang/geo's
// Polygon has no DistanceToPoint, which is why the edge query is used.
func Distance(from s2.Point, doc *Geometry) s1.Angle {
	if doc == nil {
		return s1.InfAngle()
	}
	if doc.Kind == Point && len(doc.Points) == 1 && len(doc.Lines) == 0 && len(doc.Polys) == 0 && len(doc.Parts) == 0 {
		return pointDistance(from, doc.Points[0])
	}
	idx := doc.ShapeIndex()
	if idx.Len() == 0 {
		return s1.InfAngle()
	}
	q := s2.NewClosestEdgeQuery(idx, s2.NewClosestEdgeQueryOptions().IncludeInteriors(true))
	d := q.Distance(s2.NewMinDistanceToPointTarget(from))
	if d.IsInfinity() {
		return s1.InfAngle()
	}
	return d.Angle()
}

// pointDistance is the one formula both Near and Distance use for point
// documents, so a distance computed from an index value equals the one
// computed from the parsed document bit for bit.
func pointDistance(center, p s2.Point) s1.Angle { return center.Distance(p) }
