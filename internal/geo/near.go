package geo

import (
	"bytes"
	"container/heap"
	"math"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// Scan visits every index entry whose cell lies in r, in key order. ksID
// and val may alias iterator memory valid only during the visit call;
// visit returns false to stop the scan early.
type Scan func(r Range, visit func(cell s2.CellID, ksID, val []byte) bool) error

// Probe visits every index entry stored at exactly cell.
type Probe func(cell s2.CellID, visit func(ksID, val []byte) bool) error

// Load returns the parsed geometry of a document's indexed field. It
// returns (nil, nil) when the document is gone or no longer indexable, and
// Near skips it.
type Load func(ksID []byte) (*Geometry, error)

// NearOpts bound a kNN search.
type NearOpts struct {
	// Min and Max bound the distance, both inclusive. Min below 0 counts as
	// 0. Max of 0 or below means no upper bound; the query layer passes
	// s1.Angle(math.SmallestNonzeroFloat64) for $maxDistance: 0.
	Min, Max      s1.Angle
	Limit         int // 0 = unlimited
	QueryMaxCells int // 0 = DefaultQueryMaxCells
	// Filter applies the rest of the query to a candidate inside the
	// distance bounds; nil accepts every candidate. Near calls it at most
	// once per ksID.
	Filter func(ksID []byte) bool
}

// nearSeed is the first round's width, 1 km as in the design.
const nearSeed = 1000.0

// Near emits documents in ascending distance from center, ties broken by
// ksID bytes, until emit returns false, Limit documents are out, or every
// document within Max has been emitted.
//
// It runs MongoDB's expanding annulus. Each round covers the cap of radius
// hi around center (hi starts at Min + 1 km, capped at Max), subtracts the
// cells scanned in earlier rounds, scans the remaining descendant ranges
// and the ancestor probes not issued before, and computes the exact
// distance of each new candidate: points from the index value without a
// row read, other shapes through Load and Distance. Candidates within
// [Min, Max] that pass Filter go on a min-heap. Because the covering is a
// superset of the cap, every document within hi has been seen once the
// round ends, so heap entries at or below hi are emitted in order. The
// width of the next round doubles when this round emitted fewer than 300
// documents and halves above 600. Cells wholly inside the cap of radius
// Min are never scanned.
//
// Memory is the candidates beyond the current radius plus a set of the
// ksIDs seen so far.
func Near(p Params, center s2.Point, o NearOpts, scan Scan, probe Probe, load Load,
	emit func(ksID []byte, d s1.Angle) bool) error {
	return near(p, center, o, scan, probe, load, emit, nil)
}

// near is Near with a hook that sees every completed round's outer radius
// and emitted count, so tests can check the width schedule.
func near(p Params, center s2.Point, o NearOpts, scan Scan, probe Probe, load Load,
	emit func(ksID []byte, d s1.Angle) bool, onRound func(hi s1.Angle, emitted int)) error {
	maxA := o.Max
	if maxA <= 0 || maxA > math.Pi {
		maxA = math.Pi
	}
	minA := o.Min
	if minA < 0 {
		minA = 0
	}
	if minA > maxA {
		return nil
	}

	var scanned s2.CellUnion
	if minA > 2*annulusPad {
		inner := s2.CapFromCenterAngle(center, minA-2*annulusPad)
		scanned = s2.CellUnionFromUnion(p.queryCoverer(o.QueryMaxCells).InteriorCovering(inner))
	}

	n := &nearRun{
		center: center, minA: minA, maxA: maxA, filter: o.Filter, load: load,
		seen: make(map[string]struct{}),
	}
	probed := make(map[s2.CellID]struct{})
	emitted := 0
	step := Angle(nearSeed)
	hi := min(maxA, minA+step)

	for {
		plan, next := p.PlanAnnulus(center, hi, scanned, o.QueryMaxCells)
		scanned = next
		for _, r := range plan.Ranges {
			if err := scan(r, n.visitCell); err != nil {
				return err
			}
			if n.err != nil {
				return n.err
			}
		}
		for _, c := range plan.Probes {
			if _, ok := probed[c]; ok {
				continue
			}
			probed[c] = struct{}{}
			if err := probe(c, n.visit); err != nil {
				return err
			}
			if n.err != nil {
				return n.err
			}
		}

		final := hi >= maxA || coversSphere(scanned)
		round := 0
		for n.h.Len() > 0 && (final || n.h[0].d <= hi) {
			it := heap.Pop(&n.h).(nearItem)
			if !emit(it.id, it.d) {
				return nil
			}
			emitted++
			round++
			if o.Limit > 0 && emitted >= o.Limit {
				return nil
			}
		}
		if onRound != nil {
			onRound(hi, round)
		}
		if final {
			return nil
		}
		switch {
		case round < 300:
			step *= 2
		case round > 600:
			step /= 2
		}
		hi = min(maxA, hi+step)
	}
}

// coversSphere reports whether a normalised union holds all six faces.
func coversSphere(cu s2.CellUnion) bool {
	for f := 0; f < 6; f++ {
		if !cu.ContainsCellID(s2.CellIDFromFace(f)) {
			return false
		}
	}
	return true
}

type nearRun struct {
	center     s2.Point
	minA, maxA s1.Angle
	filter     func([]byte) bool
	load       Load
	seen       map[string]struct{}
	h          nearHeap
	err        error
}

func (n *nearRun) visitCell(_ s2.CellID, ksID, val []byte) bool { return n.visit(ksID, val) }

// visit evaluates a candidate the first time its ksID appears.
func (n *nearRun) visit(ksID, val []byte) bool {
	if _, ok := n.seen[string(ksID)]; ok {
		return true
	}
	id := string(ksID)
	n.seen[id] = struct{}{}

	kind, ll, err := DecodeValue(val)
	if err != nil {
		n.err = err
		return false
	}
	var d s1.Angle
	if kind == ValuePoint {
		d = pointDistance(n.center, s2.PointFromLatLng(ll))
	} else {
		g, err := n.load(ksID)
		if err != nil {
			n.err = err
			return false
		}
		if g == nil {
			return true
		}
		d = Distance(n.center, g)
	}
	if d < n.minA || d > n.maxA {
		return true
	}
	if n.filter != nil && !n.filter(ksID) {
		return true
	}
	heap.Push(&n.h, nearItem{d: d, id: []byte(id)})
	return true
}

type nearItem struct {
	d  s1.Angle
	id []byte
}

// nearHeap is a min-heap on (distance, ksID).
type nearHeap []nearItem

func (h nearHeap) Len() int { return len(h) }
func (h nearHeap) Less(i, j int) bool {
	if h[i].d != h[j].d {
		return h[i].d < h[j].d
	}
	return bytes.Compare(h[i].id, h[j].id) < 0
}
func (h nearHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *nearHeap) Push(x any)   { *h = append(*h, x.(nearItem)) }
func (h *nearHeap) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}
