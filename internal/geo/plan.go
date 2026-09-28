package geo

import (
	"slices"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// Range is an inclusive range of cell ids: every key whose cell lies in
// [Lo, Hi]. The descendants of cell c, c included, are
// Range{c.RangeMin(), c.RangeMax()}. Hi is never above the last leaf cell,
// so Hi+1 is a safe exclusive upper bound for an iterator.
type Range struct{ Lo, Hi s2.CellID }

// Plan is the set of index reads that finds every document whose indexed
// cells can intersect a query region.
//
// Ranges hold documents indexed at a query cell or below it; read each with
// one bounded iterator over [idx|Lo, idx|Hi+1) with TotalOrderSeek, because
// the range crosses 12-byte prefixes. Probes hold documents indexed at a
// cell coarser than a query cell; read each as the 12-byte prefix idx|cell
// with PrefixSameAsStart. A document can appear under several keys, so
// callers deduplicate _ids.
type Plan struct {
	Ranges []Range     // sorted by Lo, overlapping ranges merged
	Probes []s2.CellID // sorted, deduplicated, none inside a range
}

// PlanRegion plans the index reads for r. The query covering uses at most
// queryMaxCells cells (DefaultQueryMaxCells when 0) with levels clamped to
// [Coarsest, Finest], so each cell needs at most Finest-Coarsest probes.
func (p Params) PlanRegion(r s2.Region, queryMaxCells int) Plan {
	return p.planCells(p.queryCoverer(queryMaxCells).Covering(r))
}

// annulusPad widens the cap PlanAnnulus covers so that a document whose
// computed distance is exactly outer still falls inside the covering
// despite rounding; 1e-9 rad is about 6 mm.
const annulusPad = s1.Angle(1e-9)

// PlanAnnulus plans one round of the kNN search: the covering of the cap
// of radius outer around center, minus the cells already in scanned. It
// returns the plan for the remaining cells and the new scanned union
// (scanned plus the cap's covering, normalised), which the caller passes to
// the next round. The plan's probes are every ancestor of every remaining
// cell down to Coarsest; Near skips probes it issued in earlier rounds.
func (p Params) PlanAnnulus(center s2.Point, outer s1.Angle, scanned s2.CellUnion, queryMaxCells int) (Plan, s2.CellUnion) {
	cov := p.queryCoverer(queryMaxCells).Covering(s2.CapFromCenterAngle(center, outer+annulusPad))
	todo := cov
	if len(scanned) > 0 {
		todo = s2.CellUnionFromDifference(cov, scanned)
	}
	return p.planCells(todo), s2.CellUnionFromUnion(scanned, cov)
}

// planCells turns covering cells into merged descendant ranges and
// deduplicated ancestor probes.
func (p Params) planCells(cells []s2.CellID) Plan {
	var plan Plan
	if len(cells) == 0 {
		return plan
	}
	ranges := make([]Range, 0, len(cells))
	var probes []s2.CellID
	for _, c := range cells {
		ranges = append(ranges, Range{Lo: c.RangeMin(), Hi: c.RangeMax()})
		for l := c.Level() - 1; l >= p.Coarsest; l-- {
			probes = append(probes, c.Parent(l))
		}
	}
	plan.Ranges = mergeRanges(ranges)

	slices.Sort(probes)
	probes = slices.Compact(probes)
	kept := probes[:0]
	for _, c := range probes {
		if !inRanges(plan.Ranges, c) {
			kept = append(kept, c)
		}
	}
	if len(kept) > 0 {
		plan.Probes = kept
	}
	return plan
}

// mergeRanges sorts ranges and merges those that overlap or touch.
func mergeRanges(rs []Range) []Range {
	slices.SortFunc(rs, func(a, b Range) int {
		switch {
		case a.Lo < b.Lo:
			return -1
		case a.Lo > b.Lo:
			return 1
		}
		return 0
	})
	out := rs[:0]
	for _, r := range rs {
		if n := len(out); n > 0 && r.Lo <= out[n-1].Hi+1 {
			if r.Hi > out[n-1].Hi {
				out[n-1].Hi = r.Hi
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// inRanges reports whether c lies inside one of the sorted, disjoint ranges.
func inRanges(rs []Range, c s2.CellID) bool {
	i, _ := slices.BinarySearchFunc(rs, c, func(r Range, c s2.CellID) int {
		switch {
		case r.Hi < c:
			return -1
		case r.Lo > c:
			return 1
		}
		return 0
	})
	return i < len(rs) && rs[i].Lo <= c && c <= rs[i].Hi
}
