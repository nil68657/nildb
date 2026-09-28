package geo

import (
	"reflect"
	"testing"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// rangeCell recovers the cell whose descendant range r is, or reports false
// when r is a merge of several cells.
func rangeCell(r Range) (s2.CellID, bool) {
	c := s2.CellID(uint64(r.Lo)/2 + uint64(r.Hi)/2 + 1)
	return c, c.IsValid() && c.RangeMin() == r.Lo && c.RangeMax() == r.Hi
}

func checkPlan(t *testing.T, p Params, plan Plan) (cells s2.CellUnion) {
	t.Helper()
	for i, r := range plan.Ranges {
		if r.Lo > r.Hi {
			t.Fatalf("range %d inverted", i)
		}
		if i > 0 && plan.Ranges[i-1].Hi >= r.Lo {
			t.Fatalf("ranges %d and %d overlap or are unsorted", i-1, i)
		}
		c, ok := rangeCell(r)
		if !ok {
			t.Fatalf("range %d is not one cell", i)
		}
		if l := c.Level(); l < p.Coarsest || l > p.Finest {
			t.Fatalf("query cell level %d outside [%d, %d]", l, p.Coarsest, p.Finest)
		}
		cells = append(cells, c)
	}
	for i, c := range plan.Probes {
		if i > 0 && plan.Probes[i-1] >= c {
			t.Fatalf("probes unsorted or duplicated at %d", i)
		}
		if l := c.Level(); l < p.Coarsest || l >= p.Finest {
			t.Fatalf("probe level %d outside [%d, %d)", l, p.Coarsest, p.Finest)
		}
		if inRanges(plan.Ranges, c) {
			t.Fatalf("probe %v inside a range", c)
		}
	}
	// Every ancestor down to Coarsest of every query cell is probed.
	probes := make(map[s2.CellID]bool)
	for _, c := range plan.Probes {
		probes[c] = true
	}
	for _, c := range cells {
		for l := c.Level() - 1; l >= p.Coarsest; l-- {
			if !probes[c.Parent(l)] {
				t.Fatalf("ancestor %v of %v not probed", c.Parent(l), c)
			}
		}
	}
	return cells
}

func TestPlanRegionClamps(t *testing.T) {
	p := DefaultParams()
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(48.85, 2.35))

	// A 1 m cap would be covered by level-22 cells; clamping stops at 16.
	tiny := p.PlanRegion(s2.CapFromCenterAngle(center, Angle(1)), 0)
	cells := checkPlan(t, p, tiny)
	if len(cells) == 0 || len(cells) > 4 {
		t.Fatalf("tiny cap: %d cells", len(cells))
	}
	for _, c := range cells {
		if c.Level() != p.Finest {
			t.Fatalf("tiny cap cell at level %d, want %d", c.Level(), p.Finest)
		}
	}
	if max := len(cells) * (p.Finest - p.Coarsest); len(tiny.Probes) > max || len(tiny.Probes) < p.Finest-p.Coarsest {
		t.Fatalf("tiny cap: %d probes for %d cells", len(tiny.Probes), len(cells))
	}

	// A hemisphere would fit in face cells; clamping keeps level 2.
	huge := p.PlanRegion(s2.CapFromCenterAngle(center, s1.Angle(1.5)), 0)
	for _, c := range checkPlan(t, p, huge) {
		if c.Level() < p.Coarsest {
			t.Fatalf("huge cap cell at level %d", c.Level())
		}
	}

	poly := mustParse(t, `{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]]]}`, ParseOpts{})
	mid := p.PlanRegion(poly.Region(), 8)
	if n := len(checkPlan(t, p, mid)); n > 8 {
		t.Fatalf("maxCells 8 gave %d cells", n)
	}
}

func TestMergeRanges(t *testing.T) {
	got := mergeRanges([]Range{{10, 20}, {1, 3}, {15, 30}, {4, 5}, {40, 50}, {31, 31}})
	want := []Range{{1, 5}, {10, 31}, {40, 50}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeRanges = %v, want %v", got, want)
	}
	if inRanges(want, 6) || !inRanges(want, 31) || !inRanges(want, 1) || inRanges(want, 51) {
		t.Fatal("inRanges wrong")
	}
}

func TestPlanAnnulus(t *testing.T) {
	p := DefaultParams()
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(-33.9, 151.2))
	plan1, scanned1 := p.PlanAnnulus(center, Angle(5e3), nil, 0)
	cells1 := checkPlan(t, p, plan1)
	plan2, scanned2 := p.PlanAnnulus(center, Angle(200e3), scanned1, 0)
	cells2 := checkPlan(t, p, plan2)

	// Round two never rescans round one.
	u1 := s2.CellUnionFromUnion(cells1)
	for _, c := range cells2 {
		if u1.IntersectsCellID(c) {
			t.Fatalf("round two rescans %v", c)
		}
	}
	// Together they cover the second cap's covering, which is scanned2.
	both := s2.CellUnionFromUnion(cells1, cells2)
	cov := p.queryCoverer(0).Covering(s2.CapFromCenterAngle(center, Angle(200e3)+annulusPad))
	for _, c := range cov {
		if !both.ContainsCellID(c) {
			t.Fatalf("cell %v of the second cap not scanned", c)
		}
	}
	if !scanned2.Contains(both) || !both.Contains(scanned2) {
		t.Fatal("scanned union differs from the cells planned")
	}
	// A third call with the same radius has nothing left to do.
	plan3, _ := p.PlanAnnulus(center, Angle(200e3), scanned2, 0)
	if len(plan3.Ranges) != 0 || len(plan3.Probes) != 0 {
		t.Fatalf("repeat round planned %d ranges %d probes", len(plan3.Ranges), len(plan3.Probes))
	}
}
