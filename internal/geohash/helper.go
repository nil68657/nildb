package geohash

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

const (
	// EarthRadiusMeters is Earth's quadratic mean radius for WGS-84
	// (EARTH_RADIUS_IN_METERS in geohash_helper.c).
	EarthRadiusMeters = 6372797.560856

	// MercatorMax is half the width of the Mercator projection in metres
	// (MERCATOR_MAX). Doubling a radius until it reaches this value counts
	// the interleaving steps a search cell needs.
	MercatorMax = 20037726.37

	// degToRad is D_R in geohash_helper.c, M_PI / 180.0. Go evaluates the
	// constant exactly and rounds once; that yields the same double as the C
	// double division, 0x3f91df46a2529d39.
	degToRad = math.Pi / 180.0
)

func degRad(a float64) float64 { return float64(a * degToRad) }
func radDeg(a float64) float64 { return a / degToRad }

// EstimateStepsByRadius mirrors geohashEstimateStepsByRadius: the number of
// interleaving steps whose cells are wide enough for a search of
// radiusMeters around latitude lat. The result stays within 1..26, and past
// 66 and 80 degrees of latitude it drops by one and by two steps because
// Mercator cells narrow towards the poles.
//
// The C loop never ends for a negative radius; Redis rejects one while
// parsing. Here a negative radius leaves the loop at once and yields step 1.
func EstimateStepsByRadius(radiusMeters, lat float64) uint8 {
	if radiusMeters == 0 {
		return 26
	}
	step := 1
	for radiusMeters > 0 && radiusMeters < MercatorMax {
		radiusMeters *= 2
		step++
	}
	step -= 2 // Make sure the range is included in most of the base cases.

	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}

	if step < 1 {
		step = 1
	}
	if step > 26 {
		step = 26
	}
	return uint8(step)
}

// Shape is a GEOSEARCH area centred on Lon, Lat: a circle of Radius metres,
// or a box Width metres wide and Height metres high. A non-zero Radius
// selects the circle; a zero Radius with a non-zero Width or Height selects
// the box. All zero is a circle of radius zero, which matches only members at
// the centre point, the set BYRADIUS 0 and BYBOX 0 0 both match in Redis.
//
// Redis keeps the unit's conversion factor in its GeoShape and multiplies as
// it goes. Callers here convert first: Radius = radius*factor, Width =
// width*factor, Height = height*factor. Those are the products Redis forms,
// and halving them afterwards is exact, so Contains and Bounds return the
// same doubles Redis computes.
type Shape struct {
	Lon, Lat      float64
	Radius        float64
	Width, Height float64
}

func (s Shape) isBox() bool { return s.Radius == 0 && (s.Width != 0 || s.Height != 0) }

// Bounds mirrors geohashBoundingBox: the degrees the shape spans. The
// longitude half-width is measured on the edge of the box nearest the pole,
// where meridians are closest, so the box covers the whole shape.
func (s Shape) Bounds() (lon, lat Range64) {
	height, width := s.Radius, s.Radius
	if s.isBox() {
		height, width = s.Height/2, s.Width/2
	}

	latDelta := radDeg(height / EarthRadiusMeters)
	lonDeltaTop := radDeg(width / EarthRadiusMeters / math.Cos(degRad(s.Lat+latDelta)))
	lonDeltaBottom := radDeg(width / EarthRadiusMeters / math.Cos(degRad(s.Lat-latDelta)))

	lonDelta := lonDeltaTop
	if s.Lat < 0 {
		lonDelta = lonDeltaBottom
	}
	return Range64{s.Lon - lonDelta, s.Lon + lonDelta},
		Range64{s.Lat - latDelta, s.Lat + latDelta}
}

// searchCells is the GeoHashRadius of geohash_helper.h: the centre cell of a
// search and its eight neighbours, with the ruled-out neighbours zeroed.
type searchCells struct {
	hash      Bits
	area      Area
	neighbors [8]Bits
}

// cells mirrors geohashCalculateAreasByShapeWGS84.
func (s Shape) cells() searchCells {
	lonB, latB := s.Bounds()
	minLon, maxLon := lonB.Min, lonB.Max
	minLat, maxLat := latB.Min, latB.Max

	// A box is searched at the radius of its circumscribed circle.
	radiusMeters := s.Radius
	if s.isBox() {
		w, h := s.Width/2, s.Height/2
		radiusMeters = math.Sqrt(float64(w*w) + float64(h*h))
	}
	steps := EstimateStepsByRadius(radiusMeters, s.Lat)

	lonRange, latRange := coordRange()
	hash, _ := encode(lonRange, latRange, s.Lon, s.Lat, steps)
	neighbors := Neighbors(hash)
	area, _ := decode(lonRange, latRange, hash)

	// When the centre sits near the edge of its cell, one of the four side
	// neighbours may stop short of the bounding box. One step less makes the
	// nine cells cover it.
	decreaseStep := false
	{
		north, _ := decode(lonRange, latRange, neighbors[North])
		south, _ := decode(lonRange, latRange, neighbors[South])
		east, _ := decode(lonRange, latRange, neighbors[East])
		west, _ := decode(lonRange, latRange, neighbors[West])

		if north.Lat.Max < maxLat {
			decreaseStep = true
		}
		if south.Lat.Min > minLat {
			decreaseStep = true
		}
		if east.Lon.Max < maxLon {
			decreaseStep = true
		}
		if west.Lon.Min > minLon {
			decreaseStep = true
		}
	}

	if steps > 1 && decreaseStep {
		steps--
		hash, _ = encode(lonRange, latRange, s.Lon, s.Lat, steps)
		neighbors = Neighbors(hash)
		area, _ = decode(lonRange, latRange, hash)
	}

	// Drop the neighbours on every side where the centre cell already
	// reaches past the bounding box.
	if steps >= 2 {
		if area.Lat.Min < minLat {
			neighbors[South], neighbors[SouthWest], neighbors[SouthEast] = Bits{}, Bits{}, Bits{}
		}
		if area.Lat.Max > maxLat {
			neighbors[North], neighbors[NorthEast], neighbors[NorthWest] = Bits{}, Bits{}, Bits{}
		}
		if area.Lon.Min < minLon {
			neighbors[West], neighbors[SouthWest], neighbors[NorthWest] = Bits{}, Bits{}, Bits{}
		}
		if area.Lon.Max > maxLon {
			neighbors[East], neighbors[SouthEast], neighbors[NorthEast] = Bits{}, Bits{}, Bits{}
		}
	}
	return searchCells{hash: hash, area: area, neighbors: neighbors}
}

// Range is the score interval of one search cell, Min inclusive and Max
// exclusive, the zrangespec (minex=0, maxex=1) that geoGetPointsInRange
// queries. scoresOfGeoHashBox sets Min to the cell aligned to 52 bits and Max
// to the next cell at the same step aligned the same way, so a scan of
// [Min, Max) returns every member whose score starts with the cell's bits.
type Range struct{ Min, Max Hash }

// Contains reports whether h lies in the cell.
func (r Range) Contains(h Hash) bool { return h >= r.Min && h < r.Max }

// scoresOf mirrors scoresOfGeoHashBox.
func scoresOf(b Bits) Range {
	min := Align52Bits(b)
	b.Bits++
	return Range{Min: min, Max: Align52Bits(b)}
}

// Areas returns the score ranges GEOSEARCH scans for s, in the order
// membersOfAllNeighbors visits them: centre, north, south, east, west,
// north-east, north-west, south-east, south-west. Zeroed neighbours are left
// out, and so is any cell equal to the last one emitted, which is how Redis
// avoids reading a cell twice when a radius of thousands of kilometres makes
// neighbours coincide.
func (s Shape) Areas() []Range {
	c := s.cells()
	n := c.neighbors
	order := [9]Bits{c.hash, n[North], n[South], n[East], n[West],
		n[NorthEast], n[NorthWest], n[SouthEast], n[SouthWest]}

	out := make([]Range, 0, len(order))
	last := 0
	for i, b := range order {
		if b.IsZero() {
			continue
		}
		if last != 0 && b == order[last] {
			continue
		}
		out = append(out, scoresOf(b))
		last = i
	}
	return out
}

// latDistance mirrors geohashGetLatDistance. With no longitude difference the
// haversine reduces to the arc between the two latitudes.
func latDistance(lat1, lat2 float64) float64 {
	return float64(EarthRadiusMeters * math.Abs(degRad(lat2)-degRad(lat1)))
}

// Distance mirrors geohashGetDistance: the haversine great-circle distance in
// metres between two longitude, latitude pairs.
func Distance(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r := degRad(lon1)
	lon2r := degRad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	// Same meridian: skip the trigonometry.
	if v == 0.0 {
		return latDistance(lat1, lat2)
	}
	lat1r := degRad(lat1)
	lat2r := degRad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := float64(u*u) + float64(math.Cos(lat1r)*math.Cos(lat2r)*v*v)
	return float64(2.0 * EarthRadiusMeters * math.Asin(math.Sqrt(a)))
}

// distanceIfInRadius mirrors geohashGetDistanceIfInRadiusWGS84.
func distanceIfInRadius(x1, y1, x2, y2, radius float64) (float64, bool) {
	d := Distance(x1, y1, x2, y2)
	return d, !(d > radius)
}

// distanceIfInRectangle mirrors geohashGetDistanceIfInRectangle: a point is
// inside when its latitude arc from the centre is at most height/2 and its
// longitude arc, measured along the point's own parallel, is at most width/2.
// The latitude test runs first because it needs no trigonometry.
func distanceIfInRectangle(width, height, x1, y1, x2, y2 float64) (float64, bool) {
	if latDistance(y2, y1) > height/2 {
		return 0, false
	}
	if Distance(x2, y2, x1, y2) > width/2 {
		return 0, false
	}
	return Distance(x1, y1, x2, y2), true
}

// Contains mirrors geoWithinShape. lon, lat is a member's decoded position.
// It returns the member's great-circle distance from the centre in metres
// and whether the member lies in the shape; the distance is only meaningful
// when ok is true.
func (s Shape) Contains(lon, lat float64) (dist float64, ok bool) {
	if s.isBox() {
		return distanceIfInRectangle(s.Width, s.Height, s.Lon, s.Lat, lon, lat)
	}
	return distanceIfInRadius(s.Lon, s.Lat, lon, lat, s.Radius)
}

// FormatDistance prints a distance with four decimals the way GEODIST and
// WITHDIST replies do. Redis 7.2 formats through fixedpoint_d2string, which
// rounds m*10000 to the nearest integer under the default rounding mode
// (ties to even) and then inserts the point. That agrees with "%.4f" except
// when the multiplication itself rounds across a tie.
func FormatDistance(m float64) string {
	if m == 0 {
		return "0.0000"
	}
	scaled := math.RoundToEven(m * 10000)
	negative := scaled < 0
	if negative {
		scaled = -scaled
	}

	digits := strconv.FormatUint(uint64(scaled), 10)
	if len(digits) < 5 {
		digits = strings.Repeat("0", 5-len(digits)) + digits
	}
	whole, frac := digits[:len(digits)-4], digits[len(digits)-4:]
	if negative {
		return "-" + whole + "." + frac
	}
	return whole + "." + frac
}

// Unit is a distance unit GEO commands accept. ToMeters is the factor
// extractUnitOrReply returns: multiply a value in the unit by it to get
// metres, divide metres by it to get back.
type Unit struct {
	Name     string
	ToMeters float64
}

// FromMeters converts a distance in metres to the unit, the division Redis
// applies to every reported distance.
func (u Unit) FromMeters(m float64) float64 { return m / u.ToMeters }

// ParseUnit mirrors extractUnitOrReply. The match ignores ASCII case, as
// strcasecmp does, and the error text is the reply Redis sends.
func ParseUnit(s string) (Unit, error) {
	switch asciiLower(s) {
	case "m":
		return Unit{Name: "m", ToMeters: 1}, nil
	case "km":
		return Unit{Name: "km", ToMeters: 1000}, nil
	case "ft":
		return Unit{Name: "ft", ToMeters: 0.3048}, nil
	case "mi":
		return Unit{Name: "mi", ToMeters: 1609.34}, nil
	}
	return Unit{}, errors.New("unsupported unit provided. please use M, KM, FT, MI")
}

func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}
