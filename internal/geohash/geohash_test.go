package geohash

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

type place struct {
	name     string
	lon, lat float64
}

// sicily is the data set of the GEOADD, GEORADIUS and GEOSEARCH
// documentation examples.
var sicily = []place{
	{"Palermo", 13.361389, 38.115556},
	{"Catania", 15.087269, 37.502669},
	{"edge1", 12.758489, 38.788135},
	{"edge2", 17.241510, 38.788135},
}

// nyc is the data set of tests/unit/geo.tcl with the scores that its
// "Check geoset values" test expects from ZRANGE WITHSCORES.
var nyc = []struct {
	place
	score Hash
}{
	{place{"wtc one", -74.0131604, 40.7126674}, 1791873972053020},
	{place{"union square", -73.9903085, 40.7362513}, 1791875485187452},
	{place{"central park n/q/r", -73.9733487, 40.7648057}, 1791875761332224},
	{place{"4545", -73.9564142, 40.7480973}, 1791875796750882},
	{place{"lic market", -73.9454966, 40.747533}, 1791875804419201},
	{place{"q4", -73.9375699, 40.7498929}, 1791875830079666},
	{place{"jfk", -73.7858139, 40.6428986}, 1791895905559723},
}

func nycPlaces() []place {
	out := make([]place, len(nyc))
	for i, m := range nyc {
		out[i] = m.place
	}
	return out
}

func mustEncode(t *testing.T, lon, lat float64) Hash {
	t.Helper()
	h, err := Encode(lon, lat)
	if err != nil {
		t.Fatalf("Encode(%v, %v): %v", lon, lat, err)
	}
	return h
}

// stored returns the position GEOPOS reports for a coordinate after GEOADD,
// which is what a FROMMEMBER search uses as its centre.
func stored(t *testing.T, lon, lat float64) (float64, float64) {
	t.Helper()
	return mustEncode(t, lon, lat).Decode()
}

func rangesString(ranges []Range) string {
	parts := make([]string, len(ranges))
	for i, r := range ranges {
		parts[i] = fmt.Sprintf("[%d,%d)", r.Min, r.Max)
	}
	return strings.Join(parts, " ")
}

func inRanges(ranges []Range, h Hash) bool {
	for _, r := range ranges {
		if r.Contains(h) {
			return true
		}
	}
	return false
}

// search runs the GEOSEARCH pipeline over an in-memory member list: encode
// each member as GEOADD would, keep the ones whose score falls in an Areas
// range, decode those and apply Contains. It returns the matching names
// sorted, with their distances in metres.
func search(t *testing.T, s Shape, members []place) ([]string, map[string]float64) {
	t.Helper()
	ranges := s.Areas()
	var found []string
	dists := map[string]float64{}
	for _, m := range members {
		h := mustEncode(t, m.lon, m.lat)
		if !inRanges(ranges, h) {
			continue
		}
		lon, lat := h.Decode()
		if d, ok := s.Contains(lon, lat); ok {
			found = append(found, m.name)
			dists[m.name] = d
		}
	}
	sort.Strings(found)
	return found, dists
}

func TestEncodeVectors(t *testing.T) {
	tests := []struct {
		name     string
		lon, lat float64
		want     Hash
	}{
		{"Palermo", 13.361389, 38.115556, 3479099956230698},
		{"Catania", 15.087269, 37.502669, 3479447370796909},
		{"south-west corner", LonMin, LatMin, 0},
	}
	for _, m := range nyc {
		tests = append(tests, struct {
			name     string
			lon, lat float64
			want     Hash
		}{m.name, m.lon, m.lat, m.score})
	}
	for _, tc := range tests {
		got := mustEncode(t, tc.lon, tc.lat)
		if got != tc.want {
			t.Errorf("Encode(%s %v,%v) = %d, want %d", tc.name, tc.lon, tc.lat, got, tc.want)
		}
	}
}

func TestEncodeInvalid(t *testing.T) {
	tests := []struct {
		lon, lat float64
		want     string
	}{
		{181, 0, "invalid longitude,latitude pair 181.000000,0.000000"},
		{-180.5, 10, "invalid longitude,latitude pair -180.500000,10.000000"},
		{0, 85.06, "invalid longitude,latitude pair 0.000000,85.060000"},
		{0, -90, "invalid longitude,latitude pair 0.000000,-90.000000"},
		{math.Inf(1), 0, "invalid longitude,latitude pair inf,0.000000"},
		{0, math.Inf(-1), "invalid longitude,latitude pair 0.000000,-inf"},
		{math.NaN(), 0, "invalid longitude,latitude pair nan,0.000000"},
	}
	for _, tc := range tests {
		_, err := Encode(tc.lon, tc.lat)
		if err == nil {
			t.Errorf("Encode(%v, %v): no error, want %q", tc.lon, tc.lat, tc.want)
			continue
		}
		if err.Error() != tc.want {
			t.Errorf("Encode(%v, %v) error = %q, want %q", tc.lon, tc.lat, err.Error(), tc.want)
		}
	}

	// The limits themselves are valid input.
	for _, p := range [][2]float64{{LonMax, LatMax}, {LonMin, LatMin}, {LonMax, LatMin}, {LonMin, LatMax}} {
		if _, err := Encode(p[0], p[1]); err != nil {
			t.Errorf("Encode(%v, %v): %v", p[0], p[1], err)
		}
	}
}

func TestDecodeVectors(t *testing.T) {
	// GEOPOS replies from the GEOADD documentation. Redis prints them through
	// %.17Lf, so the literals carry more digits than a double holds.
	tests := []struct {
		hash     Hash
		lon, lat float64
	}{
		{3479099956230698, 13.36138933897018433, 38.11555639549629859},
		{3479447370796909, 15.08726745843887329, 37.50266842333162032},
	}
	for _, tc := range tests {
		lon, lat := tc.hash.Decode()
		if math.Abs(lon-tc.lon) > 1e-13 || math.Abs(lat-tc.lat) > 1e-13 {
			t.Errorf("Hash(%d).Decode() = %.17g,%.17g, want %.17g,%.17g", tc.hash, lon, lat, tc.lon, tc.lat)
		}
	}

	// GEOPOS on the corner hash returns the centre of the first cell.
	lon, lat := Hash(0).Decode()
	if lon <= LonMin || lat <= LatMin || lon > LonMin+1e-5 || lat > LatMin+1e-5 {
		t.Errorf("Hash(0).Decode() = %v,%v, want the centre of the south-west cell", lon, lat)
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		lon, lat float64
		want     string
	}{
		{13.361389, 38.115556, "sqc8b49rny0"}, // GEOHASH documentation
		{15.087269, 37.502669, "sqdtr74hyu0"},
		{-5.6, 42.6, "ezs42e44yx0"}, // tests/unit/geo.tcl, from Wikipedia
	}
	for _, tc := range tests {
		got := mustEncode(t, tc.lon, tc.lat).String()
		if got != tc.want {
			t.Errorf("Encode(%v,%v).String() = %q, want %q", tc.lon, tc.lat, got, tc.want)
		}
	}

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1000; i++ {
		lon := LonMin + rng.Float64()*(LonMax-LonMin)
		lat := LatMin + rng.Float64()*(LatMax-LatMin)
		s := mustEncode(t, lon, lat).String()
		if len(s) != 11 {
			t.Fatalf("String() = %q, want 11 characters", s)
		}
		if strings.Trim(s, geoAlphabet) != "" {
			t.Fatalf("String() = %q, contains a character outside %q", s, geoAlphabet)
		}
	}
	for _, p := range [][2]float64{{LonMax, LatMax}, {LonMin, LatMin}} {
		if s := mustEncode(t, p[0], p[1]).String(); len(s) != 11 || strings.Trim(s, geoAlphabet) != "" {
			t.Errorf("Encode(%v,%v).String() = %q", p[0], p[1], s)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	worst := 0.0
	for i := 0; i < 10000; i++ {
		lon := LonMin + rng.Float64()*(LonMax-LonMin)
		lat := LatMin + rng.Float64()*(LatMax-LatMin)
		glon, glat := mustEncode(t, lon, lat).Decode()
		d := Distance(lon, lat, glon, glat)
		if d > worst {
			worst = d
		}
		if d > 0.6 {
			t.Fatalf("(%v,%v) decoded to (%v,%v), %v m away", lon, lat, glon, glat, d)
		}
	}
	t.Logf("worst round-trip error over 10000 points: %.4f m", worst)
}

func TestDistance(t *testing.T) {
	palermo := Hash(3479099956230698)
	catania := Hash(3479447370796909)
	plon, plat := palermo.Decode()
	clon, clat := catania.Decode()
	m := Distance(plon, plat, clon, clat)

	km, err := ParseUnit("km")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		dist float64
		want string
	}{
		{"GEODIST Sicily Palermo Catania", m, "166274.1516"},
		{"GEODIST Sicily Palermo Catania km", km.FromMeters(m), "166.2742"},
		{"GEODIST Sicily Palermo Palermo", Distance(plon, plat, plon, plat), "0.0000"},
		{"reversed arguments", Distance(clon, clat, plon, plat), "166274.1516"},
	}
	for _, tc := range tests {
		if got := FormatDistance(tc.dist); got != tc.want {
			t.Errorf("%s: FormatDistance(%v) = %q, want %q", tc.name, tc.dist, got, tc.want)
		}
	}

	// Same meridian takes the latitude-only path.
	if got := Distance(10, 20, 10, 21); got != latDistance(20, 21) {
		t.Errorf("Distance on one meridian = %v, want %v", got, latDistance(20, 21))
	}
}

func TestWithDistVectors(t *testing.T) {
	mi, err := ParseUnit("mi")
	if err != nil {
		t.Fatal(err)
	}
	km, _ := ParseUnit("KM")

	// "GEOSEARCH with small distance": GEORADIUS points -122.407107 37.794300
	// 30 mi ASC WITHDIST returns {1 0.0001} {2 9.8182}.
	small := []place{{"1", -122.407107, 37.794300}, {"2", -122.227336, 37.794300}}
	found, dists := search(t, Shape{Lon: -122.407107, Lat: 37.794300, Radius: 30 * mi.ToMeters}, small)
	if got := strings.Join(found, " "); got != "1 2" {
		t.Fatalf("30 mi search found %q, want \"1 2\"", got)
	}
	for name, want := range map[string]string{"1": "0.0001", "2": "9.8182"} {
		if got := FormatDistance(mi.FromMeters(dists[name])); got != want {
			t.Errorf("member %s: %s mi, want %s", name, got, want)
		}
	}

	// "GEORADIUSBYMEMBER withdist (sorted)": 7 km around "wtc one".
	clon, clat := stored(t, nyc[0].lon, nyc[0].lat)
	found, dists = search(t, Shape{Lon: clon, Lat: clat, Radius: 7 * km.ToMeters}, nycPlaces())
	want := map[string]string{
		"wtc one":            "0.0000",
		"union square":       "3.2544",
		"central park n/q/r": "6.7000",
		"4545":               "6.1975",
		"lic market":         "6.8969",
	}
	if len(found) != len(want) {
		t.Fatalf("7 km around wtc one found %v, want %d members", found, len(want))
	}
	for name, w := range want {
		d, ok := dists[name]
		if !ok {
			t.Errorf("member %q missing", name)
			continue
		}
		if got := FormatDistance(km.FromMeters(d)); got != w {
			t.Errorf("member %q: %s km, want %s", name, got, w)
		}
	}
}

func TestAreasDocExample(t *testing.T) {
	// GEORADIUS Sicily 15 37 200 km WITHDIST.
	s := Shape{Lon: 15, Lat: 37, Radius: 200000}
	ranges := s.Areas()
	if len(ranges) == 0 {
		t.Fatal("Areas() returned no ranges")
	}
	for _, h := range []Hash{3479099956230698, 3479447370796909} {
		if !inRanges(ranges, h) {
			t.Errorf("hash %d is not in any of %s", h, rangesString(ranges))
		}
	}
	for _, r := range ranges {
		if r.Min >= r.Max {
			t.Errorf("range %s is empty", rangesString([]Range{r}))
		}
	}

	km, _ := ParseUnit("km")
	tests := []struct {
		name string
		hash Hash
		want string
	}{
		{"Palermo", 3479099956230698, "190.4424"},
		{"Catania", 3479447370796909, "56.4413"},
	}
	for _, tc := range tests {
		lon, lat := tc.hash.Decode()
		d, ok := s.Contains(lon, lat)
		if !ok {
			t.Errorf("%s: Contains = false", tc.name)
			continue
		}
		if got := FormatDistance(km.FromMeters(d)); got != tc.want {
			t.Errorf("%s: %s km, want %s", tc.name, got, tc.want)
		}
	}
}

func TestSearchVectors(t *testing.T) {
	km := 1000.0
	corner := []place{
		{"edge1", 12.758489, 38.788135}, {"edge2", 17.241510, 38.788135},
		{"edge3", 17.250000, 35.202000}, {"edge4", 12.750000, 35.202000},
		{"edge5", 12.748489955781654, 37}, {"edge6", 15, 38.798135872540925},
		{"edge7", 17.251510044218346, 37}, {"edge8", 15, 35.201864127459075},
		{"corner1", 12.692799634687903, 38.798135872540925},
		{"corner2", 17.200560937451133, 35.201864127459075},
		{"corner3", 12.799439062548865, 35.201864127459075},
		{"corner4", 17.200560937451133, 35.201864127459075},
	}
	n1lon, n1lat := stored(t, -0.15307903289794921875, 85)
	n3lon, n3lat := stored(t, -4.95211958885192871094, 85)
	n5lon, n5lat := stored(t, -45, 65.50900022111811438208)
	polelon, polelat := stored(t, 45, 65)

	tests := []struct {
		name    string
		shape   Shape
		members []place
		want    string
	}{
		{"GEOSEARCH vs GEORADIUS: byradius 200 km",
			Shape{Lon: 15, Lat: 37, Radius: 200 * km}, sicily, "Catania Palermo"},
		{"GEOSEARCH vs GEORADIUS: bybox 400 400 km",
			Shape{Lon: 15, Lat: 37, Width: 400 * km, Height: 400 * km}, sicily, "Catania Palermo edge1 edge2"},
		{"GEOSEARCH corner point test",
			Shape{Lon: 15, Lat: 37, Width: 400 * km, Height: 400 * km}, corner, "edge1 edge2 edge5 edge7"},
		{"GEOSEARCH non square, long and narrow",
			Shape{Lon: 15, Lat: 37, Width: 400 * km, Height: 2 * km},
			[]place{{"test1", 12.75, 36.995}, {"test2", 12.75, 36.50}, {"test3", 13.00, 36.50}}, "test1"},
		{"GEOSEARCH non square, western hemisphere point",
			Shape{Lon: 15, Lat: 37, Width: 3000 * km, Height: 2 * km},
			[]place{{"test1", 12.75, 36.995}, {"test2", 12.75, 36.50}, {"test3", -1, 37.00}}, "test1 test3"},
		{"GEOSEARCH the box spans 180",
			Shape{Lon: 179, Lat: 37, Width: 400 * km, Height: 400 * km},
			[]place{{"point1", 179.5, 36}, {"point2", -179.5, 36}}, "point1 point2"},
		{"GEOSEARCH the box spans -180",
			Shape{Lon: -179, Lat: 37, Width: 400 * km, Height: 400 * km},
			[]place{{"point1", 179.5, 36}, {"point2", -179.5, 36}}, "point1 point2"},
		{"GEORADIUS simple (sorted)",
			Shape{Lon: -73.9798091, Lat: 40.7598464, Radius: 3 * km}, nycPlaces(), "4545 central park n/q/r union square"},
		{"GEOSEARCH simple (sorted)",
			Shape{Lon: -73.9798091, Lat: 40.7598464, Width: 6 * km, Height: 6 * km}, nycPlaces(),
			"4545 central park n/q/r lic market union square"},
		{"GEOSEARCH FROMMEMBER simple (sorted)",
			func() Shape {
				lon, lat := stored(t, nyc[0].lon, nyc[0].lat)
				return Shape{Lon: lon, Lat: lat, Width: 14 * km, Height: 14 * km}
			}(), nycPlaces(), "4545 central park n/q/r lic market q4 union square wtc one"},
		{"oblique direction 1",
			Shape{Lon: n1lon, Lat: n1lat, Radius: 4891.94},
			[]place{{"n1", -0.15307903289794921875, 85}, {"n2", 0.3515625, 85.00019260486917005437}}, "n1 n2"},
		{"oblique direction 2",
			Shape{Lon: n3lon, Lat: n3lat, Radius: 156544},
			[]place{{"n3", -4.95211958885192871094, 85}, {"n4", 11.25, 85.0511}}, "n3 n4"},
		{"oblique direction 3",
			Shape{Lon: n5lon, Lat: n5lat, Radius: 5009431},
			[]place{{"n5", -45, 65.50900022111811438208}, {"n6", 90, 85.0511}}, "n5 n6"},
		{"crossing pole search",
			Shape{Lon: polelon, Lat: polelat, Radius: 5009431},
			[]place{{"n1", 45, 65}, {"n2", -135, 85.05}}, "n1 n2"},
		{"GEORADIUS HUGE, issue #2767",
			Shape{Lon: 0, Lat: 0, Radius: 50000 * km},
			[]place{{"user_000000", -47.271613776683807, -54.534504198047678}}, "user_000000"},
		{"radius zero matches the centre only",
			func() Shape {
				lon, lat := stored(t, 13.361389, 38.115556)
				return Shape{Lon: lon, Lat: lat}
			}(), sicily, "Palermo"},
	}
	for _, tc := range tests {
		found, _ := search(t, tc.shape, tc.members)
		if got := strings.Join(found, " "); got != tc.want {
			t.Errorf("%s: found %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestBoxDistances(t *testing.T) {
	// GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 400 400 km ASC WITHDIST from
	// the GEOSEARCH documentation.
	km, _ := ParseUnit("km")
	s := Shape{Lon: 15, Lat: 37, Width: 400 * km.ToMeters, Height: 400 * km.ToMeters}
	_, dists := search(t, s, sicily)
	want := map[string]string{
		"Catania": "56.4413",
		"Palermo": "190.4424",
		"edge2":   "279.7403",
		"edge1":   "279.7405",
	}
	for name, w := range want {
		d, ok := dists[name]
		if !ok {
			t.Errorf("%s not found", name)
			continue
		}
		if got := FormatDistance(km.FromMeters(d)); got != w {
			t.Errorf("%s: %s km, want %s", name, got, w)
		}
	}
}

func TestEstimateStepsByRadius(t *testing.T) {
	tests := []struct {
		radius, lat float64
		want        uint8
	}{
		{0, 0, 26},
		{0, 85, 26},
		{1e-9, 0, 26}, // 55 doublings, clamped
		{1000, 0, 14},
		{1000, 66, 14},
		{1000, 66.5, 13},
		{1000, -66.5, 13},
		{1000, 80, 13},
		{1000, 80.5, 12},
		{1000, -85, 12},
		{200000, 37, 6},
		{MercatorMax, 0, 1},
		{5e7, 0, 1},
		{-1, 0, 1},
	}
	for _, tc := range tests {
		if got := EstimateStepsByRadius(tc.radius, tc.lat); got != tc.want {
			t.Errorf("EstimateStepsByRadius(%v, %v) = %d, want %d", tc.radius, tc.lat, got, tc.want)
		}
	}

	prev := uint8(26)
	for r := 1.0; r < 1e8; r *= 1.5 {
		got := EstimateStepsByRadius(r, 0)
		if got > prev {
			t.Fatalf("EstimateStepsByRadius(%v, 0) = %d rose above %d", r, got, prev)
		}
		prev = got
	}
}

func TestNeighbors(t *testing.T) {
	b, ok := EncodeWGS84(13.361389, 38.115556, StepMax)
	if !ok {
		t.Fatal("EncodeWGS84 failed")
	}
	area, ok := DecodeWGS84(b)
	if !ok {
		t.Fatal("DecodeWGS84 failed")
	}
	n := Neighbors(b)
	dec := func(d Direction) Area {
		a, ok := DecodeWGS84(n[d])
		if !ok {
			t.Fatalf("DecodeWGS84(%v) failed", n[d])
		}
		return a
	}

	// Adjacent cells share an edge; both sides compute it from the same
	// integer, so the doubles are identical.
	if dec(East).Lon.Min != area.Lon.Max || dec(East).Lat != area.Lat {
		t.Errorf("east %+v does not abut %+v", dec(East), area)
	}
	if dec(West).Lon.Max != area.Lon.Min || dec(West).Lat != area.Lat {
		t.Errorf("west %+v does not abut %+v", dec(West), area)
	}
	if dec(North).Lat.Min != area.Lat.Max || dec(North).Lon != area.Lon {
		t.Errorf("north %+v does not abut %+v", dec(North), area)
	}
	if dec(South).Lat.Max != area.Lat.Min || dec(South).Lon != area.Lon {
		t.Errorf("south %+v does not abut %+v", dec(South), area)
	}
	if dec(NorthEast).Lon != dec(East).Lon || dec(NorthEast).Lat != dec(North).Lat {
		t.Errorf("north-east %+v is not east of north", dec(NorthEast))
	}
	if dec(SouthWest).Lon != dec(West).Lon || dec(SouthWest).Lat != dec(South).Lat {
		t.Errorf("south-west %+v is not west of south", dec(SouthWest))
	}
	if dec(NorthWest).Lon != dec(West).Lon || dec(NorthWest).Lat != dec(North).Lat {
		t.Errorf("north-west %+v is not west of north", dec(NorthWest))
	}
	if dec(SouthEast).Lon != dec(East).Lon || dec(SouthEast).Lat != dec(South).Lat {
		t.Errorf("south-east %+v is not east of south", dec(SouthEast))
	}

	// The easternmost cell at step 2 wraps to the westernmost one.
	edge, _ := EncodeWGS84(179, 0, 2)
	east, _ := DecodeWGS84(Neighbors(edge)[East])
	if east.Lon.Min != LonMin {
		t.Errorf("east of the easternmost cell starts at %v, want %v", east.Lon.Min, LonMin)
	}
	top, _ := EncodeWGS84(0, 85, 2)
	north, _ := DecodeWGS84(Neighbors(top)[North])
	if north.Lat.Min != LatMin {
		t.Errorf("north of the northernmost cell starts at %v, want %v", north.Lat.Min, LatMin)
	}
}

func TestAlign52Bits(t *testing.T) {
	b, _ := EncodeWGS84(13.361389, 38.115556, StepMax)
	if got := Align52Bits(b); got != 3479099956230698 {
		t.Errorf("Align52Bits at step 26 = %d, want 3479099956230698", got)
	}
	b6, _ := EncodeWGS84(13.361389, 38.115556, 6)
	if got := Align52Bits(b6); got != Hash(b6.Bits<<40) {
		t.Errorf("Align52Bits at step 6 = %d, want %d", got, b6.Bits<<40)
	}
	r := scoresOf(b6)
	if r.Min != Hash(b6.Bits<<40) || r.Max != Hash((b6.Bits+1)<<40) {
		t.Errorf("scoresOf = %s, want [%d,%d)", rangesString([]Range{r}), b6.Bits<<40, (b6.Bits+1)<<40)
	}
	if !r.Contains(3479099956230698) {
		t.Errorf("the step-6 cell of Palermo does not contain its own hash")
	}
}

func TestAreasHugeRadius(t *testing.T) {
	// A 16751 km radius (a geo.tcl regression vector) searches at step 1,
	// where the world is four cells and the neighbours coincide.
	s := Shape{Lon: -1.8175081637769495, Lat: 20.665668878082954, Radius: 16751000}
	if got := EstimateStepsByRadius(s.Radius, s.Lat); got != 1 {
		t.Fatalf("step = %d, want 1", got)
	}
	ranges := s.Areas()
	if len(ranges) != 4 {
		t.Fatalf("Areas() = %v, want the 4 distinct cells of step 1", ranges)
	}
	for i := range ranges {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[i] == ranges[j] {
				t.Errorf("Areas() repeats %s", rangesString(ranges[i:i+1]))
			}
		}
	}
}

// TestAreasCoverShapes checks the property the nine-cell search relies on:
// every member Contains accepts has its score in one of the Areas ranges.
// Shapes and points follow the constraints of the geo.tcl fuzz tests, and
// the same 0.1% band along the edge is forgiven, since Redis itself misses
// points there.
func TestAreasCoverShapes(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	kmRadius := func() float64 {
		if rng.Intn(10) == 0 {
			return float64(rng.Intn(5000)+10) * 1000
		}
		return float64(rng.Intn(200)+10) * 1000
	}

	// The regression vectors of geo.tcl, as km lon lat.
	vectors := [][3]float64{
		{7083, 81.634948934258375, 30.561509253718668},
		{5416, -70.863281847379767, -46.347003465679947},
		{6064, -89.818768962202014, -40.463868561416803},
		{156, 149.29737817929004, 15.95807862745508},
		{143, 59.235461856813856, 66.269555127373678},
		{187, -101.88575239939883, 49.061997951502917},
		{154, -90.187939661642517, 66.615930412251487},
		{145, 163.03472387745728, 64.012747720821181},
		{143, 137.86663517256579, 63.986745399416776},
		{151, 59.149620271823181, 65.204186651485145},
		{149, 84.062063109158544, -65.685403922426232},
		{16751, -1.8175081637769495, 20.665668878082954},
	}
	var shapes []Shape
	for _, v := range vectors {
		shapes = append(shapes,
			Shape{Lon: v[1], Lat: v[2], Radius: v[0] * 1000},
			Shape{Lon: v[1], Lat: v[2], Width: v[0] * 1000, Height: v[0] * 1000})
	}
	for i := 0; i < 400; i++ {
		lon := -180 + rng.Float64()*360
		lat := -70 + rng.Float64()*140
		if i%2 == 0 {
			shapes = append(shapes, Shape{Lon: lon, Lat: lat, Radius: kmRadius()})
		} else {
			shapes = append(shapes, Shape{Lon: lon, Lat: lat, Width: kmRadius(), Height: kmRadius()})
		}
	}

	for _, s := range shapes {
		ranges := s.Areas()
		for i := range ranges {
			for j := i + 1; j < len(ranges); j++ {
				if ranges[i] == ranges[j] {
					t.Errorf("%+v: Areas() repeats %s", s, rangesString(ranges[i:i+1]))
				}
			}
		}

		// Sample points in and around the bounding box.
		lonB, latB := s.Bounds()
		lonSpan := lonB.Max - lonB.Min
		latSpan := latB.Max - latB.Min
		inner := Shape{Lon: s.Lon, Lat: s.Lat, Radius: s.Radius * 0.999, Width: s.Width * 0.999, Height: s.Height * 0.999}
		for j := 0; j < 1500; j++ {
			plon := lonB.Min - 0.1*lonSpan + rng.Float64()*1.2*lonSpan
			plat := latB.Min - 0.1*latSpan + rng.Float64()*1.2*latSpan
			for plon > LonMax {
				plon -= 360
			}
			for plon < LonMin {
				plon += 360
			}
			// A member on the limit itself gets a hash with bit 52 set, which
			// no search range reaches; Redis has the same blind spot.
			if plon >= LonMax || plat >= LatMax || plat <= LatMin {
				continue
			}

			h := mustEncode(t, plon, plat)
			dlon, dlat := h.Decode()
			if _, ok := s.Contains(dlon, dlat); !ok {
				continue
			}
			if inRanges(ranges, h) {
				continue
			}
			if _, ok := inner.Contains(dlon, dlat); !ok {
				continue // within 0.1% of the edge
			}
			t.Errorf("%+v: member at %v,%v (hash %d) is inside but not in %s", s, dlon, dlat, h, rangesString(ranges))
		}
	}
}

func TestParseUnit(t *testing.T) {
	tests := []struct {
		in   string
		name string
		to   float64
	}{
		{"m", "m", 1}, {"M", "m", 1},
		{"km", "km", 1000}, {"KM", "km", 1000}, {"Km", "km", 1000},
		{"ft", "ft", 0.3048}, {"FT", "ft", 0.3048},
		{"mi", "mi", 1609.34}, {"MI", "mi", 1609.34},
	}
	for _, tc := range tests {
		u, err := ParseUnit(tc.in)
		if err != nil {
			t.Errorf("ParseUnit(%q): %v", tc.in, err)
			continue
		}
		if u.Name != tc.name || u.ToMeters != tc.to {
			t.Errorf("ParseUnit(%q) = %+v, want {%s %v}", tc.in, u, tc.name, tc.to)
		}
	}

	const want = "unsupported unit provided. please use M, KM, FT, MI"
	for _, in := range []string{"", "yd", "meters", "k", "Km"} {
		_, err := ParseUnit(in)
		if err == nil || err.Error() != want {
			t.Errorf("ParseUnit(%q) error = %v, want %q", in, err, want)
		}
	}
}

func TestFormatDistance(t *testing.T) {
	// The first vectors are the fixedpoint_d2string cases in util.c.
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0.0000"},
		{math.Copysign(0, -1), "0.0000"},
		{0.0001, "0.0001"},
		{6.0642951598391699e-05, "0.0001"},
		{0.01, "0.0100"},
		{-0.01, "-0.0100"},
		{1, "1.0000"},
		{0.00004, "0.0000"},
		{0.775, "0.7750"},
		{166274.15156960033, "166274.1516"},
		{12345678.9, "12345678.9000"},
	}
	for _, tc := range tests {
		if got := FormatDistance(tc.in); got != tc.want {
			t.Errorf("FormatDistance(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
