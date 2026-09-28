// Package geohash ports the geohash code of Redis 7.2 to Go: src/geohash.c,
// src/geohash_helper.c and the GEO command helpers of src/geo.c (52-bit score
// alignment, the GEOHASH base32 string, unit parsing, distance formatting and
// the nine-cell candidate filter). Redis publishes that code under the
// BSD-3-Clause licence reproduced in LICENSE-REDIS-BSD.txt next to this file.
//
// The package holds no storage. GEOADD stores the Hash that Encode returns as
// the sorted-set score of a member. GEOSEARCH scans the score ranges that
// Shape.Areas returns, decodes each candidate score with Hash.Decode and keeps
// the members for which Shape.Contains reports true.
//
// The arithmetic follows the C source operation for operation. Every product
// that feeds an addition, and every product a function returns, is wrapped in
// float64(...) because the Go arm64 backend otherwise emits a fused
// multiply-add, also across inlined calls, which rounds once instead of twice
// and would put results one ulp away from an x86-64 Redis build.
package geohash

import (
	"fmt"
	"math"
	"strconv"
)

const (
	// StepMax is the number of interleaving steps of a stored hash. 26 steps
	// give 52 bits, which the 53-bit mantissa of a double holds exactly, so a
	// hash survives the round trip through a sorted-set score.
	StepMax = 26

	// Coordinate limits of EPSG:900913 / EPSG:3785 / OSGEO:41001. Redis
	// cannot geocode the poles.
	LatMin = -85.05112878
	LatMax = 85.05112878
	LonMin = -180
	LonMax = 180
)

// Bits is a geohash of Step interleaving steps, so 2*Step significant bits,
// with latitude in the even bit positions and longitude in the odd ones. It
// mirrors GeoHashBits in geohash.h.
type Bits struct {
	Bits uint64
	Step uint8
}

// IsZero reports whether b is the all-zero value that
// geohashCalculateAreasByShapeWGS84 writes into a neighbour it has ruled out
// of a search (the HASHISZERO macro).
func (b Bits) IsZero() bool { return b.Bits == 0 && b.Step == 0 }

// Range64 is a closed interval of degrees. It mirrors GeoHashRange.
type Range64 struct{ Min, Max float64 }

func (r Range64) isZero() bool { return r.Min == 0 && r.Max == 0 }

// Area is one geohash cell with the degrees it spans. It mirrors GeoHashArea.
type Area struct {
	Hash     Bits
	Lon, Lat Range64
}

// Center returns the midpoint of the cell, clamped to the coordinate limits.
// It mirrors geohashDecodeAreaToLongLat.
func (a Area) Center() (lon, lat float64) {
	lon = (a.Lon.Min + a.Lon.Max) / 2
	if lon > LonMax {
		lon = LonMax
	}
	if lon < LonMin {
		lon = LonMin
	}
	lat = (a.Lat.Min + a.Lat.Max) / 2
	if lat > LatMax {
		lat = LatMax
	}
	if lat < LatMin {
		lat = LatMin
	}
	return lon, lat
}

// Direction indexes the array that Neighbors returns, in the order of the
// GeoDirection enum of geohash.h.
type Direction int

// The eight neighbours of a cell.
const (
	North Direction = iota
	East
	West
	South
	SouthWest
	SouthEast
	NorthWest
	NorthEast
)

// interleave64 spreads the low 32 bits of x into the even positions and the
// low 32 bits of y into the odd positions of the result. The masks and
// shifts are the "Interleave bits by Binary Magic Numbers" sequence from
// graphics.stanford.edu/~seander/bithacks.html, as in geohash.c.
func interleave64(xlo, ylo uint32) uint64 {
	const (
		b0 = 0x5555555555555555
		b1 = 0x3333333333333333
		b2 = 0x0F0F0F0F0F0F0F0F
		b3 = 0x00FF00FF00FF00FF
		b4 = 0x0000FFFF0000FFFF
	)
	x := uint64(xlo)
	y := uint64(ylo)

	x = (x | (x << 16)) & b4
	y = (y | (y << 16)) & b4

	x = (x | (x << 8)) & b3
	y = (y | (y << 8)) & b3

	x = (x | (x << 4)) & b2
	y = (y | (y << 4)) & b2

	x = (x | (x << 2)) & b1
	y = (y | (y << 2)) & b1

	x = (x | (x << 1)) & b0
	y = (y | (y << 1)) & b0

	return x | (y << 1)
}

// deinterleave64 reverses interleave64: the even bits of the input end up in
// the low 32 bits of the result and the odd bits in the high 32.
func deinterleave64(interleaved uint64) uint64 {
	const (
		b0 = 0x5555555555555555
		b1 = 0x3333333333333333
		b2 = 0x0F0F0F0F0F0F0F0F
		b3 = 0x00FF00FF00FF00FF
		b4 = 0x0000FFFF0000FFFF
		b5 = 0x00000000FFFFFFFF
	)
	x := interleaved
	y := interleaved >> 1

	x = (x | (x >> 0)) & b0
	y = (y | (y >> 0)) & b0

	x = (x | (x >> 1)) & b1
	y = (y | (y >> 1)) & b1

	x = (x | (x >> 2)) & b2
	y = (y | (y >> 2)) & b2

	x = (x | (x >> 4)) & b3
	y = (y | (y >> 4)) & b3

	x = (x | (x >> 8)) & b4
	y = (y | (y >> 8)) & b4

	x = (x | (x >> 16)) & b5
	y = (y | (y >> 16)) & b5

	return x | (y << 32)
}

// coordRange returns the ranges Redis encodes against
// (geohashGetCoordRange).
func coordRange() (lon, lat Range64) {
	return Range64{LonMin, LonMax}, Range64{LatMin, LatMax}
}

// encode mirrors geohashEncode. It fails for a step outside 1..32, an empty
// range, or a coordinate outside the EPSG limits or the given ranges. A NaN
// coordinate fails too: the C code would reach an undefined double-to-integer
// conversion, which Redis avoids only because its argument parser rejects
// "nan" first.
func encode(lonRange, latRange Range64, lon, lat float64, step uint8) (Bits, bool) {
	if step > 32 || step == 0 || latRange.isZero() || lonRange.isZero() {
		return Bits{}, false
	}
	if math.IsNaN(lon) || math.IsNaN(lat) ||
		lon > LonMax || lon < LonMin || lat > LatMax || lat < LatMin {
		return Bits{}, false
	}

	h := Bits{Step: step}
	if lat < latRange.Min || lat > latRange.Max ||
		lon < lonRange.Min || lon > lonRange.Max {
		return h, false
	}

	latOffset := (lat - latRange.Min) / (latRange.Max - latRange.Min)
	lonOffset := (lon - lonRange.Min) / (lonRange.Max - lonRange.Min)

	// Fixed point at the step size. The double-to-uint32 conversion truncates
	// toward zero in C and in Go.
	scale := float64(uint64(1) << step)
	latOffset *= scale
	lonOffset *= scale
	h.Bits = interleave64(uint32(latOffset), uint32(lonOffset))
	return h, true
}

// EncodeWGS84 mirrors geohashEncodeWGS84: it interleaves lon and lat at the
// given step against the EPSG:900913 ranges. ok is false when encode rejects
// the input.
func EncodeWGS84(lon, lat float64, step uint8) (Bits, bool) {
	lonRange, latRange := coordRange()
	return encode(lonRange, latRange, lon, lat, step)
}

// decode mirrors geohashDecode.
func decode(lonRange, latRange Range64, h Bits) (Area, bool) {
	if h.IsZero() || latRange.isZero() || lonRange.isZero() {
		return Area{}, false
	}

	sep := deinterleave64(h.Bits) // low word latitude, high word longitude
	latScale := latRange.Max - latRange.Min
	lonScale := lonRange.Max - lonRange.Min
	ilato := uint32(sep)
	ilono := uint32(sep >> 32)
	div := float64(uint64(1) << h.Step)

	// The cell spans [i/2^step, (i+1)/2^step) of the range on each axis.
	// ilato+1 wraps at 2^32 exactly as the uint32_t arithmetic in C does.
	return Area{
		Hash: h,
		Lat: Range64{
			Min: latRange.Min + float64(float64(ilato)/div*latScale),
			Max: latRange.Min + float64(float64(ilato+1)/div*latScale),
		},
		Lon: Range64{
			Min: lonRange.Min + float64(float64(ilono)/div*lonScale),
			Max: lonRange.Min + float64(float64(ilono+1)/div*lonScale),
		},
	}, true
}

// DecodeWGS84 mirrors geohashDecodeWGS84: the cell that b names against the
// EPSG:900913 ranges. ok is false only for the zero Bits.
func DecodeWGS84(b Bits) (Area, bool) {
	lonRange, latRange := coordRange()
	return decode(lonRange, latRange, b)
}

// moveX mirrors geohash_move_x: it steps the longitude bits (the odd
// positions) by d cells, wrapping modulo 2^Step.
func moveX(h Bits, d int8) Bits {
	if d == 0 {
		return h
	}
	x := h.Bits & 0xaaaaaaaaaaaaaaaa
	y := h.Bits & 0x5555555555555555

	zz := uint64(0x5555555555555555) >> (64 - uint(h.Step)*2)
	if d > 0 {
		x += zz + 1
	} else {
		x |= zz
		x -= zz + 1
	}

	x &= uint64(0xaaaaaaaaaaaaaaaa) >> (64 - uint(h.Step)*2)
	h.Bits = x | y
	return h
}

// moveY mirrors geohash_move_y: it steps the latitude bits (the even
// positions) by d cells, wrapping modulo 2^Step.
func moveY(h Bits, d int8) Bits {
	if d == 0 {
		return h
	}
	x := h.Bits & 0xaaaaaaaaaaaaaaaa
	y := h.Bits & 0x5555555555555555

	zz := uint64(0xaaaaaaaaaaaaaaaa) >> (64 - uint(h.Step)*2)
	if d > 0 {
		y += zz + 1
	} else {
		y |= zz
		y -= zz + 1
	}

	y &= uint64(0x5555555555555555) >> (64 - uint(h.Step)*2)
	h.Bits = x | y
	return h
}

// Neighbors returns the eight cells around b, indexed by Direction. It
// mirrors geohashNeighbors: the east neighbour of the easternmost cell wraps
// to the westernmost one, and north wraps to south the same way.
func Neighbors(b Bits) [8]Bits {
	var n [8]Bits
	n[East] = moveX(b, 1)
	n[West] = moveX(b, -1)
	n[South] = moveY(b, -1)
	n[North] = moveY(b, 1)
	n[NorthWest] = moveY(moveX(b, -1), 1)
	n[NorthEast] = moveY(moveX(b, 1), 1)
	n[SouthEast] = moveY(moveX(b, 1), -1)
	n[SouthWest] = moveY(moveX(b, -1), -1)
	return n
}

// Hash is a 52-bit aligned geohash, the integer GEOADD stores as the
// sorted-set score of a member.
type Hash uint64

// Align52Bits mirrors geohashAlign52Bits: it shifts b left so that its top
// bit lands at bit 51. Step must not exceed StepMax.
func Align52Bits(b Bits) Hash {
	return Hash(b.Bits << (52 - uint(b.Step)*2))
}

// Encode mirrors what GEOADD does with a coordinate: it rejects one outside
// the EPSG:900913 limits with the text extractLongLatOrReply sends (the reply
// writer adds the "ERR " prefix), and otherwise returns EncodeWGS84 at
// StepMax aligned to 52 bits.
func Encode(lon, lat float64) (Hash, error) {
	if math.IsNaN(lon) || math.IsNaN(lat) ||
		lon < LonMin || lon > LonMax || lat < LatMin || lat > LatMax {
		return 0, fmt.Errorf("invalid longitude,latitude pair %s,%s", cFloat(lon), cFloat(lat))
	}
	b, _ := EncodeWGS84(lon, lat, StepMax)
	return Align52Bits(b), nil
}

// cFloat formats f as C's printf("%f") does. Go's %f prints "+Inf" and
// "-Inf" where C prints "inf" and "-inf".
func cFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	return strconv.FormatFloat(f, 'f', 6, 64)
}

// Decode returns the centre of the StepMax cell that h names, the coordinate
// GEOPOS reports. It mirrors decodeGeohash in geo.c.
func (h Hash) Decode() (lon, lat float64) {
	a, _ := DecodeWGS84(Bits{Bits: uint64(h), Step: StepMax})
	return a.Center()
}

// geoAlphabet is the base32 alphabet of the public geohash format.
const geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// String returns the 11-character geohash that GEOHASH reports. Redis encodes
// latitude against -85.05..85.05 while the public format uses -90..90, so
// geohashCommand decodes the score and re-encodes the centre against the
// public ranges before emitting base32. The 52 bits fill ten characters; the
// eleventh is always '0'.
func (h Hash) String() string {
	lon, lat := h.Decode()
	b, _ := encode(Range64{-180, 180}, Range64{-90, 90}, lon, lat, StepMax)

	var buf [11]byte
	for i := range buf {
		idx := 0
		if i < 10 {
			idx = int((b.Bits >> (52 - uint(i+1)*5)) & 0x1f)
		}
		buf[i] = geoAlphabet[idx]
	}
	return string(buf[:])
}
