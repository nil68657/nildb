package geo

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// EarthRadiusMetres is MongoDB's kRadiusOfEarthInMeters. golang/geo's
// earth.Radius (6371.01 km) is not used anywhere in NilDB's 2dsphere code.
const EarthRadiusMetres = 6378100.0

// DefaultQueryMaxCells is the query-side covering budget
// (nildb.geo-query-max-cells) used when a caller passes 0.
const DefaultQueryMaxCells = 16

// Metres converts an angle on the unit sphere to metres on the Earth.
func Metres(a s1.Angle) float64 { return a.Radians() * EarthRadiusMetres }

// Angle converts metres on the Earth to an angle on the unit sphere.
func Angle(metres float64) s1.Angle { return s1.Angle(metres / EarthRadiusMetres) }

// Params are the covering parameters of one 2dsphere index. They are frozen
// in the catalog record at index creation so later changes to the defaults
// never change the cells an existing index holds.
type Params struct {
	Coarsest int // lowest cell level any non-point entry uses
	Finest   int // highest cell level any non-point entry uses
	MaxCells int // covering budget per indexed geometry
}

// DefaultParams returns MongoDB's 2dsphere index version 3 and 4 defaults:
// the levels whose average edge is closest to 2000 km and 110 m at radius
// EarthRadiusMetres (levels 2 and 16), and 20 cells per covering.
func DefaultParams() Params {
	return Params{
		Coarsest: s2.AvgEdgeMetric.ClosestLevel(2000e3 / EarthRadiusMetres),
		Finest:   s2.AvgEdgeMetric.ClosestLevel(110 / EarthRadiusMetres),
		MaxCells: 20,
	}
}

// Index value kinds, the first byte of a geo CF value.
const (
	ValuePoint byte = 0x01 // followed by lat and lng
	ValueShape byte = 0x02 // nothing follows; read the row
)

// Entry is one key of a document in a 2dsphere index.
type Entry struct {
	Cell   s2.CellID
	Kind   byte      // ValuePoint (LatLng valid) or ValueShape
	LatLng s2.LatLng // the exact parsed position when Kind is ValuePoint
}

// AppendValue appends the geo CF value for e: 0x01 followed by LatLng.Lat
// and LatLng.Lng as big-endian IEEE 754 radians (17 bytes), or the single
// byte 0x02. Radians are stored rather than degrees so that DecodeValue
// returns bit-for-bit the LatLng Parse produced, and a point's distance
// computed from the index equals Distance on the parsed document.
func (e Entry) AppendValue(dst []byte) []byte {
	if e.Kind != ValuePoint {
		return append(dst, ValueShape)
	}
	dst = append(dst, ValuePoint)
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(e.LatLng.Lat.Radians()))
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(e.LatLng.Lng.Radians()))
}

// ErrBadValue reports a geo CF value that is neither layout.
var ErrBadValue = errors.New("geo: malformed 2dsphere index value")

// DecodeValue parses a value written by Entry.AppendValue.
func DecodeValue(val []byte) (kind byte, ll s2.LatLng, err error) {
	switch {
	case len(val) == 1 && val[0] == ValueShape:
		return ValueShape, s2.LatLng{}, nil
	case len(val) == 17 && val[0] == ValuePoint:
		lat := math.Float64frombits(binary.BigEndian.Uint64(val[1:9]))
		lng := math.Float64frombits(binary.BigEndian.Uint64(val[9:17]))
		return ValuePoint, s2.LatLng{Lat: s1.Angle(lat), Lng: s1.Angle(lng)}, nil
	}
	return 0, s2.LatLng{}, ErrBadValue
}

// Entries returns the index keys for g, sorted by cell. A Point yields its
// level-30 leaf cell with the position in the value. Every other kind
// yields the union of its parts' RegionCoverer coverings between Coarsest
// and Finest with at most MaxCells cells per part, normalised and then
// expanded back so no cell is coarser than Coarsest, all with ValueShape.
func (p Params) Entries(g *Geometry) []Entry {
	if g == nil {
		return nil
	}
	if ll, ok := g.LatLng(); ok {
		return []Entry{{Cell: s2.CellIDFromLatLng(ll), Kind: ValuePoint, LatLng: ll}}
	}
	rc := p.indexCoverer()
	var cells s2.CellUnion
	for _, a := range g.atoms(nil) {
		cells = append(cells, rc.Covering(a.region())...)
	}
	if len(cells) == 0 {
		return nil
	}
	cells.Normalize()
	cells.Denormalize(rc.MinLevel, 1)
	out := make([]Entry, len(cells))
	for i, c := range cells {
		out[i] = Entry{Cell: c, Kind: ValueShape}
	}
	return out
}

func (p Params) indexCoverer() *s2.RegionCoverer {
	maxCells := p.MaxCells
	if maxCells <= 0 {
		maxCells = DefaultParams().MaxCells
	}
	return &s2.RegionCoverer{MinLevel: p.Coarsest, MaxLevel: p.Finest, LevelMod: 1, MaxCells: maxCells}
}

// queryCoverer clamps query coverings to the index's levels.
func (p Params) queryCoverer(maxCells int) *s2.RegionCoverer {
	if maxCells <= 0 {
		maxCells = DefaultQueryMaxCells
	}
	return &s2.RegionCoverer{MinLevel: p.Coarsest, MaxLevel: p.Finest, LevelMod: 1, MaxCells: maxCells}
}
