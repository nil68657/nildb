package layout

import (
	"encoding/binary"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Document-side keys. Every one starts with a 4-byte id, coll_id in doc and
// idx_id in idx, geo and col, which the idFilter compaction filter reads.
// ks(v) is keyenc.Encode (or EncodeDesc for a descending index field).
//
//	doc         | coll_id (4) | ks(_id) |                  ->  raw BSON
//	idx         | idx_id (4) | ks(f1) | ... | ks(fk) | ks(_id) |  ->  (empty)      non-unique
//	            | idx_id (4) | ks(f1) | ... | ks(fk) |            ->  | ks(_id) |  unique
//	geo         | idx_id (4) | cell_id (8) | ks(_id) |     ->  | 0x01 | lat f64 | lng f64 |  or  | 0x02 |
//	col         | idx_id (4) | ks(_id) |                   ->  | BSON type (1) | BSON value (n) |

// Geo value kinds, the first byte of a geo entry's value.
const (
	GeoPoint byte = 0x01 // followed by lat and lng
	GeoShape byte = 0x02 // shapes and multi-geometries; refine against the row
)

// DocKey appends | coll_id | ks(_id) |.
func DocKey(dst []byte, coll uint32, ksID []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, coll)
	return append(dst, ksID...)
}

// SplitDocKey parses a doc key, or a col key, which has the same shape. It
// returns zeros for keys shorter than 4 bytes.
func SplitDocKey(k []byte) (coll uint32, ksID []byte) {
	if len(k) < 4 {
		return 0, nil
	}
	return binary.BigEndian.Uint32(k), k[4:]
}

// IdxKey appends | idx_id | ksValues... |. For a non-unique index the
// caller appends ks(_id) as well; a unique index stores ks(_id) as the
// value instead.
func IdxKey(dst []byte, idx uint32, ksValues ...[]byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, idx)
	for _, v := range ksValues {
		dst = append(dst, v...)
	}
	return dst
}

// GeoKey appends | idx_id | cell_id | ks(_id) |. Cell ids are S2 cell ids,
// which sort along the Hilbert curve as big-endian uint64s.
func GeoKey(dst []byte, idx uint32, cell uint64, ksID []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, idx)
	dst = binary.BigEndian.AppendUint64(dst, cell)
	return append(dst, ksID...)
}

// GeoCellPrefix appends | idx_id | cell_id |, the 12-byte prefix the geo
// column family's prefix extractor and bloom filter work on.
func GeoCellPrefix(dst []byte, idx uint32, cell uint64) []byte {
	dst = binary.BigEndian.AppendUint32(dst, idx)
	return binary.BigEndian.AppendUint64(dst, cell)
}

// SplitGeoKey parses a geo key. It returns zeros for keys shorter than 12
// bytes.
func SplitGeoKey(k []byte) (idx uint32, cell uint64, ksID []byte) {
	if len(k) < 12 {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint32(k), binary.BigEndian.Uint64(k[4:]), k[12:]
}

// GeoPointValue appends the value of a point entry: 0x01, then latitude and
// longitude as big-endian IEEE-754 bits.
func GeoPointValue(dst []byte, lat, lng float64) []byte {
	dst = append(dst, GeoPoint)
	dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(lat))
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(lng))
}

// GeoShapeValue appends the value of a shape or multi-geometry entry.
func GeoShapeValue(dst []byte) []byte { return append(dst, GeoShape) }

// DecodeGeoValue parses a geo entry value. lat and lng are set only for
// GeoPoint.
func DecodeGeoValue(v []byte) (kind byte, lat, lng float64, err error) {
	switch {
	case len(v) == 17 && v[0] == GeoPoint:
		return GeoPoint, math.Float64frombits(binary.BigEndian.Uint64(v[1:])), math.Float64frombits(binary.BigEndian.Uint64(v[9:])), nil
	case len(v) == 1 && v[0] == GeoShape:
		return GeoShape, 0, 0, nil
	}
	return 0, 0, 0, fmt.Errorf("%w: geo value %x", ErrCorrupt, v)
}

// ColKey appends | idx_id | ks(_id) |.
func ColKey(dst []byte, idx uint32, ksID []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, idx)
	return append(dst, ksID...)
}

// ColValue appends a columnar value: the BSON type byte, then the raw BSON
// element value bytes (not ks()), so $sum reads the number and $unwind the
// array.
func ColValue(dst []byte, v bson.RawValue) []byte {
	dst = append(dst, byte(v.Type))
	return append(dst, v.Value...)
}

// DecodeColValue parses a columnar value. The returned Value aliases b.
func DecodeColValue(b []byte) (bson.RawValue, error) {
	if len(b) == 0 {
		return bson.RawValue{}, fmt.Errorf("%w: empty column value", ErrCorrupt)
	}
	return bson.RawValue{Type: bson.Type(b[0]), Value: b[1:]}, nil
}

// IDPrefix appends the 4-byte id that starts every doc, idx, geo and col
// key of one collection or index.
func IDPrefix(dst []byte, id uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, id)
}

// IDBounds returns [lo, hi) holding every key of one collection or index:
// lo is the id, hi the id plus one. hi is nil (no upper bound) for
// math.MaxUint32. Drops pass these to store.DeleteRanges.
func IDBounds(id uint32) (lo, hi []byte) {
	lo = IDPrefix(nil, id)
	if id == math.MaxUint32 {
		return lo, nil
	}
	return lo, IDPrefix(nil, id+1)
}
