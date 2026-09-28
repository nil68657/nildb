package layout

import (
	"encoding/binary"
	"fmt"
)

// Catalog records in the default column family. Keys are printable so
// ROCKS.SCAN default reads like a directory.
//
//	| 0x00 |                    ->  "nildb-layout-1"
//	| 'c' | namespace |         ->  BSON {coll_id, created_ms}
//	| 'i' | coll_id (4) | name | ->  BSON index record
//	| 'n' | coll_id (4) |       ->  int64 LE document count (merge nildb.i64add)
//	| 's' | name |              ->  next id u32 ("coll", "idx"), last version u64 ("version"), big-endian

// LayoutMarkerKey is the key of the layout marker. Open refuses a store
// whose marker is not LayoutMarker.
var LayoutMarkerKey = []byte{0}

// LayoutMarker is the value stored under LayoutMarkerKey.
const LayoutMarker = "nildb-layout-1"

// Catalog key tags, the first byte of each record kind.
const (
	CollTag  byte = 'c'
	IndexTag byte = 'i'
	CountTag byte = 'n'
	SeqTag   byte = 's'
)

// Sequence names for SeqKey.
const (
	SeqColl    = "coll"    // next coll_id, u32 big-endian
	SeqIdx     = "idx"     // next idx_id, u32 big-endian
	SeqVersion = "version" // last issued Redis collection version, u64 big-endian
)

// CollKey returns | 'c' | namespace |. CollKey("") is the prefix of every
// collection record.
func CollKey(ns string) []byte {
	k := make([]byte, 0, 1+len(ns))
	k = append(k, CollTag)
	return append(k, ns...)
}

// SplitCollKey returns the namespace of a collection record key.
func SplitCollKey(k []byte) (ns string, ok bool) {
	if len(k) < 1 || k[0] != CollTag {
		return "", false
	}
	return string(k[1:]), true
}

// IndexKey returns | 'i' | coll_id | name |. IndexKey(coll, "") is the
// prefix of every index record of one collection.
func IndexKey(coll uint32, name string) []byte {
	k := make([]byte, 0, 5+len(name))
	k = append(k, IndexTag)
	k = binary.BigEndian.AppendUint32(k, coll)
	return append(k, name...)
}

// SplitIndexKey parses an index record key.
func SplitIndexKey(k []byte) (coll uint32, name string, ok bool) {
	if len(k) < 5 || k[0] != IndexTag {
		return 0, "", false
	}
	return binary.BigEndian.Uint32(k[1:]), string(k[5:]), true
}

// CountKey returns | 'n' | coll_id |, the key of a collection's document
// count.
func CountKey(coll uint32) []byte {
	k := make([]byte, 0, 5)
	k = append(k, CountTag)
	return binary.BigEndian.AppendUint32(k, coll)
}

// SeqKey returns | 's' | name |; name is SeqColl, SeqIdx or SeqVersion.
func SeqKey(name string) []byte {
	k := make([]byte, 0, 1+len(name))
	k = append(k, SeqTag)
	return append(k, name...)
}

// AppendCount appends n as 8 bytes little-endian: the document count value
// and the operand of the nildb.i64add merge (use a negative n to subtract).
func AppendCount(dst []byte, n int64) []byte {
	return binary.LittleEndian.AppendUint64(dst, uint64(n))
}

// DecodeCount parses a document count value.
func DecodeCount(v []byte) (int64, error) {
	if len(v) != 8 {
		return 0, fmt.Errorf("%w: count of %d bytes", ErrCorrupt, len(v))
	}
	return int64(binary.LittleEndian.Uint64(v)), nil
}
