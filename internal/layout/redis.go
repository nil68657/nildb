// Package layout builds and parses every RocksDB key and value NilDB
// writes, following architecture.md section 3: Redis metadata and the
// version map in the meta column family, collection elements in sub, the
// sorted-set score index in zscore, document rows, secondary index, 2dsphere
// and columnar entries, and the catalog records in default. It also issues
// the 64-bit collection versions.
//
// Integers are big-endian unless a function says otherwise. Builders append
// to dst and return the extended slice; parsers return views into their
// input, never copies.
package layout

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrCorrupt wraps every parse failure of a stored value.
var ErrCorrupt = errors.New("layout: corrupt value")

// NumDBs is the number of Redis databases; SELECT accepts 0 to NumDBs-1 and
// the database number is the first byte of every metadata key.
const NumDBs = 16

// VersionMapPrefix is the first byte of every version-map key in meta. It is
// above every database number, so SCAN over [db] never sees it.
const VersionMapPrefix byte = 0x76 // 'v'

// Type is the Redis type stored in bits 0-3 of the metadata flags byte.
type Type uint8

// Redis types. The zero Type means "no key".
const (
	TString Type = 1
	THash   Type = 2
	TList   Type = 3
	TSet    Type = 4
	TZSet   Type = 5
)

// String returns the name Redis's TYPE command replies with: "string",
// "hash", "list", "set" or "zset"; "none" for the zero Type.
func (t Type) String() string {
	switch t {
	case 0:
		return "none"
	case TString:
		return "string"
	case THash:
		return "hash"
	case TList:
		return "list"
	case TSet:
		return "set"
	case TZSet:
		return "zset"
	}
	return fmt.Sprintf("Type(%d)", uint8(t))
}

// IsCollection reports whether t keeps its elements under a version in the
// sub column family (hash, list, set, zset).
func (t Type) IsCollection() bool { return t >= THash && t <= TZSet }

// LayoutVersion is the value of bits 4-7 of the metadata flags byte.
// DecodeMeta refuses any other.
const LayoutVersion = 0

// Metadata value sizes. A string's header is followed by its payload.
const (
	StringHeaderLen = 1 + 8                   // flags, expire_ms
	CollMetaLen     = StringHeaderLen + 8 + 8 // + version, size
	ListMetaLen     = CollMetaLen + 8 + 8     // + head, tail
)

// ListStart is the head and tail of a new list. Live indexes are
// [Head, Tail); LPUSH writes at Head-1, RPUSH at Tail.
const ListStart uint64 = 1 << 63

// MetaKey appends | db (1) | user key |, the meta key of a Redis key.
// MetaKey(dst, db, nil) is the first key of database db, the start of SCAN.
func MetaKey(dst []byte, db uint8, key []byte) []byte {
	dst = append(dst, db)
	return append(dst, key...)
}

// SplitMetaKey parses a meta key. ok is false for keys that do not start
// with a database number (version-map keys and anything above NumDBs).
func SplitMetaKey(k []byte) (db uint8, key []byte, ok bool) {
	if len(k) == 0 || k[0] >= NumDBs {
		return 0, nil, false
	}
	return k[0], k[1:], true
}

// Meta is a decoded metadata value.
//
//	string         | flags (1) | expire_ms (8) | payload (n) |
//	hash/set/zset  | flags (1) | expire_ms (8) | version (8) | size (8) |
//	list           | flags (1) | expire_ms (8) | version (8) | size (8) | head (8) | tail (8) |
type Meta struct {
	Type       Type
	ExpireMS   int64  // absolute Unix ms; 0 means no expiry
	Version    uint64 // collections only
	Size       int64  // collections only: element count
	Head, Tail uint64 // lists only
	Payload    []byte // strings only; DecodeMeta returns a view into its input
}

// Encode appends the metadata value. Fields that the type does not store
// are ignored. A Type outside TString..TZSet writes only the 9-byte header,
// which DecodeMeta rejects.
func (m Meta) Encode(dst []byte) []byte {
	dst = append(dst, byte(m.Type)&0x0F|LayoutVersion<<4)
	dst = binary.BigEndian.AppendUint64(dst, uint64(m.ExpireMS))
	switch m.Type {
	case TString:
		return append(dst, m.Payload...)
	case THash, TSet, TZSet, TList:
		dst = binary.BigEndian.AppendUint64(dst, m.Version)
		dst = binary.BigEndian.AppendUint64(dst, uint64(m.Size))
		if m.Type == TList {
			dst = binary.BigEndian.AppendUint64(dst, m.Head)
			dst = binary.BigEndian.AppendUint64(dst, m.Tail)
		}
	}
	return dst
}

// DecodeMeta parses a metadata value. A string's Payload aliases v.
func DecodeMeta(v []byte) (Meta, error) {
	if len(v) < StringHeaderLen {
		return Meta{}, fmt.Errorf("%w: metadata of %d bytes", ErrCorrupt, len(v))
	}
	if lv := v[0] >> 4; lv != LayoutVersion {
		return Meta{}, fmt.Errorf("%w: metadata layout version %d", ErrCorrupt, lv)
	}
	m := Meta{Type: Type(v[0] & 0x0F), ExpireMS: int64(binary.BigEndian.Uint64(v[1:]))}
	want := CollMetaLen
	switch m.Type {
	case TString:
		m.Payload = v[StringHeaderLen:]
		return m, nil
	case THash, TSet, TZSet:
	case TList:
		want = ListMetaLen
	default:
		return Meta{}, fmt.Errorf("%w: metadata type %d", ErrCorrupt, m.Type)
	}
	if len(v) != want {
		return Meta{}, fmt.Errorf("%w: %s metadata of %d bytes, want %d", ErrCorrupt, m.Type, len(v), want)
	}
	m.Version = binary.BigEndian.Uint64(v[9:])
	m.Size = int64(binary.BigEndian.Uint64(v[17:]))
	if m.Type == TList {
		m.Head = binary.BigEndian.Uint64(v[25:])
		m.Tail = binary.BigEndian.Uint64(v[33:])
	}
	return m, nil
}

// Expired reports whether the key is logically gone at nowMS: it has an
// expiry and the expiry is not in the future (Redis deletes a key whose
// expire time equals now).
func (m Meta) Expired(nowMS int64) bool {
	return m.ExpireMS != 0 && m.ExpireMS <= nowMS
}

// VersionKey appends | 0x76 | version (8) |, the version-map key of a
// collection version.
func VersionKey(dst []byte, version uint64) []byte {
	dst = append(dst, VersionMapPrefix)
	return binary.BigEndian.AppendUint64(dst, version)
}

// SplitVersionKey parses a version-map key.
func SplitVersionKey(k []byte) (version uint64, ok bool) {
	if len(k) != 9 || k[0] != VersionMapPrefix {
		return 0, false
	}
	return binary.BigEndian.Uint64(k[1:]), true
}

// VersionEntry is the value of a version-map key: which Redis key owns the
// version and when it expires. The sub-key compaction filter reads it.
//
//	| expire_ms (8) | db (1) | user key (n) |
type VersionEntry struct {
	ExpireMS int64
	DB       uint8
	Key      []byte // DecodeVersionEntry returns a view into its input
}

// Encode appends the version-map value.
func (e VersionEntry) Encode(dst []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(e.ExpireMS))
	dst = append(dst, e.DB)
	return append(dst, e.Key...)
}

// DecodeVersionEntry parses a version-map value. Key aliases v.
func DecodeVersionEntry(v []byte) (VersionEntry, error) {
	if len(v) < 9 {
		return VersionEntry{}, fmt.Errorf("%w: version entry of %d bytes", ErrCorrupt, len(v))
	}
	return VersionEntry{ExpireMS: int64(binary.BigEndian.Uint64(v)), DB: v[8], Key: v[9:]}, nil
}

// Expired reports whether the entry's key is logically gone at nowMS, with
// the same rule as Meta.Expired.
func (e VersionEntry) Expired(nowMS int64) bool {
	return e.ExpireMS != 0 && e.ExpireMS <= nowMS
}
