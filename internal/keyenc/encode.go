// Package keyenc encodes BSON values into byte strings whose bytes.Compare
// order is MongoDB's comparison order: MinKey < Undefined < Null (and
// missing) < numbers < strings < objects < arrays < BinData < ObjectId <
// bool < Date < Timestamp < regex < DBPointer < JavaScript < CodeWScope <
// MaxKey, with values of one class compared the way MongoDB compares them.
//
// An encoding is one type byte (the T* constants, taken from MongoDB
// KeyString) followed by a payload:
//
//   - numbers: one numeric class, exact for every int32, int64 and double, so
//     1, int64(1) and 1.0 encode identically and 2^53+1 sorts between the
//     doubles 2^53 and 2^53+2 (see numeric.go for the class table);
//   - strings, symbols and JavaScript code: the bytes with 0x00 written as
//     0x00 0xFF, then a 0x00 terminator; field names, regex parts and
//     DBPointer namespaces go through the same escaping but hold no 0x00
//     (Encode refuses a DBPointer namespace with one), so the decoder reads
//     them up to the first 0x00;
//   - objects: per field the value's type byte, the escaped field name and
//     its terminator, the value's payload; then a 0x00 terminator;
//   - arrays: each element's full encoding, then a 0x00 terminator;
//   - BinData: length (4, big-endian), subtype, bytes; ObjectId: 12 bytes;
//     bool: 0x00 or 0x01; Date: int64 ms with the sign bit flipped;
//     Timestamp: t (4) then i (4); regex: escaped pattern and options.
//
// EncodeDesc flips every byte, type byte included, so a descending index
// field reverses the cross-type order too. Decimal128 is refused with
// ErrUnsupported. Decode recovers a value that encodes to the same bytes;
// the original numeric type and the String/Symbol distinction are not kept.
package keyenc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Type bytes. They are MongoDB KeyString's canonical type bytes, so that
// comparing the first byte of two encodings reproduces MongoDB's cross-type
// order.
const (
	TMinKey     byte = 10
	TUndefined  byte = 15
	TNull       byte = 20 // also a missing field
	TNumber     byte = 30 // int32, int64, double
	TString     byte = 60 // String and Symbol
	TObject     byte = 70
	TArray      byte = 80
	TBinData    byte = 90
	TOID        byte = 100
	TBool       byte = 110
	TDate       byte = 120
	TTimestamp  byte = 130
	TRegex      byte = 140
	TDBPointer  byte = 150
	TCode       byte = 160
	TCodeWScope byte = 170
	TMaxKey     byte = 240
)

// maxDepth bounds object and array nesting on both encode and decode.
// Documents are validated to depth 100 at insert; the margin leaves room for
// CodeWScope scopes and for callers that wrap values.
const maxDepth = 200

var (
	// ErrUnsupported is returned for Decimal128. Its text is the reply body
	// architecture.md section 3 fixes; command handlers prefix "ERR ".
	ErrUnsupported = errors.New("Decimal128 is not supported in indexes or arithmetic in v1")
	// ErrMalformed is returned by Encode for BSON bytes that do not parse,
	// an unknown BSON type, nesting deeper than 200, or a DBPointer whose
	// namespace holds 0x00.
	ErrMalformed = errors.New("keyenc: malformed BSON value")
	// ErrCorrupt is returned by Decode, DecodeDesc, Skip and SkipDesc for
	// bytes that no Encode call produces.
	ErrCorrupt = errors.New("keyenc: corrupt key encoding")
)

// TypeOrder returns the type byte for a BSON type: TNumber for every numeric
// type including Decimal128, TString for String and Symbol, TNull for the
// zero Type (a missing field), and 0 for a byte that is not a BSON type.
func TypeOrder(t bson.Type) byte {
	switch t {
	case 0, bson.TypeNull:
		return TNull
	case bson.TypeMinKey:
		return TMinKey
	case bson.TypeUndefined:
		return TUndefined
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128:
		return TNumber
	case bson.TypeString, bson.TypeSymbol:
		return TString
	case bson.TypeEmbeddedDocument:
		return TObject
	case bson.TypeArray:
		return TArray
	case bson.TypeBinary:
		return TBinData
	case bson.TypeObjectID:
		return TOID
	case bson.TypeBoolean:
		return TBool
	case bson.TypeDateTime:
		return TDate
	case bson.TypeTimestamp:
		return TTimestamp
	case bson.TypeRegex:
		return TRegex
	case bson.TypeDBPointer:
		return TDBPointer
	case bson.TypeJavaScript:
		return TCode
	case bson.TypeCodeWithScope:
		return TCodeWScope
	case bson.TypeMaxKey:
		return TMaxKey
	}
	return 0
}

// Encode appends the ascending encoding of v to dst. The zero RawValue
// (Type 0, no bytes, what bson.Raw.Lookup returns for an absent path)
// encodes as a missing field, the same bytes as Null. On error dst is
// returned with its original length.
func Encode(dst []byte, v bson.RawValue) ([]byte, error) {
	start := len(dst)
	out, err := appendValue(dst, v.Type, v.Value, 0)
	if err != nil {
		return dst[:start], err
	}
	return out, nil
}

// EncodeDesc appends the descending encoding of v: Encode's bytes, each
// XORed with 0xFF.
//
// A String (or Symbol, or JavaScript) encoding is a byte prefix of the
// encoding of the same string extended by a 0x00 byte and more. Where a
// descending string ends the key, "a" therefore still sorts before
// "a\x00...", the ascending order. Any value encoded after it (such as the
// ks(_id) of a non-unique index entry) restores the descending order.
func EncodeDesc(dst []byte, v bson.RawValue) ([]byte, error) {
	start := len(dst)
	out, err := Encode(dst, v)
	if err != nil {
		return out, err
	}
	invert(out[start:])
	return out, nil
}

// EncodeMissing appends the encoding of a missing field: TNull, the same
// byte a BSON Null encodes to, since MongoDB compares the two as equal.
func EncodeMissing(dst []byte) []byte { return append(dst, TNull) }

// AppendString appends the full encoding of a String with contents s.
func AppendString(dst []byte, s string) []byte {
	dst = append(dst, TString)
	return appendEscaped(dst, s)
}

// Next returns the smallest byte string greater than every string that
// starts with prefix: prefix with trailing 0xFF bytes dropped and its last
// remaining byte incremented. It returns nil when prefix is empty or all
// 0xFF, meaning no upper bound. prefix is not modified.
func Next(prefix []byte) []byte {
	n := len(prefix)
	for n > 0 && prefix[n-1] == 0xFF {
		n--
	}
	if n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, prefix[:n])
	out[n-1]++
	return out
}

// ValueRange returns [lo, hi) covering every key that starts with enc (one
// complete Encode or EncodeDesc output) followed by at least one more
// encoded value, ascending or descending: lo is enc+0x01 and hi is
// enc+0xFF. Use it for "leading field equals v" scans of compound keys, for
// example a non-unique index entry, which always ends with ks(_id). Unlike
// [enc, Next(enc)) it excludes the strings that extend a String value with
// a 0x00 byte, whose encodings share enc as a byte prefix.
func ValueRange(enc []byte) (lo, hi []byte) {
	lo = make([]byte, len(enc)+1)
	copy(lo, enc)
	lo[len(enc)] = 0x01
	hi = make([]byte, len(enc)+1)
	copy(hi, enc)
	hi[len(enc)] = 0xFF
	return lo, hi
}

func invert(b []byte) {
	for i := range b {
		b[i] ^= 0xFF
	}
}

// appendEscaped appends s with every 0x00 written as 0x00 0xFF, then the
// 0x00 terminator.
func appendEscaped[T ~string | ~[]byte](dst []byte, s T) []byte {
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			dst = append(dst, s[start:i+1]...)
			dst = append(dst, 0xFF)
			start = i + 1
		}
	}
	dst = append(dst, s[start:]...)
	return append(dst, 0)
}

func appendValue(dst []byte, t bson.Type, val []byte, depth int) ([]byte, error) {
	if t == 0 {
		if len(val) != 0 {
			return dst, ErrMalformed
		}
		return append(dst, TNull), nil
	}
	if t == bson.TypeDecimal128 {
		return dst, ErrUnsupported
	}
	tb := TypeOrder(t)
	if tb == 0 {
		return dst, ErrMalformed
	}
	dst = append(dst, tb)
	return appendPayload(dst, t, val, depth)
}

// appendPayload appends the encoding of val after its type byte. val must be
// exactly one BSON value of type t.
func appendPayload(dst []byte, t bson.Type, val []byte, depth int) ([]byte, error) {
	switch t {
	case bson.TypeNull, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
		if len(val) != 0 {
			return dst, ErrMalformed
		}
		return dst, nil
	case bson.TypeDouble:
		if len(val) != 8 {
			return dst, ErrMalformed
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(val))
		return appendDoublePayload(dst, f), nil
	case bson.TypeInt32:
		if len(val) != 4 {
			return dst, ErrMalformed
		}
		return appendInt64Payload(dst, int64(int32(binary.LittleEndian.Uint32(val)))), nil
	case bson.TypeInt64:
		if len(val) != 8 {
			return dst, ErrMalformed
		}
		return appendInt64Payload(dst, int64(binary.LittleEndian.Uint64(val))), nil
	case bson.TypeDecimal128:
		return dst, ErrUnsupported
	case bson.TypeString, bson.TypeSymbol:
		s, ok := bsonString(val)
		if !ok || len(s)+5 != len(val) {
			return dst, ErrMalformed
		}
		return appendEscaped(dst, s), nil
	case bson.TypeEmbeddedDocument:
		return appendDocPayload(dst, val, false, depth+1)
	case bson.TypeArray:
		return appendDocPayload(dst, val, true, depth+1)
	case bson.TypeBinary:
		if len(val) < 5 {
			return dst, ErrMalformed
		}
		n := int32(binary.LittleEndian.Uint32(val))
		if n < 0 || int(n)+5 != len(val) {
			return dst, ErrMalformed
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(n))
		return append(dst, val[4:]...), nil
	case bson.TypeObjectID:
		if len(val) != 12 {
			return dst, ErrMalformed
		}
		return append(dst, val...), nil
	case bson.TypeBoolean:
		if len(val) != 1 || val[0] > 1 {
			return dst, ErrMalformed
		}
		return append(dst, val[0]), nil
	case bson.TypeDateTime:
		if len(val) != 8 {
			return dst, ErrMalformed
		}
		return binary.BigEndian.AppendUint64(dst, binary.LittleEndian.Uint64(val)^1<<63), nil
	case bson.TypeTimestamp:
		if len(val) != 8 {
			return dst, ErrMalformed
		}
		// BSON stores i in the low and t in the high 32 bits of a
		// little-endian uint64; its big-endian form is t then i.
		return binary.BigEndian.AppendUint64(dst, binary.LittleEndian.Uint64(val)), nil
	case bson.TypeRegex:
		p := bytes.IndexByte(val, 0)
		if p < 0 {
			return dst, ErrMalformed
		}
		o := bytes.IndexByte(val[p+1:], 0)
		if o < 0 || p+1+o+1 != len(val) {
			return dst, ErrMalformed
		}
		dst = appendEscaped(dst, val[:p])
		return appendEscaped(dst, val[p+1:p+1+o]), nil
	case bson.TypeDBPointer:
		// The namespace is followed by a raw ObjectId, so it must not hold
		// 0x00 (MongoDB namespaces cannot); an escaped 0x00 there would be
		// indistinguishable from the terminator and an ObjectId starting
		// with 0xFF.
		s, ok := bsonString(val)
		if !ok || len(s)+5+12 != len(val) || bytes.IndexByte(s, 0) >= 0 {
			return dst, ErrMalformed
		}
		dst = append(dst, TString)
		dst = appendEscaped(dst, s)
		return append(dst, val[len(val)-12:]...), nil
	case bson.TypeJavaScript:
		s, ok := bsonString(val)
		if !ok || len(s)+5 != len(val) {
			return dst, ErrMalformed
		}
		dst = append(dst, TString)
		return appendEscaped(dst, s), nil
	case bson.TypeCodeWithScope:
		if len(val) < 4 || int(int32(binary.LittleEndian.Uint32(val))) != len(val) {
			return dst, ErrMalformed
		}
		s, ok := bsonString(val[4:])
		if !ok {
			return dst, ErrMalformed
		}
		dst = append(dst, TString)
		dst = appendEscaped(dst, s)
		dst = append(dst, TObject)
		return appendDocPayload(dst, val[4+4+len(s)+1:], false, depth+1)
	}
	return dst, ErrMalformed
}

// appendDocPayload encodes a BSON document (or array, whose field names are
// skipped) that must span all of doc.
func appendDocPayload(dst, doc []byte, array bool, depth int) ([]byte, error) {
	if depth > maxDepth || len(doc) < 5 {
		return dst, ErrMalformed
	}
	n := int32(binary.LittleEndian.Uint32(doc))
	if int(n) != len(doc) || doc[n-1] != 0 {
		return dst, ErrMalformed
	}
	body := doc[4 : n-1]
	for len(body) > 0 {
		t := bson.Type(body[0])
		nameLen := bytes.IndexByte(body[1:], 0)
		if t == 0 || nameLen < 0 {
			return dst, ErrMalformed
		}
		name := body[1 : 1+nameLen]
		rest := body[2+nameLen:]
		size, ok := valueSize(t, rest)
		if !ok {
			return dst, ErrMalformed
		}
		var err error
		if array {
			dst, err = appendValue(dst, t, rest[:size], depth)
		} else {
			if t == bson.TypeDecimal128 {
				return dst, ErrUnsupported
			}
			tb := TypeOrder(t)
			if tb == 0 {
				return dst, ErrMalformed
			}
			dst = append(dst, tb)
			dst = appendEscaped(dst, name)
			dst, err = appendPayload(dst, t, rest[:size], depth)
		}
		if err != nil {
			return dst, err
		}
		body = rest[size:]
	}
	return append(dst, 0), nil
}

// bsonString parses a BSON string (int32 length including the NUL, bytes,
// NUL) at the start of b and returns its contents.
func bsonString(b []byte) ([]byte, bool) {
	if len(b) < 5 {
		return nil, false
	}
	n := int32(binary.LittleEndian.Uint32(b))
	if n < 1 || int(n) > len(b)-4 || b[4+n-1] != 0 {
		return nil, false
	}
	return b[4 : 4+n-1], true
}

// valueSize returns the size of the BSON value of type t at the start of b.
func valueSize(t bson.Type, b []byte) (int, bool) {
	switch t {
	case bson.TypeDouble, bson.TypeDateTime, bson.TypeInt64, bson.TypeTimestamp:
		return 8, len(b) >= 8
	case bson.TypeInt32:
		return 4, len(b) >= 4
	case bson.TypeDecimal128:
		return 16, len(b) >= 16
	case bson.TypeObjectID:
		return 12, len(b) >= 12
	case bson.TypeBoolean:
		return 1, len(b) >= 1
	case bson.TypeNull, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
		return 0, true
	case bson.TypeString, bson.TypeJavaScript, bson.TypeSymbol:
		s, ok := bsonString(b)
		return len(s) + 5, ok
	case bson.TypeDBPointer:
		s, ok := bsonString(b)
		return len(s) + 5 + 12, ok && len(b) >= len(s)+5+12
	case bson.TypeEmbeddedDocument, bson.TypeArray, bson.TypeCodeWithScope:
		if len(b) < 4 {
			return 0, false
		}
		n := int32(binary.LittleEndian.Uint32(b))
		return int(n), n >= 5 && int(n) <= len(b)
	case bson.TypeBinary:
		if len(b) < 5 {
			return 0, false
		}
		n := int32(binary.LittleEndian.Uint32(b))
		return int(n) + 5, n >= 0 && int(n) <= len(b)-5
	case bson.TypeRegex:
		p := bytes.IndexByte(b, 0)
		if p < 0 {
			return 0, false
		}
		o := bytes.IndexByte(b[p+1:], 0)
		return p + 1 + o + 1, o >= 0
	}
	return 0, false
}
