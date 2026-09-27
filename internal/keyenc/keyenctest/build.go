package keyenctest

import (
	"encoding/binary"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Missing is the zero RawValue, what bson.Raw.Lookup returns for an absent
// path. keyenc encodes it like Null and Compare ranks it with Null.
func Missing() bson.RawValue { return bson.RawValue{} }

// MinKey returns a BSON MinKey.
func MinKey() bson.RawValue { return bson.RawValue{Type: bson.TypeMinKey} }

// MaxKey returns a BSON MaxKey.
func MaxKey() bson.RawValue { return bson.RawValue{Type: bson.TypeMaxKey} }

// Null returns a BSON Null.
func Null() bson.RawValue { return bson.RawValue{Type: bson.TypeNull} }

// Undefined returns a BSON Undefined.
func Undefined() bson.RawValue { return bson.RawValue{Type: bson.TypeUndefined} }

// Int32 returns a BSON int32.
func Int32(i int32) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt32, Value: binary.LittleEndian.AppendUint32(nil, uint32(i))}
}

// Int64 returns a BSON int64.
func Int64(i int64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt64, Value: binary.LittleEndian.AppendUint64(nil, uint64(i))}
}

// Double returns a BSON double with exactly the bits of f (-0 and NaN
// payloads are kept).
func Double(f float64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDouble, Value: binary.LittleEndian.AppendUint64(nil, math.Float64bits(f))}
}

// Decimal returns a BSON Decimal128.
func Decimal(d bson.Decimal128) bson.RawValue {
	h, l := d.GetBytes()
	v := binary.LittleEndian.AppendUint64(nil, l)
	return bson.RawValue{Type: bson.TypeDecimal128, Value: binary.LittleEndian.AppendUint64(v, h)}
}

// String returns a BSON String; s may hold 0x00 bytes.
func String(s string) bson.RawValue {
	return bson.RawValue{Type: bson.TypeString, Value: appendString(nil, s)}
}

// Symbol returns a BSON Symbol.
func Symbol(s string) bson.RawValue {
	return bson.RawValue{Type: bson.TypeSymbol, Value: appendString(nil, s)}
}

// JS returns a BSON JavaScript code value.
func JS(code string) bson.RawValue {
	return bson.RawValue{Type: bson.TypeJavaScript, Value: appendString(nil, code)}
}

// Bool returns a BSON boolean.
func Bool(b bool) bson.RawValue {
	v := []byte{0}
	if b {
		v[0] = 1
	}
	return bson.RawValue{Type: bson.TypeBoolean, Value: v}
}

// Date returns a BSON UTC datetime of ms milliseconds since the epoch.
func Date(ms int64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDateTime, Value: binary.LittleEndian.AppendUint64(nil, uint64(ms))}
}

// Timestamp returns a BSON timestamp.
func Timestamp(t, i uint32) bson.RawValue {
	return bson.RawValue{Type: bson.TypeTimestamp, Value: binary.LittleEndian.AppendUint64(nil, uint64(t)<<32|uint64(i))}
}

// OID returns a BSON ObjectId.
func OID(id bson.ObjectID) bson.RawValue {
	return bson.RawValue{Type: bson.TypeObjectID, Value: append([]byte(nil), id[:]...)}
}

// Binary returns a BSON BinData whose stored bytes after the subtype are
// payload. For subtype 2 the caller includes the inner int32 length.
func Binary(subtype byte, payload []byte) bson.RawValue {
	v := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
	v = append(v, subtype)
	return bson.RawValue{Type: bson.TypeBinary, Value: append(v, payload...)}
}

// Regex returns a BSON regular expression. Neither string may hold 0x00.
func Regex(pattern, options string) bson.RawValue {
	v := append([]byte(pattern), 0)
	v = append(v, options...)
	return bson.RawValue{Type: bson.TypeRegex, Value: append(v, 0)}
}

// DBPointer returns a BSON DBPointer.
func DBPointer(ns string, id bson.ObjectID) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDBPointer, Value: append(appendString(nil, ns), id[:]...)}
}

// CodeWScope returns a BSON JavaScript-with-scope value.
func CodeWScope(code string, scope bson.Raw) bson.RawValue {
	v := []byte{0, 0, 0, 0}
	v = appendString(v, code)
	v = append(v, scope...)
	binary.LittleEndian.PutUint32(v, uint32(len(v)))
	return bson.RawValue{Type: bson.TypeCodeWithScope, Value: v}
}

// Field is one element of a document built by Doc or DocRaw.
type Field struct {
	Name  string
	Value bson.RawValue
}

// F is shorthand for Field{name, v}.
func F(name string, v bson.RawValue) Field { return Field{Name: name, Value: v} }

// DocRaw returns the BSON document holding fields in order. Names must not
// hold 0x00; duplicate names are kept as given.
func DocRaw(fields ...Field) bson.Raw {
	d := []byte{0, 0, 0, 0}
	for _, f := range fields {
		d = append(d, byte(f.Value.Type))
		d = append(d, f.Name...)
		d = append(d, 0)
		d = append(d, f.Value.Value...)
	}
	d = append(d, 0)
	binary.LittleEndian.PutUint32(d, uint32(len(d)))
	return d
}

// Doc returns DocRaw(fields...) as an embedded-document value.
func Doc(fields ...Field) bson.RawValue {
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: DocRaw(fields...)}
}

// Array returns a BSON array of vals keyed "0", "1", ...
func Array(vals ...bson.RawValue) bson.RawValue {
	fields := make([]Field, len(vals))
	for i, v := range vals {
		fields[i] = Field{Name: strconv.Itoa(i), Value: v}
	}
	return bson.RawValue{Type: bson.TypeArray, Value: DocRaw(fields...)}
}

func appendString(dst []byte, s string) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(s)+1))
	dst = append(dst, s...)
	return append(dst, 0)
}
