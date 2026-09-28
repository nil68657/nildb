package doc

import (
	"errors"

	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// SortSpec is a parsed sort document, fields in priority order. Its element
// type is the anonymous struct build-plan.md fixes, so a slice with the same
// element type (catalog index keys) converts to SortSpec directly.
type SortSpec []struct {
	Field string
	Desc  bool
}

// MaxSortKeys is MongoDB's limit on the fields of one sort.
const MaxSortKeys = 32

// ParseSort parses {field: 1 | -1, ...}. An empty document gives an empty
// spec.
func ParseSort(s bson.Raw) (SortSpec, error) {
	var out SortSpec
	it := newIter(s)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		name := string(e.name)
		if name == "" {
			return nil, badValue("FieldPath cannot be constructed with empty string")
		}
		for _, c := range splitPath(name) {
			if c == "" {
				return nil, badValue("FieldPath field names may not be empty strings.")
			}
			if c[0] == '$' {
				return nil, badValue("FieldPath field names may not start with '$'.")
			}
		}
		v := e.value()
		if v.Type == bson.TypeEmbeddedDocument {
			if _, ok := lookupField(v.Value, "$meta"); ok {
				return nil, badValue("unsupported sort value {$meta: ...} in v1")
			}
		}
		n, ok := numberOf(v)
		f := 0.0
		if ok {
			f = n.float()
		}
		if f != 1 && f != -1 {
			return nil, badValue("$sort key ordering must be 1 (for ascending) or -1 (for descending)")
		}
		out = append(out, struct {
			Field string
			Desc  bool
		}{name, f == -1})
	}
	if len(s) > 0 {
		if err := it.err(); err != nil {
			return nil, err
		}
	}
	if len(out) > MaxSortKeys {
		return nil, badValue("cannot sort with more than %d keys", MaxSortKeys)
	}
	return out, nil
}

// SortKey appends to dst the sort key of doc under s: for each field the
// keyenc encoding (EncodeDesc for descending fields) of the value MongoDB
// sorts by. A field whose path reaches several values (arrays, or arrays of
// documents) sorts by the smallest under Compare when ascending and the
// largest when descending; an empty array counts as Undefined and a missing
// field as Null. bytes.Compare of two keys built with the same spec orders
// the documents; append ks(_id) for a total order. Decimal128 values, which
// keyenc refuses, are keyed by their nearest double.
//
// When the last field is descending the key ends with one extra 0x01 byte.
// A descending string encoding is a byte prefix of the encoding of the same
// string extended by 0x00 (keyenc's documented corner), so without the
// trailing byte "" would sort before "\x00" at the end of a key. Keys are
// for comparison; do not decode them with keyenc.
func SortKey(dst []byte, doc bson.Raw, s SortSpec) []byte {
	for _, f := range s {
		vals, _ := IndexValues(doc, f.Field)
		best := vals[0]
		for _, v := range vals[1:] {
			c := Compare(v, best)
			if (!f.Desc && c < 0) || (f.Desc && c > 0) {
				best = v
			}
		}
		dst = appendKey(dst, best, f.Desc)
	}
	if len(s) > 0 && s[len(s)-1].Desc {
		dst = append(dst, 0x01)
	}
	return dst
}

func appendKey(dst []byte, v bson.RawValue, desc bool) []byte {
	enc := keyenc.Encode
	if desc {
		enc = keyenc.EncodeDesc
	}
	out, err := enc(dst, v)
	if err == nil {
		return out
	}
	if errors.Is(err, keyenc.ErrUnsupported) {
		if out, err = enc(dst, withoutDecimals(v)); err == nil {
			return out
		}
	}
	missingKey := keyenc.EncodeMissing(nil)
	if desc {
		missingKey[0] ^= 0xFF
	}
	return append(dst, missingKey...)
}

// withoutDecimals returns v with every Decimal128, however deeply nested,
// replaced by its nearest double.
func withoutDecimals(v bson.RawValue) bson.RawValue {
	switch v.Type {
	case bson.TypeDecimal128:
		n, ok := numberOf(v)
		if !ok {
			return v
		}
		return doubleValue(n.float())
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		var w builder
		w.begin()
		it := newIter(v.Value)
		for {
			e, ok := it.next()
			if !ok {
				break
			}
			w.elem(string(e.name), withoutDecimals(e.value()))
		}
		return bson.RawValue{Type: v.Type, Value: w.end()}
	case bson.TypeCodeWithScope:
		code, scope, ok := codeWScopeParts(v.Value)
		if !ok {
			return v
		}
		s := withoutDecimals(docValue(scope))
		out := binary32(nil, 0)
		out = binary32(out, uint32(len(code)+1))
		out = append(out, code...)
		out = append(out, 0)
		out = append(out, s.Value...)
		putUint32(out, uint32(len(out)))
		return bson.RawValue{Type: v.Type, Value: out}
	}
	return v
}

func binary32(dst []byte, v uint32) []byte {
	return append(dst, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func putUint32(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
}
