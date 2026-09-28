package doc

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// MaxDocSize is the largest document NilDB stores, MongoDB's 16 MiB.
const MaxDocSize = 16 * 1024 * 1024

// MaxDepth is the deepest nesting a stored document may have. The top-level
// document is level 1 and every embedded document or array adds one.
const MaxDepth = 100

// missing is the zero RawValue, what a path lookup returns for an absent
// field. keyenc encodes it like Null.
var missing bson.RawValue

// elem is one element of a BSON document. All slices alias the document.
type elem struct {
	name []byte
	typ  bson.Type
	val  []byte
	raw  []byte // type byte, name, NUL, value
}

func (e elem) value() bson.RawValue { return bson.RawValue{Type: e.typ, Value: e.val} }

// iter walks the elements of one BSON document.
type iter struct {
	b   []byte // remaining element bytes, without the final NUL
	bad bool
}

// newIter checks the document header and returns an iterator over its
// elements. A malformed header gives an iterator that reports an error at
// the first next call.
func newIter(doc []byte) iter {
	if len(doc) < 5 {
		return iter{bad: true}
	}
	n := int32(binary.LittleEndian.Uint32(doc))
	if int(n) != len(doc) || doc[n-1] != 0 {
		return iter{bad: true}
	}
	return iter{b: doc[4 : n-1]}
}

// next returns the next element. ok is false at the end or on malformed
// bytes; err reports which.
func (it *iter) next() (e elem, ok bool) {
	if it.bad || len(it.b) == 0 {
		return elem{}, false
	}
	t := bson.Type(it.b[0])
	nl := bytes.IndexByte(it.b[1:], 0)
	if t == 0 || nl < 0 {
		it.bad = true
		return elem{}, false
	}
	rest := it.b[2+nl:]
	size, good := valueSize(t, rest)
	if !good {
		it.bad = true
		return elem{}, false
	}
	e = elem{name: it.b[1 : 1+nl], typ: t, val: rest[:size], raw: it.b[:2+nl+size]}
	it.b = rest[size:]
	return e, true
}

func (it iter) err() error {
	if it.bad {
		return errMalformed
	}
	return nil
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
		s, ok := cstr32(b)
		return len(s) + 5, ok
	case bson.TypeDBPointer:
		s, ok := cstr32(b)
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

// cstr32 parses a BSON string (int32 length including the NUL, bytes, NUL)
// at the start of b and returns its contents.
func cstr32(b []byte) ([]byte, bool) {
	if len(b) < 5 {
		return nil, false
	}
	n := int32(binary.LittleEndian.Uint32(b))
	if n < 1 || int(n) > len(b)-4 || b[4+n-1] != 0 {
		return nil, false
	}
	return b[4 : 4+n-1], true
}

// lookupField returns the value of the first element called name in doc.
func lookupField(doc []byte, name string) (bson.RawValue, bool) {
	it := newIter(doc)
	for {
		e, ok := it.next()
		if !ok {
			return missing, false
		}
		if string(e.name) == name {
			return e.value(), true
		}
	}
}

// arrayValues returns the elements of a BSON array in order.
func arrayValues(arr []byte) []bson.RawValue {
	it := newIter(arr)
	var out []bson.RawValue
	for {
		e, ok := it.next()
		if !ok {
			return out
		}
		out = append(out, e.value())
	}
}

// arrayLen counts the elements of a BSON array.
func arrayLen(arr []byte) int {
	it := newIter(arr)
	n := 0
	for {
		if _, ok := it.next(); !ok {
			return n
		}
		n++
	}
}

// docEmpty reports whether a document has no elements.
func docEmpty(d []byte) bool { return len(d) <= 5 }

// builder appends BSON documents. Nested documents are opened with begin
// and closed with end.
type builder struct {
	b      []byte
	starts []int
}

func (w *builder) begin() {
	w.starts = append(w.starts, len(w.b))
	w.b = append(w.b, 0, 0, 0, 0)
}

func (w *builder) end() []byte {
	s := w.starts[len(w.starts)-1]
	w.starts = w.starts[:len(w.starts)-1]
	w.b = append(w.b, 0)
	binary.LittleEndian.PutUint32(w.b[s:], uint32(len(w.b)-s))
	return w.b[s:]
}

// beginElem opens an embedded document or array element called name.
func (w *builder) beginElem(t bson.Type, name string) {
	w.b = append(w.b, byte(t))
	w.b = append(w.b, name...)
	w.b = append(w.b, 0)
	w.begin()
}

func (w *builder) elem(name string, v bson.RawValue) {
	w.b = append(w.b, byte(v.Type))
	w.b = append(w.b, name...)
	w.b = append(w.b, 0)
	w.b = append(w.b, v.Value...)
}

func (w *builder) rawElem(raw []byte) { w.b = append(w.b, raw...) }

// buildDoc returns a fresh document holding the given names and values.
func buildDoc(names []string, vals []bson.RawValue) []byte {
	var w builder
	w.begin()
	for i, n := range names {
		w.elem(n, vals[i])
	}
	return w.end()
}

// buildArray returns a fresh BSON array of vals keyed "0", "1", ...
func buildArray(vals []bson.RawValue) []byte {
	var w builder
	w.begin()
	var key []byte
	for i, v := range vals {
		key = strconv.AppendInt(key[:0], int64(i), 10)
		w.elem(string(key), v)
	}
	return w.end()
}

func itoa(i int) string { return strconv.Itoa(i) }

func arrayValue(vals []bson.RawValue) bson.RawValue {
	return bson.RawValue{Type: bson.TypeArray, Value: buildArray(vals)}
}

func docValue(d []byte) bson.RawValue {
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d}
}

func int32Value(i int32) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt32, Value: binary.LittleEndian.AppendUint32(nil, uint32(i))}
}

func int64Value(i int64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt64, Value: binary.LittleEndian.AppendUint64(nil, uint64(i))}
}

func doubleValue(f float64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDouble, Value: binary.LittleEndian.AppendUint64(nil, math.Float64bits(f))}
}

func stringValue(s string) bson.RawValue {
	v := binary.LittleEndian.AppendUint32(nil, uint32(len(s)+1))
	v = append(v, s...)
	return bson.RawValue{Type: bson.TypeString, Value: append(v, 0)}
}

var nullValue = bson.RawValue{Type: bson.TypeNull}

var undefinedValue = bson.RawValue{Type: bson.TypeUndefined}

// strValue returns the bytes of a String or Symbol value.
func strValue(v bson.RawValue) ([]byte, bool) {
	if v.Type != bson.TypeString && v.Type != bson.TypeSymbol {
		return nil, false
	}
	s, ok := cstr32(v.Value)
	return s, ok && len(s)+5 == len(v.Value)
}

// regexParts returns the pattern and options of a Regex value.
func regexParts(v bson.RawValue) (pattern, options string, ok bool) {
	if v.Type != bson.TypeRegex {
		return "", "", false
	}
	p := bytes.IndexByte(v.Value, 0)
	if p < 0 {
		return "", "", false
	}
	o := bytes.IndexByte(v.Value[p+1:], 0)
	if o < 0 {
		return "", "", false
	}
	return string(v.Value[:p]), string(v.Value[p+1 : p+1+o]), true
}

func regexValue(pattern, options string) bson.RawValue {
	v := append([]byte(pattern), 0)
	v = append(v, options...)
	return bson.RawValue{Type: bson.TypeRegex, Value: append(v, 0)}
}

// isNumberType reports whether t is one of the four numeric BSON types.
func isNumberType(t bson.Type) bool {
	switch t {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return true
	}
	return false
}

// truthy is MongoDB's BSONElement::trueValue: false for false, zero, null,
// undefined and a missing value; true for everything else.
func truthy(v bson.RawValue) bool {
	switch v.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return false
	case bson.TypeBoolean:
		return len(v.Value) == 1 && v.Value[0] != 0
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		n, ok := numberOf(v)
		return !ok || !n.isZero()
	}
	return true
}

// typeName returns MongoDB's name for a BSON type, as used in $type aliases
// and error messages.
func typeName(t bson.Type) string {
	switch t {
	case 0:
		return "missing"
	case bson.TypeDouble:
		return "double"
	case bson.TypeString:
		return "string"
	case bson.TypeEmbeddedDocument:
		return "object"
	case bson.TypeArray:
		return "array"
	case bson.TypeBinary:
		return "binData"
	case bson.TypeUndefined:
		return "undefined"
	case bson.TypeObjectID:
		return "objectId"
	case bson.TypeBoolean:
		return "bool"
	case bson.TypeDateTime:
		return "date"
	case bson.TypeNull:
		return "null"
	case bson.TypeRegex:
		return "regex"
	case bson.TypeDBPointer:
		return "dbPointer"
	case bson.TypeJavaScript:
		return "javascript"
	case bson.TypeSymbol:
		return "symbol"
	case bson.TypeCodeWithScope:
		return "javascriptWithScope"
	case bson.TypeInt32:
		return "int"
	case bson.TypeTimestamp:
		return "timestamp"
	case bson.TypeInt64:
		return "long"
	case bson.TypeDecimal128:
		return "decimal"
	case bson.TypeMinKey:
		return "minKey"
	case bson.TypeMaxKey:
		return "maxKey"
	}
	return "unknown"
}

// showValue renders v in relaxed Extended JSON for error messages.
func showValue(v bson.RawValue) string {
	if v.Type == 0 {
		return "missing"
	}
	b, err := appendEJSONValue(nil, v, false)
	if err != nil {
		return "<" + typeName(v.Type) + ">"
	}
	return string(b)
}

// showField renders {name: value} for error messages.
func showField(name string, v bson.RawValue) string {
	return "{" + name + ": " + showValue(v) + "}"
}
