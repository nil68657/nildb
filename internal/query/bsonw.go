package query

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// BSON reading and writing helpers. internal/doc keeps its own versions
// unexported, and these stay small: an element iterator that aliases the
// document, a builder for new documents, and constructors for the values
// operators produce.

// elem is one element of a BSON document or array. Every slice aliases the
// document it came from.
type elem struct {
	name []byte
	typ  bson.Type
	val  []byte
	raw  []byte // type byte, name, NUL, value
}

func (e elem) value() bson.RawValue { return bson.RawValue{Type: e.typ, Value: e.val} }

// elems walks the elements of one BSON document or array.
type elems struct {
	b   []byte
	bad bool
}

func iterDoc(d []byte) elems {
	if len(d) < 5 {
		return elems{bad: true}
	}
	n := int(int32(binary.LittleEndian.Uint32(d)))
	if n != len(d) || d[n-1] != 0 {
		return elems{bad: true}
	}
	return elems{b: d[4 : n-1]}
}

func (it *elems) next() (elem, bool) {
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
	size, ok := valueSize(t, rest)
	if !ok {
		it.bad = true
		return elem{}, false
	}
	e := elem{name: it.b[1 : 1+nl], typ: t, val: rest[:size], raw: it.b[:2+nl+size]}
	it.b = rest[size:]
	return e, true
}

// valueSize returns the byte size of the BSON value of type t that starts b.
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
		if len(b) < 5 {
			return 0, false
		}
		n := int(int32(binary.LittleEndian.Uint32(b)))
		return n + 4, n >= 1 && n+4 <= len(b)
	case bson.TypeDBPointer:
		if len(b) < 5 {
			return 0, false
		}
		n := int(int32(binary.LittleEndian.Uint32(b)))
		return n + 4 + 12, n >= 1 && n+4+12 <= len(b)
	case bson.TypeEmbeddedDocument, bson.TypeArray, bson.TypeCodeWithScope:
		if len(b) < 4 {
			return 0, false
		}
		n := int(int32(binary.LittleEndian.Uint32(b)))
		return n, n >= 5 && n <= len(b)
	case bson.TypeBinary:
		if len(b) < 5 {
			return 0, false
		}
		n := int(int32(binary.LittleEndian.Uint32(b)))
		return n + 5, n >= 0 && n+5 <= len(b)
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

// lookup returns the first element called name in the document d.
func lookup(d []byte, name string) (bson.RawValue, bool) {
	it := iterDoc(d)
	for {
		e, ok := it.next()
		if !ok {
			return bson.RawValue{}, false
		}
		if string(e.name) == name {
			return e.value(), true
		}
	}
}

// arrayValues returns the elements of a BSON array (or document) in order.
func arrayValues(a []byte) []bson.RawValue {
	var out []bson.RawValue
	it := iterDoc(a)
	for {
		e, ok := it.next()
		if !ok {
			return out
		}
		out = append(out, e.value())
	}
}

// builder appends BSON documents; nested documents and arrays open with
// beginElem and close with end.
type builder struct {
	b      []byte
	starts []int
}

func (w *builder) begin() {
	w.starts = append(w.starts, len(w.b))
	w.b = append(w.b, 0, 0, 0, 0)
}

// end closes the innermost open document and returns its bytes, which
// alias the builder until it grows again.
func (w *builder) end() []byte {
	s := w.starts[len(w.starts)-1]
	w.starts = w.starts[:len(w.starts)-1]
	w.b = append(w.b, 0)
	binary.LittleEndian.PutUint32(w.b[s:], uint32(len(w.b)-s))
	return w.b[s:]
}

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

// oneField returns the document {name: v}.
func oneField(name string, v bson.RawValue) bson.Raw {
	var w builder
	w.begin()
	w.elem(name, v)
	return w.end()
}

func i32(v int32) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt32, Value: binary.LittleEndian.AppendUint32(nil, uint32(v))}
}

func i64(v int64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeInt64, Value: binary.LittleEndian.AppendUint64(nil, uint64(v))}
}

func f64(v float64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDouble, Value: binary.LittleEndian.AppendUint64(nil, math.Float64bits(v))}
}

// intValue returns n as an int32 when it fits and as an int64 otherwise,
// the type MongoDB gives counts.
func intValue(n int64) bson.RawValue {
	if n >= math.MinInt32 && n <= math.MaxInt32 {
		return i32(int32(n))
	}
	return i64(n)
}

func strV(s string) bson.RawValue {
	v := binary.LittleEndian.AppendUint32(nil, uint32(len(s)+1))
	v = append(v, s...)
	return bson.RawValue{Type: bson.TypeString, Value: append(v, 0)}
}

func boolV(b bool) bson.RawValue {
	if b {
		return bson.RawValue{Type: bson.TypeBoolean, Value: []byte{1}}
	}
	return bson.RawValue{Type: bson.TypeBoolean, Value: []byte{0}}
}

func dateV(ms int64) bson.RawValue {
	return bson.RawValue{Type: bson.TypeDateTime, Value: binary.LittleEndian.AppendUint64(nil, uint64(ms))}
}

var nullV = bson.RawValue{Type: bson.TypeNull}

func docV(d []byte) bson.RawValue { return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d} }

// arrV returns a BSON array of vals keyed "0", "1", ...
func arrV(vals []bson.RawValue) bson.RawValue {
	var w builder
	w.begin()
	var key []byte
	for i, v := range vals {
		key = strconv.AppendInt(key[:0], int64(i), 10)
		w.elem(string(key), v)
	}
	return bson.RawValue{Type: bson.TypeArray, Value: w.end()}
}

// strOf returns the contents of a String or Symbol value.
func strOf(v bson.RawValue) (string, bool) {
	if v.Type != bson.TypeString && v.Type != bson.TypeSymbol {
		return "", false
	}
	if len(v.Value) < 5 {
		return "", false
	}
	n := int(int32(binary.LittleEndian.Uint32(v.Value)))
	if n < 1 || n+4 != len(v.Value) {
		return "", false
	}
	return string(v.Value[4 : 4+n-1]), true
}

// relaxed renders one value as relaxed Extended JSON for EXPLAIN text.
func relaxed(v bson.RawValue) string {
	out, err := bson.MarshalExtJSON(oneField("v", v), false, false)
	if err != nil || len(out) < 6 {
		return "?"
	}
	return string(out[5 : len(out)-1]) // {"v":<value>}
}

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// plusByte returns b followed by c, in a new slice.
func plusByte(b []byte, c byte) []byte {
	out := make([]byte, len(b)+1)
	copy(out, b)
	out[len(b)] = c
	return out
}

func concat(a, b []byte) []byte {
	out := make([]byte, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}
