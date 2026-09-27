package docstore

import (
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MongoDB error codes that docstore errors carry.
const (
	CodeDuplicateKey              = 11000
	CodeCannotIndexParallelArrays = 171
	CodeGeoKeys                   = 16755 // MongoDB's "Can't extract geo keys" location code
)

// IDIndexName is the name MongoDB gives the implicit _id index; a
// duplicate _id reports it in the E11000 text.
const IDIndexName = "_id_"

// DupKeyError is a unique-index violation. Error returns MongoDB's text
// exactly, "E11000 duplicate key error collection: <ns> index: <name> dup
// key: { <field>: <value>, ... }", with no "ERR " prefix: the E11000 word
// is the error code of the RESP error line, so a handler replies
// resp.Err(err.Error()) and writeErrors carry code 11000 and the same
// text.
type DupKeyError struct {
	NS     string
	Index  string
	Fields []string        // index key fields in key order
	Values []bson.RawValue // the duplicate values; the zero value is a missing field (null)
}

func (e *DupKeyError) Error() string {
	var b strings.Builder
	b.WriteString("E11000 duplicate key error collection: ")
	b.WriteString(e.NS)
	b.WriteString(" index: ")
	b.WriteString(e.Index)
	b.WriteString(" dup key: { ")
	for i, f := range e.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(f)
		b.WriteString(": ")
		var v bson.RawValue
		if i < len(e.Values) {
			v = e.Values[i]
		}
		formatValue(&b, v)
	}
	b.WriteString(" }")
	return b.String()
}

// KeyValue returns the duplicate key as a document {field: value, ...},
// MongoDB's writeError keyValue. Missing values appear as null.
func (e *DupKeyError) KeyValue() bson.Raw {
	d := make(bson.D, 0, len(e.Fields))
	for i, f := range e.Fields {
		var v any
		if i < len(e.Values) && e.Values[i].Type != 0 {
			v = e.Values[i]
		}
		d = append(d, bson.E{Key: f, Value: v})
	}
	raw, err := bson.Marshal(d)
	if err != nil {
		return nil
	}
	return raw
}

func newDupKeyError(ns, index string, fields []string, vals []bson.RawValue) *DupKeyError {
	cp := make([]bson.RawValue, len(vals))
	for i, v := range vals {
		cp[i] = bson.RawValue{Type: v.Type, Value: append([]byte(nil), v.Value...)}
	}
	return &DupKeyError{NS: ns, Index: index, Fields: append([]string(nil), fields...), Values: cp}
}

// Code returns the MongoDB error code of err: 11000 for *DupKeyError, the
// code of a *doc.Error (catalog and docstore errors are *doc.Error), and 2
// (BadValue) for anything else.
func Code(err error) int {
	var dup *DupKeyError
	if errors.As(err, &dup) {
		return CodeDuplicateKey
	}
	return doc.Code(err)
}

// Message returns the errmsg text of err for writeErrors: the E11000 text,
// a *doc.Error's Msg (without "ERR "), or err.Error().
func Message(err error) string {
	var dup *DupKeyError
	if errors.As(err, &dup) {
		return dup.Error()
	}
	var de *doc.Error
	if errors.As(err, &de) {
		return de.Msg
	}
	return err.Error()
}

func errf(code int, msg string) error { return &doc.Error{Code: code, Msg: msg} }

// formatValue writes v the way MongoDB prints key values in E11000
// messages: strings quoted, numbers bare (doubles keep a fraction, 13.0),
// ObjectId('...'), new Date(ms), nested documents and arrays with spaces.
func formatValue(b *strings.Builder, v bson.RawValue) {
	switch v.Type {
	case 0, bson.TypeNull:
		b.WriteString("null")
	case bson.TypeUndefined:
		b.WriteString("undefined")
	case bson.TypeMinKey:
		b.WriteString("MinKey")
	case bson.TypeMaxKey:
		b.WriteString("MaxKey")
	case bson.TypeInt32:
		b.WriteString(strconv.FormatInt(int64(v.Int32()), 10))
	case bson.TypeInt64:
		b.WriteString(strconv.FormatInt(v.Int64(), 10))
	case bson.TypeDouble:
		b.WriteString(formatDouble(v.Double()))
	case bson.TypeDecimal128:
		b.WriteString("NumberDecimal(\"" + v.Decimal128().String() + "\")")
	case bson.TypeString:
		b.WriteString(strconv.Quote(v.StringValue()))
	case bson.TypeSymbol:
		b.WriteString(strconv.Quote(v.Symbol()))
	case bson.TypeBoolean:
		b.WriteString(strconv.FormatBool(v.Boolean()))
	case bson.TypeObjectID:
		oid := v.ObjectID()
		b.WriteString("ObjectId('" + hex.EncodeToString(oid[:]) + "')")
	case bson.TypeDateTime:
		b.WriteString("new Date(" + strconv.FormatInt(v.DateTime(), 10) + ")")
	case bson.TypeTimestamp:
		t, i := v.Timestamp()
		b.WriteString("Timestamp(" + strconv.FormatUint(uint64(t), 10) + ", " + strconv.FormatUint(uint64(i), 10) + ")")
	case bson.TypeBinary:
		sub, data := v.Binary()
		b.WriteString("BinData(" + strconv.Itoa(int(sub)) + ", " + strings.ToUpper(hex.EncodeToString(data)) + ")")
	case bson.TypeRegex:
		p, o := v.Regex()
		b.WriteString("/" + p + "/" + o)
	case bson.TypeJavaScript:
		b.WriteString("function() { " + v.JavaScript() + " }")
	case bson.TypeCodeWithScope:
		code, _ := v.CodeWithScope()
		b.WriteString("function() { " + code + " }")
	case bson.TypeDBPointer:
		ns, oid := v.DBPointer()
		b.WriteString("DBRef('" + ns + "', '" + hex.EncodeToString(oid[:]) + "')")
	case bson.TypeEmbeddedDocument:
		elems, err := bson.Raw(v.Value).Elements()
		if err != nil || len(elems) == 0 {
			b.WriteString("{}")
			return
		}
		b.WriteString("{ ")
		for i, e := range elems {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(e.Key())
			b.WriteString(": ")
			formatValue(b, e.Value())
		}
		b.WriteString(" }")
	case bson.TypeArray:
		vals, err := bson.RawArray(v.Value).Values()
		if err != nil || len(vals) == 0 {
			b.WriteString("[]")
			return
		}
		b.WriteString("[ ")
		for i, e := range vals {
			if i > 0 {
				b.WriteString(", ")
			}
			formatValue(b, e)
		}
		b.WriteString(" ]")
	default:
		b.WriteString("?")
	}
}

func formatDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan.0"
	case math.IsInf(f, 1):
		return "inf.0"
	case math.IsInf(f, -1):
		return "-inf.0"
	case f == math.Trunc(f) && math.Abs(f) < 1e16:
		return strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
