package doc

import (
	"bytes"
	"encoding/json"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ParseEJSON parses one MongoDB Extended JSON v2 document, canonical or
// relaxed, into raw BSON. Field order and repeated names are kept as
// written. Input that is not a single JSON object fails with
// "ERR invalid Extended JSON: ...".
func ParseEJSON(s []byte) (bson.Raw, error) {
	if !json.Valid(s) {
		var x json.RawMessage
		err := json.Unmarshal(s, &x)
		msg := "malformed JSON"
		if err != nil {
			msg = err.Error()
		}
		return nil, badValue("invalid Extended JSON: %s", msg)
	}
	t := bytes.TrimSpace(s)
	if len(t) == 0 || t[0] != '{' {
		return nil, badValue("invalid Extended JSON: expected a document")
	}
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON(t, false, &raw); err != nil {
		return nil, badValue("invalid Extended JSON: %s", err.Error())
	}
	return raw, nil
}

// FormatEJSON renders a document as canonical Extended JSON v2 with no
// insignificant whitespace, or nil when doc is not valid BSON (stored
// documents always are).
func FormatEJSON(doc bson.Raw) []byte {
	out, err := bson.MarshalExtJSON(doc, true, false)
	if err != nil {
		return nil
	}
	return out
}

// FormatEJSONValue renders one value, such as an _id for a reply, as
// canonical Extended JSON v2, or nil when v is not a valid BSON value.
func FormatEJSONValue(v bson.RawValue) []byte {
	out, err := appendEJSONValue(nil, v, true)
	if err != nil {
		return nil
	}
	return out
}

// appendEJSONValue marshals v inside a one-field document and cuts the
// value out of the output.
func appendEJSONValue(dst []byte, v bson.RawValue, canonical bool) ([]byte, error) {
	d := buildDoc([]string{""}, []bson.RawValue{v})
	out, err := bson.MarshalExtJSON(bson.Raw(d), canonical, false)
	if err != nil {
		return dst, err
	}
	// out is {"":<value>}
	return append(dst, out[4:len(out)-1]...), nil
}
