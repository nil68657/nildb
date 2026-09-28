package query

import (
	"bytes"
	"slices"
	"strings"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Count returns how many documents of coll match filter, for DOC.COUNT and
// NIL.COUNT. An empty filter reads the n/<coll_id> counter that docstore
// updates in every write batch, through the options' reader, so a count
// under a lease or inside EXEC sees what that reader sees. $near is not
// allowed here, as in MongoDB's count.
func (e *Engine) Count(coll *catalog.Collection, filter bson.Raw, o Options) (int64, error) {
	m, err := doc.Compile(filter, geoHooks{})
	if err != nil {
		return 0, err
	}
	if coll == nil {
		return 0, nil
	}
	if m.MatchAll() {
		r := o.Reader
		if r == nil {
			r = e.st
		}
		n, err := catalog.CountAt(r, coll)
		if err != nil {
			return 0, (&Exec{}).readErr(err)
		}
		return n, nil
	}
	p, err := e.PlanFind(coll, Find{Filter: filter}, o)
	if err != nil {
		return 0, err
	}
	run, err := p.Start()
	if err != nil {
		return 0, err
	}
	defer run.Close()
	run.x.startBatch()
	defer run.x.flush()
	var n int64
	for {
		_, ok, err := run.root.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			return n, nil
		}
		n++
	}
}

// Distinct returns the distinct values of field across the documents of
// coll that match filter, sorted in BSON order, for DOC.DISTINCT and
// NIL.DISTINCT. As in MongoDB, the path walks arrays of documents and an
// array at the end of the path contributes its elements; 1 and 1.0 are one
// value. The value set is charged to the run's Accountant.
func (e *Engine) Distinct(coll *catalog.Collection, field string, filter bson.Raw, o Options) ([]bson.RawValue, error) {
	if field == "" {
		return nil, exprErr(40352, "FieldPath cannot be constructed with empty string")
	}
	parts := strings.Split(field, ".")
	for _, c := range parts {
		if c == "" || c[0] == '$' {
			return nil, exprErr(doc.CodeBadValue, "invalid distinct field '%s'", field)
		}
	}
	if _, err := doc.Compile(filter, geoHooks{}); err != nil {
		return nil, err
	}
	if coll == nil {
		return nil, nil
	}
	p, err := e.PlanFind(coll, Find{Filter: filter}, o)
	if err != nil {
		return nil, err
	}
	run, err := p.Start()
	if err != nil {
		return nil, err
	}
	defer run.Close()
	run.x.startBatch()
	defer run.x.flush()
	type value struct {
		key []byte
		v   bson.RawValue
	}
	seen := make(map[string]struct{})
	var vals []value
	var ferr error
	add := func(v bson.RawValue) {
		if ferr != nil {
			return
		}
		key, err := keyenc.Encode(nil, v)
		if err != nil {
			key = append([]byte{0xFE, byte(v.Type)}, v.Value...)
		}
		if _, dup := seen[string(key)]; dup {
			return
		}
		seen[string(key)] = struct{}{}
		vals = append(vals, value{key, bson.RawValue{Type: v.Type, Value: clone(v.Value)}})
		ferr = run.x.mem.Grow("distinct", int64(2*len(key)+len(v.Value))+64)
	}
	for {
		r, ok, err := run.root.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		distinctWalk(r.Doc, parts, add)
		if ferr != nil {
			return nil, ferr
		}
	}
	slices.SortFunc(vals, func(a, b value) int { return bytes.Compare(a.key, b.key) })
	out := make([]bson.RawValue, len(vals))
	for i, v := range vals {
		out[i] = v.v
	}
	return out, nil
}

// distinctWalk calls fn with every value path reaches in d under the
// matcher's path rules: arrays of documents are walked element by element,
// a numeric component also selects an array element, and an array at the
// end of the path yields its elements.
func distinctWalk(d []byte, parts []string, fn func(bson.RawValue)) {
	v, ok := lookup(d, parts[0])
	if !ok {
		return
	}
	distinctValue(v, parts[1:], fn)
}

func distinctValue(v bson.RawValue, rest []string, fn func(bson.RawValue)) {
	if len(rest) == 0 {
		if v.Type == bson.TypeArray {
			for _, el := range arrayValues(v.Value) {
				fn(el)
			}
			return
		}
		fn(v)
		return
	}
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		distinctWalk(v.Value, rest, fn)
	case bson.TypeArray:
		idx, positional := arrayIndex(rest[0])
		for i, el := range arrayValues(v.Value) {
			if positional && i == idx {
				distinctValue(el, rest[1:], fn)
			}
			if el.Type != bson.TypeEmbeddedDocument {
				continue
			}
			if positional {
				if sub, ok := lookup(el.Value, rest[0]); ok {
					distinctValue(sub, rest[1:], fn)
				}
				continue
			}
			distinctWalk(el.Value, rest, fn)
		}
	}
}

// arrayIndex parses a path component as an array index, as doc does: ASCII
// digits with no leading zero, at most 9 of them.
func arrayIndex(s string) (int, bool) {
	if s == "" || len(s) > 9 || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}
