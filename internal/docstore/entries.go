package docstore

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/keyenc"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// MaxKeysPerIndex bounds the entries one document may put in one index.
// Only a compound index whose fields share an array produces more than one
// entry per array element (the cross product of the fields' values), and
// a large array of subdocuments could otherwise make millions.
const MaxKeysPerIndex = 100_000

// entry is one key a document puts in one index.
type entry struct {
	cf   store.CF
	key  []byte          // full key
	val  []byte          // empty (non-unique btree), ks(_id) (unique btree), geo value
	uniq []byte          // unique btree: key without the idx_id prefix, the LockUniq key
	vals []bson.RawValue // unique btree: field values for the E11000 text (alias the document)
}

// cfOf returns the column family that holds ix's entries.
func cfOf(ix *catalog.Index) store.CF {
	switch ix.Kind {
	case catalog.TwoDSphere:
		return store.CFGeo
	case catalog.Columnar:
		return store.CFCol
	}
	return store.CFIdx
}

// indexEntries returns the entries d (whose _id encodes to ksID) puts in
// ix, and whether some btree field produced several values or crossed an
// array (multikey). ksID may be nil when only the unique lock keys are
// needed.
func indexEntries(ix *catalog.Index, d bson.Raw, ksID []byte) ([]entry, bool, error) {
	switch ix.Kind {
	case catalog.Btree:
		return btreeEntries(ix, d, ksID)
	case catalog.TwoDSphere:
		ents, err := geoEntries(ix, d, ksID)
		return ents, false, err
	}
	return nil, false, catalog.ErrColumnar
}

// encodeErr turns a keyenc failure into a user-facing error.
func encodeErr(err error) error {
	if errors.Is(err, keyenc.ErrUnsupported) {
		return errf(doc.CodeBadValue, keyenc.ErrUnsupported.Error())
	}
	return errf(doc.CodeBadValue, "cannot index a malformed BSON value")
}

// btreeEntries implements the btree rules of architecture.md section 3:
// one value set per field from doc.IndexValues (array elements, [] as
// undefined, missing as null), a sparse index skips the document when
// every field is missing, at most one array per compound key (MongoDB's
// "cannot index parallel arrays"), and one entry per combination of the
// fields' values. A unique entry's key is idx_id|ks(f1)..ks(fk) with value
// ks(_id); a non-unique entry appends ks(_id) to the key.
func btreeEntries(ix *catalog.Index, d bson.Raw, ksID []byte) ([]entry, bool, error) {
	n := len(ix.Keys)
	vals := make([][]bson.RawValue, n)
	encs := make([][][]byte, n)
	multikey, allMissing := false, true
	total := 1
	for i, k := range ix.Keys {
		vs, mk := doc.IndexValues(d, k.Field)
		multikey = multikey || mk || len(vs) > 1
		vals[i] = vs
		encs[i] = make([][]byte, len(vs))
		for j, v := range vs {
			if v.Type != 0 {
				allMissing = false
			}
			var enc []byte
			var err error
			if k.Desc {
				enc, err = keyenc.EncodeDesc(nil, v)
			} else {
				enc, err = keyenc.Encode(nil, v)
			}
			if err != nil {
				return nil, false, encodeErr(err)
			}
			encs[i][j] = enc
		}
		total *= len(vs)
		if total > MaxKeysPerIndex {
			return nil, false, errf(doc.CodeBadValue, fmt.Sprintf(
				"index %s would hold more than %d keys for one document", ix.Name, MaxKeysPerIndex))
		}
	}
	if ix.Sparse && allMissing {
		return nil, multikey, nil
	}
	if n > 1 {
		if err := checkParallel(d, ix); err != nil {
			return nil, false, err
		}
	}
	cf := cfOf(ix)
	ents := make([]entry, 0, total)
	pick := make([]int, n)
	for {
		key := layout.IdxKey(nil, ix.ID)
		var tuple []bson.RawValue
		if ix.Unique {
			tuple = make([]bson.RawValue, n)
		}
		for i := range n {
			key = append(key, encs[i][pick[i]]...)
			if ix.Unique {
				tuple[i] = vals[i][pick[i]]
			}
		}
		e := entry{cf: cf}
		if ix.Unique {
			e.key, e.val, e.uniq, e.vals = key, ksID, key[4:], tuple
		} else {
			e.key = append(key, ksID...)
		}
		ents = append(ents, e)
		// Advance the odometer over the fields' value lists.
		i := n - 1
		for ; i >= 0; i-- {
			pick[i]++
			if pick[i] < len(encs[i]) {
				break
			}
			pick[i] = 0
		}
		if i < 0 {
			break
		}
	}
	return ents, multikey, nil
}

// arrayHit is the first array one field's path reaches.
type arrayHit struct {
	field int      // index key position
	path  string   // dotted location of the array, relative to the walk root
	name  string   // the array's own field name
	rest  []string // path components after the array
}

// checkParallel enforces MongoDB's compound multikey rule: the fields of
// one compound key may cross arrays only at one shared location, checked
// again inside each element of that array. Two fields that reach arrays at
// different locations fail with "cannot index parallel arrays [b] [a]",
// the later field's array first.
func checkParallel(d bson.Raw, ix *catalog.Index) error {
	paths := make([][]string, len(ix.Keys))
	for i, k := range ix.Keys {
		paths[i] = splitPath(k.Field)
	}
	return parallel(d, paths, 0)
}

func parallel(d bson.Raw, paths [][]string, depth int) error {
	if depth > doc.MaxDepth {
		return nil
	}
	var hits []arrayHit
	for i, p := range paths {
		if h, ok := firstArray(d, p); ok {
			h.field = i
			hits = append(hits, h)
		}
	}
	if len(hits) == 0 {
		return nil
	}
	first := hits[0]
	var shared [][]string
	for _, h := range hits {
		if h.path != first.path {
			return errf(CodeCannotIndexParallelArrays, "cannot index parallel arrays ["+h.name+"] ["+first.name+"]")
		}
		if len(h.rest) > 0 {
			shared = append(shared, h.rest)
		}
	}
	if len(shared) < 2 {
		return nil
	}
	arr, ok := lookupPath(d, splitPath(first.path))
	if !ok || arr.Type != bson.TypeArray {
		return nil
	}
	elems, err := bson.RawArray(arr.Value).Values()
	if err != nil {
		return nil
	}
	for _, e := range elems {
		if e.Type != bson.TypeEmbeddedDocument {
			continue
		}
		if err := parallel(bson.Raw(e.Value), shared, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// firstArray walks p through documents from d and reports the first array
// it meets, including an array at the end of the path. A numeric component
// right after an array selects that element positionally and does not
// count as crossing the array.
func firstArray(d bson.Raw, p []string) (arrayHit, bool) {
	cur := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d}
	loc := ""
	for j := 0; j < len(p); j++ {
		if cur.Type != bson.TypeEmbeddedDocument {
			return arrayHit{}, false
		}
		v, err := bson.Raw(cur.Value).LookupErr(p[j])
		if err != nil {
			return arrayHit{}, false
		}
		if loc != "" {
			loc += "."
		}
		loc += p[j]
		cur = v
		for cur.Type == bson.TypeArray && j+1 < len(p) && isIndex(p[j+1]) {
			el, err := bson.RawArray(cur.Value).IndexErr(uint(atoi(p[j+1])))
			if err != nil {
				return arrayHit{}, false
			}
			j++
			loc += "." + p[j]
			cur = el
		}
		if cur.Type == bson.TypeArray {
			return arrayHit{path: loc, name: p[j], rest: p[j+1:]}, true
		}
	}
	return arrayHit{}, false
}

// lookupPath follows components through documents and positional array
// indexes.
func lookupPath(d bson.Raw, p []string) (bson.RawValue, bool) {
	cur := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d}
	for _, c := range p {
		switch cur.Type {
		case bson.TypeEmbeddedDocument:
			v, err := bson.Raw(cur.Value).LookupErr(c)
			if err != nil {
				return bson.RawValue{}, false
			}
			cur = v
		case bson.TypeArray:
			if !isIndex(c) {
				return bson.RawValue{}, false
			}
			v, err := bson.RawArray(cur.Value).IndexErr(uint(atoi(c)))
			if err != nil {
				return bson.RawValue{}, false
			}
			cur = v
		default:
			return bson.RawValue{}, false
		}
	}
	return cur, true
}

func splitPath(p string) []string {
	var out []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '.' {
			out = append(out, p[start:i])
			start = i + 1
		}
	}
	return append(out, p[start:])
}

// isIndex matches doc's array-index components: ASCII digits, no leading
// zero except "0", at most 9 digits.
func isIndex(s string) bool {
	if s == "" || len(s) > 9 || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func geoErr(reason string) error { return errf(CodeGeoKeys, "Can't extract geo keys: "+reason) }

// GeoField returns the value a 2dsphere index on path reads from d. The
// path walks documents and positional array indexes only; a path that
// needs to traverse an array of subdocuments fails, because v1 indexes one
// geometry per document. found is false for a missing field.
func GeoField(d bson.Raw, path string) (v bson.RawValue, found bool, err error) {
	cur := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: d}
	for _, c := range splitPath(path) {
		switch cur.Type {
		case bson.TypeEmbeddedDocument:
			v, err := bson.Raw(cur.Value).LookupErr(c)
			if err != nil {
				return bson.RawValue{}, false, nil
			}
			cur = v
		case bson.TypeArray:
			if !isIndex(c) {
				return bson.RawValue{}, false, geoErr("field '" + path + "' passes through an array; arrays of geometries are not supported in v1")
			}
			v, err := bson.RawArray(cur.Value).IndexErr(uint(atoi(c)))
			if err != nil {
				return bson.RawValue{}, false, nil
			}
			cur = v
		default:
			return bson.RawValue{}, false, nil
		}
	}
	return cur, true, nil
}

// geoEntries returns a document's 2dsphere entries: none for a missing,
// null, undefined or empty value (2dsphere is always sparse), one leaf
// cell with the position in the value for a Point, the covering cells
// between the index's frozen levels for everything else.
func geoEntries(ix *catalog.Index, d bson.Raw, ksID []byte) ([]entry, error) {
	v, found, err := GeoField(d, ix.Keys[0].Field)
	if err != nil {
		return nil, err
	}
	if !found || geo.Absent(v) {
		return nil, nil
	}
	if v.Type == bson.TypeArray {
		if first, err := bson.RawArray(v.Value).IndexErr(0); err == nil && !first.IsNumber() {
			return nil, geoErr("arrays of geometries are not supported in v1")
		}
	}
	g, err := geo.Parse(v, geo.ParseOpts{AllowLegacyPair: true})
	if err != nil {
		return nil, geoErr(err.Error())
	}
	if g.StrictWinding() {
		return nil, geoErr("the strict-winding crs is only allowed in queries")
	}
	cells := ix.S2.Entries(g)
	ents := make([]entry, len(cells))
	for i, c := range cells {
		ents[i] = entry{
			cf:  store.CFGeo,
			key: layout.GeoKey(nil, ix.ID, uint64(c.Cell), ksID),
			val: c.AppendValue(nil),
		}
	}
	return ents, nil
}
