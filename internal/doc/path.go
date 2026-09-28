package doc

import (
	"strconv"
	"strings"

	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Path semantics shared by the matcher, sort keys and index keys. A dotted
// path such as "a.b.c" walks a document like this:
//
//   - in a document, the next component names a field; an absent field
//     yields a missing value;
//   - a scalar with components left yields a missing value;
//   - in an array with components left, every element that is a document
//     is walked with the same components (MongoDB's implicit array
//     traversal); other elements are skipped, and nested arrays are not
//     entered;
//   - when the next component is a non-negative integer i, the array's
//     element i is also walked positionally with the components after i
//     (an index past the end yields a missing value), and a document
//     element contributes only when it has a field literally named i;
//   - at the end of the path the value is yielded; with leaf expansion an
//     array also yields each of its elements.

// splitPath splits a dotted path into its components.
func splitPath(p string) []string { return strings.Split(p, ".") }

// arrayIndex parses a path component as an array index: ASCII digits with
// no leading zero (except "0" itself).
func arrayIndex(s string) (int, bool) {
	if s == "" || len(s) > 9 || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// walkDoc visits every value path reaches in doc. fn returns true to stop
// the walk, and walkDoc returns true when it was stopped.
func walkDoc(doc []byte, path []string, expand bool, fn func(bson.RawValue) bool) bool {
	v, ok := lookupField(doc, path[0])
	if !ok {
		return fn(missing)
	}
	return walkValue(v, path[1:], expand, fn)
}

func walkValue(v bson.RawValue, rest []string, expand bool, fn func(bson.RawValue) bool) bool {
	if len(rest) == 0 {
		if fn(v) {
			return true
		}
		if expand && v.Type == bson.TypeArray {
			it := newIter(v.Value)
			for {
				e, ok := it.next()
				if !ok {
					return false
				}
				if fn(e.value()) {
					return true
				}
			}
		}
		return false
	}
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		return walkDoc(v.Value, rest, expand, fn)
	case bson.TypeArray:
		idx, positional := arrayIndex(rest[0])
		found := false
		it := newIter(v.Value)
		for i := 0; ; i++ {
			e, ok := it.next()
			if !ok {
				break
			}
			if positional && i == idx {
				found = true
				if walkValue(e.value(), rest[1:], expand, fn) {
					return true
				}
			}
			if e.typ != bson.TypeEmbeddedDocument {
				continue
			}
			if positional {
				// A literal field named like the index counts only when
				// present; its absence is not a missing value here.
				if sub, ok := lookupField(e.val, rest[0]); ok {
					if walkValue(sub, rest[1:], expand, fn) {
						return true
					}
				}
				continue
			}
			if walkDoc(e.val, rest, expand, fn) {
				return true
			}
		}
		if positional && !found {
			return fn(missing)
		}
		return false
	}
	return fn(missing)
}

// Lookup returns the value at a dotted path without implicit array
// traversal: components walk documents, and a numeric component also walks
// into an array element. ok is false when the path is absent or would need
// to traverse an array.
func Lookup(doc bson.Raw, path string) (bson.RawValue, bool) {
	cur := bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}
	for _, c := range splitPath(path) {
		switch cur.Type {
		case bson.TypeEmbeddedDocument:
			v, ok := lookupField(cur.Value, c)
			if !ok {
				return missing, false
			}
			cur = v
		case bson.TypeArray:
			i, ok := arrayIndex(c)
			if !ok {
				return missing, false
			}
			v, ok := lookupField(cur.Value, strconv.Itoa(i))
			if !ok {
				return missing, false
			}
			cur = v
		default:
			return missing, false
		}
	}
	return cur, true
}

// IndexValues returns the values an index on path holds for doc under the
// matcher's path semantics: leaf arrays contribute their elements rather
// than themselves, an empty leaf array contributes Undefined (MongoDB's key
// for []), and an absent path contributes the zero RawValue, which keyenc
// encodes as Null. Values are deduplicated by keyenc bytes, first
// occurrence kept. multikey reports that the walk crossed an array, which
// is what the one-array-field-per-compound-key rule checks.
func IndexValues(doc bson.Raw, path string) (vals []bson.RawValue, multikey bool) {
	parts := splitPath(path)
	seen := map[string]struct{}{}
	var buf []byte
	add := func(v bson.RawValue) {
		var err error
		buf, err = keyenc.Encode(buf[:0], v)
		if err != nil {
			// Decimal128 or malformed bytes: keep the value, keyed by its
			// raw bytes, and let the caller's encoder report it.
			buf = append(append(buf[:0], 0xFE, byte(v.Type)), v.Value...)
		}
		if _, dup := seen[string(buf)]; dup {
			return
		}
		seen[string(buf)] = struct{}{}
		vals = append(vals, v)
	}
	var walk func(v bson.RawValue, rest []string)
	walk = func(v bson.RawValue, rest []string) {
		if len(rest) == 0 {
			if v.Type != bson.TypeArray {
				add(v)
				return
			}
			multikey = true
			n := 0
			it := newIter(v.Value)
			for {
				e, ok := it.next()
				if !ok {
					break
				}
				n++
				add(e.value())
			}
			if n == 0 {
				add(undefinedValue)
			}
			return
		}
		switch v.Type {
		case bson.TypeEmbeddedDocument:
			sub, ok := lookupField(v.Value, rest[0])
			if !ok {
				add(missing)
				return
			}
			walk(sub, rest[1:])
		case bson.TypeArray:
			multikey = true
			idx, positional := arrayIndex(rest[0])
			found, any := false, false
			it := newIter(v.Value)
			for i := 0; ; i++ {
				e, ok := it.next()
				if !ok {
					break
				}
				if positional && i == idx {
					found, any = true, true
					walk(e.value(), rest[1:])
				}
				if e.typ != bson.TypeEmbeddedDocument {
					continue
				}
				if positional {
					if sub, ok := lookupField(e.val, rest[0]); ok {
						any = true
						walk(sub, rest[1:])
					}
					continue
				}
				any = true
				walk(e.value(), rest)
			}
			if (positional && !found) || !any {
				add(missing)
			}
		default:
			add(missing)
		}
	}
	walk(docValue(doc), parts)
	return vals, multikey
}
