package doc

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Projection is a compiled find projection: inclusion ({a: 1, "b.c": 1}) or
// exclusion ({a: 0}), with _id included unless the projection sets _id: 0.
// Nested documents in the spec ({a: {b: 1}}) mean dotted paths. Projection
// operators ($slice, $elemMatch, $meta, positional) and computed fields are
// not supported in v1.
type Projection struct {
	root      *pnode // nil: identity
	inclusion bool
	dropID    bool
}

type pnode struct {
	kids map[string]*pnode
	leaf bool
}

// CompileProjection parses a projection document. An empty document
// projects nothing away.
func CompileProjection(p bson.Raw) (*Projection, error) {
	pr := &Projection{}
	if len(p) == 0 {
		return pr, nil
	}
	type entry struct {
		path    string
		include bool
	}
	var entries []entry
	var flatten func(prefix string, d []byte) error
	flatten = func(prefix string, d []byte) error {
		it := newIter(d)
		for {
			e, ok := it.next()
			if !ok {
				break
			}
			name := string(e.name)
			path := name
			if prefix != "" {
				path = prefix + "." + name
			}
			v := e.value()
			switch {
			case v.Type == bson.TypeBoolean || isNumberType(v.Type):
				if err := checkProjectionPath(path); err != nil {
					return err
				}
				entries = append(entries, entry{path, truthy(v)})
			case v.Type == bson.TypeEmbeddedDocument && docEmpty(v.Value):
				return badValue("An empty sub-projection is not a valid value. Found empty object at path %s", path)
			case v.Type == bson.TypeEmbeddedDocument && !isOperatorDoc(v):
				if err := flatten(path, v.Value); err != nil {
					return err
				}
			case v.Type == bson.TypeEmbeddedDocument && isOperatorDoc(v):
				oit := newIter(v.Value)
				op, _ := oit.next()
				return badValue("unsupported projection operator '%s' in v1", op.name)
			default:
				return badValue("unsupported projection value for field '%s' in v1: computed fields need $project in an aggregation", path)
			}
		}
		return it.err()
	}
	if err := flatten("", p); err != nil {
		return nil, err
	}
	mode := 0 // 1 inclusion, -1 exclusion
	idSet, idInclude := false, false
	for _, e := range entries {
		if e.path == "_id" {
			idSet, idInclude = true, e.include
			continue
		}
		want := -1
		if e.include {
			want = 1
		}
		if mode == 0 {
			mode = want
			continue
		}
		if mode != want {
			if e.include {
				return nil, badValue("Cannot do inclusion on field %s in exclusion projection", e.path)
			}
			return nil, badValue("Cannot do exclusion on field %s in inclusion projection", e.path)
		}
	}
	switch {
	case mode == 0 && idSet:
		pr.inclusion = idInclude
	case mode == 0:
		return pr, nil
	default:
		pr.inclusion = mode == 1
	}
	pr.dropID = idSet && !idInclude
	pr.root = &pnode{kids: map[string]*pnode{}}
	for _, e := range entries {
		if e.path == "_id" {
			if pr.inclusion && idInclude {
				pr.root.kids["_id"] = &pnode{leaf: true}
			}
			if !pr.inclusion && !idInclude {
				pr.root.kids["_id"] = &pnode{leaf: true}
			}
			continue
		}
		if err := pr.root.add(e.path); err != nil {
			return nil, err
		}
	}
	return pr, nil
}

func checkProjectionPath(path string) error {
	for _, c := range splitPath(path) {
		if c == "" {
			return badValue("FieldPath field names may not be empty strings.")
		}
		if c == "$" {
			return badValue("positional projection is not supported in v1")
		}
		if c[0] == '$' {
			return badValue("FieldPath field names may not start with '$'.")
		}
	}
	return nil
}

func (n *pnode) add(path string) error {
	parts := splitPath(path)
	cur := n
	for i, p := range parts {
		if cur.leaf {
			return badValue("Path collision at %s remaining portion %s", path, strings.Join(parts[i:], "."))
		}
		k := cur.kids[p]
		if k == nil {
			k = &pnode{kids: map[string]*pnode{}}
			cur.kids[p] = k
		}
		cur = k
	}
	if cur.leaf || len(cur.kids) > 0 {
		return badValue("Path collision at %s", path)
	}
	cur.leaf = true
	return nil
}

// Apply returns doc projected. An identity projection returns doc itself.
func (p *Projection) Apply(doc bson.Raw) (bson.Raw, error) {
	if p.root == nil {
		return doc, nil
	}
	if err := newIter(doc).err(); err != nil {
		return nil, err
	}
	var w builder
	if p.inclusion {
		includeDoc(&w, p.root, doc, !p.dropID)
	} else {
		excludeDoc(&w, p.root, doc)
	}
	return w.b, nil
}

// includeDoc writes the fields of d that n selects, in document order.
// keepID includes a top-level _id the projection does not name.
func includeDoc(w *builder, n *pnode, d []byte, keepID bool) {
	w.begin()
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		k := n.kids[string(e.name)]
		if k == nil {
			if keepID && string(e.name) == "_id" {
				w.rawElem(e.raw)
			}
			continue
		}
		if k.leaf {
			w.rawElem(e.raw)
			continue
		}
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			w.beginElem(e.typ, string(e.name))
			includeFields(w, k, e.val)
			w.end()
		case bson.TypeArray:
			w.beginElem(e.typ, string(e.name))
			includeArray(w, k, e.val)
			w.end()
		}
	}
	w.end()
}

// includeFields writes the selected fields of d into the open document.
func includeFields(w *builder, n *pnode, d []byte) {
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			return
		}
		k := n.kids[string(e.name)]
		if k == nil {
			continue
		}
		if k.leaf {
			w.rawElem(e.raw)
			continue
		}
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			w.beginElem(e.typ, string(e.name))
			includeFields(w, k, e.val)
			w.end()
		case bson.TypeArray:
			w.beginElem(e.typ, string(e.name))
			includeArray(w, k, e.val)
			w.end()
		}
	}
}

// includeArray projects each document (and nested array) element of an
// array; other elements are dropped, as in MongoDB.
func includeArray(w *builder, n *pnode, arr []byte) {
	it := newIter(arr)
	i := 0
	for {
		e, ok := it.next()
		if !ok {
			return
		}
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			w.beginElem(e.typ, itoa(i))
			includeFields(w, n, e.val)
			w.end()
			i++
		case bson.TypeArray:
			w.beginElem(e.typ, itoa(i))
			includeArray(w, n, e.val)
			w.end()
			i++
		}
	}
}

func excludeDoc(w *builder, n *pnode, d []byte) {
	w.begin()
	excludeFields(w, n, d)
	w.end()
}

func excludeFields(w *builder, n *pnode, d []byte) {
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			return
		}
		k := n.kids[string(e.name)]
		switch {
		case k == nil:
			w.rawElem(e.raw)
		case k.leaf:
		case e.typ == bson.TypeEmbeddedDocument:
			w.beginElem(e.typ, string(e.name))
			excludeFields(w, k, e.val)
			w.end()
		case e.typ == bson.TypeArray:
			w.beginElem(e.typ, string(e.name))
			excludeArray(w, k, e.val)
			w.end()
		default:
			w.rawElem(e.raw)
		}
	}
}

func excludeArray(w *builder, n *pnode, arr []byte) {
	it := newIter(arr)
	for i := 0; ; i++ {
		e, ok := it.next()
		if !ok {
			return
		}
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			w.beginElem(e.typ, itoa(i))
			excludeFields(w, n, e.val)
			w.end()
		case bson.TypeArray:
			w.beginElem(e.typ, itoa(i))
			excludeArray(w, n, e.val)
			w.end()
		default:
			w.elem(itoa(i), e.value())
		}
	}
}
