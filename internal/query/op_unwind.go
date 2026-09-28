package query

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() { registerStage("$unwind", parseUnwindStage) }

// unwindSpec is a parsed $unwind: {path, includeArrayIndex,
// preserveNullAndEmptyArrays}, or the short form "$path".
type unwindSpec struct {
	path     []string
	index    string // includeArrayIndex, "" for none
	preserve bool
}

func parseUnwindStage(arg bson.RawValue) (*stage, error) {
	u := &unwindSpec{}
	var path string
	switch arg.Type {
	case bson.TypeString:
		path, _ = strOf(arg)
	case bson.TypeEmbeddedDocument:
		it := iterDoc(arg.Value)
		for e, ok := it.next(); ok; e, ok = it.next() {
			v := e.value()
			switch name := string(e.name); name {
			case "path":
				s, ok := strOf(v)
				if !ok {
					return nil, exprErr(28808, "expected a string as the path for $unwind stage, got %s", typeName(v.Type))
				}
				path = s
			case "includeArrayIndex":
				s, ok := strOf(v)
				if !ok || s == "" {
					return nil, exprErr(28810, "expected a non-empty string for the includeArrayIndex option to $unwind stage, got %s", typeName(v.Type))
				}
				if strings.HasPrefix(s, "$") {
					return nil, exprErr(28822, "includeArrayIndex option to $unwind stage should not be prefixed with a '$': %s", s)
				}
				u.index = s
			case "preserveNullAndEmptyArrays":
				if v.Type != bson.TypeBoolean {
					return nil, exprErr(28809, "expected a boolean for the preserveNullAndEmptyArrays option to $unwind stage, got %s", typeName(v.Type))
				}
				u.preserve = v.Value[0] != 0
			default:
				return nil, exprErr(28811, "unrecognized option to $unwind stage: %s", name)
			}
		}
		if path == "" {
			return nil, exprErr(28812, "no path specified to $unwind stage")
		}
	default:
		return nil, exprErr(15981, "expected either a string or an object as specification for $unwind stage, got %s", typeName(arg.Type))
	}
	if !strings.HasPrefix(path, "$") {
		return nil, exprErr(28818, "path option to $unwind stage should be prefixed with a '$': %s", path)
	}
	parts, err := fieldPath(path[1:])
	if err != nil {
		return nil, err
	}
	u.path = parts
	return &stage{label: "$unwind " + path, build: func(x *Exec, in Operator) (Operator, error) {
		op := &unwindOp{x: x, in: in, spec: u}
		if u.index != "" {
			op.setIndex = newFieldSetter(u.index)
		}
		return op, nil
	}}, nil
}

// unwindOp outputs one row per element of the array at the path, with the
// path set to the element. The path walks documents only. A value that is
// not an array passes as a one-element array. Missing, null and empty
// arrays are dropped unless preserveNullAndEmptyArrays, which passes the
// row (without the field for an empty array). includeArrayIndex holds the
// element's index as an int64, or null for a row that was not unwound.
type unwindOp struct {
	x        *Exec
	in       Operator
	spec     *unwindSpec
	setIndex *fieldSetter

	cur    Row
	elems  []bson.RawValue
	i      int
	active bool
}

func (o *unwindOp) Next() (Row, bool, error) {
	for {
		if o.active && o.i < len(o.elems) {
			el, idx := o.elems[o.i], o.i
			o.i++
			d := replacePath(o.cur.Doc, o.spec.path, el, false)
			if o.setIndex != nil {
				d = o.setIndex.set(d, i64(int64(idx)))
			}
			return Row{Doc: d, ID: o.cur.ID, Dist: o.cur.Dist}, true, nil
		}
		o.active = false
		r, ok, err := o.in.Next()
		if err != nil || !ok {
			return r, ok, err
		}
		v, found := strictPath(r.Doc, o.spec.path)
		switch {
		case !found || v.Type == bson.TypeNull || v.Type == bson.TypeUndefined:
			if o.spec.preserve {
				return o.passed(r, r.Doc), true, nil
			}
		case v.Type == bson.TypeArray:
			elems := arrayValues(v.Value)
			if len(elems) > 0 {
				o.cur, o.elems, o.i, o.active = r, elems, 0, true
				continue
			}
			if o.spec.preserve {
				return o.passed(r, replacePath(r.Doc, o.spec.path, bson.RawValue{}, true)), true, nil
			}
		default:
			return o.passed(r, r.Doc), true, nil
		}
	}
}

// passed returns a row that goes through without unwinding.
func (o *unwindOp) passed(r Row, d bson.Raw) Row {
	if o.setIndex != nil {
		d = o.setIndex.set(d, nullV)
	}
	r.Doc = d
	return r
}

func (o *unwindOp) Suspend() { o.in.Suspend() }
func (o *unwindOp) Close()   { o.in.Close() }

// strictPath follows parts through documents only.
func strictPath(d []byte, parts []string) (bson.RawValue, bool) {
	v, ok := lookup(d, parts[0])
	for _, p := range parts[1:] {
		if !ok || v.Type != bson.TypeEmbeddedDocument {
			return bson.RawValue{}, false
		}
		v, ok = lookup(v.Value, p)
	}
	return v, ok
}

// replacePath returns d with the value at the document path parts set to v,
// or removed when remove is set.
func replacePath(d []byte, parts []string, v bson.RawValue, remove bool) bson.Raw {
	var w builder
	w.begin()
	it := iterDoc(d)
	for e, ok := it.next(); ok; e, ok = it.next() {
		switch {
		case string(e.name) != parts[0]:
			w.rawElem(e.raw)
		case len(parts) == 1:
			if !remove {
				w.elem(parts[0], v)
			}
		case e.typ == bson.TypeEmbeddedDocument:
			w.elem(parts[0], docV(replacePath(e.val, parts[1:], v, remove)))
		default:
			w.rawElem(e.raw)
		}
	}
	return w.end()
}
