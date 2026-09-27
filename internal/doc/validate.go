package doc

import (
	"bytes"
	"errors"

	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ValidateInsert checks a document for insertion and returns it with _id
// as its first field: an existing _id is moved to the front, and a missing
// one is generated as a new ObjectId. id is the _id value, aliasing withID.
// withID is doc itself when doc already starts with _id.
//
// Refused: malformed BSON; more than MaxDocSize bytes; nesting deeper than
// MaxDepth; a top-level field name starting with '$'; a field name holding
// '.' at any level; a repeated field name within one document; an _id that
// is an array, regex, undefined, MinKey or MaxKey, holds a Decimal128, or
// is a document with a '$'-prefixed field.
func ValidateInsert(doc bson.Raw) (withID bson.Raw, id bson.RawValue, err error) {
	it := newIter(doc)
	first := true
	idFirstAlready := false
	var idv bson.RawValue
	hasID := false
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if string(e.name) == "_id" {
			if hasID {
				return nil, missing, errf(CodeInvalidIDField, "can't have multiple _id fields in one document")
			}
			idv, hasID = e.value(), true
			idFirstAlready = first
		}
		first = false
	}
	if err := it.err(); err != nil {
		return nil, missing, err
	}
	out := []byte(doc)
	if !idFirstAlready {
		out = idFirst(doc, idv, hasID)
	}
	if len(out) > MaxDocSize {
		return nil, missing, errf(CodeBSONObjectTooLarge, "object to insert too large. size in bytes: %d, max size: %d", len(out), MaxDocSize)
	}
	if err := checkStored(out, true); err != nil {
		return nil, missing, err
	}
	fit := newIter(out)
	e, _ := fit.next()
	return out, e.value(), nil
}

// checkStored validates a whole document for storage: structure, depth,
// field names and _id. insert selects the error text MongoDB uses for a
// top-level '$' field on insert rather than on update.
func checkStored(d []byte, insert bool) error {
	if err := checkLevel(d, 1, true, insert); err != nil {
		return err
	}
	id, ok := lookupField(d, "_id")
	if !ok {
		return nil
	}
	return checkID(id)
}

func checkLevel(d []byte, depth int, top, insert bool) error {
	if depth > MaxDepth {
		return errf(15, "Document exceeds maximum nesting depth of %d", MaxDepth)
	}
	it := newIter(d)
	var names [][]byte
	var seen map[string]struct{}
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		if top && len(e.name) > 0 && e.name[0] == '$' {
			if insert {
				return badValue("Document can't have $ prefixed field names: %s", e.name)
			}
			return errf(52, "The dollar ($) prefixed field '%s' in '%s' is not valid for storage.", e.name, e.name)
		}
		if bytes.IndexByte(e.name, '.') >= 0 {
			return errf(57, "The dotted field '%s' in '%s' is not valid for storage.", e.name, e.name)
		}
		if seen == nil && len(names) < 16 {
			for _, n := range names {
				if bytes.Equal(n, e.name) {
					return badValue("duplicate field name '%s'", e.name)
				}
			}
			names = append(names, e.name)
		} else {
			if seen == nil {
				seen = make(map[string]struct{}, 32)
				for _, n := range names {
					seen[string(n)] = struct{}{}
				}
			}
			if _, dup := seen[string(e.name)]; dup {
				return badValue("duplicate field name '%s'", e.name)
			}
			seen[string(e.name)] = struct{}{}
		}
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			if err := checkLevel(e.val, depth+1, false, insert); err != nil {
				return err
			}
		case bson.TypeArray:
			if err := checkArray(e.val, depth+1, insert); err != nil {
				return err
			}
		}
	}
	return it.err()
}

func checkArray(a []byte, depth int, insert bool) error {
	if depth > MaxDepth {
		return errf(15, "Document exceeds maximum nesting depth of %d", MaxDepth)
	}
	it := newIter(a)
	for {
		e, ok := it.next()
		if !ok {
			break
		}
		var err error
		switch e.typ {
		case bson.TypeEmbeddedDocument:
			err = checkLevel(e.val, depth+1, false, insert)
		case bson.TypeArray:
			err = checkArray(e.val, depth+1, insert)
		}
		if err != nil {
			return err
		}
	}
	return it.err()
}

func checkID(id bson.RawValue) error {
	switch id.Type {
	case bson.TypeArray:
		return errf(CodeInvalidIDField, "can't use an array for _id")
	case bson.TypeRegex, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
		return errf(CodeInvalidIDField, "can't use a %s for _id", typeName(id.Type))
	case bson.TypeEmbeddedDocument:
		if err := checkIDFields(id.Value); err != nil {
			return err
		}
	}
	if _, err := keyenc.Encode(nil, id); err != nil {
		if errors.Is(err, keyenc.ErrUnsupported) {
			return errDecimal
		}
		return errMalformed
	}
	return nil
}

func checkIDFields(d []byte) error {
	it := newIter(d)
	for {
		e, ok := it.next()
		if !ok {
			return it.err()
		}
		if len(e.name) > 0 && e.name[0] == '$' {
			return errf(52, "_id fields may not contain '$'-prefixed fields: %s is not valid for storage.", e.name)
		}
		if e.typ == bson.TypeEmbeddedDocument {
			if err := checkIDFields(e.val); err != nil {
				return err
			}
		}
	}
}
