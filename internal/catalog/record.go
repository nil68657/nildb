package catalog

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/nil68657/nildb/internal/geo"
	"github.com/nil68657/nildb/internal/layout"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Record values (architecture.md section 3):
//
//	'c' | ns         -> {coll_id: int64, created_ms: int64}
//	'i' | coll | name -> {idx_id: int64, kind: string, keys: {f: 1|-1|"2dsphere"},
//	                      unique: bool, sparse: bool, expireAfterSeconds: int64,
//	                      s2: {coarsest: int32, finest: int32, maxCells: int32},
//	                      state: "building"|"ready", multikey: bool}
//
// Ids are stored as int64 so the whole uint32 range fits.

func corrupt(format string, a ...any) error {
	return fmt.Errorf("catalog: %w: %s", layout.ErrCorrupt, fmt.Sprintf(format, a...))
}

func mustMarshal(d bson.D) []byte {
	b, err := bson.Marshal(d)
	if err != nil {
		panic(err) // plain scalars and nested bson.D always marshal
	}
	return b
}

func encodeCollection(c *Collection) []byte {
	return mustMarshal(bson.D{
		{Key: "coll_id", Value: int64(c.ID)},
		{Key: "created_ms", Value: c.Created},
	})
}

func decodeCollection(ns string, v []byte) (*Collection, error) {
	raw := bson.Raw(v)
	if err := raw.Validate(); err != nil {
		return nil, corrupt("collection %q: %v", ns, err)
	}
	id, ok := raw.Lookup("coll_id").AsInt64OK()
	if !ok || id <= 0 || id >= math.MaxUint32 {
		return nil, corrupt("collection %q: bad coll_id", ns)
	}
	created, _ := raw.Lookup("created_ms").AsInt64OK()
	return &Collection{ID: uint32(id), NS: ns, Created: created}, nil
}

func encodeIndex(ix *Index) []byte {
	keys := make(bson.D, 0, len(ix.Keys))
	for _, k := range ix.Keys {
		var v any = int32(1)
		switch {
		case ix.Kind != Btree:
			v = ix.Kind.String()
		case k.Desc:
			v = int32(-1)
		}
		keys = append(keys, bson.E{Key: k.Field, Value: v})
	}
	return mustMarshal(bson.D{
		{Key: "idx_id", Value: int64(ix.ID)},
		{Key: "kind", Value: ix.Kind.String()},
		{Key: "keys", Value: keys},
		{Key: "unique", Value: ix.Unique},
		{Key: "sparse", Value: ix.Sparse},
		{Key: "expireAfterSeconds", Value: ix.ExpireAfter},
		{Key: "s2", Value: bson.D{
			{Key: "coarsest", Value: int32(ix.S2.Coarsest)},
			{Key: "finest", Value: int32(ix.S2.Finest)},
			{Key: "maxCells", Value: int32(ix.S2.MaxCells)},
		}},
		{Key: "state", Value: ix.State.String()},
		{Key: "multikey", Value: ix.Multikey()},
	})
}

func decodeIndex(name string, v []byte) (*Index, error) {
	raw := bson.Raw(v)
	if err := raw.Validate(); err != nil {
		return nil, corrupt("index %q: %v", name, err)
	}
	bad := func(what string) error { return corrupt("index %q: bad %s", name, what) }
	id, ok := raw.Lookup("idx_id").AsInt64OK()
	if !ok || id <= 0 || id >= math.MaxUint32 {
		return nil, bad("idx_id")
	}
	ks, _ := raw.Lookup("kind").StringValueOK()
	kind, ok := ParseIndexKind(ks)
	if !ok {
		return nil, bad("kind")
	}
	ix := &Index{ID: uint32(id), Name: name, Kind: kind, ExpireAfter: NoExpire, multikey: new(atomic.Bool)}
	keys, ok := raw.Lookup("keys").DocumentOK()
	if !ok {
		return nil, bad("keys")
	}
	elems, err := keys.Elements()
	if err != nil || len(elems) == 0 {
		return nil, bad("keys")
	}
	for _, e := range elems {
		k := Key{Field: e.Key()}
		if kind == Btree {
			n, ok := e.Value().AsInt64OK()
			if !ok || (n != 1 && n != -1) {
				return nil, bad("key direction")
			}
			k.Desc = n == -1
		}
		ix.Keys = append(ix.Keys, k)
	}
	ix.Unique, _ = raw.Lookup("unique").BooleanOK()
	ix.Sparse, _ = raw.Lookup("sparse").BooleanOK()
	if ttl, ok := raw.Lookup("expireAfterSeconds").AsInt64OK(); ok {
		ix.ExpireAfter = ttl
	}
	if s2, ok := raw.Lookup("s2").DocumentOK(); ok {
		get := func(k string) int {
			n, _ := s2.Lookup(k).AsInt64OK()
			return int(n)
		}
		ix.S2 = geo.Params{Coarsest: get("coarsest"), Finest: get("finest"), MaxCells: get("maxCells")}
	}
	switch st, _ := raw.Lookup("state").StringValueOK(); st {
	case "ready":
		ix.State = Ready
	case "building":
		ix.State = Building
	default:
		return nil, bad("state")
	}
	if mk, _ := raw.Lookup("multikey").BooleanOK(); mk {
		ix.multikey.Store(true)
	}
	return ix, nil
}
