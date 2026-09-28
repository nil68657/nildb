package main

import (
	"reflect"
	"testing"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/testutil"
)

func TestCatalogView(t *testing.T) {
	if testing.Short() {
		t.Skip("opens RocksDB")
	}
	e := testutil.Spawn(t, nil)
	cat, err := catalog.Open(e.Store)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := cat.Create("db.c")
	if err != nil {
		t.Fatal(err)
	}
	uniq, err := cat.AddIndex(coll, catalog.Index{Kind: catalog.Btree, Unique: true, ExpireAfter: catalog.NoExpire,
		Keys: []catalog.Key{{Field: "a"}, {Field: "b", Desc: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.AddIndex(coll, catalog.Index{Kind: catalog.Btree, ExpireAfter: catalog.NoExpire,
		Keys: []catalog.Key{{Field: "c"}}}); err != nil {
		t.Fatal(err)
	}

	v := catalogView{cat}
	id, ok := v.ResolveNS("db.c")
	if !ok || id != coll.ID {
		t.Fatalf("ResolveNS = %d, %v; want %d", id, ok, coll.ID)
	}
	if _, ok := v.ResolveNS("db.none"); ok {
		t.Error("ResolveNS found a missing namespace")
	}
	want := []command.UniqueIndex{{ID: uniq.ID, Fields: []string{"a", "b"}}}
	if got := v.UniqueIndexes(coll.ID); !reflect.DeepEqual(got, want) {
		t.Errorf("UniqueIndexes = %+v, want %+v", got, want)
	}
	if got := v.UniqueIndexes(9999); got != nil {
		t.Errorf("UniqueIndexes of a missing collection = %+v", got)
	}
}
