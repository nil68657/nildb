package command

import (
	"errors"
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

func TestLookupCaseInsensitive(t *testing.T) {
	r := NewRegistry()
	r.Register(Spec{Name: "Doc.Find", Arity: -3, Run: nop})
	for _, n := range []string{"doc.find", "DOC.FIND", "dOc.FiNd"} {
		s, ok := r.Lookup([]byte(n))
		if !ok || s.Name != "doc.find" {
			t.Errorf("Lookup(%q) = %v, %v", n, s, ok)
		}
	}
	if _, ok := r.Lookup([]byte(strings.Repeat("a", 200))); ok {
		t.Error("an overlong name resolved")
	}
	if _, ok := r.Lookup([]byte("doc.findx")); ok {
		t.Error("doc.findx resolved")
	}
	if n := testing.AllocsPerRun(100, func() { r.Lookup([]byte("DOC.FIND")) }); n > 1 {
		t.Errorf("Lookup allocates %.0f times per call", n)
	}
}

func TestRegisterPanics(t *testing.T) {
	cases := map[string][]Spec{
		"duplicate":        {{Name: "a", Arity: 1, Run: nop}, {Name: "A", Arity: 1, Run: nop}},
		"zero arity":       {{Name: "a", Run: nop}},
		"no handler":       {{Name: "a", Arity: 1}},
		"empty name":       {{Arity: 1, Run: nop}},
		"space in name":    {{Name: "a b", Arity: 1, Run: nop}},
		"duplicate sub":    {{Name: "c", Subcommands: []Spec{{Name: "x", Arity: 2, Run: nop}, {Name: "X", Arity: 2, Run: nop}}}},
		"sub without func": {{Name: "c", Subcommands: []Spec{{Name: "x", Arity: 2}}}},
	}
	for name, specs := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			NewRegistry().Register(specs...)
		})
	}
}

func TestSpecsOrderAndFullNames(t *testing.T) {
	r := NewRegistry()
	r.Register(
		Spec{Name: "b", Arity: 1, Run: nop},
		Spec{Name: "a", Subcommands: []Spec{{Name: "y", Arity: 2, Run: nop}, {Name: "x", Arity: 2, Run: nop}}},
	)
	var names []string
	for _, s := range r.Specs() {
		names = append(names, s.FullName())
		for _, c := range s.Subs() {
			names = append(names, c.FullName())
			if c.Parent() != s {
				t.Errorf("%s: wrong parent", c.FullName())
			}
		}
	}
	if got := strings.Join(names, ","); got != "b,a,a|y,a|x" {
		t.Errorf("order %s", got)
	}
	a, _ := r.Lookup([]byte("a"))
	if a.Arity != -2 || !a.IsContainer() {
		t.Errorf("container arity %d, container %v", a.Arity, a.IsContainer())
	}
	if s, ok := r.LookupFullName("A|X"); !ok || s.FullName() != "a|x" {
		t.Errorf("LookupFullName(A|X) = %v, %v", s, ok)
	}
	if _, ok := r.LookupFullName("a|z"); ok {
		t.Error("a|z resolved")
	}
}

func keyStrings(keys []store.LockKey) string {
	var parts []string
	for _, k := range keys {
		if k.Kind != store.LockRedis {
			parts = append(parts, "?")
			continue
		}
		parts = append(parts, string(k.Key)+"@"+string(rune('0'+k.NS)))
	}
	return strings.Join(parts, " ")
}

func TestKeysFirstLastStep(t *testing.T) {
	cases := []struct {
		first, last, step int
		args              []string
		want              string
	}{
		{1, 1, 1, []string{"get", "k"}, "k@3"},
		{1, -1, 2, []string{"mset", "a", "1", "b", "2"}, "a@3 b@3"},
		{1, -1, 2, []string{"mset", "a", "1", "b"}, "a@3 b@3"},
		{1, -1, 1, []string{"del", "a", "b", "c"}, "a@3 b@3 c@3"},
		{1, -2, 1, []string{"blpop", "a", "b", "0"}, "a@3 b@3"},
		{1, 2, 1, []string{"rename", "a", "b"}, "a@3 b@3"},
		{1, 2, 1, []string{"rename", "a"}, "a@3"},
		{2, -1, 1, []string{"x", "y"}, ""},
		{1, -1, 0, []string{"exists", "a", "b"}, "a@3 b@3"},
	}
	for _, c := range cases {
		kf := KeysFirstLastStep(c.first, c.last, c.step)
		keys, parsed, err := kf(&KeyCtx{DB: 3}, argv(c.args...))
		if err != nil || parsed != nil {
			t.Fatalf("%v: err %v parsed %v", c.args, err, parsed)
		}
		if got := keyStrings(keys); got != c.want {
			t.Errorf("(%d,%d,%d) %q: %q, want %q", c.first, c.last, c.step, c.args, got, c.want)
		}
	}
}

func TestFirstKeyFillsKeys(t *testing.T) {
	r := NewRegistry()
	r.Register(
		Spec{Name: "mset", Arity: -3, Flags: Write, Run: nop, FirstKey: 1, LastKey: -1, KeyStep: 2},
		Spec{Name: "get", Arity: 2, Flags: ReadOnly, Run: nop, FirstKey: 1, LastKey: 1, KeyStep: 1},
	)
	if g, _ := r.Lookup([]byte("get")); g.Keys != nil {
		t.Error("a read command got a lock set from FirstKey")
	}
	s, _ := r.Lookup([]byte("mset"))
	if s.Keys == nil {
		t.Fatal("Keys not filled from FirstKey")
	}
	keys, _, _ := s.Keys(&KeyCtx{DB: 0}, argv("mset", "a", "1", "b", "2"))
	if got := keyStrings(keys); got != "a@0 b@0" {
		t.Errorf("keys %q", got)
	}
}

func TestErrorReply(t *testing.T) {
	if got := encode(ErrorReply(Fail(resp.ErrSyntax))); got != "-ERR syntax error\r\n" {
		t.Errorf("Fail: %q", got)
	}
	if got := encode(ErrorReply(errors.New("boom"))); got != "-ERR boom\r\n" {
		t.Errorf("plain: %q", got)
	}
	wrapped := errors.Join(errors.New("ctx"), Fail(resp.ErrWrongType))
	if got := encode(ErrorReply(wrapped)); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Errorf("wrapped: %q", got)
	}
	if Fail(resp.ErrNoAuth).Error() != "NOAUTH Authentication required." {
		t.Errorf("Error text %q", Fail(resp.ErrNoAuth).Error())
	}
}

func TestInfoSections(t *testing.T) {
	r := NewRegistry()
	var ir InfoRegistrar = r
	ir.RegisterInfoSection("RocksDB", func(b *strings.Builder) { b.WriteString("a:1\r\n") })
	ir.RegisterInfoSection("x", nil)
	secs := r.InfoSections()
	if len(secs) != 1 || secs[0].Name != "rocksdb" {
		t.Fatalf("sections %+v", secs)
	}
}

type fakeCatalog struct{}

func (fakeCatalog) ResolveNS(ns string) (uint32, bool) { return 7, ns == "db.c" }
func (fakeCatalog) UniqueIndexes(uint32) []UniqueIndex {
	return []UniqueIndex{{ID: 9, Fields: []string{"a"}}}
}

func TestCatalogView(t *testing.T) {
	r := NewRegistry()
	if r.Catalog() != nil {
		t.Fatal("catalog set on a new registry")
	}
	r.SetCatalog(fakeCatalog{})
	id, ok := r.Catalog().ResolveNS("db.c")
	if !ok || id != 7 {
		t.Errorf("ResolveNS = %d, %v", id, ok)
	}
}

func TestCtxTouch(t *testing.T) {
	c := &Ctx{DB: 2}
	if _, rec := c.TouchedKeys(); rec {
		t.Fatal("recorded before any Touch")
	}
	buf := []byte("k1")
	c.Touch(buf)
	buf[0] = 'X'
	c.TouchDB(5, []byte("k2"))
	keys, rec := c.TouchedKeys()
	if !rec || len(keys) != 2 || string(keys[0].Key) != "k1" || keys[0].DB != 2 || keys[1].DB != 5 {
		t.Errorf("touched %+v, %v", keys, rec)
	}
	c2 := &Ctx{}
	c2.Touch()
	if keys, rec := c2.TouchedKeys(); !rec || len(keys) != 0 {
		t.Errorf("empty Touch: %+v, %v", keys, rec)
	}
}

func TestCtxLockInExecIsNoop(t *testing.T) {
	c := &Ctx{InExec: true}
	unlock := c.Lock([]store.LockKey{{Kind: store.LockUniq, NS: 1, Key: []byte("v")}})
	unlock()
	c.ReleaseLocks()
}

func TestLocalConn(t *testing.T) {
	c := NewLocalConn(4)
	var _ Conn = c
	c.SetDB(3)
	c.SetProto(3)
	c.SetName("n")
	c.SetLibInfo("lib", "1")
	c.Watch(3, argv("a", "b"), []bool{false, true})
	if c.ID() != 4 || c.DB() != 3 || c.Proto() != 3 || c.Name() != "n" || !c.Authenticated() || c.InMulti() {
		t.Errorf("state %+v", c)
	}
	if n, v := c.LibInfo(); n != "lib" || v != "1" {
		t.Errorf("lib %q %q", n, v)
	}
	if w := c.Watched(); len(w) != 2 || !w[1].Stale || w[0].Stale {
		t.Errorf("watched %+v", w)
	}
	c.Unwatch()
	c.Close()
	if len(c.Watched()) != 0 || !c.Closed() {
		t.Error("Unwatch or Close had no effect")
	}
}
