package command

import (
	"testing"

	"github.com/nil68657/nildb/internal/resp"
)

func nop(*Ctx, [][]byte) resp.Reply { return resp.OK() }

func argv(s ...string) [][]byte {
	out := make([][]byte, len(s))
	for i, a := range s {
		out[i] = []byte(a)
	}
	return out
}

func encode(r resp.Reply) string { return string(resp.Encode(r, 2)) }

func TestCheckArity(t *testing.T) {
	r := NewRegistry()
	r.Register(
		Spec{Name: "get", Arity: 2, Run: nop},
		Spec{Name: "mset", Arity: -3, Run: nop},
	)
	get, _ := r.Lookup([]byte("get"))
	mset, _ := r.Lookup([]byte("mset"))
	cases := []struct {
		s    *Spec
		argc int
		ok   bool
	}{
		{get, 2, true}, {get, 1, false}, {get, 3, false},
		{mset, 3, true}, {mset, 5, true}, {mset, 2, false}, {mset, 1, false},
	}
	for _, c := range cases {
		err := r.CheckArity(c.s, c.argc)
		if (err == nil) != c.ok {
			t.Errorf("%s argc %d: err %v, want ok=%v", c.s.Name, c.argc, err, c.ok)
		}
	}
	err := r.CheckArity(get, 3)
	if got, want := encode(ErrorReply(err)), "-ERR wrong number of arguments for 'get' command\r\n"; got != want {
		t.Errorf("arity reply %q, want %q", got, want)
	}
}

func TestResolveErrors(t *testing.T) {
	r := NewRegistry()
	r.Register(
		Spec{Name: "GET", Arity: 2, Run: nop},
		Spec{Name: "client", Subcommands: []Spec{
			{Name: "setname", Arity: 3, Run: nop},
			{Name: "id", Arity: 2, Run: nop},
		}},
		Spec{Name: "command", Arity: -1, Run: nop, Subcommands: []Spec{
			{Name: "count", Arity: 2, Run: nop},
		}},
	)
	cases := []struct {
		args []string
		want string // "" means resolves
		name string // full name of the resolved spec
	}{
		{[]string{"get", "k"}, "", "get"},
		{[]string{"GeT", "k"}, "", "get"},
		{[]string{"get"}, "-ERR wrong number of arguments for 'get' command\r\n", "get"},
		{[]string{"nosuch", "a", "b"}, "-ERR unknown command 'nosuch', with args beginning with: 'a' 'b' \r\n", ""},
		{[]string{"NoSuch"}, "-ERR unknown command 'NoSuch', with args beginning with: \r\n", ""},
		{[]string{"client"}, "-ERR wrong number of arguments for 'client' command\r\n", "client"},
		{[]string{"client", "SETNAME"}, "-ERR wrong number of arguments for 'client|setname' command\r\n", "client|setname"},
		{[]string{"CLIENT", "SetName", "x"}, "", "client|setname"},
		{[]string{"client", "bogus"}, "-ERR unknown subcommand 'bogus'. Try CLIENT HELP.\r\n", "client"},
		{[]string{"command"}, "", "command"},
		{[]string{"command", "COUNT"}, "", "command|count"},
		{[]string{"command", "count", "x"}, "-ERR wrong number of arguments for 'command|count' command\r\n", "command|count"},
	}
	for _, c := range cases {
		s, rep := r.Resolve(argv(c.args...))
		got := ""
		if rep != nil {
			got = encode(rep)
		}
		if got != c.want {
			t.Errorf("%q: reply %q, want %q", c.args, got, c.want)
		}
		name := ""
		if s != nil {
			name = s.FullName()
		}
		if name != c.name {
			t.Errorf("%q: resolved %q, want %q", c.args, name, c.name)
		}
	}
}
