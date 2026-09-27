package command

import (
	"strings"
	"testing"

	"github.com/nil68657/nildb/internal/resp"
)

func tableRegistry() *Registry {
	r := NewRegistry()
	r.Register(
		Spec{Name: "get", Arity: 2, Flags: ReadOnly | Fast, FirstKey: 1, LastKey: 1, KeyStep: 1, Group: "string", Since: "1.0.0", Summary: "Returns the string value of a key.", Run: nop},
		Spec{Name: "client", Group: "connection", Summary: "A container for client connection commands.", Subcommands: []Spec{
			{Name: "id", Arity: 2, Flags: Fast, Summary: "Returns the unique client ID of the connection.", Run: nop},
		}},
	)
	return r
}

func TestCommandInfoReply(t *testing.T) {
	r := tableRegistry()
	got := string(resp.Encode(CommandInfoReply(r, []string{"GET", "nosuch", "client|id"}), 2))
	want := "*3\r\n" +
		"*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@read\r\n+@string\r\n+@fast\r\n*0\r\n*0\r\n*0\r\n" +
		"$-1\r\n" +
		"*10\r\n$9\r\nclient|id\r\n:2\r\n*1\r\n+fast\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@fast\r\n+@connection\r\n*0\r\n*0\r\n*0\r\n"
	if got != want {
		t.Errorf("COMMAND INFO\n got %q\nwant %q", got, want)
	}
	all := string(resp.Encode(CommandInfoReply(r, nil), 3))
	if !strings.HasPrefix(all, "*2\r\n*10\r\n$3\r\nget\r\n:2\r\n~2\r\n") {
		t.Errorf("COMMAND in RESP3 starts %q", all[:min(60, len(all))])
	}
	if !strings.Contains(all, "$6\r\nclient\r\n:-2\r\n~0\r\n") || !strings.Contains(all, "*1\r\n*10\r\n$9\r\nclient|id") {
		t.Errorf("container entry missing its subcommand: %q", all)
	}
}

func TestCommandDocsReply(t *testing.T) {
	r := tableRegistry()
	got := string(resp.Encode(CommandDocsReply(r, []string{"get", "nosuch"}), 3))
	want := "%1\r\n$3\r\nget\r\n%3\r\n$7\r\nsummary\r\n$34\r\nReturns the string value of a key.\r\n$5\r\nsince\r\n$5\r\n1.0.0\r\n$5\r\ngroup\r\n$6\r\nstring\r\n"
	if got != want {
		t.Errorf("COMMAND DOCS get\n got %q\nwant %q", got, want)
	}
	all := string(resp.Encode(CommandDocsReply(r, nil), 2))
	if !strings.Contains(all, "$11\r\nsubcommands\r\n*2\r\n$9\r\nclient|id\r\n*4\r\n$7\r\nsummary\r\n") {
		t.Errorf("COMMAND DOCS misses subcommand docs: %q", all)
	}
	if !strings.Contains(all, "$5\r\ngroup\r\n$10\r\nconnection\r\n") {
		t.Errorf("subcommand does not inherit the group: %q", all)
	}
}

func TestCommandListAndCount(t *testing.T) {
	r := tableRegistry()
	if got := string(resp.Encode(CommandListReply(r), 2)); got != "*3\r\n$3\r\nget\r\n$6\r\nclient\r\n$9\r\nclient|id\r\n" {
		t.Errorf("COMMAND LIST %q", got)
	}
	if got := string(resp.Encode(CommandCountReply(r), 2)); got != ":2\r\n" {
		t.Errorf("COMMAND COUNT %q", got)
	}
}
