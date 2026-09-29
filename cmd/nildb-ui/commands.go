package main

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// cmdInfo is one command as COMMAND INFO and COMMAND DOCS describe it.
type cmdInfo struct {
	Name        string     `json:"name"` // "get", or "client|list" for a subcommand
	Arity       int        `json:"arity"`
	Flags       []string   `json:"flags"`
	Group       string     `json:"group,omitempty"`
	Summary     string     `json:"summary,omitempty"`
	Since       string     `json:"since,omitempty"`
	Blocked     bool       `json:"blocked,omitempty"` // --readonly refuses it
	Subcommands []*cmdInfo `json:"subcommands,omitempty"`
}

func (c *cmdInfo) has(flag string) bool {
	for _, f := range c.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

// cmdTable indexes commands and subcommands by lowercase full name.
type cmdTable struct {
	list   []*cmdInfo
	byName map[string]*cmdInfo
}

// parseCommandTable builds the table from the replies of COMMAND and
// COMMAND DOCS. A malformed entry is skipped; docs are optional.
func parseCommandTable(info, docs Value) *cmdTable {
	t := &cmdTable{byName: make(map[string]*cmdInfo)}
	for _, e := range info.Elems {
		if c := parseCommandInfo(e); c != nil {
			t.list = append(t.list, c)
			t.byName[c.Name] = c
			for _, s := range c.Subcommands {
				t.byName[s.Name] = s
			}
		}
	}
	applyDocs(t, docs)
	sort.Slice(t.list, func(i, j int) bool { return t.list[i].Name < t.list[j].Name })
	return t
}

// parseCommandInfo reads one COMMAND INFO entry: name, arity, flags,
// first key, last key, step, ACL categories, tips, key specs, subcommands.
func parseCommandInfo(e Value) *cmdInfo {
	if e.Kind != KindArray || len(e.Elems) < 3 || e.Elems[0].Kind != KindBulk {
		return nil
	}
	c := &cmdInfo{Name: strings.ToLower(e.Elems[0].Text()), Arity: int(e.Elems[1].Int), Flags: []string{}}
	for _, f := range e.Elems[2].Elems {
		c.Flags = append(c.Flags, f.Text())
	}
	if len(e.Elems) >= 10 {
		for _, s := range e.Elems[9].Elems {
			if sc := parseCommandInfo(s); sc != nil {
				c.Subcommands = append(c.Subcommands, sc)
			}
		}
	}
	return c
}

// applyDocs copies summary, since and group from a COMMAND DOCS map.
func applyDocs(t *cmdTable, docs Value) {
	if docs.Kind != KindMap {
		return
	}
	for i := 0; i+1 < len(docs.Elems); i += 2 {
		c := t.byName[strings.ToLower(docs.Elems[i].Text())]
		if c == nil {
			continue
		}
		d := docs.Elems[i+1]
		get := func(k string) string {
			v, _ := d.Lookup(k)
			return v.Text()
		}
		c.Summary, c.Since, c.Group = get("summary"), get("since"), get("group")
		if subs, ok := d.Lookup("subcommands"); ok {
			applyDocs(t, subs)
		}
	}
}

// lookup resolves args to the command or subcommand that would run them.
func (t *cmdTable) lookup(args [][]byte) *cmdInfo {
	c := t.byName[strings.ToLower(string(args[0]))]
	if c != nil && len(c.Subcommands) > 0 && len(args) >= 2 {
		if sc := t.byName[c.Name+"|"+strings.ToLower(string(args[1]))]; sc != nil {
			return sc
		}
	}
	return c
}

// adminReads are admin commands that only read: --readonly lets them run.
var adminReads = map[string]bool{"config|get": true, "client|list": true}

// writes reports whether --readonly refuses c: it writes data, or it is
// an admin command that changes the server (SHUTDOWN, CONFIG SET,
// ROCKS.COMPACT, ROCKS.CHECKPOINT, ...).
func writes(c *cmdInfo) bool {
	if c.has("write") {
		return true
	}
	return c.has("admin") && !c.has("readonly") && !adminReads[c.Name]
}

// stateCommands change what a connection is: they only run in a Console
// session, never on a pooled connection that the next request reuses.
var stateCommands = map[string]bool{
	"hello": true, "auth": true, "multi": true, "exec": true, "discard": true,
	"watch": true, "unwatch": true, "quit": true, "reset": true,
}

// commands caches the command table and reloads it when it is stale.
type commands struct {
	pool *pool

	mu     sync.Mutex
	table  *cmdTable
	loaded time.Time
	err    error
	tried  time.Time
}

// reloadAfter is how old the table may be when a new connection opens
// before it is read again; NilDB may have restarted as another build.
const reloadAfter = 30 * time.Second

// get returns the table, loading it when missing. A failed load is
// retried at most once a second.
func (c *commands) get(ctx context.Context) (*cmdTable, error) {
	c.mu.Lock()
	t, err, tried := c.table, c.err, c.tried
	c.mu.Unlock()
	if t != nil {
		return t, nil
	}
	if err != nil && time.Since(tried) < time.Second {
		return nil, err
	}
	return c.load(ctx)
}

func (c *commands) load(ctx context.Context) (*cmdTable, error) {
	vals, err := c.pool.do(ctx, 0, [][][]byte{
		{[]byte("COMMAND")},
		{[]byte("COMMAND"), []byte("DOCS")},
	}, true)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tried = time.Now()
	if err == nil && vals[0].IsErr() {
		err = errors.New(vals[0].Text())
	}
	if err != nil {
		c.err = err
		return nil, err
	}
	c.table, c.loaded, c.err = parseCommandTable(vals[0], vals[1]), time.Now(), nil
	return c.table, nil
}

// stale drops a table older than reloadAfter; the pool calls it for every
// new connection.
func (c *commands) stale() {
	c.mu.Lock()
	if c.table != nil && time.Since(c.loaded) > reloadAfter {
		c.table = nil
	}
	c.mu.Unlock()
}

// flaglessReads only read but carry no readonly flag in NilDB's table.
var flaglessReads = map[string]bool{
	"info": true, "ping": true, "echo": true, "time": true, "command": true,
	"command|count": true, "command|info": true, "command|docs": true, "command|list": true,
	"client|id": true, "client|getname": true, "client|info": true, "client|list": true, "config|get": true,
}

// readOnly reports whether every command in cmds only reads, so a failed
// attempt may be repeated. Without a table nothing is retried.
func (c *commands) readOnly(ctx context.Context, cmds [][][]byte) bool {
	t, err := c.get(ctx)
	if err != nil {
		return false
	}
	for _, args := range cmds {
		ci := t.lookup(args)
		if ci == nil || writes(ci) || !ci.has("readonly") && !flaglessReads[ci.Name] {
			return false
		}
	}
	return true
}

// blockedError is a command the console refuses to send.
type blockedError struct{ msg string }

func (e *blockedError) Error() string { return e.msg }

// check returns a *blockedError when the console must not send args:
// connection state commands outside a session, and under --readonly every
// command whose COMMAND INFO flags say it writes. Without a command table
// --readonly refuses everything rather than guess.
func (c *commands) check(ctx context.Context, args [][]byte, inSession, readonly bool) error {
	name := strings.ToLower(string(args[0]))
	if !inSession && stateCommands[name] {
		return &blockedError{strings.ToUpper(name) + " changes the connection; run it in the Console, which keeps one connection per tab"}
	}
	if !readonly {
		return nil
	}
	t, err := c.get(ctx)
	if err != nil {
		return &blockedError{"--readonly needs the command table from COMMAND, which failed: " + err.Error()}
	}
	ci := t.lookup(args)
	if ci == nil {
		c.stale()
		return nil
	}
	if writes(ci) {
		return &blockedError{"nildb-ui runs with --readonly and " + strings.ToUpper(ci.Name) + " writes"}
	}
	return nil
}

// list returns the table with Blocked filled in for --readonly.
func (c *commands) list(ctx context.Context, readonly bool) ([]*cmdInfo, error) {
	t, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*cmdInfo, len(t.list))
	for i, ci := range t.list {
		cp := *ci
		cp.Blocked = readonly && writes(ci)
		if len(ci.Subcommands) > 0 {
			cp.Subcommands = make([]*cmdInfo, len(ci.Subcommands))
			for j, sc := range ci.Subcommands {
				scp := *sc
				scp.Blocked = readonly && writes(sc)
				cp.Subcommands[j] = &scp
			}
		}
		out[i] = &cp
	}
	return out, nil
}
