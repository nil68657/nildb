package server

import (
	"strconv"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// FakeStringCommands returns minimal string commands over the meta column family
// for the server's tests: enough of SET, GET, INCR, DEL, PEXPIRE, EXISTS,
// MGET and FLUSHDB to replay multi.tcl without the redis package, plus
// PANIC (a handler that panics) and KEYSERR (a KeysFunc that fails). It is
// exported so the external server_test package can register it.
func FakeStringCommands() []command.Spec {
	w := command.Write
	return []command.Spec{
		{Name: "set", Arity: -3, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Run: tSet},
		{Name: "get", Arity: 2, Flags: command.ReadOnly, FirstKey: 1, LastKey: 1, KeyStep: 1, Run: tGet},
		{Name: "incr", Arity: 2, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Run: tIncr},
		{Name: "del", Arity: -2, Flags: w, FirstKey: 1, LastKey: -1, KeyStep: 1, Run: tDel},
		{Name: "pexpire", Arity: 3, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Run: tPexpire},
		{Name: "exists", Arity: -2, Flags: command.ReadOnly, FirstKey: 1, LastKey: -1, KeyStep: 1, Run: tExists},
		{Name: "mget", Arity: -2, Flags: command.ReadOnly | command.MultiRead, FirstKey: 1, LastKey: -1, KeyStep: 1, Run: tMget},
		{Name: "flushdb", Arity: 1, Flags: w | command.NoMulti, Run: tFlushDB},
		{Name: "panic", Arity: -1, Flags: w, FirstKey: 1, LastKey: 1, KeyStep: 1, Run: tPanic},
		{Name: "keyserr", Arity: 1, Flags: w, Run: tNop, Keys: func(*command.KeyCtx, [][]byte) ([]store.LockKey, any, error) {
			return nil, nil, command.Fail(resp.ErrSyntax)
		}},
	}
}

func tNop(*command.Ctx, [][]byte) resp.Reply { return resp.OK() }

func tPanic(c *command.Ctx, args [][]byte) resp.Reply {
	if len(args) > 1 {
		c.Txn.Put(store.CFMeta, layout.MetaKey(nil, c.DB, args[1]), layout.Meta{Type: layout.TString, Payload: []byte("half")}.Encode(nil))
	}
	panic("test panic")
}

// tLoad reads a live string key.
func tLoad(c *command.Ctx, key []byte) (layout.Meta, bool, resp.Reply) {
	v, ok, err := c.Reader().Get(store.CFMeta, layout.MetaKey(nil, c.DB, key))
	if err != nil {
		return layout.Meta{}, false, resp.Err("ERR " + err.Error())
	}
	if !ok {
		return layout.Meta{}, false, nil
	}
	m, err := layout.DecodeMeta(v)
	if err != nil {
		return layout.Meta{}, false, resp.Err("ERR " + err.Error())
	}
	if m.Expired(c.NowMS()) {
		return layout.Meta{}, false, nil
	}
	if m.Type != layout.TString {
		return layout.Meta{}, false, resp.ErrWrongType
	}
	return m, true, nil
}

func tPut(c *command.Ctx, key []byte, m layout.Meta) {
	c.Txn.Put(store.CFMeta, layout.MetaKey(nil, c.DB, key), m.Encode(nil))
}

// tSet is SET key value [PX ms] [NX].
func tSet(c *command.Ctx, args [][]byte) resp.Reply {
	m := layout.Meta{Type: layout.TString, Payload: args[2]}
	nx := false
	for i := 3; i < len(args); i++ {
		switch {
		case equalFold(args[i], "px") && i+1 < len(args):
			ms, ok := resp.ParseInt(args[i+1])
			if !ok {
				return resp.ErrNotInteger
			}
			m.ExpireMS = c.NowMS() + ms
			i++
		case equalFold(args[i], "nx"):
			nx = true
		default:
			return resp.ErrSyntax
		}
	}
	if nx {
		if _, ok, rep := tLoad(c, args[1]); rep != nil {
			return rep
		} else if ok {
			return resp.Null()
		}
	}
	tPut(c, args[1], m)
	return resp.OK()
}

func tGet(c *command.Ctx, args [][]byte) resp.Reply {
	m, ok, rep := tLoad(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !ok:
		return resp.Null()
	}
	return resp.Bulk(append([]byte(nil), m.Payload...))
}

func tIncr(c *command.Ctx, args [][]byte) resp.Reply {
	m, ok, rep := tLoad(c, args[1])
	if rep != nil {
		return rep
	}
	var n int64
	if ok {
		v, good := resp.ParseInt(m.Payload)
		if !good {
			return resp.ErrNotInteger
		}
		n = v
	} else {
		m = layout.Meta{Type: layout.TString}
	}
	n++
	m.Payload = strconv.AppendInt(nil, n, 10)
	tPut(c, args[1], m)
	return resp.Int(n)
}

// tDel deletes the live keys among args and touches exactly those.
func tDel(c *command.Ctx, args [][]byte) resp.Reply {
	var n int64
	c.Touch()
	for _, k := range args[1:] {
		_, ok, rep := tLoad(c, k)
		if rep != nil && rep != resp.ErrWrongType {
			return rep
		}
		if ok {
			c.Txn.Delete(store.CFMeta, layout.MetaKey(nil, c.DB, k))
			c.Touch(k)
			n++
		}
	}
	return resp.Int(n)
}

func tPexpire(c *command.Ctx, args [][]byte) resp.Reply {
	ms, ok := resp.ParseInt(args[2])
	if !ok {
		return resp.ErrNotInteger
	}
	m, ok, rep := tLoad(c, args[1])
	switch {
	case rep != nil:
		return rep
	case !ok:
		return resp.Int(0)
	}
	m.ExpireMS = c.NowMS() + ms
	tPut(c, args[1], m)
	return resp.Int(1)
}

func tExists(c *command.Ctx, args [][]byte) resp.Reply {
	var n int64
	for _, k := range args[1:] {
		if _, ok, _ := tLoad(c, k); ok {
			n++
		}
	}
	return resp.Int(n)
}

func tMget(c *command.Ctx, args [][]byte) resp.Reply {
	out := make([]resp.Reply, 0, len(args)-1)
	for _, k := range args[1:] {
		m, ok, _ := tLoad(c, k)
		if ok {
			out = append(out, resp.Bulk(append([]byte(nil), m.Payload...)))
		} else {
			out = append(out, resp.Null())
		}
	}
	return resp.Array(out...)
}

// tFlushDB touches the watchers of existing keys first, then deletes every
// meta entry of the db, as the real FLUSHDB is told to.
func tFlushDB(c *command.Ctx, _ [][]byte) resp.Reply {
	c.Watch.TouchExisting(c.DB, func(key []byte) bool {
		ok, _ := c.Store.GetPinned(store.CFMeta, layout.MetaKey(nil, c.DB, key), func([]byte) error { return nil })
		return ok
	})
	it := c.Store.Iter(store.CFMeta, []byte{c.DB}, []byte{c.DB + 1}, store.IterOpts{})
	var keys [][]byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	err := it.Err()
	it.Close()
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	for _, k := range keys {
		c.Txn.Delete(store.CFMeta, k)
	}
	c.Touch()
	return resp.OK()
}
