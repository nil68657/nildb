package server

import (
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// transactionSpecs are MULTI, EXEC, DISCARD, WATCH and UNWATCH. MULTI,
// EXEC, DISCARD and WATCH run at once even inside MULTI (dispatch checks
// Server.immediate); UNWATCH inside MULTI is queued, as in Redis.
func (s *Server) transactionSpecs() []command.Spec {
	return []command.Spec{
		{Name: "multi", Arity: 1, Flags: command.NoMulti | command.Fast, Group: "transactions", Since: "1.2.0",
			Summary: "Starts a transaction.", Run: s.cmdMulti},
		{Name: "exec", Arity: 1, Group: "transactions", Since: "1.2.0",
			Summary: "Executes all commands in a transaction.", Run: s.cmdExec},
		{Name: "discard", Arity: 1, Flags: command.Fast, Group: "transactions", Since: "2.0.0",
			Summary: "Discards a transaction.", Run: s.cmdDiscard},
		{Name: "watch", Arity: -2, Flags: command.NoMulti | command.Fast, FirstKey: 1, LastKey: -1, KeyStep: 1,
			Keys: command.KeysFirstLastStep(1, -1, 1), Group: "transactions", Since: "2.2.0",
			Summary: "Monitors changes to keys to determine the execution of a transaction.", Run: s.cmdWatch},
		{Name: "unwatch", Arity: 1, Flags: command.Fast, Group: "transactions", Since: "2.2.0",
			Summary: "Forgets about watched keys of a transaction.", Run: s.cmdUnwatch},
	}
}

func (s *Server) cmdMulti(ctx *command.Ctx, _ [][]byte) resp.Reply {
	c := ctx.Conn.(*conn)
	if c.inMulti {
		return resp.ErrNestedMulti
	}
	c.inMulti = true
	c.multiDirty = false
	c.queue = nil
	c.multiLen.Store(0)
	return resp.OK()
}

func (s *Server) cmdDiscard(ctx *command.Ctx, _ [][]byte) resp.Reply {
	c := ctx.Conn.(*conn)
	if !c.inMulti {
		return resp.ErrDiscardNoMulti
	}
	c.resetMulti()
	s.watch.remove(c)
	return resp.OK()
}

// prepared is one queued command with the lock set EXEC computed for it.
type prepared struct {
	spec   *command.Spec
	args   [][]byte
	keys   []store.LockKey
	parsed any
	rej    resp.Reply // KeysFunc error: becomes this command's result
}

func (s *Server) cmdExec(ctx *command.Ctx, _ [][]byte) resp.Reply {
	return s.exec(ctx.Conn.(*conn))
}

// exec runs EXEC as architecture.md section 4 describes:
//
//  1. a queue-time error aborts with EXECABORT;
//  2. lock the union of every queued command's keys and every watched key,
//     with each collection lock taken exclusive;
//  3. a watched key touched since WATCH aborts with a null array;
//  4. so does a watched key that was live at WATCH time and is now missing
//     or logically expired;
//  5. run the queue against one BeginIndexed transaction, per-command
//     errors becoming array elements;
//  6. commit once, touch the written keys, unlock, unwatch.
func (s *Server) exec(c *conn) resp.Reply {
	if !c.inMulti {
		return resp.ErrExecNoMulti
	}
	queue, dirty := c.queue, c.multiDirty
	c.resetMulti()
	defer s.watch.remove(c)
	if dirty {
		return resp.ErrExecAbort
	}

	// Lock planning. SELECT inside the queue changes the database of the
	// commands after it, so the plan follows it.
	preps := make([]prepared, len(queue))
	var all []store.LockKey
	db := c.DB()
	for i, q := range queue {
		p := &preps[i]
		p.spec, p.args = q.spec, q.args
		p.keys, p.parsed, p.rej = s.keysOf(q.spec, db, q.args)
		all = append(all, p.keys...)
		if q.spec == s.selectSpec && len(q.args) == 2 {
			if n, ok := parseDB(q.args[1]); ok {
				db = n
			}
		}
	}
	watched := s.watch.entries(c)
	for _, w := range watched {
		all = append(all, store.LockKey{Kind: store.LockRedis, NS: uint32(w.db), Key: w.key})
	}
	for i := range all {
		if all[i].Kind == store.LockColl {
			all[i].Key = collExclusive
		}
	}
	unlock := s.st.Lock(all)
	defer unlock()

	if c.watchDirty.Load() || s.watchedExpired(watched) {
		return resp.NullArray()
	}

	txn := s.st.BeginIndexed()
	replies := make([]resp.Reply, len(preps))
	var touches []command.TouchKey
	for i := range preps {
		p := &preps[i]
		if p.rej != nil {
			replies[i] = p.rej
			s.stats.record(p.spec, 0, p.rej)
			continue
		}
		start := time.Now()
		cctx := s.newCtx(c, p.spec, c.DB(), p.parsed)
		cctx.Txn = txn
		cctx.InExec = true
		before := txn.Len()
		rep, ok := s.runQueued(cctx, p.spec, p.args)
		if !ok {
			txn.Discard()
			return rep
		}
		replies[i] = rep
		s.stats.record(p.spec, time.Since(start), rep)
		touches = appendTouches(touches, cctx, p.spec, p.keys, txn.Len() > before)
	}
	if txn.Len() > 0 {
		if err := txn.Commit(); err != nil {
			return resp.Err("ERR EXEC commit failed: " + err.Error())
		}
	} else {
		txn.Discard()
	}
	for _, t := range touches {
		s.watch.Touch(t.DB, t.Key)
	}
	return resp.Array(replies...)
}

// collExclusive is the LockColl mode byte EXEC forces: exclusive.
var collExclusive = []byte{'w'}

// runQueued runs one queued handler. A panic discards the whole EXEC
// (ok false) because the shared batch may hold part of the command.
func (s *Server) runQueued(ctx *command.Ctx, spec *command.Spec, args [][]byte) (reply resp.Reply, ok bool) {
	defer ctx.ReleaseLocks()
	defer func() {
		if p := recover(); p != nil {
			reply, ok = s.panicReply(spec, p), false
		}
	}()
	return spec.Run(ctx, args), true
}

// appendTouches collects what touchAfter would touch for one command of
// an EXEC, to be applied after the commit.
func appendTouches(dst []command.TouchKey, ctx *command.Ctx, spec *command.Spec, keys []store.LockKey, wrote bool) []command.TouchKey {
	if recorded, ok := ctx.TouchedKeys(); ok {
		return append(dst, recorded...)
	}
	if !wrote || spec.Flags&command.Write == 0 {
		return dst
	}
	for _, k := range keys {
		if k.Kind == store.LockRedis {
			dst = append(dst, command.TouchKey{DB: uint8(k.NS), Key: k.Key})
		}
	}
	return dst
}

// watchedExpired is Redis's isWatchedKeyExpired: true when a key that was
// live at WATCH time is now logically expired. A key that vanished
// without a touch can only have been dropped by compaction after it
// expired, so a missing key counts as expired too. Keys flagged stale at
// WATCH time are skipped.
func (s *Server) watchedExpired(watched []watchEntry) bool {
	if len(watched) == 0 {
		return false
	}
	nowMS := s.cfg.Now().UnixMilli()
	var mk []byte
	for _, w := range watched {
		if w.stale {
			continue
		}
		mk = layout.MetaKey(mk[:0], w.db, w.key)
		live, err := liveMeta(s.st, mk, nowMS)
		if err != nil || !live {
			return true
		}
	}
	return false
}

// liveMeta reports whether the meta entry at mk exists and is not
// logically expired at nowMS.
func liveMeta(r store.Reader, mk []byte, nowMS int64) (bool, error) {
	live := false
	_, err := r.GetPinned(store.CFMeta, mk, func(v []byte) error {
		m, err := layout.DecodeMeta(v)
		if err != nil {
			return err
		}
		live = !m.Expired(nowMS)
		return nil
	})
	return live, err
}
