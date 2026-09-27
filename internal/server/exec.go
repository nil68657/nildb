package server

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

var (
	errNotInMulti = resp.Err("ERR Command not allowed inside a transaction")
	errQueueLimit = resp.Err("ERR MULTI queue limit reached")
	replyQueued   = resp.Status("QUEUED")
)

// errInternalFormat is the reply to a handler panic.
const errInternalFormat = "ERR internal error in '%s': %v"

// dispatch applies the checks Redis's processCommand applies, in its
// order (existence and arity, authentication, MULTI rules), then queues
// the command inside MULTI or runs it.
func (s *Server) dispatch(c *conn, args [][]byte) resp.Reply {
	now := time.Now()
	c.lastActive.Store(now.UnixNano())
	s.stats.commands.Add(1)

	spec, rej := s.reg.Resolve(args)
	if spec != nil {
		c.lastSpec.Store(spec)
	}
	if rej == nil && s.cfg.RequirePass != "" && !c.Authenticated() && spec.Flags&command.NoAuth == 0 {
		rej = resp.ErrNoAuth
	}
	if rej == nil && c.inMulti && !s.immediate[spec] {
		switch {
		case spec.Flags&command.NoMulti != 0:
			rej = errNotInMulti
		case len(c.queue) >= max(s.cfg.Knobs().MultiQueueMax, 1):
			rej = errQueueLimit
		default:
			c.queue = append(c.queue, queued{spec: spec, args: args})
			c.multiLen.Store(int32(len(c.queue)))
			return replyQueued
		}
	}
	if rej != nil {
		if c.inMulti {
			c.multiDirty = true
		}
		s.stats.reject(spec, rej)
		return rej
	}
	reply := s.run(c, spec, args)
	s.stats.record(spec, time.Since(now), reply)
	return reply
}

// newCtx builds the Ctx of one command.
func (s *Server) newCtx(c *conn, spec *command.Spec, db uint8, parsed any) *command.Ctx {
	return &command.Ctx{
		Conn:    c,
		Store:   s.st,
		Now:     s.cfg.Now(),
		DB:      db,
		Parsed:  parsed,
		Cfg:     s.cfg,
		Spec:    spec,
		Cursors: s.cursors,
		Watch:   s.watch,
	}
}

// keysOf runs spec's KeysFunc for db. A KeysFunc error comes back as the
// reply to send instead of running the command.
func (s *Server) keysOf(spec *command.Spec, db uint8, args [][]byte) ([]store.LockKey, any, resp.Reply) {
	if spec.Keys == nil {
		return nil, nil, nil
	}
	kc := command.KeyCtx{DB: db, Catalog: s.reg.Catalog()}
	keys, parsed, err := spec.Keys(&kc, args)
	if err != nil {
		return nil, nil, command.ErrorReply(err)
	}
	return keys, parsed, nil
}

// run executes one autocommit command: lock its keys, take a snapshot for
// MultiRead, run the handler against a fresh Begin() transaction, commit
// once if it wrote anything, touch watchers, then unlock. The reply is
// written by the caller after the locks are gone.
func (s *Server) run(c *conn, spec *command.Spec, args [][]byte) (reply resp.Reply) {
	db := c.DB()
	keys, parsed, rej := s.keysOf(spec, db, args)
	if rej != nil {
		return rej
	}
	unlock := s.st.Lock(keys)
	defer unlock()
	ctx := s.newCtx(c, spec, db, parsed)
	ctx.SetHeld(keys)
	defer ctx.ReleaseLocks()
	if spec.Flags&command.MultiRead != 0 {
		if snap := s.st.Snapshot(); snap != nil {
			ctx.Snap = snap
			defer s.st.Release(snap)
		}
	}
	txn := s.st.Begin()
	ctx.Txn = txn
	defer func() {
		if p := recover(); p != nil {
			txn.Discard()
			reply = s.panicReply(spec, p)
		}
	}()
	reply = spec.Run(ctx, args)
	wrote := txn.Len() > 0
	if wrote {
		if err := txn.Commit(); err != nil {
			return resp.Err("ERR " + err.Error())
		}
	} else {
		txn.Discard()
	}
	s.touchAfter(ctx, spec, keys, wrote)
	return reply
}

// touchAfter touches the WATCH table for a finished command: the keys the
// handler recorded with Ctx.Touch when it recorded any, otherwise the
// LockRedis keys of a Write command that wrote something. It runs while
// the command still holds its key locks, so an EXEC that locks a watched
// key sees the dirty flag or runs entirely before the write.
func (s *Server) touchAfter(ctx *command.Ctx, spec *command.Spec, keys []store.LockKey, wrote bool) {
	if recorded, ok := ctx.TouchedKeys(); ok {
		for _, k := range recorded {
			s.watch.Touch(k.DB, k.Key)
		}
		return
	}
	if !wrote || spec.Flags&command.Write == 0 {
		return
	}
	for _, k := range keys {
		if k.Kind == store.LockRedis {
			s.watch.Touch(uint8(k.NS), k.Key)
		}
	}
}

// panicReply logs a handler panic and turns it into an error reply. The
// transaction is discarded, so nothing the handler wrote is committed.
func (s *Server) panicReply(spec *command.Spec, p any) resp.Reply {
	log.Printf("nildb: panic in %s: %v\n%s", spec.FullName(), p, debug.Stack())
	return resp.Err(fmt.Sprintf(errInternalFormat, spec.FullName(), p))
}
