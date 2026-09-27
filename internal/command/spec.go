// Package command is the contract between the server and the packages
// that implement commands (redis, cmddoc, analytics, admin). A package
// describes each command with a Spec and registers it in a Registry; the
// server's executor looks the command up, checks arity, authentication
// and MULTI rules, takes the locks the Spec's KeysFunc returns, and runs
// the Handler with a Ctx that carries the transaction, the snapshot, the
// clock reading and the hooks (SCAN cursor ring, WATCH table) a handler
// needs. Nothing here imports the server, so command packages reach every
// server facility through this package.
package command

import (
	"errors"

	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// Flags describe how the executor treats a command.
type Flags uint16

const (
	// Write commands take the locks their KeysFunc returns and, after a
	// non-empty commit, touch the WATCH table for their LockRedis keys.
	Write Flags = 1 << iota
	// ReadOnly commands never write. They take no locks unless their
	// KeysFunc returns some.
	ReadOnly
	// MultiRead commands read more than one key or iterate across keys.
	// The executor takes a command-scoped snapshot before the handler runs
	// and releases it afterwards; Ctx.Reader reads through it.
	MultiRead
	// NoMulti commands are refused inside MULTI with "ERR Command not
	// allowed inside a transaction", which also aborts the transaction.
	NoMulti
	// Analytic commands run under the analytics semaphore on a lease the
	// analytics package takes itself. The executor does nothing special
	// for them.
	Analytic
	// Admin marks administrative commands (COMMAND INFO shows "admin").
	Admin
	// NoAuth commands run before authentication: AUTH, HELLO, QUIT.
	NoAuth
	// Fast marks O(1) or O(log n) commands (COMMAND INFO shows "fast").
	Fast
)

// KeysFunc returns the lock set of one invocation. args is the full
// argument vector, args[0] being the command name as the client sent it.
// It may also return a parsed form of args that the handler receives as
// Ctx.Parsed, so DOC.INSERT parses its documents once. A non-nil error
// becomes the command's reply and the handler does not run; return
// Fail(reply) to choose the exact reply.
//
// The executor passes the returned LockKeys to store.Lock and, for Write
// commands, uses the LockRedis entries (NS = db, Key = user key) to touch
// the WATCH table. Keys may alias args.
type KeysFunc func(kc *KeyCtx, args [][]byte) (keys []store.LockKey, parsed any, err error)

// KeyCtx is what a KeysFunc may consult.
type KeyCtx struct {
	DB      uint8       // the database the command runs in
	Catalog CatalogView // the view installed with Registry.SetCatalog; nil when none is
}

// CatalogView is the read-only catalog view a KeysFunc can use to turn a
// namespace into lock keys. The document packages install one with
// Registry.SetCatalog; Redis groups ignore it.
type CatalogView interface {
	// ResolveNS returns the coll_id of a namespace.
	ResolveNS(ns string) (collID uint32, ok bool)
	// UniqueIndexes lists the unique indexes of a collection.
	UniqueIndexes(collID uint32) []UniqueIndex
}

// UniqueIndex describes one unique index for lock planning.
type UniqueIndex struct {
	ID     uint32   // idx_id, the NS of its LockUniq keys
	Fields []string // indexed fields in key order
}

// Handler runs one command. args is the full argument vector, args[0]
// being the command name (and args[1] the subcommand name for a
// subcommand). It returns the reply; for an error, return an Err reply,
// never a Go error.
type Handler func(c *Ctx, args [][]byte) resp.Reply

// Spec describes one command.
type Spec struct {
	// Name is the command name, "zadd" or "doc.find". Register lowercases
	// it. For a subcommand it is the subcommand name alone, "setname".
	Name string
	// Arity follows Redis: N means exactly N arguments including the
	// name, -N means at least N. A subcommand's arity counts both the
	// container name and its own. A container left at 0 gets -2.
	Arity int
	Flags Flags
	// Keys returns the lock set. nil means no locks, except that Register
	// fills Keys with KeysFirstLastStep(FirstKey, LastKey, KeyStep) for a
	// Write command that sets FirstKey. Read commands set FirstKey for
	// COMMAND INFO and still take no locks.
	Keys KeysFunc
	// FirstKey, LastKey and KeyStep are the legacy key positions COMMAND
	// INFO reports (argv indexes; LastKey -1 is the last argument). Zero
	// FirstKey means the command names no Redis key.
	FirstKey, LastKey, KeyStep int
	// Group is the COMMAND DOCS group ("string", "generic", "connection",
	// "transactions", "server", ...); empty reports "generic".
	Group string
	// Since is the version COMMAND DOCS reports; empty omits the field.
	Since string
	// Summary is the COMMAND DOCS summary.
	Summary string
	// Run is the handler. A container with Subcommands may leave it nil;
	// when set, it runs for the bare container name (COMMAND with no
	// subcommand).
	Run Handler
	// Subcommands turns the spec into a container such as CLIENT or
	// CONFIG: args[1] picks the subcommand, whose own arity, flags, keys
	// and handler apply. Unknown subcommands reply "ERR unknown subcommand
	// '<sub>'. Try <CMD> HELP.".
	Subcommands []Spec

	full   string           // "client|setname"; Name for a top-level command
	parent *Spec            // container of a subcommand
	subs   map[string]*Spec // lowercased subcommand name -> spec
	order  []*Spec          // subcommands in registration order
}

// FullName is the name Redis prints in arity errors and COMMAND INFO:
// "get", or "client|setname" for a subcommand. It is set by Register.
func (s *Spec) FullName() string {
	if s.full == "" {
		return s.Name
	}
	return s.full
}

// Parent returns the container of a subcommand, or nil.
func (s *Spec) Parent() *Spec { return s.parent }

// Subs returns the registered subcommands in registration order.
func (s *Spec) Subs() []*Spec { return s.order }

// IsContainer reports whether the spec dispatches on a subcommand.
func (s *Spec) IsContainer() bool { return len(s.order) > 0 }

// KeysFirstLastStep builds a KeysFunc that locks (LockRedis, db, key) for
// the arguments at positions first, first+step, ... up to last. Positions
// are argv indexes (args[0] is the name); a negative last counts from the
// end (-1 is the last argument), as in COMMAND INFO. step <= 0 means 1.
// MSET is KeysFirstLastStep(1, -1, 2); GET is KeysFirstLastStep(1, 1, 1).
func KeysFirstLastStep(first, last, step int) KeysFunc {
	if step <= 0 {
		step = 1
	}
	return func(kc *KeyCtx, args [][]byte) ([]store.LockKey, any, error) {
		l := last
		if l < 0 {
			l += len(args)
		}
		if l >= len(args) {
			l = len(args) - 1
		}
		if first <= 0 || first > l {
			return nil, nil, nil
		}
		keys := make([]store.LockKey, 0, (l-first)/step+1)
		for i := first; i <= l; i += step {
			keys = append(keys, store.LockKey{Kind: store.LockRedis, NS: uint32(kc.DB), Key: args[i]})
		}
		return keys, nil, nil
	}
}

// ReplyError carries a ready reply out of a KeysFunc or CheckArity. The
// executor sends Reply unchanged.
type ReplyError struct {
	Reply resp.Reply
}

func (e *ReplyError) Error() string {
	if text, ok := resp.ErrorText(e.Reply); ok {
		return text
	}
	return "command: non-error reply"
}

// Fail wraps an error reply (resp.ErrSyntax, resp.Err("ERR ...")) as a
// Go error for a KeysFunc to return.
func Fail(r resp.Reply) error { return &ReplyError{Reply: r} }

// ErrorReply turns an error into the reply the client sees: the wrapped
// reply of a ReplyError, otherwise "ERR <err text>".
func ErrorReply(err error) resp.Reply {
	var re *ReplyError
	if errors.As(err, &re) {
		return re.Reply
	}
	return resp.Err("ERR " + err.Error())
}
