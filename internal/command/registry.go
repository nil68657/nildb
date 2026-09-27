package command

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nil68657/nildb/internal/resp"
)

// maxNameLen bounds the command names Lookup lowercases on the stack.
// No command is longer; a longer name is simply unknown.
const maxNameLen = 64

// Registry maps command names to specs. Register every command before the
// server starts serving: Lookup reads the table without a lock. INFO
// sections and the catalog view may be installed at any time.
type Registry struct {
	specs map[string]*Spec
	order []*Spec

	secMu    sync.Mutex
	sections []InfoSection

	catalog atomic.Pointer[catalogBox]
}

type catalogBox struct{ v CatalogView }

// InfoSection is one section a package added to INFO.
type InfoSection struct {
	Name string                   // lowercase section name, "rocksdb"
	Fn   func(b *strings.Builder) // writes "field:value\r\n" lines
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{specs: make(map[string]*Spec)}
}

// Register adds commands. It lowercases names, fills Keys from
// FirstKey/LastKey/KeyStep when Keys is nil and the spec has the Write
// flag (reads take no locks), and indexes subcommands. It
// panics on an empty or duplicate name, a zero arity, or a spec without a
// handler, all of which are programming errors.
func (r *Registry) Register(specs ...Spec) {
	for i := range specs {
		s := prepare(specs[i], nil)
		if _, dup := r.specs[s.Name]; dup {
			panic(fmt.Sprintf("command: %q registered twice", s.Name))
		}
		r.specs[s.Name] = s
		r.order = append(r.order, s)
	}
}

// prepare copies a spec onto the heap and validates it.
func prepare(in Spec, parent *Spec) *Spec {
	s := new(Spec)
	*s = in
	s.Name = strings.ToLower(s.Name)
	if s.Name == "" || strings.ContainsAny(s.Name, " |\r\n") {
		panic(fmt.Sprintf("command: invalid command name %q", in.Name))
	}
	if len(s.Name) > maxNameLen {
		panic(fmt.Sprintf("command: name %q longer than %d bytes", s.Name, maxNameLen))
	}
	s.parent = parent
	s.full = s.Name
	if parent != nil {
		s.full = parent.Name + "|" + s.Name
	}
	if len(in.Subcommands) > 0 {
		if parent != nil {
			panic(fmt.Sprintf("command: %q: subcommands cannot nest", s.full))
		}
		if s.Arity == 0 {
			s.Arity = -2
		}
		s.subs = make(map[string]*Spec, len(in.Subcommands))
		for i := range in.Subcommands {
			sub := prepare(in.Subcommands[i], s)
			if _, dup := s.subs[sub.Name]; dup {
				panic(fmt.Sprintf("command: %q registered twice", sub.full))
			}
			s.subs[sub.Name] = sub
			s.order = append(s.order, sub)
		}
		s.Subcommands = nil
	} else if s.Run == nil {
		panic(fmt.Sprintf("command: %q has no handler", s.full))
	}
	if s.Arity == 0 {
		panic(fmt.Sprintf("command: %q has arity 0", s.full))
	}
	if s.Keys == nil && s.FirstKey > 0 && s.Flags&Write != 0 {
		s.Keys = KeysFirstLastStep(s.FirstKey, s.LastKey, s.KeyStep)
	}
	return s
}

// Lookup finds a top-level command by name, ignoring ASCII case, without
// allocating.
func (r *Registry) Lookup(name []byte) (*Spec, bool) {
	var buf [maxNameLen]byte
	if len(name) > maxNameLen {
		return nil, false
	}
	n := lowerInto(buf[:], name)
	s, ok := r.specs[string(buf[:n])]
	return s, ok
}

// LookupFullName finds a command or a subcommand by its full name,
// "get" or "client|setname", ignoring case.
func (r *Registry) LookupFullName(name string) (*Spec, bool) {
	top, sub, hasSub := strings.Cut(strings.ToLower(name), "|")
	s, ok := r.specs[top]
	if !ok || !hasSub {
		return s, ok
	}
	c, ok := s.subs[sub]
	return c, ok
}

func lowerInto(dst, src []byte) int {
	for i, c := range src {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst[i] = c
	}
	return len(src)
}

// lookupSub finds a subcommand of a container, ignoring case.
func (s *Spec) lookupSub(name []byte) (*Spec, bool) {
	var buf [maxNameLen]byte
	if len(name) > maxNameLen {
		return nil, false
	}
	n := lowerInto(buf[:], name)
	c, ok := s.subs[string(buf[:n])]
	return c, ok
}

// Resolve finds the spec that runs args, descending into a subcommand when
// the command is a container, and checks arity. On failure it returns the
// Redis error reply: "ERR unknown command ...", "ERR unknown subcommand
// ...", or "ERR wrong number of arguments for ...". The returned spec is
// non-nil whenever the name resolved, even when arity failed, so callers
// can count the rejection against it.
func (r *Registry) Resolve(args [][]byte) (*Spec, resp.Reply) {
	if len(args) == 0 {
		return nil, resp.ErrUnknown("", nil)
	}
	s, ok := r.Lookup(args[0])
	if !ok {
		return nil, resp.ErrUnknown(string(args[0]), args[1:])
	}
	if s.IsContainer() && (len(args) >= 2 || s.Run == nil) {
		if len(args) < 2 {
			return s, resp.ErrArity(s.FullName())
		}
		sub, ok := s.lookupSub(args[1])
		if !ok {
			return s, resp.ErrUnknownSubcommand(string(args[1]), string(args[0]))
		}
		s = sub
	}
	if err := r.CheckArity(s, len(args)); err != nil {
		return s, ErrorReply(err)
	}
	return s, nil
}

// CheckArity returns a *ReplyError holding "ERR wrong number of arguments
// for '<full name>' command" when argc does not satisfy s.Arity.
func (r *Registry) CheckArity(s *Spec, argc int) error {
	if (s.Arity > 0 && argc != s.Arity) || (s.Arity < 0 && argc < -s.Arity) {
		return &ReplyError{Reply: resp.ErrArity(s.FullName())}
	}
	return nil
}

// Specs returns the top-level commands in registration order.
func (r *Registry) Specs() []*Spec { return r.order }

// SetCatalog installs the catalog view KeyCtx.Catalog carries.
func (r *Registry) SetCatalog(cv CatalogView) { r.catalog.Store(&catalogBox{v: cv}) }

// Catalog returns the installed catalog view, or nil.
func (r *Registry) Catalog() CatalogView {
	if b := r.catalog.Load(); b != nil {
		return b.v
	}
	return nil
}

// RegisterInfoSection adds an INFO section. name is matched without
// regard to case. A name equal to a built-in section ("persistence")
// appends fn's lines to that section; several registrations under one new
// name share one header. fn writes "field:value\r\n" lines (a bare "\n" is
// turned into "\r\n") and runs on the connection goroutine of the client
// that sent INFO, so it must be safe for concurrent use.
func (r *Registry) RegisterInfoSection(name string, fn func(b *strings.Builder)) {
	if fn == nil {
		return
	}
	r.secMu.Lock()
	r.sections = append(r.sections, InfoSection{Name: strings.ToLower(name), Fn: fn})
	r.secMu.Unlock()
}

// InfoSections returns the registered INFO sections in registration order.
func (r *Registry) InfoSections() []InfoSection {
	r.secMu.Lock()
	defer r.secMu.Unlock()
	return append([]InfoSection(nil), r.sections...)
}
