package command

import "github.com/nil68657/nildb/internal/resp"

// flagNames lists the flags COMMAND INFO prints, in Redis's table order.
var flagNames = []struct {
	f    Flags
	name string
}{
	{Write, "write"},
	{ReadOnly, "readonly"},
	{Admin, "admin"},
	{Fast, "fast"},
	{NoAuth, "no_auth"},
	{NoMulti, "no_multi"},
}

// groupCategory maps a COMMAND DOCS group to its ACL category.
var groupCategory = map[string]string{
	"generic":      "@keyspace",
	"string":       "@string",
	"list":         "@list",
	"set":          "@set",
	"sorted-set":   "@sortedset",
	"hash":         "@hash",
	"geo":          "@geo",
	"connection":   "@connection",
	"transactions": "@transaction",
}

// categories returns the ACL categories of s in Redis's table order:
// keyspace, read, write, the type categories, admin, fast or slow,
// dangerous, connection, transaction.
func categories(s *Spec) []resp.Reply {
	group := s.Group
	if group == "" && s.parent != nil {
		group = s.parent.Group
	}
	gc := groupCategory[group]
	var out []resp.Reply
	add := func(c string) { out = append(out, resp.Status(c)) }
	if gc == "@keyspace" {
		add(gc)
	}
	if s.Flags&ReadOnly != 0 {
		add("@read")
	}
	if s.Flags&Write != 0 {
		add("@write")
	}
	switch gc {
	case "@set", "@sortedset", "@list", "@hash", "@string", "@geo":
		add(gc)
	}
	if s.Flags&Admin != 0 {
		add("@admin")
	}
	if s.Flags&Fast != 0 {
		add("@fast")
	} else {
		add("@slow")
	}
	if s.Flags&Admin != 0 {
		add("@dangerous")
	}
	if gc == "@connection" || gc == "@transaction" {
		add(gc)
	}
	return out
}

// infoReply is one command's COMMAND INFO entry: the ten-element array
// Redis 7.2 sends (name, arity, flags, first key, last key, step, ACL
// categories, tips, key specs, subcommands).
func infoReply(s *Spec) resp.Reply {
	var flags []resp.Reply
	for _, f := range flagNames {
		if s.Flags&f.f != 0 {
			flags = append(flags, resp.Status(f.name))
		}
	}
	first, last, step := s.FirstKey, s.LastKey, s.KeyStep
	if first <= 0 {
		first, last, step = 0, 0, 0
	} else if step <= 0 {
		step = 1
	}
	subs := make([]resp.Reply, 0, len(s.order))
	for _, c := range s.order {
		subs = append(subs, infoReply(c))
	}
	return resp.Array(
		resp.Str(s.FullName()),
		resp.Int(int64(s.Arity)),
		resp.Set(flags...),
		resp.Int(int64(first)),
		resp.Int(int64(last)),
		resp.Int(int64(step)),
		resp.Set(categories(s)...),
		resp.Set(),
		resp.Array(),
		resp.Array(subs...),
	)
}

// CommandInfoReply builds the reply of COMMAND INFO name... and, with no
// names, of COMMAND: one entry per command, a null for each unknown name.
// Names may be full subcommand names ("client|setname").
func CommandInfoReply(r *Registry, names []string) resp.Reply {
	if len(names) == 0 {
		out := make([]resp.Reply, 0, len(r.order))
		for _, s := range r.order {
			out = append(out, infoReply(s))
		}
		return resp.Array(out...)
	}
	out := make([]resp.Reply, 0, len(names))
	for _, n := range names {
		if s, ok := r.LookupFullName(n); ok {
			out = append(out, infoReply(s))
		} else {
			out = append(out, resp.Null())
		}
	}
	return resp.Array(out...)
}

// docsReply is one command's COMMAND DOCS map: summary and since when
// set, group always, and the subcommands' docs keyed by full name.
func docsReply(s *Spec) resp.Reply {
	var kv []resp.Reply
	if s.Summary != "" {
		kv = append(kv, resp.Str("summary"), resp.Str(s.Summary))
	}
	if s.Since != "" {
		kv = append(kv, resp.Str("since"), resp.Str(s.Since))
	}
	group := s.Group
	if group == "" && s.parent != nil {
		group = s.parent.Group
	}
	if group == "" {
		group = "generic"
	}
	kv = append(kv, resp.Str("group"), resp.Str(group))
	if len(s.order) > 0 {
		sub := make([]resp.Reply, 0, 2*len(s.order))
		for _, c := range s.order {
			sub = append(sub, resp.Str(c.FullName()), docsReply(c))
		}
		kv = append(kv, resp.Str("subcommands"), resp.Map(sub...))
	}
	return resp.Map(kv...)
}

// CommandDocsReply builds the reply of COMMAND DOCS [name...]: a map from
// command name to its docs. With no names it covers every command;
// unknown names are left out, as in Redis.
func CommandDocsReply(r *Registry, names []string) resp.Reply {
	var kv []resp.Reply
	if len(names) == 0 {
		for _, s := range r.order {
			kv = append(kv, resp.Str(s.FullName()), docsReply(s))
		}
		return resp.Map(kv...)
	}
	for _, n := range names {
		if s, ok := r.LookupFullName(n); ok {
			kv = append(kv, resp.Str(s.FullName()), docsReply(s))
		}
	}
	return resp.Map(kv...)
}

// CommandListReply builds the reply of COMMAND LIST: every command name,
// each followed by its subcommands' full names.
func CommandListReply(r *Registry) resp.Reply {
	var out []resp.Reply
	for _, s := range r.order {
		out = append(out, resp.Str(s.FullName()))
		for _, c := range s.order {
			out = append(out, resp.Str(c.FullName()))
		}
	}
	return resp.Array(out...)
}

// CommandCountReply builds the reply of COMMAND COUNT: the number of
// top-level commands.
func CommandCountReply(r *Registry) resp.Reply { return resp.Int(int64(len(r.order))) }
