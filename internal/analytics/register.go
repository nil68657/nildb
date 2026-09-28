// Package analytics implements the NIL.* commands of architecture.md
// sections 6 and 7 with scope-v1.md's cuts: NIL.AGGREGATE, NIL.COUNT,
// NIL.DISTINCT, NIL.EXPLAIN, NIL.KEYSTATS and NIL.SNAPSHOT, plus the
// "analytics" INFO section.
//
// A command that scans runs in query's Analytic mode: it waits for a slot
// on a counting semaphore of nildb.analytics-max-concurrent (re-read on
// every acquire, so CONFIG SET takes effect) for at most
// nildb.analytics-queue-timeout, reads one store lease for its whole run
// (a fresh one, or the lease its AT option names), and reads without
// filling the block cache, at IO_LOW rate-limiter priority, through a
// byte bucket of nildb.analytics-read-bps, under the row cap
// nildb.analytics-max-rows and an optional TIMEOUT. It holds no lock while
// it iterates. A result larger than one batch becomes a cursor that keeps
// the lease and takes a semaphore slot for every DOC.CURSOR READ.
package analytics

import (
	"sync/atomic"

	"github.com/nil68657/nildb/internal/catalog"
	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/config"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/store"
)

// Analytics is the state the NIL.* commands share.
type Analytics struct {
	cfg *config.Config
	st  *store.Store
	cat *catalog.Catalog
	q   *query.Engine
	sem semaphore

	stats      query.Stats
	queries    atomic.Int64
	waits      atomic.Int64
	rejections atomic.Int64
	throttleNS atomic.Int64
}

// Register adds the NIL.* commands to r and the "analytics" section to
// info (the server, or r itself). cfg supplies the runtime knobs; cat is
// the catalog and q the query engine that internal/cmddoc was registered
// with, so NIL.AGGREGATE cursors are read with DOC.CURSOR READ. It returns
// the shared state, whose counters INFO analytics prints.
func Register(r *command.Registry, info command.InfoRegistrar, cfg *config.Config, cat *catalog.Catalog, q *query.Engine) *Analytics {
	a := &Analytics{cfg: cfg, st: q.Store(), cat: cat, q: q}
	r.Register(a.specs()...)
	info.RegisterInfoSection("analytics", a.info)
	return a
}

func (a *Analytics) specs() []command.Spec {
	const (
		group = "analytics"
		since = "1.0.0"
	)
	flags := command.Analytic | command.NoMulti | command.ReadOnly
	return []command.Spec{
		{Name: "nil.aggregate", Arity: -3, Flags: flags, Group: group, Since: since,
			Summary: "Runs an aggregation pipeline on a snapshot lease, throttled, and returns a cursor.", Run: a.aggregate},
		{Name: "nil.count", Arity: -2, Flags: flags, Group: group, Since: since,
			Summary: "Counts matching documents on a snapshot lease.", Run: a.count},
		{Name: "nil.distinct", Arity: -3, Flags: flags, Group: group, Since: since,
			Summary: "Returns the distinct values of a field on a snapshot lease.", Run: a.distinct},
		{Name: "nil.explain", Arity: 3, Flags: flags, Group: group, Since: since,
			Summary: "Shows how NIL.AGGREGATE would run a pipeline.", Run: a.explain},
		{Name: "nil.keystats", Arity: -1, Flags: flags, Group: group, Since: since,
			Summary: "Counts Redis keys by type, and those with an expiry, on a snapshot.", Run: a.keystats},
		{Name: "nil.snapshot", Flags: flags, Group: group, Since: since,
			Summary: "Creates, releases and lists snapshot leases.", Subcommands: []command.Spec{
				{Name: "create", Arity: -2, Flags: flags, Summary: "Takes a snapshot lease, optionally with a TTL in seconds.", Run: a.snapshotCreate},
				{Name: "release", Arity: 3, Flags: flags, Summary: "Releases a snapshot lease.", Run: a.snapshotRelease},
				{Name: "list", Arity: 2, Flags: flags, Summary: "Lists snapshot leases.", Run: a.snapshotList},
				{Name: "help", Arity: 2, Flags: flags, Summary: "Shows NIL.SNAPSHOT subcommands.", Run: a.snapshotHelp},
			}},
	}
}
