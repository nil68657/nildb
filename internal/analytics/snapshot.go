package analytics

import (
	"fmt"
	"strings"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

var snapshotHelp = []string{
	"NIL.SNAPSHOT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CREATE [TTL <seconds>]",
	"    Take a snapshot lease for AT <id> on NIL.* and DOC.* reads.",
	"    TTL defaults to nildb.lease-ttl and may not exceed nildb.lease-max.",
	"RELEASE <id>",
	"    Release a lease. Returns 1, or 0 when no such lease is open.",
	"LIST",
	"    List the open leases, cursor leases included.",
	"HELP",
	"    Print this help.",
}

// leaseReply is {id, seq, owner, created_ms, expires_ms}: a map in RESP3,
// the same pairs flat in RESP2.
func leaseReply(l *store.Snapshot) resp.Reply {
	return resp.Map(
		resp.Str("id"), resp.Int(int64(l.ID)),
		resp.Str("seq"), resp.Int(int64(l.Seq)),
		resp.Str("owner"), resp.Str(l.Owner),
		resp.Str("created_ms"), resp.Int(l.Created.UnixMilli()),
		resp.Str("expires_ms"), resp.Int(l.Expires.UnixMilli()),
	)
}

// snapshotCreate is NIL.SNAPSHOT CREATE [TTL seconds].
func (a *Analytics) snapshotCreate(c *command.Ctx, args [][]byte) resp.Reply {
	ttl := a.cfg.Knobs().LeaseTTL
	switch len(args) {
	case 2:
	case 4:
		if !strings.EqualFold(string(args[2]), "TTL") {
			return resp.ErrSyntax
		}
		n, ok := resp.ParseInt(args[3])
		if !ok || n <= 0 || n > int64(time.Duration(1<<62)/time.Second) {
			return resp.Err("ERR TTL must be a positive number of seconds")
		}
		ttl = time.Duration(n) * time.Second
		if limit := a.st.Config().LeaseMax; ttl > limit {
			return resp.Err(fmt.Sprintf("ERR TTL %d is above nildb.lease-max of %d seconds", n, int64(limit/time.Second)))
		}
	default:
		return resp.ErrSyntax
	}
	lease, err := a.st.Lease("NIL.SNAPSHOT "+connName(c), ttl)
	if err != nil {
		return errReply(err)
	}
	return leaseReply(lease)
}

// snapshotRelease is NIL.SNAPSHOT RELEASE id.
func (a *Analytics) snapshotRelease(_ *command.Ctx, args [][]byte) resp.Reply {
	n, ok := resp.ParseInt(args[2])
	if !ok || n < 0 {
		return resp.ErrNotInteger
	}
	lease, ok := a.st.Lookup(uint64(n))
	if !ok {
		return resp.Int(0)
	}
	a.st.Release(lease)
	return resp.Int(1)
}

// snapshotList is NIL.SNAPSHOT LIST: the unexpired leases by id.
func (a *Analytics) snapshotList(c *command.Ctx, _ [][]byte) resp.Reply {
	var items []resp.Reply
	for _, l := range a.st.Leases() {
		if c.Now.Before(l.Expires) {
			items = append(items, leaseReply(l))
		}
	}
	return resp.Array(items...)
}

func (a *Analytics) snapshotHelp(_ *command.Ctx, _ [][]byte) resp.Reply {
	items := make([]resp.Reply, len(snapshotHelp))
	for i, l := range snapshotHelp {
		items[i] = resp.Status(l)
	}
	return resp.Array(items...)
}
