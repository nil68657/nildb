package analytics

import (
	"fmt"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/layout"
	"github.com/nil68657/nildb/internal/query"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// keystatsCharge is how many bytes the scan reads between bucket takes.
const keystatsCharge = 64 << 10

// keystats is NIL.KEYSTATS [db]. On a fresh lease it counts the Redis keys
// of one database, or of all layout.NumDBs, by type, and how many of them
// carry an expiry. Keys whose expiry has passed are skipped, as every
// Redis read skips them. It scans only the meta column family, as an
// analytical scan: through the byte bucket, under the row cap, without
// filling the block cache, at IO_LOW priority.
func (a *Analytics) keystats(c *command.Ctx, args [][]byte) resp.Reply {
	lo, hi := []byte{0}, []byte{layout.NumDBs}
	db := resp.Str("all")
	switch len(args) {
	case 1:
	case 2:
		n, ok := resp.ParseInt(args[1])
		if !ok || n < 0 || n >= layout.NumDBs {
			return resp.ErrDBIndex
		}
		lo, hi = []byte{byte(n)}, []byte{byte(n) + 1}
		db = resp.Int(n)
	default:
		return resp.ErrSyntax
	}
	s, rep := a.begin(c, "NIL.KEYSTATS", opts{})
	if rep != nil {
		return rep
	}
	defer s.end(false)

	var byType [layout.TZSet + 1]int64
	var keys, expires, rows int64
	owed := 0
	charge := func() {
		s.bucket.Take(owed)
		a.stats.Bytes.Add(int64(owed))
		owed = 0
	}
	now := c.NowMS()
	it := s.opts.Reader.Iter(store.CFMeta, lo, hi, store.IterOpts{
		LowPriority: true, Readahead: 2 << 20, AsyncIO: true,
	})
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k, v := it.Key(), it.Value()
		rows++
		if s.opts.MaxRows > 0 && rows > s.opts.MaxRows {
			return errReply(query.ErrRowLimit)
		}
		if owed += len(k) + len(v); owed >= keystatsCharge {
			charge()
		}
		m, err := layout.DecodeMeta(v)
		if err != nil {
			return resp.Err(fmt.Sprintf("ERR key %q in db %d: %v", k[1:], k[0], err))
		}
		if m.Expired(now) {
			continue
		}
		keys++
		byType[m.Type]++
		if m.ExpireMS != 0 {
			expires++
		}
	}
	charge()
	a.stats.Rows.Add(rows)
	if err := it.Err(); err != nil {
		return errReply(err)
	}
	pairs := []resp.Reply{
		resp.Str("db"), db,
		resp.Str("keys"), resp.Int(keys),
		resp.Str("expires"), resp.Int(expires),
	}
	for t := layout.TString; t <= layout.TZSet; t++ {
		pairs = append(pairs, resp.Str(t.String()), resp.Int(byType[t]))
	}
	return resp.Map(pairs...)
}
