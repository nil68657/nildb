package admin

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// defaultScanCount is how many pairs ROCKS.SCAN returns without COUNT.
const defaultScanCount = 100

// cfProperties are the integer properties ROCKS.CF INFO reports, in
// reply order; the num-files-at-level<N> strings follow them.
var cfProperties = []string{
	"rocksdb.estimate-num-keys",
	"rocksdb.estimate-live-data-size",
	"rocksdb.total-sst-files-size",
	"rocksdb.live-sst-files-size",
	"rocksdb.cur-size-all-mem-tables",
	"rocksdb.size-all-mem-tables",
	"rocksdb.num-entries-active-mem-table",
	"rocksdb.num-immutable-mem-table",
	"rocksdb.mem-table-flush-pending",
	"rocksdb.compaction-pending",
	"rocksdb.estimate-pending-compaction-bytes",
	"rocksdb.block-cache-usage",
	"rocksdb.block-cache-pinned-usage",
}

// numLevels is RocksDB's default num_levels, which store keeps.
const numLevels = 7

// info is ROCKS.INFO. version is the engine's: the linked RocksDB, or the
// Rust engine library under --engine lsm, btree or pgheap.
func (a *admin) info(c *command.Ctx, _ [][]byte) resp.Reply {
	st := c.Store
	cfg := st.Config()
	shim, _ := st.ShimActive()
	names := st.CFNames()
	cfs := make([]resp.Reply, len(names))
	for i, n := range names {
		cfs[i] = resp.Str(n)
	}
	return resp.Map(
		resp.Str("version"), resp.Str(st.EngineVersion()),
		resp.Str("engine"), resp.Str(st.Engine()),
		resp.Str("column_families"), resp.Array(cfs...),
		resp.Str("dir"), resp.Str(cfg.Dir),
		resp.Str("readonly"), resp.Str(yesNo(st.ReadOnly())),
		resp.Str("block_cache_bytes"), resp.Int(cfg.BlockCacheBytes),
		resp.Str("analytics_cache_bytes"), resp.Int(cfg.AnalyticsCacheBytes),
		resp.Str("write_buffer_bytes"), resp.Int(cfg.WriteBufferBytes),
		resp.Str("bg_io_bytes_per_sec"), resp.Int(cfg.BgIOBytesPerSec),
		resp.Str("fsync"), resp.Str(cfg.Fsync),
		resp.Str("leases"), resp.Int(int64(len(st.Leases()))),
		resp.Str("max_snapshots"), resp.Int(int64(cfg.MaxSnapshots)),
		resp.Str("latest_seq"), resp.Int(int64(st.LatestSeq())),
		resp.Str("statistics"), resp.Str(yesNo(cfg.Statistics)),
		resp.Str("rate_limiter_priority_shim"), resp.Str(yesNo(shim)),
	)
}

// property is ROCKS.PROPERTY name [CF cf]. RocksDB formats integer
// properties as decimal strings, so one bulk string covers every
// property; an unknown or empty one replies null.
func (a *admin) property(c *command.Ctx, args [][]byte) resp.Reply {
	cf := store.CFDefault
	switch len(args) {
	case 2:
	case 4:
		if !equalFold(args[2], "CF") {
			return resp.ErrSyntax
		}
		var rep resp.Reply
		if cf, rep = parseCF(args[3]); rep != nil {
			return rep
		}
	default:
		return resp.ErrSyntax
	}
	v, ok := c.Store.Property(cf, string(args[1]))
	if !ok {
		return resp.Null()
	}
	return resp.Str(v)
}

// cfList is ROCKS.CF LIST.
func (a *admin) cfList(c *command.Ctx, _ [][]byte) resp.Reply {
	names := c.Store.CFNames()
	items := make([]resp.Reply, len(names))
	for i, n := range names {
		items[i] = resp.Str(n)
	}
	return resp.Array(items...)
}

// cfInfo is ROCKS.CF INFO cf: property name to integer, skipping
// properties the column family does not report.
func (a *admin) cfInfo(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[2])
	if rep != nil {
		return rep
	}
	var pairs []resp.Reply
	for _, name := range cfProperties {
		if n, ok := c.Store.IntProperty(cf, name); ok {
			pairs = append(pairs, resp.Str(name), resp.Int(int64(n)))
		}
	}
	for level := range numLevels {
		name := "rocksdb.num-files-at-level" + strconv.Itoa(level)
		v, ok := c.Store.Property(cf, name)
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			pairs = append(pairs, resp.Str(name), resp.Int(n))
		}
	}
	return resp.Map(pairs...)
}

// compact is ROCKS.COMPACT [CF cf] [FROM hex] [TO hex]. Without CF it
// compacts the eight column families one after another; a missing bound
// is open.
func (a *admin) compact(c *command.Ctx, args [][]byte) resp.Reply {
	cfs := allCFs()
	var lo, hi []byte
	for i := 1; i < len(args); i += 2 {
		if i+1 == len(args) {
			return resp.ErrSyntax
		}
		var rep resp.Reply
		switch v := args[i+1]; {
		case equalFold(args[i], "CF"):
			var cf store.CF
			cf, rep = parseCF(v)
			cfs = []store.CF{cf}
		case equalFold(args[i], "FROM"):
			lo, rep = parseHex(v)
		case equalFold(args[i], "TO"):
			hi, rep = parseHex(v)
		default:
			return resp.ErrSyntax
		}
		if rep != nil {
			return rep
		}
	}
	for _, cf := range cfs {
		if err := c.Store.Compact(cf, lo, hi); err != nil {
			return storeErr(err)
		}
	}
	return resp.OK()
}

// flush is ROCKS.FLUSH [CF cf].
func (a *admin) flush(c *command.Ctx, args [][]byte) resp.Reply {
	cfs := allCFs()
	switch len(args) {
	case 1:
	case 3:
		if !equalFold(args[1], "CF") {
			return resp.ErrSyntax
		}
		cf, rep := parseCF(args[2])
		if rep != nil {
			return rep
		}
		cfs = []store.CF{cf}
	default:
		return resp.ErrSyntax
	}
	for _, cf := range cfs {
		if err := c.Store.Flush(cf); err != nil {
			return storeErr(err)
		}
	}
	return resp.OK()
}

// flushWAL is ROCKS.FLUSHWAL [SYNC].
func (a *admin) flushWAL(c *command.Ctx, args [][]byte) resp.Reply {
	fsync := false
	switch {
	case len(args) == 2 && equalFold(args[1], "SYNC"):
		fsync = true
	case len(args) != 1:
		return resp.ErrSyntax
	}
	if err := c.Store.FlushWAL(fsync); err != nil {
		return storeErr(err)
	}
	return resp.OK()
}

// checkpoint is ROCKS.CHECKPOINT dir [CF cf]; it replies {dir, seq} and,
// for an export, cf. dir must not exist; a relative one is resolved
// against the server's working directory. seq is read just before the
// checkpoint, so every write up to it is inside; later writes may be too.
func (a *admin) checkpoint(c *command.Ctx, args [][]byte) resp.Reply {
	var cf store.CF
	export := false
	switch len(args) {
	case 2:
	case 4:
		if !equalFold(args[2], "CF") {
			return resp.ErrSyntax
		}
		var rep resp.Reply
		if cf, rep = parseCF(args[3]); rep != nil {
			return rep
		}
		export = true
	default:
		return resp.ErrSyntax
	}
	dir := string(args[1])
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	seq := c.Store.LatestSeq()
	var err error
	if export {
		err = c.Store.ExportCF(cf, dir)
	} else {
		err = c.Store.Checkpoint(dir)
	}
	if err != nil {
		return storeErr(err)
	}
	cp := &checkpoint{dir: dir, seq: seq, at: c.Now}
	pairs := []resp.Reply{resp.Str("dir"), resp.Str(dir), resp.Str("seq"), resp.Int(int64(seq))}
	if export {
		cp.cf = cf.String()
		pairs = append(pairs, resp.Str("cf"), resp.Str(cp.cf))
	}
	a.last.Store(cp)
	return resp.Map(pairs...)
}

// scan is ROCKS.SCAN cf [FROM hex] [TO hex] [COUNT n] [SNAPSHOT id]. It
// returns up to COUNT (100) pairs of [FROM, TO) as a map of hex key to
// hex value, read with total-order seek and without filling the block
// cache. The next page starts at the last key with a 00 byte appended.
func (a *admin) scan(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[1])
	if rep != nil {
		return rep
	}
	var (
		lo, hi []byte
		count  int64        = defaultScanCount
		r      store.Reader = c.Store
	)
	for i := 2; i < len(args); i += 2 {
		if i+1 == len(args) {
			return resp.ErrSyntax
		}
		switch v := args[i+1]; {
		case equalFold(args[i], "FROM"):
			lo, rep = parseHex(v)
		case equalFold(args[i], "TO"):
			hi, rep = parseHex(v)
		case equalFold(args[i], "COUNT"):
			n, ok := resp.ParseInt(v)
			if !ok {
				return resp.ErrNotInteger
			}
			if n < 1 {
				return resp.ErrSyntax
			}
			count = n
		case equalFold(args[i], "SNAPSHOT"):
			id, ok := resp.ParseInt(v)
			if !ok || id < 0 {
				return resp.ErrNotInteger
			}
			lease, ok := c.Store.Lookup(uint64(id))
			if !ok {
				return resp.Err("ERR snapshot lease expired")
			}
			r = c.Store.At(lease)
		default:
			return resp.ErrSyntax
		}
		if rep != nil {
			return rep
		}
	}
	it := r.Iter(cf, lo, hi, store.IterOpts{TotalOrderSeek: true})
	defer it.Close()
	var pairs []resp.Reply
	for it.SeekToFirst(); it.Valid() && int64(len(pairs)/2) < count; it.Next() {
		pairs = append(pairs, hexStr(it.Key()), hexStr(it.Value()))
	}
	if err := it.Err(); err != nil {
		return storeErr(err)
	}
	return resp.Map(pairs...)
}

// get is ROCKS.GET cf keyhex.
func (a *admin) get(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[1])
	if rep != nil {
		return rep
	}
	key, rep := parseHex(args[2])
	if rep != nil {
		return rep
	}
	v, ok, err := c.Store.Get(cf, key)
	if err != nil {
		return storeErr(err)
	}
	if !ok {
		return resp.Null()
	}
	return hexStr(v)
}

// sizes is ROCKS.SIZES cf fromhex tohex. It counts SST files only
// (RocksDB's INCLUDE_FILES default), not memtables. Both bounds are keys:
// an empty TO is the empty key, which makes the range empty.
func (a *admin) sizes(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[1])
	if rep != nil {
		return rep
	}
	lo, rep := parseHex(args[2])
	if rep != nil {
		return rep
	}
	hi, rep := parseHex(args[3])
	if rep != nil {
		return rep
	}
	sizes, err := c.Store.ApproxSizes(cf, [][2][]byte{{lo, hi}})
	if err != nil {
		return storeErr(err)
	}
	return resp.Int(int64(sizes[0]))
}

// seq is ROCKS.SEQ.
func (a *admin) seq(c *command.Ctx, _ [][]byte) resp.Reply {
	return resp.Int(int64(c.Store.LatestSeq()))
}

// stats is ROCKS.STATS.
func (a *admin) stats(c *command.Ctx, _ [][]byte) resp.Reply {
	s, ok := c.Store.Stats()
	if !ok {
		return resp.Err("ERR statistics disabled, start with --rocks-stats")
	}
	return resp.Verbatim("txt", s)
}

// ingest is ROCKS.INGEST cf path [path ...]. RocksDB copies the files in
// with a global sequence number; nothing checks that their keys follow
// NilDB's layout.
func (a *admin) ingest(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[1])
	if rep != nil {
		return rep
	}
	paths := make([]string, len(args)-2)
	for i, p := range args[2:] {
		paths[i] = string(p)
	}
	if err := c.Store.Ingest(cf, paths); err != nil {
		return storeErr(err)
	}
	return resp.OK()
}

// setOption is ROCKS.SETOPTION cf name value.
func (a *admin) setOption(c *command.Ctx, args [][]byte) resp.Reply {
	cf, rep := parseCF(args[1])
	if rep != nil {
		return rep
	}
	if err := c.Store.SetOption(cf, string(args[2]), string(args[3])); err != nil {
		return storeErr(err)
	}
	return resp.OK()
}

var snapshotHelp = []string{
	"ROCKS.SNAPSHOT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CREATE [TTL <seconds>]",
	"    Take a snapshot lease for ROCKS.SCAN SNAPSHOT <id> and for AT <id> on NIL.* and DOC.* reads.",
	"    TTL defaults to nildb.lease-ttl and may not exceed nildb.lease-max.",
	"RELEASE <id>",
	"    Release a lease. Returns 1, or 0 when no such lease is open.",
	"LIST",
	"    List the open leases, cursor leases included.",
	"HELP",
	"    Print this help.",
}

// leaseReply is {id, seq, owner, created_ms, expires_ms}, the shape
// NIL.SNAPSHOT replies.
func leaseReply(l *store.Snapshot) resp.Reply {
	return resp.Map(
		resp.Str("id"), resp.Int(int64(l.ID)),
		resp.Str("seq"), resp.Int(int64(l.Seq)),
		resp.Str("owner"), resp.Str(l.Owner),
		resp.Str("created_ms"), resp.Int(l.Created.UnixMilli()),
		resp.Str("expires_ms"), resp.Int(l.Expires.UnixMilli()),
	)
}

// snapshotCreate is ROCKS.SNAPSHOT CREATE [TTL seconds], with
// NIL.SNAPSHOT's checks and error texts.
func (a *admin) snapshotCreate(c *command.Ctx, args [][]byte) resp.Reply {
	var ttl time.Duration // zero takes the store's LeaseTTL
	if c.Cfg != nil {
		ttl = c.Cfg.Knobs().LeaseTTL
	}
	switch len(args) {
	case 2:
	case 4:
		if !equalFold(args[2], "TTL") {
			return resp.ErrSyntax
		}
		n, ok := resp.ParseInt(args[3])
		if !ok || n <= 0 || n > int64(time.Duration(1<<62)/time.Second) {
			return resp.Err("ERR TTL must be a positive number of seconds")
		}
		ttl = time.Duration(n) * time.Second
		if limit := c.Store.Config().LeaseMax; ttl > limit {
			return resp.Err(fmt.Sprintf("ERR TTL %d is above nildb.lease-max of %d seconds", n, int64(limit/time.Second)))
		}
	default:
		return resp.ErrSyntax
	}
	owner := "ROCKS.SNAPSHOT local"
	if c.Conn != nil {
		owner = "ROCKS.SNAPSHOT conn " + strconv.FormatUint(c.Conn.ID(), 10)
	}
	lease, err := c.Store.Lease(owner, ttl)
	if err != nil {
		return storeErr(err)
	}
	return leaseReply(lease)
}

// snapshotRelease is ROCKS.SNAPSHOT RELEASE id.
func (a *admin) snapshotRelease(c *command.Ctx, args [][]byte) resp.Reply {
	id, ok := resp.ParseInt(args[2])
	if !ok || id < 0 {
		return resp.ErrNotInteger
	}
	lease, ok := c.Store.Lookup(uint64(id))
	if !ok {
		return resp.Int(0)
	}
	c.Store.Release(lease)
	return resp.Int(1)
}

// snapshotList is ROCKS.SNAPSHOT LIST: the unexpired leases by id.
func (a *admin) snapshotList(c *command.Ctx, _ [][]byte) resp.Reply {
	var items []resp.Reply
	for _, l := range c.Store.Leases() {
		if c.Now.Before(l.Expires) {
			items = append(items, leaseReply(l))
		}
	}
	return resp.Array(items...)
}

func (a *admin) snapshotHelp(_ *command.Ctx, _ [][]byte) resp.Reply {
	items := make([]resp.Reply, len(snapshotHelp))
	for i, l := range snapshotHelp {
		items[i] = resp.Status(l)
	}
	return resp.Array(items...)
}
