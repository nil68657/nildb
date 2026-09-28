// Package admin implements the ROCKS.* commands of architecture.md
// section 6, which put ldb-style questions about the RocksDB instance
// under NilDB on the wire, and the "rocksdb" and "persistence" INFO
// sections. Every command is Admin and NoMulti. Raw keys and values cross
// the wire as hex (either case in, lowercase out), and replies with named
// fields are maps in RESP3 and flat arrays in RESP2.
package admin

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nil68657/nildb/internal/command"
	"github.com/nil68657/nildb/internal/resp"
	"github.com/nil68657/nildb/internal/store"
)

// admin is the state the ROCKS.* commands and the INFO sections share.
type admin struct {
	st   *store.Store
	last atomic.Pointer[checkpoint] // nil until a ROCKS.CHECKPOINT succeeds
}

// checkpoint records the last ROCKS.CHECKPOINT that succeeded.
type checkpoint struct {
	dir string
	cf  string // the exported column family; empty for a full checkpoint
	seq uint64 // LatestSeq read just before the checkpoint was taken
	at  time.Time
}

// Register adds the ROCKS.* commands to r and the "rocksdb" and
// "persistence" INFO sections to info, which is the *server.Server or nil
// for r itself (the server renders the sections registered on r either
// way). st is the store the server runs on. A name clash with a command
// already in r is returned as an error.
func Register(r *command.Registry, info command.InfoRegistrar, st *store.Store) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("admin: %v", p)
		}
	}()
	if info == nil {
		info = r
	}
	a := &admin{st: st}
	r.Register(a.specs()...)
	info.RegisterInfoSection("rocksdb", a.infoRocksdb)
	info.RegisterInfoSection("persistence", a.infoPersistence)
	return nil
}

func (a *admin) specs() []command.Spec {
	const (
		group = "server"
		since = "1.0.0"
		rw    = command.Admin | command.NoMulti
		ro    = rw | command.ReadOnly
	)
	return []command.Spec{
		{Name: "rocks.info", Arity: 1, Flags: ro, Group: group, Since: since,
			Summary: "Returns the linked RocksDB version, the column families, cache sizes and the lease count.", Run: a.info},
		{Name: "rocks.property", Arity: -2, Flags: ro, Group: group, Since: since,
			Summary: "Returns a RocksDB property of a column family.", Run: a.property},
		{Name: "rocks.cf", Flags: ro, Group: group, Since: since,
			Summary: "A container for column family commands.", Subcommands: []command.Spec{
				{Name: "list", Arity: 2, Flags: ro, Group: group, Since: since,
					Summary: "Lists the column families.", Run: a.cfList},
				{Name: "info", Arity: 3, Flags: ro, Group: group, Since: since,
					Summary: "Returns the size, memtable and compaction properties of a column family.", Run: a.cfInfo},
			}},
		{Name: "rocks.compact", Arity: -1, Flags: rw, Group: group, Since: since,
			Summary: "Compacts a key range of one or every column family and waits for it.", Run: a.compact},
		{Name: "rocks.flush", Arity: -1, Flags: rw, Group: group, Since: since,
			Summary: "Flushes the memtables of one or every column family and waits for it.", Run: a.flush},
		{Name: "rocks.flushwal", Arity: -1, Flags: rw, Group: group, Since: since,
			Summary: "Writes buffered WAL data to the OS and, with SYNC, fsyncs it.", Run: a.flushWAL},
		{Name: "rocks.checkpoint", Arity: -2, Flags: rw, Group: group, Since: since,
			Summary: "Writes an openable copy of the database, or the SST files of one column family, into a new directory.", Run: a.checkpoint},
		{Name: "rocks.snapshot", Flags: ro, Group: group, Since: since,
			Summary: "Creates, releases and lists snapshot leases, as NIL.SNAPSHOT does.", Subcommands: []command.Spec{
				{Name: "create", Arity: -2, Flags: ro, Group: group, Since: since,
					Summary: "Takes a snapshot lease, optionally with a TTL in seconds.", Run: a.snapshotCreate},
				{Name: "release", Arity: 3, Flags: ro, Group: group, Since: since,
					Summary: "Releases a snapshot lease.", Run: a.snapshotRelease},
				{Name: "list", Arity: 2, Flags: ro, Group: group, Since: since,
					Summary: "Lists snapshot leases.", Run: a.snapshotList},
				{Name: "help", Arity: 2, Flags: ro, Group: group, Since: since,
					Summary: "Shows ROCKS.SNAPSHOT subcommands.", Run: a.snapshotHelp},
			}},
		{Name: "rocks.scan", Arity: -2, Flags: ro, Group: group, Since: since,
			Summary: "Returns the raw key/value pairs of a column family range in hex.", Run: a.scan},
		{Name: "rocks.get", Arity: 3, Flags: ro, Group: group, Since: since,
			Summary: "Returns the raw value of a key in hex.", Run: a.get},
		{Name: "rocks.sizes", Arity: 4, Flags: ro, Group: group, Since: since,
			Summary: "Returns the approximate on-disk size of a key range.", Run: a.sizes},
		{Name: "rocks.seq", Arity: 1, Flags: ro, Group: group, Since: since,
			Summary: "Returns the sequence number of the last committed write.", Run: a.seq},
		{Name: "rocks.stats", Arity: 1, Flags: ro, Group: group, Since: since,
			Summary: "Returns RocksDB's statistics dump.", Run: a.stats},
		{Name: "rocks.ingest", Arity: -3, Flags: rw, Group: group, Since: since,
			Summary: "Adds external SST files to a column family.", Run: a.ingest},
		{Name: "rocks.setoption", Arity: 4, Flags: rw, Group: group, Since: since,
			Summary: "Changes a mutable column family option.", Run: a.setOption},
	}
}

// parseCF resolves a column family name, ignoring ASCII case.
func parseCF(b []byte) (store.CF, resp.Reply) {
	cf, ok := store.ParseCF(strings.ToLower(string(b)))
	if !ok {
		return 0, resp.Errorf("ERR unknown column family '%s'", b)
	}
	return cf, nil
}

// allCFs lists the eight column families in store order.
func allCFs() []store.CF {
	cfs := make([]store.CF, store.NumCFs)
	for i := range cfs {
		cfs[i] = store.CF(i)
	}
	return cfs
}

// parseHex decodes a hex argument. The empty string decodes to nil, which
// the store reads as an open bound.
func parseHex(b []byte) ([]byte, resp.Reply) {
	if len(b) == 0 {
		return nil, nil
	}
	out := make([]byte, hex.DecodedLen(len(b)))
	if _, err := hex.Decode(out, b); err != nil {
		return nil, resp.Errorf("ERR invalid hex string '%s'", b)
	}
	return out, nil
}

func hexStr(b []byte) resp.Reply { return resp.Str(hex.EncodeToString(b)) }

// storeErr turns a store error into the reply NIL.* and DOC.* send for
// the same failure.
func storeErr(err error) resp.Reply {
	switch {
	case errors.Is(err, store.ErrLeaseExpired):
		return resp.Err("ERR snapshot lease expired")
	case errors.Is(err, store.ErrTooManySnapshots):
		return resp.Err("ERR too many open snapshots")
	case errors.Is(err, store.ErrClosed):
		return resp.Err("ERR server is shutting down")
	}
	return resp.Err("ERR " + err.Error())
}

func equalFold(b []byte, s string) bool { return strings.EqualFold(string(b), s) }

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// version is the linked RocksDB version, "11.8.1".
func version() string {
	major, minor, patch := store.Version()
	return strconv.Itoa(major) + "." + strconv.Itoa(minor) + "." + strconv.Itoa(patch)
}
