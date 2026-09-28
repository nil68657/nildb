package admin

import (
	"strconv"
	"strings"

	"github.com/nil68657/nildb/internal/store"
)

// infoRocksdb writes the "rocksdb" INFO section. Each cf_<name> line
// carries the column family's rocksdb.estimate-num-keys and
// rocksdb.total-sst-files-size, in the style of Redis's db0 keyspace
// lines. Block cache figures come from the default CF (the main cache)
// and the col CF (the analytics cache); pending compaction bytes and
// memtable bytes are summed over the eight column families.
func (a *admin) infoRocksdb(b *strings.Builder) {
	st := a.st
	intProp := func(cf store.CF, name string) uint64 {
		n, _ := st.IntProperty(cf, name)
		return n
	}
	field(b, "rocksdb_version", version())
	field(b, "rocksdb_column_families", strings.Join(st.CFNames(), ","))
	var pending, memtables uint64
	for _, cf := range allCFs() {
		field(b, "cf_"+cf.String(),
			"estimate_num_keys="+strconv.FormatUint(intProp(cf, "rocksdb.estimate-num-keys"), 10)+
				",total_sst_files_size="+strconv.FormatUint(intProp(cf, "rocksdb.total-sst-files-size"), 10))
		pending += intProp(cf, "rocksdb.estimate-pending-compaction-bytes")
		memtables += intProp(cf, "rocksdb.cur-size-all-mem-tables")
	}
	uintField(b, "block_cache_usage", intProp(store.CFDefault, "rocksdb.block-cache-usage"))
	uintField(b, "block_cache_pinned_usage", intProp(store.CFDefault, "rocksdb.block-cache-pinned-usage"))
	uintField(b, "block_cache_capacity", intProp(store.CFDefault, "rocksdb.block-cache-capacity"))
	uintField(b, "analytics_cache_usage", intProp(store.CFCol, "rocksdb.block-cache-usage"))
	uintField(b, "analytics_cache_capacity", intProp(store.CFCol, "rocksdb.block-cache-capacity"))
	uintField(b, "cur_size_all_mem_tables", memtables)
	uintField(b, "num_snapshots", intProp(store.CFDefault, "rocksdb.num-snapshots"))
	uintField(b, "oldest_snapshot_time", intProp(store.CFDefault, "rocksdb.oldest-snapshot-time"))
	uintField(b, "estimate_pending_compaction_bytes", pending)
	uintField(b, "num_running_compactions", intProp(store.CFDefault, "rocksdb.num-running-compactions"))
	uintField(b, "num_running_flushes", intProp(store.CFDefault, "rocksdb.num-running-flushes"))
	uintField(b, "latest_sequence_number", st.LatestSeq())
	uintField(b, "nildb_leases", uint64(len(st.Leases())))
	uintField(b, "nildb_max_snapshots", uint64(st.Config().MaxSnapshots))
	uintField(b, "nildb_scheduled_compactions", uint64(st.PendingCompactions()))
	shim, _ := store.ShimActive()
	field(b, "nildb_rate_limiter_priority_shim", yesNo(shim))
	field(b, "nildb_statistics", yesNo(st.Config().Statistics))
}

// infoPersistence appends the last ROCKS.CHECKPOINT to the built-in
// "persistence" section, which already prints nildb_wal and nildb_fsync.
// The time is Unix seconds, 0 before the first checkpoint.
func (a *admin) infoPersistence(b *strings.Builder) {
	cp := a.last.Load()
	if cp == nil {
		cp = &checkpoint{}
	}
	var at int64
	if !cp.at.IsZero() {
		at = cp.at.Unix()
	}
	field(b, "nildb_last_checkpoint_dir", cp.dir)
	field(b, "nildb_last_checkpoint_cf", cp.cf)
	uintField(b, "nildb_last_checkpoint_seq", cp.seq)
	field(b, "nildb_last_checkpoint_time", strconv.FormatInt(at, 10))
}

// lineBreaks keeps a client-chosen value, such as a checkpoint path, on
// its own INFO line.
var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

func field(b *strings.Builder, name, value string) {
	b.WriteString(name)
	b.WriteByte(':')
	b.WriteString(lineBreaks.Replace(value))
	b.WriteString("\r\n")
}

func uintField(b *strings.Builder, name string, n uint64) {
	field(b, name, strconv.FormatUint(n, 10))
}
