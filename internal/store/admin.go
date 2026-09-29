package store

import (
	"fmt"
	"strconv"
)

// Property returns the string property name of cf (GetPropertyCF). DB-wide
// properties such as rocksdb.num-snapshots answer through any CF. RocksDB's
// C API returns no found flag, so an empty value reports false. The Rust
// engines answer the names docs/design/rust-engines.md lists; the store
// answers rocksdb.oldest-snapshot-time for them from its own registry.
func (s *Store) Property(cf CF, name string) (string, bool) {
	if s.closed.Load() {
		return "", false
	}
	if v, ok := s.ownProperty(name); ok {
		return strconv.FormatUint(v, 10), true
	}
	return s.kv.property(cf, name)
}

// IntProperty returns the integer property name of cf (GetIntPropertyCF).
func (s *Store) IntProperty(cf CF, name string) (uint64, bool) {
	if s.closed.Load() {
		return 0, false
	}
	if v, ok := s.ownProperty(name); ok {
		return v, true
	}
	return s.kv.intProperty(cf, name)
}

// ownProperty answers the properties the store keeps for engines other
// than RocksDB.
func (s *Store) ownProperty(name string) (uint64, bool) {
	if s.cfg.Engine == EngineRocksDB || name != "rocksdb.oldest-snapshot-time" {
		return 0, false
	}
	return s.oldestSnapshotTime(), true
}

// ApproxSizes returns the approximate on-disk size of each [lo, hi) range
// of cf (GetApproximateSizesCF). Both bounds must be set: an empty hi is
// the empty key, not an open bound. RocksDB and the LSM count table files
// only, not memtables.
func (s *Store) ApproxSizes(cf CF, ranges [][2][]byte) ([]uint64, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	return s.kv.approxSizes(cf, ranges)
}

// Compact runs a blocking manual compaction of [lo, hi) on cf
// (CompactRangeCFOpt); a nil bound is open. It is non-exclusive, so
// automatic compactions and other manual ones keep running beside it, and
// the bottommost level is compacted too when cf has a compaction filter.
// RocksDB's C API reports no error. On the Rust engines the sweeper first
// clears [lo, hi) of what RocksDB's compaction filters would drop; then
// the LSM compacts to its bottom level, pgheap runs VACUUM on the whole
// column family, and the B+ tree has nothing more to do.
func (s *Store) Compact(cf CF, lo, hi []byte) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if s.kv.sweeps() {
		if _, err := s.sweepRange(cf, lo, hi); err != nil {
			return err
		}
	}
	return s.kv.compact(cf, lo, hi)
}

// Flush flushes the memtables of cf to an SST file and waits for it. The
// Rust engines flush every column family: the LSM writes its memtable to
// table files, the B+ tree syncs its file and pgheap runs a checkpoint.
func (s *Store) Flush(cf CF) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.kv.flush(cf)
}

// FlushWAL writes buffered WAL data to the OS and, with sync, fsyncs it.
// Under Fsync "everysec" the caller runs FlushWAL(true) once a second.
func (s *Store) FlushWAL(sync bool) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.kv.flushWAL(sync)
}

// Checkpoint writes an openable copy of the database into dir, which must
// not exist (CreateCheckpoint(dir, 0) on RocksDB: hard links on the same
// filesystem), and then the ENGINE marker, so the copy opens only with
// its engine. The C API returns no sequence number; read LatestSeq
// separately.
func (s *Store) Checkpoint(dir string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.kv.checkpoint(dir); err != nil {
		return err
	}
	return writeEngineMarker(dir, s.cfg.Engine)
}

// ExportCF exports the SST files of cf into dir, which must not exist
// (Checkpoint.ExportColumnFamily). RocksDB only.
func (s *Store) ExportCF(cf CF, dir string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	k, ok := s.kv.(*rocksKV)
	if !ok {
		return s.unsupported()
	}
	return k.exportCF(cf, dir)
}

// Ingest adds external SST files to cf (IngestExternalFileCF) with default
// ingestion options: the files are copied and get a global sequence number.
// RocksDB only.
func (s *Store) Ingest(cf CF, paths []string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	k, ok := s.kv.(*rocksKV)
	if !ok {
		return s.unsupported()
	}
	return k.ingest(cf, paths)
}

// SetOption changes one mutable CF option (SetOptionsCF), for example
// disable_auto_compactions or write_buffer_size. RocksDB only.
func (s *Store) SetOption(cf CF, name, value string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	k, ok := s.kv.(*rocksKV)
	if !ok {
		return s.unsupported()
	}
	return k.setOption(cf, name, value)
}

func (s *Store) unsupported() error {
	return fmt.Errorf("%w with --engine %s", ErrUnsupported, s.cfg.Engine)
}

// LatestSeq returns the sequence number of the last committed write: a
// RocksDB or LSM sequence number, a B+ tree transaction id or a pgheap
// xid, each growing with every commit.
func (s *Store) LatestSeq() uint64 {
	if s.closed.Load() {
		return 0
	}
	return s.kv.latestSeq()
}

// CFNames returns the eight column family names in CF order.
func (s *Store) CFNames() []string {
	out := make([]string, NumCFs)
	copy(out, cfNames[:])
	return out
}

// Stats returns RocksDB's statistics dump (GetStatisticsString); ok is
// false unless Config.Statistics is on. The Rust engines keep their
// counters always, so for them it returns the nil.stats text of every
// column family whatever Config.Statistics says.
func (s *Store) Stats() (string, bool) {
	if s.closed.Load() {
		return "", false
	}
	return s.kv.stats()
}

// PendingCompactions returns how many ScheduleCompact calls are waiting
// for their timer.
func (s *Store) PendingCompactions() int { return s.compact.pending() }
