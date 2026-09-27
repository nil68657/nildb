package store

import (
	"github.com/linxGnu/grocksdb"
)

// Property returns the string property name of cf (GetPropertyCF). DB-wide
// properties such as rocksdb.num-snapshots answer through any CF. RocksDB's
// C API returns no found flag, so an empty value reports false.
func (s *Store) Property(cf CF, name string) (string, bool) {
	if s.closed.Load() {
		return "", false
	}
	v := s.db.GetPropertyCF(name, s.handle(cf))
	return v, v != ""
}

// IntProperty returns the integer property name of cf (GetIntPropertyCF).
func (s *Store) IntProperty(cf CF, name string) (uint64, bool) {
	if s.closed.Load() {
		return 0, false
	}
	return s.db.GetIntPropertyCF(name, s.handle(cf))
}

// ApproxSizes returns the approximate on-disk size of each [lo, hi) range
// of cf (GetApproximateSizesCF). Both bounds must be set: an empty hi is
// the empty key, not an open bound.
func (s *Store) ApproxSizes(cf CF, ranges [][2][]byte) ([]uint64, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rs := make([]grocksdb.Range, len(ranges))
	for i, r := range ranges {
		rs[i] = grocksdb.Range{Start: r[0], Limit: r[1]}
	}
	return s.db.GetApproximateSizesCF(s.handle(cf), rs)
}

// Compact runs a blocking manual compaction of [lo, hi) on cf
// (CompactRangeCFOpt); a nil bound is open. It is non-exclusive, so
// automatic compactions and other manual ones keep running beside it, and
// the bottommost level is compacted too when cf has a compaction filter.
// RocksDB's C API reports no error.
func (s *Store) Compact(cf CF, lo, hi []byte) error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.db.CompactRangeCFOpt(s.handle(cf), grocksdb.Range{Start: lo, Limit: hi}, s.cro)
	return nil
}

// Flush flushes the memtables of cf to an SST file and waits for it.
func (s *Store) Flush(cf CF) error {
	if s.closed.Load() {
		return ErrClosed
	}
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	fo.SetWait(true)
	return s.db.FlushCF(s.handle(cf), fo)
}

// FlushWAL writes buffered WAL data to the OS and, with sync, fsyncs it.
// Under Fsync "everysec" the caller runs FlushWAL(true) once a second.
func (s *Store) FlushWAL(sync bool) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.db.FlushWAL(sync)
}

// Checkpoint writes an openable copy of the database into dir, which must
// not exist (CreateCheckpoint(dir, 0): hard links on the same filesystem).
// The C API returns no sequence number; read LatestSeq separately.
func (s *Store) Checkpoint(dir string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	cp, err := s.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()
	return cp.CreateCheckpoint(dir, 0)
}

// ExportCF exports the SST files of cf into dir, which must not exist
// (Checkpoint.ExportColumnFamily).
func (s *Store) ExportCF(cf CF, dir string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	cp, err := s.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()
	md, err := cp.ExportColumnFamily(s.handle(cf), dir)
	if err != nil {
		return err
	}
	md.Destroy()
	return nil
}

// Ingest adds external SST files to cf (IngestExternalFileCF) with default
// ingestion options: the files are copied and get a global sequence number.
func (s *Store) Ingest(cf CF, paths []string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	io := grocksdb.NewDefaultIngestExternalFileOptions()
	defer io.Destroy()
	return s.db.IngestExternalFileCF(s.handle(cf), paths, io)
}

// SetOption changes one mutable CF option (SetOptionsCF), for example
// disable_auto_compactions or write_buffer_size.
func (s *Store) SetOption(cf CF, name, value string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.db.SetOptionsCF(s.handle(cf), []string{name}, []string{value})
}

// LatestSeq returns the sequence number of the last committed write.
func (s *Store) LatestSeq() uint64 {
	if s.closed.Load() {
		return 0
	}
	return s.db.GetLatestSequenceNumber()
}

// CFNames returns the eight column family names in CF order.
func (s *Store) CFNames() []string {
	out := make([]string, NumCFs)
	copy(out, cfNames[:])
	return out
}

// Stats returns RocksDB's statistics dump (GetStatisticsString); ok is
// false unless Config.Statistics is on.
func (s *Store) Stats() (string, bool) {
	if !s.cfg.Statistics || s.closed.Load() {
		return "", false
	}
	return s.opts.db.GetStatisticsString(), true
}

// PendingCompactions returns how many ScheduleCompact calls are waiting
// for their timer.
func (s *Store) PendingCompactions() int { return s.compact.pending() }
