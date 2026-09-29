package store

import (
	"fmt"
	"strconv"

	"github.com/linxGnu/grocksdb"
)

// rocksKV is RocksDB through grocksdb: one instance with the eight column
// families and options of architecture.md section 2, the three compaction
// filters, the nildb.i64add merge operator and the rate_limiter_priority
// shim.
type rocksKV struct {
	db       *grocksdb.DB
	cfs      [NumCFs]*grocksdb.ColumnFamilyHandle
	opts     *dbOptions
	ro       *grocksdb.ReadOptions         // shared by live point reads
	wo       *grocksdb.WriteOptions        // unsynced commits
	swo      *grocksdb.WriteOptions        // synced commits and the store's own records
	cro      *grocksdb.CompactRangeOptions // manual compactions, non-exclusive
	metaF    *metaFilter
	subF     *subFilter
	idF      *idFilter
	readOnly bool
	stat     bool
}

// rocksSnap is a RocksDB snapshot and the read options that carry it.
type rocksSnap struct {
	snap *grocksdb.Snapshot
	ro   *grocksdb.ReadOptions
}

func openRocks(cfg Config, readOnly bool, s *Store) (*rocksKV, error) {
	clock := cfg.Clock
	nowMS := func() int64 { return clock().UnixMilli() }
	k := &rocksKV{
		metaF:    &metaFilter{now: nowMS},
		subF:     &subFilter{now: nowMS},
		idF:      &idFilter{live: &s.live},
		readOnly: readOnly,
		stat:     cfg.Statistics,
	}
	o := newDBOptions(cfg, k.metaF, k.subF, k.idF)
	var (
		handles []*grocksdb.ColumnFamilyHandle
		err     error
	)
	if readOnly {
		k.db, handles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(o.db, cfg.Dir, cfNames[:], o.cf, false)
	} else {
		k.db, handles, err = grocksdb.OpenDbColumnFamilies(o.db, cfg.Dir, cfNames[:], o.cf)
	}
	if err != nil {
		o.release()
		return nil, fmt.Errorf("store: open %s: %w", cfg.Dir, err)
	}
	copy(k.cfs[:], handles)
	k.opts = o
	k.ro = grocksdb.NewDefaultReadOptions()
	k.wo = grocksdb.NewDefaultWriteOptions()
	k.swo = grocksdb.NewDefaultWriteOptions()
	k.swo.SetSync(true)
	k.cro = grocksdb.NewCompactRangeOptions()
	k.cro.SetExclusiveManualCompaction(false)
	return k, nil
}

func (k *rocksKV) name() string { return EngineRocksDB }

func (k *rocksKV) version() string {
	major, minor, patch := Version()
	return strconv.Itoa(major) + "." + strconv.Itoa(minor) + "." + strconv.Itoa(patch)
}

func (k *rocksKV) h(cf CF) *grocksdb.ColumnFamilyHandle {
	if int(cf) >= NumCFs {
		panic(fmt.Sprintf("store: unknown column family %d", cf))
	}
	return k.cfs[cf]
}

func (k *rocksKV) handle(cf CF) any { return k.h(cf) }

func (k *rocksKV) readOpts(snap kvSnap) *grocksdb.ReadOptions {
	if snap == nil {
		return k.ro
	}
	return snap.(*rocksSnap).ro
}

func (k *rocksKV) writeOpts(sync bool) *grocksdb.WriteOptions {
	if sync {
		return k.swo
	}
	return k.wo
}

func (k *rocksKV) get(snap kvSnap, cf CF, key []byte) ([]byte, bool, error) {
	return getCopy(k.db, k.readOpts(snap), k.h(cf), key)
}

func (k *rocksKV) getPinned(snap kvSnap, cf CF, key []byte) (pinned, error) {
	p, err := k.db.GetPinnedCF(k.readOpts(snap), k.h(cf), key)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (k *rocksKV) multiGet(snap kvSnap, cf CF, keys [][]byte) ([][]byte, error) {
	return multiGet(k.db, k.readOpts(snap), k.h(cf), keys)
}

func (k *rocksKV) iter(snap kvSnap, cf CF, lo, hi []byte, o IterOpts) Iterator {
	var rs *grocksdb.Snapshot
	if snap != nil {
		rs = snap.(*rocksSnap).snap
	}
	return k.newIterator(cf, lo, hi, o, rs, nil)
}

// rocksBatch is a plain WriteBatch.
type rocksBatch struct {
	k  *rocksKV
	wb *grocksdb.WriteBatch
}

func (k *rocksKV) newBatch() kvBatch { return &rocksBatch{k: k, wb: grocksdb.NewWriteBatch()} }

func (b *rocksBatch) put(cf CF, key, val []byte)       { b.wb.PutCF(b.k.h(cf), key, val) }
func (b *rocksBatch) delete(cf CF, key []byte)         { b.wb.DeleteCF(b.k.h(cf), key) }
func (b *rocksBatch) merge(cf CF, key, operand []byte) { b.wb.MergeCF(b.k.h(cf), key, operand) }
func (b *rocksBatch) deleteRange(cf CF, lo, hi []byte) { b.wb.DeleteRangeCF(b.k.h(cf), lo, hi) }
func (b *rocksBatch) destroy()                         { b.wb.Destroy() }

func (k *rocksKV) write(b kvBatch, sync bool) error {
	return k.db.Write(k.writeOpts(sync), b.(*rocksBatch).wb)
}

func (k *rocksKV) beginIndexed(s *Store) Txn { return &indexedTxn{s: s, k: k} }

func (k *rocksKV) snapshot() (kvSnap, uint64, error) {
	snap := k.db.NewSnapshot()
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(snap)
	return &rocksSnap{snap: snap, ro: ro}, snap.GetSequenceNumber(), nil
}

func (k *rocksKV) release(snap kvSnap) {
	rs := snap.(*rocksSnap)
	k.db.ReleaseSnapshot(rs.snap)
	rs.ro.Destroy()
}

func (k *rocksKV) property(cf CF, name string) (string, bool) {
	v := k.db.GetPropertyCF(name, k.h(cf))
	return v, v != ""
}

func (k *rocksKV) intProperty(cf CF, name string) (uint64, bool) {
	return k.db.GetIntPropertyCF(name, k.h(cf))
}

func (k *rocksKV) approxSizes(cf CF, ranges [][2][]byte) ([]uint64, error) {
	rs := make([]grocksdb.Range, len(ranges))
	for i, r := range ranges {
		rs[i] = grocksdb.Range{Start: r[0], Limit: r[1]}
	}
	return k.db.GetApproximateSizesCF(k.h(cf), rs)
}

// compact reports no error: RocksDB's C API has none for CompactRangeCF.
func (k *rocksKV) compact(cf CF, lo, hi []byte) error {
	k.db.CompactRangeCFOpt(k.h(cf), grocksdb.Range{Start: lo, Limit: hi}, k.cro)
	return nil
}

func (k *rocksKV) flush(cf CF) error {
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	fo.SetWait(true)
	return k.db.FlushCF(k.h(cf), fo)
}

func (k *rocksKV) flushWAL(sync bool) error { return k.db.FlushWAL(sync) }

func (k *rocksKV) checkpoint(dir string) error {
	cp, err := k.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()
	return cp.CreateCheckpoint(dir, 0)
}

func (k *rocksKV) exportCF(cf CF, dir string) error {
	cp, err := k.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()
	md, err := cp.ExportColumnFamily(k.h(cf), dir)
	if err != nil {
		return err
	}
	md.Destroy()
	return nil
}

func (k *rocksKV) ingest(cf CF, paths []string) error {
	io := grocksdb.NewDefaultIngestExternalFileOptions()
	defer io.Destroy()
	return k.db.IngestExternalFileCF(k.h(cf), paths, io)
}

func (k *rocksKV) setOption(cf CF, name, value string) error {
	return k.db.SetOptionsCF(k.h(cf), []string{name}, []string{value})
}

func (k *rocksKV) latestSeq() uint64 { return k.db.GetLatestSequenceNumber() }

func (k *rocksKV) stats() (string, bool) {
	if !k.stat {
		return "", false
	}
	return k.opts.db.GetStatisticsString(), true
}

func (k *rocksKV) shimActive() (bool, error) { return ShimActive() }

func (k *rocksKV) sweeps() bool { return false }

func (k *rocksKV) bind(s *Store) {
	if k.readOnly {
		return
	}
	k.metaF.bound.Store(true)
	k.subF.bind(k.db, k.cfs[CFMeta])
	k.idF.bound.Store(true)
}

func (k *rocksKV) stopBackground() {
	if !k.readOnly {
		k.db.DisableManualCompaction()
	}
}

// close unbinds the filters and closes RocksDB. It frees the option objects
// NilDB owns; see dbOptions for the ones it leaks on purpose. RocksDB's C
// API reports no error from close.
func (k *rocksKV) close() error {
	k.metaF.bound.Store(false)
	k.idF.bound.Store(false)
	k.subF.unbind()
	if !k.readOnly {
		k.db.CancelAllBackgroundWork(true)
	}
	for i, h := range k.cfs {
		if h != nil {
			h.Destroy()
			k.cfs[i] = nil
		}
	}
	k.db.Close()
	k.subF.destroy()
	for _, o := range []*grocksdb.WriteOptions{k.wo, k.swo} {
		if o != nil {
			o.Destroy()
		}
	}
	k.wo, k.swo = nil, nil
	if k.ro != nil {
		k.ro.Destroy()
		k.ro = nil
	}
	if k.cro != nil {
		k.cro.Destroy()
		k.cro = nil
	}
	k.opts.release()
	return nil
}
