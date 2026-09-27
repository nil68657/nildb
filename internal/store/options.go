package store

import (
	"github.com/linxGnu/grocksdb"
)

// maxSubcompactions is the DB-wide max_subcompactions. The filter race
// test raises it to 4 before Open.
var maxSubcompactions uint32 = 2

// periodicCompaction revisits files older than a week on every CF that
// carries a compaction filter, so cold garbage gets filtered.
const periodicCompaction = 7 * 86400

// dbOptions holds every option object the open database refers to.
//
// The grocksdb Options objects are never destroyed. RocksDB takes
// ownership of merge operators and prefix extractors through shared_ptr,
// and grocksdb's Options.Destroy deletes them again, a double free. The
// cost is a few KiB of option structs per Open. Caches and the write
// buffer manager are reference counted and released after the database
// closes.
type dbOptions struct {
	db        *grocksdb.Options
	cf        []*grocksdb.Options
	bbto      []*grocksdb.BlockBasedTableOptions
	mainCache *grocksdb.Cache
	colCache  *grocksdb.Cache
	wbm       *grocksdb.WriteBufferManager
}

// newDBOptions builds the DB options and the eight CF options of
// architecture.md section 2.
func newDBOptions(cfg Config, meta *metaFilter, sub *subFilter, id *idFilter) *dbOptions {
	o := &dbOptions{
		mainCache: grocksdb.NewHyperClockCache(int(cfg.BlockCacheBytes), 0),
		colCache:  grocksdb.NewHyperClockCache(int(cfg.AnalyticsCacheBytes), 0),
		wbm:       grocksdb.NewWriteBufferManager(int(cfg.WriteBufferBytes), true),
	}

	db := grocksdb.NewDefaultOptions()
	db.SetCreateIfMissing(true)
	db.SetCreateIfMissingColumnFamilies(true)
	db.SetMaxBackgroundJobs(6)
	db.SetMaxSubcompactions(maxSubcompactions)
	db.SetBytesPerSync(1 << 20)
	db.SetMaxOpenFiles(-1)
	db.SetWALRecoveryMode(grocksdb.PointInTimeRecovery)
	db.SetMaxTotalWalSize(1 << 30)
	db.SetManualWALFlush(false)
	db.SetRateLimiter(grocksdb.NewGenericRateLimiter(cfg.BgIOBytesPerSec, 100_000, 10, grocksdb.RateLimiterModeAllIo, true))
	db.SetWriteBufferManager(o.wbm)
	if cfg.Statistics {
		db.EnableStatistics()
	}
	o.db = db

	o.cf = make([]*grocksdb.Options, NumCFs)
	for cf := range CF(NumCFs) {
		opts := grocksdb.NewDefaultOptions()
		opts.SetWriteBufferManager(o.wbm)
		opts.SetLevelCompactionDynamicLevelBytes(true)
		opts.SetCompactionPri(grocksdb.KMinOverlappingRatioCompactionPri)

		if cf == CFCol {
			o.colTable(opts)
		} else {
			o.rowTable(opts, cf != CFZScore && cf != CFGeo)
		}

		switch cf {
		case CFDefault:
			opts.SetWriteBufferSize(4 << 20)
			opts.SetCompression(grocksdb.LZ4Compression)
			opts.SetMergeOperator(i64Add{})
			opts.SetMaxSuccessiveMerges(64)
		case CFMeta:
			opts.SetWriteBufferSize(64 << 20)
			opts.SetCompactionFilter(meta)
		case CFSub:
			opts.SetWriteBufferSize(64 << 20)
			opts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(8))
			opts.SetMemTablePrefixBloomSizeRatio(0.1)
			opts.SetCompactionFilter(sub)
		case CFZScore:
			opts.SetWriteBufferSize(32 << 20)
			opts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(8))
			opts.SetMemTablePrefixBloomSizeRatio(0.1)
			opts.SetCompactionFilter(sub)
		case CFDoc:
			opts.SetWriteBufferSize(64 << 20)
			opts.SetCompactionFilter(id)
		case CFIdx:
			opts.SetWriteBufferSize(32 << 20)
			opts.SetCompactionFilter(id)
		case CFGeo:
			opts.SetWriteBufferSize(16 << 20)
			opts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(12))
			opts.SetMemTablePrefixBloomSizeRatio(0.1)
			opts.SetCompactionFilter(id)
		case CFCol:
			opts.SetWriteBufferSize(64 << 20)
			opts.SetCompression(grocksdb.ZSTDCompression)
			opts.SetBottommostCompression(grocksdb.ZSTDCompression)
			opts.SetCompactionFilter(id)
		}
		if cf != CFDefault && cf != CFCol {
			opts.SetCompressionPerLevel([]grocksdb.CompressionType{
				grocksdb.NoCompression, grocksdb.NoCompression,
				grocksdb.LZ4Compression, grocksdb.LZ4Compression,
				grocksdb.LZ4Compression, grocksdb.LZ4Compression,
				grocksdb.ZSTDCompression,
			})
		}
		if cf != CFDefault {
			opts.SetPeriodicCompactionSeconds(periodicCompaction)
		}
		o.cf[cf] = opts
	}
	return o
}

// rowTable installs the row-store table options: 16 KiB blocks in the main
// cache, pinned index and filter blocks, and a Ribbon filter.
func (o *dbOptions) rowTable(opts *grocksdb.Options, wholeKey bool) {
	t := grocksdb.NewDefaultBlockBasedTableOptions()
	t.SetBlockSize(16 << 10)
	t.SetBlockCache(o.mainCache)
	t.SetCacheIndexAndFilterBlocks(true)
	t.SetPinL0FilterAndIndexBlocksInCache(true)
	t.SetPinTopLevelIndexAndFilter(true)
	t.SetFormatVersion(6)
	t.SetOptimizeFiltersForMemory(true)
	t.SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(10, 1))
	t.SetWholeKeyFiltering(wholeKey)
	opts.SetBlockBasedTableFactory(t)
	o.bbto = append(o.bbto, t)
}

// colTable installs the columnar table options: 64 KiB blocks in the
// analytics cache, pinned index blocks, no filter.
func (o *dbOptions) colTable(opts *grocksdb.Options) {
	t := grocksdb.NewDefaultBlockBasedTableOptions()
	t.SetBlockSize(64 << 10)
	t.SetBlockCache(o.colCache)
	t.SetCacheIndexAndFilterBlocks(true)
	t.SetPinL0FilterAndIndexBlocksInCache(true)
	t.SetPinTopLevelIndexAndFilter(true)
	t.SetFormatVersion(6)
	opts.SetBlockBasedTableFactory(t)
	o.bbto = append(o.bbto, t)
}

// release drops NilDB's references to the caches and the write buffer
// manager. Call it only after the database has closed (or failed to open).
func (o *dbOptions) release() {
	if o == nil {
		return
	}
	if o.mainCache != nil {
		o.mainCache.Destroy()
		o.mainCache = nil
	}
	if o.colCache != nil {
		o.colCache.Destroy()
		o.colCache = nil
	}
	if o.wbm != nil {
		o.wbm.Destroy()
		o.wbm = nil
	}
}
