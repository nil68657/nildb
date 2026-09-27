# HTAP on top of RocksDB for NilDB: definitions and requirements, TiDB/TiFlash, SingleStore, LSM secondary indexing (Luo & Carey), Redis-protocol precedents (Kvrocks, Dragonfly, Redis Search), embedded query engines (DataFusion, Arrow Go, DuckDB), and the RocksDB knobs that make snapshot-consistent, throttled analytical scans possible alongside point writes

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:htap-on-kv-store`, agent `a34208abb89baadbb`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Define NilDB's HTAP contract in the two canonical terms from the literature: freshness (max timestamp gap between an analytical read and the last committed write; target 0 for same-snapshot reads since both live in one RocksDB) and isolation (percent OLTP throughput/latency degradation while an analytical scan runs; measure it with a CH-benCHmark-style mixed run and report the TiDB-style numbers).
2. Pick the 'unified single store' architecture, not a replica: with one embedded RocksDB there is no Raft learner or log shipping, so freshness is free (a RocksDB snapshot at sequence S sees every write <= S across all column families). The cost moves entirely to isolation, which is the part of the design that needs engineering: rate limiter, cache separation, short-lived snapshots.
3. Row store + column-oriented secondary column family updated in the same WriteBatch is sound and cheap in RocksDB: CFs share the WAL and a WriteBatch spanning CFs is atomic (Column Families wiki), Kvrocks already does metadata+subkey this way, and the checkpoint API can flush all CFs atomically. Keep WAL enabled so atomic_flush is unnecessary. Do not copy Kvrocks Search's habit of writing index entries in a separate batch; that is exactly the stale-index problem Luo & Carey describe.
4. Use the Luo & Carey vocabulary for the columnar/secondary CF maintenance policy: NilDB should start with sync-full (old value read, old index entry deleted, new entry inserted, all in the row write's batch) because the old row is already being read for Redis semantics (HSET returns field count, GEOADD needs old member). Defer sync-insert plus query-time validation to a later optimization if ingest cost dominates; keep a version/timestamp in each index entry so validation is possible.
5. Layout for the analytical CF: key = collection | field | primary key (composite-key scheme), value = the field's encoded scalar, so a projection of k fields is k prefix range scans that read only those bytes; RocksDB wide columns cannot project, so do not use PutEntity for this. Keep the row (document) in its own CF for point ops. Segment-level min/max statistics (SingleStore, TiFlash stable chunks) can be approximated later with SST table properties or a per-range stats CF; they are not needed for a basic HTAP.
6. Snapshot-consistent analytics: each pipeline run takes one db.GetSnapshot(), opens all iterators (NewIterators or per-CF iterators with ReadOptions.snapshot), and releases it as soon as the scan finishes. Bound snapshot lifetime (timeout, or checkpoint + read-only open for very long jobs) because held snapshots block compaction from dropping overwritten keys and can stall writes.
7. Isolation knobs, all native RocksDB and all exposable in NilDB's config so the 'RocksDB feel' is kept: (1) NewGenericRateLimiter with Mode kAllIo and analytical ReadOptions.rate_limiter_priority = IO_LOW, leaving OLTP reads at IO_TOTAL (uncharged); (2) analytical ReadOptions.fill_cache=false, iterate_upper_bound set, auto readahead on, async_io on; (3) a separate, smaller BlockBasedTableOptions.block_cache for the columnar CF via its own table_factory, and a large high_pri_pool_ratio on the row CF cache; (4) optionally run heavy analytics in a separate process with OpenAsSecondary + TryCatchUpWithPrimary (no snapshots there, so use checkpoints for consistency); (5) WriteOptions.low_pri for bulk loads.
8. Query surface: implement a MongoDB-style stage pipeline ($match, $project, $group, $sort, $limit, $skip, $count, $geoNear) exposed over RESP (e.g. NIL.AGGREGATE coll <json-pipeline>) and mirror RediSearch's FT.AGGREGATE grammar (GROUPBY/REDUCE/SORTBY MAX/APPLY/FILTER/LIMIT/WITHCURSOR) as a second front-end over the same planner. Copy MongoDB's optimizer rules (match-first, sort+limit top-k with limit+skip, merge adjacent match/limit/skip) and RediSearch's SORTBY MAX and cursor semantics; both are documented precisely enough to implement without ambiguity.
9. Language: prefer Rust. The rocksdb crate 0.25.0 bundles RocksDB 11.8.1 with no system install, and DataFusion 55.1.0 provides a complete Arrow-native SQL/aggregation engine into which a RocksDB TableProvider (projection, filter, limit pushdown; blocking scan offloaded with spawn_blocking) plugs in directly. Go would need grocksdb (system librocksdb via Homebrew, cgo) and either a hand-written group-by/sort engine (arrow-go compute has no hash aggregates) or DuckDB via duckdb-go (cgo, table UDFs). Go remains viable if the team values it, but the Rust path has fewer moving parts for the HTAP half.
10. Reference numbers to design against: TiDB tolerates <=10% TP degradation and sub-second freshness; SingleStore avoids tombstone reconciliation on analytical reads with delete bitmaps; TiFlash pays 3.4x write amplification for a 2x faster columnar read path. For NilDB, expect the columnar CF to roughly double write amplification (two CFs per row write) and plan the rate limiter budget accordingly.
11. Expose RocksDB natively rather than hiding it: commands for CF listing, snapshot creation/release, checkpoint creation (with the checkpoint atomic_flush flag), manual compaction, property reads (rocksdb.stats, cache usage) map cleanly onto the wiki-documented APIs and satisfy the 'native RocksDB feel' requirement.
12. Geo fits the same secondary-CF pattern: Luo & Carey report LSM R-trees and grid/space-filling-curve B+-tree variants both work over LSM, and Redis/RediSearch already express geo filters (@loc:[lon lat r km], geodistance APPLY) inside the aggregation pipeline, so $geoNear/$geoWithin can be pipeline stages backed by an S2/geohash-keyed CF written in the same batch as the document.

## Open questions

- How large may the delta between 'basic' and 'real' HTAP be for this project: is a single-process scan with rate limiting and snapshots enough, or is the secondary-instance (separate process) mode a requirement from day one?
- Will the analytical CF be a full copy of every field (TiFlash-style) or only user-declared indexed fields (Kvrocks/RediSearch-style FT.CREATE schema)? The second halves write amplification and is what every Redis-protocol precedent does; the first makes ad-hoc SQL over any field possible.
- How should aggregation state be bounded (RediSearch documents COUNT_DISTINCT memory blow-up and MongoDB caps sorts at 100 MB)? A spill-to-disk or hard-limit policy needs deciding before implementing $group and SORTBY.
- Compaction filter and TTL: Redis EXPIRE on a key must also drop its columnar and geo index entries; Kvrocks solves it with per-key versions cleaned at compaction. Does NilDB adopt the same version-in-key scheme or delete index entries eagerly?
- If Rust and DataFusion are chosen, does the Redis protocol layer run on the same tokio runtime as DataFusion, and how are blocking RocksDB iterators isolated (spawn_blocking pool size, per-query thread cap) so analytics cannot starve command handling?
- Should NilDB expose SQL at all (DataFusion makes it nearly free; Kvrocks added FT.SEARCHSQL) or stay with the pipeline/RediSearch grammar only? This affects how much of the DataFusion planner is exposed versus wrapped.
- No published benchmark measures HTAP isolation for a single-node embedded store; which workload (CH-benCHmark subset, HATtrick-style, or a custom Redis+aggregation mix) will define NilDB's acceptance numbers?
- The SingleStore delete-bitmap idea (no tombstone reconciliation on scans) does not map directly onto RocksDB, where obsolete versions are hidden by sequence numbers during iteration; is the RocksDB iterator merge cost acceptable for the analytical CF, or would periodic manual compaction of that CF be needed to keep scans fast?

## Findings (37)

### 1. HTAP was coined by Gartner in 2014 as an application architecture that runs analytics and transactions concurrently on the same in-memory store; Gartner widened it in 2018 to 'In-Process HTAP', dropping the in-memory requirement.

Li & Zhang, SIGMOD 2022 tutorial 'HTAP Databases: What is New and What is Next', section 1: Gartner report [35] 2014 defines HTAP over an in-memory data store and eliminates ETL; in 2018 Gartner extended it to In-Process HTAP so 'HTAP is no longer limited to in-memory computing techniques'.

- Source: https://dbgroup.cs.tsinghua.edu.cn/ligl/papers/sigmod22-tutorial-paper.pdf
- Confidence: verified; design-critical: no

### 2. The two canonical extra requirements of an HTAP system beyond OLTP+OLAP throughput are data freshness and performance isolation; co-running analytics on a single in-memory engine costs 3x-5x OLTP throughput.

TiDB paper, section 1: 'HTAP systems need to efficiently read the latest data ... under two additional requirements: freshness and isolation'. Freshness = how recent the data seen by analytics is; isolation = guaranteed isolated performance for OLTP and OLAP. Cites CH-benCHmark study [34]: co-running analytics cut SAP HANA OLTP throughput by at least 3x and HyPer by at least 5x; similar for MemSQL.

- Source: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
- Confidence: verified; design-critical: yes

### 3. Freshness and isolation are measured as (a) the max timestamp gap between what OLAP and OLTP clients see, or the replication lag, and (b) throughput degradation of hybrid vs sequential execution; the trade-off is real: TiDB degrades <=10% but lags up to 1000 ms, HyPer's fork snapshot takes microseconds but degrades up to 40%.

'HTAP Databases: A Survey' (arXiv 2404.15670), section 2.2: data freshness is 'the lag time between the analytics and transactions', quantified as the maximum timestamp difference between OLAP and OLTP result sets or as replication latency; performance isolation is how well the system reduces OLTP/OLAP interference, quantified by throughput degradation percentage or by comparing hybrid vs sequential execution. Same section gives the TiDB (<=10%, up to 1000 ms) and HyPer (microseconds snapshot, up to 40% degradation) numbers. Benchmarks named: CH-benCHmark, HTAPBench, OLxPBench, HATtrick, HyBench.

- Source: https://arxiv.org/pdf/2404.15670
- Confidence: verified; design-critical: yes

### 4. HTAP storage architectures fall into four classes with opposite isolation/freshness profiles; the 'row store + column replica' class (TiDB, SingleStore) scores High isolation / Low freshness, while 'primary row store + in-memory column store' (Oracle, SQL Server, DB2 BLU) scores Low isolation / High freshness.

Li & Zhang tutorial, Table 1: (a) Primary Row Store + In-Memory Column Store: Oracle Dual-Format, SQL Server, DB2 BLU: TP High, AP High, Isolation Low, Freshness High. (b) Distributed Row Store + Column Store Replica: TiDB, SingleStore: Isolation High, Freshness Low. (c) Disk Row Store + Distributed Column Store: MySQL Heatwave: Isolation High, Freshness Medium. (d) Primary Column Store + Delta Row Store: SAP HANA: Isolation Low, Freshness High. Section 2.1(b): TiDB replicates Raft logs to learner nodes storing columnar data, so isolation is high but 'data freshness is low since newly-updated data may have not been merged to the column store'. Table 2 lists technique families: MVCC+logging vs 2PC+Raft+logging; in-memory delta+column scan vs log-based delta+column scan; in-memory delta merge, log-based delta merge, rebuild from primary row store; column selection; hybrid row/column scan; workload-driven and freshness-driven scheduling.

- Source: https://dbgroup.cs.tsinghua.edu.cn/ligl/papers/sigmod22-tutorial-paper.pdf
- Confidence: verified; design-critical: yes

### 5. The 2017 SIGMOD HTAP survey classifies systems by (1) single engine vs separate OLTP/OLAP engines and (2) same vs separate data organization, and notes that any row-to-column conversion delays visibility of committed data to analytics.

Özcan, Tian, Tözün, 'Hybrid Transactional/Analytical Processing: A Survey', section 2: 2.1 Single System (2.1.1 separate data organization, e.g. SAP HANA, TimesTen, MemSQL row-in-memory/column-on-disk; 2.1.2 same organization, e.g. H2TAP, Hive ACID on ORC, Impala+Kudu) and 2.2 Separate systems (2.2.1 decoupled storage via ETL, 2.2.2 shared storage). Text: 'All these systems require converting the data between row and columnar formats ... the latest committed data might not be available to the analytical queries right away'.

- Source: https://pages.cs.wisc.edu/~yxy/cs839-s20/papers/htap-survey.pdf
- Confidence: verified; design-critical: no

### 6. TiDB gets a columnar replica (TiFlash) by adding a non-voting Raft learner per Region that asynchronously receives the leader's log, and replays it in three steps: compact (drop rolled-back prewrites), decode into row tuples, transform to columnar when the row buffer exceeds a size or time limit.

TiDB paper sections 2 and 4.2: learners 'do not participate in the Raft protocols to commit logs or elect leaders so they induce little overhead on TiKV'; ALTER TABLE x SET TIFLASH REPLICA n; TiFlash partitions cover several contiguous Regions; 4.2.1 Log Replayer: (1) compacting logs by transaction status prewritten/committed/rollbacked, (2) decoding tuples into a row buffer, (3) transforming to columnar 'if the data size in the row buffer exceeds a size limit or its time duration exceeds a time interval limit'. Schema synced with a per-learner schema cache (regular + compulsive sync).

- Source: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
- Confidence: verified; design-critical: yes

### 7. TiFlash's Delta Tree keeps a row-format delta space (appended, WAL-like small files, periodically compacted) and a columnar stable space (chunks, column-by-column, Parquet-like, LZ4) with a B+ tree over the delta ordered by key and timestamp; it reads about 2x faster than an LSM tree at the cost of 3.4x higher write amplification.

TiDB paper section 4.2.3 and Table 2: delta updates are 'cached in memory and materialized into disks ... in many small files', later compacted; stable space stores row groups column by column with per-file column metadata; a B+ tree indexes delta by key and timestamp to merge with stable; DeltaTree read 0.49 s vs LSM 1.01 s at 100M tuples (0.71 vs 1.59 s at 200M), stable across 150-14000 TPS; 'write amplification of DeltaTree (16.11) is greater than the LSM tree (4.74)'. The LSM baseline used ClickHouse-style universal compaction.

- Source: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
- Confidence: verified; design-critical: yes

### 8. TiFlash consistency is enforced at read time: the learner sends a read-index request to the Region leader for the requested timestamp, waits until the leader's logs covering that timestamp are received and replayed, then serves the read, giving snapshot isolation at a timestamp (learner read).

TiDB paper section 4.2.4 'Read Process': 'learner nodes provide snapshot isolation so we can read data from TiFlash at a specific timestamp. After receiving a read request, the learner sends a read index request to its leaders to get the newest data that covers the requested timestamp ... Once the logs are written into DeltaTree, the specific data ... is read'. Section 4.1.2 defines read index and lease read. TiFlash docs (v8.5) describe the same: region replicas send a lightweight progress RPC to the leader before reads to reach Snapshot Isolation.

- Source: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
- Confidence: verified; design-critical: yes

### 9. TiDB achieves isolation by putting TiKV and TiFlash on separate servers, capping analytical table scans on TiKV at 500 MB, and letting a cost-based optimizer pick row scan, index scan or column scan; measured TP throughput drops at most ~10% (5% for latency-sensitive runs) under concurrent AP, and replication delay is <300 ms (mostly <100 ms) at 10 warehouses and mostly <1000 ms at 100 warehouses.

TiDB paper section 5.3: 'we schedule analytical and transactional queries on different engine servers, and deploy TiKV and TiFlash on separate servers'; cost equations (1)-(5) for row/column/index scans; 'limit the direct access table size on TiKV for analytical queries to at most 500 MB'. Section 6.4: 'more analytical processing clients degrade the TP throughput at most 10%'; AP throughput drops at most 5% with more TP clients. Section 6.5 / Table 4: at 10 warehouses, AC=2/TC=64, 95.98% of replication delays <100 ms, 99.62% <1000 ms; at 100 warehouses, AC=32/TC=1024, 48.77% <100 ms, 87.17% <1000 ms. MemSQL 7.0 in the same test lost >5x TP throughput as AP clients grew.

- Source: https://www.vldb.org/pvldb/vol13/p3072-huang.pdf
- Confidence: verified; design-critical: yes

### 10. Current TiFlash docs (TiDB v8.5) still describe the same design: Raft learner replication so TiKV keeps working if TiFlash is down, Snapshot Isolation via a progress check with the leader before each read, optimizer choosing TiFlash/TiKV/both by cost, and a recommendation to deploy TiFlash on separate nodes for workload isolation.

docs.pingcap.com tiflash-overview (v8.5 Self-Managed): asynchronous replication 'as a special role, Raft Learner'; consistency 'Snapshot Isolation' where region replicas send lightweight progress RPCs to the leader before reads; 'TiDB can automatically choose to use TiFlash (column-wise) or TiKV (row-wise), or use both' by read cost; deploy on separate nodes 'to ensure workload isolation'.

- Source: https://docs.pingcap.com/tidb/stable/tiflash-overview
- Confidence: verified; design-critical: no

### 11. SingleStore's unified (universal) table storage is an LSM tree whose level 0 is an in-memory rowstore (analogous to RocksDB's memtable) and whose lower levels are immutable columnstore segments; a background flusher converts rowstore rows into a segment inside one transaction, and reads must use partition-local snapshot isolation because the merger and flusher move rows between structures concurrently.

Prout et al., 'Cloud-Native Transactions and Analytics in SingleStore' (SIGMOD 2022), section 2.1.2: 'For each columnstore table, S2DB creates a rowstore table as a write-optimized store to store small writes ... This corresponds to the level 0 storage in other LSM trees, like MemTable in RocksDB'; 'A background flusher process periodically deletes rows from the rowstore and converts those rows into a columnstore segment in a transaction'; 'reads need to use partition-local snapshot isolation to guarantee a consistent view of the table'. Segments store each column in row order, compressed separately (bit packing, dictionary, RLE, LZ4), with min/max per column in an in-memory rowstore metadata table for segment elimination; sorted runs kept logarithmic by a background merger.

- Source: https://assets.contentstack.io/v3/assets/bltac01ee6daa3a1e14/bltd65ad78edbf73a98/677bcb6fc0e3a4546ec6e72c/research-paper-cloud-native-transactions-and-analytics-in-singlestore.pdf
- Confidence: verified; design-critical: yes

### 12. SingleStore deliberately avoids LSM tombstones for the columnstore: deletes flip a bit in a per-segment delete bit vector in metadata, so analytical reads never reconcile across LSM levels (TPC-H Q1 can run at 8.6 clock cycles per row on encoded data); updates modify segment metadata plus the in-memory store.

Same paper, section 4 'Unified table storage': 'Common LSM tree implementations, such as RocksDB, Cassandra, and BigTable, use tombstone entries ... reads need to reconcile results from all LSM levels ... S2DB represents deletes using a bit vector stored as part of the segment metadata'; 'for update and delete operations S2DB needs to modify the segment metadata in addition to the in-memory store'. Encodings are seekable so point reads decode only the needed row offset. Secondary indexes are two-level: per-segment inverted index plus a global hash-table LSM index that stores only hashes and handles segment deletion lazily.

- Source: https://assets.contentstack.io/v3/assets/bltac01ee6daa3a1e14/bltd65ad78edbf73a98/677bcb6fc0e3a4546ec6e72c/research-paper-cloud-native-transactions-and-analytics-in-singlestore.pdf
- Confidence: verified; design-critical: yes

### 13. SingleStore product docs describe Universal Storage as columnstore plus five OLTP features (hash indexes with optional uniqueness, subsegment access, row-level locking, selective joins, upserts); queries scan both the in-memory rowstore segment and columnstore segments; column segments hold on the order of hundreds of thousands of rows and carry a deleted-row bitmask.

docs.singlestore.com Universal Storage page lists the five features; 'How the Columnstore Works' (v9.0) says metadata per row segment includes total row count and 'a bitmask tracking which rows have been deleted', segments 'typically contain on the order of hundreds of thousands of rows', sorted segment groups have no overlapping key ranges, and queries scan 'both the in-memory rowstore segment and columnstore segments'.

- Source: https://docs.singlestore.com/db/v9.0/create-a-database/columnstore/how-the-columnstore-works/
- Confidence: verified; design-critical: no

### 14. Luo & Carey's LSM survey (arXiv v3, the VLDB Journal article) says the core difficulty of LSM secondary indexes is cleanup on update: inserting the new entry is cheap, removing the old one needs a point lookup; the four Diff-Index schemes (sync-full, sync-insert, async-simple, async-session) and the DELI / validation strategies trade ingestion cost for query-time validation.

Section 3.7.2: 'a secondary key value can change during an update. Extra work must be performed to clean up obsolete entries'. Diff-Index: sync-full does insert+cleanup synchronously (indexes always fresh, high ingestion overhead from point lookups); sync-insert only inserts, queries clean lazily; async-simple queues updates; async-session adds client-side session consistency. DELI cleans stale entries while scanning/merging the primary index, so 'queries must always validate search results by fetching records from the primary index' and index-only queries are not efficient. Luo & Carey [47]: keep a primary-key index (keys + timestamps) so queries validate secondary hits by timestamp and background cleanup avoids reading full records; batched lookups are the most effective point-lookup optimization. Filters on a key must be maintained from both old and new record on upsert or queries prune wrongly (Fig. 16).

- Source: https://arxiv.org/pdf/1812.07527
- Confidence: verified; design-critical: yes

### 15. The same survey covers two LSM secondary-index layouts (composite key = secondary key + primary key, vs key lists) and LSM spatial indexes (LSM R-tree, DHB-tree, DHVB-tree, SHB-tree, SIF), concluding the LSM R-tree 'performs reasonably well for both ingestion and query workloads', and that the final primary-key lookup step usually dominates non-index-only queries; it has no section on columnar analytics over LSM.

Section 3.7.1: composite-key vs key-list (eager/lazy) schemes; Kim et al. study of LSM spatial indexes with the R-tree conclusion; 'for non-index-only queries, the final primary key lookup step is generally dominant since it often requires a separate disk I/O for each primary key'. Section 4.2 covers RocksDB (leveling with optional tiering at L0, dynamic level sizing so <=10% space waste). A grep of the full text found no section on columnar layouts or analytics over LSM.

- Source: https://arxiv.org/pdf/1812.07527
- Confidence: verified; design-critical: no

### 16. RocksDB column families share one WAL and one WriteBatch can write to several column families atomically; RocksDB also guarantees a consistent view across column families and offers DB::NewIterators to open iterators on several CFs at one consistent point.

RocksDB wiki Column Families: 'atomically execute Write({cf1, key1, value1}, {cf2, key2, value2})'; CFs 'share the WAL' (a new WAL is created when any CF flushes; old WAL stays until all CFs flushed past it); 'consistent view of the database across Column Families'; DB::NewIterators creates iterators on multiple CFs together; each CF has its own ColumnFamilyOptions (memtable, table files, compaction).

- Source: https://github.com/facebook/rocksdb/wiki/Column-Families
- Confidence: verified; design-critical: yes

### 17. atomic_flush is only needed when some column-family writes bypass the WAL; with WAL on, recovery already restores a cross-CF consistent state. Auto-triggered atomic flush flushes all CFs, and CreateCheckpoint has its own atomic_flush flag to capture a cross-CF consistent checkpoint.

RocksDB wiki Atomic flush: all-or-nothing flush of several CFs; 'it is not necessary to use the Atomic flush option if WAL is always enabled'; cannot be changed after open. options.h (main, lines ~1600-1610): 'This option is useful when there are column families with writes NOT protected by WAL ... For auto-triggered flush, RocksDB atomically flushes ALL column families'. CheckpointOptions (options.h ~3368-3374): 'use atomic flush to flush all column families atomically ... ensures that the checkpoint captures a consistent view across all column families'.

- Source: https://github.com/facebook/rocksdb/wiki/Atomic-flush
- Confidence: verified; design-critical: yes

### 18. A RocksDB snapshot is a sequence number; reads with ReadOptions.snapshot see only entries <= that sequence, across all CFs of the DB. Holding many snapshots prevents compaction from dropping overwritten keys and can cause write stalls, so analytical snapshots must be short-lived or few.

RocksDB wiki Snapshot: snapshot 'associated with an internal sequence number'; visibility rule; 'DB will preserve the point-in-time view at the specified sequence number' during flush/compaction; hundreds of thousands of snapshots 'significantly slow down flush/compaction to the point of causing write stalls'. options.h ReadOptions::snapshot: 'If snapshot is nullptr, use an implicit snapshot of the state at the beginning of this read operation'. Basic Operations wiki: reads with a snapshot observe state at creation despite concurrent writes; release when done.

- Source: https://github.com/facebook/rocksdb/wiki/Snapshot
- Confidence: verified; design-critical: yes

### 19. RocksDB's rate limiter can throttle analytical reads, not just compaction: DBOptions::rate_limiter charges flush at IO_HIGH and compaction at IO_LOW, and user reads are charged when ReadOptions::rate_limiter_priority is set (default IO_TOTAL = not charged); the limiter's Mode must be kReadsOnly or kAllIo (default kWritesOnly), and auto_tuned varies the limit within [rate/20, rate].

options.h (main): DBOptions::rate_limiter comment lines 736-750: flush IO_HIGH, compaction IO_LOW, 'Reads associated with a ReadOptions can be charged at ReadOptions::rate_limiter_priority', 'Writes ... at WriteOptions::rate_limiter_priority'; ReadOptions::rate_limiter_priority (line ~2308) default Env::IO_TOTAL, bypassed for plain/cuckoo tables; WriteOptions::rate_limiter_priority covers automatic WAL flushes and allows only IO_USER/IO_TOTAL. rate_limiter.h: enum Mode {kReadsOnly, kWritesOnly, kAllIo}, default kWritesOnly; NewGenericRateLimiter(rate_bytes_per_sec, refill_period_us=100000, fairness=10, mode, auto_tuned=false, single_burst_bytes=0), auto_tuned adjusts within [rate_bytes_per_sec/20, rate_bytes_per_sec]. The wiki page says the limiter does not cover WAL writes and IO_USER has top priority.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/include/rocksdb/options.h
- Confidence: verified; design-critical: yes

### 20. For large scans RocksDB provides ReadOptions::fill_cache=false (do not evict hot blocks), iterate_upper_bound/lower_bound, auto readahead that grows 8 KB to 256 KB (auto_readahead_size trims prefetch to the upper bound), an explicit readahead_size, and async_io prefetch; iterators pin memtables, SST files and blocks so they should be short-lived.

options.h ReadOptions: fill_cache 'Callers may wish to set this field to false for bulk scans' to keep eviction order; readahead_size comment 'readahead starts at 8KB and doubles ... up to 256KB'; auto_readahead_size default true; async_io default false. Iterator wiki: iterate_upper_bound avoids I/O beyond the range; 'the best use of iterator is to keep it short-lived'. Basic Operations wiki: fill_cache=false 'prevents bulk scans from displacing cached data'.

- Source: https://github.com/facebook/rocksdb/wiki/Iterator
- Confidence: verified; design-critical: yes

### 21. Each column family can have its own block cache because the cache lives in BlockBasedTableOptions, which is installed through ColumnFamilyOptions::table_factory; a Cache object may also be shared across CFs and DB instances, and the LRU cache reserves a high-priority pool for index/filter blocks.

Block Cache wiki: 'BlockBasedTableOptions table_options; table_options.block_cache = cache; options.table_factory.reset(NewBlockBasedTableFactory(table_options))'; 'A Cache object can be shared by multiple RocksDB instances in the same process'; NewLRUCache(capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio); HyperClockCache 'for advanced usage only'. options.h: table_factory is a ColumnFamilyOptions field ('a block-based table factory ... with default BlockBasedTableOptions'). Combined: per-CF table factories give per-CF caches.

- Source: https://github.com/facebook/rocksdb/wiki/Block-Cache
- Confidence: verified; design-critical: yes

### 22. A RocksDB DB can be opened read-only or as a 'secondary' in another process on the same files: OpenForReadOnly is a static view, OpenAsSecondary can TryCatchUpWithPrimary; both rebuild memtables from the WAL (so disableWAL hides data), and secondaries need max_open_files=-1, cannot take snapshots or tailing iterators, and do not see CFs created after they opened.

RocksDB wiki Read-only and Secondary instances: definitions, table of differences (secondary: catch-up yes, snapshots no), 'run as a different process on the same host', 'memtables are constructed by replaying the log entries in the WAL files', limitations list. Basic Operations wiki: the same DB object is thread-safe within a process but the files cannot be opened read-write by two processes.

- Source: https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances
- Confidence: verified; design-critical: yes

### 23. RocksDB checkpoints create a consistent copy in a new directory by hard-linking SST files (same filesystem) and copying the WAL range needed for cross-CF consistency; the copy can be opened read-only for point-in-time queries.

Checkpoints wiki: Checkpoint::Create(db,&cp); cp->CreateCheckpoint(dir); directory must not exist; 'SST files will be hard-linked, otherwise ... copied'; for multiple column families 'log files will be copied for the period covering the start and end of the checkpoint, in order to provide a consistent snapshot across column families'; can be opened read-only or read-write.

- Source: https://github.com/facebook/rocksdb/wiki/Checkpoints
- Confidence: verified; design-critical: no

### 24. RocksDB offers pessimistic TransactionDB (locks keys on write) and OptimisticTransactionDB (conflict check at commit), GetForUpdate for read-write conflicts, SetSnapshot for snapshot-based validation, WriteCommitted as default policy, and WriteBatchWithIndex for read-your-own-writes inside a batch; WriteOptions::low_pri lets writes yield when compaction is behind.

Transactions wiki: TransactionDB 'All keys that are written are locked internally'; OptimisticTransactionDB conflict detection at commit; 'GetForUpdate() is the only way to establish Read-Write conflicts'; SetSnapshot semantics; WriteCommitted default with WritePrepared/WriteUnprepared alternatives; WriteBatchWithIndex for read-your-own-writes. options.h WriteOptions::low_pri: 'this write request is of lower priority if compaction is behind'; disableWAL default false.

- Source: https://github.com/facebook/rocksdb/wiki/Transactions
- Confidence: verified; design-critical: no

### 25. RocksDB wide-column entities (PutEntity/GetEntity, since 8.0) attach named columns to a key but cannot project a subset of columns on read and are not supported in transactions, so they do not deliver columnar projection for analytics.

Wide Columns wiki: PutEntity on DB and WriteBatch, GetEntity/MultiGetEntity returning PinnableWideColumns, iterator columns(), merge applies to the default column only; available since RocksDB 8.0; unsupported: transactions, user-defined timestamps, SST file writer, column-level partial updates, 'Column projections (subset retrieval)'.

- Source: https://github.com/facebook/rocksdb/wiki/Wide-Columns
- Confidence: verified; design-critical: yes

### 26. RocksDB 11.8.1 (2026-08-07) is current and adds callback-based GetAsync/MultiGetAsync plus DBOptions::read_io_executor_threads; Homebrew ships rocksdb 11.8.1 with an Apple Silicon bottle (deps gflags, lz4, snappy, zstd).

GitHub releases page: 11.8.1 (Aug 7, 2026) async read APIs and read_io_executor_threads; 11.1.2 (Jun 25, 2026) fixed read-only DB close deleting live SSTs; 11.0.4 (Apr 7, 2026). GitHub API /releases/latest: v11.8.1 published 2026-08-07. formulae.brew.sh/formula/rocksdb: stable 11.8.1, arm64 bottles for current macOS versions, dependencies gflags 2.3.1, lz4 1.10.0, snappy 1.3.1, zstd 1.5.7.

- Source: https://formulae.brew.sh/formula/rocksdb
- Confidence: verified; design-critical: no

### 27. Kvrocks (C++, RocksDB-backed, Redis protocol) is the closest precedent: it encodes every Redis type as a metadata key plus prefixed subkeys, spreads them over eight column families (default=primary subkeys, metadata, zset_score, pubsub, propagate, stream, search, index), and commits metadata + subkeys in one WriteBatch; a per-key version number makes big-key deletes lazy.

Kvrocks storage.h (unstable): enum ColumnFamilyID {PrimarySubkey=0, Metadata, SecondarySubkey, PubSub, Propagate, Stream, Search, Index}; names 'default', 'metadata', 'zset_score', 'pubsub', 'propagate', 'stream', 'search', 'index'; Write(ctx, WriteOptions, WriteBatch*); a transactional context groups writes into one batch until commit. Data-structure doc: key layout ns size|namespace|slot|key size|key|version|subkey; metadata = flags|expire|version|size; zset keeps member->score and score|member->NULL entries; version 'enables fast delete' with cleanup at compaction. redis_hash.cc MSet: batch = storage_->GetWriteBatchBase(); batch->Put(field_key, value) (default CF) then batch->Put(metadata_cf_handle_, ns_key, bytes); storage_->Write(ctx, DefaultWriteOptions(), batch->GetWriteBatch()). Latest release v2.17.0 published 2026-09-19 (GitHub API).

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/storage.h
- Confidence: verified; design-critical: yes

### 28. Kvrocks Search (since 2.11.0, 'quite experimental') supports FT.CREATE/FT.SEARCH/FT.SEARCHSQL with TAG, NUMERIC and VECTOR(HNSW) fields through the KQIR planner (SQL and RediSearch dialects, cost-based index selection, Volcano executor) but has no aggregation, no JOIN, no full-text; its index entries live in the 'search' CF and are written in separate WriteBatches from the data write.

kvrocks-search docs: command list, three field types, 'released since version 2.11.0', 'currently still quite experimental'. KQIR blog: parsers -> syntactical IR -> semantic check -> optimizer (expression, numeric-interval, planning passes with cost model) -> plan executor; missing aggregations, full-text, vector (at time of post), subqueries, JOINs, cluster mode; 'Transaction guarantees remain weak'. indexer.cc lines 230-251 and 263-283: UpdateTagIndex/UpdateNumericIndex each call storage->GetWriteBatchBase(), Put/Delete on GetCFHandle(ColumnFamilyID::Search), then storage->Write(...) on their own batch.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/search/indexer.cc
- Confidence: verified; design-critical: yes

### 29. Dragonfly is an in-memory, shared-nothing C++ Redis/Memcached replacement (no RocksDB); its FT.AGGREGATE has 'partial support' (GROUPBY, REDUCE COUNT/SUM/MIN/MAX/AVG/STDDEV, SORTBY, LIMIT, LOAD, FILTER, APPLY, ADDSCORES, PARAMS, DIALECT); latest release v2.0.0 (2026-09-16).

dragonflydb/dragonfly README: 'a modern replacement for Redis and Memcached', C++, shared-nothing keyspace sharded across threads, dashtable; no RocksDB mention. Dragonfly FT.AGGREGATE doc: clause list and 'FT.AGGREGATE has partial support in Dragonfly. Some advanced aggregation functions and options may not be fully implemented'. GitHub API: v2.0.0 published 2026-09-16; release notes for v1.40.0 mention native-width vectors, HNSW EPSILON, KNN attribute syntax, BM25/TF-IDF weights.

- Source: https://www.dragonflydb.io/docs/command-reference/search/ft.aggregate
- Confidence: verified; design-critical: no

### 30. Redis Search's FT.AGGREGATE is a re-entrant pipeline: a query filter, then any sequence of LOAD, GROUPBY+REDUCE, SORTBY [MAX n], APPLY, FILTER, LIMIT applied in order; reducers are COUNT, COUNT_DISTINCT, COUNT_DISTINCTISH (HLL ~3%), SUM, MIN, MAX, AVG, STDDEV, QUANTILE, TOLIST, FIRST_VALUE, RANDOM_SAMPLE, COLLECT (8.8.0+); fields should be SORTABLE because LOAD does an HMGET per record; WITHCURSOR pages 1000 rows by default with a 300 s idle cap.

redis.io Aggregations page: 'FILTER, LIMIT, GROUPBY, SORTBY and APPLY clauses can be repeated ... applied in the order specified'; LOAD 'hurts the performance of aggregate queries considerably since every processed record needs to execute the equivalent of HMGET'; SORTBY ... MAX sorts only the n largest; APPLY function lists incl. geodistance and time functions; cursor COUNT default 1000, MAXIDLE default 300000 ms. FT.AGGREGATE command page: complexity 'usually linear to the number of results returned'; DIALECT 4 sorting optimizations (skip sorter, partial range, hybrid). Redis Search landing page: available in Redis Open Source, Redis Software, Redis Cloud; licenses RSALv2/SSPLv1/AGPLv3.

- Source: https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/aggregations/
- Confidence: verified; design-critical: yes

### 31. MongoDB's aggregation pipeline is a sequence of stages ($match, $group, $sort, $limit, $project, ...) and its optimizer applies rules a NilDB pipeline should copy: move $match before $sort, coalesce $sort+$limit into an in-memory top-k (limit+skip), merge consecutive $match/$limit/$skip, move $skip ahead of $project, use an index only when $match is the first stage; sorts are bounded by a 100 MB memory limit unless allowDiskUse.

mongodb.com aggregation-pipeline: stages pass documents to the next stage; $out, $merge, $geoNear may appear once. aggregation-pipeline-optimization: '$sort + $limit coalescence ... only maintains the top n results in memory'; '$sort + $skip + $limit' turns into sort with limit n+skip; '$match + $match' combine with $and; '$project/$unset + $skip' moves $skip earlier; index usable by $match as first stage, $sort if not after $project/$unwind/$group, $group for $first/$last; $geoNear always uses a geospatial index; the top-k optimization applies even when items exceed the 100 MB limit with allowDiskUse.

- Source: https://www.mongodb.com/docs/manual/core/aggregation-pipeline-optimization/
- Confidence: verified; design-critical: yes

### 32. Apache DataFusion (Rust, 55.1.0 released 2026-09-11, arrow 59.2) embeds a full SQL/DataFrame engine and lets a RocksDB-backed table plug in through TableProvider::scan(projection, filters, limit) returning an ExecutionPlan whose execute() yields a SendableRecordBatchStream; supports_filters_pushdown returns Exact/Inexact/Unsupported per predicate.

docs.rs/crate/datafusion/latest: 55.1.0, 2026-09-11, arrow ^59.2.0. Custom Table Providers guide: trait methods schema(), table_type(), async scan(&self, state, projection: Option<&[usize]>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>>; supports_filters_pushdown; ExecutionPlan::execute(partition, ctx) -> SendableRecordBatchStream; 'Push work as late as possible'. DataFusion blog (2026-03-31) 'Writing Custom Table Providers': three layers, projection/filter/limit pushdown, partition by natural key ranges, use RecordBatchStreamAdapter and tokio::task::spawn_blocking for blocking I/O, verify with EXPLAIN. custom_datasource.rs example: CustomDataSource implements TableProvider, CustomExec builds a RecordBatch with UInt8Builder/UInt64Builder from a HashMap and applies RecordBatch::project; SELECT COUNT(id) drives a single-column projection.

- Source: https://datafusion.apache.org/library-user-guide/custom-table-providers.html
- Confidence: verified; design-critical: yes

### 33. No ready-made DataFusion TableProvider or DuckDB extension for RocksDB was found; the community datafusion-table-providers repos list other database backends, so a KV-backed provider must be written in-house.

Web search for DuckDB-over-RocksDB and DataFusion RocksDB TableProvider returned only the generic extension docs and the datafusion-contrib/datafusion-table-providers repositories (providers for other database systems); neither mentioned RocksDB.

- Source: https://github.com/datafusion-contrib/datafusion-table-providers
- Confidence: likely; design-critical: no

### 34. Apache Arrow Go (v18.8.0, 2026-09-08) has a pure-Go compute package with scalar/vector kernels, cast, filter, take, sort, unique and scalar count/sum, but the hash_* aggregate kernels used for GROUP BY, plus mean/min/max/variance aggregates, are explicitly 'not implemented yet' and there is no planner or SQL; a Go NilDB would have to write its own aggregation engine.

arrow/compute/doc.go (main): 'a native-go implementation of an Acero-like arrow compute engine' for users who cannot use CGO; lists implemented scalar and vector functions and scalar aggregates count and sum; states remaining scalar aggregates (mean, min/max, variance) and 'hash aggregate functions (the hash_* family used for group-by) are not implemented yet'; aggregates deliberately absent from the exprs package. pkg.go.dev arrow/compute page marks the package Experimental. GitHub API: v18.8.0 published 2026-09-08; release notes mention sort support added to compute.

- Source: https://raw.githubusercontent.com/apache/arrow-go/main/arrow/compute/doc.go
- Confidence: verified; design-critical: yes

### 35. DuckDB can be embedded from either language as an alternative analytical engine: duckdb-go v2.10505.0 (2026-07-22, DuckDB 1.5.5) needs cgo, ships prebuilt darwin/arm64 static libs, supports table and scalar UDFs and opt-in Arrow (-tags=duckdb_arrow); duckdb-rs 1.10505.0 exposes a VTab trait (bind/init/func) with Arrow helpers; the C table-function API supports projection pushdown and per-thread local init with duckdb_init_set_max_threads.

github.com/duckdb/duckdb-go README: 'Current DuckDB version v1.5.5', versioning v2.MAJOR_MINOR_PATCH.x, cgo required, prebuilt static libraries for macOS amd64/arm64, Arrow opt-in via build tag and NewArrowFromConn (not safe for concurrent use), scalar_udf.go and table_udf.go; repo moved from marcboeker/go-duckdb at v2.5.0. docs.rs duckdb vtab module: VTab trait, BindInfo/InitInfo/TableFunctionInfo, arrow_recordbatch_to_query_params, record_batch_to_duckdb_data_chunk; crate 1.10505.0 (2026-07-22). duckdb.org C API table functions: duckdb_create_table_function, set_bind/set_init/set_local_init/set_function, duckdb_table_function_supports_projection_pushdown, duckdb_init_get_column_count/index, duckdb_init_set_max_threads (default 1).

- Source: https://github.com/duckdb/duckdb-go
- Confidence: verified; design-critical: yes

### 36. Bindings are current for both languages: grocksdb v1.11.1 (Go, cgo, 2026-09-07) supports RocksDB 11.x/10.x/... against a system librocksdb and exposes column families, WriteBatch, snapshots, iterators, checkpoints, transactions, rate limiter and caches; the Rust rocksdb crate 0.25.0 (2026-08-16) bundles and builds RocksDB 11.8.1 via librocksdb-sys 0.19.0+11.8.1, needs Rust 1.88+, and has a multi-threaded-cf feature.

linxGnu/grocksdb README: 'Support 11.x, 10.x, 9.x, 8.x, 7.x, 6.x'; prerequisites librocksdb plus compression libs; CGO_CFLAGS/CGO_LDFLAGS; feature list incl. column families, WriteBatch, snapshots, iterators, checkpoints/backups, pessimistic and optimistic transactions, rate limiting, caches, compaction filters, merge operators; gaps: event listeners, compaction services. GitHub API: v1.11.1 published 2026-09-07. docs.rs/crate/rocksdb/latest: 0.25.0 released 2026-08-16, depends on librocksdb-sys ^0.19.0, MSRV 1.88.0, features snappy/lz4/zstd/zlib/bzip2, multi-threaded-cf, lto. crates.io API librocksdb-sys: newest 0.19.0+11.8.1, updated 2026-08-16.

- Source: https://docs.rs/crate/rocksdb/latest
- Confidence: verified; design-critical: yes

### 37. Homebrew has current arm64 bottles for the toolchains: go 1.27.1 and rust 1.98.1.

formulae.brew.sh/formula/go: stable 1.27.1, Apple Silicon bottles; formulae.brew.sh/formula/rust: stable 1.98.1, Apple Silicon bottles.

- Source: https://formulae.brew.sh/formula/go
- Confidence: verified; design-critical: no
