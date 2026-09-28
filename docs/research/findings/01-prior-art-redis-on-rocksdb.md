# Prior art: Redis-compatible stores on RocksDB (Kvrocks, Pika, Tidis, Titan, SSDB, LedisDB, Ardb): key encodings, column families, TTL, namespaces, transactions, skipped commands

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:prior-art-redis-on-rocksdb`, agent `afa59feb24e2b7ef3`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Adopt the Blackwidow/Kvrocks two-level layout as NilDB's base: a 'metadata' CF holding | flags | expire_ms (8B) | version (8B) | size (8B) | (+ head/tail for lists) and whole string values, a 'subkey' CF for hash/set/list elements keyed by ns|key|version|subkey, and a 'zset_score' CF for score-ordered entries. Use 8-byte fields from day one (Kvrocks' v0 4-byte fields forced a dual-encoding migration).
2. Make DEL O(1) with a per-key version (Kvrocks: 53-bit microsecond timestamp << 11 | counter; Titan: UUID object IDs). New writes after DEL get a new version, and stale-version subkeys are reclaimed by a compaction filter. Do not iterate-and-delete on DEL (SSDB/LedisDB style) for collections.
3. Encode zset scores as sign-flipped big-endian u64 (Kvrocks EncodeDoubleToUInt64, Titan EncodeFloat64, Tidis-Rust encode_f64_to_cmp_uint64) so the default bytewise comparator orders them numerically. Avoid custom comparators (Pika, Ardb) which prevent using stock RocksDB tooling (ldb/sst_dump) and break the 'native RocksDB feel'; avoid int64 scores (LedisDB, Tidis-Go, SSDB) which violate Redis float semantics.
4. Lists: array-index encoding with head/tail starting at the u64 midpoint (Kvrocks UINT64_MAX/2, Pika 2^63, SSDB INT64_MAX/2, LedisDB int32 midpoint). LINSERT in the middle needs either renumbering (Kvrocks) or Titan's float64 midpoint trick; pick u64 + renumber and document LINSERT as O(n).
5. TTL: store absolute expire-ms in metadata, check lazily on every read (Kvrocks, Titan, Tidis-2, Ardb), and reclaim via compaction filters (Kvrocks MetadataFilter + SubKeyFilter with a cached metadata lookup and a 5-minute grace window; Pika BaseMetaFilter/BaseDataFilter with type-mismatch and version checks). A time-ordered expire index (LedisDB ExpTimeType, Titan $sys:at:{ts}, SSDB internal zset) is only needed if NilDB must actively evict or emit keyspace notifications; if added, keep it in its own CF so it never pollutes data scans. Also set periodic compaction (Pika: rocksdb-ttl-second / rocksdb-periodic-second) so expired data in cold SSTs actually gets rewritten.
6. Namespaces / SELECT: a fixed 1-byte length + namespace prefix (Kvrocks) or a fixed-width db id (Tidis 1B, Titan %03d, LedisDB uvarint) keeps everything in shared CFs; Ardb's CF-per-namespace makes DROP DATABASE a DropColumnFamily and gives per-DB compaction stats but needs a bounded DB count. For a 'native RocksDB feel' NilDB can expose CFs by role (metadata, subkey, zset_score, geo_index, doc_index) and encode the DB index as a key prefix, or offer CF-per-database as an explicit option; do not do both.
7. MULTI/EXEC: follow Kvrocks (queue commands, on EXEC hold an exclusive guard, execute against a WriteBatchWithIndex for read-your-writes, commit as one atomic WriteBatch) rather than Pika (lock keys and run each command with its own write, so a crash mid-EXEC can leave partial state). WATCH can be implemented with Kvrocks-style watched-key sets or, more natively, with RocksDB OptimisticTransactionDB conflict detection, which both grocksdb and rust-rocksdb expose. Use a striped per-key LockManager (Kvrocks: 2^n mutexes, sorted multi-key acquisition) for single-command atomicity across metadata+subkeys.
8. Key escaping matters for prefix scans: Pika escapes \0 and terminates user keys with \0\0 so meta and data keys share a prefix; Kvrocks uses a 4-byte key-length field; Tidis-2 uses memcomparable 8-byte groups. Any of these is fine, but delimiter-terminated keys (Pika) let SCAN/KEYS and per-key range-deletes use one prefix bound; length-prefixed keys (Kvrocks) need the metadata CF for enumeration.
9. HTAP: Kvrocks Context (pinned snapshot + WriteBatchWithIndex per command) is the model for consistent analytical scans while OLTP writes continue; use db.GetSnapshot() for every aggregation and expose it. Secondary/analytical indexes should live in a dedicated CF with explicit subkey types (Kvrocks 'search' CF: INDEX_META, PREFIXES, FIELD_META, FIELD; TAG/NUMERIC/VECTOR) and be maintained in the same WriteBatch as the primary write to avoid KQIR's acknowledged weak consistency.
10. Geo: prior art only covers Redis GEO (52-bit geohash as zset score, 9-cell neighbour search, distance post-filter). NilDB can reuse that for GEOADD/GEOSEARCH compatibility, but MongoDB-style GeoJSON ($near, $geoWithin, $geoIntersects) needs a separate covering-cell index (S2 or geohash prefix ranges) in its own CF keyed by cell id | doc id, plus geometry stored in the document; this is new design work with no reference implementation in this set.
11. Command scope: declare an explicit skip list up front, mirroring the survivors: OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT, CLIENT TRACKING/CACHING, SCRIPT KILL/DEBUG, SWAPDB, SORT (Pika skips it), MEMORY, CLUSTER, DEBUG; keep KEYS/SCAN (metadata CF iteration), MOVE/RENAME (copy+delete with version), and blocking list ops as later milestones.
12. Language: every live RocksDB-direct Redis clone is C++; Go was used by the TiKV-backed ones (LedisDB, Titan, Tidis 1) and Rust by Tidis 2. Rust's rocksdb crate 0.25.0 bundles RocksDB 11.8.1 statically (no Homebrew librocksdb needed, single cargo build on Apple Silicon) and exposes checkpoints, compaction filters, comparators, merge operators, snapshots, WriteBatch, TransactionDB/OptimisticTransactionDB, but its src tree shows no WriteBatchWithIndex; grocksdb v1.11.1 exposes WriteBatchWithIndex, TransactionDB, OptimisticTransactionDB, checkpoints and compaction filters but is cgo and needs an external librocksdb (brew install rocksdb) with hand-set CGO_LDFLAGS, and compaction-filter callbacks cross the cgo boundary per key. If NilDB wants a Kvrocks-style EXEC (WriteBatchWithIndex) in Rust, plan to use OptimisticTransactionDB/Transaction instead, or add the WBWI FFI; otherwise Rust is the lower-friction path for a compaction-filter-heavy design on macOS.

## Open questions

- Does rust-rocksdb 0.25.0 expose WriteBatchWithIndex (needed for Kvrocks-style EXEC with read-your-writes)? The src listing shows write_batch.rs but no write_batch_wi; the transactions module may be the substitute. Needs a docs.rs check before choosing Rust.
- What is the real per-key cost of a cgo compaction filter in grocksdb versus a native Rust CompactionFilter closure? No benchmark was found in this pass; it decides whether Go can afford Kvrocks/Pika-style filters at scale.
- Pika's EXEC runs each queued command's own write (Lock(); Do(); Unlock()) rather than one WriteBatch; whether Pika guarantees crash atomicity across the transaction via binlog replay was not verified.
- Kvrocks' SubKeyFilter reads the metadata CF from inside compaction (one Get per distinct key, cached); how this behaves under NilDB's expected key cardinality and whether a Bloom filter on the metadata CF is enough to keep compaction cheap needs measurement.
- Pika's compatibility wiki says MULTI/Lua are unsupported while the v4 source registers MULTI/EXEC/WATCH; the wiki is stale, so the exact current Pika command gap list should be re-derived from pika_command.cc if it matters.
- The legacy Pika 'nemo' storage-format wiki page failed to load in this session; its list encoding (linked nodes with prev/next pointers) was not verified and is only relevant as history.
- Ardb's compaction filter records expired metas via AddExpiredKey instead of dropping them inline; how the expired list is drained and whether subkeys of expired collections are ever reclaimed on RocksDB was not traced.
- No prior art here covers GeoJSON/S2-style spatial indexing or MongoDB query semantics; a separate research task on S2/H3/geohash covering-cell indexes over RocksDB (e.g. how MongoDB's 2dsphere uses S2 cell ranges) is needed before the geo CF layout is fixed.
- Hash-field expiration (Kvrocks 'field expiration' encoding with mode/persist/lower/upper) was only skimmed; if NilDB wants HEXPIRE, the exact metadata layout and the SubKeyFilter's kUndetermined handling should be read in full.
- Homebrew availability and version of librocksdb on macOS Apple Silicon (relevant only for the Go/cgo path) was not checked in this pass.

## Findings (39)

### 1. Kvrocks user-key layout: | ns size (1B) | namespace | cluster slot (2B, only when slot-id encoding is on) | user key size (4B) | user key | version (8B) | sub key |. InternalKey::Encode writes exactly EncodeFixed8(ns.size), ns, [EncodeFixed16(slot)], EncodeFixed32(key.size), key, EncodeFixed64(version), sub_key.

Design doc gives the diagram; redis_metadata.cc InternalKey::Encode: total = 1 + ns + 4 + key + 8 + subkey (+2 if slot_id_encoded), then Fixed8 ns size, ns bytes, optional Fixed16 slotid, Fixed32 key size, key, Fixed64 version, subkey.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/redis_metadata.cc
- Confidence: verified; design-critical: yes

### 2. Kvrocks metadata value for every type starts with | flags (1B) | expire (4B or 8B) | and for collections adds | version (8B) | size (4B or 8B) |; flags encode redis type in the low 4 bits and a '64-bit common field' indicator in the MSB (version 1 => 8-byte expire in ms and 8-byte size; version 0 => 4-byte expire in s, 4-byte size). Expire 0 means never.

Design doc: 'key => | flags (1byte) | expire (Ebyte) | version (8byte) | size (Sbyte) |'; 'Version 0 uses 4-byte expire/size; Version 1 uses 8-byte expire/size (milliseconds)'; redis_metadata.h comment '<(1-bit) 64bit-common-field-indicator> 0 0 0 <(4-bit) redis-type>'.

- Source: https://kvrocks.apache.org/community/data-structure-on-rocksdb/
- Confidence: verified; design-critical: yes

### 3. Kvrocks version field = 53-bit microsecond timestamp << 11 + 11-bit atomic counter; it exists so DEL of a big collection is O(1): delete the metadata key, and subkeys with a stale version are reclaimed later by compaction.

redis_metadata.h: 'the current version: 53bit timestamp + 11bit counter'; generateVersion(): (GetTimeStampUS() << VersionCounterBits) + counter % (1<<VersionCounterBits). Design doc: 'With version, we can quickly delete the metadata and then recycle the others keys-values in compaction background threads.'

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/redis_metadata.cc
- Confidence: verified; design-critical: yes

### 4. Kvrocks per-type subkey layouts: Hash key|version|field => value; Set key|version|member => NULL; List key|version|index(8B) => value with metadata carrying head/tail; ZSet has two subkeys: key|version|member => score(8B) in the subkey CF and key|version|score(8B)|member => NULL in the zset_score CF. String is a single metadata-CF entry | flags | expire | payload |.

Design doc diagrams for String/Hash/Set/List/ZSet; redis_zset.cc: PutDouble(score) then batch->Put(member_key, score_bytes) and score_key = InternalKey(ns_key, score_bytes+member, version) put into score_cf_handle_.

- Source: https://kvrocks.apache.org/community/data-structure-on-rocksdb/
- Confidence: verified; design-critical: yes

### 5. Kvrocks list head/tail both start at UINT64_MAX/2 so LPUSH decrements head and RPUSH increments tail without renumbering; list metadata appends Fixed64 head and Fixed64 tail after the common fields.

redis_metadata.cc: 'ListMetadata::ListMetadata(bool generate_version) : Metadata(kRedisList, generate_version), head(UINT64_MAX / 2), tail(head) {}' and Encode() does PutFixed64(head); PutFixed64(tail).

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/redis_metadata.cc
- Confidence: verified; design-critical: yes

### 6. Kvrocks encodes zset scores (double) as an 8-byte big-endian integer with the sign-flip trick so RocksDB's bytewise comparator orders them numerically: if sign bit set, XOR all bits; else set the top bit.

encoding.cc EncodeDoubleToUInt64: memcpy double to u64; if (result>>63)==1 result ^= 0xffffffffffffffff else result |= 0x8000000000000000; PutDouble = PutFixed64(EncodeDoubleToUInt64(v)). redis_zset.cc comment: 'to make positive double greater than native double in lexicographical order, score is required encoding before stored in rocksdb'.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/common/encoding.cc
- Confidence: verified; design-critical: yes

### 7. Kvrocks column families today: 'default' (primary subkey), 'metadata', 'zset_score' (secondary subkey), 'pubsub', 'propagate', 'stream', 'search', 'index'. The blog explains the original five: metadata stores expire/size for complex types and also string KVs; subkey CF stores complex-type elements; ZSetScore stores only sorted-set scores; PubSub and Propagated exist to ship messages/commands to replicas through the WAL.

storage.h constants kPrimarySubkeyColumnFamilyName="default", kMetadataColumnFamilyName="metadata", kSecondarySubkeyColumnFamilyName="zset_score", plus pubsub, propagate, stream, search, index (pubsub/propagate/stream/search/index flagged is_minor=true). Blog lists the five CFs and their purpose.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/storage.h
- Confidence: verified; design-critical: yes

### 8. Kvrocks enforces TTL with lazy checks on read plus two RocksDB compaction filters: MetadataFilter drops a metadata entry when metadata.Expired(); SubKeyFilter decodes the InternalKey, looks up the owning metadata in the metadata CF (caching the last key/metadata), and drops the subkey when metadata is missing, is a single-KV type (key overwritten by SET), version mismatches, or the key expired more than 5 minutes ago (grace window to avoid a race with EXPIRE). PubSub CF filter drops everything; Propagate filter drops all but the script-command key.

compact_filter.cc: MetadataFilter::Filter returns metadata.Expired(); SubKeyFilter::GetMetadata does db->Get(Metadata CF, ComposeNamespaceKey(...)) with cached_key_/cached_metadata_; IsMetadataExpired: 'lazy_expired_ts = GetTimeStampMS() - 300000' and 'metadata.IsSingleKVType() || metadata.ExpireAt(lazy_expired_ts) || ikey.GetVersion() != metadata.version'; redis_db.cc GetMetadata returns NotFound(kErrMsgKeyExpired) when metadata->Expired().

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/compact_filter.cc
- Confidence: verified; design-critical: yes

### 9. Kvrocks namespaces replace Redis SELECT: each namespace has its own AUTH token, the namespace string is simply prepended to the user key (1-byte length prefix) and stripped on read, and namespaces are disabled in cluster mode.

Namespace doc: 'Kvrocks simply prepend the namespace prefix to the user key and remove it before retrieving'; 'operate data between namespaces would NOT affect each other'; 'The namespace would have no effect when the cluster mode was enabled like the Redis DB'.

- Source: https://kvrocks.apache.org/docs/namespace
- Confidence: verified; design-critical: yes

### 10. Kvrocks MULTI/EXEC: commands are queued with QUEUED, EXEC takes the server-wide WorkExclusivityGuard (no other worker runs), calls Storage::BeginTxn() which swaps in a rocksdb::WriteBatchWithIndex (overwrite_key=false), replays the queued commands against it, then CommitTxn() writes the whole batch once. WATCH/UNWATCH/DISCARD supported since v2.0.2. Per-key concurrency outside MULTI uses a striped LockManager (2^hash_power std::mutex, MultiLockGuard sorts lock indexes to avoid deadlock).

cmd_txn.cc CommandExec: 'storage->BeginTxn(); conn->ExecuteCommands(conn->GetMultiExecCommands())'; storage.cc BeginTxn: 'The EXEC command is exclusive...' and txn_write_batch_ = make_unique<WriteBatchWithIndex>(...); CommitTxn writes txn_write_batch_->GetWriteBatch(); redis_connection.cc: 'exec command has acquired WorkExclusivityGuard'; lock_manager.h LockManager/MultiLockGuard; supported-commands page: MULTI, EXEC, DISCARD, WATCH, UNWATCH supported (v2.0.2+).

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/storage.cc
- Confidence: verified; design-critical: yes

### 11. Kvrocks exposes native RocksDB machinery: engine::Context pins a snapshot plus a WriteBatchWithIndex so a command's reads see a fixed snapshot and its own writes; rocksdb::Checkpoint::CreateCheckpoint is used both for backups and for the replication full-sync data set.

storage.h: 'Context passes fixed snapshot and batch between APIs', 'std::unique_ptr<rocksdb::WriteBatchWithIndex> batch', GetSnapshot() lazily creates; storage.cc: 'Create checkpoint of rocksdb for backup' rocksdb::Checkpoint::Create + CreateCheckpoint(tmpdir,...) and again for the replication data_files_dir with checkpoint_info_.latest_seq.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/storage.h
- Confidence: verified; design-critical: yes

### 12. Kvrocks GEO is Redis-style only: GEOADD encodes WGS84 lon/lat to a 52-bit geohash (GEO_STEP_MAX) and stores it as the zset score; radius/box search iterates the 8 neighbours + centre cells via score ranges. There is no polygon or GeoJSON support.

redis_geo.cc: GeohashEncodeWGS84(lon, lat, GEO_STEP_MAX, &hash); Align52Bits; ZSet::Add(...); membersOfAllNeighbors over neighbors[9]; scoresOfGeoHashBox computes min/max scores. Supported-commands page: GEOSEARCH/GEOSEARCHSTORE since v2.6.0.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/types/redis_geo.cc
- Confidence: verified; design-critical: yes

### 13. Kvrocks commands not supported: OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT, SCRIPT KILL/DEBUG, CLIENT CACHING/GETREDIR/NO-EVICT/NO-TOUCH/TRACKING/TRACKINGINFO/UNBLOCK, CONFIG RESETSTAT, BRPOPLPUSH (deprecated). Lua scripting, FUNCTION (v2.7.0+), streams with consumer groups (v2.10.0+), KEYS/SCAN and MOVE (always returns 1 if key exists) are supported.

Supported-commands page (last updated 2026-07-18) lists the unsupported subcommands and version gates as above.

- Source: https://kvrocks.apache.org/docs/supported-commands
- Confidence: verified; design-critical: no

### 14. Kvrocks KQIR is the closest prior art for analytical queries over Redis structures on RocksDB: FT.CREATE indexes on HASH or JSON with TAG and NUMERIC fields (VECTOR type exists in the encoding), SQL and RediSearch queries lower to one IR, plans execute in a Volcano model over RocksDB iterators, index data lives in the 'search' CF with subkey types INDEX_META, PREFIXES, FIELD_META, FIELD, FIELD_ALIAS; the authors state the transaction guarantee is weak.

KQIR blog: 'FT.CREATE testidx ON JSON PREFIX 1 test: SCHEMA a TAG b NUMERIC', 'the Volcano model', 'Currently, the transaction guarantee of KQIR is weak'; search_encoding.h enum SearchSubkeyType {INDEX_META=0, PREFIXES=1, FIELD_META=2, FIELD=3, FIELD_ALIAS=4}, IndexFieldType {TAG=1, NUMERIC=2, VECTOR=3}.

- Source: https://kvrocks.apache.org/blog/kqir-query-engine/
- Confidence: verified; design-critical: yes

### 15. Kvrocks is the most active project in this set: latest release v2.17.0 published 2026-09-19, last push 2026-09-26, 4442 stars; the design doc states 'Most of the design were derived from Qihoo360/Blackwidow'.

GitHub releases API: tag_name v2.17.0, published_at 2026-09-19T12:02:09Z; repo API pushed_at 2026-09-26; design doc quote as given.

- Source: https://api.github.com/repos/apache/kvrocks/releases/latest
- Confidence: verified; design-critical: no

### 16. Pika legacy Blackwidow layout (pre-4.0): five separate RocksDB instances, one per type, so the same key could exist with several types. String: value = payload + 4B timestamp. Hash/Set/ZSet meta value = 4B size + 4B version + 4B timestamp; data key = 4B key_size + key + 4B version + field/member. List meta = 8B size + 4B version + 4B timestamp + 8B left index + 8B right index; data key = 4B key_size + key + 4B version + 8B index. ZSet keeps member->score in data_cf and a score_cf with key = 4B key_size + key + 4B version + 8B score + member ordered by a custom comparator (score numeric, then member).

Blackwidow wiki page (pikiwidb wiki) describes exactly these layouts, meta_cf/data_cf/score_cf, custom comparator, and 'lazy deletion' via timestamps and version numbers; Floyd discussion #2052 states Blackwidow 'maintained separate RocksDB instances for each data type'.

- Source: https://github.com/OpenAtomFoundation/pikiwidb/wiki/pika-blackwidow%E5%BC%95%E6%93%8E%E6%95%B0%E6%8D%AE%E5%AD%98%E5%82%A8%E6%A0%BC%E5%BC%8F
- Confidence: verified; design-critical: yes

### 17. Pika 4.x 'Floyd' storage: one RocksDB instance per shard with 7 column families: kMetaCF=0 (all types' metadata and whole string values), kHashesDataCF=1, kSetsDataCF=2, kListsDataCF=3, kZsetsDataCF=4, kZsetsScoreCF=5, kStreamsDataCF=6. Keys are sharded across db-instance-num RocksDB instances (default 3) by slot (default-slot-num 1024), and each Redis DB (databases, default 1, range 1-8) is a separate Storage.

storage_define.h enum ColumnFamilyIndex with comment 'kMetaCF is used to store the metadata of all types of data and all information of type string'; storage.cc Storage(3,1024,true) default, GetDBInstance by slot; pika.conf 'db-instance-num : 3', 'databases : 1' ('value range [1, 8]'), 'default-slot-num : 1024'.

- Source: https://raw.githubusercontent.com/OpenAtomFoundation/pika/unstable/src/storage/include/storage/storage_define.h
- Confidence: verified; design-critical: yes

### 18. Pika Floyd key/value formats: meta key and string key = | reserve1 (8B) | user key with \0 escaped as \0\1 and terminated by \0\0 | reserve2 (16B) |; hash/set/zset data key = | reserve1 8B | key | version 8B | data | reserve2 16B |; zset score key = | reserve1 8B | key | version 8B | score 8B | member | reserve2 16B |; base meta value = | type 1B | value | version 8B | reserve 16B | cdate 8B | timestamp 8B |; list meta value = | type 1B | list_size 8B | version 8B | left index 8B | right index 8B | reserve 16B | cdate 8B | timestamp 8B | with InitalLeftIndex = 2^63-1 and InitalRightIndex = 2^63; strings value = | type 1B | value | reserve 16B | cdate 8B | timestamp 8B |. Delimiter-terminated keys let meta and data keys share a prefix.

base_key_format.h comment '| reserve1 | key | reserve2 | 8B / 16B'; storage_define.h kEncodedTransformCharacter "\u0000\u0001", kEncodedKeyDelim "\u0000\u0000"; base_data_key_format.h '| reserve1 | key | version | data | reserve2 |'; zsets_data_key_format.h '| reserve1 | key | version | score | member | reserve2 |'; base_meta_value_format.h '| type | value | version | reserve | cdate | timestamp |'; lists_meta_value_format.h InitalLeftIndex = 9223372036854775807, InitalRightIndex = 9223372036854775808U; Floyd discussion: 'ensures data keys and meta keys share identical prefixes'.

- Source: https://raw.githubusercontent.com/OpenAtomFoundation/pika/unstable/src/storage/src/lists_meta_value_format.h
- Confidence: verified; design-critical: yes

### 19. Pika orders zset score keys with a custom RocksDB comparator ('floyd.ZSetsScoreKeyComparator') that compares the escaped key prefix, then version, then reinterprets the 8 score bytes as a double and compares numerically, then member. This avoids the sign-flip encoding but ties the score CF to a non-bytewise comparator.

custom_comparator.h ZSetsScoreKeyComparatorImpl::Compare: prefix compare via SeekUserkeyDelim, DecodeFixed64 version compare, then 'double a_score = *reinterpret_cast<const double*>(ptr_a_score)' compare.

- Source: https://raw.githubusercontent.com/OpenAtomFoundation/pika/unstable/src/storage/src/custom_comparator.h
- Confidence: verified; design-critical: yes

### 20. Pika TTL enforcement: BaseMetaFilter drops string values whose Etime != 0 && Etime < now, and collection metas that are expired or empty (Count()==0) once their version < now; BaseDataFilter looks up the meta in CF handle [0] (cached per key), drops data keys when the meta is missing, the meta type differs from the data CF's type, meta Etime passed, or meta version > data-key version. Expiry timestamps are milliseconds since Floyd. Pika also sets rocksdb-ttl-second (7 days) and rocksdb-periodic-second (3 days) to force periodic compaction of old SSTs.

base_filter.h BaseMetaFilter and BaseDataFilter code as described ('Drop[Meta key not exist]', 'Drop[Timeout]', 'Drop[data_key_version < cur_meta_version]', type mismatch 'if (type != type_) return true'); pika.conf 'rocksdb-ttl-second : 86400 * 7; rocksdb-periodic-second : 86400 * 3'; CHANGELOG: 'Changed the value in Floyd's stored data field to milliseconds'.

- Source: https://raw.githubusercontent.com/OpenAtomFoundation/pika/unstable/src/storage/src/base_filter.h
- Confidence: verified; design-critical: yes

### 21. Pika supports MULTI/EXEC/WATCH/DISCARD (registered in the command table) and GEO commands; EXEC locks the involved keys/DBs (Lock()), then runs each queued command's Do() and DoBinlog() in sequence, and unlocks. It is isolation by locking, not a single atomic RocksDB write batch.

pika_command.cc registers kCmdNameMulti, kCmdNameExec (kCmdFlagsSuspend), kCmdNameWatch, kCmdNameGeoAdd; pika_transaction.cc ExecCmd::Execute: 'SetCmdsVec(); Lock(); Do(); Unlock();' and ExecCmd::Do loops 'cmd->Do(); if (cmd->res().ok() && cmd->is_write()) cmd->DoBinlog();'.

- Source: https://raw.githubusercontent.com/OpenAtomFoundation/pika/unstable/src/pika_transaction.cc
- Confidence: verified; design-critical: yes

### 22. Pika command gaps (from its compatibility wiki, partly stale): unsupported DUMP, MIGRATE, MOVE, OBJECT, RANDOMKEY, RENAME, RENAMENX, RESTORE, SORT, TOUCH, WAIT, BITFIELD, BITOP, BRPOPLPUSH, XGROUP/XREADGROUP/XAUTOCLAIM; PEXPIRE truncates to seconds; SCAN emits types sequentially; SETBIT limited to 2^21 bits; ZADD NX/XX/CH/INCR unsupported; the 'differences' page says 'pika does not support the sort command' and KEYS/FLUSHDB accept a type filter. Latest release v4.0.4-alpha (2026-09-01).

Compatibility wiki table (○/!/×) and notes as listed; differences wiki: 'pika 不支持 sort 命令', 'keys命令允许只输出指定数据结构的所有key'; releases API tag v4.0.4-alpha published 2026-09-01. The wiki's 'no MULTI/Lua' note contradicts current source, which registers MULTI/EXEC/WATCH.

- Source: https://github.com/OpenAtomFoundation/pika/wiki/pika-%E6%94%AF%E6%8C%81%E7%9A%84redis%E6%8E%A5%E5%8F%A3%E5%8F%8A%E5%85%BC%E5%AE%B9%E6%83%85%E5%86%B5
- Confidence: verified; design-critical: no

### 23. Tidis 1.x (Go, yongman/tidis, archived; v1.1.0, last commit 2022-09-20) is TiKV-backed, not RocksDB-direct. Raw key prefix = tenantlen(2)|tenant|dbid(1)|ObjectData(1)|userkeylen(4)|userkey; then a 1-byte kind: MetaTypeKey / DataTypeKey / ScoreTypeKey. Hash data key = prefix+DataTypeKey+field; list data key = prefix+DataTypeKey+u64 index with Head=Tail=LItemInitIndex=2^32-512 and bounds [1024, 2^64-1024]; zset data key = prefix+DataTypeKey+member, score key = prefix+ScoreTypeKey+u64(score+ScoreMax)+member (int64 scores only). Meta values carry Type, Tomb, ExpireAt; expiry is checked lazily (ObjectExpired) and the ttlChecker.Run loop body is an empty switch in master; a gcChecker runs TiKV GC from a stored safe point.

codec.go comment 'tenantlen(2)|tenant|dbid(1)|typedata(1)|userkeylen(4)|userkey'; type.go consts (MetaTypeKey, DataTypeKey, ScoreTypeKey; ObjectData/ObjectTTL); t_list.go LItemInitIndex = 1<<32 - 512; t_zset.go RawZSetScoreKey uses ZScoreOffset(score)=score+ScoreMax; t_object.go ObjectExpired; ttl.go Run(): 'switch ch.dataType { case TSTRING: case THASHMETA: ... }' with empty arms; gc.go saveSafePoint/RunGC; repo README: 'This repo is archived'; GitHub API last commit 2022-09-20.

- Source: https://raw.githubusercontent.com/yongman/tidis/master/tidis/codec.go
- Confidence: verified; design-critical: yes

### 24. Tidis 2 (Rust, tidb-incubator/tidis, v0.2.1, last commit 2024-01-05) key encoding: 'x' + 2-byte instance_id + 'u' + memcomparable-encoded user key (8-byte groups + marker 0xff-pad) + kind byte ('m' meta, 'h' hash, 'l' list, 's' set, 'z' zset member, 'S' zset score) + version u16 BE + '`' + field / u64 index / score. Meta value = type byte + ttl u64 BE (ms) + version u16 (+ index_size u16 for hash/set/zset sub-meta counters, or left/right u64 for lists). Scores are f64 encoded with the sign-flip to a comparable u64 BE. Expiry is checked lazily on read (key_is_expired) and big-key deletion is asynchronous via GC version keys scanned by a tokio GC worker; MULTI/EXEC/DISCARD run as TiKV optimistic or pessimistic transactions with 1PC/async-commit options; Lua runs inside one transaction.

encode.rs constants TXN_KEY_PREFIX b'x', DATA_TYPE_USER b'u', DATA_TYPE_META b'm', DATA_TYPE_SCORE b'S', DATA_TYPE_HASH/LIST/SET/ZSET, PLACE_HOLDER b'`'; encode_bytes group/marker; encode_txnkv_list_data_key pushes idx.to_be_bytes(); encode_f64_to_cmp_uint64 with SIGN_MASK; encode_txnkv_list_meta_value = dt, ttl, version, left, right; string.rs 'if key_is_expired(ttl) { ... do_async_txnkv_string_expire_if_needed'; gc.rs run() scans encode_txnkv_gc_version_key_range on an interval; tidis-introduction.md 'two kinds of transaction models, optimistic and pessimistic', '1pc and async commit options', 'lua script will be running in a new transaction context'.

- Source: https://raw.githubusercontent.com/tidb-incubator/tidis/master/src/tikv/encoding/encode.rs
- Confidence: verified; design-critical: yes

### 25. Titan (Go, distributedio/titan, tag v0.6.0, last commit 2021-10-09, 1404 stars) is TiKV-backed. Meta key = namespace ':' DBID (fmt %03d) ':M:' key; every object has a 16-byte UUIDv4 ID and data keys are namespace ':' DBID ':D:' objectID, so DEL is O(1) by dropping the meta and queuing the data prefix for GC. Object = {ID, Type, Encoding, CreatedAt, UpdatedAt, ExpireAt}. Hash item = dkey ':' field; zset member key = dkey ':M:' member -> EncodeFloat64(score); score key = dkey ':S:' score ':' member -> nil; scores use the sign-flip big-endian trick (0.0 and -0.0 unified); lists are either a ziplist blob or a linked list keyed by float64 indexes (Lindex/Rindex, LPUSH decrements, RPUSH increments, insert-between computes a midpoint).

db.go MetaKey/DataKey append ns, ':', db.ID.Bytes(), ':M:'/':D:'; DBID.Bytes = fmt.Sprintf("%03d"); util.go UUID() = uuid.NewV4().Bytes(); object.go struct Object; hash.go hashItemKey; zset.go zsetMemberKey/zsetScoreKey; codec.go EncodeFloat64 'vi = ((vi ^ (vi >> 63)) | int64(uint64(^vi)&0x8000000000000000))'; llist.go LListMeta{Len, Lindex float64, Rindex float64}, 'if before { l.LListMeta.Lindex-- ... calculateIndex'; list.go ObjectEncodingLinkedlist/Ziplist.

- Source: https://raw.githubusercontent.com/distributedio/titan/master/db/db.go
- Confidence: verified; design-critical: yes

### 26. Titan TTL: on EXPIRE it writes an index key '$sys:0:at:' + 8-byte big-endian timestamp + ':' + metaKey; an expire worker (on the elected leader, with a lease) iterates that prefix in timestamp order, stops at ts > now, deletes the meta key, and for collections queues the data prefix under '{sys.ns}:{sys.id}:GC:{prefix}'; a GC worker deletes those ranges in batches of BatchLimit. Reads also check IsExpired (ExpireAt != 0 && ExpireAt <= now) lazily. MULTI/EXEC/DISCARD/WATCH/UNWATCH and SELECT are supported; SWAPDB, GEO*, PF*, PUBSUB/PUBLISH, EVAL/EVALSHA and X* (streams) are unchecked in the command list.

expire.go: expireKeyPrefix '$sys:0:at:', 'expireMetakeyOffset = expireTimestampOffset + 8 + len(":")', runExpire stops when ts > now, doExpire deletes meta and gcDataKey; gc.go toTiKVGCKey and gcDeleteRange with limit; docs/command_list.md '- [ ] swapdb, not supported', '- [ ] geoadd', '- [ ] pfadd', '- [ ] publish', '- [ ] eval', '- [ ] xadd', '- [x] select'.

- Source: https://raw.githubusercontent.com/distributedio/titan/master/db/expire.go
- Confidence: verified; design-critical: yes

### 27. SSDB (C++ on LevelDB, tag 1.9.9, version.h 1.9.8, last commit 2021-07-03) has no Set type and only kv/hash/zset/queue. Key prefixes are single bytes: KV 'k', HASH 'h', HSIZE 'H', ZSET 's' (member->score), ZSCORE 'z', ZSIZE 'Z', QUEUE 'q', QSIZE 'Q'. kv key = 'k'+key; hash field key = 'h' + name_len(1B) + name + '=' + field; hsize = 'H'+name; zset member key = 's' + len + name + len + member; zscore key = 'z' + len + name + ('-' if score<0 else '=') + big-endian 8-byte score + '=' + member; queue item = 'q' + len + name + big-endian u64 seq with QITEM_SEQ_INIT = INT64_MAX/2 and QFRONT_SEQ=2/QBACK_SEQ=3 reserved slots. Names are limited to 255 bytes (1-byte length).

const.h DataType constants and 'SSDB_KEY_LEN_MAX = 255', 'SSDB_SCORE_WIDTH = 9'; t_kv.h encode_kv_key; t_hash.h encode_hash_key/encode_hsize_key; t_zset.h encode_zset_key, encode_zscore_key with '-'/'=' and '#define encode_score(s) big_endian((uint64_t)(s))'; t_queue.h encode_qitem_key, QITEM_SEQ_INIT = QITEM_MAX_SEQ/2.

- Source: https://raw.githubusercontent.com/ideawu/ssdb/master/src/ssdb/const.h
- Confidence: verified; design-critical: yes

### 28. SSDB TTL works only for kv keys: ExpirationHandler stores expire-ms as the score of the key in an internal zset named "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV", keeps up to 1000 soonest keys in an in-memory SortedSet (fast_keys), and a background thread pops due keys and calls ssdb->del(key). Via the Redis protocol adapter SSDB maps redis commands to its own (del->multi_del, hlen->hsize, zadd->multi_zset, lpush->qpush_front, lrange->qslice, zrangebyscore->zscan...); DEL of a hash/zset/list is not possible from a Redis client (need hclear/zclear/qclear), and there is no MULTI, no SELECT, no SET type in the adapter table.

ttl.cpp '#define EXPIRATION_LIST_KEY "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV"', '#define BATCH_SIZE 1000', set_ttl -> ssdb->zset(list_name, key, expire_str), expire_loop -> ssdb->del(key); link_redis.cpp cmds_raw[] table; ssdb-docs redis-to-ssdb.md: 'del (not supported) -> hclear', 'You cannot delete a hash with any Redis client', 'setx(for kv type only)'.

- Source: https://raw.githubusercontent.com/ideawu/ssdb/master/src/ssdb/ttl.cpp
- Confidence: verified; design-critical: yes

### 29. LedisDB (Go, tag v0.6, last commit 2020-05-10, 4115 stars; backends goleveldb default, leveldb, rocksdb 5.1+, RAM) prefixes every key with the DB index as a uvarint (indexVarBuf) then a type byte: KV=1, Hash=2/HSize=3, List=4/LMeta=5, ZSet=6/ZSize=7/ZScore=8, Set=11/SSize=12, ExpMeta=102, ExpTime=103. Hash field key = idx + HashType + u16 keylen + key + sep + field; set member key likewise with SetType; list item key = idx + ListType + u16 keylen + key + int32 seq with head/tail starting at listInitialSeq = midpoint of [1000, 2^31-1000] (LPUSH seq-1, RPUSH seq+1); zset scores are int64 with score key = idx + ZScoreType + u16 keylen + key + ('<' if negative else '=') + big-endian u64 score + ':' + member. 16 DBs by default (SELECT 0-15), MaxDatabases 10240.

const.go type constants and MaxDatabases 10240; ledis_db.go binary.PutUvarint(index) into indexVarBuf; t_kv.go encodeKVKey; t_hash.go hEncodeHashKey; t_set.go sEncodeSetKey; t_list.go listHeadSeq/listTailSeq, listMinSeq 1000, listMaxSeq 1<<31-1000, listInitialSeq midpoint; t_zset.go zEncodeScoreKey with zsetNScoreSep '<' and zsetPScoreSep '<'+1, MinScore/MaxScore int64; doc/commands.md 'Currently, We support 16 DBs(0-15)'; README backends list.

- Source: https://raw.githubusercontent.com/ledisdb/ledisdb/master/ledis/t_zset.go
- Confidence: verified; design-critical: yes

### 30. LedisDB TTL: EXPIRE writes a time-index key idx + ExpTimeType + big-endian u64 when + dataType + key -> metaKey, and a meta key idx + ExpMetaType + dataType + key -> when. A ticker (TTLCheckInterval seconds, default 1) runs each DB's ttlChecker, which range-scans the time keys up to now+3600, deletes due keys via per-type callbacks and removes both index entries. GET does not check expiry lazily (it just reads the KV key). There is no MULTI/EXEC: the server has cmd_kv/hash/list/set/zset/scan/script/server files and the commands doc has no transaction section; Lua via gopher-lua is supported.

t_ttl.go expEncodeTimeKey/expEncodeMetaKey layouts; ttlChecker.check() 'nc = now + 3600', RangeLimitIterator(minKey, maxKey), 't.Delete(tk); t.Delete(mk); t.Commit()'; ledis.go checkTTL 'if l.cfg.TTLCheckInterval == 0 { l.cfg.TTLCheckInterval = 1 }' with time.NewTicker(...*time.Second); t_kv.go Get() = db.bucket.Get(encodeKVKey(key)); server dir listing has no cmd_multi; doc/commands.md sections KV/Hash/List/Set/ZSet/Scan/Sort/Replication/Server/Script.

- Source: https://raw.githubusercontent.com/ledisdb/ledisdb/master/ledis/t_ttl.go
- Confidence: verified; design-critical: yes

### 31. Ardb (C++, yinqiwen/ardb, last release v0.9.7 2018-04-26, last commit 2019-04-25) is multi-engine (RocksDB default, LevelDB, LMDB, WiredTiger, PerconaFT, ForestDB). Keys are typed objects: [namespace][varuint32 keylen][key][type byte][element count byte][elements...] compared by a custom RocksDBComparator (compare_keys) rather than bytewise. Types: KEY_META=1, KEY_STRING=2, KEY_HASH_FIELD=4, KEY_LIST_ELEMENT=6, KEY_SET_MEMBER=8, KEY_ZSET_SORT=10 (elements: score, member), KEY_ZSET_SCORE=11 (member->score), KEY_STREAM*=12-14, KEY_TTL_SORT=29 (elements: ttl, namespace, key). MetaObject holds format, ttl, size, list_sequential.

codec.hpp enum KeyType and struct KeyObject {ns, type, key, elements}; codec.cpp KeyObject::EncodePrefix writes varuint32 key length, key bytes, type byte, then element count and elements; SetType resizes elements (ZSET_SORT 2, TTL_SORT 3 with comment '0: ttl 1:namespace 2:ttl key'); struct MetaObject {format, ttl, size, list_sequential, stream_last_id}; rocksdb_engine.cpp class RocksDBComparator : rocksdb::Comparator using compare_keys; README engine list; releases API v0.9.7 2018-04-26.

- Source: https://raw.githubusercontent.com/yinqiwen/ardb/master/src/db/codec.hpp
- Confidence: verified; design-critical: yes

### 32. Ardb maps each namespace (SELECT accepts any string, not just an integer) to its own RocksDB column family created on demand, and uses a RocksDB compaction filter on KEY_META entries: when meta.ttl > 0 && ttl <= now it calls g_db->AddExpiredKey(ns, key) and keeps the entry (returns false) rather than dropping inline; a TTL_SORT index scanned by ScanTTLDB is used only for engines without compaction-filter support and not on slaves. MULTI/EXEC/WATCH/UNWATCH/DISCARD, EVAL, PUBSUB, streams (XADD..XPENDING), and GEOADD/GEODIST/GEOHASH/GEOPOS/GEORADIUS/GEORADIUSBYMEMBER are supported; DBSIZE is an estimate; extra commands include COMPACTDB, COMPACTALL, KEYSCOUNT, S*COUNT, IMPORT, MIGRATEDB.

rocksdb_engine.cpp GetColumnFamilyHandle: 'm_db->CreateColumnFamily(cf_options, name, &cfh)' with name = ns string; RocksDBCompactionFilter::Filter: 'if (meta.GetTTL() > 0 && meta.GetTTL() <= get_current_epoch_millis()) { g_db->AddExpiredKey(ns, k.GetKey()); return false; }', 'do not do filter for slave'; db.cpp ScanTTLDB comment 'only works with engine that has no compactfilter support', KEY_TTL_SORT scan with max_scan_keys_one_iter = 1000; ARDB-Commands wiki: 'The db id argument can be any string not just integer', DBSIZE 'Return the estimate k/v pair number', MULTI/EXEC/WATCH listed.

- Source: https://raw.githubusercontent.com/yinqiwen/ardb/master/src/db/rocksdb/rocksdb_engine.cpp
- Confidence: verified; design-critical: yes

### 33. Ardb's 2D spatial index is the same design as Redis/Kvrocks: encode lon/lat with geohash-int at 26 steps into a 52-bit integer (about 0.6 m precision), ZADD it as the score, and for radius search compute the centre cell plus 8 neighbours at a step matching the radius, run ZRANGEBYSCORE per cell, then filter by real distance; v0.9 replaced its own GEOSEARCH with Redis's GEORADIUS.

Spatial-Index wiki: 'encode ... with 26 steps which would generate a 52bit integer value ... distance precision about 0.6 meters', 'Find surrounding 8 neighbors', 'use ZRANGEBYSCORE key min max WITHSCORES', 'compare the distance with given radius value to exclude the point not in radius', 'v0.9 use redis's command(georadius) instead'.

- Source: https://github.com/yinqiwen/ardb/wiki/Spatial-Index
- Confidence: verified; design-critical: yes

### 34. Go binding grocksdb (linxGnu/grocksdb, tag v1.11.1) is cgo over the RocksDB C API and requires a prebuilt librocksdb (CGO_CFLAGS/CGO_LDFLAGS to -lrocksdb -lstdc++ -lz -lsnappy -llz4 -lzstd); it claims support for RocksDB 11.x down to 6.x and exposes checkpoint.go, compaction_filter.go, write_batch.go, write_batch_wi.go (WriteBatchWithIndex), transactiondb.go, optimistic_transaction_db.go, snapshot and column-family files, plus a testing_darwin_arm64.go.

README: 'Supports almost C API (unlike upstream). Catching up with latest version of Rocksdb', CGO_LDFLAGS lines; repo file listing includes write_batch_wi.go, transactiondb.go, optimistic_transaction_db.go, checkpoint.go, compaction_filter.go; page summary lists RocksDB '11.x, 10.x, 9.x, 8.x, 7.x, 6.x' and 'testing_darwin_arm64.go'; tags API v1.11.1.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/README.md
- Confidence: verified; design-critical: yes

### 35. Rust crate rocksdb (rust-rocksdb) v0.25.0 statically links a bundled RocksDB via librocksdb-sys 0.19.0+11.8.1 (RocksDB 11.8.1), with crate features for snappy/lz4/zstd/zlib/bzip2, jemalloc, io-uring, multi-threaded-cf and bindgen-static/runtime; its src exposes checkpoint.rs, compaction_filter.rs, compaction_filter_factory.rs, comparator.rs, merge_operator.rs, snapshot.rs, write_batch.rs, sst_file_writer.rs, backup.rs and a transactions module (TransactionDB / OptimisticTransactionDB). No write_batch_wi file is present.

Cargo.toml name = "rocksdb", version = "0.25.0", features jemalloc/io-uring/mt_static/snappy/lz4/zstd; librocksdb-sys/Cargo.toml version = "0.19.0+11.8.1"; README 'This binding is statically linked with a specific version of RocksDB', 'multi-threaded-cf'; tags v0.25.0/v0.24.0/v0.23.0; src listing as given.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/librocksdb-sys/Cargo.toml
- Confidence: verified; design-critical: yes

### 36. Project activity snapshot (GitHub API, 2026-09-26): Kvrocks pushed 2026-09-26 (v2.17.0); Pika pushed 2026-09-16 (v4.0.4-alpha); Tidis-Rust last commit 2024-01-05 (v0.2.1, 527 stars); Titan last commit 2021-10-09 (v0.6.0); Tidis-Go last commit 2022-09-20 (archived); SSDB last commit 2021-07-03 (tag 1.9.9, 8317 stars); LedisDB last commit 2020-05-10 (v0.6); Ardb last commit 2019-04-25 (v0.9.7). Only Kvrocks and Pika are live; both are C++.

commits?per_page=1, tags and repo endpoints returned the dates/tags listed for each repository.

- Source: https://api.github.com/repos/distributedio/titan
- Confidence: verified; design-critical: no

### 37. Pika's Floyd proposal includes BlobDB-aware TTL filtering (store TTL in the BlobIndex so the compaction filter can use FilterBlobByKey without reading blob files), RocksDB TTL-triggered compaction for stale SSTs, and manual compaction of a key's prefix range when a slow key is detected; Kvrocks' SubKeyFilter likewise implements FilterBlobByKey.

Floyd discussion #2052: 'stores TTL in the BlobIndex structure ... leverages RocksDB's FilterBlobByKey interface', 'built-in TTL-based compaction triggering', 'initiates manual compaction on the corresponding key prefix range'; Kvrocks compact_filter.cc SubKeyFilter::FilterBlobByKey returning kRemove/kKeep/kUndetermined.

- Source: https://github.com/OpenAtomFoundation/pika/discussions/2052
- Confidence: verified; design-critical: yes

### 38. Kvrocks bitmaps are split into 1 KiB fragments (8192 bits) with the fragment index as the subkey; hash fields can carry their own expiry (a 'field expiration' metadata mode with mode/persist/lower/upper fields and 8-byte expire prefix on each subkey value), and the compaction filters have special cases for it.

Design doc: 'We break the bitmap values into fragments(1KiB, 8192 bits/fragment), and subkey is the index of the fragment'; design doc hash section 'Field expiration encoding adds | mode | persist | lower | upper |' and 'key|version|field => | expire(8) | value |'; compact_filter.cc IsExpiredHashFieldSubkey / HasHashFieldExpirationCandidates.

- Source: https://kvrocks.apache.org/community/data-structure-on-rocksdb/
- Confidence: verified; design-critical: no

### 39. None of the seven projects implements GeoJSON geometries, $geoWithin/$geoIntersects, or any spatial index beyond point geohash-in-zset; Titan and Tidis have no GEO at all, SSDB and LedisDB have no GEO, and Kvrocks/Pika/Ardb only implement Redis GEO* over 52-bit geohash scores.

Kvrocks redis_geo.cc and Ardb Spatial-Index wiki describe geohash-in-zset; Titan command_list.md leaves geoadd unchecked; Tidis docs list only string/hash/list/set/zset; SSDB const.h has no geo type; LedisDB commands doc has KV/Hash/List/Set/ZSet only; Pika ships pika_geo.cc/pika_geohash.cc (Redis-derived).

- Source: https://raw.githubusercontent.com/distributedio/titan/master/docs/command_list.md
- Confidence: verified; design-critical: yes
