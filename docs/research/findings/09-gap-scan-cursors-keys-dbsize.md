# Implementing Redis SCAN cursors, KEYS and DBSIZE over RocksDB in NilDB: what Kvrocks, Redis, Dragonfly and RocksDB actually do

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `gap:4`, agent `a2edb90df7c52d019`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Keep the true scan position as the last returned RocksDB key, exactly as Kvrocks does. An ordered iterator that resumes with Seek(last_key)+Next over the current state already satisfies both Redis guarantees (a key present start-to-end is at or after every cursor position when the cursor passes it; a key deleted before the scan is never read), and it does better than Redis on duplicates: a keyed cursor never returns the same key twice. Port the scan.tcl 'guarantees check under write load' test on day one.
2. Translate that key into the u64 the wire contract requires with a bounded in-memory ring, as Kvrocks does (16384 entries, index = low counter bits, roughly 1-2 MB). Reserve bit fields deliberately: Kvrocks spends 16 counter, 16 timestamp, 29 hash, 3 type. NilDB also needs a Redis database index (SELECT) and possibly a snapshot or tenant id; budget bits for those (e.g. 16 counter | 16 epoch | 4 db | 3 type | 25 hash) and make the ring size a config value rather than a compile-time constant.
3. Decide explicitly what an unknown, evicted, stale, or wrong-type cursor does. Kvrocks silently restarts from 0, which keeps redis-cli happy in the common case but silently violates 'full iteration returns every key' and can make a slow client with more than 16384 in-flight cursors loop forever. Redis permits undefined behaviour but returns 'invalid cursor' for malformed input. Recommended: 'invalid cursor' error for non-numeric input (matches Redis and Dragonfly), and a configurable policy for evicted cursors (error by default, silent restart as an opt-in), plus an INFO counter for cursor misses.
4. Cursors die on restart in Kvrocks because the ring is heap-only. For NilDB, either document this (cheap) or persist the ring in a small system column family written with the same write batch; 16384 rows of key names is trivial for RocksDB and makes 'redis-cli --scan' survive a rolling restart. Either way include an epoch in the cursor so stale cursors from a previous process life are detected rather than mis-resolved.
5. Store the type byte and the absolute expiry in the metadata value (Kvrocks layout: 1-byte flags, 8-byte expire, 8-byte version, size). Then SCAN's TYPE filter and expiry check cost one small value decode per visited key with no extra Get, and KEYS/SCAN can skip expired rows in place. Because filtered rows do not count toward COUNT, add Redis's work cap (count*10 visited rows per call, then return whatever was collected plus a non-zero cursor). Kvrocks has no such cap and a SCAN TYPE for a rare type, or a keyspace full of expired keys, can traverse the whole column family in a single call.
6. Expired keys must eventually leave storage. Copy the Kvrocks pattern: a compaction filter on the metadata CF that drops rows whose expire is in the past (and a subkey filter that drops orphaned fields whose metadata version no longer exists), plus a scheduled compaction picker that targets SST files with a high deleted percentage. Expose the compaction filter and the picker in NilDB's native RocksDB surface rather than hiding them, in line with the project's 'RocksDB feel' goal.
7. Implement KEYS as Kvrocks does: split the glob into a literal prefix and a suffix pattern, Seek to namespace+prefix, stop when keys leave the prefix, skip expired metadata. Run it on a snapshot or a consistent iterator, mark it @dangerous in the command table, and consider a configurable result cap since Redis documents it as O(N) with no bound.
8. DBSIZE needs a conscious choice among three costs. (a) Kvrocks: O(1) read of a cached count, refreshed only by an explicit asynchronous 'DBSIZE SCAN' or cron; deviates from Redis (returns 0 until scanned) and is a documented Kvrocks quirk. (b) rocksdb.estimate-num-keys: O(1) but computed as entries minus deletions from table properties and memtables, wrong under overwrites and lazy expiry, sampled from at most 20 files right after open, and blind to DeleteRange. Fine for INFO, not for DBSIZE. (c) An exact live counter maintained in the same write batch as each metadata put/delete (Dragonfly's per-shard model): Redis-exact and O(1), but requires knowing on SET whether the key already existed (a bloom-filtered Get on the metadata CF) and, like Redis, counts keys that expired but were not yet reclaimed. Recommended for NilDB: (c) for DBSIZE, with the counter kept in a system CF and reconciled by an optional 'DBSIZE SCAN' style recount; expose rocksdb.estimate-num-keys per CF in INFO and label it an estimate.
9. Redis DBSIZE and INFO keyspace include logically expired keys not yet reclaimed by active or lazy expiry, so an exact counter that only decrements on actual delete is faithful to Redis, not a bug. If NilDB adds active expiry (a periodic scan of a TTL index CF), the counter should decrement there too.
10. redis-cli --scan, --bigkeys and --memkeys are the practical conformance tests: they strtoull the cursor and loop until 0. Add an integration test that populates 1000 keys and asserts 'Sampled 1000 keys in the keyspace', as Kvrocks does, and run it in both cursor modes if a string-cursor debug mode is kept.
11. Namespacing: Kvrocks stores the bare user key in the ring and re-applies the connection's namespace on resume, so cursors are shared across connections by design and are not isolated per namespace. For NilDB with SELECT-style databases, encode the db index in the cursor and refuse a cursor used against a different db instead of silently seeking in the wrong keyspace.
12. None of the above depends on Go versus Rust: both need a RocksDB binding with iterators, snapshots, write batches, compaction filters and GetIntProperty. The ring is a fixed array plus an atomic counter in either language. Compaction filters are the one place where a Rust binding (rust-rocksdb) or Go binding (grocksdb) must expose a callback into RocksDB; check that the chosen binding supports compaction filters before committing to it.

## Open questions

- Kvrocks' Database::Scan has no bound on skipped rows per call; the actual latency of SCAN TYPE for a rare type, or SCAN over a keyspace with millions of expired-but-uncompacted keys, should be measured before deciding NilDB's per-call work cap.
- How many concurrent scanners saturate a 16384-entry ring in practice (for example, several redis-cli --bigkeys runs plus application SCAN loops)? Needs a load test to size the ring and to decide whether eviction should be an error or a silent restart.
- Is persisting the cursor ring across restarts worth the write amplification, or is a cursor epoch plus documented 'cursors do not survive restart' enough for the intended Redis-replacement use?
- For an exact DBSIZE counter, what does the existence check on every SET cost in NilDB's write path (bloom-filtered metadata Get versus a merge operator), and how will FLUSHDB (DeleteRange) reset it atomically?
- How badly does rocksdb.estimate-num-keys drift under NilDB's actual workload (overwrites, TTL churn, DeleteRange) compared with the exact counter; worth exposing both in INFO to quantify.
- Kvrocks unstable was read; the shipped v2.17.0 (2026-09-19) may differ in small ways (for example scan_base.h default COUNT or the cluster slot loop). Confirm against the tag before citing line-level behaviour in NilDB docs.
- Redis unstable (post-8.x kvstore code) was read for scanCallback and dbSize; older Redis 7.x code paths differ in structure but not in the guarantees. Decide which Redis version NilDB targets for its conformance suite.
- Dragonfly's DbSlice::DbSize was not read; confirm it is a maintained counter (likely) if its per-shard model is used as the reference for NilDB's counter design.
- How should HSCAN/SSCAN/ZSCAN cursors share the ring with SCAN in NilDB: Kvrocks uses 3 type bits so a cursor from one command is rejected by another; NilDB also needs to decide whether small compound types return everything in one call (Redis does for listpack encodings) or always page.

## Findings (25)

### 1. Redis's SCAN contract: a full iteration returns every element present from start to end, never returns an element absent throughout, may return duplicates, and the cursor is a string holding an unsigned 64-bit number. A corrupted cursor yields undefined behaviour but must not crash. No server-side state is kept.

The SCAN page says a full iteration 'always retrieves all the elements that were present in the collection from the start to the end' and 'never returns any element that was NOT present ... from the start to the end'; duplicates and transiently-present elements are undefined. Reply: 'the first element is a string representing an unsigned 64 bit number (the cursor)'. Corrupted cursor: 'undefined behavior but never in a crash'. 'No server side state is taken at all.' Termination is only guaranteed if the collection stays bounded. Default COUNT is 10 and is 'just a hint'.

- Source: https://redis.io/docs/latest/commands/scan/
- Confidence: verified; design-critical: yes

### 2. Redis rejects a non-numeric cursor with an 'invalid cursor' error, and redis-cli --scan/--bigkeys/--memkeys parse the returned cursor with strtoull and loop until it is 0. A string cursor (Kvrocks' old behaviour) parses to 0 and ends the loop after one page.

db.c: parseScanCursorOrReply does `if (!string2ull(o->ptr, cursor)) { addReplyError(c, "invalid cursor"); return C_ERR; }`. redis-cli.c sendScan(): `*it = strtoull(reply->element[0]->str, NULL, 10);` and scanMode() loops `while(force_cancel_loop == 0 && cur != 0)`. Kvrocks discussion #417 documents redis-py breaking on string cursors and a maintainer explaining 'we use the iterated key as cursor, so the cursor is string type'.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/redis-cli.c
- Confidence: verified; design-critical: yes

### 3. Redis's keyspace SCAN filters the pattern first, then lazily expires the key (skipping it if expired), then applies the TYPE filter; unknown TYPE names silently match nothing rather than erroring; work per call is capped at count*10 hash-table steps.

db.c scanCallback(): pattern check via stringmatchlen, then `if (expireIfNeeded(data->db, NULL, kv, 0) != KEY_VALID) return;`, then 'Type filtering - only for database keyspace scanning' with 'For unknown types (LLONG_MAX), skip all keys'. scanGenericCommand: `long maxiterations = (count > LONG_MAX / 10) ? LONG_MAX : count * 10;` with comment about avoiding blocking on sparse tables. The SCAN doc adds that TYPE 'is also applied after elements are retrieved ... so the option does not reduce the amount of work'. Redis unstable (version.h 255.255.255) at time of reading.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/db.c
- Confidence: verified; design-critical: yes

### 4. Redis's dictScan uses reverse-binary cursor increment so iteration survives hash-table resize; it guarantees all elements present throughout are returned, at the cost of possible duplicates and having to return whole buckets per step.

dict.c comment above dictScan: 'The function guarantees all elements present in the dictionary get returned between the start and end of the iteration. However it is possible some elements get returned multiple times.' Algorithm by Pieter Noordhuis: 'the bits of the cursor are reversed, then the cursor is incremented, and finally the bits are reversed again'. Limitations listed: duplicates, must return all keys chained in a bucket, hard to understand.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/dict.c
- Confidence: verified; design-critical: no

### 5. Redis's own regression test for the SCAN guarantee ('SCAN guarantees check under write load') populates 100 keys, writes 10 random extra keys after every SCAN step, and asserts all 100 original keys were reported. Companion tests cover expired keys, TYPE, unknown TYPE, and SSCAN under deletes (#4906).

scan.tcl: `populate 100`, loop `r scan $cur` ... `for {set j 0} {$j < 10} {incr j} { r set addedkey:[randomInt 1000] foo }`, then filter to names <= 6 chars and `assert_equal 100 [llength $keys2]`. 'SCAN with expired keys' disables active expire, adds an expired string and hash, scans and asserts exactly 1000 keys returned. 'SCAN unknown type' asserts 0 keys for type 'string1' with no error.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/tests/unit/scan.tcl
- Confidence: verified; design-critical: yes

### 6. Redis DBSIZE is O(1) from an in-memory counter and counts keys that have logically expired but have not yet been reclaimed.

DBSIZE doc: complexity O(1), 'Return the number of keys in the currently-selected database.' db.c: `void dbsizeCommand(client *c) { addReplyLongLong(c,dbSize(c->db)); }` and `dbSize()` returns `kvstoreSize(db->keys)`. scan.tcl 'SCAN with expired keys with TYPE filter and PATTERN filter': 1000 keys plus three expired keys; SCAN with `match key*` lazily deletes the two matching expired keys, returns 1000, and `info keyspace` then reports keys=1001 because the non-matching expired key 'boo' was never touched.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/db.c
- Confidence: verified; design-critical: yes

### 7. Kvrocks' Redis-compatible cursor timeline: PR #1489 (merged 2023-06-25) shipped in v2.5.0 (2023-07-12) with the option off; v2.8.0 (2024-03-08) flipped the shipped kvrocks.conf to yes (PR #2103); PR #2429 (merged 2024-07-16) changed the compiled-in default to yes. The task's premise 'default since v2.5.0' is wrong. Latest release is v2.17.0 (2026-09-19).

GitHub API: PR 1489 merged_at 2023-06-25; v2.5.0 notes 'Enable it via redis-cursor-compatible in the configuration file'; v2.8.0 notes 'Set the default value of redis-cursor-compatible to yes ... Kvrocks' SCAN will be compatible with Redis by default' via PR #2103 'Set the redis-cursor-compatible field to yes in the initial configuration'; PR 2429 'feat(config): change redis-cursor-compatible default value to yes' merged 2024-07-16. config.cc: `{"redis-cursor-compatible", false, new YesNoField(&redis_cursor_compatible, true)}`. releases/latest: v2.17.0 published 2026-09-19T12:02:09Z. kvrocks.conf: 'If enabled, the cursor will be unsigned 64-bit integers. If disabled, the cursor will be a string. Default: yes'.

- Source: https://github.com/apache/kvrocks/pull/1489
- Confidence: verified; design-critical: no

### 8. Kvrocks' numeric cursor packs four fields into 64 bits: bits 0-15 a global atomic counter, bits 16-31 a seconds timestamp, bits 32-60 a hash of the key name (top 3 bits masked off), bits 61-63 the cursor type (SCAN/HSCAN/SSCAN/ZSCAN).

server.cc NumberCursor ctor: `auto hash = static_cast<uint32_t>(std::hash<std::string>{}(key_name)); auto time_stamp = static_cast<uint16_t>(util::GetTimeStamp()); constexpr uint64_t hash_mask = 0x1FFFFFFFFFFFFFFF; cursor_ = counter | time_stamp << 16 | (hash << 32 & hash_mask) | cursor_type << 61;` server.h enum CursorType { kTypeNone=0, kTypeBase=1 (SCAN), kTypeHash=2, kTypeSet=3, kTypeZSet=4 }. Comment: 'hash is used to prevent information leakage. The time_stamp is used to prevent the generation of the same cursor in the extremely short period before and after a restart.'

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/server/server.cc
- Confidence: verified; design-critical: yes

### 9. Kvrocks' cursor dictionary is a fixed in-memory ring of 16384 entries indexed by cursor % 16384 (equal to the low counter bits), overwritten in place; it holds only the last 16384 cursors issued server-wide, about 640 KB plus key strings (1-2 MB).

server.h: `constexpr const size_t CURSOR_DICT_SIZE = 1024 * 16;` with static_asserts 'must be 2^n' and '<= 2^16'; `struct CursorDictElement { NumberCursor cursor; std::string key_name; }`; `CursorDictType = std::array<CursorDictElement, CURSOR_DICT_SIZE>`; `GetIndex() { return cursor_ % CURSOR_DICT_SIZE; }`. server.cc GenerateCursorFromKeyName: `auto counter = cursor_counter_.fetch_add(1); ... cursor_dict_->at(number_cursor.GetIndex()) = {number_cursor, key_name};`. PR #1489: 'occupies about 640KB of memory. Including the length of the referenced keyname strings, its size is about 1-2M' and 'guarantees the validity of the latest 16384(1024*16) cursors'.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/server/server.h
- Confidence: verified; design-critical: yes

### 10. In Kvrocks an evicted, stale, foreign, or wrong-type cursor is silently treated as cursor 0 and the scan restarts from the beginning; no error is returned.

server.cc GetKeyNameFromCursor: parses with ParseInt<uint64_t>; 'When Cursor 0 or not a Integer return empty string'; looks up `cursor_dict_->at(number_cursor.GetIndex())`, returns key_name only if `IsMatch` (`cursor_ == element.cursor.cursor_ && cursor_type == getCursorType()`), else `return {}`. Empty key_name makes Database::Scan SeekToFirst. PR #1489: 'For invalid cursors, we treat them as a 0 cursor, which means we will start iterating over the collection from the beginning.' Kvrocks' scan_test.go has no test for garbage or stale cursors.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/server/server.cc
- Confidence: verified; design-critical: yes

### 11. Kvrocks cursors do not survive a process restart; the timestamp field only lowers the odds that an old cursor accidentally matches a new dictionary entry.

The dictionary is a `std::unique_ptr<CursorDictType>` member of Server with no persistence path in server.cc. Zhihu design article (Kvrocks project account, 2023-09-21): '由于字符串游标和整数映射存储在内存中，重启实例会导致游标失效' (because the string-to-integer mapping lives in memory, restarting the instance invalidates cursors) and describes the 16*1024 overwrite as an LRU-style limit. PR #1489: 'We add a timestamp to the cursor to ensure that the same cursor does not appear within a short period of time before and after a restart.'

- Source: https://zhuanlan.zhihu.com/p/657748948
- Confidence: verified; design-critical: yes

### 12. Kvrocks' cursor dictionary is global across connections and namespaces: the stored key_name has no namespace, and the connection's namespace is re-applied at scan time, so a cursor is intentionally reusable from another connection but is not isolated per namespace.

server.cc stores `{number_cursor, key_name}` where key_name is the user key passed from CommandScan (`keys.back()` / end_key). redis_db.cc Database::Scan: `std::string ns_cursor = AppendNamespacePrefix(cursor);` using the Database's namespace_ (constructed with conn->GetNamespace()). PR #1489: 'Our cursor is globally visible ... using the same cursor in different connections will produce the same results.'

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/redis_db.cc
- Confidence: verified; design-critical: yes

### 13. With redis-cursor-compatible off, Kvrocks returns the last key itself prefixed with '_' as the cursor and strips that prefix on input; '0' maps to the empty (start) cursor.

scan_base.h: `inline constexpr const char *kCursorPrefix = "_";` PutCursor(): `if (cursor_ == "0") cursor_ = std::string(); else cursor_ = cursor_.find(kCursorPrefix) == 0 ? cursor_.substr(strlen(kCursorPrefix)) : cursor_;`. cmd_server.cc CommandScan::GenerateOutput passes kCursorPrefix to GenerateCursorFromKeyName, which returns `prefix + key_name` when compatibility is off.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/commands/scan_base.h
- Confidence: verified; design-critical: no

### 14. Kvrocks' keyspace SCAN is a fresh RocksDB iterator over the metadata column family: Seek to namespace+cursor key, step past it, decode each metadata value, skip on TYPE mismatch and on expiry, glob-match the suffix, stop when COUNT (default 20) keys are collected, and return the last returned key as the next cursor. Skipped entries do not count toward COUNT and there is no cap on how many entries a single call may skip.

redis_db.cc Database::Scan: `auto iter = util::UniqueIterator(ctx, ctx.GetReadOptions(), metadata_cf_handle_);` ... `iter->Seek(ns_cursor); if (iter->Valid()) iter->Next();` ... `for (; iter->Valid() && cnt < limit; iter->Next()) { ... metadata.Decode(iter->value()); if (type != kRedisNone && type != metadata.Type()) continue; if (metadata.Expired()) continue; ... if (!util::StringMatch(suffix_glob, user_key.substr(prefix.size()))) continue; keys->emplace_back(user_key); cnt++; }` and `if (!keys->empty() && cnt >= limit) end_cursor->append(user_key);`. scan_base.h: `int limit_ = 20;`. Unlike Redis's `maxiterations = count*10`, nothing bounds skipped rows.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/redis_db.cc
- Confidence: verified; design-critical: yes

### 15. Kvrocks' TYPE filter costs one metadata-value decode per visited key (1-byte flags, 4- or 8-byte expire, then 8-byte version and 4/8-byte size for compound types); the type lives in the flags byte, so no subkey lookup is needed, but every key in the range is still read.

redis_metadata.cc Metadata::Decode: `GetFixed8(input, &flags)`, `GetExpire(input)`, then for non-single-KV types `GetFixed64(input, &version); GetFixedCommon(input, &size);`. Data-structure design doc: 'flags (1byte) | expire (Ebyte) | version (8byte) | size (Sbyte)', 'The encoding version (currently 0 or 1) and data type is encoded in the flags field', version 0 uses 4-byte expire/size, version 1 uses 8 bytes. scan_base.h maps the TYPE argument through RedisTypeNames and returns 'Invalid type' for unknown names (Redis instead returns zero keys).

- Source: https://kvrocks.apache.org/community/data-structure-on-rocksdb/
- Confidence: verified; design-critical: yes

### 16. Kvrocks treats a key as expired when its metadata expire field is non-zero and earlier than now (ms), or when a non-emptyable compound type has size 0; SCAN/KEYS skip such rows in place and a compaction filter on the metadata CF physically drops them later, with a cron-driven compaction checker choosing SST files by deleted percentage.

redis_metadata.cc: `bool Metadata::ExpireAt(uint64_t expired_ts) const { if (!IsEmptyableType() && size == 0) return true; if (expire == 0) return false; return expire < expired_ts; }` and `Expired() { return ExpireAt(util::GetTimeStampMS()); }`. compact_filter.cc MetadataFilter::Filter decodes the value, sets `expired = metadata.Expired()` (plus hash-field-expiration check for hashes) and `return expired;` (true = drop). kvrocks.conf: 'compaction-checker-cron * 0-7 * * *' and 'the db will periodically pick the SST file with the highest "deleted percentage" ... to compact'; force-compact-file-age / force-compact-file-min-deleted-percentage also documented.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/compact_filter.cc
- Confidence: verified; design-critical: yes

### 17. Kvrocks' KEYS is a full metadata-CF iteration bounded only by the literal glob prefix: it splits the pattern into prefix and suffix glob, seeks to namespace+prefix, and stops when keys leave that prefix, skipping expired rows.

cmd_server.cc CommandKeys: `ValidateGlob`, `const auto [prefix, suffix_glob] = util::SplitGlob(glob_pattern); redis.Keys(ctx, prefix, suffix_glob, &keys)`. redis_db.cc Database::Keys: `ns_prefix.empty() ? iter->SeekToFirst() : iter->Seek(ns_prefix); for (; iter->Valid(); iter->Next()) { if (!ns_prefix.empty() && !iter->key().starts_with(ns_prefix)) break; ... if (metadata.Expired()) { if (stats) stats->n_expired++; continue; } ...}`. Redis KEYS doc: O(N), '@dangerous', 'Use extreme care when using this command in production environments'.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/commands/cmd_server.cc
- Confidence: verified; design-critical: no

### 18. Kvrocks DBSIZE returns a cached per-namespace count from the last 'DBSIZE SCAN' (0 if never run); 'DBSIZE SCAN' launches an asynchronous full iteration of the metadata CF that counts live keys, keys with TTL, expired-but-unreclaimed keys and average TTL; a second scan on the same namespace is refused while one runs; an optional cron reschedules it.

cmd_server.cc CommandDBSize: `if (args_.size() == 1) { srv->GetLatestKeyNumStats(ns, &stats); *output = redis::Integer(stats.n_key); } else if (... "scan") { Status s = srv->AsyncScanDBSize(ns); ...} else return {Status::RedisExecErr, "DBSIZE subcommand only supports scan"};`. server.cc AsyncScanDBSize: `if (db_scan_infos_[ns].is_scanning) return {Status::NotOK, "scanning the db now"};` then task_runner_.TryPublish running `db.GetKeyNumStats(ctx, "", &stats)` and storing key_num_stats plus last_scan_time_secs. GetKeyNumStats = Keys(ctx, prefix, "*", nullptr, stats) counting n_key, n_expires, n_expired, avg_ttl (KeyNumStats in redis_metadata.h). Cron: `if (config_->dbsize_scan_cron.IsEnabled() && ...IsTimeMatch(&now))` iterates all namespaces. FAQ: 'Kvrocks doesn't store the key number directly. It needs to scan the DB and then retrieve the key number by using the dbsize scan command.' Supported-commands page: 'The response of DBSIZE and keyspace section of INFO is updated asynchronously after executing DBSIZE SCAN command.'

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/commands/cmd_server.cc
- Confidence: verified; design-critical: yes

### 19. Kvrocks' INFO exposes both the cached scan counts (keyspace section with last_dbsize_scan_timestamp) and RocksDB's per-CF estimate_keys, and documents the estimate as possibly including tombstones and expired keys.

INFO sections doc: keyspace 'db0' line format 'keys=...,expires=...,avg_ttl=...,expired=...'; 'Key counts are cached rather than maintained in memory for every write. Run DBSIZE scan to start an asynchronous scan that refreshes them.'; last_dbsize_scan_timestamp 0 means no scan yet. RocksDB section: 'estimate_keys[xxx]' described as 'Estimated number of keys; may include tombstones and expired keys'.

- Source: https://kvrocks.apache.org/docs/info-sections/
- Confidence: verified; design-critical: yes

### 20. Kvrocks verifies its cursor design with a port of Redis's write-load guarantee test and by running redis-cli --bigkeys and --memkeys end to end with redis-cursor-compatible on; it has no tests for expired keys during SCAN or for stale/garbage cursors.

tests/gocase/unit/scan/scan_test.go contains `t.Run("SCAN guarantees check under write load", ...)` asserting `require.Len(t, originKeys, 100)`, plus TestScanWithNumberCursor / TestScanWithStringCursor and 'SCAN with type args'. tests/gocase/integration/cli/cli_test.go 'Test redis-cursor-compatible mode': FlushAll, Populate 1000 keys, ConfigSet redis-cursor-compatible yes, then `runCli(t, srv, nil, "--bigkeys")` and `"--memkeys"` each `require.Contains(t, r, "Sampled 1000 keys in the keyspace")`. PR #1489: 'Added tests for redis-cli --bigkey and redis-cli --memkeys commands.'

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/tests/gocase/integration/cli/cli_test.go
- Confidence: verified; design-critical: no

### 21. In Kvrocks cluster mode (slot-encoded keys) a SCAN with a MATCH prefix walks slot by slot and visits at most 50 of the 16384 slots per call, which is why cursor handling there is more complex than in the plain namespace layout.

redis_slot.h: `constexpr const uint16_t HASH_SLOTS_SIZE = HASH_SLOTS_MASK + 1; // 16384` and `constexpr const uint16_t HASH_SLOTS_MAX_ITERATIONS = 50;`. redis_db.cc Scan: `PutFixed16(&ns_prefix, slot_start); ns_prefix.append(prefix);` and the outer `while (true)` loop with `if (slot_id > slot_start + HASH_SLOTS_MAX_ITERATIONS) { ... end_cursor->append(user_key); break; }`.

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/cluster/redis_slot.h
- Confidence: verified; design-critical: no

### 22. rocksdb.estimate-num-keys (RocksDB main, version 11.12.0) is memtable entries + immutable-memtable entries + an SST estimate, minus twice the memtable deletes, floored at 0; the SST part is sum(num_entries - num_deletions) - sum(num_deletions) over files whose table properties are loaded, extrapolated by file count. It is wrong under overwrites, merges, deletes of absent keys, and few samples.

internal_stats.cc HandleEstimateNumKeys: `estimate_keys = mem()->NumEntries() + imm()->current()->GetTotalNumEntries() + vstorage->GetEstimatedActiveKeys(); estimate_deletes = mem()->NumDeletion() + imm()...GetTotalNumDeletes(); *value = estimate_keys > estimate_deletes * 2 ? estimate_keys - (estimate_deletes * 2) : 0;`. version_set.cc GetEstimatedActiveKeys comment: 'Estimation will be inaccurate when: (1) there exist merge keys (2) keys are directly overwritten (3) deletion on non-existing keys (4) low number of samples'; `est = current_num_non_deletions_ - current_num_deletions_;` scaled by `file_count / current_num_samples_` when fewer files are sampled. db.h: 'returns estimated number of total keys in the active and unflushed immutable memtables and storage.' RocksDB FAQ: accurate counts 'require a full compaction'.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/db/internal_stats.cc
- Confidence: verified; design-critical: yes

### 23. RocksDB loads table properties for at most 20 SST files per Version at open unless max_open_files = -1, so the key estimate right after startup is an extrapolation from a small sample; and a DeleteRange tombstone counts as a single deletion regardless of how many keys it covers, so FLUSHDB-style range deletes barely move the estimate.

version_set.cc Version::UpdateAccumulatedStats: `const int kMaxInitCount = 20;` with comment 'only the first kMaxInitCount files which haven't been initialized from file will be updated with num_deletions. The motivation here is to cap the maximum I/O per Version creation.' and the max_open_files=-1 exception. MaybeInitializeFileMetaData: `file_meta->num_deletions = std::max(tp->num_deletions, tp->num_range_deletions);` table_properties.h: num_deletions 'the number of deletions in the table', num_range_deletions 'the number of range deletions in this table'.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/main/db/version_set.cc
- Confidence: verified; design-critical: yes

### 24. Dragonfly's alternative: a stateless u64 cursor that packs a 40-bit dashtable position (segment id shifted by table depth, 8-bit bucket id) shifted left 10 bits plus the shard id; the position survives table resizes, non-numeric input errors with 'invalid cursor', each call is time-boxed to 50 ms, and expired keys are skipped by TryExpire.

dash_internal.h DashCursor: `DashCursor(uint8_t depth, uint32_t seg_id, PhysicalBid bid) : val_((uint64_t(seg_id) << (40 - depth)) | bid)`, 'the cursor survives dash table resizes and will always point to the most appropriate segment with the same bucket. It uses 40 lsb bits out of 64'. generic_family.cc ScanGeneric: `ShardId sid = cursor % 1024; ... CHECK_LT(shard_count, 1024u); cursor >>= 10; ... cursor = (cursor << 10) | sid;` (via OpScan), `constexpr uint64_t kMaxScanTimeMs = 50;`, per-shard slice 'Approximately 30 microseconds'. Scan(): `if (!absl::SimpleAtoi(token, &cursor)) ... return cmd_cntx->SendError("invalid cursor");`. ScanCb: `switch (db_slice.TryExpire(...)) { case Valid: break; case ExpiredHidden: case Deleted: return false; }` and `bool matches = !opts.type_filter || prime_it->second.ObjType() == opts.type_filter;`. dash.h Traverse comment: 'if key exists (1) at the beginning of traversal, (2) stays in the table during the traversal, then Traverse() will eventually reach it even when the table shrinks or grows.'

- Source: https://raw.githubusercontent.com/dragonflydb/dragonfly/main/src/core/dash_internal.h
- Confidence: verified; design-critical: no

### 25. Dragonfly's DBSIZE sums a per-shard, per-db size counter across shards rather than scanning.

server_family.cc ServerFamily::DbSize: `shard_set->RunBriefInParallel([&](EngineShard* shard) { auto db_size = cntx->ns->GetDbSlice(shard->shard_id()).DbSize(cntx->conn_state.db_index); num_keys.fetch_add(db_size, ...); })` then SendLong. That DbSlice::DbSize is a maintained counter rather than a scan was not read in db_slice.cc.

- Source: https://raw.githubusercontent.com/dragonflydb/dragonfly/main/src/server/server_family.cc
- Confidence: likely; design-critical: no
