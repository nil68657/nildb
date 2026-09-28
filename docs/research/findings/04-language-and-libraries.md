# Go vs Rust for NilDB: exact library state (RocksDB bindings, RESP, BSON, geo, analytics) and macOS Apple Silicon install path, as of 2026-09-26

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:language-and-libraries`, agent `a9751192e9ead928b`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. RECOMMENDATION: build NilDB in Go. The four requirements that are hardest to satisfy (MongoDB-style geo, a RESP server with pipelining and pub/sub, the widest 'native RocksDB' surface, and a build that iterates in seconds on a laptop) all favour Go today: golang/geo is the only complete-enough S2 port (Polygon.Contains/Intersects, RegionCoverer, ShapeIndex, ClosestEdgeQuery; MongoDB's own 2dsphere index is S2-based), rust-s2 has no polygons at all; redcon is a maintained server framework while Rust only has a parser crate whose last release was Nov 2024; grocksdb exposes WriteBatchWithIndex, TransactionDB, OptimisticTransactionDB, checkpoints with ExportColumnFamily, compaction filters, merge operators and prefix extractors, whereas rust-rocksdb still lacks WriteBatchWithIndex; and Go links Homebrew's shared librocksdb 11.8.1 instead of compiling RocksDB from source (~6 min cold on docs.rs builders). Rust wins only on the analytics axis (DataFusion) and on avoiding cgo call overhead; for a 'basic' HTAP where analytics means scans and aggregations over snapshots, that does not outweigh the geo and protocol gaps.
2. Pin the pairing grocksdb v1.11.1 <-> Homebrew rocksdb 11.8.1 and verify it on day one with a one-file cgo smoke test (Open, Put, Get, NewCheckpoint). grocksdb 1.11.x needs RocksDB >= 11.0 (the 11.0.0 C-API change to rocksdb_slicetransform_create), and upstream only tests 11.1.2 on Ubuntu; the 11.2-11.8 C-API deltas are additive, so it should link, but treat the first successful build as the gate. If it fails, the fallback is building RocksDB 11.1.2 with grocksdb's build.sh (static) rather than switching language.
3. Storage layout that keeps the RocksDB feel visible: one column family per Redis/Mongo namespace ('kv', 'hash', 'zset', 'docs', 'geo', 'idx'), each with its own Options (prefix extractor via NewFixedPrefixTransform for hash/zset/geo prefixes, merge operator for INCR/HINCRBY/$inc via SetMergeOperator, compaction filter for TTL expiry via SetCompactionFilter). Expose CHECKPOINT <dir> (Checkpoint.CreateCheckpoint / ExportColumnFamily), SNAPSHOT/RELEASE, COMPACT <cf>, FLUSH <cf>, SCAN-by-iterator and CF.LIST/CF.CREATE as first-class NilDB commands rather than hiding them.
4. Transactions: open the store as a TransactionDB (pessimistic) and map Redis MULTI/EXEC and Mongo multi-document writes to Transaction with GetForUpdate for read-modify-write; WATCH maps naturally to OptimisticTransactionDB semantics, so pick one mode per deployment rather than mixing. Use WriteBatchWI for read-your-own-writes inside a transaction's command stream (NewIteratorWithBase for scans inside MULTI); use plain WriteBatch for pipelined autocommit writes.
5. HTAP without a query engine: serve analytical commands (AGGREGATE, COUNT-by-prefix, GROUP, $group/$match pipelines) from a DB.NewSnapshot + per-CF iterators so they never block writers and see a consistent sequence number; run them on a separate goroutine pool with ReadOptions tuned (fill_cache=false, readahead). Keep document values as BSON (bson.Raw / RawValue lets you project fields without full Unmarshal). Because the on-disk format is plain RocksDB + BSON, a Rust/DataFusion sidecar could later open a CHECKPOINT directory read-only for SQL analytics without touching the Go server; this is the escape hatch if analytics outgrows hand-written aggregation.
6. Geo design (MongoDB-compatible, S2-backed): store GeoJSON as BSON; index each geometry with s2.RegionCoverer (e.g. MinLevel 4, MaxLevel 16-20, MaxCells 8-32) into a 'geo' column family keyed <index>/<cellid-token>/<docid> with a fixed prefix extractor. $geoWithin: cover the query polygon, range-scan covering cells, then refine with Polygon.ContainsPoint / Polygon.Contains. $geoIntersects: same scan, refine with Polygon.Intersects / IntersectsCell / Polyline crossing. $near/$nearSphere: expand a Cap (s2.CapFromCenterAngle) in rings, scan cells, sort by ClosestEdgeQuery or LatLng.Distance and convert with the earth package. Do not promise polygon union/difference or robust validation: S2BooleanOperation, S2Builder and S2ValidationQuery are not ported; validate loops with Loop.Validate only.
7. RESP: redcon is RESP2-only. Answer HELLO with a RESP2 map-as-array for protover 2 and reply '-NOPROTO unsupported protocol version' for HELLO 3 so go-redis/redis-cli fall back cleanly; use redcon.PubSub for SUBSCRIBE/PSUBSCRIBE and conn.Detach() for MONITOR/streaming replies; pipelining needs no code (ReadPipeline is internal to the loop).
8. Toolchain constraints to bake into go.mod/Makefile: go 1.25+ (mongo-driver v2.9.1 requires it; Homebrew go is 1.27.1), golang/geo pinned to a pseudo-version (no tags exist), CGO_ENABLED=1 with CGO_CFLAGS=-I/opt/homebrew/include and CGO_LDFLAGS=-L/opt/homebrew/lib exported in the Makefile so 'go build' and 'go test' work without per-shell setup.
9. Install commands (macOS Apple Silicon, Go path): xcode-select --install ; brew install go rocksdb ; export CGO_CFLAGS="-I/opt/homebrew/include" ; export CGO_LDFLAGS="-L/opt/homebrew/lib" ; mkdir nildb && cd nildb && go mod init github.com/<you>/nildb ; go get github.com/linxGnu/grocksdb@v1.11.1 github.com/tidwall/redcon@v1.6.4 go.mongodb.org/mongo-driver/v2@v2.9.1 github.com/golang/geo@v0.0.0-20260818125358-b200a1149890 ; go build ./... (this pulls go 1.27.1 and rocksdb 11.8.1 + gflags 2.3.1, lz4 1.10.0, snappy 1.3.1, zstd 1.5.7; cmake is skipped because bottles are poured). Verify with: ls /opt/homebrew/opt/rocksdb/lib/librocksdb.dylib && rocksdb_ldb --version.
10. Install commands (Rust path, if the decision is overturned): xcode-select --install ; brew install rustup ; echo 'export PATH="$(brew --prefix rustup)/bin:$PATH"' >> ~/.zshrc && exec zsh ; rustup toolchain install stable && rustup default stable (stable is 1.98.x; DataFusion needs >= 1.94, rocksdb crate >= 1.88) ; cargo new nildb && cd nildb ; cargo add rocksdb@0.25.0 --features multi-threaded-cf ; cargo add redis-protocol@6 --features bytes,codec ; cargo add tokio --features full ; cargo add tokio-util@0.7 --features codec ; cargo add bson@3.1 ; cargo add geo@0.33 geojson@1 h3o@0.11 ; optionally cargo add datafusion@55.1. First 'cargo build' compiles RocksDB 11.8.1 from source (budget 5-10 minutes cold, then cached); to skip it, brew install rocksdb and export ROCKSDB_LIB_DIR=/opt/homebrew/opt/rocksdb/lib (bundled header is 11.8.1, matching Homebrew). No brew llvm is needed: bindgen finds libclang via xcode-select.

## Open questions

- Does grocksdb v1.11.1 compile and pass its test suite against Homebrew's shared RocksDB 11.8.1 on arm64 macOS? Upstream CI only builds 11.1.2 statically on Ubuntu; the C-API delta looks additive but nobody has published a result for this pairing.
- grocksdb's README lists compaction_filter as unsupported while the source has Options.SetCompactionFilter and compaction_filter_test.go; confirm the Go callback filter actually runs under RocksDB 11.8.1 before designing TTL expiry around it (fallback: TTL column families via CreateColumnFamilyWithTTL).
- What is the measured cgo overhead per Get/Put on Apple Silicon with grocksdb, and does batching (MultiGet, WriteBatch) keep NilDB competitive with Redis for point ops? Not measured in any source opened here.
- Which modern Redis clients send HELLO 3 on connect (go-redis v9 does; redis-cli only with -3), and is a NOPROTO reply enough for all of them to fall back to RESP2? Needs a compatibility test matrix.
- Does the HTAP requirement eventually mean SQL or a MongoDB aggregation pipeline over large scans? If SQL with joins/window functions is needed, DataFusion (Rust) becomes the strongest option and the language decision should be revisited, or a Rust read-only sidecar over checkpoints added.
- golang/geo has no tagged releases; should NilDB vendor a pseudo-version or track master? Its README marks S2Polygon as 'mostly complete'; which polygon operations are missing was not enumerated in the sources.
- Cold whole-tree compile time for a Rust binary depending on rocksdb + datafusion on Apple Silicon was not found in any primary source; the only numbers are docs.rs (6m22s for librocksdb-sys alone) and DataFusion's per-crate figures.
- Homebrew's rocksdb is built with -DPORTABLE=ON (no -march tuning) and links system zlib/bzip2; if throughput matters, is a locally built RocksDB with native CPU flags worth the loss of 'brew install' simplicity?
- MongoDB semantics beyond geo (e.g. $near with maxDistance in metres, GeoJSON validity rules, legacy coordinate pairs) are not defined by any library here; NilDB must specify which subset it implements.

## Findings (37)

### 1. RocksDB upstream's latest release is v11.8.1 (2026-08-07); earlier 2026 releases were 11.1.2 (06-25), 11.1.1 (04-29), 11.0.4 (04-07), 10.10.1 (02-02).

GitHub releases API for facebook/rocksdb lists v11.8.1 published 2026-08-07T17:30:50Z, then v11.1.2, v11.1.1, v11.0.4, v10.10.1, v8.11.5. HISTORY.md at tag v11.8.1 dates 11.8.1 as 07/28/2026 and 11.0.0 as 02/23/2026.

- Source: https://api.github.com/repos/facebook/rocksdb/releases?per_page=6
- Confidence: verified; design-critical: yes

### 2. grocksdb's newest tag is v1.11.1 (2026-09-07); its newest GitHub release is v1.11.0 titled 'RocksDB 11.0.4' (2026-09-07); v1.11.1 is a single commit 'CHORE Adapt RocksDB 11.1.2 (#198)' touching build.sh, c.h, options.go, options_test.go. MIT licence, 399 stars, go.mod says go 1.17, only dependency testify.

proxy.golang.org @latest returns Version v1.11.1, Time 2026-09-07T03:26:19Z. Releases API: v1.11.0 'RocksDB 11.0.4' 2026-09-07; v1.10.8 'RocksDB 10.10.1' 2026-04-19; v1.10.7 'RocksDB 10.9.1' 2026-01-30; v1.10.2 'RocksDB v10.4.2' 2025-08-12. Compare v1.11.0...v1.11.1: total_commits 1, message 'CHORE Adapt RocksDB 11.1.2 (#198)'. Repo API: license MIT, stargazers 399, pushed 2026-09-07.

- Source: https://api.github.com/repos/linxGnu/grocksdb/compare/v1.11.0...v1.11.1
- Confidence: verified; design-critical: yes

### 3. grocksdb v1.11.x requires RocksDB 11.x (not 10.x) because RocksDB 11.0.0 changed the C API: it removed the in_range callback parameter from rocksdb_slicetransform_create().

RocksDB HISTORY.md, 11.0.0 (02/23/2026) 'Public API Changes': 'Remove deprecated SliceTransform::InRange() virtual method and the in_range callback parameter from rocksdb_slicetransform_create() in the C API.' grocksdb v1.11.0 is the release that 'Adapt[s] RocksDB 11.0.4'; v1.10.x releases track 10.x.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/v11.8.1/HISTORY.md
- Confidence: verified; design-critical: yes

### 4. grocksdb v1.11.1 is built and tested upstream against RocksDB 11.1.2 (static, Ubuntu only); it is not tested against 11.8.1 or on macOS, but RocksDB's C-API changes from 11.2.0 to 11.8.1 are additive only, so linking grocksdb v1.11.1 against Homebrew's 11.8.1 should work.

build.sh in grocksdb master sets rocksdb_version="11.1.2" (static, -DPORTABLE=1, no bz2). CI go.yml matrix is os: [ubuntu-latest] only, running 'make libs' then 'go test -tags testing'. RocksDB HISTORY entries 11.2.0..11.8.1 mention the C API only as additions: 11.4.0 adds rocksdb_options_set_memtable_batch_lookup_optimization etc.; 11.5.0: 'Expanded the C API ... the signatures of pre-existing functions are unchanged'; 11.8.0 adds rocksdb_flushoptions_set_listener_wait(). No removals or renames of rocksdb_* functions appear in those sections. A GitHub issue search for '11.8' in grocksdb returns 0 results.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/build.sh
- Confidence: likely; design-critical: yes

### 5. grocksdb exposes TransactionDB, OptimisticTransactionDB, column families, merge operators, compaction filters, snapshots, WriteBatchWithIndex, DeleteRange, checkpoints and prefix extractors, all as Go APIs.

Source on master: transactiondb.go has OpenTransactionDb, OpenTransactionDbColumnFamilies, TransactionBegin, NewSnapshot, NewCheckpoint; transaction.go has Prepare/Commit/Rollback, GetForUpdate(+CF/Pinned), SetSavePoint/RollbackToSavePoint, GetSnapshot, GetWriteBatchWI, SetCommitTimestamp; optimistic_transaction_db.go has OpenOptimisticTransactionDb(+ColumnFamilies), TransactionBegin, NewCheckpoint; db.go has OpenDbColumnFamilies, CreateColumnFamily(+WithTTL/+WithImport), DeleteRangeCF, NewSnapshot/ReleaseSnapshot, NewCheckpoint, WriteWI, Merge/MergeCF, CompactRange*, Flush*; write_batch.go has DeleteRange/DeleteRangeCF, Merge, SetSavePoint; write_batch_wi.go has NewWriteBatchWI, Put/Delete/Merge/DeleteRange(+CF), Get, GetFromDB(+CF, Pinnable), NewIteratorWithBase(+CF/ReadOpts); checkpoint.go has CreateCheckpoint(dir, logSizeForFlush) and ExportColumnFamily; slice_transform.go has NewFixedPrefixTransform, NewNoopPrefixTransform and a SliceTransform interface with Go callbacks; merge_operator.go has FullMerge/PartialMerge callbacks; compaction_filter.go defines a CompactionFilter interface (Filter(level,key,val) (remove,newVal)); options.go has SetCompactionFilter (line 167), SetComparator (185), SetMergeOperator (204), SetPrefixExtractor (815); snapshot.go has GetSequenceNumber.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/options.go
- Confidence: verified; design-critical: yes

### 6. grocksdb's README 'not supported' list still names compaction_filter/compaction_filter_factory/compaction_filter_context even though Options.SetCompactionFilter and a CompactionFilter callback exist in the source; other gaps are event listeners, compaction service, putv/mergev, get_db_identity, writebatch_iterate*, table_properties_collector_factory, sst_partitioner_factory, file_checksum_gen_factory.

README 'API Support: Almost C API, excepts:' lists get_db_identity; putv/mergev/deletev/delete_rangev; compaction_filter/compaction_filter_factory/compaction_filter_context; transactiondb_property_value/int; writebatch_update_timestamps/writebatch_wi_update_timestamps/writebatch_iterate_cf; approximate_sizes_cf_with_flags; logger_create_callback_logger; get_into_buffer(_cf); event listeners (flushjobinfo*, compactionjobinfo*, ...); compactionservice; rocksdb_open_and_compact*; rocksdb_writebatch_iterate*; sst_partitioner_factory; table_properties_collector_factory. compaction_filter.go and compaction_filter_test.go exist in the tree.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/README.md
- Confidence: verified; design-critical: no

### 7. grocksdb's default cgo link line is '-lrocksdb -pthread -lstdc++ -ldl -lm -lzstd -llz4 -lz -lsnappy'; build tags grocksdb_clean_link and grocksdb_no_link reduce it; the maintainer's own darwin_arm64 test stub uses the same library set.

non_builtin.go: '#cgo LDFLAGS: -lrocksdb -pthread -lstdc++ -ldl -lm -lzstd -llz4 -lz -lsnappy' under build constraint '!grocksdb_no_link && !grocksdb_clean_link'. README: clean_link base flags are '-lrocksdb -pthread -lstdc++ -ldl'; README build example: CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" go build. testing_darwin_arm64.go: '#cgo LDFLAGS: -L${SRCDIR}/dist/darwin_arm64/lib -lrocksdb -pthread -lstdc++ -ldl -lm -lzstd -llz4 -lz -lsnappy'.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/master/non_builtin.go
- Confidence: verified; design-critical: yes

### 8. Homebrew's rocksdb formula is 11.8.1: runtime deps gflags 2.3.1, lz4 1.10.0, snappy 1.3.1, zstd 1.5.7; build dep cmake; bzip2 from macOS; bottles for arm64_golden_gate, arm64_tahoe, arm64_sequoia, arm64_sonoma; built as a shared lib with PORTABLE=ON, USE_RTTI=ON, WITH_BZ2/LZ4/SNAPPY/ZLIB/ZSTD=ON, C++20; not keg-only; also installs rocksdb_ldb, rocksdb_sst_dump, rocksdb_stress and friends.

formulae.brew.sh API: versions.stable 11.8.1, dependencies ['gflags','lz4','snappy','zstd'], build_dependencies ['cmake'], uses_from_macos ['bzip2'], license 'GPL-2.0-only OR Apache-2.0', bottle files arm64_golden_gate/arm64_tahoe/arm64_sequoia/arm64_sonoma/sonoma/arm64_linux/x86_64_linux, caveats None, 30d installs 116. rocksdb.rb: url .../v11.8.1.tar.gz; cmake args -DPORTABLE=ON -DUSE_RTTI=ON -DWITH_BZ2=ON -DWITH_LZ4=ON -DWITH_SNAPPY=ON -DWITH_ZLIB=ON -DWITH_ZSTD=ON -DROCKSDB_BUILD_SHARED=ON; no keg_only line; bin.install of ldb => rocksdb_ldb, sst_dump => rocksdb_sst_dump; test compiles with -std=c++20 -lstdc++ -lz -lbz2 -lrocksdb -lsnappy -llz4 -lzstd.

- Source: https://raw.githubusercontent.com/Homebrew/homebrew-core/HEAD/Formula/r/rocksdb.rb
- Confidence: verified; design-critical: yes

### 9. On Apple Silicon Homebrew's prefix is /opt/homebrew; rocksdb, snappy, lz4, zstd and gflags are all non-keg-only, so their headers and libraries are symlinked into /opt/homebrew/include and /opt/homebrew/lib (per-formula paths under /opt/homebrew/opt/<name>).

docs.brew.sh/Installation: 'The script installs Homebrew to its default prefix (/opt/homebrew for Apple Silicon, /usr/local for macOS Intel ...)'. Formula Cookbook: 'a formula is keg-only if it is not symlinked into Homebrew's prefix'. formulae.brew.sh API: snappy 1.3.1 keg_only False; lz4 1.10.0 keg_only False; zstd 1.5.7 keg_only False (deps lz4, xz); gflags 2.3.1 keg_only False; rocksdb.rb has no keg_only.

- Source: https://docs.brew.sh/Installation
- Confidence: verified; design-critical: yes

### 10. 'brew install go rocksdb' installs go (no dependencies) plus rocksdb and its runtime deps gflags, lz4, snappy, zstd (zstd pulls lz4 and xz); cmake is NOT installed because build-only deps are skipped when pouring a bottle.

go.json: dependencies [], build_dependencies [], requirements macos >= 12. rocksdb.json: dependencies ['gflags','lz4','snappy','zstd'], build_dependencies ['cmake']. zstd.json deps ['lz4','xz']. Formula Cookbook: ':build means this is a build-time only dependency so it can be skipped when installing from a bottle'.

- Source: https://docs.brew.sh/Formula-Cookbook
- Confidence: verified; design-critical: no

### 11. Homebrew's go formula is 1.27.1 (aliases go@1.27, golang), BSD-3-Clause, requires macOS >= 12, bottled for all arm64 macOS releases, 79,115 installs in the last 30 days.

formulae.brew.sh API go.json: versions.stable '1.27.1', aliases ['go@1.27','golang','google-go'], requirements [{name: macos, version: '12'}], bottle files include arm64_golden_gate, arm64_tahoe, arm64_sequoia, arm64_sonoma; analytics install 30d {'go': 79115}.

- Source: https://formulae.brew.sh/api/formula/go.json
- Confidence: verified; design-critical: no

### 12. cgo needs a C compiler on PATH (Apple clang from Xcode Command Line Tools); CGO_CFLAGS/CGO_LDFLAGS are appended to #cgo directives; -I, -l, -L style flags are allowed by default.

cmd/cgo docs: 'The cgo tool is enabled by default for native builds on systems where it is expected to work. It is disabled by default ... when the CC environment variable is unset' and the default C compiler cannot be found; 'When building, the CGO_CFLAGS, CGO_CPPFLAGS, CGO_CXXFLAGS, CGO_FFLAGS and CGO_LDFLAGS environment variables are added to the flags derived from these directives'; 'For security reasons, only a limited set of flags are allowed, notably -D, -U, -I, and -l.' Homebrew docs: 'When building formulae from source, Command Line Tools (CLT) for Xcode (from xcode-select --install ...)'.

- Source: https://pkg.go.dev/cmd/cgo
- Confidence: verified; design-critical: no

### 13. tidwall/redcon is at v1.6.4 (2026-07-22), go 1.20, MIT, 2,307 stars; it is a RESP server framework with pipelining (ReadPipeline/PeekPipeline), pub/sub (redcon.PubSub with Publish/Subscribe/Psubscribe), TLS (ListenAndServeTLS/NewServerTLS), connection Detach() for long-lived streams, and Writer methods WriteString/WriteBulk/WriteInt/WriteInt64/WriteUint64/WriteError/WriteArray/WriteNull/WriteRaw/WriteAny/WriteBulkFrom.

proxy.golang.org @latest: v1.6.4, 2026-07-22. go.mod: go 1.20; requires tidwall/btree v1.1.0, tidwall/match v1.1.1. README features: 'Support for pipelining and telnet commands', 'TLS Support', 'Compatible pub/sub support', 'Multithreaded'; example uses redcon.PubSub ps.Publish/Subscribe/Psubscribe. redcon.go: Conn interface with Detach() DetachedConn, ReadPipeline(), PeekPipeline(); funcs NewServer, NewServerTLS, NewServerNetwork, Serve, ListenAndServe, ListenAndServeTLS; conn.Write* methods at lines 478-501.

- Source: https://raw.githubusercontent.com/tidwall/redcon/master/redcon.go
- Confidence: verified; design-critical: no

### 14. redcon speaks RESP2 only: it has no RESP3 types (map, double, boolean, push) and no HELLO handling.

grep -ci for 'resp3|hello' over redcon.go and resp.go on master returns 0 and 0. The Writer exposes only simple string, bulk, integer, error, array, null and raw writes.

- Source: https://raw.githubusercontent.com/tidwall/redcon/master/resp.go
- Confidence: verified; design-critical: yes

### 15. go.mongodb.org/mongo-driver/v2 is at v2.9.1 (2026-09-10) and its go.mod requires go 1.25.0; the bson package provides D, M, A, E, Raw, RawValue, RawElement, RawArray, Marshal/Unmarshal/MarshalValue, extended-JSON codecs, ObjectID, a registry for custom codecs, and bson struct tags (omitempty, minsize, truncate, inline).

proxy.golang.org @latest: v2.9.1, 2026-09-10T13:09:19Z, tag refs/tags/v2.9.1. .mod: 'go 1.25.0', requires golang.org/x/crypto v0.53.0, klauspost/compress v1.19.2, xdg-go/scram. bson directory at v2.9.1 contains raw.go, raw_value.go, raw_element.go, raw_array.go, objectid.go, primitive.go, vector.go, extjson_*.go, registry.go, marshal.go, unmarshal.go, struct_tag_parser.go. pkg.go.dev page (v2.9.1, Sep 10 2026, Apache-2.0) documents D (ordered), M (unordered map), A, E, Raw, RawValue, RawElement, Marshal/Unmarshal/MarshalValue, NewRegistry, ObjectID and the bson tag options.

- Source: https://proxy.golang.org/go.mongodb.org/mongo-driver/v2/@v/v2.9.1.mod
- Confidence: verified; design-critical: no

### 16. golang/geo has no tagged releases; the latest pseudo-version is v0.0.0-20260818125358-b200a1149890 (2026-08-18), go 1.23.0, Apache-2.0, 1,854 stars, last push 2026-09-21.

proxy.golang.org @latest: Version v0.0.0-20260818125358-b200a1149890, Time 2026-08-18T12:53:58Z. GitHub tags API returns []. go.mod: 'go 1.23.0'. Repo API: license Apache-2.0, stars 1854, pushed 2026-09-21T18:21:30Z.

- Source: https://proxy.golang.org/github.com/golang/geo/@latest
- Confidence: verified; design-critical: no

### 17. golang/geo's s2 package covers what MongoDB-style 2dsphere queries need: Cap, Cell, CellID, CellUnion, LatLng, Rect, Loop, Polyline, RegionCoverer, ShapeIndex, ClosestEdge/FurthestEdge queries, ContainsPoint, CrossingEdge, ConvexHull and Earth are feature-complete; Polygon is 'mostly complete' and has ContainsPoint, ContainsCell, IntersectsCell, Contains(*Polygon), Intersects(*Polygon) and Area. Missing entirely: S2BooleanOperation, S2Builder, S2PointIndex/S2ClosestPoint, S2ClosestCell, S2RegionTermIndexer, S2ValidationQuery.

README status table: ✅ S2Cap, S2Cell, S2CellId, S2CellUnion, S2LatLng, S2LatLngRect, S2Loop, S2Polyline, S2RegionCoverer, S2ShapeIndex, S2ClosestEdge, S2FurthestEdge, S2ContainsPoint, S2CrossingEdge, S2ConvexHull, S2Earth, S2Predicates; 🟡 S2Polygon, S2CellIndex, S2LaxPolygon/Polyline; ❌ S2BooleanOperation, S2Builder, S2PointIndex, S2ClosestPoint, S2ClosestCell, S2RegionTermIndexer, S2ValidationQuery. s2/polygon.go: func (p *Polygon) ContainsPoint (line 591), ContainsCell (615), IntersectsCell (639), Contains(o *Polygon) (826), Intersects(o *Polygon) (869), Area (1005). s2/edge_query.go: NewClosestEdgeQuery (254), FindEdges (303), Distance (312), IsDistanceLess (325).

- Source: https://raw.githubusercontent.com/golang/geo/master/README.md
- Confidence: verified; design-critical: yes

### 18. Go has no DataFusion equivalent; Apache Arrow for Go exists (arrow-go v18.8.0, 2026-09-04) but is a columnar memory/compute library, not a SQL query engine.

proxy.golang.org: github.com/apache/arrow-go/v18 @latest = v18.8.0, 2026-09-04T16:46:17Z. No Go query engine was in scope of this research; the claim about the absence of a Go DataFusion port is from general knowledge, not a source opened here.

- Source: https://proxy.golang.org/github.com/apache/arrow-go/v18/@latest
- Confidence: likely; design-critical: yes

### 19. The Rust rocksdb crate is at 0.25.0 (2026-08-16), MSRV 1.88.0, Apache-2.0, 55.6M total downloads; it depends on librocksdb-sys 0.19.0+11.8.1, i.e. it bundles and statically links RocksDB 11.8.1. Previous release 0.24.0 (2025-08-10) bundled 10.4.2.

crates.io API: rocksdb max_version 0.25.0, updated 2026-08-16T12:04:05Z, rust_version 1.88.0, downloads 55,576,637; librocksdb-sys versions 0.19.0+11.8.1 (2026-08-16, rust_version 1.88.0), 0.17.3+10.4.2 (2025-08-10), 0.17.1+9.9.3 (2024-12-24). Cargo.toml: version 0.25.0, rust-version 1.88.0, librocksdb-sys version 0.19.0 with features ['static']. CHANGELOG 0.25.0 (2026-08-16): 'Update to RocksDB 11.8.1', 'Bump Rust to 1.88.0', 'Add checkpoint support to TransactionDB', 'Add flush and flush_wal wrappers to TransactionDB', 'Add custom logging callbacks', 'Add a feature-gated trait for exposing raw C pointers'.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/CHANGELOG.md
- Confidence: verified; design-critical: yes

### 20. rust-rocksdb builds RocksDB from bundled source with the cc crate (parallel, -std=c++20) and generates bindings with bindgen 0.72 from c.h; cmake is not required; build-dependencies are cc, bindgen, rustflags and (only for io-uring) pkg-config. README requirements: 'Clang and LLVM'.

librocksdb-sys/Cargo.toml [build-dependencies]: cc = { version = '1.0', features = ['parallel'] }, bindgen = { version = '0.72', default-features = false }, pkg-config optional, rustflags = '0.1'. build.rs: bindgen_rocksdb() reads rocksdb/include/rocksdb/c.h (override ROCKSDB_INCLUDE_DIR); build_rocksdb() uses cc::Build with rocksdb/ includes; default C++ standard '-std=c++20' (ROCKSDB_CXX_STD override); links libc++ on apple targets (cpp_link_stdlib). README: '## Requirements - Clang and LLVM'; 'This binding is statically linked with a specific version of RocksDB'.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/librocksdb-sys/build.rs
- Confidence: verified; design-critical: yes

### 21. bindgen's libclang is found on macOS from the Xcode Command Line Tools without installing Homebrew llvm: clang-sys searches the toolchain directory under xcode-select --print-path, and rust-rocksdb's own CI builds on macos-latest with no extra LLVM step (only Windows runs 'choco install llvm').

clang-sys README: libclang shared libraries are searched in 'the directory provided by the LIBCLANG_PATH environment variable' and 'macOS only: the toolchain directory in the directory provided by xcode-select --print-path'; default is dynamic linking to libclang.dylib and the 'runtime' feature loads it at run time (rust-rocksdb enables bindgen-runtime by default). rust.yml test job matrix build: [Linux, macOS, Windows], os macos-latest; steps 'choco install llvm -y' guarded by runner.os == 'Windows', 'apt-get install libjemalloc-dev' guarded by Linux; no macOS-specific install step. bindgen's requirements page itself recommends 'brew install llvm' on macOS and 'Clang 9.0 or greater'. Closed issue #918 'Failed to build on macOS 14.6' was caused by a stray LIBCLANG_PATH pointing at an ESP32 clang, not by a missing libclang.

- Source: https://raw.githubusercontent.com/KyleMayes/clang-sys/master/README.md
- Confidence: likely; design-critical: yes

### 22. rust-rocksdb can link Homebrew's librocksdb instead of compiling it: set ROCKSDB_LIB_DIR (dylib by default, ROCKSDB_STATIC for static), optionally ROCKSDB_INCLUDE_DIR for bindgen and SNAPPY_LIB_DIR; ROCKSDB_COMPILE=1 forces the bundled build. The crate's bundled header is 11.8.1, the same version Homebrew ships today.

build.rs try_to_find_and_link_lib(): reads '{lib_name}_COMPILE', '{lib_name}_LIB_DIR', '{lib_name}_STATIC' and emits cargo:rustc-link-search / rustc-link-lib; main() calls it for 'ROCKSDB' and, if the snappy feature is on, 'SNAPPY'; bindgen uses env ROCKSDB_INCLUDE_DIR or 'rocksdb/include'. librocksdb-sys version 0.19.0+11.8.1; Homebrew rocksdb 11.8.1.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/librocksdb-sys/build.rs
- Confidence: verified; design-critical: no

### 23. A cold build of librocksdb-sys 0.19.0+11.8.1 took 6m 22s and rocksdb 0.25.0 took 6m 46s on docs.rs builders (rustc 1.100.0-nightly); a 2025 user report says the 0.23.0 upgrade pushed build time 'over a minute on MacPro'. No measured Apple Silicon number exists in the sources; the C++ compile is cached by cargo after the first build. The jemalloc feature is disabled on darwin.

docs.rs builds page for librocksdb-sys 0.19.0+11.8.1: one entry, 'rustc 1.100.0-nightly (67854e511 2026-08-15)', success, 'took 6m 22s'. docs.rs builds for rocksdb 0.25.0: 'took 6m 46s'. Issue #983 (2025-02-22, open): 'after that the build times has increased to over a minute on MacPro'. build.rs: NO_JEMALLOC_TARGETS = ['android','dragonfly','musl','darwin'].

- Source: https://docs.rs/crate/librocksdb-sys/0.19.0+11.8.1/builds
- Confidence: verified; design-critical: no

### 24. rust-rocksdb Cargo features: default = snappy, lz4, zstd, zlib, bzip2, bindgen-runtime; optional multi-threaded-cf (RwLock-guarded column-family create/drop; alternatively DBWithThreadMode<MultiThreaded>), jemalloc, io-uring, serde1, lto, mt_static (Windows), bindgen-static (musl), rtti, valgrind, raw-ptr.

Cargo.toml [features]: default = ['snappy','lz4','zstd','zlib','bzip2','bindgen-runtime']; jemalloc, io-uring, valgrind, mt_static, rtti, multi-threaded-cf = [], serde1 = ['serde'], bindgen-static, lto, raw-ptr. README: 'If you need to modify column families concurrently, enable the crate feature multi-threaded-cf ... Alternatively, you can directly create DBWithThreadMode<MultiThreaded>'.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/Cargo.toml
- Confidence: verified; design-critical: no

### 25. rust-rocksdb 0.25.0 exposes TransactionDB (pessimistic) and OptimisticTransactionDB with Transaction::get_for_update, commit/rollback/prepare, savepoints and per-transaction snapshots; Checkpoint (create_checkpoint, create_checkpoint_with_log_size, also on TransactionDB); DB::delete_range_cf; WriteBatch delete_range/delete_range_cf/merge/single_delete; Options::set_merge_operator(_associative), set_compaction_filter(_factory), set_prefix_extractor(SliceTransform), set_comparator; snapshots (SnapshotWithThreadMode); prefix_iterator; column families via open_cf/open_cf_descriptors and DBWithThreadMode<MultiThreaded>.

docs.rs rocksdb 0.25.0 lists structs Transaction, TransactionDB, TransactionDBOptions, TransactionOptions, OptimisticTransactionOptions, SnapshotWithThreadMode, SliceTransform, WriteBatchWithTransaction, ColumnFamily, BoundColumnFamily, DBIteratorWithThreadMode, SstFileWriter, WriteBufferManager, and type aliases DB, OptimisticTransactionDB, Snapshot, WriteBatch. Source: transactions/transaction_db.rs pub fn transaction(), transaction_opt, prepared_transactions, write(WriteBatchWithTransaction<true>); transactions/transaction.rs commit, prepare, snapshot(), rollback, set_savepoint, rollback_to_savepoint, get_for_update(_cf/_opt/_pinned), iterator/prefix_iterator; checkpoint.rs new, create_checkpoint, create_checkpoint_with_log_size (lines 46-199, incl. TransactionDB impl); db.rs delete_range_cf, delete_range_cf_opt, prefix_iterator, open_cf*, flush*; write_batch.rs delete_range (491), delete_range_cf (510), merge (321), single_delete (383); db_options.rs set_merge_operator_associative (1743), set_merge_operator (1767), set_compaction_filter (1810), set_compaction_filter_factory (1841), set_comparator (1865), set_prefix_extractor (1922).

- Source: https://docs.rs/rocksdb/latest/rocksdb/
- Confidence: verified; design-critical: yes

### 26. rust-rocksdb does NOT expose WriteBatchWithIndex; the request has been open since November 2023.

docs.rs listing for rocksdb 0.25.0 contains no WriteBatchWithIndex/WBWI item (WriteBatch is WriteBatchWithTransaction<const TRANSACTION: bool>). GitHub issue search: #836 'Is WriteBatchWithIndex supported?' open, created 2023-11-23; #922 'Expose get_pinned variants for WriteBatchWithIndex' open 2024-08-24; PR #923 open 2024-08-24.

- Source: https://github.com/rust-rocksdb/rust-rocksdb/issues/836
- Confidence: verified; design-critical: yes

### 27. redis-protocol (Rust) 6.0.0 (2024-11-15) is a RESP2+RESP3 frame parser/encoder with a tokio-util codec feature, pub/sub message utilities and cluster hash-slot helpers; it is not a server framework, and the repo's last push was 2025-02-13 (71 stars).

crates.io: max_version 6.0.0, updated 2024-11-15, description 'An implementation of the RESP2 and RESP3 protocols', 9.28M downloads. README features: 'Supports RESP2 and RESP3 frames, including streaming frames', 'RESP2 and RESP3 codec interfaces', 'Publish-subscribe message utilities', frame types OwnedFrame/BytesFrame/RangeFrame; build features std, resp2, resp3, bytes, codec, convert, index-map; no_std supported. GitHub API: pushed_at 2025-02-13T16:53:45Z, archived false, open_issues 8.

- Source: https://raw.githubusercontent.com/aembke/redis-protocol.rs/main/README.md
- Confidence: verified; design-critical: yes

### 28. tidwall's Rust port redcon.rs is stale (0.1.2, 2022-05-14; last push 2023-01-29) and the 'resp' crate is 1.0.3 from 2022-08; a Rust NilDB would write its own server loop on tokio (tokio-util 0.7.19, 2026-07-21) over the redis-protocol codec or a hand-written RESP2/RESP3 parser.

crates.io: redcon max 0.1.2 updated 2022-05-14, repo tidwall/redcon.rs pushed 2023-01-29; resp max 1.0.3 updated 2022-08-19, recent downloads 1,900; tokio-util max 0.7.19 updated 2026-07-21.

- Source: https://crates.io/api/v1/crates/redcon
- Confidence: verified; design-critical: no

### 29. The Rust bson crate is 3.1.0 (2025-11-20), MSRV 1.81, actively maintained (repo pushed 2026-09-24); it provides the Bson enum, Document, doc!/bson! macros, serde integration, and opt-in features chrono-0_4, uuid-1, time-0_3, serde_with-3, serde_json-1, large_dates.

crates.io: max_version 3.1.0, updated 2025-11-20, rust_version 1.81, 26.46M downloads, repo mongodb/bson-rust. README: 'Requirements - Rust 1.81+'; bson = "3.1.0"; feature table chrono-0_4, uuid-1, time-0_3, serde_with-3, serde_path_to_error, compat-3-0-0, large_dates, serde_json-1, facet-unstable; Bson enum, Document::from_reader, doc! macro. GitHub API: pushed 2026-09-24, 459 stars.

- Source: https://raw.githubusercontent.com/mongodb/bson-rust/main/README.md
- Confidence: verified; design-critical: no

### 30. h3o 0.11.0 (2026-08-29) is a from-scratch, 100% Rust, no-C-dependency H3 implementation (BSD-3-Clause, 544 stars).

crates.io: max_version 0.11.0, updated 2026-08-29, 1.82M downloads. README: 'This is not a binding of the reference implementation, but a reimplementation from scratch', goals 'To be 100% Rust (no C deps)'; example LatLng::new(...).to_cell(Resolution::Nine). GitHub: pushed 2026-08-29, 544 stars.

- Source: https://raw.githubusercontent.com/HydroniumLabs/h3o/master/README.md
- Confidence: verified; design-critical: no

### 31. The Rust s2 crate (rust-s2) 0.2.0 (2026-08-19, MSRV 1.86) is an incomplete port: its README states lines and polygons are not implemented yet, so it cannot back polygon $geoWithin/$geoIntersects.

crates.io: max_version 0.2.0 updated 2026-08-19, rust_version 1.86, 656k downloads, previous 0.1.0 2026-07-07 and 0.0.13 2024-10-02. README 'Status': 'This is an ongoing port, further along in some areas than others — in particular, lines and polygons aren't implemented yet.' GitHub: 97 stars, pushed 2026-09-24.

- Source: https://raw.githubusercontent.com/yjh0502/rust-s2/master/README.md
- Confidence: verified; design-critical: yes

### 32. georust geo 0.33.1 (2026-04-20, MSRV 1.88) provides full DE-9IM relations (contains/intersects), boolean ops, buffering, haversine and other spherical distances, and GeoJSON IO via the geojson crate (1.0.0, 2026-03-16); geo-types is 0.7.20 (2026-08-02).

crates.io: geo max 0.33.1 updated 2026-04-20 rust_version 1.88, 22.96M downloads; geojson max 1.0.0 updated 2026-03-16; geo-types 0.7.20 updated 2026-08-02. README: 'Full DE-9IM support and topological relationship calculations such as containment and intersection', 'Boolean operations on geometries (clip, union, difference, intersection, xor)', 'Buffer / offset operations', 'Euclidean, as well as spherical, haversine and other non-planar length and distance calculations', 'IO using the geojson and geozero crates'. GitHub: pushed 2026-09-23, 1,941 stars.

- Source: https://raw.githubusercontent.com/georust/geo/main/README.md
- Confidence: verified; design-critical: yes

### 33. DataFusion is at 55.1.0 (2026-09-11) with MSRV 1.94.0; the crate has 47 direct normal dependencies (arrow, parquet, sqlparser, tokio, object_store, 25 datafusion-* sub-crates, ...), default features nested/crypto/datetime/encoding/regex/string/unicode expressions, compression, parquet, recursive_protection and sql; the workspace Cargo.lock has 676 packages (upper bound on the transitive tree); lib.rs estimates ~56-79 MB / ~1.5M SLoC of dependencies.

crates.io: datafusion max_version 55.1.0, updated 2026-09-11T15:03:52Z, rust_version 1.94.0 (54.x was 1.88.0), crate_size 306,973 bytes, 27.6M downloads. /55.1.0/dependencies: 47 normal deps listed. /55.1.0 features: default ['nested_expressions','crypto_expressions','datetime_expressions','encoding_expressions','regex_expressions','string_expressions','unicode_expressions','compression','parquet','recursive_protection','sql']. Workspace Cargo.toml: rust-version = '1.94.0', version = '55.1.0'. Cargo.lock: 676 '[[package]]' entries. lib.rs page: '~56–79MB', '~1.5M SLoC'.

- Source: https://crates.io/api/v1/crates/datafusion/55.1.0/dependencies
- Confidence: verified; design-critical: yes

### 34. DataFusion's compile cost is large but recently improved: the core crate alone compiled in 88.9s before PR #24329 (merged 2026-08-13, in 55.0.0+) and 8.2s after; a 2024 issue reported 40s for the core crate; the docs warn that lto + codegen-units=1 'significantly increases --release build times'. No cold whole-tree Apple Silicon number was found.

PR #24329 'perf(core): cut datafusion core compile time ~10x', merged 2026-08-13T17:36:49Z: 'cargo rustc -p datafusion --lib with -Ztime-passes: base 88.9s total / 75.0s evaluate_obligation; after second commit 8.2s / 234ms'; sibling PRs #24325 (catalog ~6x), #24326 (session ~1.7x), #24330 (catalog-listing ~4.4x). Issue #13814 (2024-12-17): 'Compiling the datafusion crate currently takes 40 seconds on my machine'. Crate-configuration docs: 'using a single codegen unit significantly increases --release build times'; recommends RUSTFLAGS='-C target-cpu=native'.

- Source: https://api.github.com/repos/apache/datafusion/pulls/24329
- Confidence: verified; design-critical: no

### 35. Homebrew's rustup formula is 1.29.1 (formerly rustup-init), keg-only ('it conflicts with rust'), bottled for arm64; it installs rustup plus proxy symlinks cargo, rustc, rustfmt, clippy-driver, rust-analyzer, cargo-clippy, cargo-fmt, cargo-miri, rustdoc, and a fallback settings.toml with default_toolchain = 'stable'; the caveat says to put "$(brew --prefix rustup)/bin" on PATH and that rustup-init is no longer provided. Its build dependency is Homebrew's rust 1.98.1, so current stable Rust is 1.98.x.

formulae.brew.sh API: name rustup, oldnames ['rustup-init'], versions.stable 1.29.1, build_dependencies ['rust'], uses_from_macos ['curl','xz'], bottle files arm64_golden_gate/tahoe/sequoia/sonoma, 30d installs 7,895; caveats: 'To use rustup, ensure you have "$(brew --prefix rustup)/bin" in your $PATH ... This formula no longer provides rustup-init.' rustup.rb: keg_only 'it conflicts with rust'; mv bin/'rustup-init', bin/'rustup'; bin.install_symlink for cargo, cargo-clippy, cargo-fmt, cargo-miri, clippy-driver, rls, rust-analyzer, rust-gdb, rust-lldb, rustc, rustdoc, rustfmt; settings.toml 'default_toolchain = "stable"' via RUSTUP_OVERRIDE_UNIX_FALLBACK_SETTINGS. The formula page (WebFetch) reported the build dep as 'rust 1.98.1'.

- Source: https://raw.githubusercontent.com/Homebrew/homebrew-core/HEAD/Formula/r/rustup.rb
- Confidence: verified; design-critical: no

### 36. rustup's docs for package-manager installs: keep rustup's proxies first on PATH (Homebrew example: 'eval $(/opt/homebrew/bin/brew shellenv)' then '. $HOME/.cargo/env'); toolchains are installed with 'rustup toolchain install <name>' and selected with 'rustup default <name>'.

already-installed-rust.html: 'on any machine with rustup installed, you would like to have rustup proxies showing up first in $PATH'; Mac/Homebrew example shows brew shellenv followed by sourcing $HOME/.cargo/env. concepts/toolchains.html: '$ rustup toolchain install stable-x86_64-pc-windows-msvc', '$ rustup default my-toolchain'.

- Source: https://rust-lang.github.io/rustup/installation/already-installed-rust.html
- Confidence: verified; design-critical: no

### 37. Homebrew officially supports macOS Sequoia (15) or newer and needs Xcode Command Line Tools only when building from source; the target machine (Darwin 27.2.0) is newer than that, and bottles exist for arm64_golden_gate, so every formula in this plan pours a bottle.

docs.brew.sh/Installation: 'macOS Sequoia (15) (or higher) installed on officially supported hardware'; 'When building formulae from source, Command Line Tools (CLT) for Xcode (from xcode-select --install ...)'. Bottle lists for rocksdb, go, rustup all include arm64_golden_gate. Environment reports OS Version Darwin 27.2.0.

- Source: https://docs.brew.sh/Installation
- Confidence: verified; design-critical: no
