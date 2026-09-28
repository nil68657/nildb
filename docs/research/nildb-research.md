# NilDB research summary

The research ran on 2026-09-26 as a Claude Code workflow. Seven agents each covered one area, two agents re-checked design-critical claims against their sources, a critic listed what was still missing, and one gap agent per critic question was meant to answer it. The Claude session limit stopped six of the eight gap agents and all three attempts at writing this summary. This file was written on 2026-09-27 from the saved agent outputs, which are exported unedited in [findings/](findings/).

## Files

| File | Area | Findings |
|---|---|---|
| [01-prior-art-redis-on-rocksdb.md](findings/01-prior-art-redis-on-rocksdb.md) | Kvrocks, Pika, Tidis, Titan, SSDB, LedisDB, Ardb: key encodings, column families, TTL, transactions | 39 |
| [02-htap-on-kv-store.md](findings/02-htap-on-kv-store.md) | HTAP requirements, TiDB/TiFlash, SingleStore, LSM secondary indexes, embedded query engines | 37 |
| [03-geo-indexing.md](findings/03-geo-indexing.md) | Redis GEO, MongoDB 2dsphere, S2, Go and Rust S2 libraries, KV geo layouts | 39 |
| [04-language-and-libraries.md](findings/04-language-and-libraries.md) | Go vs Rust library state on 2026-09-26, macOS install path | 37 |
| [05-redis-compat-surface.md](findings/05-redis-compat-surface.md) | RESP2/RESP3, pipelining, command set, error strings, test tooling | 46 |
| [06-document-model-and-mongo-surface.md](findings/06-document-model-and-mongo-surface.md) | BSON, CRUD/query/aggregation semantics, index encoding, FerretDB | 58 |
| [07-rocksdb-native-features.md](findings/07-rocksdb-native-features.md) | RocksDB features to build on and expose, grocksdb vs rust-rocksdb coverage | 37 |
| [08-gap-transactions-and-watch.md](findings/08-gap-transactions-and-watch.md) | Critic gap 3: MULTI/EXEC/WATCH on RocksDB, WATCH over expiry | 20 |
| [09-gap-scan-cursors-keys-dbsize.md](findings/09-gap-scan-cursors-keys-dbsize.md) | Critic gap 4: SCAN cursors, KEYS, DBSIZE | 25 |
| [fact-checks.md](findings/fact-checks.md) | 11 re-checked claims, 4 corrected | |
| [critic-gaps.md](findings/critic-gaps.md) | Critic verdict and the 8 gaps, with which ones were answered | |

The agents labelled 282 of the 293 area findings as read in the primary source. Each finding carries its URL.

## What the research supports

- **Go over Rust.** golang/geo is the only S2 port with polygons, `RegionCoverer` and `ClosestEdgeQuery`; the Rust `s2` crate has no polylines or polygons. grocksdb exposes `WriteBatchWithIndex`; rust-rocksdb does not. See 03 and 04. The HTAP and RocksDB areas leaned Rust (DataFusion, rust-rocksdb), and gap 1, which was meant to settle the language with experiments, never ran. The design panel took Go as a fixed input.
- **Toolchain.** Homebrew ships Go 1.27.1 and RocksDB 11.8.1 for arm64. grocksdb v1.11.1 is tested upstream against RocksDB 11.1.2; the fact check diffed `include/rocksdb/c.h` from 11.1.2 to 11.8.1 and found 743 new functions and no type or arity changes among the 1048 shared ones. `make rocksmoke` passes on this Mac (last run 2026-09-27).
- **Redis types on RocksDB.** Kvrocks's layout: one metadata entry per key (type, expiry, version, size) plus sub-keys, a per-key version so `DEL` of a collection is one metadata delete, sign-flipped big-endian doubles for sorted-set scores, lazy expiry on read plus compaction filters for garbage. See 01.
- **Transactions.** A plain `rocksdb::DB` with NilDB's own striped key locks; `EXEC` locks every queued and watched key, checks WATCH, runs the queue into one `WriteBatchWithIndex`, commits once, and never rolls back on a per-command error. WATCH over expiry has to be checked at `EXEC` because lazy expiry produces no delete event. Avoid a server-wide shared mutex: on macOS libc++ it would make every client wait behind a long aggregation. See 08.
- **SCAN.** Keep the real position as the last returned RocksDB key and map it to the unsigned 64-bit cursor Redis clients expect through a server-side table; reply `ERR invalid cursor` to unknown cursors instead of restarting silently. `DBSIZE` can iterate, keep exact counters, or read `rocksdb.estimate-num-keys`, which is wrong under overwrites. See 09.
- **Redis GEO.** A sorted set whose score is a 52-bit interleaved geohash, latitude clamped to ±85.05112878. Port from Redis 7.2 (BSD-3-Clause) or Kvrocks (Apache-2.0); Redis 8.x source is tri-licensed and must not be copied. See 03.
- **MongoDB 2dsphere.** S2 cell ids in an index column family. MongoDB's v3 index defaults are finest level `ClosestLevel(110 / R)`, coarsest `ClosestLevel(2,000,000 / R)`, 20 cells per covering (corrected in the fact check; source `initialize2dsphereParams`). See 03 and fact-checks.
- **Documents.** Raw BSON through mongo-driver/v2 `bson`, index keys ordered like MongoDB's KeyString so `bytes.Compare` matches MongoDB's cross-type comparison order. See 06.
- **Protocol.** RESP2 byte-exact, RESP3 on `HELLO 3`, and Redis's exact `Protocol error:` strings from `networking.c`. go-redis v9 negotiates RESP3 by default. See 05.
- **HTAP.** One RocksDB instance and one sequence number give analytical reads zero staleness through snapshots. The engineering work goes into isolation: `fill_cache=false` on scans, bounded analytical concurrency, a Go-side byte throttle (grocksdb v1.11.1 has no `rocksdb_readoptions_set_rate_limiter_priority`), iterator deadlines, and checkpoints opened read-only in a second process for long jobs. See 02 and 07.

## What it did not settle

Six critic gaps were never researched (full questions in [critic-gaps.md](findings/critic-gaps.md)):

1. An experiment to decide Go vs Rust against all four hard requirements.
2. How Rockset's converged index and Kvrocks's JSON type lay out per-field columns, and the write amplification they pay. The columnar design in the proposals has no precedent behind it. v1 defers the columnar index.
3. macOS durability and a throughput baseline. Homebrew's librocksdb is built with `F_FULLFSYNC`; rust-rocksdb's build does not define it for darwin. No benchmark exists yet.
4. A per-source licensing table: Redis 7.2 (BSD-3-Clause, copyable), Redis 8.x (tri-licensed), MongoDB server (SSPL), CockroachDB `pkg/geo` (CockroachDB Software License), mongodb/specifications (CC BY-NC-SA 3.0). Until someone checks, copy code only from Redis 7.2 and Apache-2.0 projects, and treat the rest as reading material.
5. Geo details no library provides, such as Redis's exact step estimation and MongoDB's `$near` annulus growth.
6. A MongoDB conformance suite to reuse (FerretDB's Apache-2.0 integration tests are the lead) and which wire versions to advertise.

Firecrawl's quota ran out during this research. Further research has to use ordinary web search and fetch.
