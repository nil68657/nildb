# NilDB status

Updated 2026-09-28 22:15 CDT. Read this first, then `docs/design/scope-v1.md`, `docs/design/build-plan.md` and `docs/design/rust-engines.md`.

## Done

Merged into `main` on 2026-09-28 (times CDT):

- [PR #1](https://github.com/nil68657/nildb/pull/1), 02:06: research, design docs, foundation packages, the query engine, `DOC.*` and `NIL.*`.
- [PR #2](https://github.com/nil68657/nildb/pull/2), 03:05: the Rust LSM-tree and B+-tree engines, their C interface and the Go binding `internal/nilengine`.
- [PR #3](https://github.com/nil68657/nildb/pull/3), 11:35: the Redis data commands, `ROCKS.*` admin and the `cmd/nildb` server binary.
- [PR #4](https://github.com/nil68657/nildb/pull/4), 16:03: pgheap, the PostgreSQL-style heap engine.

Open for review:

- [PR #5](https://github.com/nil68657/nildb/pull/5), branch `nc_nildb_feature_20260928_b4`: `--engine rocksdb|lsm|btree|pgheap`, the engine interface inside `internal/store`, and per-engine tests and benchmarks.

## How to build and test

- `make build` builds `bin/nildb` for RocksDB only. `make build-engines` builds it with all four engines (`-tags nilengine`).
- `make test` runs the default Go suite. `make test-engines` runs it once per engine, and `make rust-test` runs the Rust tests.
- The Rust toolchain is in `/opt/homebrew/opt/rustup/bin`; the Makefile finds `cargo` there.

## Next steps

1. A true read-only open for the Rust engines (shared `flock`, no recovery writes, no background threads). Until then `--readonly` on a Rust engine can write recovery state into the directory.
2. LSM: run the expiry and dropped-id filters inside the compaction loop, and throttle flush and compaction writes with a token bucket that charges low-priority reads.
3. pgheap: split large delete-ranges into janitor batches of a few thousand keys; a drop now sets xmax on every row in one batch under the writer lock.
4. Compression in the Rust engines.
5. A pinned-read C call, so values stop being copied twice across cgo.
6. `ROCKS.STATS` on pgheap repeats the engine-wide lines once per column family.
7. Run pgheap on the phase-1 benchmark workloads (fill, read, seek and mixed at 2,000,000 keys).
8. `redis-benchmark` and the Redis TCL compatibility runs. Both need `brew install redis`; ask Arka first.
9. Write `cmd/htapbench` (planned in `docs/design/architecture.md`) and run it per engine.
10. A test for a successful `ROCKS.INGEST`; only its error paths are tested.
11. A real README for the public repo.

## Rules

- Changes go in through a feature branch and a pull request. Never push to `main` and never merge.
- Run go commands through the Makefile, which exports the cgo flags for Homebrew RocksDB 11.8.1.

## Untracked work of unknown origin

The local tree holds `cmd/nildb-ui/` (a web UI with a RESP3 client) and uncommitted `ui` and `run-ui` targets in the Makefile. Nothing from the NilDB PRs wrote them, and they are in no PR. Leave them alone unless Arka says otherwise.
