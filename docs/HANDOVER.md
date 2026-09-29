# NilDB status

Updated 2026-09-28 18:30 CDT. Read this before continuing work, then `docs/design/scope-v1.md`, `docs/design/build-plan.md` and `docs/design/rust-engines.md`.

## Done

- Research (2026-09-26) and design docs: `docs/research/nildb-research.md`, `docs/design/architecture.md`, `docs/design/build-plan.md`, `docs/design/scope-v1.md`, `docs/design/rust-engines.md`.
- Merged into `main` on 2026-09-28:
  - [PR #1](https://github.com/nil68657/nildb/pull/1): build scaffold, research and v1 design docs, foundation packages, the query engine, `DOC.*` and `NIL.*`.
  - [PR #2](https://github.com/nil68657/nildb/pull/2): the Rust LSM-tree and B+-tree engines, the C interface, the Go binding `internal/nilengine` and its build targets.
  - [PR #3](https://github.com/nil68657/nildb/pull/3): the Redis data commands, `ROCKS.*` and the `cmd/nildb` server binary.
- Open: [PR #4](https://github.com/nil68657/nildb/pull/4), the pgheap engine (`rust/pgheap`, `NIL_ENGINE_PGHEAP` = 3), from branch `nc_nildb_feature_20260928_b3`.
- Phase 2, the Rust engines behind `internal/store` (`docs/design/rust-engines.md`, section "Phase 2"), is built but not committed. It lives in the working tree on top of the PR #4 branch. `nildb --engine rocksdb|lsm|btree|pgheap` picks the engine at startup; `rocksdb` stays the default, and the Rust engines need a binary from `make build-engines`.
- Checks on 2026-09-28, all passing: `make vet` and `go vet -tags nilengine ./...`, `make test`, `make test-engines` (every package on all four engines), `make nilengine-test`, `make rust-test` (240 passed, 6 ignored) and `cargo clippy`. Pass counts per engine and the end-to-end benchmark (`make bench-engines`) are in the design doc.

## Next

1. Publish phase 2 as a pull request after PR #4, or stacked on it.
2. Give the three engines a read-only open (shared `flock`, no recovery writes, no background threads). Until then `--readonly` on a Rust engine can write recovery state into the directory.
3. pgheap: split large delete-ranges into janitor batches of a few thousand keys; a drop now sets xmax on every row in one batch.
4. LSM: run the expiry and dropped-id filters inside compactions, fed the live id set through a new C call, and add a token bucket for flush and compaction writes that charges `LowPriority` reads.
5. Benchmark each engine with `redis-benchmark -P 16` and `cmd/htapbench`, and pgheap on the phase-1 fill, read, seek and mixed workloads.
6. Compression in the Rust engines.
7. v1 milestone M9 in `docs/design/build-plan.md`: `tests/protocol`, `tests/gocase`, `docs/compat/*`, the README and the v1.0 tag.

## Rules

- Commit and push only through a feature branch and a pull request; never merge.
- Run go commands through the Makefile, which exports the cgo flags for Homebrew RocksDB 11.8.1.
- `redis-cli` and `redis-benchmark` are not installed; ask Arka before `brew install redis`.
