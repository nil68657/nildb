# NilDB status

Updated 2026-09-27 14:20 CDT. Read this before continuing work, then `docs/design/scope-v1.md` and `docs/design/build-plan.md`.

## Done

- Research (2026-09-26) and design docs: `docs/research/nildb-research.md`, `docs/design/architecture.md`, `docs/design/build-plan.md`, `docs/design/scope-v1.md`.
- Foundation packages with passing tests (`make vet test` green on 2026-09-27): `internal/resp`, `config`, `keyenc`, `layout`, `geo`, `geohash`, `doc`, `store`, `command`, `server`, `testutil`, `catalog`, `docstore`.
- Public repo: https://github.com/nil68657/nildb. Changes go in through pull requests to `main`; nothing is pushed to `main` directly.

## Not done

- Redis data commands (`internal/redis`), the query engine (`internal/query`), `DOC.*` (`internal/cmddoc`), `NIL.*` (`internal/analytics`), `ROCKS.*` (`internal/admin`) and the `cmd/nildb` binary. `make build` fails until `cmd/nildb` exists.
- At 14:11 on 2026-09-27 Arka asked for the implementation to use both Go and Rust, with an LSM tree and a B+ tree. How the work splits between the two languages is being decided; check the newest design note in `docs/design/` before starting storage work.

## Rules

- Commit and push only through a feature branch and a pull request; never merge.
- Run go commands through the Makefile, which exports the cgo flags for Homebrew RocksDB 11.8.1.
- `redis-cli` and `redis-benchmark` are not installed; ask Arka before `brew install redis`.
