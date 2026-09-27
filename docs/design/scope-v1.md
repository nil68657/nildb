# NilDB v1 scope

Written 2026-09-27 to reconcile two documents that disagree about what v1 contains.

- [architecture.md](architecture.md) and [build-plan.md](build-plan.md) are the code contracts: package layout, exported names and signatures, byte layouts, concurrency rules, error strings. Code against them.
- [proposals/amendments-2026-09-27.md](proposals/amendments-2026-09-27.md) records the scope Arka chose ("basic"). The design panel's synthesis later pulled some of its deferred items back into v1. Where the two disagree on scope, this file decides.

## Deferred to v1.1

These stay designed in architecture.md so v1.1 needs no layout change, but v1 does not implement them:

1. **Columnar index maintenance.** `store` still opens the `col` column family with its options, so no migration is needed later. `DOC.CREATEINDEX` with a `"columnar"` key spec returns `ERR columnar indexes are not supported in v1`. The planner has two sources, `index` and `rowscan`; `NIL.EXPLAIN` reports one of those.
2. **Background index builds.** Every index, including 2dsphere, builds synchronously under the collection's exclusive lock (`LockColl` with `'w'`). `DOC.CREATEINDEX` returns when the index is ready. Index records still carry `state`, always `ready` in v1.
3. **Document TTL indexes and the sweeper.** `EXPIREAFTERSECONDS` returns `ERR TTL indexes are not supported in v1`.
4. **`NIL.GEOGROUP`, `NIL.STATS`, `NIL.GROUP`.** Use `NIL.AGGREGATE`.
5. **`cmd/htapbench`.** v1 ships an HTAP interference test inside the Go test suite instead (`tests/htap`), with smaller numbers so it runs in under two minutes.

## v1 aggregation surface

Stages: `$match`, `$project` (inclusion, exclusion, and computed fields), `$addFields`/`$set`, `$unset`, `$group`, `$sort`, `$limit`, `$skip`, `$count`, `$unwind`, `$geoNear` (first stage only).

Accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`, `$addToSet`.

Expressions: field paths, literals, `$literal`, `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$and`, `$or`, `$not`, `$cond`, `$ifNull`, `$concat`, `$toLower`, `$toUpper`, `$size`, `$year`, `$month`, `$dayOfMonth`. Anything else returns `ERR unsupported expression operator '<op>' in v1`.

## Everything else

Everything else in architecture.md sections 1 to 10 is v1, including `ROCKS.*`, `NIL.AGGREGATE`, `NIL.COUNT`, `NIL.DISTINCT`, `NIL.EXPLAIN`, `NIL.KEYSTATS`, `NIL.SNAPSHOT`, `DOC.AGGREGATE`, the 2dsphere operators, and the `rate_limiter_priority` cgo shim.

`redis-cli` and `redis-benchmark` are not installed on this Mac (`brew install redis` provides them). Compatibility tests in v1 use go-redis v9 and raw TCP; the `make compat` and `make tcl` targets are written but need Redis installed to run.
