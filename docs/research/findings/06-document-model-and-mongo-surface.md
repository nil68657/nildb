# MongoDB-alternative document-store surface over the Redis protocol for NilDB: BSON model, CRUD/query/aggregation semantics, geo, KV-backed index encoding, FerretDB lessons, RedisJSON/RediSearch command shapes, MongoDB wire-protocol feasibility, and Go-vs-Rust library availability

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:document-model-and-mongo-surface`, agent `a1fb5a8f64b91782b`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Store documents as raw BSON bytes (not JSON) keyed by collection-id + order-preserving-encoded _id. FerretDB v1's sjson wrapper and its long list of divergences (no NUL, no nested arrays, no NaN, -0 lost) came from squeezing BSON into JSONB; v2's whole point was a native BSON type. A RocksDB value column family holding BSON avoids that class of bug for free.
2. Build one order-preserving key codec and use it for _id, every secondary index, and range bounds. Follow KeyString's structure: a canonical type byte that reproduces MongoDB's cross-type order (MinKey < Null < Number < String < Object < Array < BinData < ObjectId < Bool < Date < Timestamp < Regex < MaxKey), then the value (ints and doubles collapsed into one numeric class, FDB-style sign-flip doubles, strings with 0x00 -> 0x00 0xFF escaping and 0x00 terminator), then bit-inverted bytes for descending fields, then the doc id, then a trailing TypeBits-style tag so 1 vs 1.0 round-trip exactly. Keep MongoDB's exclusive-bound discriminators (kLess/kEnd/kGreater) so $lt/$lte/$gt/$gte become Seek positions.
3. Secondary index key = [index-id][encoded value(s)][encoded _id] in a dedicated column family with a fixed-length prefix extractor over index-id; non-unique indexes carry _id in the key (CockroachDB non-unique rule) and unique indexes use [index-id][value] as the key with _id in the value so RocksDB itself catches E11000 on a read-before-write inside the same WriteBatch. Missing field indexes as null (one null allowed per unique index unless the index is sparse), arrays emit one entry per distinct element (multikey), and compound indexes reject documents with two array fields.
4. Implement the query matcher on decoded BSON with MongoDB's null/missing rules baked in: $ne, $nin, $not match missing fields; $exists:true includes null; comparison operators use type bracketing and 'any element matches' on arrays; $elemMatch pins all sub-predicates to one element while dotted paths do not. These are the semantics FerretDB spent years chasing; getting them right in the executor matters more than raw speed.
5. Index-usable predicates for v1: $eq, $gt/$gte/$lt/$lte ranges, $in (one seek per value), ^-anchored $regex (prefix range), plus compound-prefix rule and sort in same/reverse direction. Everything else ($ne, $nin, $not, unanchored $regex, $exists:false, $where) falls back to a collection scan under a RocksDB snapshot, which is also the HTAP analytical path.
6. Aggregation v1 as a pull-based operator chain over a snapshot iterator: $match (reuse matcher and index planner), $project/$addFields/$unset, $unwind, $sort (external sort spilling to a temp column family or files above 100 MB; append _id as tie-breaker for determinism), $group (hash aggregate with $sum/$avg/$min/$max/$count/$push/$addToSet/$first/$last), $limit/$skip (fuse $sort+$limit into top-k), $count, $geoNear (first stage only, rewritten into a geo index scan + distance projection + sort). Skip $lookup/$facet/$setWindowFields initially.
7. Geo: adopt S2 cell coverings exactly as MongoDB does. Index a point at level 30 and shapes at coarsest/finest levels around 2000 km / 110 m with max 20 covering cells (MongoDB v3 defaults); store [index-id][cell-id][_id] keys. $near = expanding ring of coverings with a bounded priority queue, $geoWithin/$geoIntersects = cover the query geometry, scan candidate cells, then run an exact spherical predicate on the stored geometry. Reject non-closed rings and hemisphere-or-larger polygons without the strictwinding CRS, same as MongoDB.
8. Language choice, on the evidence gathered here: Go has the only reasonably complete S2 port (golang/geo: RegionCoverer, Loop, Polygon, ShapeIndex, ClosestEdgeQuery, ContainsPointQuery), an official BSON module with a raw API, and a FerretDB-adjacent wire package; Rust's s2 crate has no loops or polygons, the geo crate's predicates are planar, and geo-only spherical polygon tests would need hand-written code. Rust gets you the DocumentDB gateway precedent and mongowire/bson crates. Unless another sub-agent's RocksDB-binding findings tilt hard the other way, geo makes Go the lower-risk pick for a MongoDB-style geo surface.
9. Redis-side command shape: follow the RediSearch precedent rather than inventing tokens. Documents: DOC.INSERT coll json..., DOC.FIND coll filter-json [PROJECT json] [SORT json] [LIMIT off n] returning [total, [docs...]] in RESP2 and a map {total_results, results} in RESP3; DOC.UPDATE coll filter update [MULTI] [UPSERT] returning n/nModified/upserted; DOC.DELETE coll filter [LIMIT 0|1]; DOC.AGGREGATE coll pipeline-json [CURSOR n]. Accept MongoDB Extended JSON so ObjectId/Date/Decimal128 survive the text protocol; emit RESP3 maps/doubles/booleans/null when HELLO 3 was negotiated and flat arrays otherwise.
10. Index DDL over RESP: DOC.CREATEINDEX coll {"field":1,"loc":"2dsphere"} [UNIQUE] [SPARSE] [EXPIREAFTERSECONDS n] mirroring createIndexes, with FT.CREATE-style INDEXMISSING semantics documented as MongoDB's sparse flag. Keep the 64-indexes-per-collection and 32-fields-per-compound limits.
11. TTL: a single background thread every 60 s walking [ttl-index-id][date-key] prefixes, deleting in batches of at most 50,000 docs / 1 s per index inside a WriteBatch, only for Date values (arrays: earliest date).
12. Cursors: mirror find/getMore. DOC.FIND returns cursor id + first batch (min 101 docs / 16 MiB); DOC.CURSOR READ id [COUNT n] and DOC.CURSOR DEL id, each cursor pinned to a RocksDB snapshot with a 30-minute idle timeout. This is also the FT.AGGREGATE WITHCURSOR shape, so Redis users will recognise it.
13. Native RocksDB feel: expose column families (one per collection data + one per index family), snapshots (every read command runs on an explicit snapshot id you can also create with DB.SNAPSHOT), write batches (MULTI/EXEC maps onto one WriteBatch), checkpoints (DB.CHECKPOINT path), compaction (DB.COMPACT cf) and iterators (DB.SCAN cf prefix) as first-class commands, and print RocksDB properties in INFO.
14. MongoDB wire protocol as a later phase is feasible and bounded: 16-byte header, OP_MSG kind-0/kind-1 sections, optional CRC-32C, OP_COMPRESSED optional, OP_QUERY only for hello/isMaster. The server must answer hello with isWritablePrimary:true, helloOk:true, maxWireVersion at least 13 (5.0 features) and minWireVersion 0, maxBsonObjectSize 16777216, maxMessageSizeBytes 48000000, maxWriteBatchSize 100000, logicalSessionTimeoutMinutes 30, and then dispatch insert/find/getMore/update/delete/aggregate/createIndexes/listCollections/listIndexes/ping/buildInfo/endSessions. Reuse the same executor; only the transport differs.
15. Reject up front what MongoDB itself restricts, to avoid FerretDB-style edge cases: dotted or $-prefixed keys (return the 5.0 rule as the reason), _id of array/regex/undefined type, documents over 16 MiB or 100 levels, deprecated BSON types on write, $where and $jsonSchema.

## Open questions

- Exact wire versions for MongoDB 6.0 and 8.0 (17 and 25 by arithmetic from the verified anchors 5.1=14, 7.0=21, 8.2=27, 9.0=29) were not read from a primary table; confirm before advertising maxWireVersion.
- Which RocksDB bindings are current for Go (grocksdb) and Rust (rust-rocksdb) on macOS Apple Silicon, and whether they expose column families, snapshots, WriteBatch, checkpoints and prefix extractors: outside this brief, but it is the other half of the language decision.
- Regex engine gap: MongoDB uses PCRE2; Go's regexp (RE2) and Rust's regex crate lack backreferences and lookaround. Decide whether to document the subset or bind PCRE2 via cgo/FFI.
- Decimal128 arithmetic for $inc/$sum/$avg: the BSON libraries carry the type but neither Go nor Rust has a standard IEEE decimal128 arithmetic package; decide whether v1 supports decimal in accumulators or errors out.
- Collation: MongoDB strings sort bytewise by default and collation is opt-in; confirm v1 ships bytewise only.
- How far to take MongoDB's $sort 'not stable' allowance versus RediSearch's deterministic paging advice; the proposal above appends _id as a tie-breaker, which differs from MongoDB behavior but is safer for cursors.
- Whether to encode index keys with MongoDB KeyString V1 numeric encoding (which unifies int/long/double/decimal into one comparable byte form) or the simpler FoundationDB split ints/doubles with a numeric normalisation step; the former is exact for decimal128 but more code.
- Multi-document transaction semantics for the Redis surface (MULTI/EXEC as one WriteBatch gives atomicity but no interactive reads); FerretDB v2 still has none, so shipping without them is defensible, but HTAP claims should state the isolation actually provided (snapshot reads, atomic batches).

## Findings (58)

### 1. BSON 1.1 defines 21 element types by a single type byte; a document is int32 total-length + element list + 0x00, and keys are NUL-terminated cstrings (no embedded NUL).

Spec v1.1 type bytes: 0x01 double, 0x02 string (int32 len + UTF-8 + NUL), 0x03 document, 0x04 array (document with keys "0","1",...), 0x05 binary (int32 len + subtype byte + bytes), 0x06 undefined (deprecated), 0x07 ObjectId (12 bytes), 0x08 bool, 0x09 UTC datetime int64 ms, 0x0A null, 0x0B regex (cstring pattern + cstring options), 0x0C DBPointer (deprecated), 0x0D JS code, 0x0E symbol (deprecated), 0x0F code_w_scope (deprecated), 0x10 int32, 0x11 timestamp uint64, 0x12 int64, 0x13 decimal128, 0xFF MinKey, 0x7F MaxKey. All integers little-endian.

- Source: https://bsonspec.org/spec.html
- Confidence: verified; design-critical: yes

### 2. MongoDB's $type numbers/aliases match the BSON bytes (1 double ... 19 decimal, -1 minKey, 127 maxKey) and four types are deprecated: undefined, dbPointer, symbol, javascriptWithScope.

Docs list each type with number and alias string; 'number' alias covers int, long, double, decimal. A minimal NilDB can refuse or read-only-tolerate the four deprecated types.

- Source: https://www.mongodb.com/docs/manual/reference/bson-types/
- Confidence: verified; design-critical: no

### 3. ObjectId is 12 bytes: 4-byte big-endian unsigned Unix-seconds timestamp, 5-byte per-process random value, 3-byte big-endian counter initialised to a random value and incremented per id, wrapping to 0 after 16,777,215.

Driver spec: timestamp must be read as unsigned 32-bit (extends range to 2106); random value is per machine+process and must be regenerated after fork; counter reset to 0 on overflow; duplicates possible beyond 16M ids/second/process. Server docs add that ObjectIds are not monotonic (1-second resolution, client clocks). Server generates one when _id is omitted on insert.

- Source: https://github.com/mongodb/specifications/blob/master/source/bson-objectid/objectid.md
- Confidence: verified; design-critical: yes

### 4. Document rules: _id is required, unique, immutable, always relocated to first position, may be any BSON type except array/regex/undefined; max document size is 16 MiB; nesting depth max 100; field order is preserved on write; duplicate field names are unsupported; field names cannot contain NUL.

Documents page states each rule; _id subfield names cannot begin with $; dot notation is 'embedded.field' and 'array.index' (zero-based); positional update operators $, $[], $[identifier].

- Source: https://www.mongodb.com/docs/manual/core/document/
- Confidence: verified; design-critical: yes

### 5. Since MongoDB 5.0, field names may contain '.' or start with '$' on insert, but such fields cannot be indexed, cannot be queried via dot notation, cannot be _id subfields, and need $getField/$setField to read or write.

Dot-dollar considerations page lists the restrictions; a period in a field name is always interpreted as a path separator by find/update, so NilDB can legitimately reject dotted keys in v1 (FerretDB v1 did the same).

- Source: https://www.mongodb.com/docs/manual/core/dot-dollar-considerations/
- Confidence: verified; design-critical: yes

### 6. MongoDB's cross-type sort order is: MinKey < Null < Numbers < Symbol/String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < Regex < JS < JS-with-scope < MaxKey; all numeric types compare as one class; a missing field sorts as null.

Comparison-order page. Arrays: ascending sort uses the smallest element, descending uses the largest; [] sorts before null. Objects compare pairwise in field order (type, then key name, then value). BinData compares length, then subtype, then bytes. Strings compare bytewise unless a collation is set. Comparison predicates ($lt/$gt) use type bracketing: only values of the operand's type class match.

- Source: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
- Confidence: verified; design-critical: yes

### 7. The insert command takes documents[] with ordered (default true = stop at first error); reply is {ok, n, writeErrors:[{index, code, errmsg}]}; duplicate key is code 11000 (E11000); a batch holds at most 100,000 documents.

Command reference shows the syntax and that with ordered:false the remaining documents are still attempted and each failure is listed by zero-based index; code 121 is schema-validation failure.

- Source: https://www.mongodb.com/docs/manual/reference/command/insert/
- Confidence: verified; design-critical: yes

### 8. The find command carries filter, sort, projection, skip, limit, batchSize, singleBatch, hint, collation, allowDiskUse, let and more; the reply is {cursor:{id, ns, firstBatch:[...]}, ok:1}; first batch is min(101 docs, 16 MiB) and later batches come from getMore {getMore:<id>, collection, batchSize}.

Command reference lists every field and the response shape; batchSize 0 opens a cursor without returning documents; getMore inherits readConcern and must run in the same session as the originating find.

- Source: https://www.mongodb.com/docs/manual/reference/command/find/
- Confidence: verified; design-critical: yes

### 9. Projection cannot mix inclusion and exclusion except for _id; supports $slice, $elemMatch, positional $, $meta and aggregation expressions; limit(0) means no limit; cursor method order does not matter; in-memory sort is capped at 100 MB unless allowDiskUse.

db.collection.find reference: 'natural order' applies when no sort is given; negative limit behaves like positive but closes the cursor; sort/limit/skip are applied logically regardless of chaining order.

- Source: https://www.mongodb.com/docs/manual/reference/method/db.collection.find/
- Confidence: verified; design-critical: no

### 10. The update command takes updates:[{q, u, multi (default false), upsert (default false), arrayFilters, hint, collation, sort (8.0)}]; u may be an operator document, a replacement document (multi must be false, _id is never replaced), or an aggregation pipeline (only $addFields/$set, $project/$unset, $replaceRoot/$replaceWith); reply carries n, nModified, upserted:[{index,_id}], writeErrors.

Command reference. Upsert _id comes from an equality match on _id in q, otherwise a new ObjectId is generated; nModified can be less than n when an update is a no-op; multi:true stops at the first failing document.

- Source: https://www.mongodb.com/docs/manual/reference/command/update/
- Confidence: verified; design-critical: yes

### 11. The delete command takes deletes:[{q, limit, collation, hint}] where limit 0 deletes all matches (deleteMany) and limit 1 deletes one (deleteOne); reply is {ok, n}.

Command reference. The docs do not define which document deleteOne removes when several match, so NilDB can pick the first in _id order.

- Source: https://www.mongodb.com/docs/manual/reference/command/delete/
- Confidence: verified; design-critical: no

### 12. Update operators: $set (creates nested paths), $unset, $inc (creates a missing field, errors if the target is non-numeric), $mul, $rename, $min, $max, $currentDate, $setOnInsert; array: $push (errors if target is not an array), $pop, $pull, $pullAll, $addToSet, with modifiers $each, $slice, $sort, $position; positional $, $[], $[identifier]; $bit. Since 5.0 operators are applied in lexicographic (numeric for numeric names) field order.

Update-operator reference; the document also notes pipeline-form updates since 4.2.

- Source: https://www.mongodb.com/docs/manual/reference/operator/update/
- Confidence: verified; design-critical: yes

### 13. The query-predicate catalog (docs labelled MongoDB 8.3) is: comparison $eq $ne $gt $gte $lt $lte $in $nin; logical $and $or $nor $not; array $all $elemMatch $size; element $exists $type; bitwise $bitsAllSet/AllClear/AnySet/AnyClear; geo $geoWithin $geoIntersects $near $nearSphere; misc $regex $mod $expr $jsonSchema $where.

Query operator reference page grouped by category. $where and $jsonSchema are the two that a non-JavaScript engine can skip.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/
- Confidence: verified; design-critical: yes

### 14. $ne, $nin and $not all match documents that lack the field; $not accepts only an operator expression or a regex, not a bare value; $ne against an array matches when no element equals the operand, and $ne against an array literal is a whole-array inequality.

$ne page (matches missing field; low selectivity for indexes) and $not page (includes documents without the field; {runtime:{$not:120}} is invalid; works with /regex/ and {$regex}). Arrays with $not can surprise, docs point to $nor.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/ne/
- Confidence: verified; design-critical: yes

### 15. $exists:true matches documents that have the field even when its value is null; $exists:false matches only documents lacking the field.

$exists page; it also notes $exists does not exist in aggregation expressions (use $type 'missing') and that $ne:null is the way to exclude nulls.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/exists/
- Confidence: verified; design-critical: yes

### 16. Array query semantics: {tags:['a','b']} is an exact ordered match, {tags:'a'} matches any element; comparison operators match if any element satisfies them; {'a.h':14,'a.w':21} may be satisfied by different elements while $elemMatch requires one element to satisfy all conditions; 'arr.1' addresses index position; $size matches exact length; $all requires all listed values.

Query-arrays tutorial and the $elemMatch reference (which also forbids $where and $text inside $elemMatch and shows the scalar form {results:{$elemMatch:{$gte:80,$lt:85}}}).

- Source: https://www.mongodb.com/docs/manual/tutorial/query-arrays/
- Confidence: verified; design-critical: yes

### 17. $regex uses PCRE2 since MongoDB 6.1 (PCRE 8.x before); options i, m, x, s, u (x and s need the $options form); a pattern anchored with ^ or \A becomes an index range scan; regex objects may appear inside $in but the {$regex} operator form may not; $not accepts either form.

$regex reference: no 'g' flag, (*UCP) supported from 6.1, case-insensitive regex does not use case-insensitive indexes. Go's RE2 and Rust's regex crate are not PCRE2; backreferences and lookaround would be unsupported.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/regex/
- Confidence: verified; design-critical: no

### 18. Aggregation stage catalog (docs 8.3): $match, $project, $addFields/$set, $unset, $group, $sort, $limit, $skip, $unwind, $count, $lookup, $graphLookup, $facet, $bucket, $bucketAuto, $sortByCount, $replaceRoot/$replaceWith, $sample, $unionWith, $setWindowFields, $densify, $fill, $documents, $redact, $merge, $out, $geoNear and others; $geoNear, $collStats, $changeStream must be first; $out/$merge must be last.

Pipeline-stage reference including which stages may be used in update pipelines ($addFields/$set, $project/$unset, $replaceRoot/$replaceWith).

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/
- Confidence: verified; design-critical: yes

### 19. $group requires _id (null groups everything) and supports accumulators $sum, $avg, $min, $max, $count, $push, $addToSet, $first, $last, $firstN, $lastN, $topN, $bottomN, $top, $bottom, $stdDevPop, $stdDevSamp, $median, $percentile, $mergeObjects, $accumulator, $concatArrays, $setUnion; it is a blocking stage capped at 100 MB unless it spills with allowDiskUse, and output order is not guaranteed.

$group reference; $sum ignores non-numeric values; {$count:{}} accumulator form exists alongside the $count stage.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
- Confidence: verified; design-critical: yes

### 20. $unwind has a string form and an object form {path, includeArrayIndex, preserveNullAndEmptyArrays}; a non-array value is treated as a one-element array (index null); missing/null/empty arrays are dropped unless preserveNullAndEmptyArrays:true.

$unwind reference with the behavior table for missing, null and [] under both settings.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
- Confidence: verified; design-critical: no

### 21. $project follows the same inclusion/exclusion rule as find projection (only _id may be excluded from an inclusion), accepts computed expressions and $$REMOVE, supports dotted and nested-document syntax, errors on parent/child path collisions and on an empty spec, and cannot index arrays.

$project reference; docs also say an early $project rarely helps performance because the planner already prunes fields.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/
- Confidence: verified; design-critical: no

### 22. $sort is not stable: ties come back in any order unless a unique key such as _id is appended; at most 32 sort keys; $sort followed directly by $limit keeps only top-n in memory; arrays sort by their minimum ascending and maximum descending; 100 MB memory cap with disk spill.

$sort aggregation reference including the {$meta:'textScore'} form.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/
- Confidence: verified; design-critical: yes

### 23. Pipeline limits: each result document at most 16 MiB, at most 1000 stages, 100 MB per stage before spilling; since 6.0 allowDiskUseByDefault decides whether $group, $sort, $bucket, $bucketAuto, $setWindowFields, $sortByCount spill or error.

Aggregation pipeline limits page; $search is exempt because it runs out of process.

- Source: https://www.mongodb.com/docs/manual/core/aggregation-pipeline-limits/
- Confidence: verified; design-critical: no

### 24. $geoNear must be the first stage, needs a 2d/2dsphere index (key required when several exist), takes near, distanceField (optional since 8.1 for non-timeseries), spherical, maxDistance/minDistance (meters for GeoJSON, radians for legacy; expressions allowed since 7.2), query (no $near inside), distanceMultiplier, includeLocs; output is sorted nearest-first with no default limit; 8.0 rejects non-Point GeoJSON in near.

$geoNear reference. NilDB can implement it as the $near filter plus a distance projection and a sort.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
- Confidence: verified; design-critical: yes

### 25. GeoJSON in MongoDB is {type, coordinates} with longitude first; supported types are Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection; legacy [lon, lat] pairs are also accepted; $near/$nearSphere sort by distance and cannot be combined with .sort() or used inside aggregation (use $geoNear); $geoWithin accepts $geometry, $box, $polygon, $center, $centerSphere (radius in radians).

Geospatial-queries page including index requirements: $geoIntersects needs 2dsphere semantics, $geoWithin/$near work with 2dsphere or 2d.

- Source: https://www.mongodb.com/docs/manual/geospatial-queries/
- Confidence: verified; design-critical: yes

### 26. $geoWithin polygons must be closed rings (first == last); by default a polygon denotes the smaller of the two areas it cuts the sphere into, and hemisphere-or-larger polygons need crs 'urn:x-mongodb:crs:strictwinding:EPSG:4326' with counter-clockwise winding; no index is required and results are unsorted.

$geoWithin reference; $geoIntersects page adds: any GeoJSON type, spherical semantics, 2dsphere index optional, and no guarantee for edge/vertex-only touching cases.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/
- Confidence: verified; design-critical: yes

### 27. $near with $geometry Point uses meters for $maxDistance/$minDistance and needs a 2dsphere index; legacy [x,y] form uses a 2d index with $maxDistance in radians and no $minDistance; results are nearest-first; not allowed in aggregation; 8.0+ accepts only Point.

$near reference; using .sort() on top adds a second sort and is discouraged.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/near/
- Confidence: verified; design-critical: yes

### 28. 2dsphere indexes take GeoJSON or legacy pairs (auto-converted to Point), use WGS84 lon/lat with wraparound, are always sparse (null/missing/empty-array excluded), may be compounded with non-geo fields, and in MongoDB 8.3 default to 2dsphereIndexVersion 4.

2dsphere index page; downgrading FCV below 8.3 requires dropping v4 indexes. S2 cell parameters finestIndexedLevel/coarsestIndexedLevel are exposed as index options.

- Source: https://www.mongodb.com/docs/manual/core/indexes/index-types/geospatial/2dsphere/
- Confidence: verified; design-critical: yes

### 29. MongoDB's S2 covering defaults (v7.0 source, initialize2dsphereParams): for index version > 2, finestIndexedLevel = level whose average cell edge is closest to 110 m, coarsestIndexedLevel = closest to 2000 km, maxCellsInCovering = 20; for version 1/2 it was 500 m, 100 km and 50 cells; points are always indexed at the finest level; radius = kRadiusOfEarthInMeters.

Read directly from src/mongo/db/index/expression_params.cpp on the v7.0 branch (the master branch has since moved this code); the comment says the defaults were tuned for buildings and state regions and that the levels apply to non-point shapes.

- Source: https://raw.githubusercontent.com/mongodb/mongo/v7.0/src/mongo/db/index/expression_params.cpp
- Confidence: verified; design-critical: yes

### 30. Redis's own geo surface is GEOADD/GEOSEARCH on a sorted set whose score is a 52-bit geohash: GEOSEARCH key FROMMEMBER|FROMLONLAT BYRADIUS r unit|BYBOX w h unit [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH], since Redis 6.2.

GEOSEARCH reference; reply is an array of member names or nested arrays of [name, dist, hash, [lon,lat]]. This is the Redis-feel precedent for a point-radius query, but it has no polygons or GeoJSON.

- Source: https://redis.io/docs/latest/commands/geosearch/
- Confidence: verified; design-critical: no

### 31. Go's golang/geo S2 port implements CellID, Cell, CellUnion, Cap, Loop, Polyline, LatLng, RegionCoverer, ShapeIndex, ClosestEdgeQuery, FurthestEdgeQuery, ContainsPointQuery, EdgeCrosser and TextFormat; Polygon, CellIndex and Lax shapes are 'mostly complete'; S2BooleanOperation, S2Builder, S2PointIndex, S2BufferOperation are absent.

README status table; the library has active CI (CodeQL, golangci-lint, Scorecard). This covers cell coverings for $near/$geoWithin/$geoIntersects and point-in-polygon on the sphere.

- Source: https://github.com/golang/geo
- Confidence: verified; design-critical: yes

### 32. The Rust s2 crate (yjh0502/rust-s2) is an incomplete port: CellID, Cell, CellUnion, RegionCoverer, Cap and Rect exist, but the README states lines and polygons are not implemented (no Loop, Polygon, Polyline).

Repository README: 'an ongoing port', 'lines and polygons aren't implemented yet', 97 stars, Apache-2.0. Spherical polygon containment/intersection would have to come from elsewhere.

- Source: https://github.com/yjh0502/rust-s2
- Confidence: verified; design-critical: yes

### 33. The Rust geo crate 0.33.1 (2026-09-23, MIT/Apache-2.0) provides Point/LineString/Polygon/MultiPolygon/GeometryCollection with Contains, Intersects, Within, Relate (DE-9IM), BooleanOps, plus Haversine/Geodesic/Rhumb distance metrics; its topological predicates are planar, only distances and some areas are spherical or geodesic.

docs.rs crate overview lists the traits and metric spaces; there is no S2-style hierarchical cell covering.

- Source: https://docs.rs/geo/latest/geo/
- Confidence: verified; design-critical: yes

### 34. FoundationDB's tuple layer is a proven order-preserving multi-type key encoding: 0x00 null, 0x01 bytes and 0x02 UTF-8 string (embedded 0x00 escaped as 0x00 0xFF, terminated by 0x00), 0x05 nested tuple, 0x0c..0x1c integers (0x14 = zero, negative ints big-endian one's complement so -1 is 0x13 0xFE), 0x20 float, 0x21 double (flip all bits when negative, only the sign bit when positive), 0x26 false, 0x27 true, 0x30 UUID, 0x33 versionstamp; 0xFF is reserved so the escape stays valid.

design/tuple.md in the FoundationDB repo; ordering across types follows the type code order and NaN/infinity/signed zero are handled by the float transform.

- Source: https://github.com/apple/foundationdb/blob/main/design/tuple.md
- Confidence: verified; design-critical: yes

### 35. CockroachDB keys are /tableID/indexID/indexed-values/[primary-key columns]/familyID; a non-unique secondary index puts the primary key in the key, a unique index puts it in the value so the KV layer enforces uniqueness; encoding markers: null 0x00, notnull 0x01, float NaN/neg/zero/pos 0x02..0x05, bytes 0x12, ints in 0x80..0xfd with intZero = 136 (one byte for small magnitudes, length-prefixed otherwise), descending variants complement bytes (nullDesc 0xff); bytes escape 0x00 as 0x00 0xff and terminate with 0x00 0x01.

docs/tech-notes/encoding.md (structure, enc(x) <= enc(y) iff x <= y, NULL columns omitted) and pkg/util/encoding/encoding.go (marker constants and varint scheme).

- Source: https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md
- Confidence: verified; design-critical: yes

### 36. MongoDB's own index format, KeyString, encodes BSON keys so that memcmp order equals BSON comparison order; the buffer is encoded values + optional RecordId + trailing TypeBits, where TypeBits recover the exact original type (int vs long vs double) without affecting order; descending fields are stored bit-inverted; canonical type bytes are kMinKey 10, kUndefined 15, kNullish 20, kNumeric 30..51 (NaN, negative large ... positive large), kStringLike 60, kObject 70, kArray 80, kBinData 90, kOID 100, kBool 110/111, kDate 120, kTimestamp 130, kRegEx 140, kDBRef 150, kCode 160, kCodeWithScope 170, kMaxKey 240; discriminators kLess 1, kEnd 4, kGreater 254; strings terminate with 0x00 and escape embedded NUL as 0x00 0xFF.

key_string.h comments (TypeBits 'cannot be stored in place since we don't want them to affect the ordering (1 and 1.0 compare as equal)', V0/V1 numeric encodings, kInclusive/kExclusiveBefore/kExclusiveAfter) and key_string.cpp constants and memcpy_flipBits; RecordId uses a variable-length encoding whose size is readable from the last byte.

- Source: https://github.com/mongodb/mongo/blob/master/src/mongo/db/storage/key_string/key_string.h
- Confidence: verified; design-critical: yes

### 37. Compound indexes hold up to 32 fields, each with its own direction; an index serves queries on any prefix of its fields and sorts in the same or exactly reversed direction of all fields; at most one hashed field; per document at most one indexed field may be an array.

Compound index page (ESR guideline, prefix rule) and multikey page (compound multikey restriction, one key per distinct array element, automatic multikey detection, hashed indexes cannot be multikey).

- Source: https://www.mongodb.com/docs/manual/core/indexes/index-types/index-compound/
- Confidence: verified; design-critical: yes

### 38. Unique indexes store a null key for documents missing the field, so only one document may lack it (unless the index is also sparse); compound uniqueness applies to the tuple; in a multikey unique index duplicate values inside one document are allowed but the same value across documents is an E11000 duplicate key error.

Unique index page with the Arya/Jon Snow example and the ["a@test.com","a@test.com"] example; since 5.0 a unique sparse and unique non-sparse index may coexist on the same key pattern.

- Source: https://www.mongodb.com/docs/manual/core/index-unique/
- Confidence: verified; design-critical: yes

### 39. TTL indexes are single-field only (not _id, not compound, not capped collections), created with expireAfterSeconds 0..2147483647 on a Date or array-of-Dates field (earliest date wins; non-date or missing values never expire); a background thread runs every 60 seconds, since 6.1 deletes in batches and stops after 50,000 documents or 1 second per index before moving on; expireAfterSeconds is changed via collMod.

TTL index page; expired documents may linger 0-60 s.

- Source: https://www.mongodb.com/docs/manual/core/index-ttl/
- Confidence: verified; design-critical: no

### 40. Server limits relevant to NilDB: 16 MiB document, 100 nesting levels, 64 indexes per collection, 32 fields per compound index, 32 sort keys, namespace 255 bytes, database name under 64 bytes, collection names must start with a letter or underscore and cannot contain '$', NUL or 'system.'; write batch 100,000 ops; 1000 pipeline stages.

Limits page; the pre-4.2 1024-byte index key limit no longer applies.

- Source: https://www.mongodb.com/docs/manual/reference/limits/
- Confidence: verified; design-critical: no

### 41. FerretDB v2 (current docs v2.7/v2.8) is a Go proxy that translates the MongoDB 5.0+ wire protocol to SQL against PostgreSQL with the DocumentDB extension; v1 lives on the main-v1 branch; Apache-2.0; module path github.com/FerretDB/FerretDB/v2/ferretdb.

docs.ferretdb.io introduction and the GitHub README ('drop-in replacement for MongoDB 5.0+ in many cases').

- Source: https://github.com/FerretDB/FerretDB
- Confidence: verified; design-critical: no

### 42. FerretDB v1 mapped a MongoDB database to a PostgreSQL schema, a collection to a table and a document to a row with one JSONB column (SQLite backend: database = file, collection = table, one JSON1 column), and preserved BSON types by wrapping documents in 'sjson': a '$s' schema object with '$k' (key order) and 'p' (per-field type descriptors: binary subtype 's', regex options 'o', dates as epoch ms, int32 vs int64, ObjectId, timestamp).

v1.24 'Understanding FerretDB' page and the package doc of internal/handler/sjson/sjson.go on the main-v1 branch.

- Source: https://raw.githubusercontent.com/FerretDB/FerretDB/main-v1/internal/handler/sjson/sjson.go
- Confidence: verified; design-critical: yes

### 43. FerretDB v1's documented divergences from MongoDB were: no NUL bytes in strings, no nested arrays, keys may not contain '.' or start with '$', doubles may not be Infinity/-Infinity/NaN and updates producing them fail, -0 becomes 0, duplicate keys in one insert rejected, names may not start with '_ferretdb_', database names Latin-only, collection names valid UTF-8, error messages may differ from MongoDB while codes match.

v1.24 'Known differences' page. These are exactly the corners a JSON-on-SQL mapping cannot represent; a BSON-native store avoids most of them.

- Source: https://docs.ferretdb.io/v1.24/diff/
- Confidence: verified; design-critical: yes

### 44. FerretDB moved to Microsoft's DocumentDB PostgreSQL extension for v2.0 (RC1 announced 2025-01-23) because storing documents as generic JSONB was slow and limited; the announcement claims up to 20x faster for some workloads and adds vector search and replication. DocumentDB consists of pg_documentdb_core (a native BSON type for Postgres), pg_documentdb (the CRUD/API layer), pg_documentdb_extended_rum (index access method) and pg_documentdb_gw, a MongoDB-wire gateway written in Rust (Cargo.toml, documentdb_gateway* crates); MIT licensed.

FerretDB v2 release blog post and the microsoft/documentdb repository (README and pg_documentdb_gw directory listing).

- Source: https://blog.ferretdb.io/ferretdb-releases-v2-faster-more-compatible-mongodb-alternative/
- Confidence: verified; design-critical: yes

### 45. FerretDB v2 still lists gaps: abortTransaction/commitTransaction unimplemented (no multi-document transactions), no cloneCollectionAsCapped/convertToCapped, no role management commands, error messages may differ from MongoDB, collection names must be valid UTF-8.

docs.ferretdb.io/migration/compatibility/ ('All drivers and applications compatible with MongoDB 5.0+ should be compatible with FerretDB').

- Source: https://docs.ferretdb.io/migration/compatibility/
- Confidence: verified; design-critical: no

### 46. JSON.SET key path value [NX|XX] [FPHA FP16|BF16|FP32|FP64]: a new key can only be created at the root path ($ or .), a missing final object member is created when the parent exists, a missing intermediate path returns nil, NX/XX gate on whether the path matches, and errors are 'ERR new objects must be created at the root', 'ERR wrong static path', 'ERR index out of bounds'; FPHA arrived in Redis 8.8.

JSON.SET command page (RESP2/RESP3 return tables and examples such as JSON.SET doc $..a 3 updating every match).

- Source: https://redis.io/docs/latest/commands/json.set/
- Confidence: verified; design-critical: no

### 47. JSON.GET key [INDENT s] [NEWLINE s] [SPACE s] [path ...] returns a bulk string; a JSONPath ($...) yields a JSON array of all matches, a legacy path (. or bare name) yields a single value, and several paths yield a JSON object keyed by path; RESP3 defaults the path to $.

JSON.GET command page plus the JSON path page, which lists supported JSONPath syntax: $, .name, ['name'], [i], [start:end:step], .., *, ?() filters with ==, !=, <, <=, >, >=, =~, &&, ||, !, in/nin, subsetof/anyof/noneof, size, empty, and the '~' key selector.

- Source: https://redis.io/docs/latest/commands/json.get/
- Confidence: verified; design-critical: no

### 48. FT.CREATE idx ON HASH|JSON [PREFIX n p...] [FILTER expr] ... SCHEMA <identifier> [AS alias] TEXT|TAG|NUMERIC|GEO|GEOSHAPE [SPHERICAL|FLAT]|VECTOR [SORTABLE [UNF]] [NOINDEX] [INDEXEMPTY] [INDEXMISSING] declares a secondary index where, for JSON, the identifier is a JSONPath and AS gives it a query alias; GEO stores 'lon,lat' strings, GEOSHAPE stores WKT polygons; limits are 1024 attributes and 128 TEXT attributes.

FT.CREATE reference; INDEXMISSING/INDEXEMPTY (v2.10) exist because missing values are otherwise not indexed, mirroring MongoDB's sparse vs non-sparse distinction.

- Source: https://redis.io/docs/latest/commands/ft.create/
- Confidence: verified; design-critical: yes

### 49. FT.SEARCH idx query [NOCONTENT] [RETURN n field [AS alias]...] [SORTBY f ASC|DESC [WITHCOUNT]] [LIMIT off n] [PARAMS n k v...] [DIALECT d] [TIMEOUT ms] replies in RESP2 as [total, id1, [f, v, ...], id2, ...] (LIMIT default 0 10; LIMIT 0 0 returns only the count) and in RESP3 as a map {total_results, results:[maps], attributes, format, warning}; geo radius is written inside the query string as @loc:[lon lat r km] and FILTER/GEOFILTER arguments are deprecated in favor of dialect syntax.

FT.SEARCH reference; unsorted LIMIT paging is documented as non-deterministic and the docs recommend SORTBY on a unique field or FT.AGGREGATE WITHCURSOR.

- Source: https://redis.io/docs/latest/commands/ft.search/
- Confidence: verified; design-critical: yes

### 50. FT.AGGREGATE idx query [LOAD n f...|LOAD *] [GROUPBY n prop... REDUCE fn nargs args [AS name]...] [SORTBY n prop ASC|DESC... [MAX n]] [APPLY expr AS name] [LIMIT off n] [FILTER expr] [WITHCURSOR [COUNT n] [MAXIDLE ms]] [PARAMS ...] [DIALECT d] is a token-encoded pipeline; reducers are COUNT, COUNT_DISTINCT, COUNT_DISTINCTISH, SUM, MIN, MAX, AVG, STDDEV, QUANTILE, TOLIST, FIRST_VALUE, RANDOM_SAMPLE and COLLECT (Redis 8.8); APPLY has string, math, date and geodistance() functions; GROUPBY 0 reduces everything; RESP2 reply is an array whose first integer is meaningless, RESP3 is a map like FT.SEARCH.

FT.AGGREGATE reference; example FT.AGGREGATE gh '*' GROUPBY 1 @actor REDUCE COUNT 0 AS num SORTBY 2 @num DESC MAX 10 is the Redis shape of $group + $sort + $limit.

- Source: https://redis.io/docs/latest/commands/ft.aggregate/
- Confidence: verified; design-critical: yes

### 51. RESP2 has + simple string, - error, : integer, $ bulk string ($-1 null), * array (*-1 null); RESP3 adds _ null, # boolean, , double, ( big number, ! bulk error, = verbatim string, % map, | attribute, ~ set, > push; clients send commands as arrays of bulk strings, negotiate with HELLO 3 (server answers -NOPROTO or -ERR unknown command on older servers), and RESP2 represents maps as flat key/value arrays.

Redis protocol specification; Redis itself does not emit streamed strings/aggregates.

- Source: https://redis.io/docs/latest/develop/reference/protocol-spec/
- Confidence: verified; design-critical: yes

### 52. MongoDB wire messages start with a 16-byte little-endian header {messageLength, requestID, responseTo, opCode}; OP_MSG (2013) is {flagBits, sections..., optional CRC-32C}: bit 0 checksumPresent, bit 1 moreToCome, bit 16 exhaustAllowed; section kind 0 is one BSON body, kind 1 is {int32 size, cstring identifier, BSON...} document sequence; OP_COMPRESSED (2012) wraps messages with noop/snappy/zlib/zstd; OP_REPLY/OP_UPDATE/OP_INSERT/OP_GET_MORE/OP_DELETE/OP_KILL_CURSORS were removed in 5.1 and OP_QUERY (2004) survives only for hello/isMaster.

Wire protocol reference; bits 0-15 of flagBits must error on unknown values, bits 16-31 must be ignored; checksums are skipped under TLS.

- Source: https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
- Confidence: verified; design-critical: yes

### 53. The hello command (4.2+, successor to isMaster) must return isWritablePrimary, maxBsonObjectSize (16 MB), maxMessageSizeBytes (48 MB), maxWriteBatchSize (100,000), localTime, logicalSessionTimeoutMinutes, connectionId, minWireVersion, maxWireVersion, readOnly, ok, plus optional compression and saslSupportedMechs; replica-set fields (hosts, setName, primary, me, electionId) are only needed when emulating a replica set.

hello command reference; drivers use it for discovery and capability negotiation.

- Source: https://www.mongodb.com/docs/manual/reference/command/hello/
- Confidence: verified; design-critical: yes

### 54. Per the driver handshake spec, the first message on a connection is hello (or legacy isMaster with helloOk:true) sent as OP_MSG, carrying a client metadata document (driver, os, platform, optional application.name, env) capped at 512 bytes, optional saslSupportedMechs, speculativeAuthenticate, compression, loadBalanced and a 'backpressure' field; drivers then check maxWireVersion/minWireVersion overlap and helloOk.

mongodb-handshake/handshake.md in the specifications repo. A server that answers hello with helloOk:true and sane wire versions is accepted by modern drivers.

- Source: https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.md
- Confidence: verified; design-critical: yes

### 55. Wire version numbers: server enum defines SUPPORTS_OP_MSG=6 (3.6), REPLICA_SET_TRANSACTIONS=7 (4.0), 4.4=9, 5.0=13, 5.1=14, 9.0=29 and LATEST_WIRE_VERSION = 7 + releases since 4.0; the Node driver pins MIN_SUPPORTED_WIRE_VERSION=9 (server 4.4), MAX=29 (9.0), and maps 7.0 to 21 and 8.2 to 27. By the same arithmetic 6.0 = 17 and 8.0 = 25.

src/mongo/db/wire_version.h (master and v8.0) and node-mongodb-native src/cmap/wire_protocol/constants.ts. The 6.0=17 and 8.0=25 values are interpolated, not read verbatim; the specifications 'wireversion-featurelist' table returned 404.

- Source: https://raw.githubusercontent.com/mongodb/node-mongodb-native/main/src/cmap/wire_protocol/constants.ts
- Confidence: likely; design-critical: no

### 56. Go wire-protocol code exists (github.com/FerretDB/wire with OP_MSG/OP_QUERY/OP_REPLY and a wirebson package, Apache-2.0) but its README says it is still being extracted from FerretDB and should not be used yet; Rust has mongowire 0.1.1 (2026-09-11, Apache-2.0) with OP_MSG/OP_QUERY/OP_REPLY/OP_COMPRESSED, CRC-32C validation, a tokio codec/server feature and handler traits, built on the wirebson crate, also early-stage.

FerretDB/wire README and docs.rs/mongowire. Either way the framing is small enough to hand-write in a few hundred lines.

- Source: https://docs.rs/mongowire/latest/mongowire/
- Confidence: verified; design-critical: no

### 57. BSON libraries are mature in both languages: Go go.mongodb.org/mongo-driver/v2/bson v2.9.1 (2026-09-10, Apache-2.0) with D/M/A/E, Raw/RawValue, ObjectID/NewObjectID, Decimal128, Marshal/Unmarshal and a codec Registry; Rust bson 3.1.0 (2026-08-31, MIT, MongoDB-maintained) with Document, Bson enum, RawDocument/RawDocumentBuf zero-copy API, ObjectId, Decimal128, serde and extended JSON.

pkg.go.dev and docs.rs pages for the two crates/modules.

- Source: https://docs.rs/bson/latest/bson/
- Confidence: verified; design-critical: yes

### 58. RocksDB prefix seek: set options.prefix_extractor (NewCappedPrefixTransform recommended), enable a prefix bloom via filter_policy with whole_key_filtering=false; use read_options.auto_prefix_mode (6.8+) or total_order_seek; iterating past the prefix without total_order_seek is undefined (deleted keys, wrong order); the safe idiom is Seek(prefix) and stop when key no longer starts_with(prefix), or set iterate_upper_bound.

RocksDB wiki Prefix-Seek page.

- Source: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
- Confidence: verified; design-critical: yes
