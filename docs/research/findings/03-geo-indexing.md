# Geospatial indexing for NilDB: Redis GEO, MongoDB 2dsphere, S2 fundamentals, Go vs Rust S2 libraries, and KV-backed geo layouts (CockroachDB, Tile38, Kvrocks)

Exported on 2026-09-27 from the NilDB deep-research workflow (Claude Code run `wf_1b4f4c38-bbf`, step `research:geo-indexing`, agent `a71e1fb3e371f0563`, researched 2026-09-26). The text is the agent's structured output, unedited; confidence labels are the agent's own.

## Design implications

1. Language: choose Go. golang/geo has every S2 piece NilDB's Mongo-style queries need (CellID ranges, CellUnion, Cap, Loop, Polygon.Contains/Intersects/ContainsPoint, RegionCoverer with Covering/InteriorCovering, ShapeIndex + ClosestEdgeQuery) marked complete or mostly complete and is what CockroachDB builds on. The Rust s2 crate 0.2.0 has no RegionCoverer, Loop, Polygon, Polyline or ShapeIndex ('lines and polygons aren't implemented yet'); doing NilDB in Rust means writing a spherical region coverer and polygon containment/intersection yourself, or falling back to georust/geo whose Contains/Intersects are planar and would break $geoIntersects and big-polygon semantics.
2. Two separate geo key types, not one. (a) Redis GEO keys are ZSETs whose score is the exact Redis 52-bit interleaved geohash (26+26 bits, lat clamped to +/-85.05112878, haversine with R=6372797.560856 m); implement GEOSEARCH with the 9-cell neighbor algorithm and per-cell score-range iteration, exactly as Kvrocks does, so WITHHASH/GEOHASH/GEODIST outputs are byte-for-byte Redis-compatible. (b) Mongo-style collections index GeoJSON with S2 cell ids at +/-90 latitude with meters distances. Do not try to serve $geoIntersects from the geohash sorted set: geohash cells are not a hierarchy you can cover polygons with cheaply and Redis's model is points only.
3. Column-family layout for the S2 index (native RocksDB feel): a dedicated CF, e.g. geo_idx, keyed [index_id u32][cell_id u64 big-endian][doc_id], value = empty or the leaf cell/coords. Because every S2 cell's descendants are one inclusive id range [RangeMin, RangeMax], each covering cell becomes one RocksDB iterator range scan with iterate_lower_bound/upper_bound, and ancestor checks are point Gets. This is exactly MongoDB's S2CellIdsToIntervalsWithParents and CockroachDB's UnionKeySpans; expose the covering, the spans, and the iterator count in EXPLAIN-style output so the RocksDB mechanics stay visible.
4. Points get one key (their level-30 leaf cell); LineString/Polygon/Multi* get a RegionCoverer covering bounded by [coarsest, finest] levels and max cells. Copy MongoDB v3 defaults as the starting point: finest 110 m (about level 16), coarsest 2000 km (about level 2), max 20 cells for stored shapes, and expose them per index like CockroachDB (s2_max_level, s2_level_mod, s2_max_cells) rather than hard-coding. Query coverings need their own, smaller budget (CockroachDB default 4 cells; MongoDB uses server parameters internalQueryS2GeoMaxCells/CoarsestLevel/FinestLevel).
5. Query algorithms: $geoWithin/$geoIntersects = cover the query geometry, union the descendant ranges of each covering cell plus point-lookups of each ancestor cell down to the coarsest indexed level, dedupe doc ids, then re-check every candidate exactly with s2.Polygon.Contains / Intersects on the document's actual geometry (false positives are expected; both CockroachDB and MongoDB do this). $near/$nearSphere/$geoNear = MongoDB's expanding annulus: seed the radius with a density estimate, cover (outer Cap minus inner Cap), subtract the CellUnion already scanned, scan, compute exact haversine distances, emit sorted batches, grow the increment x2 below ~300 results and halve above ~600; honor $minDistance/$maxDistance and stop early. S2ClosestEdgeQuery cannot be used directly because it needs an in-memory ShapeIndex.
6. Index maintenance in write batches: an update must delete the old covering keys and insert the new ones in the same rocksdb::WriteBatch as the document write. Either recompute the old covering from the previous document value read under the same snapshot, or store a reverse entry doc_id -> [cell ids] in the index CF so deletes never need re-covering. Keep the doc's original coordinates (or leaf cell) available without reading the whole document, e.g. as the index value, so distance filtering during $near stays inside the index CF.
7. Semantics to encode explicitly: GeoJSON coordinates are [lon, lat]; legacy [x, y] pairs are accepted and converted to points (MongoDB v4 parses GeoJSON before legacy); polygons are S2 loops with interior on the left, shells at even depth and holes at odd depth (PolygonFromLoops / nested form); a single-ring polygon larger than a hemisphere is only honored when the query carries crs urn:x-mongodb:crs:strictwinding:EPSG:4326, otherwise the smaller side is meant; $geoWithin is 'entirely within' and unsorted; $geoIntersects makes no promise about edge-only contact; $near GeoJSON distances are meters, legacy 2d/$centerSphere radii are radians. Skip the legacy planar 2d index type entirely; implement only 2dsphere semantics.
8. HTAP angle: because S2 ids are Hilbert-ordered, analytical aggregations by area are prefix scans on the index CF: truncate cell ids to a parent level L (id.Parent(L)) while iterating a snapshot and group counts/sums per cell, or scan a whole covering range in one iterator. Run these scans on a RocksDB snapshot or checkpoint so point writes to the same CF do not interfere; that is the concrete meaning of 'transactional point ops plus analytical scans over the same data' for geo.
9. Do not use H3/h3o for the primary index: its 64-bit ids put the resolution field above the digit fields, so descendants across resolutions are not one contiguous key range and every containment query would need one range per resolution; S2's single [RangeMin, RangeMax] per cell is what makes the KV layout cheap. rstar and Tile38's R-tree are in-memory 'divide the objects' structures and cannot be persisted as RocksDB key ranges; use them, if at all, only as an optional memory cache.
10. Redis GEO compatibility details worth a test suite from day one: GEOADD rejects |lat| > 85.05112878 and lon outside +/-180; GEOSEARCH BYBOX uses a bounding box computed at the center latitude; results for huge radii dedupe identical neighbor ranges; COUNT n ANY may return unsorted; GEOHASH returns 11 base32 chars computed with +/-90 latitude; GEODIST uses haversine with R=6372797.560856 (documented worst-case error 0.5%); GEORADIUS/GEORADIUSBYMEMBER still exist but are deprecated aliases of GEOSEARCH (since 6.2).

## Open questions

- MongoDB's kRadiusOfEarthInMeters value was not read from source in this pass (only its use); Redis uses 6372797.560856 m. NilDB must pick one radius per key type and document it, since GEODIST and $geoNear distances will differ from the originals by the ratio of radii.
- golang/geo marks S2Polygon 'mostly complete' but pkg.go.dev shows no TODOs on Contains/Intersects; which C++ methods are missing (likely S2BooleanOperation-based ops and some encoding) needs a quick check of polygon.go before relying on polygon-vs-polygon Intersects for $geoIntersects with holes.
- The rust-s2 README points at its issue tracker for status; whether a RegionCoverer or Loop port is actively in progress there (which could change the Rust calculus in 6-12 months) was not checked.
- MongoDB's query-side coverer limits (internalQueryS2GeoMaxCells, internalQueryS2GeoCoarsestLevel, internalQueryS2GeoFinestLevel) were seen as parameter names but their default values were not read; the density-estimator's exact seed radius and the 300/600 thresholds in geo_near.cpp were read on master, not on a tagged release.
- Whether NilDB should keep ancestor-cell keys for stored shapes (CockroachDB style, which speeds CoveredBy) or only covering cells with ancestor point lookups at query time (MongoDB style, fewer keys per document) depends on the expected polygon-vs-point ratio in workloads; not decidable from documentation alone.
- The s2cell_hierarchy devguide page's level/size table came back garbled through the fetch tool (e.g. it printed level 10 as 324 km2 whereas the statistics page says 81.07 km2 average); all level numbers in this report use the s2cell_statistics page, and the derived MongoDB level mappings (16/2 and 14/6) should be confirmed by calling s2.AvgEdgeMetric.ClosestLevel in Go.
- How Kvrocks handles GEOSEARCH result ordering and COUNT ANY across the 9 boxes on RocksDB (it must scan every box before sorting, same as Redis) was not examined beyond the range-scan structure; measuring its latency on a RocksDB-backed ZSet would set a realistic baseline for NilDB's Redis-GEO path.

## Findings (39)

### 1. Redis GEO is a sorted set whose score is a 52-bit interleaved geohash; indexable latitude is limited to +/-85.05112878 (EPSG:3785 / Web Mercator), so areas near the poles cannot be indexed.

GEOADD doc: data is stored in the key as a sorted set; latitude and longitude bits are interleaved to form a unique 52-bit integer, which a double score represents without losing precision. Valid longitudes -180..180, valid latitudes -85.05112878..85.05112878 'as specified by EPSG:900913 / EPSG:3785 / OSGEO:41001'; out-of-range coordinates return an error. Radius/box queries check '1+8 areas' by stripping low bits from the score and range-querying the sorted set per area. Distance uses the Haversine formula on a sphere; worst-case error 'may be up to 0.5%'. There is no GEODEL; use ZREM. GEOADD is O(log N) per item, since 3.2.0; NX/XX/CH since 6.2.0.

- Source: https://redis.io/docs/latest/commands/geoadd/
- Confidence: verified; design-critical: yes

### 2. GEOSEARCH (Redis 6.2.0+) replaces GEORADIUS/GEORADIUSBYMEMBER and defines the query surface NilDB must match: FROMMEMBER|FROMLONLAT, BYRADIUS r unit | BYBOX w h unit, units M|KM|FT|MI, ASC|DESC, COUNT n [ANY], WITHCOORD, WITHDIST, WITHHASH.

Doc states complexity 'O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided as the filter and M is the number of items inside the shape'. COUNT without ANY sorts everything matching the area, so a large area with a small COUNT may be slow; ANY returns as soon as enough matches are found, possibly unsorted. WITHHASH returns 'the raw 52-bit geohash-encoded score'. Reply shape: array of member names, or arrays of [member, dist, hash, [lon, lat]] when WITH* options are given (RESP2 and RESP3 identical).

- Source: https://redis.io/docs/latest/commands/geosearch/
- Confidence: verified; design-critical: yes

### 3. Redis's geohash encoder (geohash.c) interleaves 26 bits of latitude and 26 bits of longitude via a lookup-table interleave64 with 0x5555555555555555-style magic constants, lat bits in even positions and lon bits in odd; 8 neighbors are computed by geohash_move_x / geohash_move_y arithmetic on the hash.

File header: copyright 2013-2014 yinqiwen (the ardb geohash implementation), later Matt Stancliff and Redis Ltd, BSD license. Constants GEO_LAT_MIN/MAX, GEO_LONG_MIN/MAX follow 'constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001' with a comment that 'We can't geocode at the north/south pole'. geohashEncodeWGS84 converts to fixed point by step (up to 32 in the general encoder; GEO_STEP_MAX 26 is what geo.c passes), geohashDecode reverses with deinterleave64, geohashNeighbors applies directional moves.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/geohash.c
- Confidence: verified; design-critical: yes

### 4. Redis's neighbor-cell search picks a step (precision) by doubling the radius until it exceeds MERCATOR_MAX, then backs off 2 steps, subtracts 1 more above |66 deg| latitude and another above |80 deg|, clamps to [1,26]; it then computes a bounding box, the center cell plus 8 neighbors, lowers the step again if the 9 cells do not cover the box, and zeroes neighbors that fall entirely outside the box.

geohash_helper.c: geohashEstimateStepsByRadius: 'if (range_meters == 0) return 26; int step = 1; while (range_meters < MERCATOR_MAX) { range_meters *= 2; step++; } step -= 2;' then the >66 / >80 latitude decrements and clamping to 1..26. MERCATOR_MAX = 20037726.37, EARTH_RADIUS_IN_METERS = 6372797.560856 (the haversine radius). geohashBoundingBox derives lat/lon deltas from height/EARTH_RADIUS_IN_METERS with cosine-adjusted longitude. geohashCalculateAreasByShapeWGS84 encodes the center, gets neighbors, runs a 'decrease_step' check ('Check if the step is enough at the limits of the covered area') and re-encodes at step-1 if not, then excludes 'useless' neighbor areas outside min/max lat/lon. Also defines R_MAJOR 6378137.0, R_MINOR 6356752.3142 for the Mercator helpers. geohashGetDistance is haversine with a shortcut when longitudes are equal.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/geohash_helper.c
- Confidence: verified; design-critical: yes

### 5. geo.c turns GEOADD into ZADD with score = geohashAlign52Bits(hash), and answers searches by iterating each of the 9 cells as a ZRANGEBYSCORE over the cell's aligned score range and re-checking every candidate against the exact shape; duplicate neighbor ranges are skipped for very large radii; COUNT ANY breaks out early.

geoaddCommand: 'geohashEncodeWGS84(xy[0], xy[1], GEO_STEP_MAX, &hash); GeoHashFix52Bits bits = geohashAlign52Bits(hash);' then rewrites argv into a ZADD. membersOfAllNeighbors loops over the 9 neighbors calling membersOfGeoHashBox; scoresOfGeoHashBox left-shifts the hash bits to 52-bit alignment to get [min,max]; geoWithinShape computes the distance and appends only if inside. Comment: 'When a huge Radius (in the 5000 km range or more) is used, adjacent neighbors can be the same, leading to duplicated elements', so equal bits/step ranges are skipped. COUNT ANY: 'if (ga->used && limit && ga->used >= limit) break;'. GEOHASH command re-encodes with the standard +/-90 lat range and emits 11 base32 chars from '0123456789bcdefghjkmnpqrstuvwxyz'.

- Source: https://raw.githubusercontent.com/redis/redis/unstable/src/geo.c
- Confidence: verified; design-critical: yes

### 6. The GEOHASH command returns 11-character standard geohash strings (prefix-truncatable, 'no precision is lost compared to the Redis internal 52-bit representation'), computed with the standard +/-90 latitude bounds rather than Redis's internal +/-85 encoding.

Doc: Redis normally uses 'a variation of the Geohash technique where positions are encoded using 52-bit integers' whose min/max coordinates differ from the standard; GEOHASH returns a standard Wikipedia-style geohash. Example: Palermo -> 'sqc8b49rny0', Catania -> 'sqdtr74hyu0'. O(1) per member, since 3.2.0.

- Source: https://redis.io/docs/latest/commands/geohash/
- Confidence: verified; design-critical: no

### 7. MongoDB 2dsphere indexes have four versions: v1 (2.4), v2 (2.6, adds MultiPoint/MultiLineString/MultiPolygon/GeometryCollection), v3 (3.2, numeric instead of string cell-id keys, default 3.2-8.2), v4 (default from 8.3; only change is trying GeoJSON parsing before legacy-point parsing). 2dsphere indexes are always sparse and a compound 2dsphere index may contain several location fields.

s2_common.h on master: S2_INDEX_VERSION_3 'Introduced performance improvements and changed the key type from string to numeric'; S2_INDEX_VERSION_4 'Changed parsing order for object-type geometry elements to try GeoJSON parsing before legacy point parsing.' 2dsphere docs: 'Starting in MongoDB 8.3, 2dsphereIndexVersion is set to version 4 by default' and FCV downgrade below 8.3 requires dropping v4 indexes; '2dsphere indexes are always sparse'; legacy coordinate pairs are converted to GeoJSON points; longitude -180..180 and latitude -90..90 inclusive, coordinates 'wrap' around the sphere. Version table also on the 2dsphere-index-versions page (https://www.mongodb.com/docs/manual/core/indexes/index-types/geospatial/2dsphere/2dsphere-index-versions/).

- Source: https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/index/geo/s2_common.h
- Confidence: verified; design-critical: no

### 8. MongoDB supports exactly seven GeoJSON types (Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection; the last four need 2dsphere), longitude-first coordinates, closed non-self-intersecting rings with at least four positions, exterior ring first, holes fully inside and non-overlapping, WGS84 as the CRS, and a custom CRS 'urn:x-mongodb:crs:strictwinding:EPSG:4326' for single-ring polygons larger than a hemisphere.

GeoJSON reference page: 'If specifying latitude and longitude coordinates, list the longitude first, and then latitude'; 'A LinearRing is a closed LineString with at least four coordinate pairs. The first and last coordinates must be identical'; single ring 'cannot self-intersect'; multiple rings: first is exterior, interior rings entirely contained, cannot intersect, overlap, or share an edge; 'MongoDB uses the WGS84 reference system for geospatial queries on GeoJSON objects.' $geoWithin/$geoIntersects pages give the strictwinding CRS for big polygons and note that with the default CRS a bigger-than-hemisphere polygon queries the complement.

- Source: https://www.mongodb.com/docs/manual/reference/geojson/
- Confidence: verified; design-critical: yes

### 9. $near returns documents sorted nearest-to-farthest, requires a geospatial index, takes $maxDistance/$minDistance in meters for the GeoJSON form (2dsphere) and $maxDistance in radians for the legacy form (2d, no $minDistance), is not allowed in aggregation pipelines, cannot be combined with another special-index operator, and since 8.0 rejects any GeoJSON type other than Point.

$near page: 'The $near operator sorts documents by distance'; GeoJSON syntax with $maxDistance/$minDistance '<distance in meters>'; legacy syntax '$maxDistance: <distance in radians>'; '$near is a Match Execution operator and is not permitted in aggregation pipelines'; 'Starting in MongoDB 8.0, $near, $nearSphere, and $geoNear validate that the type of the specified GeoJSON points is Point.' Applying sort() afterwards triggers a second sort.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/near/
- Confidence: verified; design-critical: yes

### 10. $nearSphere always computes spherical distance; with a 2dsphere index it is equivalent to $near on GeoJSON (meters), with a 2d index it takes radians and $minDistance is unavailable.

$nearSphere page: 'MongoDB calculates distances for $nearSphere using spherical geometry'; requires 2dsphere for GeoJSON points or 2d for legacy pairs; GeoJSON form: $minDistance/$maxDistance in meters; legacy form in radians; '$minDistance is available only if the query uses the 2dsphere index'; sorts by distance; not permitted in aggregation pipelines. The geospatial overview table lists $near(GeoJSON,2dsphere)=spherical, $near(legacy,2d)=flat, $nearSphere=spherical for both.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/nearSphere/
- Confidence: verified; design-critical: yes

### 11. $geoWithin selects documents whose geometry lies entirely within a shape (GeoJSON Polygon/MultiPolygon via $geometry, or legacy $box/$polygon/$center/$centerSphere), needs no index, and returns unsorted results.

Page: 'Selects documents with geospatial data that exists entirely within a specified shape'; '$geoWithin does not require a geospatial index. However, a geospatial index will improve query performance'; 'The $geoWithin operator does not return sorted results'; big-polygon queries require the strictwinding CRS. Overview table: $geometry and $centerSphere are spherical, $box/$polygon/$center are flat; $centerSphere radius is in radians (distance / earth radius).

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/
- Confidence: verified; design-critical: yes

### 12. $geoIntersects selects documents whose geometry has a non-empty intersection with a GeoJSON object; only 2dsphere can accelerate it; it does not guarantee edge/vertex-touching counts as intersecting.

Page: 'Selects documents whose geospatial data intersects with a specified GeoJSON object; i.e. where the intersection of the data and the specified object is non-empty'; 'Only the 2dsphere geospatial index supports $geoIntersects'; '$geoIntersects does not guarantee that it will consider a polygon to intersect with its own edges; its own vertices; or another polygon sharing vertices or edges but no interior space'; bigger-than-hemisphere geometries with the default CRS query the complement.

- Source: https://www.mongodb.com/docs/manual/reference/operator/query/geoIntersects/
- Confidence: verified; design-critical: yes

### 13. $geoNear is an aggregation stage that must be first in the pipeline, requires a geospatial index (key required when the collection has more than one 2d or 2dsphere index; 2d is preferred over 2dsphere when both exist and key is absent), takes near/distanceField/spherical/maxDistance/minDistance/query/distanceMultiplier/includeLocs/key, has no default 100-document limit, and measures distance to the nearest point of the document geometry's perimeter.

Page: 'You can only use $geoNear as the first stage of a pipeline'; distanceField 'Starting in MongoDB 8.1 ... is optional for queries on non-timeseries collections'; maxDistance/minDistance 'in meters if the specified point is GeoJSON and in radians if ... legacy coordinate pairs' (constant expressions allowed since 7.2); spherical: true uses $nearSphere semantics, false uses $near semantics ('spherical geometry for 2dsphere indexes and planar geometry for 2d indexes'), default false; 'You cannot specify a $near predicate in the query field'; '$geoNear calculates distance based on the nearest point of the input document's perimeter'; '$geoNear no longer has a default limit of 100 documents'.

- Source: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
- Confidence: verified; design-critical: yes

### 14. MongoDB's per-operator geometry model: 2dsphere/GeoJSON operators ($near, $nearSphere, $geoWithin $geometry, $geoWithin $centerSphere, $geoIntersects) are spherical; 2d legacy operators ($near legacy, $geoWithin $box/$polygon/$center) are flat; GeoJSON distances are meters, legacy $centerSphere radii are radians (distance / earth radius); 2d indexes cannot wrap around the poles.

Geospatial Queries overview: operator table with index support (2dsphere: $geoIntersects, $geoWithin, $near, $nearSphere; 2d: $geoWithin, $near, $nearSphere) and the spherical/flat table; 'Using a 2d index for queries on spherical data can return incorrect results or an error. For example, 2d indexes don't support spherical queries that wrap around the poles'; legacy pairs should be arrays because 'some languages do not preserve object field ordering'.

- Source: https://www.mongodb.com/docs/manual/geospatial-queries/
- Confidence: verified; design-critical: yes

### 15. The legacy 2d index is a planar geohash index over legacy coordinate pairs with 'bits' precision default 26 (range 1..32) and min/max bounds default -180.0/180.0; it cannot index GeoJSON and values do not wrap around a sphere.

createIndex options: bits 'the number of precision of the stored geohash value of the location data', range 1-32 inclusive, default 26; min default -180.0; max default 180.0; 2d indexes ignore sparse and do not support collation. 2d index page: supports 'calculations on a flat, Euclidean plane'; 'You cannot use 2d indexes for queries on GeoJSON objects'; 'Unlike 2dsphere index coordinates, 2d indexes values do not wrap around a sphere' (https://www.mongodb.com/docs/manual/core/indexes/index-types/geospatial/2d/).

- Source: https://www.mongodb.com/docs/manual/reference/method/db.collection.createIndex/
- Confidence: verified; design-critical: no

### 16. MongoDB's 2dsphere S2 parameters (v8.0 source): index version 3 uses finestIndexedLevel = S2::kAvgEdge.GetClosestLevel(110 m / R), coarsestIndexedLevel = GetClosestLevel(2000 km / R), maxCellsInCovering = 20; versions <= 2 used 500 m, 100 km and 50 cells. Non-point shapes are indexed only within [coarsest, finest]; points are indexed as a single leaf cell.

expression_params.cpp (v8.0): 'out->radius = kRadiusOfEarthInMeters;' 'long long defaultFinestIndexedLevel = S2::kAvgEdge.GetClosestLevel(110.0 / out->radius);' (500.0 for versions <= 2); 'long long defaultCoarsestIndexedLevel = S2::kAvgEdge.GetClosestLevel(2000 * 1000.0 / out->radius);' (100.0 * 1000 for <= 2); 'long long defaultMaxCellsInCovering = 20;' (50 for <= 2); option field name '2dsphereIndexVersion'. s2_common.h (v8.0): finestIndexedLevel 'What's the finest grained level that we'll index for non-points?'; coarsestIndexedLevel 'When we search in larger coverings we know we can stop here -- we index nothing coarser than this'; maxCellsInCovering 'really an advisory parameter that we pass to the cover generator'; maxKeysPerInsert caps the cartesian product; S2CellIdToIndexKey emits numeric keys for v3 and strings before (https://raw.githubusercontent.com/mongodb/mongo/v8.0/src/mongo/db/index/s2_common.h).

- Source: https://raw.githubusercontent.com/mongodb/mongo/v8.0/src/mongo/db/index/expression_params.cpp
- Confidence: verified; design-critical: yes

### 17. Mapped onto the S2 statistics table, MongoDB's v3 defaults correspond to roughly level 16 (finest, avg edge 106-148 m) and level 2 (coarsest, avg edge 1825-2489 km); the v<=2 defaults to about level 14 (425-593 m) and level 6 (108-151 km).

s2cell_statistics table rows I read: level 2 avg area 5313188.26 km2, edge '1825 km' to '2489 km', 96 cells; level 6 20754.64 km2, '108 km' to '151 km'; level 7 5188.66 km2, '54 km' to '76 km'; level 13 1.27 km2, '850 m' to '1185 m'; level 14 0.32 km2, '425 m' to '593 m'; level 15 79172.67 m2, '212 m' to '296 m'; level 16 19793.17 m2, '106 m' to '148 m', 25B cells; level 20 77.32 m2, '7 m' to '9 m'; level 23 1.21 m2, '83 cm' to '116 cm'; level 30 0.74 cm2, '6 mm' to '9 mm', 7e18 cells. The level numbers are my derivation from GetClosestLevel(110/R etc.), not a value printed in MongoDB code, hence 'likely'.

- Source: https://s2geometry.io/resources/s2cell_statistics
- Confidence: likely; design-critical: yes

### 18. MongoDB generates 2dsphere keys by projecting each geometry onto the sphere and running S2RegionCoverer (configured from the index params) to get one key per covering cell; keys are combined with other compound-index fields by cartesian product, capped by a max-keys-per-document limit; 2d keys are geohash strings from a GeoHashConverter.

expression_keys_private.cpp (v8.0): S2GetKeysForElement 'Project the geometry into spherical space' then 'S2RegionCoverer coverer; params.configureCoverer(geoContainer, &coverer);' and coverer.GetCovering(); 'We take the cartesian product of all keys when appending' with a check 'cells.size() * keysToAdd.size() > maxKeys' against gIndexMaxNumGeneratedKeysPerDocument; keys appended via S2CellIdToIndexKeyStringAppend; 2d path: 'params.geoHashConverter->hash(locObj, &obj).appendHashMin(&keyString)'.

- Source: https://raw.githubusercontent.com/mongodb/mongo/v8.0/src/mongo/db/index/expression_keys_private.cpp
- Confidence: verified; design-critical: yes

### 19. MongoDB turns a query region into index bounds by covering it with S2RegionCoverer (min/max level from internalQueryS2GeoCoarsestLevel/FinestLevel, max cells from internalQueryS2GeoMaxCells) and, for every covering cell, scanning the descendant range [range_min, range_max] plus a point lookup for each ancestor cell up to the coarsest indexed level, because a large indexed shape may have been indexed at a coarser cell than the query cell.

expression_index.cpp (v8.0): get2dsphereCovering sets 'set_min_level(minLevel)', 'set_max_level(maxLevel)', 'set_max_cells(gInternalQueryS2GeoMaxCells.load())'; S2CellIdsToIntervalsWithParents: 'long long start = static_cast<long long>(interval.range_min().id()); long long end = static_cast<long long>(interval.range_max().id())' -> makeRangeInterval with both bounds inclusive, then parent cells added as point intervals 'until we hit the coarsest indexed cell', with the comment that entries inserted with a coarser cell value 'may' match; 2d uses GeoHashsToIntervalsWithParents with appendHashMin/appendHashMax.

- Source: https://raw.githubusercontent.com/mongodb/mongo/v8.0/src/mongo/db/query/expression_index.cpp
- Confidence: verified; design-critical: yes

### 20. MongoDB executes $near/$geoNear on 2dsphere as an expanding annulus: a density estimator seeds the first radius, each round covers the annulus (outer cap minus inner cap) with S2 cells, subtracts cells already scanned, scans the index, exact-filters, and grows the increment x2 when a round returns fewer than 300 results or halves it above 600.

geo_near.cpp (master, src/mongo/db/exec/classic/): 'if (lastIntervalStats.numResultsReturned < 300) _boundsIncrement *= 2; else if (lastIntervalStats.numResultsReturned > 600) _boundsIncrement /= 2;'; buildS2Region builds an inner (inverted) cap and outer cap with the comment 'Currently a workaround to fix occasional floating point errors in S2, where sometimes points near the axis will not be returned if inner == 0'; 'std::vector<S2CellId> cover = ExpressionMapping::get2dsphereCovering(*region);' then 'diffUnion.GetDifference(&coverUnion, &_scannedCells);'; the density estimator samples finest-level cells and stops when documents are found or the top level is reached; the 2d variant uses GeoHash coverings, R2CellUnion dedup, and a TwoDPtInAnnulusExpression matcher.

- Source: https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/exec/classic/geo_near.cpp
- Confidence: verified; design-critical: yes

### 21. S2 cell ids are 64-bit integers: 3 face bits (6 cube faces) plus 61 position bits along a Hilbert curve (2 bits per level for 30 levels plus a trailing 1 that marks the level); ids increase along one continuous space-filling curve over the whole sphere, and every cell's descendants occupy the inclusive contiguous id range [range_min, range_max].

s2cell_id.h: 'id = [face][face_pos]', 'face: a 3-bit number (range 0..5) encoding the cube face', 'face_pos: a 61-bit number encoding the position of the center of this cell along the Hilbert curve over this face'; kFaceBits = 3, kNumFaces = 6, kMaxLevel = 30, kPosBits = 61 (2*kMaxLevel+1), kMaxSize = 1<<30; 'Sequentially increasing cell ids follow a continuous space-filling curve over the entire sphere'; range_min/range_max 'return the range of cell ids that are contained within this cell (including itself). The range is *inclusive* (i.e. test using >= and <=)'; parent(), child(), next(), prev(), ToToken/FromToken with 'FromToken(ToToken(x)) == x even when x is an invalid cell id'. Devguide (https://s2geometry.io/devguide/s2cell_hierarchy) adds: six faces projected from a cube, 31 levels 0-30, leaf cells about 1 cm across, 'if the S2CellIds of two cells are close together, then the cells are also close together'.

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2cell_id.h
- Confidence: verified; design-critical: yes

### 22. S2RegionCoverer approximates any S2Region as a cell union; defaults are max_cells 8, min_level 0, max_level 30, level_mod 1; max_cells is a target not a bound (up to 6 cells may be returned regardless, min_level overrides it, and below 4 cells the covering area can be arbitrarily large); GetCovering is a superset, GetInteriorCovering a subset, GetFastCovering a looser fast approximation.

s2region_coverer.h: kDefaultMaxCells = 8; 'For any setting of max_cells(), up to 6 cells may be returned if that is the minimum number required (e.g. if the region intersects all six cube faces)'; 'min_level() takes priority over max_cells(), i.e. cells below the given level will never be used even if this causes a large number of cells to be returned'; 'If max_cells() is less than 4, the area of the covering may be arbitrarily large compared to the area of the original region even if the region is convex'; GetCovering/GetInteriorCovering/GetFastCovering/CanonicalizeCovering. Devguide: 'the output does not always use the maximum number of cells allowed'; level_mod 2 or 3 'effectively allows the branching factor of the S2CellId hierarchy to be increased'; coverings may be non-normalized when min_level/level_mod apply.

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2region_coverer.h
- Confidence: verified; design-critical: yes

### 23. S2CellUnion is a sorted vector of non-overlapping cell ids, normalized so any 4 siblings collapse into their parent; it supports Contains/Intersects, union/intersection/difference, level- and radius-based Expand, and cap/rect bounds.

s2cell_union.h: 'An S2CellUnion is a region consisting of cells of various sizes. Typically a cell union is used to approximate some other shape'; 'represented as a vector of sorted, non-overlapping S2CellIds'; 'By default the vector is also normalized, meaning that groups of 4 child cells have been replaced by their parent cell whenever possible'; FromVerbatim caveat that 4 children are then not considered to contain the parent; Expand(int expand_level) and Expand(S1Angle min_radius, int max_level_diff).

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2cell_union.h
- Confidence: verified; design-critical: yes

### 24. S2Cap is the region type for radius queries: a spherical cap (disc on the sphere) built from center+angle/height/area, with Expanded(), Contains/Intersects, MayIntersect(S2Cell), Contains(S2Cell) and GetCellUnionBound so it can be fed to S2RegionCoverer.

s2cap.h: 'S2Cap represents a disc-shaped region defined by a center and radius'; 'this shape is called a spherical cap (rather than disc) because it is not planar'; FromCenterHeight uses 'the distance from the center point to the cutoff plane'; FromCenterArea uses 'the solid angle subtended by the cap'; Expanded() returns 'a cap that contains all points within a given distance of this cap'; implements S2Region (GetCellUnionBound, MayIntersect(S2Cell), Contains(S2Cell)).

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2cap.h
- Confidence: verified; design-critical: yes

### 25. S2Polygon models GeoJSON-style polygons with holes as nested loops (shells at even depth, holes at odd depth, interior on each loop's left side), supports Contains(S2Point), Contains(S2Polygon), Intersects(S2Polygon), Contains/MayIntersect(S2Cell), and has explicit full and empty polygons; InitNested vs InitOriented decide whether input loops are nested or oriented.

s2polygon.h: 'the interior of a loop is defined to be its left-hand side'; 'Shells have even depths, and holes have odd depths'; loops are reordered so GetParent/GetLastDescendant/S2Loop::depth traverse the hierarchy; InitNested expects nested loops where the interior is points contained by an odd number of loops; InitOriented expects loops oriented so the interior is on the left (shells and holes have opposite orientations); 'the entire sphere (known as the full polygon)', 'the empty polygon has no loops at all'; GetRectBound/GetCapBound; validity checked at init when --s2debug is on.

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2polygon.h
- Confidence: verified; design-critical: yes

### 26. S2's in-memory kNN (S2ClosestEdgeQuery over S2ShapeIndex) works by keeping a priority queue of index cells ordered by distance to the target, repeatedly popping the closest cell and either splitting it into 4 children or processing its edges, and shrinking a distance limit as results fill; it needs the whole S2ShapeIndex in memory, so it is not directly usable over a RocksDB-resident index.

s2closest_edge_query_base.h: 'Repeatedly find the closest S2Cell to target and either split it into its four children or process all of its edges'; brute force option examines 'every edge rather than using the S2ShapeIndex'; pruning 'distance_limit_ = (--result_set_.end())->distance() - options().max_error()'; supports 'the k edges of geometry A that are closest to a given point P'. s2closest_edge_query.h: options max_results, max_distance, max_error, include_interiors, use_brute_force; 'By default *all* edges are returned, so you should always specify either max_results() or max_distance() or both'. S2ShapeIndex devguide: the index is 'a quad-tree that starts with the six top-level faces of the S2Cell hierarchy and adaptively splits nodes that intersect too many edges', storing only non-empty leaf nodes; MutableS2ShapeIndex batches updates lazily (https://s2geometry.io/devguide/s2shapeindex).

- Source: https://raw.githubusercontent.com/google/s2geometry/master/src/s2/s2closest_edge_query_base.h
- Confidence: verified; design-critical: yes

### 27. golang/geo (github.com/golang/geo/s2) is the only S2 port with the full set NilDB needs: Cap, Cell, CellID, CellUnion, Loop, Polyline, LatLngRect, RegionCoverer, ShapeIndex, ClosestEdge(Query), ContainsPoint(Query), CrossingEdge(Query), Earth, Predicates are marked complete; Polygon, CellIndex, LaxLoop, LaxPolygon are 'mostly complete'; PointIndex, Builder, BooleanOperation, RegionIntersection are absent. It has no semver tags (pseudo-version v0.0.0-...-b200a11, published Aug 18 2026) and requires go 1.23.0.

README status table (raw README.md): S2Cap, S2Cell, S2CellId, S2CellUnion, S2Loop, S2Polyline, S2LatLng, S2LatLngRect, S2RegionCoverer, S2ShapeIndex, S2ClosestEdge, S2ContainsPoint, S2CrossingEdge, S2Earth, S2Predicates, S2EdgeCrosser = complete; S2CellIndex, S2Polygon, S2LaxLoop, S2LaxPolygon = mostly complete; S2PointIndex, S2RegionIntersection, S2Builder, S2BooleanOperation and encoding types = not available; 'This is not an official Google product.' GitHub tags page: 'There aren't any releases here' (https://github.com/golang/geo/tags). go.mod: module github.com/golang/geo, 'go 1.23.0', only go-cmp and go-units as indirect deps (https://raw.githubusercontent.com/golang/geo/master/go.mod). pkg.go.dev shows 'Published: Aug 18, 2026', version v0.0.0-...-b200a11, and lists CellID.RangeMin/RangeMax/Parent/Children/ToToken, CellUnion.Normalize/Denormalize/ContainsCellID/IntersectsCellID, CapFromCenterAngle, RegionCoverer{MinLevel,MaxLevel,LevelMod,MaxCells}.Covering/InteriorCovering/FastCovering, EdgeQuery via NewClosestEdgeQuery/FindEdges/Distance/IsDistanceLess (https://pkg.go.dev/github.com/golang/geo/s2).

- Source: https://raw.githubusercontent.com/golang/geo/master/README.md
- Confidence: verified; design-critical: yes

### 28. golang/geo's Polygon type exposes what $geoWithin/$geoIntersects need on the sphere: PolygonFromLoops / PolygonFromOrientedLoops / PolygonFromCell / FullPolygon, ContainsPoint, Contains(*Polygon), Intersects(*Polygon), ContainsCell, IntersectsCell, CellUnionBound, CapBound, RectBound, Distance(Point), Area, Centroid, Validate, Encode/Decode, and the Shape interface methods for ShapeIndex.

pkg.go.dev Polygon section lists exactly these constructors and methods (also Loops, NumLoops, Loop(k), Parent(k), LastDescendant(k), IsEmpty, IsFull, Invert, BoundaryNear, Dimension, Edge, NumEdges, NumChains, Chain, ChainEdge, ChainPosition, ReferencePoint); no TODO or 'not yet implemented' notes appear in the published docs, so what the README's 'mostly complete' marker omits is not stated there.

- Source: https://pkg.go.dev/github.com/golang/geo/s2#Polygon
- Confidence: verified; design-critical: yes

### 29. Go's RegionCoverer carries the C++ semantics: MinLevel overrides MaxCells, up to 6 cells (3 for tiny convex regions) can be returned regardless of MaxCells, MaxCells < 4 can blow up covering area; Covering() returns a normalized CellUnion, InteriorCovering() a contained one, FastCovering() a looser starting point, CellUnion() ignores MinLevel/LevelMod.

regioncoverer.go doc comments: MinLevel 'the minimum cell level to be used', MaxLevel, LevelMod, MaxCells 'the maximum desired number of cells in the approximation'; 'cells below the given level will never be used even if this causes a large number of cells to be returned'; 'For any setting of MaxCells, up to 6 cells may be returned if that is the minimum number of cells required'; 'Up to 3 cells may be returned even for very tiny convex regions'; 'If MaxCells is less than 4, the area of the covering may be arbitrarily large compared to the area of the original region'.

- Source: https://raw.githubusercontent.com/golang/geo/master/s2/regioncoverer.go
- Confidence: verified; design-critical: yes

### 30. The Rust 's2' crate (yjh0502/rust-s2) is alive but incomplete: 0.2.0 released Aug 19 2026 (0.1.0 Jul 7 2026, 0.0.13 Oct 2 2024), Apache-2.0, ~97 stars; it ships CellID, Cell, CellUnion, Cap, Rect, LatLng, Point, Region, Metric, predicates and edge utilities, but has no RegionCoverer, Loop, Polygon, Polyline, ShapeIndex or edge/closest-edge query, and its README says 'lines and polygons aren't implemented yet'.

crates.io API: name s2, 'S2 geometric library', repository https://github.com/yjh0502/rust-s2, max version 0.2.0, updated 2026-08-19, 656,580 total / 135,652 recent downloads, versions 0.2.0 (2026-08-19), 0.1.0 (2026-07-07), 0.0.13 (2024-10-02), 0.0.12 (2023-01-15), 0.0.11 (2022-12-26) (https://crates.io/api/v1/crates/s2). GitHub README: 'This is an ongoing port, further along in some areas than others — in particular, lines and polygons aren't implemented yet', Apache-2.0, 97 stars, 258 commits, points to the issue tracker for status (https://github.com/yjh0502/rust-s2). docs.rs source tree src/s2/: cap.rs, cell.rs, cellid.rs, cellunion.rs, edge_clipping.rs, edgeutil.rs, latlng.rs, metric.rs, mod.rs, point.rs, predicates.rs, random.rs, rect.rs, rect_bounder.rs, region.rs, shape.rs, stuv.rs, edge_crossings/; no region_coverer / loop / polygon / polyline / shapeindex / edge_query file. docs.rs module index for 0.2.0 shows 61.49% documented and no RegionCoverer/Loop/Polygon/Polyline/ShapeIndex/EdgeQuery types (https://docs.rs/s2/latest/s2/).

- Source: https://docs.rs/crate/s2/latest/source/src/s2/
- Confidence: verified; design-critical: yes

### 31. h3o is a mature pure-Rust H3 (hexagonal) implementation: 0.11.0 released Aug 29 2026, BSD-3-Clause, 1.8M downloads, '100% Rust (no C deps)', with optional geo/serde/std features; it is a different cell system from S2 and does not give MongoDB-compatible cell semantics.

crates.io API: h3o 'A Rust implementation of the H3 geospatial indexing system', repo HydroniumLabs/h3o, max version 0.11.0, updated 2026-08-29, 1,817,962 downloads, versions 0.11.0 (2026-08-29), 0.10.0 (2026-05-24), 0.9.5 (2026-05-02), 0.9.4 (2025-12-05), 0.9.3 (2025-09-25), features geo/serde/std/tools/typed_floats, BSD-3-Clause (https://crates.io/api/v1/crates/h3o). README goals: 'To be safer/harder to misuse by leveraging the strong typing of Rust', 'To be 100% Rust (no C deps): painless compilation to WASM, easier LTO', as fast or faster than the reference library (https://raw.githubusercontent.com/HydroniumLabs/h3o/master/README.md).

- Source: https://crates.io/api/v1/crates/h3o
- Confidence: verified; design-critical: no

### 32. H3 cell ids are not usable as a single descendant key range: the 64-bit layout puts the 4-bit resolution field above the base cell (7 bits) and the 15 3-bit digits (unused digits set to 7), so children at resolution r+1 do not sort inside their parent's id neighborhood; a KV range scan per resolution would be needed.

H3 indexing docs: layout of reserved bit, mode (4 bits; 0 invalid, 1 cell, 2 directed edge, 4 vertex), mode-dependent field, resolution (4 bits), base cell (7 bits), 15 digit fields of 3 bits each. The page does not state anything about prefix or range relationships between parents and children; the non-contiguity is my inference from the resolution field being more significant than the digits.

- Source: https://h3geo.org/docs/core-library/h3Indexing/
- Confidence: likely; design-critical: yes

### 33. The georust 'geo' crate (0.33.1, Apr 20 2026, MSRV 1.88, MIT/Apache-2.0, 23M downloads) provides planar geometry types and predicates (Contains, Intersects, Within, Relate/DE-9IM, BooleanOps, Area, Centroid, Simplify, ConvexHull) plus Haversine, Geodesic and Rhumb metric spaces for lon/lat distances; its topological predicates are planar, not spherical.

crates.io API: geo 'Geospatial primitives and algorithms', repo georust/geo, max version 0.33.1, updated 2026-04-20, 22,955,329 downloads, rust_version 1.88, versions 0.33.1 (2026-04-20), 0.33.0 (2026-04-15), 0.32.0 (2025-12-05), 0.31.0 (2025-09-01), 0.30.0 (2025-03-24) (https://crates.io/api/v1/crates/geo). docs.rs 0.33.1: 'The geo crate provides planar geospatial geometries and algorithms'; geometry types Point, MultiPoint, Line, LineString, MultiLineString, Polygon, MultiPolygon, Rect, Triangle, GeometryCollection, Coord; algorithms Area (planar and geodesic), Centroid, Contains, Intersects, Within, Relate (DE-9IM), boolean ops, simplification, convex hull, densify, closest point; metric spaces: 'Euclidean plane measures distance with the pythagorean formula. Not suitable for lon/lat geometries', while Haversine, Geodesic and Rhumb are 'Only suitable for lon/lat geometries' (https://docs.rs/geo/latest/geo/).

- Source: https://docs.rs/geo/latest/geo/
- Confidence: verified; design-critical: yes

### 34. rstar (0.13.0, May 24 2026, MSRV 1.85, 45M downloads) is an in-memory n-dimensional R*-tree with nearest_neighbor_iter, nearest_neighbors, locate_within_distance, locate_in_envelope(_intersecting), locate_at_point, bulk_load, insert and remove; it is a memory index, not a KV-encodable one.

crates.io API: rstar 'An R*-tree spatial index', repo georust/rstar, max version 0.13.0, updated 2026-05-24, 45,298,001 downloads, rust_version 1.85, versions 0.13.0 (2026-05-24), 0.12.2 (2024-11-05), 0.12.1 yanked, 0.12.0 (2024-01-28), 0.11.0 (2023-05-31) (https://crates.io/api/v1/crates/rstar). docs.rs RTree: nearest_neighbor 'Returns the nearest neighbor for a given point', nearest_neighbor_iter 'Returns all elements of the tree sorted by their distance to a given point', nearest_neighbors, locate_within_distance 'Returns all elements of the tree within a certain distance', locate_in_envelope, locate_in_envelope_intersecting, locate_at_point, locate_all_at_point, bulk_load, insert, remove, pop_nearest_neighbor. README: 'A flexible, n-dimensional r*-tree implementation for the Rust ecosystem, suitable for use as a spatial index', 'optimized for nearest neighbor search', integrates with geo (https://raw.githubusercontent.com/georust/rstar/master/README.md).

- Source: https://docs.rs/rstar/latest/rstar/struct.RTree.html
- Confidence: verified; design-critical: no

### 35. The georust 'geohash' crate (0.13.2, May 24 2026, MIT/Apache-2.0) implements string geohashes only: encode(coord, len), decode -> (coord, lon_err, lat_err), neighbor(hash, Direction), neighbors(hash); it does not produce Redis's 52-bit integer form.

crates.io API: geohash 'Geohash implementation for Rust.', repo georust/geohash.rs, max version 0.13.2, updated 2026-05-24, 3,664,958 downloads, versions 0.13.2 (2026-05-24), 0.13.1 (2024-03-13), 0.13.0 (2023-01-15), 0.12.0 (2021-10-08), 0.11.0 (2021-01-17) (https://crates.io/api/v1/crates/geohash). docs.rs: 'Geohash algorithm implementation in Rust. It encodes/decodes a longitude-latitude tuple into/from a hashed string', functions encode/decode/neighbor/neighbors, Direction enum, Coord and Rect types, example encode(c, 9usize) -> 'ww8p1r4t8'.

- Source: https://docs.rs/geohash/latest/geohash/
- Confidence: verified; design-critical: no

### 36. CockroachDB indexes spatial columns as an inverted (GIN) index over S2 cell ids: it takes the 'divide the space' approach (data-independent S2 quadtree numbered by a Hilbert curve) instead of an R-tree, stores cell-id -> primary key for each cell in an object's covering, answers queries with containment/intersection between coverings ('false positives but no false negatives') then re-checks exactly; defaults are s2_max_level 30, s2_level_mod 1, s2_max_cells 4; GEOGRAPHY leaf cells are 1 cm, GEOMETRY precision is the index bounding length / 4^30.

Docs: 'CockroachDB takes the divide the space approach to spatial indexing. This is necessary to preserve CockroachDB's ability to scale horizontally'; 'uses the S2 geometry library to divide the space being indexed into a quadtree data structure with a set number of levels and a data-independent shape'; 'each node in the quadtree is numbered using a Hilbert space-filling curve which preserves locality of reference'; 'CockroachDB stores spatial indexes as a special type of GIN index. The spatial index maps from a location, which is a square cell in the quadtree, to one or more shapes whose coverings include that location'; 'This retrieves false positives but no false negatives'; parameter table: s2_level_mod default 1 (1-3), s2_max_level default 30 (1-30), s2_max_cells default 4 (1-30), geometry_min_x/max_x/min_y/max_y from SRID or +/-2^31; 'The leaf nodes of the S2 quadtree are at level 30, and for GEOGRAPHY measure 1cm across the Earth's surface'; GEOMETRY 'precision you get there is the bounding length of the GEOMETRY index divided by 4^30'; benefits listed: scales horizontally, no balancing, inserts need no locking, simpler bulk ingest; 'Most users should not need to change the default settings.'

- Source: https://docs.cockroachlabs.com/docs/stable/spatial-indexes
- Confidence: verified; design-critical: yes

### 37. CockroachDB's pkg/geo/geoindex turns each relationship into a key expression over the inverted index: Covers/Intersects/DWithin produce a union of sorted non-overlapping key spans (descendant ranges of covering cells plus ancestor lookups), CoveredBy produces a reverse-Polish expression of unions and intersections over ancestor paths; ancestors are used so a shape indexed at a coarser cell is still found; the default S2 config is MinLevel 0, MaxLevel 30, LevelMod 1, MaxCells 4.

geoindex.go interface comments: 'InvertedIndexKeys returns the keys to store this object under when adding it to the index'; 'Covers returns the index spans to read and union for the relationship ST_Covers(g, x)'; 'CoveredBy returns the index entries to read and the expression to compute for ST_CoveredBy(g, x)'; 'Intersects returns the index spans to read and union for the relationship ST_Intersects(g, x)'; 'DWithin returns the index spans to read and union for the relationship ST_DWithin(g, x, distanceMeters)'; GeometryIndex adds DFullyWithin; UnionKeySpans for Covers/Intersects, RPKeyExpr (RPN over union/intersection of ancestor paths) for CoveredBy; DefaultS2Config MinLevel 0, MaxLevel 30, LevelMod 1, MaxCells 4. Blog (vendor design post): index stores 'the cell-ID and the primary key of the table', a cell id can appear for many rows; queries compute the covering of the query shape and use set operations over ancestors/descendants, then 'a lookup join that retrieves the original shape and does a precise evaluation'; GEOMETRY shapes outside the bounds are 'clipped and indexed with both the cell-IDs corresponding to the part that falls in the finite space as well as a special overflow cell-ID'; 'For certain workloads we've observed 3x reduction in false positives' from also storing bounding boxes (https://www.cockroachlabs.com/blog/how-we-built-spatial-indexing/).

- Source: https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/geo/geoindex/geoindex.go
- Confidence: verified; design-critical: yes

### 38. Tile38 (Go, MIT) is the reference Redis-protocol geo server but is memory-resident: each collection keeps objects in a btree.Map keyed by id and in an in-memory rtree.RTreeGN[float32] keyed by bounding rect; NEARBY is a kNN over the R-tree with a geodetic distance callback, WITHIN/INTERSECTS scan the R-tree by bounds then check geojson Within/Intersects exactly; durability is an append-only file (data/appendonly.aof, on by default) rather than a KV store.

collection.go imports github.com/tidwall/btree, rtree, geojson; struct fields 'objs btree.Map[string, *object.Object] // sorted by id', 'spatial rtree.RTreeGN[float32, *object.Object] // geospatially indexed', 'values *btree.BTreeG[*object.Object] // sorted by value+id', 'expires *btree.BTreeG[*object.Object] // sorted by ex+id'; indexInsert uses 'min, max = rtreeRect(item.Rect())' with rtreeValueDown/Up rounding; Within/Intersects call geoSearch then 'o.Geo().Within(obj)' / 'o.Geo().Intersects(gobj)'; Nearby calls 'c.spatial.Nearby(...)' with geodeticDistAlgo; 'const yieldStep = 256'. README: 'in-memory geolocation data store, spatial index, and realtime geofencing server', 'Spatial index with search methods such as Nearby, Within, and Intersects', object types 'lat/lon points, bounding boxes, XYZ tiles, Geohashes, and GeoJSON', 'Redis RESP' plus HTTP/WebSockets/Telnet, 'In-memory database that persists on disk', '100% Go' (https://raw.githubusercontent.com/tidwall/tile38/master/README.md). Configuration page: '--appendonly yes/no : AOF persistence (default: yes)', '--appendfilename path : AOF path (default: data/appendonly.aof)', '-d path : data directory (default: data)' (https://tile38.com/topics/configuration). tidwall/rtree README: 'An in-memory R-Tree implementation for Go. It's designed for Tile38 and is optimized for fast rect inserts and replacements' with a split that 'attempts to minimize intensive operations such as pre-sorting the children and comparing overlaps & area sizes' (https://github.com/tidwall/rtree).

- Source: https://raw.githubusercontent.com/tidwall/tile38/master/internal/collection/collection.go
- Confidence: verified; design-critical: no

### 39. Apache Kvrocks already runs the full Redis GEO command set on RocksDB by reusing Redis's geohash code on top of its ZSet encoding: GEOADD stores the 52-bit geohash as the ZSet score, searches walk the 9 neighbor boxes with RangeByScore per box and post-filter by exact distance; the ZSet is stored as two subkey families (key|version|member -> score in the default CF and key|version|score|member -> empty in the 'zset_score' CF) with an order-preserving double encoding, plus a metadata CF entry holding flags/expire/version/size.

redis_geo.cc: 'GeohashEncodeWGS84(geo_point.longitude, geo_point.latitude, GEO_STEP_MAX, &hash); GeoHashFix52Bits bits = GeoHashHelper::Align52Bits(hash)'; 'GeoHashRadius georadius = GeoHashHelper::GetAreasByShapeWGS84(geo_shape); membersOfAllNeighbors(...)'; 'scoresOfGeoHashBox(hash, &min, &max); return getPointsInRange(ctx, user_key, static_cast<double>(min), static_cast<double>(max), geo_shape, geo_points)'; appendIfWithinShape and GetDistanceIfInRadiusWGS84 do the exact filter. Supported commands page: GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS(_RO), GEORADIUSBYMEMBER(_RO) since v1.1.12, GEOSEARCH and GEOSEARCHSTORE since v2.6.0 (https://kvrocks.apache.org/docs/supported-commands). Design doc: ZSet subkeys 'key|version|member => score' and 'key|version|score|member => NULL'; metadata 'flags | expire | version | size'; 'Version is used to accomplish fast delete' (https://kvrocks.apache.org/community/data-structure-on-rocksdb). storage.h: kMetadataColumnFamilyName = "metadata", kSecondarySubkeyColumnFamilyName = "zset_score", plus pubsub, propagate, stream, search, index CFs; ColumnFamilyID::SecondarySubkey 'Stores score values for zset and secondary keys' (https://raw.githubusercontent.com/apache/kvrocks/unstable/src/storage/storage.h). redis_zset.cc: 's = batch->Put(score_cf_handle_, score_key, Slice());', 'PutDouble(&score_bytes, it->score); score_bytes.append(it->member)', RangeByScore uses iter->Seek/SeekForPrev with iterate_lower_bound/iterate_upper_bound and 'iter->key().starts_with(prefix_key)' (https://raw.githubusercontent.com/apache/kvrocks/unstable/src/types/redis_zset.cc).

- Source: https://raw.githubusercontent.com/apache/kvrocks/unstable/src/types/redis_geo.cc
- Confidence: verified; design-critical: yes
