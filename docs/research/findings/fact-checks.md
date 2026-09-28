# Fact checks

Exported on 2026-09-27 from the verify steps of Claude Code run `wf_1b4f4c38-bbf`. Each check re-read the cited source for a design-critical claim.

## verify:language-and-libraries (agent `a4ceb3db242573f3a`)

### 1. confirmed: grocksdb v1.11.1 is built and tested upstream against RocksDB 11.1.2, statically linked.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/v1.11.1/build.sh

### 2. confirmed: grocksdb v1.11.1 CI runs on Ubuntu only; it is not tested against RocksDB 11.8.1 or on macOS.

- Source: https://raw.githubusercontent.com/linxGnu/grocksdb/v1.11.1/.github/workflows/go.yml

### 3. corrected: RocksDB's C-API changes from 11.2.0 to 11.8.1 are additive only.

Correction: RocksDB has no v11.2.0 git tag; tags go v11.1.2 (2026-06-25), v11.8.0, v11.8.1 (2026-08-07), and HISTORY.md's 11.2.0 to 11.7.0 sections were never tagged. The relevant range is 11.1.2 (what grocksdb pins) to 11.8.1. Diffing include/rocksdb/c.h across that range: 743 new rocksdb_* identifiers, zero type or arity changes across the 1048 functions present in both (only parameter renames), and one removed declaration, rocksdb_writebatch_wi_create_from, which had no implementation in db/c.cc at 11.1.2 and is never called by grocksdb. HISTORY.md for the 11.x C-API expansion states that signatures of pre-existing functions are unchanged. So: additive apart from one dead declaration.

- Source: https://raw.githubusercontent.com/facebook/rocksdb/v11.8.1/include/rocksdb/c.h

### 4. confirmed: Linking grocksdb v1.11.1 against Homebrew's RocksDB 11.8.1 should work.

- Source: https://formulae.brew.sh/api/formula/rocksdb.json

### 5. corrected: Go has no DataFusion equivalent.

Correction: There is no native-Go port of DataFusion of any weight (cedricziel/datafusion-golang is a 0-star reimplementation attempt), and Apache DataFusion's own README lists only Python, Java, Comet and Ballista. But datafusion-contrib/datafusion-go exists: a community-maintained cgo binding in the datafusion-contrib org that exposes DataFusion as a database/sql driver plus Arrow APIs, requires Go 1.26+ and cgo, ships prebuilt native libraries for darwin-arm64/amd64, linux-amd64/arm64 and windows-amd64, and published v0.550100.0 on 2026-09-26 (repo created 2026-05-29). So Go has a DataFusion binding, not a DataFusion reimplementation.

- Source: https://raw.githubusercontent.com/datafusion-contrib/datafusion-go/main/README.md

### 6. confirmed: Apache Arrow for Go's latest release is arrow-go v18.8.0, dated 2026-09-04.

- Source: https://proxy.golang.org/github.com/apache/arrow-go/v18/@latest

### 7. confirmed: arrow-go is a columnar memory/compute library, not a SQL query engine.

- Source: https://raw.githubusercontent.com/apache/arrow-go/main/README.md

### 8. corrected: bindgen's libclang is found on macOS from the Xcode Command Line Tools without Homebrew llvm because clang-sys searches the toolchain directory under xcode-select --print-path.

Correction: The conclusion holds; the mechanism is different for a Command Line Tools-only machine. clang-sys 1.9.1 build/common.rs does two things on macOS: it searches a hard-coded DIRECTORIES_MACOS list that includes /Library/Developer/CommandLineTools/usr/lib and /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib, and it separately joins `xcode-select --print-path` with Toolchains/XcodeDefault.xctoolchain/usr/lib. With only the CLT installed, xcode-select prints /Library/Developer/CommandLineTools and that joined toolchain path does not exist, so the CLT's libclang.dylib (which ships at /Library/Developer/CommandLineTools/usr/lib/libclang.dylib, confirmed on this Mac) is found through the hard-coded CommandLineTools entry, not the xcode-select branch. bindgen 0.73.2 depends on clang-sys; librocksdb-sys 0.19.0+11.8.1 builds with bindgen 0.72.

- Source: https://raw.githubusercontent.com/KyleMayes/clang-sys/master/build/common.rs

### 9. confirmed: rust-rocksdb's CI builds on macos-latest with no extra LLVM step; only Windows runs 'choco install llvm'.

- Source: https://raw.githubusercontent.com/rust-rocksdb/rust-rocksdb/master/.github/workflows/rust.yml

## verify:geo-indexing (agent `ab5c81908316bab5b`)

### 1. corrected: Mapped onto the S2 statistics table, MongoDB's v3 defaults correspond to roughly level 16 (finest, avg edge 106-148 m) and level 2 (coarsest, avg edge 1825-2489 km); the v<=2 defaults to about level 14 (425-593 m) and level 6 (108-151 km).

Correction: Partly right, three fixes. (1) Source of truth is MongoDB's initialize2dsphereParams (r8.0.0 src/mongo/db/index/expression_params.cpp; on master it now lives in src/mongo/db/index/geo/s2_common.cpp). For 2dsphereIndexVersion > 2 it sets finestIndexedLevel = S2::kAvgEdge.GetClosestLevel(110.0 / radius), coarsestIndexedLevel = GetClosestLevel(2000 * 1000.0 / radius), maxCellsInCovering 20; for version <= 2 it uses 500 m, 100 km and 50 cells. With MongoDB's bundled S2 (kAvgEdge deriv 1.4592137 rad at level 0 under the default quadratic projection; GetClosestLevel(v) = GetMinLevel(sqrt(2)*v), i.e. nearest level in log2 space; kRadiusOfEarthInMeters = 6378.1 km) these resolve to level 16 and level 2 for v3, and to level 14 and level 7, not 6, for v1/v2: 100 km falls below the geometric midpoint (102.8 km) between level 6 (kAvgEdge 145.4 km) and level 7 (72.7 km), so GetClosestLevel returns 7. (2) The metre ranges quoted are not average edges. The s2geometry.io statistics table has no average-edge column; 106-148 m, 1825-2489 km, 425-593 m and 108-151 km are its 'Random cell 1 (UK) min edge length / max edge length' columns for levels 16, 2, 14 and 6. Level 7's row reads 54-76 km (UK) and 72-78 km (US). (3) Two omissions that matter for an indexing design: since v3 these levels apply only to non-point geometries; configureCoverer indexes points at a single level, kPointIndexedLevel = S2::kMaxCellLevel = 30 (6-9 mm edges), not at the 110 m 'finest' default. And as of MongoDB 8.3 the default is 2dsphereIndexVersion 4, which keeps the identical 110 m / 2000 km defaults; the S2IndexVersion enum comment says v4 only changed parsing order (GeoJSON tried before legacy points).

- Source: https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/index/expression_params.cpp

### 2. confirmed: H3 cell ids are not usable as a single descendant key range: the 64-bit layout puts the 4-bit resolution field above the base cell (7 bits) and the 15 3-bit digits (unused digits set to 7), so children at resolution r+1 do not sort inside their parent's id neighborhood; a KV range scan per resolution would be needed.

- Source: https://h3geo.org/docs/library/index/cell
