// rocksmoke checks that the RocksDB library NilDB links against exposes the
// features NilDB depends on: column families, atomic multi-CF write batches,
// merge operators, compaction filters, snapshots, bounded iterators,
// checkpoints and properties. Run it after installing RocksDB.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/linxGnu/grocksdb"
)

type counterMerge struct{}

func (counterMerge) FullMerge(key, existing []byte, operands [][]byte) ([]byte, bool) {
	sum := int64(0)
	if len(existing) > 0 {
		n, err := strconv.ParseInt(string(existing), 10, 64)
		if err != nil {
			return nil, false
		}
		sum = n
	}
	for _, op := range operands {
		n, err := strconv.ParseInt(string(op), 10, 64)
		if err != nil {
			return nil, false
		}
		sum += n
	}
	return []byte(strconv.FormatInt(sum, 10)), true
}

func (counterMerge) PartialMerge(key, left, right []byte) ([]byte, bool) {
	a, err1 := strconv.ParseInt(string(left), 10, 64)
	b, err2 := strconv.ParseInt(string(right), 10, 64)
	if err1 != nil || err2 != nil {
		return nil, false
	}
	return []byte(strconv.FormatInt(a+b, 10)), true
}

func (counterMerge) Name() string { return "nildb.counter" }

type gcFilter struct{}

func (gcFilter) Filter(level int, key, val []byte) (bool, []byte) {
	return bytes.HasPrefix(key, []byte("gc:")), nil
}

func (gcFilter) Name() string { return "nildb.gc" }

func (gcFilter) SetIgnoreSnapshots(bool) {}

func (gcFilter) Destroy() {}

func must(step string, err error) {
	if err != nil {
		fmt.Printf("FAIL %s: %v\n", step, err)
		os.Exit(1)
	}
}

func check(step string, ok bool, detail string) {
	if !ok {
		fmt.Printf("FAIL %s: %s\n", step, detail)
		os.Exit(1)
	}
	fmt.Printf("ok   %s\n", step)
}

func main() {
	dir, err := os.MkdirTemp("", "rocksmoke")
	must("mkdtemp", err)
	defer os.RemoveAll(dir)

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	metaOpts := grocksdb.NewDefaultOptions()
	metaOpts.SetMergeOperator(counterMerge{})
	metaOpts.SetCompactionFilter(gcFilter{})
	metaOpts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(3))

	subOpts := grocksdb.NewDefaultOptions()
	subOpts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(3))

	db, cfs, err := grocksdb.OpenDbColumnFamilies(opts, dir,
		[]string{"default", "meta", "sub"},
		[]*grocksdb.Options{grocksdb.NewDefaultOptions(), metaOpts, subOpts})
	must("open with column families", err)
	defer db.Close()
	meta, sub := cfs[1], cfs[2]

	wo := grocksdb.NewDefaultWriteOptions()
	ro := grocksdb.NewDefaultReadOptions()

	wb := grocksdb.NewWriteBatch()
	wb.PutCF(meta, []byte("k1"), []byte("v1"))
	wb.PutCF(sub, []byte("k1:a"), []byte("1"))
	wb.PutCF(sub, []byte("k1:b"), []byte("2"))
	wb.PutCF(sub, []byte("k2:a"), []byte("3"))
	must("multi-cf write batch", db.Write(wo, wb))
	wb.Destroy()

	v, err := db.GetCF(ro, meta, []byte("k1"))
	must("get", err)
	check("atomic multi-cf batch + get", v.Exists() && string(v.Data()) == "v1", string(v.Data()))
	v.Free()

	must("merge 1", db.MergeCF(wo, meta, []byte("cnt"), []byte("5")))
	must("merge 2", db.MergeCF(wo, meta, []byte("cnt"), []byte("7")))
	v, err = db.GetCF(ro, meta, []byte("cnt"))
	must("get merged", err)
	check("merge operator (5+7)", string(v.Data()) == "12", string(v.Data()))
	v.Free()

	snap := db.NewSnapshot()
	must("put after snapshot", db.PutCF(wo, meta, []byte("k9"), []byte("late")))
	sro := grocksdb.NewDefaultReadOptions()
	sro.SetSnapshot(snap)
	v, err = db.GetCF(sro, meta, []byte("k9"))
	must("snapshot get", err)
	check("snapshot isolation hides later write", !v.Exists(), "k9 visible under snapshot")
	v.Free()
	db.ReleaseSnapshot(snap)

	iro := grocksdb.NewDefaultReadOptions()
	iro.SetIterateUpperBound([]byte("k1;"))
	iro.SetFillCache(false)
	it := db.NewIteratorCF(iro, sub)
	var seen []string
	for it.Seek([]byte("k1:")); it.Valid(); it.Next() {
		seen = append(seen, string(it.Key().Data()))
	}
	must("iterator", it.Err())
	it.Close()
	check("bounded prefix iterator", len(seen) == 2 && seen[0] == "k1:a" && seen[1] == "k1:b", fmt.Sprint(seen))

	must("put gc key", db.PutCF(wo, meta, []byte("gc:1"), []byte("x")))
	must("flush", db.FlushCF(meta, grocksdb.NewDefaultFlushOptions()))
	db.CompactRangeCF(meta, grocksdb.Range{})
	v, err = db.GetCF(ro, meta, []byte("gc:1"))
	must("get after compaction", err)
	check("compaction filter dropped gc: key", !v.Exists(), "gc:1 survived compaction")
	v.Free()

	cp, err := db.NewCheckpoint()
	must("new checkpoint", err)
	cpDir := filepath.Join(dir, "cp")
	must("create checkpoint", cp.CreateCheckpoint(cpDir, 0))
	cp.Destroy()
	entries, err := os.ReadDir(cpDir)
	must("read checkpoint dir", err)
	check("checkpoint directory populated", len(entries) > 0, "empty")

	n, ok := db.GetIntProperty("rocksdb.estimate-num-keys")
	check("int property estimate-num-keys", ok, "property missing")
	stats := db.GetProperty("rocksdb.stats")
	check("string property rocksdb.stats", len(stats) > 100, "stats too short")
	fmt.Printf("estimate-num-keys=%d\n", n)
	fmt.Println("rocksmoke: all checks passed")
}
