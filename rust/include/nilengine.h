/*
 * nilengine.h: C interface to NilDB's storage engines, an LSM tree, a
 * copy-on-write B+ tree and a PostgreSQL-style heap (pgheap) written in Rust
 * (rust/capi builds libnilengine.a).
 * docs/design/rust-engines.md describes the engines and their formats.
 *
 * Conventions
 *
 * - nil_db, nil_batch, nil_snapshot and nil_iter are opaque handles. The
 *   library creates and frees them; a NULL handle makes a call fail (or do
 *   nothing where the call has no error output).
 * - A function that can fail takes `char **errptr`. On failure it stores a
 *   malloc'd, NUL-terminated message there, freeing any message already
 *   present; on success it leaves *errptr unchanged. Initialize the pointer
 *   to NULL and free a message with nil_free. errptr may be NULL.
 *   Messages start with a class: "io", "corruption", "invalid-argument",
 *   "not-found", "busy", "closed", "unsupported" or "panic", then ": ".
 * - Every buffer or string the library returns is malloc'd; free it with
 *   nil_free.
 * - Keys and values are byte strings passed as (pointer, length). A NULL
 *   pointer is allowed only with length 0.
 * - Column families are numbered by their position in the name array given
 *   to nil_open.
 * - No exported function lets a Rust panic unwind into C; a panic becomes a
 *   "panic: ..." error.
 * - A nil_db may be used from several threads at once. A nil_batch or a
 *   nil_iter belongs to one thread at a time.
 */
#ifndef NILENGINE_H
#define NILENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct nil_db nil_db;
typedef struct nil_batch nil_batch;
typedef struct nil_snapshot nil_snapshot;
typedef struct nil_iter nil_iter;

/* Engine kinds for nil_open. */
#define NIL_ENGINE_LSM 1
#define NIL_ENGINE_BTREE 2
#define NIL_ENGINE_PGHEAP 3

/* Library version, a static NUL-terminated string. */
const char *nil_version(void);

/* Frees a buffer, string or error message returned by this library. */
void nil_free(void *p);

/*
 * Opens (creating when allowed) the database in directory `dir` with the
 * `num_cfs` column families named in `cf_names`; missing families are
 * created. `options` is NULL or a list of name=value pairs separated by ';',
 * ',' or spaces, for example "write_buffer_size=64m;cache_bytes=256m".
 * Sizes take k, m or g suffixes (powers of 1024). Recognized names:
 * create_if_missing, cache_bytes, write_buffer_size, max_immutable_memtables,
 * block_size, block_restart_interval, bloom_bits_per_key, target_file_size,
 * level0_compaction_trigger, level0_stop_writes_trigger,
 * max_bytes_for_level_base, level_size_multiplier, num_levels (LSM);
 * page_size, max_unsynced_free_pages (B+ tree); fillfactor,
 * checkpoint_wal_bytes, autovacuum, autovacuum_threshold,
 * autovacuum_scale_percent, autovacuum_naptime_ms (pgheap). cache_bytes
 * sizes the block cache, the page cache or pgheap's buffer pool. Returns
 * NULL on failure; another open of the same directory fails with "busy".
 */
nil_db *nil_open(int engine, const char *dir, const char *const *cf_names, size_t num_cfs,
                 const char *options, char **errptr);

/*
 * Syncs, stops background work, releases the directory lock and frees the
 * handle, even when it reports an error. Release snapshots and destroy
 * iterators first.
 */
void nil_close(nil_db *db, char **errptr);

/* NIL_ENGINE_LSM, NIL_ENGINE_BTREE or NIL_ENGINE_PGHEAP; 0 for a NULL handle. */
int nil_engine_kind(const nil_db *db);

/* Longest key the engine accepts (the B+ tree limit depends on its page size). */
size_t nil_max_key_len(const nil_db *db);

/* Write batches: operations applied atomically, in order, by nil_write. */
nil_batch *nil_batch_new(void);
void nil_batch_put(nil_batch *b, uint32_t cf, const char *key, size_t klen, const char *val, size_t vlen);
void nil_batch_delete(nil_batch *b, uint32_t cf, const char *key, size_t klen);
/* Deletes [start, end). start == end deletes nothing; start > end fails at nil_write. */
void nil_batch_delete_range(nil_batch *b, uint32_t cf, const char *start, size_t slen, const char *end,
                            size_t elen);
/*
 * Adds an 8-byte little-endian signed operand to the value at key (i64add;
 * a missing value counts as 0). The engine resolves it at commit, so a
 * stored value of another length makes nil_write fail.
 */
void nil_batch_merge(nil_batch *b, uint32_t cf, const char *key, size_t klen, const char *operand, size_t olen);
size_t nil_batch_count(const nil_batch *b);
void nil_batch_clear(nil_batch *b);
void nil_batch_destroy(nil_batch *b);

/*
 * Applies the batch atomically. With sync != 0 the batch is durable when the
 * call returns; otherwise a process crash keeps it and a power loss may
 * drop it together with every later batch. A batch that received a NULL
 * pointer with a nonzero length is refused.
 */
void nil_write(nil_db *db, const nil_batch *b, int sync, char **errptr);

/*
 * Reads one key from the latest state, or from `snap` when it is not NULL.
 * Returns a malloc'd copy of the value and sets *vlen, or returns NULL when
 * the key is absent or on error. A found empty value is a non-NULL pointer
 * with *vlen == 0.
 */
char *nil_get(nil_db *db, const nil_snapshot *snap, uint32_t cf, const char *key, size_t klen, size_t *vlen,
              char **errptr);

/*
 * Reads n keys from one consistent view (snap, or a view taken for the
 * call). Fills vals[i] with a malloc'd copy and vlens[i] with its length,
 * or NULL and 0 when absent. On error every vals[i] is NULL.
 */
void nil_multi_get(nil_db *db, const nil_snapshot *snap, uint32_t cf, size_t n, const char *const *keys,
                   const size_t *klens, char **vals, size_t *vlens, char **errptr);

/*
 * Snapshots pin the state at creation. The sequence number (LSM),
 * transaction id (B+ tree) or last committed xid (pgheap) grows with every
 * commit. A released snapshot stays readable through iterators opened on
 * it. On pgheap an open snapshot also keeps VACUUM from removing the
 * versions it can see.
 */
nil_snapshot *nil_snapshot_new(nil_db *db, char **errptr);
uint64_t nil_snapshot_seq(const nil_snapshot *s);
void nil_snapshot_release(nil_snapshot *s);

/*
 * Opens an iterator over [lower, upper) of column family cf, reading `snap`
 * or the state at the time of the call. A NULL bound is open; a non-NULL
 * bound is a key even when its length is 0. fill_cache != 0 lets the reads
 * populate the block or page cache. The iterator starts unpositioned.
 */
nil_iter *nil_iter_new(nil_db *db, const nil_snapshot *snap, uint32_t cf, const char *lower, size_t lower_len,
                       const char *upper, size_t upper_len, int fill_cache, char **errptr);
void nil_iter_seek_to_first(nil_iter *it);
void nil_iter_seek_to_last(nil_iter *it);
/* First key >= key, never below the lower bound. */
void nil_iter_seek(nil_iter *it, const char *key, size_t klen);
/* Last key <= key, never at or above the upper bound. */
void nil_iter_seek_for_prev(nil_iter *it, const char *key, size_t klen);
/* next and prev require a valid iterator; on an invalid one they do nothing. */
void nil_iter_next(nil_iter *it);
void nil_iter_prev(nil_iter *it);
int nil_iter_valid(const nil_iter *it);
/*
 * Key and value of the current entry, or NULL (length 0) when the iterator
 * is invalid. The pointers stay valid until the next positioning call or
 * nil_iter_destroy; do not free them. nil_iter_value may read a large value
 * from disk; if that read fails it returns NULL and nil_iter_status reports
 * the error.
 */
const char *nil_iter_key(const nil_iter *it, size_t *klen);
const char *nil_iter_value(nil_iter *it, size_t *vlen);
/* Reports the error that made the iterator invalid, if any. */
void nil_iter_status(const nil_iter *it, char **errptr);
void nil_iter_destroy(nil_iter *it);

/*
 * Makes every write committed before the call durable. The LSM writes its
 * memtables to table files; the B+ tree syncs its data file; pgheap runs a
 * checkpoint.
 */
void nil_flush(nil_db *db, char **errptr);

/* Syncs the log (LSM, pgheap) or the data file (B+ tree) when sync != 0. */
void nil_flush_wal(nil_db *db, int sync, char **errptr);

/*
 * Compacts [start, end) of cf (NULL is an open end) down to the bottom LSM
 * level, dropping deleted data no snapshot can see. pgheap runs VACUUM on
 * the whole column family. The B+ tree returns at once.
 */
void nil_compact_range(nil_db *db, uint32_t cf, const char *start, size_t slen, const char *end, size_t elen,
                       char **errptr);

/*
 * Reads a property as a malloc'd string, or NULL for an unknown name.
 * Names follow RocksDB where the meaning matches (rocksdb.estimate-num-keys,
 * rocksdb.num-snapshots, rocksdb.num-files-at-level<N>,
 * rocksdb.total-sst-files-size, rocksdb.cur-size-all-mem-tables,
 * rocksdb.estimate-pending-compaction-bytes, rocksdb.block-cache-usage,
 * rocksdb.stats); engine-specific ones start with "nil." (nil.stats,
 * nil.latest-sequence, nil.num-range-tombstones, nil.btree.depth,
 * nil.btree.num-pages, nil.btree.free-pages, nil.pgheap.n-dead-tup,
 * nil.pgheap.n-tup-hot-upd, ...).
 */
char *nil_property(nil_db *db, uint32_t cf, const char *name);

/*
 * Sequence number (LSM), transaction id (B+ tree) or xid (pgheap) of the
 * last commit.
 */
uint64_t nil_latest_sequence(nil_db *db);

/*
 * Writes an openable copy of the database into directory `dir`, which must
 * not exist ("invalid-argument") and whose parent must ("not-found"). The
 * copy holds every write committed before the call and is a consistent
 * state: a prefix of the commit history, no batch in part. It is built in
 * `dir` with ".tmp" appended and renamed into place, so a crash never leaves
 * a partial copy under `dir`. Writers keep running. The LSM flushes its
 * memtables and hard-links its table files; the B+ tree copies its data
 * file (a clone on APFS) and rewrites its meta pages; pgheap runs a
 * checkpoint and copies its files and then its WAL, which the copy replays
 * when it is opened.
 */
void nil_checkpoint(nil_db *db, const char *dir, char **errptr);

/*
 * Estimates the bytes each of the n ranges [starts[i], limits[i]) of cf
 * takes on disk and stores them in sizes[i]. Both ends are keys (NULL with
 * length 0 is the empty key); a range with start >= limit is 0, and so is a
 * range that holds no key. The LSM counts table files only, as RocksDB's
 * GetApproximateSizes does by default; the B+ tree and pgheap estimate from
 * one root-to-leaf descent per end. On error every sizes[i] is 0.
 */
void nil_approximate_sizes(nil_db *db, uint32_t cf, size_t n, const char *const *starts,
                           const size_t *start_lens, const char *const *limits,
                           const size_t *limit_lens, uint64_t *sizes, char **errptr);

#ifdef __cplusplus
}
#endif

#endif /* NILENGINE_H */
