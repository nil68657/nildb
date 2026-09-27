// Exports the RocksDB header version to Go (see version.go).
#include <rocksdb/version.h>

extern "C" {
int nildb_rocksdb_major(void) { return ROCKSDB_MAJOR; }
int nildb_rocksdb_minor(void) { return ROCKSDB_MINOR; }
int nildb_rocksdb_patch(void) { return ROCKSDB_PATCH; }
}
