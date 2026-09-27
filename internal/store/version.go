package store

// version_cxx.cc includes <rocksdb/version.h>, a C++ header (it pulls in
// <string>), and exports the three macros as C functions. cgo does not
// pass CGO_CFLAGS to C++ files, so the Homebrew include directory is named
// here; elsewhere set CGO_CXXFLAGS=-I<prefix>/include.

/*
#cgo CXXFLAGS: -std=c++17
#cgo darwin,arm64 CXXFLAGS: -I/opt/homebrew/include
#cgo darwin,amd64 CXXFLAGS: -I/usr/local/include
int nildb_rocksdb_major(void);
int nildb_rocksdb_minor(void);
int nildb_rocksdb_patch(void);
*/
import "C"

// Version returns the RocksDB version NilDB was compiled against, from
// ROCKSDB_MAJOR, ROCKSDB_MINOR and ROCKSDB_PATCH in <rocksdb/version.h>.
func Version() (major, minor, patch int) {
	return int(C.nildb_rocksdb_major()), int(C.nildb_rocksdb_minor()), int(C.nildb_rocksdb_patch())
}
