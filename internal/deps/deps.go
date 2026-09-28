//go:build tools

// Package deps pins NilDB's third-party modules in go.mod before the packages
// that import them exist, so `go mod tidy` keeps them and builds can run with
// GOPROXY=off. Delete this file once every module below has a real importer.
package deps

import (
	_ "github.com/cespare/xxhash/v2"
	_ "github.com/golang/geo/r3"
	_ "github.com/golang/geo/s1"
	_ "github.com/golang/geo/s2"
	_ "github.com/linxGnu/grocksdb"
	_ "github.com/redis/go-redis/v9"
	_ "go.mongodb.org/mongo-driver/v2/bson"
)
