// Package redis implements NilDB's Redis data commands: strings, keys and
// expiry, hashes, lists, sets, sorted sets, GEO and the SCAN family. Every
// key has a metadata entry in the meta column family; strings keep their
// payload there, and hashes, lists, sets and sorted sets keep their
// elements in sub (and zscore) under a 64-bit version, as
// internal/layout and architecture.md section 3 describe.
package redis

import (
	"fmt"

	"github.com/nil68657/nildb/internal/command"
)

// groups holds one registration function per command group. Register
// calls them in order. A file that adds a group appends to groups from
// its own init function, for example
//
//	func init() { groups = append(groups, registerHash) }
//
// so adding a type never edits this file.
var groups = []func(*command.Registry){registerString, registerGeneric, registerExpire, registerScan}

// Register adds every Redis data command to r. command.Registry.Register
// panics on a name registered twice; Register turns that into an error.
func Register(r *command.Registry) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("redis: register: %v", p)
		}
	}()
	for _, g := range groups {
		g(r)
	}
	return nil
}
