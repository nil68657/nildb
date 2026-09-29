package store

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/nil68657/nildb/internal/layout"
)

// txnModel is committed state plus a transaction's writes, per family.
type txnModel map[CF]map[string][]byte

func (m txnModel) clone() txnModel {
	out := txnModel{}
	for cf, kv := range m {
		out[cf] = maps.Clone(kv)
	}
	return out
}

// between returns the model's keys of cf in [lo, hi) in order; a nil
// bound is open.
func (m txnModel) between(cf CF, lo, hi []byte) []string {
	var out []string
	for k := range m[cf] {
		if (lo == nil || k >= string(lo)) && (hi == nil || k < string(hi)) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// walk drives an iterator through random seeks and steps and compares each
// position with the model.
func walk(t *testing.T, rng *rand.Rand, r Reader, m txnModel, cf CF, key func() []byte) {
	t.Helper()
	var lo, hi []byte
	if rng.IntN(3) > 0 {
		lo = key()
	}
	if rng.IntN(3) > 0 {
		hi = key()
	}
	if lo != nil && hi != nil && bytes.Compare(lo, hi) > 0 {
		lo, hi = hi, lo
	}
	want := m.between(cf, lo, hi)
	it := r.Iter(cf, lo, hi, IterOpts{FillCache: true})
	defer it.Close()
	pos := -1
	var trace []string
	for range 40 {
		switch rng.IntN(6) {
		case 0:
			it.SeekToFirst()
			pos = min(0, len(want)-1)
			trace = append(trace, "first")
		case 1:
			it.SeekToLast()
			pos = len(want) - 1
			trace = append(trace, "last")
		case 2:
			k := key()
			it.Seek(k)
			pos, _ = slices.BinarySearch(want, string(k))
			if pos == len(want) {
				pos = -1
			}
			trace = append(trace, "seek "+string(k))
		case 3:
			k := key()
			it.SeekForPrev(k)
			n, found := slices.BinarySearch(want, string(k))
			if found {
				n++
			}
			pos = n - 1
			trace = append(trace, "seekForPrev "+string(k))
		case 4:
			if pos < 0 {
				continue
			}
			it.Next()
			if pos++; pos == len(want) {
				pos = -1
			}
			trace = append(trace, "next")
		default:
			if pos < 0 {
				continue
			}
			it.Prev()
			pos--
			trace = append(trace, "prev")
		}
		if err := it.Err(); err != nil {
			t.Fatalf("%s [%q, %q) after %v: %v", cf, lo, hi, trace, err)
		}
		if it.Valid() != (pos >= 0) {
			t.Fatalf("%s [%q, %q) after %v: valid %v, want position %d of %d", cf, lo, hi, trace, it.Valid(), pos, len(want))
		}
		if pos >= 0 {
			if k := string(it.Key()); k != want[pos] {
				t.Fatalf("%s [%q, %q) after %v: key %q, want %q", cf, lo, hi, trace, k, want[pos])
			}
			if v := it.Value(); !bytes.Equal(v, m[cf][want[pos]]) {
				t.Fatalf("%s key %q after %v: value %x, want %x", cf, want[pos], trace, v, m[cf][want[pos]])
			}
		}
	}
}

// TestIndexedTxnMatchesModel checks an indexed transaction's reads of its
// own writes against a model: BeginIndexed (RocksDB's WriteBatchWI, the
// overlay on the Rust engines) and the overlay over every engine.
func TestIndexedTxnMatchesModel(t *testing.T) {
	for _, kind := range []string{"BeginIndexed", "overlay"} {
		t.Run(kind, func(t *testing.T) {
			s := openTest(t, nil)
			rng := rand.New(rand.NewPCG(11, uint64(len(kind))))
			key := func() []byte { return fmt.Appendf(nil, "k%03d", rng.IntN(80)) }
			counter := func() []byte { return layout.CountKey(uint32(rng.IntN(4))) }
			val := func() []byte { return fmt.Appendf(nil, "v%d", rng.IntN(1_000_000)) }
			committed := txnModel{CFSub: {}, CFDefault: {}}
			base := s.Begin()
			for range 50 {
				k, v := key(), val()
				base.Put(CFSub, k, v)
				committed[CFSub][string(k)] = v
			}
			commit(t, base)
			// The layout marker and the version seed live in default too.
			for _, k := range [][]byte{layout.LayoutMarkerKey, layout.SeqKey(layout.SeqVersion)} {
				if v, ok := mustGet(t, s, CFDefault, k); ok {
					committed[CFDefault][string(k)] = v
				}
			}
			// RocksDB's base-delta iterator does not resolve merges, so
			// iterating default with pending merges is overlay-only.
			iterDefault := kind == "overlay" || s.Engine() != EngineRocksDB

			for round := range 25 {
				var txn Txn
				if kind == "overlay" {
					txn = newOverlayTxn(s)
				} else {
					txn = s.BeginIndexed()
				}
				m := committed.clone()
				merged := false
				for range 80 {
					switch op := rng.IntN(12); {
					case op < 4:
						k, v := key(), val()
						txn.Put(CFSub, k, v)
						m[CFSub][string(k)] = v
					case op < 6:
						k := key()
						txn.Delete(CFSub, k)
						delete(m[CFSub], string(k))
					case op < 8:
						k, d := counter(), int64(rng.IntN(200)-100)
						sum, _ := layout.DecodeCount(m[CFDefault][string(k)])
						txn.Merge(CFDefault, k, layout.AppendCount(nil, d))
						m[CFDefault][string(k)] = layout.AppendCount(nil, sum+d)
						merged = true
					case op == 8:
						k := counter()
						if rng.IntN(2) == 0 {
							v := layout.AppendCount(nil, int64(rng.IntN(50)))
							txn.Put(CFDefault, k, v)
							m[CFDefault][string(k)] = v
						} else {
							txn.Delete(CFDefault, k)
							delete(m[CFDefault], string(k))
						}
					case op == 9:
						for _, cf := range []CF{CFSub, CFDefault} {
							k := key()
							if cf == CFDefault {
								k = counter()
							}
							v, ok := mustGet(t, txn, cf, k)
							if want, wok := m[cf][string(k)]; ok != wok || !bytes.Equal(v, want) {
								t.Fatalf("round %d: Get(%s, %q) = %x, %v; want %x, %v", round, cf, k, v, ok, want, wok)
							}
						}
						keys := [][]byte{key(), key(), key()}
						vals, err := txn.MultiGet(CFSub, keys)
						if err != nil {
							t.Fatal(err)
						}
						for i, k := range keys {
							if want := m[CFSub][string(k)]; !bytes.Equal(vals[i], want) || (vals[i] == nil) != (want == nil) {
								t.Fatalf("round %d: MultiGet %q = %x, want %x", round, k, vals[i], want)
							}
						}
					default:
						walk(t, rng, txn, m, CFSub, key)
						if iterDefault || !merged {
							walk(t, rng, txn, m, CFDefault, counter)
						}
					}
				}
				if err := txn.DeleteRange(CFSub, []byte("a"), []byte("b")); !errors.Is(err, ErrRangeInIndexedTxn) {
					t.Fatalf("DeleteRange: %v", err)
				}
				if rng.IntN(4) == 0 {
					txn.Discard()
				} else {
					commit(t, txn)
					committed = m
				}
				for _, cf := range []CF{CFSub, CFDefault} {
					it := s.Iter(cf, nil, nil, IterOpts{})
					it.SeekToFirst()
					got := collect(t, it, true)
					it.Close()
					if want := committed.between(cf, nil, nil); !slices.Equal(got, want) {
						t.Fatalf("round %d: %s holds %q after the commit, want %q", round, cf, got, want)
					}
					for k, v := range committed[cf] {
						wantValue(t, s, cf, []byte(k), v)
					}
				}
			}
		})
	}
}
