package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
)

// overlayTxn is EXEC's read-your-writes transaction on engines without a
// WriteBatchWithIndex. Every write goes into an engine batch, in order, and
// into an overlay that keeps each key's latest put or delete and the merge
// operands after it. Reads look in the overlay first and fall back to live
// committed state, applying pending i64add operands to the committed value;
// iterators merge the overlay with an engine iterator in both directions,
// the job RocksDB's NewIteratorWithBase does. Commit writes the batch once
// and the engine resolves the merges itself.
type overlayTxn struct {
	s    *Store
	b    kvBatch
	ov   [NumCFs]*overlay
	n    int
	err  error // first Merge misuse; Commit returns it and writes nothing
	done bool
}

func newOverlayTxn(s *Store) *overlayTxn { return &overlayTxn{s: s} }

// opKind is what an overlay entry did last to its key.
type opKind uint8

const (
	opMerge  opKind = iota // only merges: they apply to the committed value
	opPut                  // a put, then the merges
	opDelete               // a delete, then the merges
)

type ovEntry struct {
	kind   opKind
	val    []byte
	merges [][]byte
}

// overlay holds one family's entries: a map for lookups, and the keys in
// order for iterators. Keys added since the last sort wait in pending.
type overlay struct {
	m       map[string]*ovEntry
	sorted  []string
	pending []string
}

// keysIn returns the overlay's keys in [lo, hi) in order; a nil or empty
// bound is open. The slice is never written again, so an iterator may
// keep it while the transaction adds keys.
func (o *overlay) keysIn(lo, hi []byte) []string {
	if len(o.pending) > 0 {
		slices.Sort(o.pending)
		merged := make([]string, 0, len(o.sorted)+len(o.pending))
		i, j := 0, 0
		for i < len(o.sorted) || j < len(o.pending) {
			if j == len(o.pending) || (i < len(o.sorted) && o.sorted[i] < o.pending[j]) {
				merged = append(merged, o.sorted[i])
				i++
			} else {
				merged = append(merged, o.pending[j])
				j++
			}
		}
		o.sorted, o.pending = merged, nil
	}
	a, b := 0, len(o.sorted)
	if len(lo) > 0 {
		a, _ = slices.BinarySearch(o.sorted, string(lo))
	}
	if len(hi) > 0 {
		b, _ = slices.BinarySearch(o.sorted, string(hi))
	}
	if a >= b {
		return nil
	}
	return o.sorted[a:b:b]
}

func (t *overlayTxn) batch() kvBatch {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	if t.b == nil {
		t.b = t.s.kv.newBatch()
	}
	return t.b
}

func (t *overlayTxn) entry(cf CF, key []byte) *ovEntry {
	checkCF(cf)
	o := t.ov[cf]
	if o == nil {
		o = &overlay{m: make(map[string]*ovEntry)}
		t.ov[cf] = o
	}
	e := o.m[string(key)]
	if e == nil {
		e = &ovEntry{}
		k := string(key)
		o.m[k] = e
		o.pending = append(o.pending, k)
	}
	return e
}

func (t *overlayTxn) lookup(cf CF, key []byte) *ovEntry {
	if int(cf) >= NumCFs || t.ov[cf] == nil {
		return nil
	}
	return t.ov[cf].m[string(key)]
}

// i64Fold applies nildb.i64add operands to base the way the engines do at
// commit: a missing or empty base counts as 0, two's-complement wraparound,
// and a stored value or operand of another length than 8 fails.
func i64Fold(base []byte, ops [][]byte) ([]byte, error) {
	var sum int64
	if len(base) > 0 {
		if len(base) != 8 {
			return nil, fmt.Errorf("store: i64add over a stored value of %d bytes, want 8", len(base))
		}
		sum = int64(binary.LittleEndian.Uint64(base))
	}
	for _, op := range ops {
		if len(op) != 8 {
			return nil, fmt.Errorf("store: i64add operand of %d bytes, want 8", len(op))
		}
		sum += int64(binary.LittleEndian.Uint64(op))
	}
	return binary.LittleEndian.AppendUint64(nil, uint64(sum)), nil
}

// resolve returns the value an overlay entry gives its key. committed is
// the key's committed value (nil when absent); it matters only for an
// entry that holds merges alone.
func resolve(e *ovEntry, committed []byte) ([]byte, bool, error) {
	var base []byte
	switch e.kind {
	case opPut:
		if len(e.merges) == 0 {
			return e.val, true, nil
		}
		base = e.val
	case opDelete:
		if len(e.merges) == 0 {
			return nil, false, nil
		}
	default:
		base = committed
	}
	v, err := i64Fold(base, e.merges)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (t *overlayTxn) Get(cf CF, key []byte) ([]byte, bool, error) {
	e := t.lookup(cf, key)
	if e == nil {
		return t.s.Get(cf, key)
	}
	var committed []byte
	if e.kind == opMerge {
		v, ok, err := t.s.Get(cf, key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			committed = v
		}
	}
	v, ok, err := resolve(e, committed)
	if !ok || err != nil {
		return nil, false, err
	}
	return clone(v), true, nil
}

func (t *overlayTxn) GetPinned(cf CF, key []byte, fn func([]byte) error) (bool, error) {
	if t.lookup(cf, key) == nil {
		return t.s.GetPinned(cf, key, fn)
	}
	v, ok, err := t.Get(cf, key)
	if !ok || err != nil {
		return false, err
	}
	return true, fn(v)
}

func (t *overlayTxn) MultiGet(cf CF, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	var rest [][]byte
	var at []int
	for i, k := range keys {
		if t.lookup(cf, k) == nil {
			rest = append(rest, k)
			at = append(at, i)
			continue
		}
		v, ok, err := t.Get(cf, k)
		if err != nil {
			return nil, err
		}
		if ok {
			out[i] = v
		}
	}
	if len(rest) > 0 {
		vals, err := t.s.MultiGet(cf, rest)
		if err != nil {
			return nil, err
		}
		for j, v := range vals {
			out[at[j]] = v
		}
	}
	return out, nil
}

func (t *overlayTxn) Iter(cf CF, lo, hi []byte, o IterOpts) Iterator {
	if t.s.closed.Load() {
		return &failedIter{err: ErrClosed}
	}
	ov := t.lookupCF(cf)
	if ov == nil || len(ov.m) == 0 {
		return t.s.Iter(cf, lo, hi, o)
	}
	skeys := ov.keysIn(lo, hi)
	keys := make([][]byte, len(skeys))
	ents := make([]*ovEntry, len(skeys))
	for i, k := range skeys {
		keys[i], ents[i] = []byte(k), ov.m[k]
	}
	return &overlayIter{base: t.s.Iter(cf, lo, hi, o), keys: keys, ents: ents}
}

func (t *overlayTxn) lookupCF(cf CF) *overlay {
	checkCF(cf)
	return t.ov[cf]
}

func (t *overlayTxn) Put(cf CF, key, val []byte) {
	t.batch().put(cf, key, val)
	e := t.entry(cf, key)
	e.kind, e.val, e.merges = opPut, clone(val), nil
	t.n++
}

func (t *overlayTxn) Delete(cf CF, key []byte) {
	t.batch().delete(cf, key)
	e := t.entry(cf, key)
	e.kind, e.val, e.merges = opDelete, nil, nil
	t.n++
}

func (t *overlayTxn) Merge(cf CF, key, operand []byte) {
	b := t.batch()
	if err := mergeErr(cf); err != nil {
		if t.err == nil {
			t.err = err
		}
		return
	}
	b.merge(cf, key, operand)
	e := t.entry(cf, key)
	e.merges = append(e.merges, clone(operand))
	t.n++
}

// DeleteRange always fails, as it does on RocksDB's WriteBatchWithIndex;
// commands that need one are NoMulti.
func (t *overlayTxn) DeleteRange(cf CF, lo, hi []byte) error {
	if t.done {
		panic("store: Txn used after Commit or Discard")
	}
	return ErrRangeInIndexedTxn
}

func (t *overlayTxn) Len() int { return t.n }

func (t *overlayTxn) Commit() error {
	if t.done {
		return errTxnDone
	}
	defer t.Discard()
	switch {
	case t.err != nil:
		return t.err
	case t.n == 0:
		return nil
	case t.s.closed.Load():
		return ErrClosed
	}
	return t.s.kv.write(t.b, t.s.syncCommits())
}

func (t *overlayTxn) Discard() {
	t.done = true
	if t.b != nil {
		t.b.destroy()
		t.b = nil
	}
	t.ov = [NumCFs]*overlay{}
}

// Where an overlayIter's current entry comes from.
const (
	atNone  = iota // invalid
	atBase         // the engine iterator
	atDelta        // the overlay, alone or over the same committed key
)

// overlayIter merges an engine iterator with the overlay keys in the same
// bounds. In each direction both sides stand at or past the current entry
// and the nearer one is current; an overlay key that hides its committed
// value (a delete without later merges) moves both sides on. A change of
// direction seeks both sides relative to the current key.
type overlayIter struct {
	base  Iterator
	keys  [][]byte   // overlay keys in the bounds, in order
	ents  []*ovEntry // their entries
	di    int        // overlay position
	fwd   bool
	cur   int
	equal bool   // the overlay entry sits over a committed key
	val   []byte // the overlay entry's value
	err   error
}

func (i *overlayIter) deltaValid() bool { return i.di >= 0 && i.di < len(i.keys) }

// settle makes the nearer side current, skipping hidden overlay keys.
func (i *overlayIter) settle() {
	i.cur, i.equal, i.val = atNone, false, nil
	for i.err == nil {
		bv, dv := i.base.Valid(), i.deltaValid()
		if !bv {
			if err := i.base.Err(); err != nil {
				i.err = err
				return
			}
		}
		if !dv {
			if bv {
				i.cur = atBase
			}
			return
		}
		c := 1 // the overlay key comes first when the base side is done
		if bv {
			c = bytes.Compare(i.base.Key(), i.keys[i.di])
			if !i.fwd {
				c = -c
			}
			if c < 0 {
				i.cur = atBase
				return
			}
		}
		equal := bv && c == 0
		var committed []byte
		if equal {
			committed = i.base.Value()
		}
		v, ok, err := resolve(i.ents[i.di], committed)
		if err != nil {
			i.err = err
			return
		}
		if !ok {
			i.stepDelta()
			if equal {
				i.stepBase()
			}
			continue
		}
		i.cur, i.equal, i.val = atDelta, equal, v
		return
	}
}

func (i *overlayIter) stepDelta() {
	if i.fwd {
		i.di++
	} else {
		i.di--
	}
}

func (i *overlayIter) stepBase() {
	if i.fwd {
		i.base.Next()
	} else {
		i.base.Prev()
	}
}

// lowerBound is the first overlay index with key >= k.
func (i *overlayIter) lowerBound(k []byte) int {
	n, _ := slices.BinarySearchFunc(i.keys, k, bytes.Compare)
	return n
}

func (i *overlayIter) Seek(k []byte) {
	i.fwd, i.err = true, nil
	i.base.Seek(k)
	i.di = i.lowerBound(k)
	i.settle()
}

func (i *overlayIter) SeekForPrev(k []byte) {
	i.fwd, i.err = false, nil
	i.base.SeekForPrev(k)
	n := i.lowerBound(k)
	if n < len(i.keys) && bytes.Equal(i.keys[n], k) {
		n++
	}
	i.di = n - 1
	i.settle()
}

func (i *overlayIter) SeekToFirst() {
	i.fwd, i.err = true, nil
	i.base.SeekToFirst()
	i.di = 0
	i.settle()
}

func (i *overlayIter) SeekToLast() {
	i.fwd, i.err = false, nil
	i.base.SeekToLast()
	i.di = len(i.keys) - 1
	i.settle()
}

func (i *overlayIter) Valid() bool { return i.cur != atNone && i.err == nil }

func (i *overlayIter) Next() {
	if !i.Valid() {
		return
	}
	if !i.fwd {
		k := clone(i.Key())
		i.fwd = true
		i.base.Seek(k)
		if i.base.Valid() && bytes.Equal(i.base.Key(), k) {
			i.base.Next()
		}
		i.di = i.lowerBound(k)
		if i.di < len(i.keys) && bytes.Equal(i.keys[i.di], k) {
			i.di++
		}
		i.settle()
		return
	}
	i.advance()
}

func (i *overlayIter) Prev() {
	if !i.Valid() {
		return
	}
	if i.fwd {
		k := clone(i.Key())
		i.fwd = false
		i.base.SeekForPrev(k)
		if i.base.Valid() && bytes.Equal(i.base.Key(), k) {
			i.base.Prev()
		}
		i.di = i.lowerBound(k) - 1
		i.settle()
		return
	}
	i.advance()
}

// advance moves past the current entry in the current direction.
func (i *overlayIter) advance() {
	switch {
	case i.cur == atBase:
		i.stepBase()
	case i.equal:
		i.stepBase()
		i.stepDelta()
	default:
		i.stepDelta()
	}
	i.settle()
}

func (i *overlayIter) Key() []byte {
	switch i.cur {
	case atBase:
		return i.base.Key()
	case atDelta:
		return i.keys[i.di]
	}
	return nil
}

func (i *overlayIter) Value() []byte {
	switch i.cur {
	case atBase:
		return i.base.Value()
	case atDelta:
		return i.val
	}
	return nil
}

func (i *overlayIter) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.base.Err()
}

func (i *overlayIter) Close() { i.base.Close() }
