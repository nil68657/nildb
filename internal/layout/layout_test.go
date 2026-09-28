package layout

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/nil68657/nildb/internal/keyenc"
	kt "github.com/nil68657/nildb/internal/keyenc/keyenctest"
)

func ks(t *testing.T, v bson.RawValue) []byte {
	t.Helper()
	b, err := keyenc.Encode(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestGoldens pins the bytes of every layout in architecture.md section 3.
// Hex strings are split at field boundaries.
func TestGoldens(t *testing.T) {
	oid := bson.ObjectID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	ksOne := "1e05bff0000000000000" // keyenc of int32 1
	ksOID := "64000102030405060708090a0b"
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		// meta keys
		{"MetaKey", MetaKey(nil, 3, []byte("foo")), "03" + "666f6f"},
		{"MetaKey empty user key", MetaKey(nil, 15, nil), "0f"},
		{"VersionKey", VersionKey(nil, 0x0102030405060708), "76" + "0102030405060708"},
		// meta values
		{"string meta", Meta{Type: TString, Payload: []byte("hi")}.Encode(nil), "01" + "0000000000000000" + "6869"},
		{"string meta with TTL", Meta{Type: TString, ExpireMS: 1700000000000, Payload: []byte("hi")}.Encode(nil), "01" + "0000018bcfe56800" + "6869"},
		{"empty string meta", Meta{Type: TString}.Encode(nil), "01" + "0000000000000000"},
		{"hash meta", Meta{Type: THash, Version: 0x0a0b, Size: 3}.Encode(nil), "02" + "0000000000000000" + "0000000000000a0b" + "0000000000000003"},
		{"set meta", Meta{Type: TSet, ExpireMS: 1, Version: 2, Size: 3}.Encode(nil), "04" + "0000000000000001" + "0000000000000002" + "0000000000000003"},
		{"zset meta", Meta{Type: TZSet, Version: 2, Size: 1}.Encode(nil), "05" + "0000000000000000" + "0000000000000002" + "0000000000000001"},
		{"list meta", Meta{Type: TList, ExpireMS: 5, Version: 7, Size: 2, Head: ListStart - 1, Tail: ListStart + 1}.Encode(nil),
			"03" + "0000000000000005" + "0000000000000007" + "0000000000000002" + "7fffffffffffffff" + "8000000000000001"},
		{"version entry", VersionEntry{ExpireMS: 0x10, DB: 2, Key: []byte("k")}.Encode(nil), "0000000000000010" + "02" + "6b"},
		// sub and zscore
		{"SubKey", SubKey(nil, 1, []byte("f")), "0000000000000001" + "66"},
		{"ListKey", ListKey(nil, 1, ListStart), "0000000000000001" + "8000000000000000"},
		{"ScoreKey 1.5", ScoreKey(nil, 1, 1.5, []byte("m")), "0000000000000001" + "bff8000000000000" + "6d"},
		{"ScoreKey 0", ScoreKey(nil, 1, 0, []byte("m")), "0000000000000001" + "8000000000000000" + "6d"},
		{"ScoreKey -0", ScoreKey(nil, 1, math.Copysign(0, -1), []byte("m")), "0000000000000001" + "8000000000000000" + "6d"},
		{"ScoreKey -1", ScoreKey(nil, 1, -1, nil), "0000000000000001" + "400fffffffffffff"},
		{"ScoreValue 2", ScoreValue(nil, 2), "c000000000000000"},
		{"ScoreValue -0", ScoreValue(nil, math.Copysign(0, -1)), "8000000000000000"},
		{"VersionPrefix", VersionPrefix(nil, 0xff), "00000000000000ff"},
		// documents
		{"DocKey", DocKey(nil, 7, ks(t, kt.Int32(1))), "00000007" + ksOne},
		{"IdxKey", IdxKey(nil, 9, ks(t, kt.String("a")), ks(t, kt.Int32(1))), "00000009" + "3c6100" + ksOne},
		{"IdxKey no values", IdxKey(nil, 9), "00000009"},
		{"GeoKey", GeoKey(nil, 9, 0x89c25a0000000000, ks(t, kt.OID(oid))), "00000009" + "89c25a0000000000" + ksOID},
		{"GeoCellPrefix", GeoCellPrefix(nil, 9, 0x89c25a0000000000), "00000009" + "89c25a0000000000"},
		{"GeoPointValue", GeoPointValue(nil, 40.5, -73.25), "01" + "4044400000000000" + "c052500000000000"},
		{"GeoShapeValue", GeoShapeValue(nil), "02"},
		{"ColKey", ColKey(nil, 9, ks(t, kt.Int32(1))), "00000009" + ksOne},
		{"ColValue", ColValue(nil, kt.Int32(5)), "10" + "05000000"},
		{"IDPrefix", IDPrefix(nil, 0x01020304), "01020304"},
		// catalog
		{"LayoutMarkerKey", LayoutMarkerKey, "00"},
		{"LayoutMarker", []byte(LayoutMarker), hex.EncodeToString([]byte("nildb-layout-1"))},
		{"CollKey", CollKey("shop.orders"), "63" + "73686f702e6f7264657273"},
		{"IndexKey", IndexKey(7, "a_1"), "69" + "00000007" + "615f31"},
		{"CountKey", CountKey(7), "6e" + "00000007"},
		{"SeqKey coll", SeqKey(SeqColl), "73" + "636f6c6c"},
		{"SeqKey idx", SeqKey(SeqIdx), "73" + "696478"},
		{"SeqKey version", SeqKey(SeqVersion), "73" + "76657273696f6e"},
		{"AppendCount 1", AppendCount(nil, 1), "0100000000000000"},
		{"AppendCount -1", AppendCount(nil, -1), "ffffffffffffffff"},
	}
	for _, c := range cases {
		if got := hex.EncodeToString(c.got); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// TestAppendsToDst checks that builders extend dst rather than overwrite it.
func TestAppendsToDst(t *testing.T) {
	pre := []byte("xy")
	for name, got := range map[string][]byte{
		"MetaKey":       MetaKey(pre, 1, []byte("k")),
		"VersionKey":    VersionKey(pre, 1),
		"Meta.Encode":   Meta{Type: THash}.Encode(pre),
		"VersionEntry":  VersionEntry{}.Encode(pre),
		"SubKey":        SubKey(pre, 1, nil),
		"ListKey":       ListKey(pre, 1, 1),
		"ScoreKey":      ScoreKey(pre, 1, 1, nil),
		"ScoreValue":    ScoreValue(pre, 1),
		"DocKey":        DocKey(pre, 1, nil),
		"IdxKey":        IdxKey(pre, 1),
		"GeoKey":        GeoKey(pre, 1, 1, nil),
		"GeoPointValue": GeoPointValue(pre, 1, 1),
		"ColKey":        ColKey(pre, 1, nil),
		"ColValue":      ColValue(pre, kt.Null()),
		"IDPrefix":      IDPrefix(pre, 1),
		"AppendCount":   AppendCount(pre, 1),
	} {
		if !bytes.HasPrefix(got, []byte("xy")) {
			t.Errorf("%s dropped dst: %x", name, got)
		}
	}
}

func TestSplitMetaKey(t *testing.T) {
	db, key, ok := SplitMetaKey(MetaKey(nil, 3, []byte("foo")))
	if !ok || db != 3 || string(key) != "foo" {
		t.Errorf("SplitMetaKey = %d %q %v", db, key, ok)
	}
	if db, key, ok := SplitMetaKey([]byte{0}); !ok || db != 0 || len(key) != 0 {
		t.Errorf("empty user key: %d %q %v", db, key, ok)
	}
	for _, k := range [][]byte{nil, {NumDBs}, VersionKey(nil, 1), {0xFF, 'a'}} {
		if _, _, ok := SplitMetaKey(k); ok {
			t.Errorf("SplitMetaKey(%x) accepted a non-meta key", k)
		}
	}
	if v, ok := SplitVersionKey(VersionKey(nil, 42)); !ok || v != 42 {
		t.Errorf("SplitVersionKey = %d %v", v, ok)
	}
	for _, k := range [][]byte{nil, {VersionMapPrefix}, MetaKey(nil, 1, []byte("12345678")), append(VersionKey(nil, 1), 0)} {
		if _, ok := SplitVersionKey(k); ok {
			t.Errorf("SplitVersionKey(%x) accepted", k)
		}
	}
}

func TestMetaRoundTrip(t *testing.T) {
	metas := []Meta{
		{Type: TString, Payload: []byte{}},
		{Type: TString, ExpireMS: math.MaxInt64, Payload: []byte("\x00\xffvalue")},
		{Type: THash, ExpireMS: 1, Version: math.MaxUint64 - 1, Size: 1 << 40},
		{Type: TSet, Version: 1, Size: 0},
		{Type: TZSet, ExpireMS: -1, Version: 2, Size: 9},
		{Type: TList, Version: 3, Size: 4, Head: ListStart - 2, Tail: ListStart + 2},
	}
	for _, m := range metas {
		enc := m.Encode(nil)
		got, err := DecodeMeta(enc)
		if err != nil {
			t.Fatalf("DecodeMeta(%x): %v", enc, err)
		}
		if got.Type != m.Type || got.ExpireMS != m.ExpireMS || got.Version != m.Version || got.Size != m.Size ||
			got.Head != m.Head || got.Tail != m.Tail || !bytes.Equal(got.Payload, m.Payload) {
			t.Errorf("round trip %+v -> %+v", m, got)
		}
		if m.Type == TString && len(enc) > StringHeaderLen && &got.Payload[0] != &enc[StringHeaderLen] {
			t.Errorf("string Payload does not alias the input")
		}
	}
	// Fields a type does not store are dropped.
	got, err := DecodeMeta(Meta{Type: TString, Version: 9, Size: 9, Head: 9, Payload: []byte("v")}.Encode(nil))
	if err != nil || got.Version != 0 || got.Size != 0 || got.Head != 0 {
		t.Errorf("string meta kept collection fields: %+v %v", got, err)
	}
}

func TestDecodeMetaErrors(t *testing.T) {
	hash := Meta{Type: THash, Version: 1, Size: 1}.Encode(nil)
	list := Meta{Type: TList, Version: 1}.Encode(nil)
	bad := map[string][]byte{
		"empty":            nil,
		"short header":     make([]byte, 8),
		"layout version 1": append([]byte{0x11}, make([]byte, 8)...),
		"type 0":           make([]byte, 9),
		"type 6":           append([]byte{0x06}, make([]byte, 24)...),
		"hash short":       hash[:len(hash)-1],
		"hash long":        append(bytes.Clone(hash), 0),
		"list as coll len": list[:CollMetaLen],
		"list long":        append(bytes.Clone(list), 0),
	}
	for name, v := range bad {
		if _, err := DecodeMeta(v); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestExpiredBoundary(t *testing.T) {
	cases := []struct {
		expire, now int64
		want        bool
	}{
		{0, 0, false},
		{0, math.MaxInt64, false}, // 0 means no expiry
		{1000, 999, false},
		{1000, 1000, true}, // expire_ms <= now: Redis treats the key as gone
		{1000, 1001, true},
		{-5, 0, true}, // an absolute time in the past
		{math.MaxInt64, math.MaxInt64 - 1, false},
	}
	for _, c := range cases {
		if got := (Meta{Type: TString, ExpireMS: c.expire}).Expired(c.now); got != c.want {
			t.Errorf("Meta{ExpireMS: %d}.Expired(%d) = %v, want %v", c.expire, c.now, got, c.want)
		}
		if got := (VersionEntry{ExpireMS: c.expire}).Expired(c.now); got != c.want {
			t.Errorf("VersionEntry{ExpireMS: %d}.Expired(%d) = %v, want %v", c.expire, c.now, got, c.want)
		}
	}
}

func TestVersionEntryRoundTrip(t *testing.T) {
	for _, e := range []VersionEntry{{}, {ExpireMS: -1, DB: 15, Key: []byte("\x00key")}, {ExpireMS: 1700000000000, DB: 3, Key: []byte("k")}} {
		got, err := DecodeVersionEntry(e.Encode(nil))
		if err != nil || got.ExpireMS != e.ExpireMS || got.DB != e.DB || !bytes.Equal(got.Key, e.Key) {
			t.Errorf("round trip %+v -> %+v %v", e, got, err)
		}
	}
	if _, err := DecodeVersionEntry(make([]byte, 8)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("short entry err = %v", err)
	}
}

func TestTypeString(t *testing.T) {
	want := map[Type]string{0: "none", TString: "string", THash: "hash", TList: "list", TSet: "set", TZSet: "zset", 9: "Type(9)"}
	for ty, s := range want {
		if ty.String() != s {
			t.Errorf("Type(%d).String() = %q, want %q", ty, ty.String(), s)
		}
	}
	for ty, coll := range map[Type]bool{0: false, TString: false, THash: true, TList: true, TSet: true, TZSet: true, 6: false} {
		if ty.IsCollection() != coll {
			t.Errorf("Type(%d).IsCollection() = %v", ty, !coll)
		}
	}
}

func TestSubKeys(t *testing.T) {
	v, sub, ok := SplitSubKey(SubKey(nil, 77, []byte("field")))
	if !ok || v != 77 || string(sub) != "field" {
		t.Errorf("SplitSubKey = %d %q %v", v, sub, ok)
	}
	if _, _, ok := SplitSubKey(make([]byte, 7)); ok {
		t.Errorf("SplitSubKey accepted 7 bytes")
	}
	v, idx, ok := SplitListKey(ListKey(nil, 5, ListStart-3))
	if !ok || v != 5 || idx != ListStart-3 {
		t.Errorf("SplitListKey = %d %d %v", v, idx, ok)
	}
	if _, _, ok := SplitListKey(make([]byte, 17)); ok {
		t.Errorf("SplitListKey accepted 17 bytes")
	}
	if VersionOf(SubKey(nil, 99, []byte("x"))) != 99 || VersionOf([]byte{1}) != 0 {
		t.Errorf("VersionOf wrong")
	}
	lo, hi := VersionBounds(0xff)
	if hex.EncodeToString(lo) != "00000000000000ff" || hex.EncodeToString(hi) != "0000000000000100" {
		t.Errorf("VersionBounds(0xff) = %x %x", lo, hi)
	}
	for _, k := range [][]byte{SubKey(nil, 0xff, nil), SubKey(nil, 0xff, []byte{0xff, 0xff}), ListKey(nil, 0xff, math.MaxUint64), ScoreKey(nil, 0xff, math.Inf(1), []byte{0xff})} {
		if bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
			t.Errorf("key %x outside VersionBounds(0xff)", k)
		}
	}
	if _, hi := VersionBounds(math.MaxUint64); hi != nil {
		t.Errorf("VersionBounds(max) hi = %x, want nil", hi)
	}
}

// TestScoreKeyOrder checks that zscore keys of one version sort by score,
// then member, with -0 equal to 0.
func TestScoreKeyOrder(t *testing.T) {
	type entry struct {
		score  float64
		member string
	}
	ordered := []entry{
		{math.Inf(-1), "a"}, {-math.MaxFloat64, "a"}, {-1, "z"}, {-math.SmallestNonzeroFloat64, "a"},
		{math.Copysign(0, -1), "a"}, {0, "b"}, {math.Copysign(0, -1), "c"},
		{math.SmallestNonzeroFloat64, ""}, {1, "a"}, {1, "a\x00"}, {1, "b"},
		{float64(1 << 52), "a"}, {math.MaxFloat64, "a"}, {math.Inf(1), "a"},
	}
	for i := 1; i < len(ordered); i++ {
		a := ScoreKey(nil, 3, ordered[i-1].score, []byte(ordered[i-1].member))
		b := ScoreKey(nil, 3, ordered[i].score, []byte(ordered[i].member))
		if bytes.Compare(a, b) >= 0 {
			t.Errorf("%v before %v: %x >= %x", ordered[i-1], ordered[i], a, b)
		}
	}
	for _, e := range ordered {
		v, score, member := SplitScoreKey(ScoreKey(nil, 3, e.score, []byte(e.member)))
		want := e.score
		if want == 0 {
			want = 0
		}
		if v != 3 || math.Float64bits(score) != math.Float64bits(want) || string(member) != e.member {
			t.Errorf("SplitScoreKey(%v) = %d %v %q", e, v, score, member)
		}
		got, err := DecodeScoreValue(ScoreValue(nil, e.score))
		if err != nil || math.Float64bits(got) != math.Float64bits(want) {
			t.Errorf("score value %v -> %v %v", e.score, got, err)
		}
	}
	if v, s, m := SplitScoreKey(make([]byte, 15)); v != 0 || s != 0 || m != nil {
		t.Errorf("SplitScoreKey(short) = %d %v %x", v, s, m)
	}
	if _, err := DecodeScoreValue(make([]byte, 7)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("DecodeScoreValue(short) err = %v", err)
	}
	// A GEO cell's score range [h<<shift, (h+1)<<shift) is one zscore range:
	// integral doubles keep integer order.
	lo := ScoreKey(nil, 3, float64(uint64(0x123)<<40), nil)
	hi := ScoreKey(nil, 3, float64(uint64(0x124)<<40), nil)
	in := ScoreKey(nil, 3, float64(uint64(0x123)<<40|0xFFFFFFFFFF), []byte("m"))
	if bytes.Compare(lo, in) > 0 || bytes.Compare(in, hi) >= 0 {
		t.Errorf("geohash score range does not hold its cell")
	}
}

func TestDocKeys(t *testing.T) {
	id := ks(t, kt.String("order-1"))
	coll, ksID := SplitDocKey(DocKey(nil, 12, id))
	if coll != 12 || !bytes.Equal(ksID, id) {
		t.Errorf("SplitDocKey = %d %x", coll, ksID)
	}
	if c, k := SplitDocKey([]byte{1, 2, 3}); c != 0 || k != nil {
		t.Errorf("SplitDocKey(short) = %d %x", c, k)
	}
	idx, cell, ksID := SplitGeoKey(GeoKey(nil, 4, 0x89c25a0000000001, id))
	if idx != 4 || cell != 0x89c25a0000000001 || !bytes.Equal(ksID, id) {
		t.Errorf("SplitGeoKey = %d %x %x", idx, cell, ksID)
	}
	if i, c, k := SplitGeoKey(make([]byte, 11)); i != 0 || c != 0 || k != nil {
		t.Errorf("SplitGeoKey(short) = %d %x %x", i, c, k)
	}
	kind, lat, lng, err := DecodeGeoValue(GeoPointValue(nil, -33.8688, 151.2093))
	if err != nil || kind != GeoPoint || lat != -33.8688 || lng != 151.2093 {
		t.Errorf("DecodeGeoValue(point) = %d %v %v %v", kind, lat, lng, err)
	}
	if kind, _, _, err := DecodeGeoValue(GeoShapeValue(nil)); err != nil || kind != GeoShape {
		t.Errorf("DecodeGeoValue(shape) = %d %v", kind, err)
	}
	for _, v := range [][]byte{nil, {GeoPoint}, {GeoShape, 0}, {0x03}, make([]byte, 17)} {
		if _, _, _, err := DecodeGeoValue(v); !errors.Is(err, ErrCorrupt) {
			t.Errorf("DecodeGeoValue(%x) err = %v", v, err)
		}
	}
	doc := kt.Doc(kt.F("a", kt.Int32(1)))
	got, err := DecodeColValue(ColValue(nil, doc))
	if err != nil || got.Type != doc.Type || !bytes.Equal(got.Value, doc.Value) {
		t.Errorf("DecodeColValue = %v %v", got, err)
	}
	if _, err := DecodeColValue(nil); !errors.Is(err, ErrCorrupt) {
		t.Errorf("DecodeColValue(nil) err = %v", err)
	}

	// Every key of an id lies in IDBounds(id), and no key of a neighbour.
	lo, hi := IDBounds(0x000000ff)
	if hex.EncodeToString(lo) != "000000ff" || hex.EncodeToString(hi) != "00000100" {
		t.Errorf("IDBounds = %x %x", lo, hi)
	}
	maxKS := ks(t, kt.MaxKey())
	for _, k := range [][]byte{DocKey(nil, 0xff, maxKS), IdxKey(nil, 0xff, maxKS, maxKS), GeoKey(nil, 0xff, math.MaxUint64, maxKS), ColKey(nil, 0xff, nil)} {
		if bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) >= 0 {
			t.Errorf("key %x outside IDBounds", k)
		}
	}
	for _, k := range [][]byte{DocKey(nil, 0xfe, maxKS), DocKey(nil, 0x100, nil)} {
		if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
			t.Errorf("neighbour key %x inside IDBounds", k)
		}
	}
	if _, hi := IDBounds(math.MaxUint32); hi != nil {
		t.Errorf("IDBounds(max) hi = %x", hi)
	}
	if next := keyenc.Next(IDPrefix(nil, 7)); !bytes.Equal(next, IDPrefix(nil, 8)) {
		t.Errorf("keyenc.Next(IDPrefix(7)) = %x", next)
	}
}

func TestCatalogKeys(t *testing.T) {
	if ns, ok := SplitCollKey(CollKey("shop.orders")); !ok || ns != "shop.orders" {
		t.Errorf("SplitCollKey = %q %v", ns, ok)
	}
	if _, ok := SplitCollKey(IndexKey(1, "x")); ok {
		t.Errorf("SplitCollKey accepted an index key")
	}
	coll, name, ok := SplitIndexKey(IndexKey(7, "loc_2dsphere"))
	if !ok || coll != 7 || name != "loc_2dsphere" {
		t.Errorf("SplitIndexKey = %d %q %v", coll, name, ok)
	}
	for _, k := range [][]byte{CollKey("abcd"), {IndexTag, 0, 0, 0}, nil} {
		if _, _, ok := SplitIndexKey(k); ok {
			t.Errorf("SplitIndexKey(%x) accepted", k)
		}
	}
	if !bytes.HasPrefix(IndexKey(7, "a_1"), IndexKey(7, "")) || bytes.HasPrefix(IndexKey(8, "a_1"), IndexKey(7, "")) {
		t.Errorf("IndexKey(coll, \"\") is not the per-collection prefix")
	}
	for _, n := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		got, err := DecodeCount(AppendCount(nil, n))
		if err != nil || got != n {
			t.Errorf("count round trip %d -> %d %v", n, got, err)
		}
	}
	if _, err := DecodeCount(make([]byte, 4)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("DecodeCount(short) err = %v", err)
	}
	// Catalog record kinds never collide with the layout marker or each
	// other.
	tags := map[byte]bool{LayoutMarkerKey[0]: true}
	for _, k := range [][]byte{CollKey(""), IndexKey(0, ""), CountKey(0), SeqKey("")} {
		if tags[k[0]] {
			t.Errorf("tag %q reused", k[0])
		}
		tags[k[0]] = true
	}
}
