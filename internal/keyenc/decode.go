package keyenc

import (
	"encoding/binary"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Decode reads one ascending encoding from the front of b and returns a
// value that encodes to the same bytes, plus the bytes after it. The value
// is freshly allocated and does not alias b.
//
// Information the encoding folds away does not come back. A number decodes
// as int32 when it is an integer that fits, as int64 when it is an integer
// below 2^63 in magnitude, and as a double otherwise (NaN, fractions,
// infinities, magnitudes of 2^63 and above). Symbol decodes as String, a
// missing field as Null.
func Decode(b []byte) (v bson.RawValue, rest []byte, err error) {
	return decode(b, 0x00)
}

// DecodeDesc reads one EncodeDesc output from the front of b.
func DecodeDesc(b []byte) (bson.RawValue, []byte, error) {
	return decode(b, 0xFF)
}

// Skip returns the bytes after the ascending encoding at the front of b
// without building the value, for example to reach the ks(_id) at the end of
// an index entry.
func Skip(b []byte) ([]byte, error) {
	return skip(b, 0x00)
}

// SkipDesc is Skip for an EncodeDesc output.
func SkipDesc(b []byte) ([]byte, error) {
	return skip(b, 0xFF)
}

func decode(b []byte, x byte) (bson.RawValue, []byte, error) {
	r := reader{b: b, x: x}
	tb, ok := r.readByte()
	if !ok {
		return bson.RawValue{}, b, ErrCorrupt
	}
	t, val, err := r.decodePayload(tb, nil, 0)
	if err != nil {
		return bson.RawValue{}, b, err
	}
	return bson.RawValue{Type: t, Value: val}, b[r.i:], nil
}

func skip(b []byte, x byte) ([]byte, error) {
	r := reader{b: b, x: x}
	if err := r.skipValue(0); err != nil {
		return b, err
	}
	return b[r.i:], nil
}

// reader walks an encoding, XORing every byte with x (0x00 ascending, 0xFF
// descending) as it reads.
type reader struct {
	b []byte
	i int
	x byte
}

func (r *reader) readByte() (byte, bool) {
	if r.i >= len(r.b) {
		return 0, false
	}
	c := r.b[r.i] ^ r.x
	r.i++
	return c, true
}

func (r *reader) peekByte() (byte, bool) {
	if r.i >= len(r.b) {
		return 0, false
	}
	return r.b[r.i] ^ r.x, true
}

func (r *reader) expect(c byte) bool {
	got, ok := r.readByte()
	return ok && got == c
}

// appendN appends the next n bytes, un-XORed, to dst.
func (r *reader) appendN(dst []byte, n int) ([]byte, bool) {
	if n < 0 || n > len(r.b)-r.i {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, r.b[r.i:r.i+n]...)
	if r.x != 0 {
		invert(dst[start:])
	}
	r.i += n
	return dst, true
}

func (r *reader) uint64() (uint64, bool) {
	if len(r.b)-r.i < 8 {
		return 0, false
	}
	u := binary.BigEndian.Uint64(r.b[r.i:])
	r.i += 8
	if r.x != 0 {
		u = ^u
	}
	return u, true
}

func (r *reader) uint32() (uint32, bool) {
	if len(r.b)-r.i < 4 {
		return 0, false
	}
	u := binary.BigEndian.Uint32(r.b[r.i:])
	r.i += 4
	if r.x != 0 {
		u = ^u
	}
	return u, true
}

// appendUnescaped appends an escaped string's contents to dst and consumes
// its terminator.
func (r *reader) appendUnescaped(dst []byte) ([]byte, bool) {
	for {
		c, ok := r.readByte()
		if !ok {
			return dst, false
		}
		if c != 0 {
			dst = append(dst, c)
			continue
		}
		if n, ok := r.peekByte(); ok && n == 0xFF {
			r.i++
			dst = append(dst, 0)
			continue
		}
		return dst, true
	}
}

// appendCStr appends the bytes up to the next 0x00 and consumes that 0x00.
// Field names, regex parts and DBPointer namespaces hold no 0x00, so their
// escaping never fires, and the byte after the terminator may be 0xFF (a
// payload or an ObjectId), which appendUnescaped would misread as an escape.
func (r *reader) appendCStr(dst []byte) ([]byte, bool) {
	for {
		c, ok := r.readByte()
		if !ok {
			return dst, false
		}
		if c == 0 {
			return dst, true
		}
		dst = append(dst, c)
	}
}

// appendString decodes a string into a BSON string value (int32 length,
// bytes, NUL) appended to dst. cstr selects appendCStr over appendUnescaped.
func (r *reader) appendString(dst []byte, cstr bool) ([]byte, bool) {
	at := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	var ok bool
	if cstr {
		dst, ok = r.appendCStr(dst)
	} else {
		dst, ok = r.appendUnescaped(dst)
	}
	if !ok {
		return dst, false
	}
	dst = append(dst, 0)
	binary.LittleEndian.PutUint32(dst[at:], uint32(len(dst)-at-4))
	return dst, true
}

// appendCString decodes a field name or regex part into a BSON cstring.
func (r *reader) appendCString(dst []byte) ([]byte, bool) {
	dst, ok := r.appendCStr(dst)
	return append(dst, 0), ok
}

// decodePayload decodes the payload that follows type byte tb, appends the
// BSON value bytes to dst and reports the BSON type chosen.
func (r *reader) decodePayload(tb byte, dst []byte, depth int) (bson.Type, []byte, error) {
	switch tb {
	case TMinKey:
		return bson.TypeMinKey, dst, nil
	case TUndefined:
		return bson.TypeUndefined, dst, nil
	case TNull:
		return bson.TypeNull, dst, nil
	case TMaxKey:
		return bson.TypeMaxKey, dst, nil
	case TNumber:
		class, ok := r.readByte()
		if !ok {
			return 0, dst, ErrCorrupt
		}
		var p uint64
		switch numberPayloadLen(class) {
		case 0:
		case 8:
			if p, ok = r.uint64(); !ok {
				return 0, dst, ErrCorrupt
			}
		default:
			return 0, dst, ErrCorrupt
		}
		n, ok := decodeNumber(class, p)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		switch {
		case !n.isInt:
			return bson.TypeDouble, binary.LittleEndian.AppendUint64(dst, math.Float64bits(n.f)), nil
		case n.i == int64(int32(n.i)):
			return bson.TypeInt32, binary.LittleEndian.AppendUint32(dst, uint32(int32(n.i))), nil
		default:
			return bson.TypeInt64, binary.LittleEndian.AppendUint64(dst, uint64(n.i)), nil
		}
	case TString:
		dst, ok := r.appendString(dst, false)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeString, dst, nil
	case TObject:
		dst, err := r.decodeDoc(dst, depth+1)
		return bson.TypeEmbeddedDocument, dst, err
	case TArray:
		dst, err := r.decodeArray(dst, depth+1)
		return bson.TypeArray, dst, err
	case TBinData:
		n, ok := r.uint32()
		if !ok || n > math.MaxInt32 {
			return 0, dst, ErrCorrupt
		}
		dst = binary.LittleEndian.AppendUint32(dst, n)
		if dst, ok = r.appendN(dst, int(n)+1); !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeBinary, dst, nil
	case TOID:
		dst, ok := r.appendN(dst, 12)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeObjectID, dst, nil
	case TBool:
		c, ok := r.readByte()
		if !ok || c > 1 {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeBoolean, append(dst, c), nil
	case TDate:
		u, ok := r.uint64()
		if !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeDateTime, binary.LittleEndian.AppendUint64(dst, u^1<<63), nil
	case TTimestamp:
		u, ok := r.uint64()
		if !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeTimestamp, binary.LittleEndian.AppendUint64(dst, u), nil
	case TRegex:
		dst, ok := r.appendCString(dst)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		if dst, ok = r.appendCString(dst); !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeRegex, dst, nil
	case TDBPointer:
		if !r.expect(TString) {
			return 0, dst, ErrCorrupt
		}
		dst, ok := r.appendString(dst, true)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		if dst, ok = r.appendN(dst, 12); !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeDBPointer, dst, nil
	case TCode:
		if !r.expect(TString) {
			return 0, dst, ErrCorrupt
		}
		dst, ok := r.appendString(dst, false)
		if !ok {
			return 0, dst, ErrCorrupt
		}
		return bson.TypeJavaScript, dst, nil
	case TCodeWScope:
		at := len(dst)
		dst = append(dst, 0, 0, 0, 0)
		if !r.expect(TString) {
			return 0, dst, ErrCorrupt
		}
		dst, ok := r.appendString(dst, false)
		if !ok || !r.expect(TObject) {
			return 0, dst, ErrCorrupt
		}
		dst, err := r.decodeDoc(dst, depth+1)
		if err != nil {
			return 0, dst, err
		}
		binary.LittleEndian.PutUint32(dst[at:], uint32(len(dst)-at))
		return bson.TypeCodeWithScope, dst, nil
	}
	return 0, dst, ErrCorrupt
}

// decodeDoc decodes an object payload into a BSON document appended to dst.
func (r *reader) decodeDoc(dst []byte, depth int) ([]byte, error) {
	if depth > maxDepth {
		return dst, ErrCorrupt
	}
	at := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	for {
		tb, ok := r.readByte()
		if !ok {
			return dst, ErrCorrupt
		}
		if tb == 0 {
			break
		}
		typeAt := len(dst)
		dst = append(dst, 0)
		if dst, ok = r.appendCString(dst); !ok {
			return dst, ErrCorrupt
		}
		t, out, err := r.decodePayload(tb, dst, depth)
		if err != nil {
			return out, err
		}
		dst = out
		dst[typeAt] = byte(t)
	}
	dst = append(dst, 0)
	binary.LittleEndian.PutUint32(dst[at:], uint32(len(dst)-at))
	return dst, nil
}

// decodeArray decodes an array payload into a BSON array (a document keyed
// "0", "1", ...) appended to dst.
func (r *reader) decodeArray(dst []byte, depth int) ([]byte, error) {
	if depth > maxDepth {
		return dst, ErrCorrupt
	}
	at := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	for idx := 0; ; idx++ {
		tb, ok := r.readByte()
		if !ok {
			return dst, ErrCorrupt
		}
		if tb == 0 {
			break
		}
		typeAt := len(dst)
		dst = append(dst, 0)
		dst = strconv.AppendInt(dst, int64(idx), 10)
		dst = append(dst, 0)
		t, out, err := r.decodePayload(tb, dst, depth)
		if err != nil {
			return out, err
		}
		dst = out
		dst[typeAt] = byte(t)
	}
	dst = append(dst, 0)
	binary.LittleEndian.PutUint32(dst[at:], uint32(len(dst)-at))
	return dst, nil
}

// skipValue consumes one full encoding (type byte and payload).
func (r *reader) skipValue(depth int) error {
	tb, ok := r.readByte()
	if !ok {
		return ErrCorrupt
	}
	return r.skipPayload(tb, depth)
}

func (r *reader) skipN(n int) bool {
	if n < 0 || n > len(r.b)-r.i {
		return false
	}
	r.i += n
	return true
}

func (r *reader) skipEscaped() bool {
	for {
		c, ok := r.readByte()
		if !ok {
			return false
		}
		if c != 0 {
			continue
		}
		if n, ok := r.peekByte(); ok && n == 0xFF {
			r.i++
			continue
		}
		return true
	}
}

func (r *reader) skipCStr() bool {
	for {
		c, ok := r.readByte()
		if !ok {
			return false
		}
		if c == 0 {
			return true
		}
	}
}

func (r *reader) skipPayload(tb byte, depth int) error {
	ok := true
	switch tb {
	case TMinKey, TUndefined, TNull, TMaxKey:
	case TNumber:
		var class byte
		if class, ok = r.readByte(); ok {
			n := numberPayloadLen(class)
			ok = n >= 0 && r.skipN(n)
		}
	case TString:
		ok = r.skipEscaped()
	case TObject:
		if depth+1 > maxDepth {
			return ErrCorrupt
		}
		for {
			ftb, more := r.readByte()
			if !more {
				return ErrCorrupt
			}
			if ftb == 0 {
				return nil
			}
			if !r.skipCStr() {
				return ErrCorrupt
			}
			if err := r.skipPayload(ftb, depth+1); err != nil {
				return err
			}
		}
	case TArray:
		if depth+1 > maxDepth {
			return ErrCorrupt
		}
		for {
			c, more := r.peekByte()
			if !more {
				return ErrCorrupt
			}
			if c == 0 {
				r.i++
				return nil
			}
			if err := r.skipValue(depth + 1); err != nil {
				return err
			}
		}
	case TBinData:
		var n uint32
		if n, ok = r.uint32(); ok {
			ok = n <= math.MaxInt32 && r.skipN(int(n)+1)
		}
	case TOID:
		ok = r.skipN(12)
	case TBool:
		ok = r.skipN(1)
	case TDate, TTimestamp:
		ok = r.skipN(8)
	case TRegex:
		ok = r.skipCStr() && r.skipCStr()
	case TDBPointer:
		ok = r.expect(TString) && r.skipCStr() && r.skipN(12)
	case TCode:
		ok = r.expect(TString) && r.skipEscaped()
	case TCodeWScope:
		if !r.expect(TString) || !r.skipEscaped() || !r.expect(TObject) {
			return ErrCorrupt
		}
		return r.skipPayload(TObject, depth)
	default:
		return ErrCorrupt
	}
	if !ok {
		return ErrCorrupt
	}
	return nil
}
