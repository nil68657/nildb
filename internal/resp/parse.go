package resp

import (
	"math"
	"strconv"
	"strings"
)

// maxFloatLen is MAX_LONG_DOUBLE_CHARS (5 KiB); string2ld refuses input of
// this length or longer.
const maxFloatLen = 5 << 10

// ParseInt is string2ll from Redis 7.2 util.c, the parser behind every
// integer argument: base 10, an optional leading '-', no '+', no leading
// zeros, no "-0", no whitespace, fewer than 21 bytes, and within int64.
func ParseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) >= 21 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	neg, p := false, 0
	if b[0] == '-' {
		neg, p = true, 1
		if p == len(b) {
			return 0, false
		}
	}
	if b[p] < '1' || b[p] > '9' {
		return 0, false
	}
	v := uint64(b[p] - '0')
	for p++; p < len(b); p++ {
		c := b[p]
		if c < '0' || c > '9' {
			return 0, false
		}
		if v > math.MaxUint64/10 {
			return 0, false
		}
		v *= 10
		if v > math.MaxUint64-uint64(c-'0') {
			return 0, false
		}
		v += uint64(c - '0')
	}
	if neg {
		if v > 1<<63 {
			return 0, false
		}
		return -int64(v), true
	}
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

// ParseFloat accepts what string2ld in Redis 7.2 util.c accepts: the full
// strtold syntax (an optional sign, a decimal or 0x hexadecimal mantissa,
// an optional exponent, "inf" or "infinity" in any case) with nothing
// before or after it. It refuses the empty string, leading whitespace,
// NaN, input of 5 KiB or more, results that overflow to infinity, and
// nonzero input that underflows to zero. A denormal result is accepted, as
// strtold's ERANGE is ignored for it. Redis parses into a long double; on
// this platform that is a float64, which is what ParseFloat returns.
//
// "-0" returns negative zero; callers that store a score canonicalise it.
func ParseFloat(b []byte) (float64, bool) {
	if len(b) == 0 || len(b) >= maxFloatLen {
		return 0, false
	}
	neg := b[0] == '-'
	rest := b
	if b[0] == '-' || b[0] == '+' {
		rest = b[1:]
	}
	if len(rest) > 0 && rest[0]|0x20 == 'i' {
		if !equalFold(rest, "inf") && !equalFold(rest, "infinity") {
			return 0, false
		}
		if neg {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	}
	hex := len(rest) >= 2 && rest[0] == '0' && rest[1]|0x20 == 'x'
	digit, expChar := isDecDigit, byte('e')
	i := 0
	if hex {
		digit, expChar = isHex, 'p'
		i = 2
	}
	digits, nonzero := 0, false
	scan := func() {
		for i < len(rest) && digit(rest[i]) {
			if rest[i] != '0' {
				nonzero = true
			}
			i++
			digits++
		}
	}
	scan()
	if i < len(rest) && rest[i] == '.' {
		i++
		scan()
	}
	if digits == 0 {
		return 0, false
	}
	hasExp := false
	if i < len(rest) && rest[i]|0x20 == expChar {
		i++
		if i < len(rest) && (rest[i] == '+' || rest[i] == '-') {
			i++
		}
		start := i
		for i < len(rest) && isDecDigit(rest[i]) {
			i++
		}
		if i == start {
			return 0, false
		}
		hasExp = true
	}
	if i != len(rest) {
		return 0, false
	}
	s := string(b)
	if hex && !hasExp {
		// strtold takes a hexadecimal mantissa without an exponent;
		// strconv needs the 'p'.
		s += "p0"
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, false
	}
	if f == 0 && nonzero {
		return 0, false
	}
	return f, true
}

func isDecDigit(c byte) bool { return c >= '0' && c <= '9' }

// equalFold reports whether b equals the lowercase ASCII word w, ignoring
// ASCII case in b.
func equalFold(b []byte, w string) bool {
	if len(b) != len(w) {
		return false
	}
	for i := range len(b) {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != w[i] {
			return false
		}
	}
	return true
}

// FormatFloat returns f the way Redis 7.2 prints a double reply (d2string
// in util.c, used by addReplyDouble for ZSCORE, ZINCRBY, ZADD INCR, ZRANGE
// WITHSCORES and every other double reply, in both RESP2 and RESP3).
func FormatFloat(f float64) string {
	var tmp [32]byte
	return string(AppendFloat(tmp[:0], f))
}

// AppendFloat appends FormatFloat(f) to dst.
//
// The rules, from d2string and fpconv_dtoa's emit_digits: NaN is "nan",
// infinities are "inf" and "-inf", zero is "0" or "-0"; an integral value
// in [-2^62, 2^62] prints as an integer; anything else prints its Grisu2
// digits (grisu.go) as a plain integer when the decimal exponent is below
// the digit count plus 7, as a plain decimal when the value has fewer than
// 7 fractional positions or its exponent is between -3 and 3, and in
// scientific form "d.ddde+X" (no exponent padding) otherwise. Examples:
// 3.14, 0.30000000000000004, 1e+21, 1e-7, 9223372036854776000. The output
// always parses back to f.
func AppendFloat(dst []byte, f float64) []byte {
	switch {
	case math.IsNaN(f):
		return append(dst, "nan"...)
	case math.IsInf(f, 1):
		return append(dst, "inf"...)
	case math.IsInf(f, -1):
		return append(dst, "-inf"...)
	case f == 0:
		if math.Signbit(f) {
			return append(dst, "-0"...)
		}
		return append(dst, '0')
	}
	// double2ll: -LLONG_MAX/2 and LLONG_MAX/2 round to -2^62 and 2^62.
	if f >= -(1<<62) && f <= 1<<62 {
		if i := int64(f); float64(i) == f {
			return strconv.AppendInt(dst, i, 10)
		}
	}
	var digits [18]byte
	nd, k := grisu2(f, &digits)
	neg := f < 0
	if neg {
		dst = append(dst, '-')
	}
	return emitDigits(dst, digits[:nd], k, neg)
}

// FormatFloatHuman prints f the way INCRBYFLOAT and HINCRBYFLOAT store and
// return their result in Redis 7.2 (ld2string in LD_STR_HUMAN mode): plain
// decimal, never an exponent, at most 17 fractional digits, trailing zeros
// and a trailing '.' removed, "-0" printed as "0". Redis formats a long
// double with "%.17Lf"; with a float64 that would print 10.6 as
// 10.59999999999999964, so this prints the shortest round-trip digits and
// rounds to 17 fractional digits only when the shortest form has more.
// Results can still differ from Redis on x86-64 in the last digits,
// because Redis also does the addition in 80-bit precision (Redis there
// gives 0.3 for 0.1 plus 0.2; float64 gives 0.30000000000000004).
func FormatFloatHuman(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if dot := strings.IndexByte(s, '.'); dot >= 0 && len(s)-dot-1 > 17 {
		s = strconv.FormatFloat(f, 'f', 17, 64)
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		s = "0"
	}
	return s
}

// emitDigits lays out a digit string times 10^k with the thresholds of
// fpconv_dtoa's emit_digits. The scientific form keeps at most 18 digits,
// 17 for a negative number, as emit_digits does.
func emitDigits(dst, digits []byte, k int, neg bool) []byte {
	nd := len(digits)
	exp := abs(k + nd - 1)
	if k >= 0 && exp < nd+7 {
		dst = append(dst, digits...)
		for range k {
			dst = append(dst, '0')
		}
		return dst
	}
	if k < 0 && (k > -7 || exp < 4) {
		offset := nd + k
		if offset <= 0 {
			dst = append(dst, '0', '.')
			for range -offset {
				dst = append(dst, '0')
			}
			return append(dst, digits...)
		}
		dst = append(dst, digits[:offset]...)
		dst = append(dst, '.')
		return append(dst, digits[offset:]...)
	}
	limit := 18
	if neg {
		limit = 17
	}
	nd = min(nd, limit)
	dst = append(dst, digits[0])
	if nd > 1 {
		dst = append(dst, '.')
		dst = append(dst, digits[1:nd]...)
	}
	dst = append(dst, 'e')
	if k+nd-1 < 0 {
		dst = append(dst, '-')
	} else {
		dst = append(dst, '+')
	}
	cent := 0
	if exp > 99 {
		cent = exp / 100
		dst = append(dst, byte(cent)+'0')
		exp -= cent * 100
	}
	if exp > 9 {
		dec := exp / 10
		dst = append(dst, byte(dec)+'0')
		exp -= dec * 10
	} else if cent != 0 {
		dst = append(dst, '0')
	}
	return append(dst, byte(exp%10)+'0')
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
