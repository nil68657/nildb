// This file is a Go port of fpconv_dtoa.c and fpconv_powers.h from Redis
// 7.2 (deps/fpconv): Florian Loitsch's Grisu2, as Redis uses it to print
// doubles. The copyright notices and the Boost Software License 1.0 that
// cover it are in LICENSE-FPCONV-BOOST.txt in this directory.
//
// Grisu2 is not always shortest: for about one double in a thousand it
// picks a longer or differently rounded digit string (Redis prints
// 9317.81304 as "9317.813039999999"). Porting it rather than using
// strconv's shortest digits keeps ZSCORE and friends byte-identical.

package resp

import (
	"math"
	"math/big"
)

// diyFP is fpconv's Fp: frac * 2^exp.
type diyFP struct {
	frac uint64
	exp  int
}

const (
	fracMask   = 0x000FFFFFFFFFFFFF
	expMask    = 0x7FF0000000000000
	hiddenBit  = 0x0010000000000000
	expBias    = 1023 + 52
	nPowers    = 87
	stepPowers = 8
	firstPower = -348
	expMaxPow  = -32
	expMinPow  = -60
)

var tens = [20]uint64{
	10000000000000000000, 1000000000000000000, 100000000000000000,
	10000000000000000, 1000000000000000, 100000000000000,
	10000000000000, 1000000000000, 100000000000,
	10000000000, 1000000000, 100000000,
	10000000, 1000000, 100000,
	10000, 1000, 100,
	10, 1,
}

// powersTen holds 10^k for k = -348, -340, ..., 340 as a 64-bit mantissa
// with its top bit set, rounded to nearest, times 2^exp. It is the table
// in fpconv_powers.h, computed here rather than copied.
var powersTen = func() (t [nPowers]diyFP) {
	for i := range t {
		t[i] = pow10DiyFP(firstPower + i*stepPowers)
	}
	return t
}()

func pow10DiyFP(k int) diyFP {
	num, den := big.NewInt(1), big.NewInt(1)
	p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(k))), nil)
	if k >= 0 {
		num = p
	} else {
		den = p
	}
	e := num.BitLen() - den.BitLen() - 64
	for {
		n, d := new(big.Int).Set(num), new(big.Int).Set(den)
		if e >= 0 {
			d.Lsh(d, uint(e))
		} else {
			n.Lsh(n, uint(-e))
		}
		q, r := new(big.Int).QuoRem(n, d, new(big.Int))
		if c := r.Lsh(r, 1).Cmp(d); c > 0 || (c == 0 && q.Bit(0) == 1) {
			q.Add(q, big.NewInt(1))
		}
		switch {
		case q.BitLen() > 64:
			e++
		case q.BitLen() < 64:
			e--
		default:
			return diyFP{frac: q.Uint64(), exp: e}
		}
	}
}

func buildFP(d float64) diyFP {
	bits := math.Float64bits(d)
	f := diyFP{frac: bits & fracMask, exp: int((bits & expMask) >> 52)}
	if f.exp != 0 {
		f.frac += hiddenBit
		f.exp -= expBias
	} else {
		f.exp = -expBias + 1
	}
	return f
}

func normalizeFP(f *diyFP) {
	for f.frac&hiddenBit == 0 {
		f.frac <<= 1
		f.exp--
	}
	const shift = 64 - 52 - 1
	f.frac <<= shift
	f.exp -= shift
}

func normalizedBoundaries(f diyFP) (lower, upper diyFP) {
	upper.frac = f.frac<<1 + 1
	upper.exp = f.exp - 1
	for upper.frac&(hiddenBit<<1) == 0 {
		upper.frac <<= 1
		upper.exp--
	}
	const uShift = 64 - 52 - 2
	upper.frac <<= uShift
	upper.exp -= uShift

	lShift := 1
	if f.frac == hiddenBit {
		lShift = 2
	}
	lower.frac = f.frac<<lShift - 1
	lower.exp = f.exp - lShift
	lower.frac <<= uint(lower.exp - upper.exp)
	lower.exp = upper.exp
	return lower, upper
}

func multiplyFP(a, b diyFP) diyFP {
	const lomask = 0x00000000FFFFFFFF
	ahbl := (a.frac >> 32) * (b.frac & lomask)
	albh := (a.frac & lomask) * (b.frac >> 32)
	albl := (a.frac & lomask) * (b.frac & lomask)
	ahbh := (a.frac >> 32) * (b.frac >> 32)
	tmp := (ahbl & lomask) + (albh & lomask) + (albl >> 32)
	tmp += 1 << 31 // round up
	return diyFP{frac: ahbh + ahbl>>32 + albh>>32 + tmp>>32, exp: a.exp + b.exp + 64}
}

func cachedPow10(exp int) (diyFP, int) {
	const oneLogTen = 0.30102999566398114
	approx := int(float64(-(exp + nPowers)) * oneLogTen)
	idx := (approx - firstPower) / stepPowers
	for {
		current := exp + powersTen[idx].exp + 64
		if current < expMinPow {
			idx++
			continue
		}
		if current > expMaxPow {
			idx--
			continue
		}
		return powersTen[idx], firstPower + idx*stepPowers
	}
}

func roundDigit(digits []byte, n int, delta, rem, kappa, frac uint64) {
	for rem < frac && delta-rem >= kappa && (rem+kappa < frac || frac-rem > rem+kappa-frac) {
		digits[n-1]--
		rem += kappa
	}
}

func generateDigits(w, upper, lower diyFP, digits []byte, k *int) int {
	wfrac := upper.frac - w.frac
	delta := upper.frac - lower.frac
	shift := uint(-upper.exp)
	one := uint64(1) << shift
	part1 := upper.frac >> shift
	part2 := upper.frac & (one - 1)

	idx, kappa := 0, 10
	for divp := 10; kappa > 0; divp++ {
		div := tens[divp]
		digit := part1 / div
		if digit != 0 || idx != 0 {
			digits[idx] = byte(digit) + '0'
			idx++
		}
		part1 -= digit * div
		kappa--
		if tmp := part1<<shift + part2; tmp <= delta {
			*k += kappa
			roundDigit(digits, idx, delta, tmp, div<<shift, wfrac)
			return idx
		}
	}
	unit := 18
	for {
		part2 *= 10
		delta *= 10
		kappa--
		digit := part2 >> shift
		if digit != 0 || idx != 0 {
			digits[idx] = byte(digit) + '0'
			idx++
		}
		part2 &= one - 1
		if part2 < delta {
			*k += kappa
			roundDigit(digits, idx, delta, part2, one, wfrac*tens[unit])
			return idx
		}
		unit--
	}
}

// grisu2 writes the decimal digits of |d| into digits and returns their
// count n and the power k with |d| ≈ digits * 10^k. d must be finite and
// nonzero.
func grisu2(d float64, digits *[18]byte) (n, k int) {
	w := buildFP(d)
	lower, upper := normalizedBoundaries(w)
	normalizeFP(&w)
	cp, ck := cachedPow10(upper.exp)
	w = multiplyFP(w, cp)
	upper = multiplyFP(upper, cp)
	lower = multiplyFP(lower, cp)
	lower.frac++
	upper.frac--
	k = -ck
	n = generateDigits(w, upper, lower, digits[:], &k)
	return n, k
}
