package redis

// This file ports stringmatchlen from Redis 7.2 src/util.c, which Redis
// publishes under the BSD-3-Clause licence (see
// internal/geohash/LICENSE-REDIS-BSD.txt). KEYS and the MATCH option of
// SCAN, HSCAN, SSCAN and ZSCAN match with it.

// maxGlobNesting is the recursion depth past which stringmatchlen gives
// up and reports no match, Redis's guard against patterns such as
// "*a*a*a*...*b".
const maxGlobNesting = 1000

// globMatch reports whether s matches the glob pattern: '*' matches any
// run of bytes, '?' any single byte, "[...]" a set with '^' negation and
// "a-z" ranges, and '\' quotes the next byte. An unterminated '[' treats
// the end of the pattern as ']'. nocase folds ASCII letters.
func globMatch(pattern, s []byte, nocase bool) bool {
	skipLonger := false
	return globMatchImpl(pattern, s, nocase, &skipLonger, 0)
}

func globMatchImpl(pattern, s []byte, nocase bool, skipLonger *bool, nesting int) bool {
	if nesting > maxGlobNesting {
		return false
	}
	p := 0
	for p < len(pattern) && len(s) > 0 {
		switch pattern[p] {
		case '*':
			for p+1 < len(pattern) && pattern[p+1] == '*' {
				p++
			}
			if p+1 == len(pattern) {
				return true
			}
			for len(s) > 0 {
				if globMatchImpl(pattern[p+1:], s, nocase, skipLonger, nesting+1) {
					return true
				}
				if *skipLonger {
					return false
				}
				s = s[1:]
			}
			// The rest of the pattern matches nowhere in the rest of s, so
			// no earlier '*' can help by matching more bytes.
			*skipLonger = true
			return false
		case '?':
			s = s[1:]
		case '[':
			p++
			not := p < len(pattern) && pattern[p] == '^'
			if not {
				p++
			}
			p = globSet(pattern, p, s[0], nocase, not)
			if p < 0 {
				return false
			}
			s = s[1:]
		case '\\':
			if len(pattern)-p >= 2 {
				p++
			}
			if !sameByte(pattern[p], s[0], nocase) {
				return false
			}
			s = s[1:]
		default:
			if !sameByte(pattern[p], s[0], nocase) {
				return false
			}
			s = s[1:]
		}
		p++
		if len(s) == 0 {
			for p < len(pattern) && pattern[p] == '*' {
				p++
			}
			break
		}
	}
	return p == len(pattern) && len(s) == 0
}

// globSet matches c against the set that starts at pattern[p], just after
// "[" or "[^". It returns the index of the closing ']' (or of the last
// pattern byte when the set is unterminated, so the caller's p++ reaches
// the end), or -1 when c does not match.
func globSet(pattern []byte, p int, c byte, nocase, not bool) int {
	match := false
	for {
		rest := len(pattern) - p
		if rest >= 2 && pattern[p] == '\\' {
			p++
			if pattern[p] == c {
				match = true
			}
		} else if rest > 0 && pattern[p] == ']' {
			break
		} else if rest == 0 {
			p--
			break
		} else if rest >= 3 && pattern[p+1] == '-' {
			// C compares these as char, which is signed on x86-64 and on
			// Apple arm64, so bytes from 0x80 up sort below ASCII.
			start, end, cc := int8(pattern[p]), int8(pattern[p+2]), int8(c)
			if start > end {
				start, end = end, start
			}
			if nocase {
				start, end, cc = int8(lower(byte(start))), int8(lower(byte(end))), int8(lower(byte(cc)))
			}
			p += 2
			if cc >= start && cc <= end {
				match = true
			}
		} else if sameByte(pattern[p], c, nocase) {
			match = true
		}
		p++
	}
	if not {
		match = !match
	}
	if !match {
		return -1
	}
	return p
}

func sameByte(a, b byte, nocase bool) bool {
	return a == b || (nocase && lower(a) == lower(b))
}

// lower is C's tolower in the C locale.
func lower(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

// globPrefix returns the literal bytes a pattern starts with, before its
// first '*', '?', '[' or '\'. Every key the pattern matches begins with
// them, so KEYS seeks there instead of scanning the whole database.
func globPrefix(pattern []byte) []byte {
	for i, c := range pattern {
		switch c {
		case '*', '?', '[', '\\':
			return pattern[:i]
		}
	}
	return pattern
}
