package server

// globMatch reports whether s matches the Redis glob pattern: '*' any run,
// '?' any byte, '[...]' a set with '^' negation and 'a-z' ranges, '\' to
// quote the next byte. nocase folds ASCII letters. CONFIG GET matches
// parameter names with it, case-insensitively.
func globMatch(pattern, s string, nocase bool) bool {
	lower := func(c byte) byte {
		if nocase && 'A' <= c && c <= 'Z' {
			return c + 'a' - 'A'
		}
		return c
	}
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if globMatch(pattern[1:], s[i:], nocase) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			pattern = pattern[1:]
			not := len(pattern) > 0 && pattern[0] == '^'
			if not {
				pattern = pattern[1:]
			}
			match := false
			c := lower(s[0])
			for len(pattern) > 0 && pattern[0] != ']' {
				switch {
				case pattern[0] == '\\' && len(pattern) >= 2:
					if lower(pattern[1]) == c {
						match = true
					}
					pattern = pattern[2:]
				case len(pattern) >= 3 && pattern[1] == '-':
					lo, hi := lower(pattern[0]), lower(pattern[2])
					if lo > hi {
						lo, hi = hi, lo
					}
					if lo <= c && c <= hi {
						match = true
					}
					pattern = pattern[3:]
				default:
					if lower(pattern[0]) == c {
						match = true
					}
					pattern = pattern[1:]
				}
			}
			if len(pattern) > 0 {
				pattern = pattern[1:] // the closing ']'
			}
			if not {
				match = !match
			}
			if !match {
				return false
			}
			s = s[1:]
		case '\\':
			if len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || lower(pattern[0]) != lower(s[0]) {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]
		}
	}
	return len(s) == 0
}
