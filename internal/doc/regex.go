package doc

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// compileRegex compiles a MongoDB regular expression with Go's RE2 engine.
// Options: i (case-insensitive), m (^ and $ match at line breaks), s (dot
// matches newline), x (extended: unescaped whitespace and #-comments outside
// character classes are dropped), u (accepted; RE2 is always Unicode-aware).
// RE2 has no backreferences or lookaround; such patterns fail with
// MongoDB's "Regular expression is invalid" text.
func compileRegex(pattern, options string) (*regexp.Regexp, error) {
	var flags strings.Builder
	extended := false
	for _, o := range options {
		switch o {
		case 'i', 'm', 's':
			if !strings.ContainsRune(flags.String(), o) {
				flags.WriteRune(o)
			}
		case 'x':
			extended = true
		case 'u':
		default:
			return nil, errf(CodeInvalidRegexOptions, "invalid flag in regex options: %c", o)
		}
	}
	if strings.IndexByte(pattern, 0) >= 0 {
		return nil, errf(CodeInvalidRegex, "Regular expression cannot contain an embedded null byte")
	}
	src := pattern
	if extended {
		src = stripExtended(src)
	}
	if flags.Len() > 0 {
		src = "(?" + flags.String() + ")" + src
	}
	re, err := regexp.Compile(src)
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "error parsing regexp: ")
		return nil, errf(CodeInvalidRegex, "Regular expression is invalid: %s", msg)
	}
	return re, nil
}

// stripExtended applies PCRE's x option: whitespace and comments from # to
// the end of the line are removed outside character classes, and an escaped
// whitespace character stands for itself.
func stripExtended(p string) string {
	var b strings.Builder
	inClass := false
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '\\' && i+1 < len(p):
			n := p[i+1]
			if isSpace(n) {
				// RE2 reads whitespace literally, so the escaped
				// character is written bare.
				b.WriteByte(n)
			} else {
				b.WriteByte(c)
				b.WriteByte(n)
			}
			i++
		case inClass:
			if c == ']' {
				inClass = false
			}
			b.WriteByte(c)
		case c == '[':
			inClass = true
			b.WriteByte(c)
			// A ']' right after '[' or '[^' is a literal member.
			if i+1 < len(p) && p[i+1] == '^' {
				b.WriteByte('^')
				i++
			}
			if i+1 < len(p) && p[i+1] == ']' {
				b.WriteByte(']')
				i++
			}
		case isSpace(c):
		case c == '#':
			for i+1 < len(p) && p[i+1] != '\n' {
				i++
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// regexPrefix returns the literal prefix every string matching the regex
// must start with, for index bounds. It returns "" unless the pattern is
// anchored at the start of the string (^ without the m option, or \A),
// uses neither the i nor the x option, and has no alternation.
func regexPrefix(pattern, options string) string {
	if strings.ContainsAny(options, "ix") {
		return ""
	}
	var rest string
	switch {
	case strings.HasPrefix(pattern, `\A`):
		rest = pattern[2:]
	case strings.HasPrefix(pattern, "^") && !strings.Contains(options, "m"):
		rest = pattern[1:]
	default:
		return ""
	}
	if hasAlternation(pattern) {
		return ""
	}
	var b strings.Builder
	lastLen := 0 // bytes of the last literal written, dropped before ?, * or {
	for len(rest) > 0 {
		c := rest[0]
		switch {
		case c == '\\' && len(rest) >= 2 && rest[1] == 'Q':
			end := strings.Index(rest[2:], `\E`)
			lit := rest[2:]
			if end >= 0 {
				lit = rest[2 : 2+end]
				rest = rest[2+end+2:]
			} else {
				rest = ""
			}
			if lit == "" {
				continue
			}
			b.WriteString(lit)
			_, lastLen = utf8.DecodeLastRuneInString(lit)
			continue
		case c == '\\':
			if len(rest) < 2 {
				return trimLast(b.String(), 0)
			}
			n := rest[1]
			if n < 0x80 && !isPunct(n) {
				// \d, \w, \n and friends are classes or escapes, not
				// literals: stop.
				return b.String()
			}
			b.WriteByte(n)
			lastLen = 1
			rest = rest[2:]
		case strings.IndexByte(".[]()*+?{}|^$", c) >= 0:
			if c == '*' || c == '?' || c == '{' {
				return trimLast(b.String(), lastLen)
			}
			return b.String()
		default:
			r, size := utf8.DecodeRuneInString(rest)
			if r == utf8.RuneError && size <= 1 {
				return b.String()
			}
			b.WriteString(rest[:size])
			lastLen = size
			rest = rest[size:]
		}
	}
	return b.String()
}

func trimLast(s string, n int) string {
	if n > len(s) {
		return ""
	}
	return s[:len(s)-n]
}

func isPunct(c byte) bool {
	return (c >= '!' && c <= '/') || (c >= ':' && c <= '@') || (c >= '[' && c <= '`') || (c >= '{' && c <= '~')
}

// hasAlternation reports an unescaped '|' outside character classes.
func hasAlternation(p string) bool {
	inClass := false
	for i := 0; i < len(p); i++ {
		switch c := p[i]; {
		case c == '\\':
			i++
		case inClass:
			if c == ']' {
				inClass = false
			}
		case c == '[':
			inClass = true
		case c == '|':
			return true
		}
	}
	return false
}
