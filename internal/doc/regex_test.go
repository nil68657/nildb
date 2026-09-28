package doc

import (
	"strings"
	"testing"
)

func TestRegexPrefix(t *testing.T) {
	cases := []struct {
		pattern, options, want string
	}{
		{"^abc", "", "abc"},
		{"^abc.*", "", "abc"},
		{`\Aabc`, "", "abc"},
		{`\Aabc`, "m", "abc"},
		{"^abc", "m", ""},
		{"^abc", "i", ""},
		{"^abc", "x", ""},
		{"^abc", "s", "abc"},
		{"abc", "", ""},
		{"^ab|cd", "", ""},
		{"^a[|]b", "", "a"},
		{`^a\.b`, "", "a.b"},
		{`^a\db`, "", "a"},
		{"^abc?", "", "ab"},
		{"^abc*", "", "ab"},
		{"^abc{2}", "", "ab"},
		{"^abc+", "", "abc"},
		{"^ab(c)", "", "ab"},
		{"^héllo", "", "héllo"},
		{"^hé?", "", "h"},
		{`^\Qa.b\Ec`, "", "a.bc"},
		{`^\Qa.b`, "", "a.b"},
		{"^", "", ""},
		{"^$", "", ""},
	}
	for _, c := range cases {
		if got := regexPrefix(c.pattern, c.options); got != c.want {
			t.Errorf("regexPrefix(%q, %q) = %q, want %q", c.pattern, c.options, got, c.want)
		}
	}
}

func TestCompileRegex(t *testing.T) {
	ok := []struct{ pattern, options, subject string }{
		{"^ABC$", "i", "abc"},
		{"a.c", "s", "a\nc"},
		{"^c", "m", "ab\nc"},
		{"a  b # tail", "x", "ab"},
		{`a\#b`, "x", "a#b"},
		{"[ ]", "x", " "},
		{"[^ ]x", "x", "ax"},
		{"[]a]", "x", "]"},
		{"é", "u", "café"},
		{"(?P<n>a)b", "", "ab"},
	}
	for _, c := range ok {
		re, err := compileRegex(c.pattern, c.options)
		if err != nil {
			t.Errorf("compileRegex(%q, %q): %v", c.pattern, c.options, err)
			continue
		}
		if !re.MatchString(c.subject) {
			t.Errorf("%q /%s does not match %q", c.pattern, c.options, c.subject)
		}
	}
	bad := []struct{ pattern, options, want string }{
		{"(", "", "ERR Regular expression is invalid: "},
		{`(a)\1`, "", "ERR Regular expression is invalid: "},
		{"a(?<=b)", "", "ERR Regular expression is invalid: "},
		{"a", "q", "ERR invalid flag in regex options: q"},
		{"a\x00b", "", "ERR Regular expression cannot contain an embedded null byte"},
	}
	for _, c := range bad {
		_, err := compileRegex(c.pattern, c.options)
		if err == nil || !strings.HasPrefix(err.Error(), c.want) {
			t.Errorf("compileRegex(%q, %q) = %v, want %q", c.pattern, c.options, err, c.want)
		}
	}
}
