package redis

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		nocase     bool
		want       bool
	}{
		{"*", "anything", false, true},
		{"*", "", false, false}, // stringmatchlen never enters its loop for an empty string
		{"", "", false, true},
		{"h?llo", "hello", false, true},
		{"h?llo", "hllo", false, false},
		{"h*llo", "hllo", false, true},
		{"h*llo", "heeeello", false, true},
		{"h*llo", "heeeellox", false, false},
		{"h[ae]llo", "hello", false, true},
		{"h[ae]llo", "hallo", false, true},
		{"h[ae]llo", "hillo", false, false},
		{"h[^e]llo", "hallo", false, true},
		{"h[^e]llo", "hello", false, false},
		{"h[a-b]llo", "hbllo", false, true},
		{"h[a-b]llo", "hcllo", false, false},
		{"h[b-a]llo", "hallo", false, true},
		{`h\*llo`, "h*llo", false, true},
		{`h\*llo`, "hello", false, false},
		{"[abc", "a", false, true},
		{"[abc", "d", false, false},
		{`a\`, `a\`, false, true},
		{`[\]]`, "]", false, true},
		{"[]]", "]", false, false},
		{"key:1??", "key:123", false, true},
		{"key:1??", "key:12", false, false},
		{"key:*", "key:", false, true},
		{"a*", "a", false, true},
		{"a**", "a", false, true},
		{"*b", "ab", false, true},
		{"HELLO", "hello", true, true},
		{"H[A-Z]LLO", "hello", true, true},
		{"HELLO", "hello", false, false},
		{"[a-\xff]", "b", false, false}, // C compares char, signed: 0xff is -1
		{"[a-\xff]", "A", false, true},
	}
	for _, tc := range cases {
		if got := globMatch([]byte(tc.pattern), []byte(tc.s), tc.nocase); got != tc.want {
			t.Errorf("globMatch(%q, %q, %v) = %v, want %v", tc.pattern, tc.s, tc.nocase, got, tc.want)
		}
	}
}

func TestGlobAbusivePatterns(t *testing.T) {
	// keyspace.tcl "Regression for pattern matching long nested loops".
	if globMatch([]byte("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b"), bytes.Repeat([]byte("a"), 40), false) {
		t.Error("nested stars matched")
	}
	// "... very long nested loops": the nesting guard gives up.
	if globMatch([]byte(strings.Repeat("*?", 50000)), bytes.Repeat([]byte("a"), 50000), false) {
		t.Error("50000 nested stars matched")
	}
}

func TestGlobPrefix(t *testing.T) {
	for pattern, want := range map[string]string{
		"user:*": "user:", "a?b": "a", "abc": "abc", `\*x`: "", "[ab]c": "", "": "",
	} {
		if got := string(globPrefix([]byte(pattern))); got != want {
			t.Errorf("globPrefix(%q) = %q, want %q", pattern, got, want)
		}
	}
}

func TestParseCursor(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		ok   bool
	}{
		{"0", 0, true}, {"", 0, true}, {"00", 0, true}, {"+5", 5, true}, {"-1", math.MaxUint64, true},
		{"18446744073709551615", math.MaxUint64, true}, {"18446744073709551616", 0, false},
		{" 1", 0, false}, {"1 ", 0, false}, {"abc", 0, false}, {"+", 0, false}, {"-", 0, false}, {"1.5", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseCursor([]byte(tc.in))
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("parseCursor(%q) = %d, %v, want %d, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestRandomKeyBetween(t *testing.T) {
	a, b := []byte("user:1000"), []byte("user:9")
	for range 200 {
		k := randomKeyBetween(a, b)
		if !bytes.HasPrefix(k, []byte("user:")) || k[5] < '1' || k[5] > '9' {
			t.Fatalf("randomKeyBetween = %q", k)
		}
	}
	if k := randomKeyBetween([]byte("x"), []byte("x")); string(k) != "x" {
		t.Errorf("single key: %q", k)
	}
	if k := randomKeyBetween([]byte("ab"), []byte("abc")); !bytes.HasPrefix(k, []byte("ab")) || len(k) != 10 {
		t.Errorf("prefix pair: %q", k)
	}
}
