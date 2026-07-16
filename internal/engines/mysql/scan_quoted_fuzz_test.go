// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Differential fuzz for scanQuotedStringDelim, the Bug-191 rewrite
// (audit 2026-07-16 M2.4). The rewrite split decoding into a pre-scan +
// an escape-free fast path / escape-aware slow path; the 1M-input
// differential that validated it pre-release was run locally and
// discarded, so this file commits the harness as a permanent gate — the
// seed corpus runs on every `go test`, and the scheduled fuzz job can
// point -fuzz at it for deep runs.
//
// The oracle is refScanQuotedString below: an INDEPENDENT decoder
// written directly from MySQL's documented string-literal grammar
// (escape table + doubled-delimiter rule), one byte at a time, with no
// pre-scan, no capacity math, and no shared helpers. Deliberately NOT
// the pre-Bug-191 sluice decoder: that implementation is the code this
// rewrite replaced, so differencing against it would pin sluice's own
// history (bugs included) rather than the grammar — an oracle must be
// derived from the spec, not from the previous implementation.
//
// Correctness of the oracle is anchored two ways: it is simple enough
// to check against the manual by eye (one switch, no state beyond the
// cursor), and TestScanQuotedStringDelim_Matrix pins both decoders'
// shared expectations against hand-derived byte values.

package mysql

import (
	"bytes"
	"testing"
)

// refScanQuotedString is the reference decoder: it decodes the
// delim-quoted literal at the start of s per MySQL's string-literal
// grammar and reports the index just past the closing delimiter.
// ok=false for a missing opener, a dangling backslash, or a literal
// with no closing delimiter — the same refusals the production scanner
// documents.
func refScanQuotedString(s string, delim byte) (raw []byte, end int, ok bool) {
	if s == "" || s[0] != delim {
		return nil, 0, false
	}
	out := []byte{}
	for i := 1; i < len(s); {
		switch c := s[i]; c {
		case '\\':
			if i+1 >= len(s) {
				return nil, 0, false // dangling backslash
			}
			switch next := s[i+1]; next {
			case '%', '_':
				// LIKE-pattern escapes keep the backslash (the manual's
				// string-literal escape table: '\%' means the two bytes).
				out = append(out, '\\', next)
			case '0':
				out = append(out, 0x00)
			case 'b':
				out = append(out, 0x08)
			case 't':
				out = append(out, 0x09)
			case 'n':
				out = append(out, 0x0A)
			case 'r':
				out = append(out, 0x0D)
			case 'Z':
				out = append(out, 0x1A)
			default:
				out = append(out, next) // unknown escape drops the backslash
			}
			i += 2
		case delim:
			if i+1 < len(s) && s[i+1] == delim {
				out = append(out, delim) // doubled delimiter
				i += 2
				continue
			}
			return out, i + 1, true
		default:
			out = append(out, c)
			i++
		}
	}
	return nil, 0, false // no closing delimiter
}

// FuzzScanQuotedStringDelim differentially fuzzes the production
// scanner against the reference decoder, over BOTH delimiters (the
// class: the single-quote form is the information_schema/pscale shape,
// the double-quote form is mydumper >=1.0's default emit shape). The
// seeds cover the escape-adjacency
// matrix (escape next to doubled delimiter, escape at the very end,
// LIKE escapes, the full escape table), the empty literal, and
// value-at-end-of-input shapes — the corners the committed matrix test
// was observed to sample around, not on.
func FuzzScanQuotedStringDelim(f *testing.F) {
	seeds := []string{
		"",                   // empty input
		"QQ",                 // empty literal, at end of input
		"QQ,(2,",             // empty literal, trailing text
		"QabcQ",              // plain value at end of input
		"QabcQ),(",           // plain value, trailing text
		"Qabc",               // unterminated
		`Qab\`,               // dangling backslash
		`Qab\Q`,              // unterminated via escaped closer
		`Qa\QbQ`,             // escaped delimiter
		"QaQQbQ",             // doubled delimiter
		"QQQQ",               // only a doubled delimiter
		`Q\QQQQ`,             // escape ADJACENT to doubled delimiter (the corner class)
		`QQQ\QQ`,             // doubled delimiter then escape
		`Q\0\b\t\n\r\Z\\Q`,   // the full escape table
		`Qa\%b\_cQ`,          // LIKE escapes keep the backslash
		`Q\qQ`,               // unknown escape drops the backslash
		"Qa\x00\nb\xffcQ",    // raw NUL / newline / invalid-UTF-8 bytes
		"QaDbQ",              // the other quote char rides through raw
		"DabcD",              // wrong opener (the other delimiter's literal)
		"Qvery long value Q", // value then trailing tail
	}
	for _, seed := range seeds {
		for _, useDouble := range []bool{false, true} {
			q, d := byte('\''), byte('"')
			if useDouble {
				q, d = '"', '\''
			}
			expanded := bytes.ReplaceAll([]byte(seed), []byte("Q"), []byte{q})
			expanded = bytes.ReplaceAll(expanded, []byte("D"), []byte{d})
			f.Add(string(expanded), useDouble)
		}
	}
	f.Fuzz(func(t *testing.T, s string, useDouble bool) {
		delim := byte('\'')
		if useDouble {
			delim = '"'
		}
		gotRaw, gotEnd, gotOK := scanQuotedStringDelim(s, delim)
		wantRaw, wantEnd, wantOK := refScanQuotedString(s, delim)
		if gotOK != wantOK {
			t.Fatalf("scanQuotedStringDelim(%q, %q) ok=%v; reference ok=%v", s, delim, gotOK, wantOK)
		}
		if !gotOK {
			return
		}
		if !bytes.Equal(gotRaw, wantRaw) || gotEnd != wantEnd {
			t.Fatalf("scanQuotedStringDelim(%q, %q) = %q end=%d; reference %q end=%d",
				s, delim, gotRaw, gotEnd, wantRaw, wantEnd)
		}
		if gotRaw == nil {
			t.Fatalf("scanQuotedStringDelim(%q, %q) returned a NIL decoded value — nil binds as SQL NULL downstream; an empty literal must stay a non-nil empty slice", s, delim)
		}
	})
}

// TestScanQuotedStringDelim_EmptyLiteralNonNil pins the empty-literal
// shape deterministically (audit 2026-07-16): a zero-length quoted
// literal decodes to a NON-nil empty []byte. A nil here would bind as
// SQL NULL at the
// driver layer, silently converting an empty VARBINARY/TEXT value into
// NULL — the fuzz target asserts the same invariant on every input.
func TestScanQuotedStringDelim_EmptyLiteralNonNil(t *testing.T) {
	for _, delim := range []byte{'\'', '"'} {
		in := string([]byte{delim, delim})
		raw, end, ok := scanQuotedStringDelim(in, delim)
		if !ok || end != 2 || len(raw) != 0 {
			t.Fatalf("scanQuotedStringDelim(%q) = %q end=%d ok=%v; want empty, end 2, ok", in, raw, end, ok)
		}
		if raw == nil {
			t.Errorf("scanQuotedStringDelim(%q) decoded the empty literal to NIL — that binds as SQL NULL, not an empty value", in)
		}
	}
}
