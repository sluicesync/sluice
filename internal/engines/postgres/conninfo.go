// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "strings"

// The libpq key/value connection-string grammar, in ONE place, for the
// two things sluice does with such a string: READ a setting out of it,
// and REWRITE it with a setting removed.
//
// # Why this file exists
//
// `parseKVFields` (source_identity.go) already read the grammar
// correctly — quoted values, backslash escapes, whitespace around `=`,
// last occurrence winning — because audit 2026-09-15 A0915-VF2-F3 found
// that a `strings.Fields` walk made two different sources render one
// identity. That fix reached the IDENTITY depth only. Every REWRITER in
// the tree still split on whitespace and re-joined the survivors, which
// is a strictly worse bug on a strictly worse path:
//
//	password='s schema=x' dbname=real
//
// is ONE password setting to libpq and pgx. A `strings.Fields` walk sees
// three tokens, recognises the middle one as `schema=`, drops it, and
// hands the driver `password='s dbname=real` — a corrupted credential
// and an unterminated quote. Filed as A0915-VF2-PGDSN-1 and deliberately
// not fixed between the v0.154.0 release commit and its tag, because
// changing what reaches the driver at that moment is the shape CLAUDE.md
// warns about.
//
// # Why rewriting returns SPANS rather than re-serialising a map
//
// The obvious fix — parse into a map, delete a key, print it back — has
// to re-quote every value it prints, and a quoting bug on the CONNECTION
// path is a failed connection or, worse, a silently different one. The
// operator's own bytes are already correct; the only thing that needs to
// change is that one setting should not be there.
//
// So [parseConnInfo] returns each setting with the byte range it occupies
// in the ORIGINAL string, and [stripSettings] deletes exactly those
// ranges. Every other byte — spacing, quoting, escapes, an exotic value
// nothing in this package understands — reaches the driver untouched,
// because it is never re-rendered. The round trip is the identity
// function on any DSN that carries none of the stripped keywords, which
// is what the pins assert.
//
// # What this grammar is NOT
//
// It is not a validator. A malformed tail (a keyword with no `=`) stops
// the walk and keeps what was parsed, exactly as `parseKVFields` always
// did: the connection path refuses a DSN the driver cannot read, with
// the DRIVER's message, which is better than any message this package
// could invent. `stripSettings` therefore leaves a malformed tail in
// place rather than truncating at it — dropping bytes the walk did not
// understand is how the defect above happened in the first place.

// connInfoSetting is one `keyword = value` setting, plus the half-open
// byte range [Start, End) it occupies in the string it was parsed from.
//
// Value is UNESCAPED and UNQUOTED (what the driver will see); the span
// is over the RAW bytes (what the operator wrote). The two deliberately
// differ, and that is the point: callers that need the value read Value,
// callers that need to rewrite use the span and never re-render.
type connInfoSetting struct {
	Key   string // lower-cased keyword, as libpq compares them
	Value string // unescaped, unquoted
	Start int    // first byte of the keyword
	End   int    // one past the last byte of the value
}

// parseConnInfo tokenises a libpq key/value connection string, returning
// every setting in the order it appears — including repeats, which the
// caller resolves (libpq and pgx take the LAST occurrence).
//
// The walk is the one `parseKVFields` has always used, lifted here so the
// reading and rewriting paths cannot drift apart. Three things a
// whitespace split gets wrong, all in the direction that makes two
// different sources look like one:
//
//   - a SINGLE-QUOTED value may contain spaces and `=`, so `dbname='a b'`
//     is one setting, not the tokens `dbname='a` and `b'`;
//   - a backslash escapes the next character, inside quotes and out, so
//     `dbname=a\ b` is one value;
//   - a repeated keyword takes the LAST value.
func parseConnInfo(dsn string) []connInfoSetting {
	var out []connInfoSetting
	i, n := 0, len(dsn)
	skipSpace := func() {
		for i < n && isConnInfoSpace(dsn[i]) {
			i++
		}
	}
	for i < n {
		skipSpace()
		if i >= n {
			break
		}
		start := i
		for i < n && !isConnInfoSpace(dsn[i]) && dsn[i] != '=' {
			i++
		}
		key := dsn[start:i]
		skipSpace()
		if i >= n || dsn[i] != '=' {
			// A keyword with no `=`. Not ours to diagnose — stop and keep
			// what we have, so the driver reports it in its own words.
			break
		}
		i++
		skipSpace()
		var val strings.Builder
		if i < n && dsn[i] == '\'' {
			i++
			for i < n {
				if dsn[i] == '\\' && i+1 < n {
					val.WriteByte(dsn[i+1])
					i += 2
					continue
				}
				if dsn[i] == '\'' {
					i++
					break
				}
				val.WriteByte(dsn[i])
				i++
			}
		} else {
			for i < n && !isConnInfoSpace(dsn[i]) {
				if dsn[i] == '\\' && i+1 < n {
					val.WriteByte(dsn[i+1])
					i += 2
					continue
				}
				val.WriteByte(dsn[i])
				i++
			}
		}
		if key != "" {
			out = append(out, connInfoSetting{
				Key:   strings.ToLower(key),
				Value: val.String(),
				Start: start,
				End:   i,
			})
		}
	}
	return out
}

// SplitSchemaFromKVDSN returns the `schema` setting a libpq key/value
// connection string carries — empty when it carries none — together with
// the DSN that setting removed, ready to hand to the driver.
//
// Exported for `pgtrigger`, which speaks exactly this DSN grammar and
// used to keep its own whitespace-splitting copy of it. Delegating rather
// than copying is the same argument its SourceIdentity already makes: if
// the two ever disagreed about a schema, a `migrate --resume` across the
// two drivers against ONE database would be refused, and no test
// exercising either engine alone would notice.
//
// Callers wanting a different keyword, or several, use the unexported
// [stripSettings]; this one is deliberately narrow because the schema
// setting is the only sluice-specific parameter either engine strips.
func SplitSchemaFromKVDSN(dsn string) (schema, rest string) {
	return parseKVFields(dsn)["schema"], stripSettings(dsn, "schema")
}

// stripSettings returns dsn with every setting whose keyword matches one
// of keys removed, and EVERY OTHER BYTE preserved verbatim.
//
// Keys are compared case-insensitively, the way libpq compares keywords.
// Removing a setting also removes the run of whitespace that separated it
// from the previous setting, so the result carries no double space and no
// leading or trailing one; nothing else is touched.
//
// This is the operation four call sites in this tree used to perform by
// `strings.Fields` + `strings.Join`, which silently deleted any token
// that happened to live inside a quoted value (A0915-VF2-PGDSN-1).
func stripSettings(dsn string, keys ...string) string {
	if len(keys) == 0 {
		return dsn
	}
	drop := make(map[string]bool, len(keys))
	for _, k := range keys {
		drop[strings.ToLower(k)] = true
	}

	settings := parseConnInfo(dsn)
	var b strings.Builder
	b.Grow(len(dsn))
	prev := 0
	for _, s := range settings {
		if !drop[s.Key] {
			continue
		}
		// Absorb the whitespace run in front of this setting, so removing
		// a middle setting does not leave a double separator behind.
		cut := s.Start
		for cut > prev && isConnInfoSpace(dsn[cut-1]) {
			cut--
		}
		b.WriteString(dsn[prev:cut])
		prev = s.End
	}
	b.WriteString(dsn[prev:])

	// A leading separator can survive when the FIRST setting was removed.
	return strings.TrimLeft(b.String(), " \t\n\r\v\f")
}
