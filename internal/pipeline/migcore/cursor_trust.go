// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// Legacy resume-cursor trust checks (audit 2026-07-15 CRITICAL-2 /
// HIGH-1).
//
// Cursor values persisted by a pre-envelope release rode plain
// encoding/json, which mangled two classes silently: []byte-derived
// strings had every invalid-UTF-8 byte replaced with U+FFFD at
// Marshal, and integers above 2^53 drifted through float64 at decode.
// The ir.TableProgress cursor envelope fixes the round-trip for new
// writes, and its decoder parses legacy bare integers exactly — but a
// legacy value already bearing the mangling fingerprints is
// unrecoverable, and resuming from it silently skips or replays PK
// ranges. These helpers let the resume sites — which know the PK
// column types — detect those fingerprints and refuse (backfill) or
// degrade to truncate-and-redo (migrate) instead of walking from a
// lie.

// SuspectLegacyCursor reports why a persisted PK cursor is
// untrustworthy, or "" when it is clean. cursor[i] aligns with the
// table's PK column i (chunk bounds are PK-tuple prefixes, so a
// shorter slice still aligns). Two fingerprints:
//
//   - a string containing U+FFFD: the pre-envelope Marshal replaced
//     invalid-UTF-8 cursor bytes with the replacement character. (New
//     writers keep U+FFFD out of bare strings — a genuine one wears
//     the bytes envelope — so a bare stored U+FFFD is definitive. The
//     named wart: a pre-envelope TEXT cursor whose value genuinely
//     contained U+FFFD is flagged too; the remedy re-walks rows the
//     guard already excludes, so the false positive costs time, never
//     correctness.)
//   - a float where the PK column is integral: integer cursors are
//     never floats on any shipping reader/executor, so a float there
//     is the pre-envelope decode's lossy float64 — possibly drifted.
//
// Named residual (audit 2026-07-16, considered and accepted): a
// pre-envelope binary that RESUMED a >2^53 integer-PK run drifted the
// cursor through float64 and RE-PERSISTED it as bare integral digits
// (json.Marshal(float64(9007199254740995)) emits "9007199254740996"),
// which now decode as an exact int64 — the float fingerprint never
// fires. Distrusting every legacy bare integer >2^53 would close it
// but truncate-redo/refuse every large-PK legacy resume for a
// population that requires a pre-envelope resume-of-resume on a >2^53
// PK; `--restart` stays the operator remedy when that history is
// suspected.
func SuspectLegacyCursor(table *ir.Table, cursor []any) string {
	return suspectCursor(table, cursor, false)
}

// SuspectLegacyMigrateCursor is [SuspectLegacyCursor] plus one
// migrate-only fingerprint: a bare string where the PK column is
// binary-family. The pre-envelope migrate path persisted []byte
// cursors through plain json.Marshal, which base64-encodes — so a
// legacy bare string over a BINARY/VARBINARY/BLOB column is base64
// garbage, content-indistinguishable from real bytes. (The backfill
// executors stringified bytes BEFORE the store, so a clean valid-UTF-8
// bare string there is byte-exact and [SuspectLegacyCursor] keeps
// trusting it.) New writers store bytes as the envelope, which decodes
// to []byte, so fresh rows never trip this.
func SuspectLegacyMigrateCursor(table *ir.Table, cursor []any) string {
	return suspectCursor(table, cursor, true)
}

func suspectCursor(table *ir.Table, cursor []any, distrustBareStringOverBinary bool) string {
	if table == nil || table.PrimaryKey == nil {
		return ""
	}
	pk := table.PrimaryKey.Columns
	for i, v := range cursor {
		var colType ir.Type
		if i < len(pk) {
			if col := LookupColumn(table, pk[i].Column); col != nil {
				colType = unwrapDomain(col.Type)
			}
		}
		switch t := v.(type) {
		case string:
			if strings.ContainsRune(t, utf8.RuneError) {
				return fmt.Sprintf("cursor value %d (PK column %q) contains U+FFFD — a pre-envelope sluice release mangled non-UTF-8 binary cursor bytes at store time", i, pkColumnName(pk, i))
			}
			if distrustBareStringOverBinary && isBinaryFamily(colType) {
				return fmt.Sprintf("cursor value %d (PK column %q) is a bare string over a binary-family column — a pre-envelope sluice release stored []byte cursors as base64 text", i, pkColumnName(pk, i))
			}
		case float64:
			if _, integral := colType.(ir.Integer); integral {
				return fmt.Sprintf("cursor value %d (PK column %q) is float-typed where the column is an integer — a pre-envelope sluice release's lossy float64 decode (values above 2^53 drift)", i, pkColumnName(pk, i))
			}
		}
	}
	return ""
}

// pkColumnName names PK column i for the suspicion messages,
// tolerating a cursor wider than the PK (malformed state — the width
// itself fails downstream validation loudly).
func pkColumnName(pk []ir.IndexColumn, i int) string {
	if i < len(pk) {
		return pk[i].Column
	}
	return "?"
}

// unwrapDomain resolves an ir.Domain to its base type (mirrors
// [IsOrderablePKType]'s handling).
func unwrapDomain(t ir.Type) ir.Type {
	for {
		dom, ok := t.(ir.Domain)
		if !ok || dom.BaseType == nil {
			return t
		}
		t = dom.BaseType
	}
}

// isBinaryFamily reports whether t is a binary-family column type
// (the types whose cursor values are raw bytes on every reader).
func isBinaryFamily(t ir.Type) bool {
	switch t.(type) {
	case ir.Binary, ir.Varbinary, ir.Blob:
		return true
	default:
		return false
	}
}
