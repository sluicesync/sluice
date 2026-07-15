// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// MySQL-TIME range-mismatch notice — surfaces, loudly and before any
// data moves, that sluice maps MySQL `TIME` to PostgreSQL `time`
// (Bug 187).
//
// MySQL `TIME` is semantically a DURATION with range
// -838:59:59…838:59:59; PostgreSQL `time` is a time-of-day
// (00:00:00–24:00:00, never negative). The default mapping is correct
// for the overwhelmingly common time-of-day usage, but any stored
// duration outside PG `time`'s window has no representation on the
// target: the copy REFUSES loudly at that row (pgx declines to encode;
// surfaced inside SLUICE-E-BULKCOPY-TABLE-FAILED) — zero silent loss,
// but the operator meets a raw driver error mid-copy, after earlier
// tables have already copied. This advisory moves the discovery to
// `schema preview` and `migrate`/`sync` preflight, naming each TIME
// column and the lossless `--type-override TABLE.COL=interval` escape
// hatch (PG `interval` holds the full MySQL TIME range; the override
// is first-class — see translate/mappings.go and ir.Interval).
//
// The default mapping deliberately stays `time`: durations in TIME
// columns are the minority, and interval-by-default would surprise
// every time-of-day schema. Same advisory-not-refusal posture as the
// unsigned-bigint notice this file is modelled on.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// MySQLTimeRangeNotice names one column whose MySQL `TIME` type maps
// to PostgreSQL `time`, with the duration-range caveat. The fields
// identify the column precisely enough that the operator can target
// it with `--type-override`.
type MySQLTimeRangeNotice struct {
	// Table is the source-side table the column lives in.
	Table string
	// Column is the affected column name.
	Column string
}

// ScanMySQLTimeRangeNotices walks schema and returns one
// [MySQLTimeRangeNotice] per column whose IR type is ir.Time (MySQL
// `TIME(p)`). Cross-engine MySQL-family → Postgres only — returns nil
// for any other engine pair or a nil schema, since the range mismatch
// only exists when a MySQL duration lands on PG `time`.
//
// Callers scan the POST-override schema, so a column the operator has
// already overridden to `interval` (ir.Interval) or text no longer
// matches and the notice is suppressed for it — same convention as
// the unsigned-bigint scanner.
//
// Results are sorted by (table, column) so rendering is stable across
// runs.
func ScanMySQLTimeRangeNotices(schema *ir.Schema, sourceEngine, targetEngine string) []MySQLTimeRangeNotice {
	if schema == nil {
		return nil
	}
	if !IsMySQLFamily(sourceEngine) || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}

	var out []MySQLTimeRangeNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			if _, ok := col.Type.(ir.Time); !ok {
				continue
			}
			out = append(out, MySQLTimeRangeNotice{
				Table:  tbl.Name,
				Column: col.Name,
			})
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Column < out[j].Column
	})
	return out
}

// MySQLTimeRangeNoticeError renders an advisory (non-fatal) error
// describing every MySQL `TIME` → PG `time` range mismatch in schema,
// or nil when there are none. The caller decides how to surface it
// (WARN log at `migrate`/`sync` preflight). It is never a hard
// refusal: time-of-day TIME columns — the common case — must still
// migrate, and an out-of-range duration already fails loudly at copy
// time rather than corrupting.
//
// contextID is the caller's phase label ("migrate" / "sync
// cold-start") so the same diagnostic reads correctly at either
// surface.
//
// Returns nil for non-MySQL-family→PG pairs (ScanMySQLTimeRangeNotices
// short-circuits those).
func MySQLTimeRangeNoticeError(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	notices := ScanMySQLTimeRangeNotices(schema, sourceEngine, targetEngine)
	if len(notices) == 0 {
		return nil
	}
	return errors.New(renderMySQLTimeRangeNotice(notices, contextID))
}

// renderMySQLTimeRangeNotice builds the multi-line operator-facing
// message body. Split out so future surfaces share identical wording.
func renderMySQLTimeRangeNotice(notices []MySQLTimeRangeNotice, contextID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d MySQL TIME column(s) map to PostgreSQL `time` "+
		"(a time-of-day, 00:00:00-24:00:00, never negative); MySQL TIME is a "+
		"DURATION spanning -838:59:59..838:59:59", contextID, len(notices))
	b.WriteString(". Any stored value that is negative or at/above 24:00:00 has " +
		"no representation on the target and the copy will REFUSE loudly at that " +
		"row (never silently clamp). Migration proceeds. Affected columns:")
	for _, n := range notices {
		b.WriteString("\n  - ")
		fmt.Fprintf(&b, "%s.%s", n.Table, n.Column)
	}
	b.WriteString("\nIf a column stores durations (elapsed time, offsets), override " +
		"it per-column with `--type-override TABLE.COL=interval` — PG `interval` " +
		"holds the full MySQL TIME range losslessly, on both migrate and " +
		"continuous sync.")
	return b.String()
}
