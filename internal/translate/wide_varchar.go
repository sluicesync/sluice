// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Wide-varchar down-map notice — surfaces, loudly and before any
// DDL/data moves, that sluice down-maps a wide bounded PostgreSQL
// `varchar(N)` to a MySQL TEXT-family type because MySQL cannot
// represent it as VARCHAR (catalog Bug 72).
//
// MySQL's utf8mb4 VARCHAR is creatable only up to ~16383 characters
// (Error 1074 above that), and a VARCHAR that wide also exhausts the
// 65535-byte InnoDB row budget (Error 1118). The pre-fix behaviour
// emitted `VARCHAR(N)` literally and the migration died with a raw
// MySQL error at create-tables. The chosen policy mirrors the
// established unbounded PG `text` → MySQL `LONGTEXT` precedent: faithful
// down-map to the smallest TEXT tier that still holds N characters, and
// satisfy the loud-failure tenet not by a silent type swap but by this
// advisory surfaced at BOTH `schema preview` and `migrate` preflight.
// It is a NOTICE, not a refusal: wide free-text columns are common and
// must still migrate. Operators who need a specific MySQL storage shape
// override per-column with `--type-override TABLE.COL=...`.
//
// PG → PG is unaffected: `varchar(N)` round-trips unchanged (PG's
// varchar has no such limit), so the scan short-circuits non-MySQL
// targets.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// wideVarcharThresholdChars mirrors mysqlMaxInlineVarcharChars in
// internal/engines/mysql/ddl_emit.go. Duplicated rather than imported
// to keep the IR-first tenet's "no engine-package imports in
// translate" rule; the two must move together (a test pins the
// boundary on the engine side and the notice scans the same one).
const wideVarcharThresholdChars = 16000

// MySQL TEXT-tier byte ceilings, mirroring the constants in
// internal/engines/mysql/ddl_emit.go. Duplicated (not imported) for
// the same IR-first "no engine-package imports in translate" reason as
// wideVarcharThresholdChars; the two must move together.
const (
	wideVarcharBytesPerChar = 4
	textMaxBytes            = 65535
	mediumTextMaxBytes      = 16777215
)

// mysqlTextTierForWideVarcharIR is the translate-package mirror of
// mysqlTextTierForWideVarchar in internal/engines/mysql/ddl_emit.go.
// Used by RetargetForEngine so `sluice schema diff` predicts the same
// TEXT tier the migrate emitter lands on for a wide varchar(N)
// (catalog Bug 72).
func mysqlTextTierForWideVarcharIR(length int) (ir.TextSize, bool) {
	if length <= wideVarcharThresholdChars {
		return 0, false
	}
	worstCaseBytes := length * wideVarcharBytesPerChar
	switch {
	case worstCaseBytes <= textMaxBytes:
		return ir.TextRegular, true
	case worstCaseBytes <= mediumTextMaxBytes:
		return ir.TextMedium, true
	default:
		return ir.TextLong, true
	}
}

// WideVarcharNotice names one column whose wide PG `varchar(N)` is
// down-mapped to a MySQL TEXT-family type. Length is the source-side
// declared character length so the operator can see exactly how wide
// the column was and target it with `--type-override`.
type WideVarcharNotice struct {
	// Table is the source-side table the column lives in.
	Table string
	// Column is the affected column name.
	Column string
	// Length is the source `varchar(N)` declared length.
	Length int
}

// ScanWideVarcharNotices walks schema and returns one
// [WideVarcharNotice] per column whose IR type is an [ir.Varchar]
// wider than MySQL keeps as VARCHAR. Cross-engine Postgres → MySQL
// only — returns nil for any other engine pair or a nil schema, since
// the down-map only happens when emitting MySQL DDL from a Postgres
// source (PG → PG round-trips varchar unchanged).
//
// Results are sorted by (table, column) so rendering is stable.
func ScanWideVarcharNotices(schema *ir.Schema, sourceEngine, targetEngine string) []WideVarcharNotice {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "postgres") || !isMySQLTarget(targetEngine) {
		return nil
	}

	var out []WideVarcharNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			vc, ok := col.Type.(ir.Varchar)
			if !ok || vc.Length <= wideVarcharThresholdChars {
				continue
			}
			out = append(out, WideVarcharNotice{
				Table:  tbl.Name,
				Column: col.Name,
				Length: vc.Length,
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

// WideVarcharNoticeError renders an advisory (non-fatal) error
// describing every wide PG `varchar(N)` → MySQL TEXT-tier down-map in
// schema, or nil when there are none. Never a hard refusal: wide
// free-text columns are common and must still migrate.
//
// contextID is the caller's phase label ("schema preview" / "migrate")
// so the same diagnostic reads correctly at either surface.
//
// Returns nil for non-PG→MySQL pairs (ScanWideVarcharNotices
// short-circuits those).
func WideVarcharNoticeError(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	notices := ScanWideVarcharNotices(schema, sourceEngine, targetEngine)
	if len(notices) == 0 {
		return nil
	}
	return errors.New(renderWideVarcharNotice(notices, contextID))
}

// renderWideVarcharNotice builds the multi-line operator-facing message
// body. Split out so the preview formatter and the migrate preflight
// share identical wording.
func renderWideVarcharNotice(notices []WideVarcharNotice, contextID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d wide PostgreSQL `varchar(N)` column(s) are down-mapped to "+
		"a MySQL TEXT-family type; MySQL cannot represent a VARCHAR this wide "+
		"(utf8mb4's ~16383-char creatable cap, and the 65535-byte InnoDB row "+
		"limit)", contextID, len(notices))
	b.WriteString(". This is a deliberate, documented cross-engine policy " +
		"(mirrors the unbounded PG `text` → MySQL `LONGTEXT` down-map; the TEXT " +
		"tier is sized so the column never holds fewer characters than the " +
		"source declared). Migration proceeds. Affected columns:")
	for _, n := range notices {
		b.WriteString("\n  - ")
		fmt.Fprintf(&b, "%s.%s (varchar(%d))", n.Table, n.Column, n.Length)
	}
	b.WriteString("\nIf a column needs a specific MySQL storage shape, override it " +
		"per-column with `--type-override TABLE.COL=...`.")
	return b.String()
}
