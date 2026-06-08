// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Unconstrained-numeric range-narrowing notice — surfaces, loudly and
// before any DDL/data moves, that sluice maps an unconstrained
// PostgreSQL `numeric` (arbitrary precision; declared with NO
// precision/scale) to MySQL's widest representable `DECIMAL(65,30)`
// (catalog Bug 69).
//
// MySQL has no unbounded DECIMAL. The pre-fix behaviour collapsed the
// absent precision/scale to (0,0) and emitted DECIMAL(0,0) — silent
// decimal-precision data loss (3.14159 → 3). The chosen policy mirrors
// the established `bigint unsigned` → `bigint` precedent
// (unsigned_bigint.go): map UNIFORMLY to the widest MySQL form
// (DECIMAL(65,30) — MySQL's documented max precision 65 / scale 30),
// preserving far more than the old DECIMAL(0,0), and satisfy the
// loud-failure tenet not by silent narrowing but by this advisory
// surfaced at BOTH `schema preview` and `migrate` preflight. It is a
// NOTICE, not a refusal: unconstrained `numeric` is ubiquitous in PG
// schemas, so a hard refusal would over-block. Operators whose values
// genuinely need a different precision override per-column with
// `--type-override TABLE.COL=decimal(N,M)`.
//
// PG → PG is unaffected: the unconstrained numeric round-trips as bare
// `NUMERIC` (no narrowing), so the scan short-circuits non-MySQL
// targets. The `numeric[]` array case is likewise out of scope here:
// arrays land as MySQL JSON (retarget.go), which stores the values as
// text with no decimal-precision loss — there is nothing to narrow.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// UnconstrainedNumericNotice names one column whose unconstrained PG
// `numeric` maps to MySQL `DECIMAL(65,30)`. The fields identify the
// column precisely enough that the operator can target it with
// `--type-override`.
type UnconstrainedNumericNotice struct {
	// Table is the source-side table the column lives in.
	Table string
	// Column is the affected column name.
	Column string
}

// ScanUnconstrainedNumericNotices walks schema and returns one
// [UnconstrainedNumericNotice] per column whose IR type is an
// unconstrained [ir.Decimal] (bare PG `numeric` / `decimal`).
// Cross-engine Postgres → MySQL only — returns nil for any other engine
// pair or a nil schema, since the DECIMAL(65,30) widening only happens
// when emitting MySQL DDL from a Postgres source (PG → PG round-trips as
// bare NUMERIC with no narrowing).
//
// Results are sorted by (table, column) so rendering is stable across
// runs.
func ScanUnconstrainedNumericNotices(schema *ir.Schema, sourceEngine, targetEngine string) []UnconstrainedNumericNotice {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "postgres") || !isMySQLTarget(targetEngine) {
		return nil
	}

	var out []UnconstrainedNumericNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			d, ok := col.Type.(ir.Decimal)
			if !ok || !d.Unconstrained {
				continue
			}
			out = append(out, UnconstrainedNumericNotice{
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

// isMySQLTarget reports whether engine is a MySQL-family target name.
// PlanetScale is MySQL-wire-compatible and shares the same DECIMAL
// precision ceiling, so it's covered too — mirrors isMySQLSource's
// intent on the target side.
func isMySQLTarget(engine string) bool {
	return strings.EqualFold(engine, "mysql") ||
		strings.EqualFold(engine, "planetscale")
}

// UnconstrainedNumericNoticeError renders an advisory (non-fatal) error
// describing every unconstrained PG `numeric` → MySQL `DECIMAL(65,30)`
// widening in schema, or nil when there are none. The caller decides
// whether to treat it as advisory (log + proceed — the default for
// `migrate`) or informational (render in the preview output). It is
// never a hard refusal: unconstrained numeric is ubiquitous in PG
// schemas and must still migrate.
//
// contextID is the caller's phase label ("schema preview" / "migrate")
// so the same diagnostic reads correctly at either surface.
//
// Returns nil for non-PG→MySQL pairs (ScanUnconstrainedNumericNotices
// short-circuits those).
func UnconstrainedNumericNoticeError(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	notices := ScanUnconstrainedNumericNotices(schema, sourceEngine, targetEngine)
	if len(notices) == 0 {
		return nil
	}
	return errors.New(renderUnconstrainedNumericNotice(notices, contextID))
}

// renderUnconstrainedNumericNotice builds the multi-line operator-facing
// message body. Split out so the preview formatter and the migrate
// preflight share identical wording.
func renderUnconstrainedNumericNotice(notices []UnconstrainedNumericNotice, contextID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d unconstrained PostgreSQL `numeric` column(s) map to "+
		"MySQL `DECIMAL(65,30)`; values requiring more than 65 total digits or "+
		"more than 30 fractional digits are NOT representable on the target",
		contextID, len(notices))
	b.WriteString(". This is a deliberate, documented cross-engine policy " +
		"(MySQL has no unbounded DECIMAL; DECIMAL(65,30) is MySQL's widest " +
		"representable fixed-point form and preserves far more than the legacy " +
		"behaviour). Migration proceeds. Affected columns:")
	for _, n := range notices {
		b.WriteString("\n  - ")
		fmt.Fprintf(&b, "%s.%s", n.Table, n.Column)
	}
	b.WriteString("\nIf a column needs a different precision/scale, override it " +
		"per-column with `--type-override TABLE.COL=decimal(N,M)`.")
	return b.String()
}
