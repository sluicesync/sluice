// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Unsigned-bigint range-narrowing notice — surfaces, loudly and
// before any DDL/data moves, that sluice maps MySQL `bigint unsigned`
// to PostgreSQL `bigint` (Bug 11).
//
// PostgreSQL has no unsigned 64-bit integer. The earlier policy split
// the mapping (NUMERIC(20,0) for a plain `bigint unsigned` column,
// BIGINT for an AUTO_INCREMENT one — because PG's `GENERATED ... AS
// IDENTITY` is only valid on smallint/integer/bigint, never numeric).
// That split made a `bigint unsigned` FK child column (→ NUMERIC(20,0))
// type-incompatible with the `bigint unsigned AUTO_INCREMENT` PK it
// referenced (→ BIGINT IDENTITY): the FK creation failed SQLSTATE
// 42804. Since `id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY` +
// `*_id BIGINT UNSIGNED` FK columns is the DEFAULT schema shape of
// essentially every Rails/Laravel/Django/Sequelize/Prisma MySQL app,
// every default ORM schema hit this.
//
// sluice now maps `bigint unsigned` UNIFORMLY to PG `bigint` so the PK
// and FK types match by construction. The cost is a deliberate,
// documented range narrowing: values in (2^63-1, 2^64-1] are not
// representable in PG `bigint`. The loud-failure tenet is satisfied
// not by silently narrowing but by this advisory notice surfaced at
// BOTH `schema preview` and `migrate` preflight. It is a NOTICE
// (advisory) by default, not a hard refusal — the universal ORM schema
// must still migrate — but it must be loud and visible. Operators who
// genuinely need the full 2^64 range override per-column with
// `--type-override TABLE.COL=decimal(20,0)` (PG numeric(20,0) holds
// 2^64-1; or =text to carry it as text).

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// UnsignedBigintNotice names one column whose MySQL `bigint unsigned`
// type maps to PostgreSQL `bigint`, with the (2^63, 2^64) range loss.
// The fields identify the column precisely enough that the operator
// can target it with `--type-override`.
type UnsignedBigintNotice struct {
	// Table is the source-side table the column lives in.
	Table string
	// Column is the affected column name.
	Column string
	// AutoIncrement is true when the column is a `bigint unsigned
	// AUTO_INCREMENT` (typically the primary key). Surfaced so the
	// operator sees that the autoincrement key itself is in scope —
	// in practice these monotonically-allocated IDs never approach
	// 2^63, which is why the uniform mapping is safe in the common
	// case.
	AutoIncrement bool
}

// ScanUnsignedBigintNotices walks schema and returns one
// [UnsignedBigintNotice] per column whose IR type is a 64-bit unsigned
// integer (MySQL `bigint unsigned`). Cross-engine MySQL → Postgres
// only — returns nil for any other engine pair or a nil schema, since
// the bigint→bigint narrowing only happens when emitting PG DDL from a
// MySQL source.
//
// Results are sorted by (table, column) so rendering is stable across
// runs.
func ScanUnsignedBigintNotices(schema *ir.Schema, sourceEngine, targetEngine string) []UnsignedBigintNotice {
	if schema == nil {
		return nil
	}
	if !isMySQLSource(sourceEngine) || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}

	var out []UnsignedBigintNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			iv, ok := col.Type.(ir.Integer)
			if !ok {
				continue
			}
			if iv.Width != 64 || !iv.Unsigned {
				continue
			}
			out = append(out, UnsignedBigintNotice{
				Table:         tbl.Name,
				Column:        col.Name,
				AutoIncrement: iv.AutoIncrement,
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

// isMySQLSource reports whether engine is a MySQL-family source name.
// PlanetScale is MySQL-wire-compatible and shares the same unsigned
// semantics, so it's covered too — mirrors the gap scanner's intent
// even though ScanMySQLToPGGaps keys strictly on "mysql".
func isMySQLSource(engine string) bool {
	return strings.EqualFold(engine, "mysql") ||
		strings.EqualFold(engine, "planetscale")
}

// UnsignedBigintNoticeError renders an advisory (non-fatal) error
// describing every MySQL `bigint unsigned` → PG `bigint` narrowing in
// schema, or nil when there are none. The caller decides whether to
// treat it as advisory (log + proceed — the default for `migrate`) or
// informational (render in the preview output). It is never a hard
// refusal: the universal ORM schema must still migrate.
//
// contextID is the caller's phase label ("schema preview" / "migrate")
// so the same diagnostic reads correctly at either surface.
//
// Returns nil for non-MySQL→PG pairs (ScanUnsignedBigintNotices
// short-circuits those).
func UnsignedBigintNoticeError(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	notices := ScanUnsignedBigintNotices(schema, sourceEngine, targetEngine)
	if len(notices) == 0 {
		return nil
	}
	return errors.New(renderUnsignedBigintNotice(notices, contextID))
}

// renderUnsignedBigintNotice builds the multi-line operator-facing
// message body. Split out so the preview formatter and the migrate
// preflight share identical wording.
func renderUnsignedBigintNotice(notices []UnsignedBigintNotice, contextID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d MySQL `bigint unsigned` column(s) map to PostgreSQL "+
		"`bigint`; values greater than 2^63-1 (9223372036854775807) are NOT "+
		"representable on the target", contextID, len(notices))
	b.WriteString(". This is a deliberate, documented cross-engine policy " +
		"(PostgreSQL has no unsigned 64-bit type; the uniform `bigint` " +
		"mapping keeps PRIMARY KEY / FOREIGN KEY types consistent so " +
		"foreign keys to AUTO_INCREMENT keys are created successfully). " +
		"Migration proceeds. Affected columns:")
	for _, n := range notices {
		b.WriteString("\n  - ")
		fmt.Fprintf(&b, "%s.%s", n.Table, n.Column)
		if n.AutoIncrement {
			b.WriteString(" (AUTO_INCREMENT — autoincrement IDs in practice never reach 2^63)")
		}
	}
	b.WriteString("\nIf a column genuinely stores values above 2^63-1, override " +
		"it per-column with `--type-override TABLE.COL=decimal(20,0)` " +
		"— PG numeric(20,0) preserves the full unsigned 64-bit range (2^64-1 is 20 " +
		"digits). (`--type-override TABLE.COL=text` carries it as text instead. Note a " +
		"numeric/text column cannot also be an IDENTITY/AUTO_INCREMENT key.)")
	return b.String()
}
