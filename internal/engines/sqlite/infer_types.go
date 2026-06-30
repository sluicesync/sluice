// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// This file implements [ir.InferredTypeValidator] for both the file
// [SchemaReader] and the live-D1 [D1SchemaReader] — the source half of the
// opt-in, data-validated rich-type inference (ADR-0144). The pipeline picks
// candidates by name-hint and asks "does EVERY non-NULL value conform to this
// richer target?"; the engine answers with ONE cheap aggregate per check
// (COUNT, no row transfer), pushed down to the source. A conforming column is
// then promoted by the pipeline via an injected override that rides the
// existing Bug-161 override decode in value_decode.go — so this adds NO new
// value-conversion code, only validation queries.
//
// Both readers run the SAME SQL; the only difference is the transport (the file
// reader's *sql.DB vs the D1 reader's HTTP /query). The shared core is
// [validateInferredType], parameterised over a [scalarCounter].

// scalarCounter runs an aggregate `SELECT COUNT(*) AS n …` and returns the
// single int64 result. It abstracts the file path (*sql.DB.QueryRowContext)
// from the D1 path (HTTP /query), so the validation logic is written once.
type scalarCounter func(ctx context.Context, query string) (int64, error)

// The GLOB patterns are the heart of the temporal/uuid validation. SQLite GLOB
// is case-sensitive and matches the WHOLE string (anchored both ends), with `*`
// = any run, `?` = any single char, and `[...]` char classes. Each pattern is
// pinned by a dedicated test (the Bug-74 discipline — pin the class).
const (
	// isoDateGlob matches a bare ISO-8601 calendar date `YYYY-MM-DD` (exactly
	// 10 chars — no `*`, so the anchoring rejects trailing garbage).
	isoDateGlob = `[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]`

	// isoDateTimeGlob matches an ISO-8601 datetime: the date, a `T` OR space
	// separator (`[ T]`), then `HH:MM:SS`. The trailing `*` admits an optional
	// fractional part and/or zone (`.123`, `Z`, `+05:00`); the decoder
	// (decodeTemporal, value_decode.go) is the second net — it loud-refuses any
	// tail it cannot parse, so a pathological "…:00 GARBAGE" can never silently
	// corrupt (it aborts the migrate naming the row).
	isoDateTimeGlob = `[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][ T][0-9][0-9]:[0-9][0-9]:[0-9][0-9]*`

	// offsetGlob matches a value whose LAST six chars are a signed `±HH:MM`
	// UTC offset (ISO-8601 extended form, with the colon). zuluGlob matches a
	// trailing `Z`. A value carries an explicit offset iff it matches EITHER —
	// the timestamptz-vs-timestamp resolution (never invent a zone).
	offsetGlob = `*[+-][0-9][0-9]:[0-9][0-9]`
	zuluGlob   = `*Z`
)

// uuidGlob is the 8-4-4-4-12 hex UUID pattern, case-insensitive via the
// `[0-9a-fA-F]` char class (GLOB itself is case-sensitive). Built once at init
// from 32 hex char-classes (GLOB has no `{n}` quantifier, so each nibble is
// spelled out). The `cus_abc123` non-UUID failure case (ADR-0144 / the pscale
// data-loss) does not match → it is kept `text`, never promoted.
var uuidGlob = buildUUIDGlob()

func buildUUIDGlob() string {
	const h = `[0-9a-fA-F]`
	seg := func(n int) string { return strings.Repeat(h, n) }
	return seg(8) + "-" + seg(4) + "-" + seg(4) + "-" + seg(4) + "-" + seg(12)
}

// ValidateInferredType implements [ir.InferredTypeValidator] for the file
// reader, running each aggregate over the read-only *sql.DB.
func (r *SchemaReader) ValidateInferredType(
	ctx context.Context, table, column string, target ir.Type,
) (conforms bool, resolved ir.Type, validated int64, err error) {
	return validateInferredType(ctx, r.countQuery, table, column, target)
}

// countQuery runs a scalar COUNT over the file reader's connection.
func (r *SchemaReader) countQuery(ctx context.Context, query string) (int64, error) {
	var n int64
	if err := r.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlite: infer-types validation query failed: %w", err)
	}
	return n, nil
}

// ValidateInferredType implements [ir.InferredTypeValidator] for the live-D1
// reader, running each aggregate over the HTTP /query transport. The SAME SQL
// the file path runs (the only difference is the row source).
func (r *D1SchemaReader) ValidateInferredType(
	ctx context.Context, table, column string, target ir.Type,
) (conforms bool, resolved ir.Type, validated int64, err error) {
	return validateInferredType(ctx, r.countQuery, table, column, target)
}

// countQuery runs a scalar COUNT over the D1 HTTP transport. The COUNT result
// is a small integer (well within float64's exact range), so the bare-JSON
// catalog path is safe here — the CAST/typeof exactness is only needed for the
// DATA read (ADR-0132 §5).
func (r *D1SchemaReader) countQuery(ctx context.Context, query string) (int64, error) {
	rows, err := r.client.queryRows(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, errors.New("d1: infer-types validation query returned no rows")
	}
	return rowInt(rows[0], "n")
}

// validateInferredType is the engine-shared ADR-0144 validation core. It runs a
// total-non-NULL count first (zero ⇒ nothing validated ⇒ never promote — an
// all-NULL/empty column is no evidence the rich type is correct), then a
// per-family violation count. conforms is true iff there are zero violations
// AND validated > 0. For the temporal family it also resolves timestamptz vs
// timestamp from the data.
func validateInferredType(
	ctx context.Context, count scalarCounter, table, column string, target ir.Type,
) (conforms bool, resolved ir.Type, validated int64, err error) {
	qt := quoteSQLiteIdent(table)
	qc := quoteSQLiteIdent(column)

	total, err := count(ctx, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL", qt, qc,
	))
	if err != nil {
		return false, nil, 0, err
	}
	if total == 0 {
		// No non-NULL values: nothing contradicts the rich type, but nothing
		// confirms it either. Refuse to promote on zero evidence (ADR-0144).
		return false, target, 0, nil
	}

	switch target.(type) {
	case ir.Boolean:
		// Every non-NULL value ∈ {0,1}. A non-numeric TEXT (e.g. "yes") in an
		// INTEGER-affinity column, or any integer other than 0/1, is a
		// violation → kept INTEGER. (The decoder also accepts truthy TEXT, but
		// inference is deliberately conservative: only true 0/1 columns.)
		bad, berr := count(ctx, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND %s NOT IN (0, 1)",
			qt, qc, qc,
		))
		if berr != nil {
			return false, nil, 0, berr
		}
		return bad == 0, ir.Boolean{}, total, nil

	case ir.JSON:
		// Every non-NULL value is valid JSON AND an object/array — so a bare
		// `'123'` or `'free'` (valid JSON number/parse-error) is NOT promoted
		// (the documented false-positive guard).
		bad, berr := count(ctx, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL "+
				"AND NOT (json_valid(%s) = 1 AND json_type(%s) IN ('object', 'array'))",
			qt, qc, qc, qc,
		))
		if berr != nil {
			return false, nil, 0, berr
		}
		return bad == 0, ir.JSON{Binary: true}, total, nil

	case ir.UUID:
		bad, berr := count(ctx, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND %s NOT GLOB '%s'",
			qt, qc, qc, uuidGlob,
		))
		if berr != nil {
			return false, nil, 0, berr
		}
		return bad == 0, ir.UUID{}, total, nil

	case ir.Timestamp:
		return validateInferredTimestamp(ctx, count, qt, qc, total)

	default:
		return false, nil, 0, fmt.Errorf(
			"sqlite: --infer-types has no validator for target %s", target.String(),
		)
	}
}

// validateInferredTimestamp validates the temporal family and resolves the
// timezone-awareness from the data. qt/qc are already-quoted identifiers; total
// is the (>0) non-NULL count. Conformance: every non-NULL value matches the
// bare-date OR the datetime GLOB. Resolution: timestamptz iff EVERY value
// carries an explicit offset/`Z`, else naive timestamp — sluice never invents a
// zone (a naive value must NOT silently become timestamptz).
func validateInferredTimestamp(
	ctx context.Context, count scalarCounter, qt, qc string, total int64,
) (conforms bool, resolved ir.Type, validated int64, err error) {
	bad, err := count(ctx, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND NOT (%s GLOB '%s' OR %s GLOB '%s')",
		qt, qc, qc, isoDateGlob, qc, isoDateTimeGlob,
	))
	if err != nil {
		return false, nil, 0, err
	}
	if bad != 0 {
		return false, ir.Timestamp{}, total, nil
	}

	// noOffset = how many non-NULL values do NOT carry an explicit offset/`Z`.
	// Zero ⇒ every value is zoned ⇒ timestamptz; otherwise naive timestamp.
	noOffset, err := count(ctx, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND NOT (%s GLOB '%s' OR %s GLOB '%s')",
		qt, qc, qc, offsetGlob, qc, zuluGlob,
	))
	if err != nil {
		return false, nil, 0, err
	}
	// Precision 6 mirrors the `timestamptz` --type-override alias (mappings.go);
	// PG renders timestamp(6)/timestamptz(6), the SQLite/ISO common case.
	return true, ir.Timestamp{Precision: 6, WithTimeZone: noOffset == 0}, total, nil
}

// quoteSQLiteIdent renders a table/column name as a double-quoted SQL
// identifier, escaping an embedded double-quote by doubling. Names come from
// the source's own catalog (trusted), but the quoting still defends against an
// embedded-quote identifier and keeps the validation SQL well-formed.
func quoteSQLiteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
