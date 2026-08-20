// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// boolRangePreflightBudgetMS caps each per-table probe (via a MAX_EXECUTION_TIME
// hint) so a clean, unindexed TINYINT(1) column on a huge table times out into
// the best-effort WARN path instead of making the operator wait on an unbounded
// full scan. A misused column's out-of-range values are found in milliseconds,
// far under this — the budget only ever bites a genuinely-clean large table, and
// the decode-time guard remains the correctness floor when it does. 2 s lets a
// moderately large clean table finish and confirm itself clean while flatly
// capping the pathological case.
const boolRangePreflightBudgetMS = 2000

// PreflightBoolRanges implements [ir.BoolRangePreflighter]. Before any data
// moves, it fails FAST when a source TINYINT(1) column — which sluice maps to
// ir.Boolean per MySQL's BOOL/BOOLEAN convention — holds a value outside {0,1}.
// This is the same silent-loss class the per-read-path decode guard
// ([checkTinyBoolRange]) refuses; the guard is the correctness floor, and this
// preflight is a fail-fast layer so a large multi-table copy refuses at
// planning time rather than partway through.
//
// For each in-scope table carrying at least one TINYINT(1) column it runs a
// single SHORT-CIRCUITING probe — `SELECT 1 ... WHERE (c1 NOT IN (0,1)) OR ...
// LIMIT 1` — which returns IMMEDIATELY when bad data exists (a column genuinely
// used as a small integer has out-of-range values throughout, found in
// milliseconds) and, on a clean table, uses an index on the column if one
// exists (also instant). On a hit it pinpoints the offending column + an example
// value and returns the coded SLUICE-E-VALUE-TINYINT1-RANGE refusal.
//
// The COST the majority pays — a genuinely-clean, unindexed column, where the
// probe would otherwise full-scan to prove absence — is bounded by a
// MAX_EXECUTION_TIME hint ([boolRangePreflightBudgetMS]): the scan is capped, so
// a correct 0/1 column on a huge table never makes the operator wait on an
// unbounded scan. Measured: on a clean 10 M-row table an uncapped probe took
// ~1.6 s and grew linearly with size; the cap holds it flat.
//
// BEST-EFFORT by design: a probe that cannot COMPLETE within the budget — the
// clean-huge-table case above (it returns errno 3024, the same code as
// PlanetScale's own statement wall) or any transient error — is a WARN, never a
// refusal, so the preflight itself never blocks a legitimate migration; the
// decode-time guard still catches anything it could not confirm. Everything runs
// on the reader's own *sql.DB, which is live on every flavor (it is how the
// schema was read), PlanetScale/Vitess via vtgate included.
func (r *SchemaReader) PreflightBoolRanges(ctx context.Context, schema *ir.Schema, rowFilters map[string]string) error {
	if r.db == nil || r.schema == "" || schema == nil {
		return nil
	}

	// Per-table set of columns that are STILL ir.Boolean in the (post-override)
	// schema. This is the load-bearing filter: --type-override runs BEFORE this
	// preflight (ApplyMappings, migrate.go), so a TINYINT(1) column the operator
	// re-typed to an integer is ir.Integer here and MUST NOT be probed — probing
	// it would refuse the exact remedy the refusal recommends. Dispatch on the
	// STORAGE type (ir.UnwrapDomain) per the Bug 233 domain-transparency rule
	// (identity for a MySQL source, but it keeps this on the guarded path).
	boolCols := make(map[string]map[string]struct{})
	for _, t := range schema.Tables {
		for _, c := range t.Columns {
			if _, isBool := ir.UnwrapDomain(c.Type).(ir.Boolean); isBool {
				if boolCols[t.Name] == nil {
					boolCols[t.Name] = make(map[string]struct{})
				}
				boolCols[t.Name][c.Name] = struct{}{}
			}
		}
	}
	if len(boolCols) == 0 {
		return nil
	}

	// Authoritative TINYINT(1) column set. COLUMN_TYPE = 'tinyint(1)' already
	// excludes the unsigned spelling ('tinyint(1) unsigned'); the EXTRA filter
	// excludes auto_increment — exactly translateType's ir.Boolean rule. This
	// also excludes BIT(1)-sourced booleans, whose single bit can never be out
	// of range, so we never waste a scan on one.
	byTable, err := r.tinyint1Columns(ctx)
	if err != nil {
		slog.WarnContext(ctx, "mysql: could not enumerate TINYINT(1) columns for the pre-copy range preflight; "+
			"relying on the per-row guard during the copy", slog.String("error", err.Error()))
		return nil
	}

	for _, t := range schema.Tables { // iterate in schema order for a stable probe sequence
		stillBool := boolCols[t.Name]
		if len(stillBool) == 0 {
			continue
		}
		// Probe only the raw TINYINT(1) columns that are STILL ir.Boolean —
		// i.e. not overridden to an integer (GAP #1) and not BIT(1)-sourced.
		var cols []string
		for _, c := range byTable[t.Name] {
			if _, ok := stillBool[c]; ok {
				cols = append(cols, c)
			}
		}
		if len(cols) == 0 {
			continue
		}
		if err := r.preflightTableBoolRanges(ctx, t.Name, cols, rowFilters[t.Name]); err != nil {
			return err // coded refusal — an out-of-range value was confirmed
		}
	}
	return nil
}

// andBoolRangeFilter prepends the operator's `--where` predicate (already
// validated + intended to be passed to the source verbatim, ADR-0173) to a
// range predicate, so the probe only ever considers rows the copy would move.
// The range predicate is wrapped in its own parens because for the COMBINED
// probe it is an OR of per-column checks: `(filter) AND col1 NOT IN (0,1) OR
// col2 …` would bind AND tighter than OR and silently apply the filter to only
// the first column. An empty filter returns the predicate byte-for-byte
// unchanged, so the common unfiltered path is identical to before F-1.
func andBoolRangeFilter(filter, rangePred string) string {
	if strings.TrimSpace(filter) == "" {
		return rangePred
	}
	return "(" + filter + ") AND (" + rangePred + ")"
}

// tinyint1Columns returns the signed, non-auto-increment TINYINT(1) columns per
// table in the reader's database — the exact set translateType maps to
// ir.Boolean.
func (r *SchemaReader) tinyint1Columns(ctx context.Context) (map[string][]string, error) {
	const q = `SELECT TABLE_NAME, COLUMN_NAME
	           FROM information_schema.columns
	           WHERE TABLE_SCHEMA = ?
	             AND COLUMN_TYPE = 'tinyint(1)'
	             AND (EXTRA IS NULL OR EXTRA NOT LIKE '%auto_increment%')`
	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string][]string)
	for rows.Next() {
		var tbl, col string
		if err := rows.Scan(&tbl, &col); err != nil {
			return nil, err
		}
		out[tbl] = append(out[tbl], col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// preflightTableBoolRanges probes one table's TINYINT(1) columns for an
// out-of-range value with a single short-circuiting query; on a hit it
// pinpoints the culprit column + an example value and returns the coded
// refusal. A probe that cannot complete WARNs and returns nil (best-effort).
func (r *SchemaReader) preflightTableBoolRanges(ctx context.Context, table string, cols []string, rowFilter string) error {
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(table)
	preds := make([]string, len(cols))
	for i, c := range cols {
		preds[i] = quoteIdent(c) + " NOT IN (0,1)"
	}
	// The MAX_EXECUTION_TIME hint caps the scan so a clean, unindexed column on
	// a huge table can't make the operator wait — it times out into the WARN
	// path below rather than full-scanning. A misused column's out-of-range
	// values are found long before the cap. The `--where` filter (if any) is
	// ANDed in so the probe never refuses on a row the copy would not move (F-1).
	combined := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ 1 FROM %s WHERE %s LIMIT 1",
		boolRangePreflightBudgetMS, tableRef, andBoolRangeFilter(rowFilter, strings.Join(preds, " OR ")))

	var probe int
	switch err := r.db.QueryRowContext(ctx, combined).Scan(&probe); {
	case errors.Is(err, sql.ErrNoRows):
		return nil // every TINYINT(1) column in this table is in range
	case err != nil:
		slog.WarnContext(ctx, "mysql: could not preflight TINYINT(1) value ranges for a table — the probe did not "+
			"complete (a clean huge table can time out or hit a statement-time limit); relying on the per-row guard "+
			"during the copy", slog.String("table", r.schema+"."+table), slog.String("error", err.Error()))
		return nil
	}

	// A row matched, so at least one column holds a non-NULL value outside
	// {0,1} (NULL NOT IN (0,1) is UNKNOWN, never TRUE, so NULLs do not match).
	// Pinpoint the culprit for the message — the same predicate, per column, so
	// one MUST match unless the bad row was concurrently deleted between the two
	// queries; each pinpoint short-circuits because the bad data is present.
	for _, c := range cols {
		var v sql.NullInt64
		q := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ %s FROM %s WHERE %s LIMIT 1",
			boolRangePreflightBudgetMS, quoteIdent(c), tableRef, andBoolRangeFilter(rowFilter, quoteIdent(c)+" NOT IN (0,1)"))
		switch err := r.db.QueryRowContext(ctx, q).Scan(&v); {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			continue // stay best-effort; another column may still pinpoint
		}
		if v.Valid {
			return checkTinyBoolRange(table, c, v.Int64)
		}
	}

	// The combined probe matched but no single column pinpointed (the bad row
	// was deleted between queries, or a transient pinpoint error). Do not
	// fabricate a value — WARN and proceed under the decode-time guard.
	slog.WarnContext(ctx, "mysql: a TINYINT(1) range preflight matched a table but could not pinpoint the column "+
		"(the offending row may have been concurrently deleted); relying on the per-row guard during the copy",
		slog.String("table", r.schema+"."+table))
	return nil
}
