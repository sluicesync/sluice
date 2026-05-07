// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ExactRowCount implements [ir.Verifier]. Returns the exact row count
// for the given table via SELECT COUNT(*).
//
// Authoritative count (vs. [SchemaReader.CountRows] / RowReader's
// CountRows which use pg_class.reltuples for ETA hints): `sluice
// verify` needs counts that won't silently disagree with what's
// actually stored, so we pay the full-scan cost.
//
// Schema-qualified by the schema the reader is bound to (default
// `public`). Returns (0, error) on any operational failure; (0, nil)
// is reserved for "table is empty."
//
// PG's COUNT(*) is unconstrained (no equivalent of PlanetScale's
// row-read budget), so PG never needs the chunked-count fallback the
// MySQL engine implements for PS-MySQL large tables.
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: ExactRowCount: table is nil")
	}
	if r.db == nil {
		return 0, errors.New("postgres: ExactRowCount: reader not opened")
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`,
		quoteIdent(r.schema), quoteIdent(table.Name))
	var count int64
	if err := r.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("postgres: ExactRowCount %s.%s: %w",
			r.schema, table.Name, err)
	}
	return count, nil
}

// SampleRowHashes implements [ir.SampleVerifier]. Picks n rows
// deterministically (same seed → same row set on source + target),
// computes the row-content hash server-side via the requested algo,
// returns sorted by PK.
//
// Sampling strategy: ORDER BY md5(<pk-as-text> || '<seed>') LIMIT n.
// This pseudorandomly orders rows and slices the first n; the
// MD5(pk||seed) function is deterministic so source and target
// running the query with the same seed pick the same row subset.
// Note: even with `algo=SHA256`, sampling-order uses MD5 — the
// sample selection just needs determinism, not collision resistance.
// The strict-hash path applies to the row-content hash, not the
// sample-selection ordering.
//
// **Single-PK constraint** (v0.14.0 MVP): the sampling SQL needs the
// PK to construct the seed-derived ordering. Tables without a PK or
// with a composite PK fall back to "sample-mode-not-supported-for-
// this-table"; the orchestrator surfaces a clear per-table SKIPPED
// reason so the operator knows which tables verify only by count.
//
// Hash strategy: MD5 (default) or SHA-256 (operator opt-in via
// --strict-hash, v0.14.2+) of CONCAT_WS('|', col1::text, ...). PG's
// CONCAT_WS skips NULLs, matching MySQL's behavior, so NULL handling
// is consistent same-engine.
//
// SHA-256 path: PG 11+ ships sha256() in core (no pgcrypto needed),
// returning bytea. ENCODE(..., 'hex') gives a 64-char hex string
// matching the MD5() shape.
func (r *SchemaReader) SampleRowHashes(ctx context.Context, table *ir.Table, n int, seed int64, algo ir.HashAlgorithm) ([]ir.SampledRowHash, error) {
	if table == nil {
		return nil, errors.New("postgres: SampleRowHashes: table is nil")
	}
	if r.db == nil {
		return nil, errors.New("postgres: SampleRowHashes: reader not opened")
	}
	if n <= 0 {
		return nil, fmt.Errorf("postgres: SampleRowHashes: n must be > 0, got %d", n)
	}
	pkCols, err := singleOrCompositePKColumns(table)
	if err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s: %w", table.Name, err)
	}
	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = fmt.Sprintf("COALESCE(%s::text, '')", quoteIdent(c.Name))
	}
	pkExpr := strings.Join(quotedPKList(pkCols), ` || '|' || `)
	if len(pkCols) == 1 {
		pkExpr = fmt.Sprintf(`%s::text`, quoteIdent(pkCols[0]))
	}
	concatExpr := "CONCAT_WS('|', " + strings.Join(cols, ", ") + ")"
	var hashExpr string
	switch algo {
	case ir.HashSHA256:
		// PG 11+: sha256() is built-in, returns bytea; encode to hex
		// to match the MD5 shape.
		hashExpr = "ENCODE(SHA256(" + concatExpr + "::bytea), 'hex')"
	default:
		hashExpr = "MD5(" + concatExpr + ")"
	}
	q := fmt.Sprintf(
		`SELECT %s AS pk, %s AS hash FROM %s.%s ORDER BY MD5(%s || '%d') LIMIT %d`,
		pkExpr, hashExpr,
		quoteIdent(r.schema), quoteIdent(table.Name),
		pkExpr, seed, n,
	)
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s.%s: %w", r.schema, table.Name, err)
	}
	defer func() { _ = rows.Close() }()

	var samples []ir.SampledRowHash
	for rows.Next() {
		var pk, hash string
		if err := rows.Scan(&pk, &hash); err != nil {
			return nil, fmt.Errorf("postgres: SampleRowHashes scan %s.%s: %w", r.schema, table.Name, err)
		}
		samples = append(samples, ir.SampledRowHash{PrimaryKey: pk, Hash: hash})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes rows %s.%s: %w", r.schema, table.Name, err)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].PrimaryKey < samples[j].PrimaryKey })
	return samples, nil
}

// singleOrCompositePKColumns returns the PK column names for the
// table, or an error when sample-mode can't fire (no PK).
func singleOrCompositePKColumns(t *ir.Table) ([]string, error) {
	if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
		return nil, errors.New("table has no primary key (sample-mode requires a PK for deterministic sampling)")
	}
	cols := make([]string, len(t.PrimaryKey.Columns))
	for i, c := range t.PrimaryKey.Columns {
		cols[i] = c.Column
	}
	return cols, nil
}

// quotedPKList returns each name wrapped in PG identifier quotes.
func quotedPKList(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return out
}
