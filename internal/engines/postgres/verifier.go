// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
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
	// ADR-0173 Phase 1: AND the operator's `--where` filter for this table
	// into the COUNT so verify compares matching-source rows against the
	// filtered target subset. Empty when no filter is set for this table;
	// always parenthesized so a disjunctive predicate is fully scoped.
	q := fmt.Sprintf(`SELECT pg_catalog.COUNT(*) FROM %s.%s`,
		quoteIdent(r.schema), quoteIdent(table.Name))
	q += rowFilterWhereSQL(r.rowFilters[table.Name])
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
// **Key constraint**: the sampling SQL needs a key to construct the
// seed-derived ordering — cols.Key, the SOURCE table's primary key
// (single or composite). A keyless source table is refused by the
// orchestrator before this is called, and lands SKIPPED with a reason
// so the operator knows which tables verify only by count.
//
// Hash strategy: MD5 (default) or SHA-256 (operator opt-in via
// --strict-hash, v0.14.2+) of CONCAT_WS('|', <token>, ...) over the
// columns cols names, read by name in the order cols fixes — never in
// the table's declared order (VERIFY-SAMPLE-COLUMN-ORDER,
// [ir.SampleColumns]). Each token is [pgSampleToken]'s injective
// rendering of one column.
//
// SHA-256 path: PG 11+ ships sha256() in core (no pgcrypto needed),
// returning bytea. ENCODE(..., 'hex') gives a 64-char hex string
// matching the MD5() shape.
func (r *SchemaReader) SampleRowHashes(ctx context.Context, table *ir.Table, cols ir.SampleColumns, n int, seed int64, algo ir.HashAlgorithm) ([]ir.SampledRowHash, error) {
	if table == nil {
		return nil, errors.New("postgres: SampleRowHashes: table is nil")
	}
	if r.db == nil {
		return nil, errors.New("postgres: SampleRowHashes: reader not opened")
	}
	if n <= 0 {
		return nil, fmt.Errorf("postgres: SampleRowHashes: n must be > 0, got %d", n)
	}
	keyCols, err := sampleColumnNames(table, cols.Key)
	if err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s: key: %w", table.Name, err)
	}
	hashedCols, err := sampleColumnNames(table, cols.Hashed)
	if err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s: %w", table.Name, err)
	}
	if len(keyCols) == 0 || len(hashedCols) == 0 {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s: the plan names %d key and %d hashed columns; sample-mode needs at least one of each (a PK for deterministic sampling, and something to hash)",
			table.Name, len(keyCols), len(hashedCols))
	}
	tokens := make([]string, len(hashedCols))
	for i, c := range hashedCols {
		tokens[i] = pgSampleToken(c)
	}
	keys := make([]string, len(keyCols))
	for i, c := range keyCols {
		keys[i] = quoteIdent(c) + "::text"
	}
	pkExpr := strings.Join(keys, ` || '|' || `)
	// No NULL reaches CONCAT_WS (whose NULL-skipping is what made NULL
	// and '' collide before): every token renders NULL as 'N'.
	concatExpr := "pg_catalog.CONCAT_WS('|', " + strings.Join(tokens, ", ") + ")"
	var hashExpr string
	switch algo {
	case ir.HashSHA256:
		// PG 11+: sha256() is built-in, returns bytea; encode to hex
		// to match the MD5 shape.
		//
		// The text reaches sha256() through convert_to, NOT a `::bytea`
		// cast. The cast PARSES its input as a bytea literal, so a
		// backslash in any rendered value was an escape: '\101' and 'A'
		// became the same byte and hashed equal (a corrupted target
		// reported clean under --strict-hash, the operator's
		// higher-assurance mode), and a value carrying '\x' mid-string —
		// any bytea column, rendered '\xff00' — failed the whole table
		// with "invalid input syntax for type bytea". convert_to takes
		// the characters as they are.
		hashExpr = "pg_catalog.ENCODE(pg_catalog.SHA256(pg_catalog.CONVERT_TO(" + concatExpr + ", 'UTF8')), 'hex')"
	default:
		hashExpr = "pg_catalog.MD5(" + concatExpr + ")"
	}
	// ADR-0173 Phase 1: AND the operator's `--where` filter into the
	// FROM clause (before ORDER BY) so the sampled population is the
	// matching-source subset; always parenthesized.
	where := rowFilterWhereSQL(r.rowFilters[table.Name])
	q := fmt.Sprintf(
		`SELECT %s AS pk, %s AS hash FROM %s.%s%s ORDER BY pg_catalog.MD5(%s || '%d') LIMIT %d`,
		pkExpr, hashExpr,
		quoteIdent(r.schema), quoteIdent(table.Name), where,
		pkExpr, seed, n,
	)
	// The row-content hash is computed server-side over ::text
	// renderings, and float4/float8 text output depends on the SESSION's
	// extra_float_digits — inherited from each endpoint's
	// server/database/role default (Bug 194's verify face). Two
	// endpoints with different defaults (Supabase ships 0 server-wide;
	// stock PG ≥ 12 ships 1) render the SAME stored float differently —
	// a false mismatch — and, worse, a source at efd=0 renders a true
	// value identically to a target holding its rounded corruption — a
	// false clean. Pin the query's session to 3 (shortest-exact on every
	// version) so both sides hash canonical renderings.
	//
	// The pin rides an explicit transaction as a SET LOCAL (the F1
	// review finding): a statement-level SET is required (poolers strip
	// the GUC from startup packets — Supavisor), but a bare autocommit
	// SET followed by the query is not pooler-proof either — under
	// TRANSACTION-mode pooling (Supabase's recommended :6543 endpoint)
	// the two statements may land on DIFFERENT backends, silently
	// unpinning the hash query. A transaction pins one backend; SET
	// LOCAL scopes the pin to it, so nothing leaks back into the pool.
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s.%s: begin: %w", r.schema, table.Name, err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit
	if _, err := tx.ExecContext(ctx, "SET LOCAL extra_float_digits = 3"); err != nil {
		return nil, fmt.Errorf("postgres: SampleRowHashes %s.%s: pin extra_float_digits: %w", r.schema, table.Name, err)
	}
	rows, err := tx.QueryContext(ctx, q)
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

// sampleColumnNames checks every name against t's columns and returns
// names unchanged. A name t does not carry is an error: the
// orchestrator planned the comparison from both tables, so a miss here
// means the plan and the table disagree, and hashing fewer columns than
// planned would silently narrow what verify compares.
func sampleColumnNames(t *ir.Table, names []string) ([]string, error) {
	present := make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		present[c.Name] = struct{}{}
	}
	for _, n := range names {
		if _, ok := present[n]; !ok {
			return nil, fmt.Errorf("column %q is not on the table", n)
		}
	}
	return names, nil
}

// pgSampleToken renders one column's value for the sample-mode row
// hash. The rendering is INJECTIVE, which the old one — each column's
// ::text with NULL replaced by the empty string, joined by '|' — was
// not: NULL and the empty string rendered alike (a target holding an
// empty string where the source held NULL hashed clean), and a value
// could borrow its neighbour's separator ('a|' +
// 'b' against 'a' + '|b'). Now NULL is 'N', and every value is 'V',
// its character length, ':' and its text. Every Postgres type has a
// text output function, so unlike MySQL's utf8mb4 cast there is no
// value ::text cannot render; bytea renders as hex or escape text,
// both injective (the session's bytea_output only decides which).
func pgSampleToken(name string) string {
	text := quoteIdent(name) + "::text"
	return fmt.Sprintf("CASE WHEN %s IS NULL THEN 'N' ELSE 'V' || pg_catalog.length(%s)::text || ':' || %s END",
		quoteIdent(name), text, text)
}
