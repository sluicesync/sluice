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

// defaultCountChunkSize controls the chunked-count fallback for
// PlanetScale large tables. PS enforces a per-query row-read budget
// (~100K rows by default); 50K leaves a 2x safety margin. Configurable
// via SchemaReader.CountChunkSize when wired to the CLI / config; the
// constant is the conservative default.
const defaultCountChunkSize = 50000

// ExactRowCount implements [ir.Verifier]. Returns the exact row count
// for the given table.
//
// On vtgate flavors (PlanetScale / Vitess — [Flavor.usesVStream]) the
// PRIMARY path is [olapCount]: `SET workload='olap'` + `SELECT COUNT(*)`
// on a dedicated connection. OLAP streams the scan, so it is not bound by
// the OLTP max-statement-execution-time limit (MySQL errno 3024, ~900s)
// that kills a plain count(*) over a large clustered index — and it works
// for EVERY PK shape, closing the gap the single-integer-PK-only chunked
// path leaves for composite/string/UUID/no-PK tables (ADR-0147). On any
// OLAP failure we WARN and fall through to the tested chunked/single-shot
// path below (a safety net during the OLAP transition).
//
// The fall-through path is the ONLY path on vanilla MySQL (no vtgate
// `workload` var, no errno-3024 wall):
//   - **Single-shot SELECT COUNT(*)** when the table has no usable
//     integer PK (composite PK, no PK, or non-integer PK). Simple,
//     works against vanilla MySQL.
//   - **Chunked SUM(COUNT(*) per PK range)** when the table has a
//     single integer PK. Splits the count across PK ranges of
//     [defaultCountChunkSize] rows each. Scales to billions of rows.
//
// Authoritative count (vs. RowReader's CountRows which uses
// information_schema.tables.table_rows for ETA hints — that's
// approximate and lags actual cardinality on InnoDB tables that
// haven't been ANALYZE-d recently): `sluice verify` needs counts
// that won't silently disagree with what's stored, so we pay the
// full-scan cost.
//
// Returns (0, error) on any operational failure; (0, nil) is reserved
// for "table is empty."
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("mysql: ExactRowCount: table is nil")
	}
	if r.db == nil {
		return 0, errors.New("mysql: ExactRowCount: reader not opened")
	}
	// ADR-0173 Phase 1: a `--where` filter for this table is ANDed into
	// every count path so verify compares matching-source rows against the
	// filtered target subset. Empty when no filter is set for this table.
	predicate := r.rowFilters[table.Name]
	if r.flavor.usesVStream() {
		n, err := olapCount(ctx, r.db, table.Name, predicate)
		if err == nil {
			return n, nil
		}
		// Safety net: OLAP is the primary count path on vtgate flavors, but
		// keep the tested chunked/single-shot path reachable during the
		// transition (ADR-0147 tracks retiring the fallback once OLAP-count
		// is field-proven). Name the table so a persistent fallback is
		// visible in the logs rather than silent.
		slog.WarnContext(ctx, "mysql: OLAP row-count failed; falling back to chunked/single-shot count",
			"table", table.Name, "error", err)
	}
	pkCol, ok := singleIntegerPKColumn(table)
	if !ok {
		return singleShotCount(ctx, r.db, table.Name, predicate)
	}
	return chunkedCount(ctx, r.db, table.Name, pkCol, defaultCountChunkSize, predicate)
}

// whereAnd returns the SQL fragment that AND-composes an operator `--where`
// predicate (ADR-0173 Phase 1) onto a count query. ALWAYS parenthesized so
// a disjunctive predicate can't escape any existing bounds. lead is " WHERE "
// when the query has no WHERE yet, or " AND " when it already carries one.
// Empty predicate returns "".
func whereAnd(lead, predicate string) string {
	if predicate == "" {
		return ""
	}
	return lead + "(" + predicate + ")"
}

// SampleRowHashes implements [ir.SampleVerifier]. Same shape as the
// Postgres sibling — see that doc for the strategy + cross-engine
// constraint discussion.
//
// Columns are read by name in the order cols fixes, never in the
// table's declared order (VERIFY-SAMPLE-COLUMN-ORDER, [ir.SampleColumns]),
// and each one is rendered by [mysqlSampleToken] — an injective
// encoding, so NULL, the empty string, a value containing the
// separator, and binary data each hash distinctly.
//
// Hash algorithm: MD5 (default) or SHA-256 via SHA2(..., 256) when
// algo == HashSHA256. Both produce hex-string output of consistent
// width (32 chars MD5; 64 chars SHA-256).
func (r *SchemaReader) SampleRowHashes(ctx context.Context, table *ir.Table, cols ir.SampleColumns, n int, seed int64, algo ir.HashAlgorithm) ([]ir.SampledRowHash, error) {
	if table == nil {
		return nil, errors.New("mysql: SampleRowHashes: table is nil")
	}
	if r.db == nil {
		return nil, errors.New("mysql: SampleRowHashes: reader not opened")
	}
	if n <= 0 {
		return nil, fmt.Errorf("mysql: SampleRowHashes: n must be > 0, got %d", n)
	}
	keyCols, err := sampleColumnsOf(table, cols.Key)
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: key: %w", table.Name, err)
	}
	hashedCols, err := sampleColumnsOf(table, cols.Hashed)
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: %w", table.Name, err)
	}
	if len(keyCols) == 0 || len(hashedCols) == 0 {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: the plan names %d key and %d hashed columns; sample-mode needs at least one of each (a PK for deterministic sampling, and something to hash)",
			table.Name, len(keyCols), len(hashedCols))
	}
	tokens := make([]string, len(hashedCols))
	for i, c := range hashedCols {
		tokens[i] = mysqlSampleToken(c, algo)
	}
	keys := make([]string, len(keyCols))
	for i, c := range keyCols {
		keys[i] = mysqlSampleKeyText(c)
	}
	pkSelect := keys[0]
	if len(keys) > 1 {
		pkSelect = "CONCAT_WS('|', " + strings.Join(keys, ", ") + ")"
	}
	// CONCAT, not CONCAT_WS: CONCAT_WS SKIPS a NULL argument, so a token
	// that went NULL would silently drop out of the hash. No token is
	// meant to — NULL renders 'N' — so CONCAT makes one that did null the
	// whole hash, which the scan below refuses naming the row.
	concatExpr := "CONCAT(" + strings.Join(tokens, ", '|', ") + ")"
	hashExpr := mysqlDigest(concatExpr, algo)
	q := fmt.Sprintf(
		"SELECT %s AS pk, %s AS hash FROM %s%s ORDER BY MD5(CONCAT(%s, '%d')) LIMIT %d",
		pkSelect, hashExpr,
		quoteIdent(table.Name),
		whereAnd(" WHERE ", r.rowFilters[table.Name]),
		pkSelect, seed, n,
	)
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: %w", table.Name, err)
	}
	defer func() { _ = rows.Close() }()

	var samples []ir.SampledRowHash
	for rows.Next() {
		var pk string
		var hash sql.NullString
		if err := rows.Scan(&pk, &hash); err != nil {
			return nil, fmt.Errorf("mysql: SampleRowHashes scan %s: %w", table.Name, err)
		}
		if !hash.Valid {
			return nil, fmt.Errorf("mysql: SampleRowHashes %s: row %s: the row's hash input rendered NULL (a column's rendering came back NULL for a non-NULL value); refusing rather than hashing fewer columns",
				table.Name, pk)
		}
		samples = append(samples, ir.SampledRowHash{PrimaryKey: pk, Hash: hash.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes rows %s: %w", table.Name, err)
	}
	ir.SortSampledRowHashes(samples)
	return samples, nil
}

// olapCount returns SELECT COUNT(*) FROM table executed under vtgate's
// `workload = 'olap'` on a DEDICATED connection. OLAP streams the scan, so
// the count is NOT bound by the OLTP max-statement-execution-time limit
// (MySQL errno 3024, ~900s) that kills a plain count(*) over a large
// clustered index on PlanetScale/Vitess; the optimizer still auto-narrows to
// a small secondary index when one exists. It works for EVERY PK shape,
// which is why it supersedes the single-integer-PK-only [chunkedCount] on
// vtgate flavors (ADR-0147).
//
// The `SET workload` is scoped to the dedicated connection and NEVER leaked
// into a pooled/session-wide connection (the v0.99.15 lesson) — mirroring
// [RowReader.queryFullScan]'s no-PK full scan: the conn is returned to the
// pool on Close, and the go-sql-driver's session reset
// (COM_RESET_CONNECTION) clears the session workload before any reuse.
//
// count(*) is exact in any workload mode — OLAP changes the
// timeout/streaming behavior, not the result — so there is no value-fidelity
// risk here.
func olapCount(ctx context.Context, db *sql.DB, tableName, predicate string) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("mysql: olap-count conn %s: %w", tableName, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET workload = 'olap'"); err != nil {
		return 0, fmt.Errorf("mysql: olap-count set workload %s: %w", tableName, err)
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(tableName)) + whereAnd(" WHERE ", predicate)
	var count int64
	if err := conn.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: olap-count %s: %w", tableName, err)
	}
	return count, nil
}

// singleShotCount returns SELECT COUNT(*) FROM table. Fast on small tables.
// On a large clustered table on PlanetScale/Vitess it can hit the OLTP
// max-statement-execution-time wall (MySQL errno 3024) — that is a
// statement-*execution-time* limit, NOT a rows-returned cap (a count(*)
// returns one row); the OLAP primary path in [SchemaReader.ExactRowCount]
// exists specifically to stream past it (ADR-0147).
func singleShotCount(ctx context.Context, db *sql.DB, tableName, predicate string) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(tableName)) + whereAnd(" WHERE ", predicate)
	var count int64
	if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: count %s: %w", tableName, err)
	}
	return count, nil
}

// chunkedCount sums COUNT(*) across PK ranges of chunkSize rows so
// each subquery stays under PlanetScale's per-query row-read budget.
// Cost: ⌈row_count / chunkSize⌉ + 1 queries (the +1 is for MIN/MAX).
// For a 1M-row table at chunkSize=50000, that's 21 queries — fast on
// any modern MySQL, well under PS's per-query budget.
func chunkedCount(ctx context.Context, db *sql.DB, tableName, pkCol string, chunkSize int64, predicate string) (int64, error) {
	// Get PK bounds. The `--where` predicate (ADR-0173) is deliberately NOT
	// applied to the MIN/MAX bounds — the filtered rows still fall inside
	// [min,max], and each per-chunk COUNT below ANDs the predicate, so the
	// total counts exactly the matching rows (a filtered-out chunk simply
	// contributes zero).
	boundsQ := fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`,
		quoteIdent(pkCol), quoteIdent(pkCol), quoteIdent(tableName))
	var minV, maxV sql.NullInt64
	if err := db.QueryRowContext(ctx, boundsQ).Scan(&minV, &maxV); err != nil {
		return 0, fmt.Errorf("mysql: chunked-count bounds %s: %w", tableName, err)
	}
	if !minV.Valid || !maxV.Valid {
		return 0, nil // empty table
	}
	andPred := whereAnd(" AND ", predicate)
	// Walk PK ranges. Half-open interval [start, end); the final
	// chunk uses <= maxV to include the maximum row.
	var total int64
	for start := minV.Int64; start <= maxV.Int64; start += chunkSize {
		end := start + chunkSize
		var partial int64
		var partQ string
		if end > maxV.Int64 {
			partQ = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s <= ?`,
				quoteIdent(tableName), quoteIdent(pkCol), quoteIdent(pkCol)) + andPred
			if err := db.QueryRowContext(ctx, partQ, start, maxV.Int64).Scan(&partial); err != nil {
				return 0, fmt.Errorf("mysql: chunked-count partial [%d..%d] %s: %w", start, maxV.Int64, tableName, err)
			}
		} else {
			partQ = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s < ?`,
				quoteIdent(tableName), quoteIdent(pkCol), quoteIdent(pkCol)) + andPred
			if err := db.QueryRowContext(ctx, partQ, start, end).Scan(&partial); err != nil {
				return 0, fmt.Errorf("mysql: chunked-count partial [%d..%d) %s: %w", start, end, tableName, err)
			}
		}
		total += partial
	}
	return total, nil
}

// singleIntegerPKColumn reports whether the table has a single
// integer-typed primary key. Tables with composite PKs, no PK, or
// non-integer PK return ok=false; the caller falls back to single-
// shot counting.
func singleIntegerPKColumn(t *ir.Table) (string, bool) {
	if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) != 1 {
		return "", false
	}
	pkName := t.PrimaryKey.Columns[0].Column
	for _, c := range t.Columns {
		if c.Name != pkName {
			continue
		}
		// Storage type, not declared type (Bug 233). Cross-engine verify
		// hands this the SOURCE table, so a PG `CREATE DOMAIN d AS
		// bigint` primary key reaches here wrapped while the MySQL column
		// it counts is an ordinary BIGINT. Missing the unwrap costs only
		// speed (the caller falls back to single-shot counting, correct
		// for every type), which is exactly why it would never have been
		// noticed.
		if _, ok := ir.UnwrapDomain(c.Type).(ir.Integer); ok {
			return pkName, true
		}
		return "", false
	}
	return "", false
}

// sampleColumnsOf resolves names against t's columns, in the order
// names gives. A name t does not carry is an error: the orchestrator
// planned the comparison from both tables, so a miss here means the
// plan and the table disagree, and hashing fewer columns than planned
// would silently narrow what verify compares.
func sampleColumnsOf(t *ir.Table, names []string) ([]*ir.Column, error) {
	byName := make(map[string]*ir.Column, len(t.Columns))
	for _, c := range t.Columns {
		byName[c.Name] = c
	}
	out := make([]*ir.Column, len(names))
	for i, n := range names {
		c, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("column %q is not on the table", n)
		}
		out[i] = c
	}
	return out, nil
}

// mysqlSampleToken renders one column's value for the sample-mode row
// hash: 'N' for NULL, else a one-letter tag and a fixed-width digest of
// the value. The old rendering — each column cast to CHAR with NULL
// replaced by the empty string, joined by '|' — read a corrupted target
// as clean in four ways, each now closed:
//
//   - NULL and the empty string rendered alike. Now NULL is 'N' and
//     every value is 'V'- or 'X'-tagged.
//   - A value could borrow its neighbour's separator ('a|' + 'b' against
//     'a' + '|b'). A fixed-width digest per column cannot.
//   - Binary data was not rendered at all. Under the utf8mb4 connection
//     charset sluice always uses, MySQL's CAST(<bytes> AS CHAR) is NULL —
//     with only a warning — for any byte sequence that is not valid
//     UTF-8: measured on 8.0 for GEOMETRY POINT(1 2) (a WKB double is
//     rarely valid UTF-8), BIT(8) b'11111111' and VARBINARY 0x61FF62. Each
//     rendered as the empty string, so any two such values hashed equal.
//     The binary family ([mysqlSampleBinary]) is now digested from its raw
//     bytes ('X'). That type dispatch is what the guarantee rests on:
//     MariaDB's CAST does not return NULL there but substitutes '?' (11.4:
//     0x61FF62 → 'a?b'), so for a binary value mis-typed as text the
//     per-row CAST-is-NULL fallback below is a MySQL-only backstop, not a
//     guarantee. Bytes declared as text (CHARACTER SET binary) map to the
//     binary family and are pinned on both servers.
//   - FLOAT was rendered at 6 significant digits (see [mysqlSampleText]).
//
// Each value is digested on its own, and that is load-bearing, not
// tidiness: MySQL's CONCAT returns NULL — warning 1301, nothing louder —
// when its result would exceed max_allowed_packet, so a length-prefixed
// text token for a large value went NULL, CONCAT_WS skipped it, and two
// different LONGTEXT values hashed equal. A digest is 32 or 64 hex
// characters whatever the value's size.
func mysqlSampleToken(c *ir.Column, algo ir.HashAlgorithm) string {
	q := quoteIdent(c.Name)
	bytesDigest := "CONCAT('X', " + mysqlDigest(q, algo) + ")"
	if mysqlSampleBinary(c.Type) {
		return fmt.Sprintf("IF(%s IS NULL, 'N', %s)", q, bytesDigest)
	}
	text := mysqlSampleText(c)
	return fmt.Sprintf("CASE WHEN %s IS NULL THEN 'N' WHEN %s IS NULL THEN %s%s ELSE CONCAT('V', %s) END",
		q, text, bytesDigest, mysqlJSONTooLargeArm(c, text), mysqlDigest(text, algo))
}

// mysqlJSONTooLargeArm is the one place MySQL's oversized-value failure
// is NOT a NULL. Past max_allowed_packet, MySQL 8 renders a JSON value
// through CAST(… AS CHAR) as the EMPTY STRING (warning 1301) — measured
// on 8.0 with two different 20,000-character documents under a 16 KiB
// packet: both cast to length 0 and digested to d41d8cd9…, so they hashed
// equal and neither the CAST-is-NULL fallback nor the row refusal fired.
// No valid JSON document is empty, so an empty rendering of a non-NULL
// JSON value is turned into a NULL token, which nulls the row's hash and
// refuses the table naming the row. MariaDB's JSON is a LONGTEXT alias
// whose oversized values already go NULL; the arm is harmless there.
func mysqlJSONTooLargeArm(c *ir.Column, text string) string {
	if _, ok := ir.UnwrapDomain(c.Type).(ir.JSON); !ok {
		return ""
	}
	return " WHEN CHAR_LENGTH(" + text + ") = 0 THEN NULL"
}

// mysqlSampleText is the exact text rendering of a non-binary column.
//
// Single-precision FLOAT is read through `(col * 1E0)`: MySQL and MariaDB
// do NOT round-trip FLOAT in CAST(… AS CHAR) — they print 6 significant
// digits, so 1234567 and 1234568 both render 1234570 and 0.1234567 and
// 0.1234568 both render 0.123457, and a sample hash over that rendering
// reads a corrupted FLOAT as clean. The promotion to DOUBLE is exact and
// DOUBLE prints shortest-round-trip. `* 1E0`, not `+ 0E0`: addition loses
// the sign of a negative zero. Same expression, for the same reason, as
// the row reader's [selectColumnExpr].
func mysqlSampleText(c *ir.Column) string {
	q := quoteIdent(c.Name)
	if f, ok := ir.UnwrapDomain(c.Type).(ir.Float); ok && f.Precision == ir.FloatSingle {
		return "CAST((" + q + " * 1E0) AS CHAR)"
	}
	return "CAST(" + q + " AS CHAR)"
}

// mysqlSampleKeyText renders one key column for the sampled row's key
// string — readable text where the value has one (it is what the
// mismatch report prints), the bytes in hex where it does not (a
// BINARY(16) UUID key cast to utf8mb4 is NULL for most values, which
// would collapse distinct keys into one). A FLOAT key goes through the
// same exact rendering as a FLOAT value; at 6 digits two keys collapse.
func mysqlSampleKeyText(c *ir.Column) string {
	q := quoteIdent(c.Name)
	if mysqlSampleBinary(c.Type) {
		return "HEX(" + q + ")"
	}
	return fmt.Sprintf("COALESCE(%s, HEX(%s))", mysqlSampleText(c), q)
}

// mysqlSampleBinary reports the column families whose values are bytes
// rather than text, and so must never be rendered through a utf8mb4 cast.
func mysqlSampleBinary(t ir.Type) bool {
	switch ir.UnwrapDomain(t).(type) {
	case ir.Binary, ir.Varbinary, ir.Blob, ir.Bit, ir.Geometry:
		return true
	}
	return false
}

// mysqlDigest wraps expr in the hex digest algo selects.
func mysqlDigest(expr string, algo ir.HashAlgorithm) string {
	if algo == ir.HashSHA256 {
		return "SHA2(" + expr + ", 256)"
	}
	return "MD5(" + expr + ")"
}
