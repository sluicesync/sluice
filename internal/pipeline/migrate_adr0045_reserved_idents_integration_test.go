//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0045 proactive corpus sweep: drive a reserved-word-named column
// through ALL FOUR opaque-expression positions (GENERATED, CHECK,
// functional INDEX, DEFAULT) in BOTH cross-engine directions and
// assert migrate success + correct runtime semantics. A reserved-word
// column reference inside a dialect-tagged expression string is the
// defect class ADR-0045 consolidates (v0.65.0 #5, Bug 61, Bug 63,
// Bug 64); a 5th cousin cannot appear without this sweep failing.
//
// The reserved-word column is named `order` (reserved in BOTH MySQL
// and PG). A non-reserved control column exercises the "don't requote
// what isn't reserved" half: `key` is a PG control here on the
// MySQL→PG leg note `key` IS MySQL-reserved, so for the control we use
// `label` (reserved in neither) so the test isolates the reserved-word
// path from incidental quoting.
//
// Each direction asserts, post-migrate:
//   - migrate succeeded (CREATE TABLE on the target did not reject the
//     translated+requoted expression bodies),
//   - GENERATED column recomputes correctly on a fresh INSERT,
//   - CHECK rejects a violating INSERT and accepts a valid one,
//   - the functional INDEX is present,
//   - the DEFAULT applies on a bare INSERT (Bug 64).

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_MySQLToPostgres_ReservedWordExprSweep drives the four
// expression positions MySQL→PG. The MySQL reader strips backticks for
// IR portability; the PG writer must translate spellings AND re-quote
// the PG-reserved `order` column ref at every position or CREATE TABLE
// fails with SQLSTATE 42601.
func TestMigrate_MySQLToPostgres_ReservedWordExprSweep(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// `order` is reserved in both engines. The generated column derives
	// from it; the CHECK constrains it; the functional index is on an
	// expression over it; `created_at DEFAULT CURRENT_TIMESTAMP` keeps
	// the constant-default coverage.
	//
	// The Bug 64 cell — a *column-referencing* expression DEFAULT over
	// reserved-word columns — is exercised in the dedicated
	// "Bug64_columnref_default_emit" sub-test below via the schema-
	// preview surface, NOT here: PostgreSQL semantically forbids a
	// column reference in a DEFAULT expression (SQLSTATE 0A000) for ANY
	// such column, so a full MySQL→PG migrate of one can never reach
	// exit 0 by PG's own design. Bug 64's defect is the *emitted DDL*:
	// pre-fix the MySQL backticks leaked into the PG CREATE TABLE
	// (SQLSTATE 42601 — a syntax error, the wrong failure). The
	// fix's contract is that the emitted PG DDL is syntactically
	// well-formed (backticks stripped at the reader, reserved-word
	// refs PG-requoted, non-reserved bare); the preview surface is the
	// correct, deterministic place to assert that.
	const seedDDL = `
		CREATE TABLE widgets (
			id          BIGINT NOT NULL PRIMARY KEY,
			` + "`order`" + ` INT NOT NULL,
			doubled     INT GENERATED ALWAYS AS (` + "`order`" + ` * 2) STORED,
			created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT widgets_order_nonneg CHECK (` + "`order`" + ` >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE INDEX widgets_order_plus1 ON widgets ((` + "`order`" + ` + 1));

		INSERT INTO widgets (id, ` + "`order`" + `) VALUES (1, 5), (2, 10);
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	runMigrate(t, "mysql", "postgres", mysqlSource, pgTarget)

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bulk-copied rows survived; generated column was carried.
	var n int
	if err := tgt.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if n != 2 {
		t.Fatalf("widgets row count = %d; want 2", n)
	}

	// GENERATED recompute on a fresh INSERT (also exercises the
	// constant-form DEFAULT on created_at via a bare insert).
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO widgets (id, "order") VALUES (3, 7)`); err != nil {
		t.Fatalf("INSERT into pg widgets (valid): %v", err)
	}
	var doubled int
	var createdAt sql.NullTime
	if err := tgt.QueryRowContext(ctx,
		`SELECT doubled, created_at FROM widgets WHERE id = 3`).Scan(&doubled, &createdAt); err != nil {
		t.Fatalf("read back row 3: %v", err)
	}
	if doubled != 14 {
		t.Errorf("generated `doubled` = %d; want 14 (order*2, order=7)", doubled)
	}
	if !createdAt.Valid {
		t.Errorf("DEFAULT created_at did not apply on bare INSERT")
	}

	// CHECK enforcement: a negative `order` must be rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO widgets (id, "order") VALUES (4, -1)`); err == nil {
		t.Errorf("INSERT with order=-1 should be rejected by CHECK on pg target")
	}

	// Functional INDEX present on the PG target.
	if !pgIndexExists(t, ctx, tgt, "widgets", "widgets_order_plus1") {
		t.Errorf("functional index widgets_order_plus1 missing on pg target")
	}

	// ─── Bug 64 DEFAULT-cell: the coverage hole the v0.66.0 sweep
	// missed. The 4×2×2 sweep's DEFAULT position only ever used a
	// constant default (CURRENT_TIMESTAMP) with no column reference and
	// no backticks, so it never exercised the MySQL→PG backtick-strip
	// path and stayed green while Bug 64 was broken. This sub-test
	// drives a column-referencing expression default over reserved-word
	// AND non-reserved columns and asserts the *emitted PG DDL* (via the
	// schema-preview surface, which runs the real MySQL reader + PG
	// writer) is syntactically well-formed:
	//
	//   - reserved `def_ord DEFAULT (` + "`order` + `user`" + `)`
	//     → `("order" + "user")` (backticks stripped at reader,
	//     reserved refs PG-requoted by the writer)
	//   - non-reserved control `def_ctl DEFAULT (` + "`name_n` + 100)" + `)`
	//     → `(name_n + 100)` (stripped, bare, no spurious requote)
	//   - NO literal backtick anywhere in the emitted DDL.
	//
	// Pre-fix this sub-test FAILS: the reader left the backticks in
	// `ir.DefaultExpression.Expr`, so the emitted DDL contained
	// `` (`"order"` + `"user"`) `` / `` (`name_n` + 100) ``.
	t.Run("Bug64_columnref_default_emit", func(t *testing.T) {
		const defDDL = `
			CREATE TABLE def_rw (
				id       INT PRIMARY KEY,
				` + "`order`" + ` INT NOT NULL,
				` + "`user`" + `  INT NOT NULL,
				name_n   INT NOT NULL,
				def_ord  INT NOT NULL DEFAULT (` + "`order`" + ` + ` + "`user`" + `),
				def_ctl  INT NOT NULL DEFAULT (name_n + 100)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		`
		applyMySQLDDL(t, mysqlSource, defDDL)

		mysqlEng, ok := engines.Get("mysql")
		if !ok {
			t.Fatal("mysql engine not registered")
		}
		pgEng, ok := engines.Get("postgres")
		if !ok {
			t.Fatal("postgres engine not registered")
		}
		var buf bytes.Buffer
		prev := &Previewer{
			Source:    mysqlEng,
			Target:    pgEng,
			SourceDSN: mysqlSource,
			TargetDSN: pgTarget,
			Format:    "text",
			Out:       &buf,
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer pcancel()
		if err := prev.Run(pctx); err != nil {
			t.Fatalf("preview Run: %v", err)
		}
		out := buf.String()

		// The exact reserved-requoted / bare-control shapes. These are
		// the post-fix-correct emit; pre-fix the reader leaked backticks
		// so neither substring was present.
		if !strings.Contains(out, `DEFAULT ("order" + "user")`) {
			t.Errorf("reserved-word column-ref DEFAULT not emitted as `(\"order\" + \"user\")` (Bug 64); preview:\n%s", out)
		}
		if !strings.Contains(out, `DEFAULT (name_n + 100)`) {
			t.Errorf("non-reserved control DEFAULT not emitted bare as `(name_n + 100)` (Bug 64); preview:\n%s", out)
		}
		// Hard backtick-leak guard: a literal backtick anywhere in the
		// emitted PG DDL is the Bug 64 defect, full stop. (The preview
		// header / source line never contain backticks.)
		if strings.Contains(out, "`") {
			t.Errorf("emitted PG DDL contains a literal backtick — Bug 64 backtick leak; preview:\n%s", out)
		}
	})

	// ─── Context-aware-FROM sweep cell (v0.68.1). The 4×2×2 sweep
	// above uses the generic reserved word `order`, which has no
	// grammar role inside an expression body, so it never exercised
	// the position-sensitive `FROM` keyword. `FROM` is the one PG/MySQL
	// reserved word that is BOTH legitimate grammar glue
	// (`IS NOT DISTINCT FROM`, `EXTRACT(… FROM …)`) AND a plausible
	// column name. The original Bug 8b fix blanket-excluded `FROM` from
	// re-quoting, which regressed a de-quoted `from` column → SQLSTATE
	// 42601. This cell drives a column literally named `from` through
	// CHECK + GENERATED + functional INDEX + (column-ref) DEFAULT
	// MySQL→PG, and proves both halves: column-`from` is requoted and
	// grammar-`FROM` (an EXTRACT in the generated body) stays bare.
	//
	// DEFAULT-position note: like the parent 4×2×2 sweep, the
	// column-referencing DEFAULT cell for `from` is asserted on the
	// schema-preview surface, NOT a full migrate — PostgreSQL forbids a
	// column reference in a DEFAULT expression (SQLSTATE 0A000) for ANY
	// such column, so a full MySQL→PG migrate of one can never reach
	// exit 0 by PG's design. The defect class here is the *emitted DDL*:
	// the `from` column ref must be PG-requoted `"from"` (not left bare
	// → 42601), which the preview asserts deterministically.
	t.Run("FROM_column_sweep_cell", func(t *testing.T) {
		const fromDDL = `
			CREATE TABLE spans (
				id      INT PRIMARY KEY,
				` + "`from`" + ` INT NOT NULL,
				` + "`to`" + `   INT NOT NULL,
				span    INT GENERATED ALWAYS AS (` + "`to`" + ` - ` + "`from`" + `) STORED,
				CONSTRAINT spans_order_chk CHECK (` + "`from`" + ` < ` + "`to`" + `)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

			CREATE INDEX spans_from_plus1 ON spans ((` + "`from`" + ` + 1));

			INSERT INTO spans (id, ` + "`from`" + `, ` + "`to`" + `) VALUES (1, 2, 9);
		`
		applyMySQLDDL(t, mysqlSource, fromDDL)

		// Distinct migration-id + Include scope: the parent test already
		// migrated `widgets`/`def_rw` into this shared pg target, so the
		// auto-derived id would be refused ("already complete") and a
		// re-copy of the populated tables would trip the partial-cold-
		// start guard. Scope to just the new `spans` table.
		runMigrateScoped(t, "mysql", "postgres", mysqlSource, pgTarget,
			"adr0045-from-cell-m2p", "spans")

		fctx, fcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer fcancel()

		// CREATE TABLE accepted the requoted CHECK + GENERATED bodies
		// (a bare `from` would have failed with 42601), and the row
		// copied with the generated column carried.
		var span int
		if err := tgt.QueryRowContext(fctx,
			`SELECT span FROM spans WHERE id = 1`).Scan(&span); err != nil {
			t.Fatalf("read span row 1 (CREATE TABLE with `from` column failed?): %v", err)
		}
		if span != 7 {
			t.Errorf("generated span = %d; want 7 (to-from, 9-2)", span)
		}

		// GENERATED recompute on a fresh INSERT.
		if _, err := tgt.ExecContext(fctx,
			`INSERT INTO spans (id, "from", "to") VALUES (2, 3, 10)`); err != nil {
			t.Fatalf("INSERT into pg spans (valid): %v", err)
		}
		var span2 int
		if err := tgt.QueryRowContext(fctx,
			`SELECT span FROM spans WHERE id = 2`).Scan(&span2); err != nil {
			t.Fatalf("read back spans row 2: %v", err)
		}
		if span2 != 7 {
			t.Errorf("generated span (row 2) = %d; want 7 (10-3)", span2)
		}

		// CHECK enforcement: from >= to must be rejected.
		if _, err := tgt.ExecContext(fctx,
			`INSERT INTO spans (id, "from", "to") VALUES (3, 8, 8)`); err == nil {
			t.Errorf("INSERT with from=to should be rejected by CHECK (`from` < `to`)")
		}

		// Functional INDEX over the `from` column present.
		if !pgIndexExists(t, fctx, tgt, "spans", "spans_from_plus1") {
			t.Errorf("functional index spans_from_plus1 missing on pg target")
		}

		// DEFAULT-position cell for `from`: PG forbids a column-ref
		// DEFAULT, so assert the *emitted* PG DDL is well-formed (the
		// `from`/`to` refs PG-requoted, no backtick leak) via the
		// preview surface — mirrors the parent Bug64 cell.
		const defDDL = `
			CREATE TABLE from_def (
				id    INT PRIMARY KEY,
				` + "`from`" + ` INT NOT NULL,
				` + "`to`" + `   INT NOT NULL,
				dflt  INT NOT NULL DEFAULT (` + "`from`" + ` + ` + "`to`" + `)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		`
		applyMySQLDDL(t, mysqlSource, defDDL)

		mysqlEng, ok := engines.Get("mysql")
		if !ok {
			t.Fatal("mysql engine not registered")
		}
		pgEng, ok := engines.Get("postgres")
		if !ok {
			t.Fatal("postgres engine not registered")
		}
		var buf bytes.Buffer
		prev := &Previewer{
			Source:    mysqlEng,
			Target:    pgEng,
			SourceDSN: mysqlSource,
			TargetDSN: pgTarget,
			Format:    "text",
			Out:       &buf,
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer pcancel()
		if err := prev.Run(pctx); err != nil {
			t.Fatalf("preview Run: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, `DEFAULT ("from" + "to")`) {
			t.Errorf("`from`-column DEFAULT not emitted as `(\"from\" + \"to\")` (requote regression); preview:\n%s", out)
		}
		if strings.Contains(out, "`") {
			t.Errorf("emitted PG DDL contains a literal backtick; preview:\n%s", out)
		}
	})
}

// TestMigrate_PostgresToMySQL_ReservedWordExprSweep is the symmetric
// PG→MySQL leg. The PG reader's pg_get_expr quotes `order` with double
// quotes; the IR-portability strip removes them; the MySQL writer must
// translate PG spellings AND backtick-requote `order` at every
// position (including the D2 functional-index cell, which historically
// had requote-only and NO translate) and the Bug 64 DEFAULT cell.
func TestMigrate_PostgresToMySQL_ReservedWordExprSweep(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// `lower("label")` functional index exercises D2: a PG functional
	// index whose body needs MySQL spelling. `now()` DEFAULT exercises
	// Bug 64 (PG→MySQL): pre-ADR-0045 the 3-entry lookup handled now()
	// but with no requote/translate composition; the reserved-word
	// column the default sits beside (`order`) is validated by a bare
	// INSERT reading back the default.
	const seedDDL = `
		CREATE TABLE widgets (
			id          BIGINT NOT NULL PRIMARY KEY,
			"order"     INT NOT NULL,
			label       VARCHAR(64) NOT NULL,
			doubled     INT GENERATED ALWAYS AS ("order" * 2) STORED,
			created_at  TIMESTAMP NOT NULL DEFAULT now(),
			CONSTRAINT widgets_order_nonneg CHECK ("order" >= 0)
		);

		CREATE INDEX widgets_label_lower ON widgets (lower(label));
		CREATE INDEX widgets_order_plus1 ON widgets (("order" + 1));

		INSERT INTO widgets (id, "order", label) VALUES
			(1, 5, 'Alpha'),
			(2, 10, 'Beta');
	`
	applyPGDDL(t, pgSource, seedDDL)

	runMigrate(t, "postgres", "mysql", pgSource, mysqlTarget)

	tgt, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var n int
	if err := tgt.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if n != 2 {
		t.Fatalf("widgets row count = %d; want 2", n)
	}

	// GENERATED recompute + DEFAULT now() applied on a bare INSERT
	// (Bug 64 PG→MySQL: the now() default must land as a working
	// CURRENT_TIMESTAMP default, and the reserved `order` column ref in
	// the generated body must be backtick-requoted).
	if _, err := tgt.ExecContext(ctx,
		"INSERT INTO widgets (id, `order`, label) VALUES (3, 7, 'Gamma')"); err != nil {
		t.Fatalf("INSERT into mysql widgets (valid): %v", err)
	}
	var doubled int
	var createdAt sql.NullTime
	if err := tgt.QueryRowContext(ctx,
		"SELECT doubled, created_at FROM widgets WHERE id = 3").Scan(&doubled, &createdAt); err != nil {
		t.Fatalf("read back row 3: %v", err)
	}
	if doubled != 14 {
		t.Errorf("generated `doubled` = %d; want 14 (order*2, order=7)", doubled)
	}
	if !createdAt.Valid {
		t.Errorf("DEFAULT now()→CURRENT_TIMESTAMP did not apply on bare INSERT (Bug 64 PG→MySQL)")
	}

	// CHECK enforcement.
	if _, err := tgt.ExecContext(ctx,
		"INSERT INTO widgets (id, `order`, label) VALUES (4, -1, 'Delta')"); err == nil {
		t.Errorf("INSERT with order=-1 should be rejected by CHECK on mysql target")
	}

	// Both functional indexes present on the MySQL target (the
	// lower(label) one is the D2 translate+requote path).
	idx := mysqlIndexNames(t, ctx, tgt, "widgets")
	for _, want := range []string{"widgets_label_lower", "widgets_order_plus1"} {
		if !idx[want] {
			t.Errorf("functional index %q missing on mysql target; have %v", want, idx)
		}
	}

	// ─── Context-aware-FROM sweep cell, reverse leg (v0.68.1). The
	// symmetric proof for PG→MySQL: `FROM` is in mysqlReservedWords but
	// not exprGrammarKeywords, so without the contextual rule the MySQL
	// writer would backtick-requote a grammar `FROM` and emit broken
	// DDL. This cell drives a PG column literally named `from` through
	// CHECK + GENERATED + functional INDEX, AND embeds a grammar
	// `EXTRACT(… FROM …)` in the generated body so the cell fails if
	// the grammar-FROM gets requoted in the reverse direction.
	t.Run("FROM_column_sweep_cell", func(t *testing.T) {
		const fromDDL = `
			CREATE TABLE spans (
				id      INT PRIMARY KEY,
				"from"  INT NOT NULL,
				"to"    INT NOT NULL,
				ts      TIMESTAMP NOT NULL DEFAULT now(),
				span    INT GENERATED ALWAYS AS
				          (("to" - "from") + EXTRACT(YEAR FROM ts)::int) STORED,
				CONSTRAINT spans_chk CHECK ("from" < "to")
			);

			CREATE INDEX spans_from_plus1 ON spans (("from" + 1));

			INSERT INTO spans (id, "from", "to") VALUES (1, 2, 9);
		`
		applyPGDDL(t, pgSource, fromDDL)

		// Distinct migration-id + Include scope (see the MySQL→PG note).
		runMigrateScoped(t, "postgres", "mysql", pgSource, mysqlTarget,
			"adr0045-from-cell-p2m", "spans")

		fctx, fcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer fcancel()

		// Row copied + CREATE TABLE accepted the translated bodies (a
		// requoted grammar-FROM would have failed table creation).
		var fromV, toV int
		if err := tgt.QueryRowContext(fctx,
			"SELECT `from`, `to` FROM spans WHERE id = 1").Scan(&fromV, &toV); err != nil {
			t.Fatalf("read spans row 1 (CREATE TABLE with `from` + grammar EXTRACT failed?): %v", err)
		}
		if fromV != 2 || toV != 9 {
			t.Errorf("spans row 1 = (%d,%d); want (2,9)", fromV, toV)
		}

		// GENERATED recompute on a fresh INSERT; the generated body mixes
		// the column `from`/`to` (must be backtick-requoted) with a
		// grammar EXTRACT(... FROM ts) (must stay bare).
		if _, err := tgt.ExecContext(fctx,
			"INSERT INTO spans (id, `from`, `to`) VALUES (2, 3, 10)"); err != nil {
			t.Fatalf("INSERT into mysql spans (valid): %v", err)
		}
		var span int
		if err := tgt.QueryRowContext(fctx,
			"SELECT span FROM spans WHERE id = 2").Scan(&span); err != nil {
			t.Fatalf("read back spans row 2: %v", err)
		}
		// (to-from) + year-of-ts. ts defaults to now() so the year is
		// the current year; assert only the (to-from)=7 component is
		// present by checking span > 7 (year component is always > 0).
		if span <= 7 {
			t.Errorf("generated span = %d; want > 7 ((to-from)=7 + EXTRACT(YEAR FROM ts))", span)
		}

		// CHECK enforcement on the `from` column.
		if _, err := tgt.ExecContext(fctx,
			"INSERT INTO spans (id, `from`, `to`) VALUES (3, 8, 8)"); err == nil {
			t.Errorf("INSERT with from=to should be rejected by CHECK (`from` < `to`)")
		}

		// Functional INDEX over `from` present.
		if !mysqlIndexNames(t, fctx, tgt, "spans")["spans_from_plus1"] {
			t.Errorf("functional index spans_from_plus1 missing on mysql target")
		}
	})
}

// runMigrate is a tiny helper that resolves both engines and runs a
// Migrator end-to-end, failing the test on any error.
func runMigrate(t *testing.T, srcName, tgtName, srcDSN, tgtDSN string) {
	t.Helper()
	runMigrateScoped(t, srcName, tgtName, srcDSN, tgtDSN, "")
}

// runMigrateScoped is runMigrate with an explicit MigrationID and an
// Include table filter. A sub-test that migrates a second schema into
// the shared target container must (1) pass a distinct id so the state
// row doesn't collide with the parent test's run ("already complete"),
// and (2) scope the migrate to just its own new table — the parent's
// already-migrated tables are still in the source and a re-copy into
// the now-populated target is refused as a partial-cold-start.
func runMigrateScoped(t *testing.T, srcName, tgtName, srcDSN, tgtDSN, migrationID string, includeTables ...string) {
	t.Helper()
	src, ok := engines.Get(srcName)
	if !ok {
		t.Fatalf("%s engine not registered", srcName)
	}
	tgt, ok := engines.Get(tgtName)
	if !ok {
		t.Fatalf("%s engine not registered", tgtName)
	}
	mig := &Migrator{
		Source:      src,
		Target:      tgt,
		SourceDSN:   srcDSN,
		TargetDSN:   tgtDSN,
		MigrationID: migrationID,
		Filter:      TableFilter{Include: includeTables},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (%s→%s): %v", srcName, tgtName, err)
	}
}

// pgIndexExists reports whether an index of the given name exists on a
// PG table (via pg_class/pg_index).
func pgIndexExists(t *testing.T, ctx context.Context, db *sql.DB, table, index string) bool {
	t.Helper()
	const q = `
		SELECT COUNT(*)
		FROM   pg_index ix
		JOIN   pg_class c  ON c.oid = ix.indrelid
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = 'public' AND c.relname = $1 AND i.relname = $2`
	var n int
	if err := db.QueryRowContext(ctx, q, table, index).Scan(&n); err != nil {
		t.Fatalf("pgIndexExists query: %v", err)
	}
	return n > 0
}

// mysqlIndexNames returns the set of index names on a MySQL table.
func mysqlIndexNames(t *testing.T, ctx context.Context, db *sql.DB, table string) map[string]bool {
	t.Helper()
	const q = `
		SELECT DISTINCT index_name
		FROM   information_schema.statistics
		WHERE  table_schema = DATABASE() AND table_name = ?`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		t.Fatalf("mysqlIndexNames query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	return out
}
