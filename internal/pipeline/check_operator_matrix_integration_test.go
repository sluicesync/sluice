//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL-family operator matrix for CHECK expressions (audit
// 2026-08-11 ARCH-1) — the machine measuring, instead of the manual
// policy of "add spellings from measurements".
//
// # Why this exists
//
// Bug 242's infix gate listed the two REGEXP spellings and nothing
// else. The sibling word operators (DIV, MOD, XOR, prefix !) rendered
// into real catalogs the same way and died in PostgreSQL's parser
// (SQLSTATE 42601) mid-migrate, after earlier tables had been created
// — the exact failure the gate was built for, on operators one
// question away from the ones it listed. The CLAUDE.md sibling-sweep
// rule names this shape; this matrix is its mechanical enforcement for
// the operator family: one CHECK per operator in the MySQL grammar's
// operator table, created on BOTH real flavors, read back through the
// engine reader, and every rendered token asserted to be
// translator-rewritten, PG-accepted, or gate-refused. A new escape —
// a server rendering an operator neither the translator nor the gate
// knows — fails a named test here instead of a customer's migration.
//
// # What the verdicts mean
//
//   - opApplies: the gate must NOT refuse it, the real migrate must
//     land it, and the CHECK must exist on the PG target (the
//     independent oracle — a writer that silently dropped CHECKs
//     would otherwise green this leg vacuously).
//   - opRefused: the pre-DDL gate (curated or backstop) must refuse
//     it. If the server ever stops rendering the escape spelling, the
//     expectation fails and the entry is re-measured — that is the
//     premise-pin half.
//   - opServerRejects: this flavor's server refuses the CREATE, so no
//     rendering exists to escape (MariaDB has no MEMBER OF / -> / ->>).
//     If a future line starts accepting it, the expectation fails and
//     the new rendering gets classified.
//   - opLateTypeError: the rendering reaches PostgreSQL and dies LOUD
//     at CREATE — pinned as loud-late rather than silently exempted.
//     Today that is the chained-comparison rendering both flavors
//     produce for a bare `!a`, and the two flavors die differently:
//     MySQL parenthesizes (`(0 = a) = 0`), which parses and fails
//     typecheck (42883); MariaDB renders `a = 0 = 0`, which
//     PostgreSQL's non-associative `=` rejects at parse (42601). The
//     MariaDB shape is therefore a KNOWN, DOCUMENTED parse-level
//     escape: refusing it up front would need a real expression parser
//     (a token scan cannot tell `a = 0 = 0` from the PG-valid
//     `(a = 0) = (b = 0)` without precedence), the `!a`-alone spelling
//     has no other way to arise, and the failure is loud before any
//     row moves. If either flavor's rendering changes class, the
//     phase-4 pin below fails and this paragraph gets re-derived.
//
// # Scope: parse-level, not semantic-equivalence
//
// A rendered operator PostgreSQL parses with DIFFERENT semantics
// (`^` is bitwise XOR on MySQL, numeric power on PG; integer `/`
// keeps fractions on MySQL, truncates on PG; jsonb `->` takes a key
// where MySQL took a JSON path) lands as opApplies here: the CHECK
// applies, and its strictness may differ — a constraint-enforcement
// shift, not row data loss. That residue is filed in the audit
// backlog (2026-08-13) rather than silently blessed; this comment is
// the in-tree pointer.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

type opVerdict int

const (
	opApplies opVerdict = iota
	opRefused
	opServerRejects
	opLateTypeError
)

type checkOpCase struct {
	op    string
	expr  string
	json  bool // the table carries a JSON column the expression needs
	mysql opVerdict
	maria opVerdict
}

func (c checkOpCase) verdict(flavor string) opVerdict {
	if flavor == "mariadb" {
		return c.maria
	}
	return c.mysql
}

// checkOperatorCases is one CHECK per operator in the MySQL grammar's
// operator-precedence table, spelled as an operator would write it.
// The per-flavor verdicts are the 2026-08-11/13 measurement
// (mysql:8.0 + 8.4 render identically; mariadb 10.11/11.4/11.8/12.3
// render identically), so a rendering change on either flavor's next
// line fails the exact entry that drifted.
var checkOperatorCases = []checkOpCase{
	// MySQL 8 rewrites `!` to not(...); MariaDB preserves it (prefix-!).
	{op: "op_bang", expr: `!(a <=> 99)`, mysql: opApplies, maria: opRefused},
	// Both flavors render a bare `!a` as a CHAINED comparison
	// (`(0 = a) = 0` / `a = 0 = 0`) — PG parses it and dies 42883.
	{op: "op_bang_plain", expr: `(!a) = 0`, mysql: opLateTypeError, maria: opLateTypeError},
	{op: "op_bitnot", expr: `(~a) <> 0`, mysql: opApplies, maria: opApplies},
	{op: "op_uminus", expr: `(-a) < 100`, mysql: opApplies, maria: opApplies},
	{op: "op_uplus", expr: `(+a) > 0`, mysql: opApplies, maria: opApplies},
	// Parses on PG with POWER semantics — see the scope note above.
	{op: "op_bitxor", expr: `(a ^ b) >= 0`, mysql: opApplies, maria: opApplies},
	{op: "op_mul", expr: `(a * b) >= 0`, mysql: opApplies, maria: opApplies},
	// Parses on PG with truncating integer division — scope note above.
	{op: "op_divslash", expr: `(a / 2) < 100`, mysql: opApplies, maria: opApplies},
	// DIV renders infix on BOTH flavors; PG has no parse for it.
	{op: "op_divkw", expr: `(a DIV 2) = 0`, mysql: opRefused, maria: opRefused},
	// MariaDB renders BOTH `%` and `MOD` as infix MOD; MySQL renders both as `%`.
	{op: "op_pct", expr: `(a % 3) = 0`, mysql: opApplies, maria: opRefused},
	{op: "op_modkw", expr: `(a MOD 3) = 0`, mysql: opApplies, maria: opRefused},
	{op: "op_plus", expr: `(a + b) >= 0`, mysql: opApplies, maria: opApplies},
	{op: "op_shl", expr: `(a << 1) >= 0`, mysql: opApplies, maria: opApplies},
	{op: "op_shr", expr: `(a >> 1) >= 0`, mysql: opApplies, maria: opApplies},
	{op: "op_band", expr: `(a & 1) = 0`, mysql: opApplies, maria: opApplies},
	{op: "op_bor", expr: `(a | 1) > 0`, mysql: opApplies, maria: opApplies},
	{op: "op_eq", expr: `a = 5`, mysql: opApplies, maria: opApplies},
	// MySQL renders NOT(<=>) — the translator's IS NOT DISTINCT FROM
	// rewrite carries it; MariaDB renders the prefix-! spelling.
	{op: "op_nullsafe", expr: `NOT (a <=> 99)`, mysql: opApplies, maria: opRefused},
	{op: "op_ge", expr: `a >= 0`, mysql: opApplies, maria: opApplies},
	{op: "op_ne", expr: `a <> 9`, mysql: opApplies, maria: opApplies},
	{op: "op_nebang", expr: `a != 9`, mysql: opApplies, maria: opApplies},
	{op: "op_istrue", expr: `(a = 1) IS NOT TRUE`, mysql: opApplies, maria: opApplies},
	{op: "op_isnull", expr: `(c IS NULL) OR (a > 0)`, mysql: opApplies, maria: opApplies},
	{op: "op_like", expr: `c LIKE 'x%'`, mysql: opApplies, maria: opApplies},
	{op: "op_notlike", expr: `c NOT LIKE 'q%'`, mysql: opApplies, maria: opApplies},
	{op: "op_likeescape", expr: `c LIKE 'x%' ESCAPE '|'`, mysql: opApplies, maria: opApplies},
	// MySQL renders regexp_like() (refused by the function allowlist —
	// ICU vs POSIX); MariaDB renders the Bug 242 infix spelling.
	{op: "op_regexp", expr: `c REGEXP '^x'`, mysql: opRefused, maria: opRefused},
	{op: "op_rlike", expr: `c RLIKE '^y'`, mysql: opRefused, maria: opRefused},
	{op: "op_notregexp", expr: `c NOT REGEXP '^z'`, mysql: opRefused, maria: opRefused},
	{op: "op_in", expr: `a IN (1,2,3)`, mysql: opApplies, maria: opApplies},
	{op: "op_notin", expr: `a NOT IN (7,8)`, mysql: opApplies, maria: opApplies},
	{op: "op_memberof", expr: `a MEMBER OF (j)`, json: true, mysql: opRefused, maria: opServerRejects},
	// MySQL renders -> / ->> as json_extract()/json_unquote(), which
	// the translator rewrites to jsonb's -> / ->>.
	{op: "op_arrow", expr: `(j -> '$.x') IS NOT NULL`, json: true, mysql: opApplies, maria: opServerRejects},
	{op: "op_arrow2", expr: `(j ->> '$.x') <> 'q'`, json: true, mysql: opApplies, maria: opServerRejects},
	{op: "op_between", expr: `a BETWEEN 1 AND 10`, mysql: opApplies, maria: opApplies},
	{op: "op_notbetween", expr: `a NOT BETWEEN 90 AND 99`, mysql: opApplies, maria: opApplies},
	{op: "op_and", expr: `(a > 0) AND (b > 0)`, mysql: opApplies, maria: opApplies},
	{op: "op_andand", expr: `(a > 0) && (b > 0)`, mysql: opApplies, maria: opApplies},
	{op: "op_or", expr: `(a > 0) OR (b > 0)`, mysql: opApplies, maria: opApplies},
	{op: "op_oror", expr: `(a > 0) || (b > 0)`, mysql: opApplies, maria: opApplies},
	// XOR renders infix on BOTH flavors; PG has no logical XOR.
	{op: "op_xor", expr: `(a > 0) XOR (b > 0)`, mysql: opRefused, maria: opRefused},
	{op: "op_not", expr: `NOT (a = 1)`, mysql: opApplies, maria: opApplies},
	// Renders through soundex() on both flavors (MySQL adds
	// convert(... using ...)); the function allowlist refuses both.
	{op: "op_soundslike", expr: `c SOUNDS LIKE 'smith'`, mysql: opRefused, maria: opRefused},
	// Both flavors render BINARY as cast(c as char charset binary),
	// which rewriteCASTCharCharset translates to a plain VARCHAR/TEXT
	// cast.
	{op: "op_binary", expr: `(BINARY c) = 'x'`, mysql: opApplies, maria: opApplies},
	// Column-level COLLATE parses on PG but the MySQL collation cannot
	// exist there (42704) — refused up front.
	{op: "op_collate", expr: `(c COLLATE utf8mb4_bin) = 'x'`, mysql: opRefused, maria: opRefused},
	{op: "op_case", expr: `(CASE WHEN a > 0 THEN 1 ELSE 0 END) = 1`, mysql: opApplies, maria: opApplies},
	// Bare INTERVAL grammar renders on both flavors; PG only parses
	// the quoted literal form.
	{op: "op_interval", expr: `(d + INTERVAL 1 DAY) > d`, mysql: opRefused, maria: opRefused},
}

func TestCheckOperatorFamilyMatrix_MySQLToPG(t *testing.T) {
	sourceDSN, _, cleanup := startMySQL(t)
	defer cleanup()
	runCheckOperatorFamilyMatrix(t, "mysql", sourceDSN)
}

func TestCheckOperatorFamilyMatrix_MariaDBToPG(t *testing.T) {
	sourceDSN, _, cleanup := startMariaDB(t)
	defer cleanup()
	runCheckOperatorFamilyMatrix(t, "mariadb", sourceDSN)
}

func runCheckOperatorFamilyMatrix(t *testing.T, flavor, sourceDSN string) {
	t.Helper()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open %s source: %v", flavor, err)
	}
	defer func() { _ = db.Close() }()

	// Phase 1 — create one table per operator and attach its CHECK.
	// A server rejection is itself a measured verdict.
	created := map[string]opVerdict{}
	for _, c := range checkOperatorCases {
		verdict := c.verdict(flavor)
		cols := "a INT, b INT, c VARCHAR(40), d DATE"
		if c.json {
			cols += ", j JSON"
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", c.op, cols)); err != nil {
			t.Fatalf("create %s: %v", c.op, err)
		}
		_, alterErr := db.ExecContext(ctx, fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s_chk CHECK (%s)", c.op, c.op, c.expr,
		))
		switch {
		case alterErr != nil && verdict == opServerRejects:
			// Measured: this flavor cannot render the operator, so no
			// spelling exists to escape. Drop the empty table.
			if _, err := db.ExecContext(ctx, "DROP TABLE "+c.op); err != nil {
				t.Fatalf("drop %s: %v", c.op, err)
			}
		case alterErr != nil:
			t.Fatalf("%s: the server rejected the %s CHECK it accepted at measurement time (%v) — re-measure the matrix entry", flavor, c.op, alterErr)
		case verdict == opServerRejects:
			t.Fatalf("%s: the server now ACCEPTS %s (%q) — a new rendering exists; classify it and update the matrix", flavor, c.op, c.expr)
		default:
			created[c.op] = verdict
		}
	}

	// Phase 2 — read the schema back through the ENGINE reader (the
	// exact IR spelling the gates and the translator will see) and
	// classify every rendered CHECK against the pre-DDL gates.
	eng, ok := engines.Get(flavor)
	if !ok {
		t.Fatalf("%s engine not registered", flavor)
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	reader, err := eng.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := reader.ReadSchema(ctx)
	if closer, okc := reader.(interface{ Close() error }); okc {
		defer func() { _ = closer.Close() }()
	}
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	byName := map[string]*ir.Table{}
	for _, tbl := range schema.Tables {
		byName[tbl.Name] = tbl
	}
	for op, verdict := range created {
		tbl := byName[op]
		if tbl == nil {
			t.Fatalf("%s missing from the read schema", op)
		}
		// Anti-vacuity: the reader must surface the rendered CHECK. A
		// reader that silently dropped it would green every leg below.
		if len(tbl.CheckConstraints) == 0 || strings.TrimSpace(tbl.CheckConstraints[0].Expr) == "" {
			t.Fatalf("%s: the reader surfaced no CHECK body — the matrix would be vacuous", op)
		}
		single := &ir.Schema{Tables: []*ir.Table{tbl}}
		curated := translate.RefuseOnLoudGaps(single, flavor, "postgres", "migrate", nil)
		backstop := translate.RefuseOnUntranslatableExprs(single, flavor, "postgres", "migrate", nil)
		refused := curated != nil || backstop != nil
		switch verdict {
		case opRefused:
			if !refused {
				t.Errorf("ESCAPE: %s renders %q on %s, both pre-DDL gates pass it, and PostgreSQL cannot accept it — "+
					"the migrate would die mid-pipeline (the Bug 242 shape)", op, tbl.CheckConstraints[0].Expr, flavor)
			}
		case opApplies, opLateTypeError:
			if refused {
				err := curated
				if err == nil {
					err = backstop
				}
				t.Errorf("FALSE POSITIVE: %s renders %q on %s and the gate refuses a construct the pipeline handles:\n%v",
					op, tbl.CheckConstraints[0].Expr, flavor, err)
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Phase 3 — drop the refused tables and the late-type pin's table,
	// then run the REAL migrate to a REAL PostgreSQL: every surviving
	// rendered operator must be translator-rewritten or PG-accepted.
	for op, verdict := range created {
		if verdict != opApplies {
			if _, err := db.ExecContext(ctx, "DROP TABLE "+op); err != nil {
				t.Fatalf("drop %s: %v", op, err)
			}
		}
	}
	mig := &Migrator{
		Source: eng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: pgTarget,
		MigrationID: "check-op-matrix-" + flavor,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate %s → postgres with every gate-passed operator CHECK: %v\n"+
			"An unrefused rendering PostgreSQL rejects is an ESCAPE — add its arm and its matrix entry.", flavor, err)
	}

	// The independent oracle: the CHECKs exist ON THE TARGET, by the
	// target server's own catalog — not by sluice's report of success.
	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()
	applied := 0
	for op, verdict := range created {
		if verdict != opApplies {
			continue
		}
		applied++
		var n int
		if err := pg.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.table_constraints
			WHERE table_name = $1 AND constraint_type = 'CHECK'
			  AND constraint_name NOT LIKE '%not_null'`, op).Scan(&n); err != nil {
			t.Fatalf("count target CHECKs for %s: %v", op, err)
		}
		if n == 0 {
			t.Errorf("%s migrated clean but carries NO CHECK on the PG target — the apply leg is vacuous for it", op)
		}
	}
	if applied < 20 {
		t.Fatalf("only %d operator CHECKs reached the apply leg — the matrix has gone vacuous", applied)
	}

	// Phase 4 — the loud-late residue, pinned per flavor: a bare `!a`
	// renders as a chained comparison on both flavors, and PostgreSQL
	// kills each rendering loudly at CREATE — MySQL's parenthesized
	// form as a 42883 type error, MariaDB's flat form as a 42601 parse
	// error (see the file doc's opLateTypeError paragraph for why the
	// MariaDB shape is a documented escape rather than a refusal). If
	// PG ever accepts either, or a refusal arm starts catching them,
	// this pin fails and the exemption gets re-derived.
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE op_bang_plain (a INT, b INT, c VARCHAR(40), d DATE)"); err != nil {
		t.Fatalf("recreate op_bang_plain: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"ALTER TABLE op_bang_plain ADD CONSTRAINT op_bang_plain_chk CHECK ((!a) = 0)"); err != nil {
		t.Fatalf("re-add op_bang_plain CHECK: %v", err)
	}
	late := &Migrator{
		Source: eng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: pgTarget,
		Filter:      migcore.TableFilter{Include: []string{"op_bang_plain"}},
		MigrationID: "check-op-matrix-" + flavor + "-late",
	}
	lateErr := late.Run(ctx)
	if lateErr == nil {
		t.Fatal("the chained-comparison rendering of `!a` now applies cleanly on PostgreSQL — " +
			"the opLateTypeError exemption no longer holds; re-derive it")
	}
	if strings.Contains(lateErr.Error(), "refuses before any DDL") {
		t.Fatalf("the chained-comparison rendering is now gate-refused, not late-loud — move it to opRefused:\n%v", lateErr)
	}
	wantLate := "operator does not exist" // MySQL renders (0 = a) = 0 → 42883
	if flavor == "mariadb" {
		wantLate = "syntax error" // MariaDB renders a = 0 = 0 → 42601
	}
	if !strings.Contains(lateErr.Error(), wantLate) {
		t.Fatalf("the `!a` rendering failed with something other than the pinned %q — re-derive the exemption:\n%v", wantLate, lateErr)
	}
}
