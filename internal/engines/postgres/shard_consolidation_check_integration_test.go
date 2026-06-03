//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0065 CHECK constraint shape: full
// classify → apply → probe loop on real PG 16 containers. Mirrors
// the existing v0.78.0 RENAME COLUMN integration matrix.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// pgCheckExprMatrix returns the same {simple, JSON, datetime}
// matrix the pipeline-side classifier tests exercise, with PG's
// dialect spellings.
func pgCheckExprMatrix() []struct {
	family string
	expr   string
} {
	return []struct {
		family string
		expr   string
	}{
		{"simple", "qty >= 0"},
		{"json", "(payload->>'kind') = 'order'"},
		{"datetime", "start_date <= end_date"},
	}
}

// TestShapeDeltaApplier_AlterAddDropCheck_IdempotentRoundtrip pins
// the ADD then DROP path: each method is idempotent on the
// post-state, exercised across every CHECK family.
func TestShapeDeltaApplier_AlterAddDropCheck_IdempotentRoundtrip(t *testing.T) {
	for _, c := range pgCheckExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."chk_addrop" (
				id INT PRIMARY KEY,
				qty INT,
				payload JSONB,
				start_date DATE,
				end_date DATE
			)`); err != nil {
				t.Fatalf("create table: %v", err)
			}

			eng := Engine{}
			sw, err := eng.OpenSchemaWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaWriter: %v", err)
			}
			defer func() { _ = sw.(*SchemaWriter).Close() }()

			table := &ir.Table{Schema: "public", Name: "chk_addrop"}
			chk := &ir.CheckConstraint{Name: "chk_" + c.family, Expr: c.expr, ExprDialect: "postgres"}
			pgsw := sw.(*SchemaWriter)

			// Apply twice — idempotent.
			if err := pgsw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterAddCheck (1st, %s): %v", c.family, err)
			}
			if err := pgsw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterAddCheck (2nd, idempotent): %v", err)
			}

			// Verify the catalog shows the constraint.
			var present bool
			if err := db.QueryRowContext(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_constraint con
				JOIN pg_catalog.pg_class       rel ON rel.oid     = con.conrelid
				JOIN pg_catalog.pg_namespace   nsp ON nsp.oid     = rel.relnamespace
				WHERE nsp.nspname = 'public' AND rel.relname = 'chk_addrop' AND con.conname = $1 AND con.contype = 'c'
			)`, "chk_"+c.family).Scan(&present); err != nil {
				t.Fatalf("verify constraint: %v", err)
			}
			if !present {
				t.Errorf("constraint chk_%s missing after AlterAddCheck", c.family)
			}

			// Drop twice — idempotent.
			if err := pgsw.AlterDropCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterDropCheck (1st): %v", err)
			}
			if err := pgsw.AlterDropCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterDropCheck (2nd, idempotent): %v", err)
			}
			if err := db.QueryRowContext(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_constraint con
				JOIN pg_catalog.pg_class       rel ON rel.oid     = con.conrelid
				JOIN pg_catalog.pg_namespace   nsp ON nsp.oid     = rel.relnamespace
				WHERE nsp.nspname = 'public' AND rel.relname = 'chk_addrop' AND con.conname = $1 AND con.contype = 'c'
			)`, "chk_"+c.family).Scan(&present); err != nil {
				t.Fatalf("verify constraint gone: %v", err)
			}
			if present {
				t.Errorf("constraint chk_%s should be absent after AlterDropCheck", c.family)
			}
		})
	}
}

// TestShapeDeltaApplier_AlterModifyCheck_PG pins the MODIFY shape:
// DROP + ADD against the same target, idempotent.
func TestShapeDeltaApplier_AlterModifyCheck_PG(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."chk_modify" (
		id INT PRIMARY KEY,
		qty INT,
		CONSTRAINT chk_qty CHECK (qty >= 0)
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{Schema: "public", Name: "chk_modify"}
	oldChk := &ir.CheckConstraint{Name: "chk_qty", Expr: "qty >= 0", ExprDialect: "postgres"}
	newChk := &ir.CheckConstraint{Name: "chk_qty", Expr: "qty > 0", ExprDialect: "postgres"}
	pgsw := sw.(*SchemaWriter)

	if err := pgsw.AlterModifyCheck(ctx, table, oldChk, newChk); err != nil {
		t.Fatalf("AlterModifyCheck: %v", err)
	}

	// Verify the expression is the NEW one. pg_get_constraintdef
	// renders `CHECK ((qty > 0))` — outer parens are PG-canonical.
	var def string
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(con.oid, true)
		FROM   pg_catalog.pg_constraint con
		JOIN   pg_catalog.pg_class      rel ON rel.oid     = con.conrelid
		JOIN   pg_catalog.pg_namespace  nsp ON nsp.oid     = rel.relnamespace
		WHERE  nsp.nspname = 'public' AND rel.relname = 'chk_modify' AND con.conname = 'chk_qty'`).Scan(&def); err != nil {
		t.Fatalf("read constraintdef: %v", err)
	}
	if !strings.Contains(def, "qty > 0") {
		t.Errorf("constraintdef = %q, want to contain 'qty > 0'", def)
	}
}

// TestShardConsolidationProber_AddCheck_PG pins the probe outcomes
// across families. Applied when present, NotApplied when absent.
func TestShardConsolidationProber_AddCheck_PG(t *testing.T) {
	for _, c := range pgCheckExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."chk_probe" (
				id INT PRIMARY KEY,
				qty INT,
				payload JSONB,
				start_date DATE,
				end_date DATE
			)`); err != nil {
				t.Fatalf("create table: %v", err)
			}
			if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."chk_probe"
				ADD CONSTRAINT `+`"chk_existing_`+c.family+`"`+` CHECK (`+c.expr+`)`); err != nil {
				t.Fatalf("seed constraint: %v", err)
			}

			eng := Engine{}
			a, err := eng.OpenChangeApplier(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenChangeApplier: %v", err)
			}
			defer func() {
				if cc, ok := a.(interface{ Close() error }); ok {
					_ = cc.Close()
				}
			}()
			applier := a.(*ChangeApplier)
			table := &ir.Table{Schema: "public", Name: "chk_probe"}

			// Present → Applied.
			outcome, err := applier.ProbeAddCheck(ctx, table,
				[]*ir.CheckConstraint{{Name: "chk_existing_" + c.family}})
			if err != nil {
				t.Fatalf("ProbeAddCheck (present): %v", err)
			}
			if outcome != ir.ProbeOutcomeApplied {
				t.Errorf("outcome = %v, want Applied", outcome)
			}

			// Absent → NotApplied.
			outcome, err = applier.ProbeAddCheck(ctx, table,
				[]*ir.CheckConstraint{{Name: "chk_no_such"}})
			if err != nil {
				t.Fatalf("ProbeAddCheck (absent): %v", err)
			}
			if outcome != ir.ProbeOutcomeNotApplied {
				t.Errorf("outcome = %v, want NotApplied", outcome)
			}

			// DropCheck inverts.
			outcome, err = applier.ProbeDropCheck(ctx, table,
				[]*ir.CheckConstraint{{Name: "chk_no_such"}})
			if err != nil {
				t.Fatalf("ProbeDropCheck (absent): %v", err)
			}
			if outcome != ir.ProbeOutcomeApplied {
				t.Errorf("DropCheck outcome = %v, want Applied", outcome)
			}
		})
	}
}

// TestShardConsolidationProber_ModifyCheck_PG pins the four
// outcomes the modify-check probe distinguishes — same matrix as
// the v0.78.0 RENAME COLUMN probe (silent-divergence catch).
func TestShardConsolidationProber_ModifyCheck_PG(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."chk_probe_mod" (
		id INT PRIMARY KEY,
		qty INT,
		CONSTRAINT chk_old CHECK (qty >= 0)
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if cc, ok := a.(interface{ Close() error }); ok {
			_ = cc.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "chk_probe_mod"}
	newChk := &ir.CheckConstraint{Name: "chk_new", Expr: "qty > 0", ExprDialect: "postgres"}

	// Pre-modify: chk_old present, chk_new absent → NotApplied.
	outcome, err := applier.ProbeModifyCheck(ctx, table, "chk_old", newChk)
	if err != nil {
		t.Fatalf("ProbeModifyCheck (pre): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("pre-modify outcome = %v, want NotApplied", outcome)
	}

	// Land the modify (DROP + ADD).
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."chk_probe_mod" DROP CONSTRAINT chk_old`); err != nil {
		t.Fatalf("drop chk_old: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."chk_probe_mod" ADD CONSTRAINT chk_new CHECK (qty > 0)`); err != nil {
		t.Fatalf("add chk_new: %v", err)
	}

	// Post-modify: chk_old absent, chk_new present, matching expr → Applied.
	outcome, err = applier.ProbeModifyCheck(ctx, table, "chk_old", newChk)
	if err != nil {
		t.Fatalf("ProbeModifyCheck (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("post-modify outcome = %v, want Applied", outcome)
	}

	// Wrong expression on chk_new → Inconsistent + error (silent-divergence catch).
	wrongChk := &ir.CheckConstraint{Name: "chk_new", Expr: "qty < 999", ExprDialect: "postgres"}
	outcome, err = applier.ProbeModifyCheck(ctx, table, "chk_old", wrongChk)
	if err == nil {
		t.Fatal("expected error on expression mismatch")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("wrong-expr outcome = %v, want Inconsistent", outcome)
	}
	if !strings.Contains(err.Error(), "does not match recorded") {
		t.Errorf("error should name the mismatch: %v", err)
	}

	// Both absent → Inconsistent.
	outcome, err = applier.ProbeModifyCheck(ctx, table, "chk_no_such_old", &ir.CheckConstraint{Name: "chk_no_such_new"})
	if err == nil {
		t.Fatal("expected error when neither constraint exists")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("both-absent outcome = %v, want Inconsistent", outcome)
	}
}

// TestAlterAddCheck_CrossEngineRefusesLoudly_PG pins the safety
// floor: a PG-target apply with a MySQL-tagged Expr containing a
// known-untranslatable token (json_extract) refuses BEFORE issuing
// SQL with an operator-actionable error.
func TestAlterAddCheck_CrossEngineRefusesLoudly_PG(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."chk_xeng" (id INT PRIMARY KEY, payload JSONB)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()
	pgsw := sw.(*SchemaWriter)
	table := &ir.Table{Schema: "public", Name: "chk_xeng"}

	// MySQL-tagged Expr whose JSON_EXTRACT the translator CANNOT
	// rewrite (the path arg is a concat(), not a simple '$.key'
	// literal), so the json_extract( token survives into the PG
	// output → refuse loudly. Bug 77 symmetric (task #73): the refuse
	// now matches the POST-translation OUTPUT only, so a translatable
	// `json_extract(payload, '$.k')` (which rewrites to (payload->'k'))
	// would correctly NOT be refused; this expr exercises the
	// genuinely-untranslatable case where the token survives.
	chk := &ir.CheckConstraint{
		Name:        "chk_json_xeng",
		Expr:        "json_extract(payload, concat('$.', 'k')) = 'v'",
		ExprDialect: "mysql",
	}
	err = pgsw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk})
	if err == nil {
		t.Fatal("expected cross-engine refuse-loudly, got nil error")
	}
	if !strings.Contains(err.Error(), "refuse loudly") {
		t.Errorf("error should be a refuse-loudly: %v", err)
	}
	// Bug 77 symmetric (task #73): the recovery hint is "drop the
	// CHECK on the source" — NOT --expr-override, which only targets
	// generated columns and never applied to CHECK constraints.
	if !strings.Contains(err.Error(), "drop the CHECK on the source") {
		t.Errorf("error should mention the drop-source-CHECK recovery: %v", err)
	}

	// Verify nothing landed on the target — the refusal must be
	// pre-emit so the catalog is untouched.
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class       rel ON rel.oid     = con.conrelid
		JOIN pg_catalog.pg_namespace   nsp ON nsp.oid     = rel.relnamespace
		WHERE nsp.nspname = 'public' AND rel.relname = 'chk_xeng' AND con.conname = 'chk_json_xeng' AND con.contype = 'c'
	)`).Scan(&present); err != nil {
		t.Fatalf("verify nothing landed: %v", err)
	}
	if present {
		t.Errorf("constraint should NOT have been added on refuse-loudly")
	}
}
