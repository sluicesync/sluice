//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0065 CHECK constraint shape on MySQL
// 8.0+. CHECK is enforced by MySQL since 8.0.16; sluice supports
// 8.0.x.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func mysqlCheckExprMatrix() []struct {
	family string
	expr   string
} {
	return []struct {
		family string
		expr   string
	}{
		{"simple", "qty >= 0"},
		{"json", "JSON_EXTRACT(payload, '$.kind') = 'order'"},
		{"datetime", "start_date <= end_date"},
	}
}

func TestShapeDeltaApplier_MySQL_AlterAddDropCheck_Idempotent(t *testing.T) {
	for _, c := range mysqlCheckExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			dsn, cleanup := startMySQLForApplier(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			if _, err := db.ExecContext(ctx, "CREATE TABLE `chk_addrop` ("+
				"id INT PRIMARY KEY, qty INT, payload JSON, start_date DATE, end_date DATE)"); err != nil {
				t.Fatalf("create table: %v", err)
			}

			eng := Engine{}
			sw, err := eng.OpenSchemaWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaWriter: %v", err)
			}
			defer func() { _ = sw.(*SchemaWriter).Close() }()
			mysw := sw.(*SchemaWriter)
			table := &ir.Table{Name: "chk_addrop"}
			chk := &ir.CheckConstraint{Name: "chk_" + c.family, Expr: c.expr, ExprDialect: "mysql"}

			if err := mysw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterAddCheck (1st, %s): %v", c.family, err)
			}
			if err := mysw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterAddCheck (2nd, idempotent): %v", err)
			}

			var present bool
			if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.CHECK_CONSTRAINTS"+
				" WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ?)", "chk_"+c.family).Scan(&present); err != nil {
				t.Fatalf("verify constraint: %v", err)
			}
			if !present {
				t.Errorf("constraint chk_%s missing after AlterAddCheck", c.family)
			}

			if err := mysw.AlterDropCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterDropCheck (1st): %v", err)
			}
			if err := mysw.AlterDropCheck(ctx, table, []*ir.CheckConstraint{chk}); err != nil {
				t.Fatalf("AlterDropCheck (2nd, idempotent): %v", err)
			}
			if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.CHECK_CONSTRAINTS"+
				" WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ?)", "chk_"+c.family).Scan(&present); err != nil {
				t.Fatalf("verify constraint gone: %v", err)
			}
			if present {
				t.Errorf("constraint chk_%s should be absent after AlterDropCheck", c.family)
			}
		})
	}
}

func TestShapeDeltaApplier_MySQL_AlterModifyCheck(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `chk_modify` ("+
		"id INT PRIMARY KEY, qty INT, CONSTRAINT chk_qty CHECK (qty >= 0))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()
	mysw := sw.(*SchemaWriter)
	table := &ir.Table{Name: "chk_modify"}
	oldChk := &ir.CheckConstraint{Name: "chk_qty", Expr: "qty >= 0", ExprDialect: "mysql"}
	newChk := &ir.CheckConstraint{Name: "chk_qty", Expr: "qty > 0", ExprDialect: "mysql"}

	if err := mysw.AlterModifyCheck(ctx, table, oldChk, newChk); err != nil {
		t.Fatalf("AlterModifyCheck: %v", err)
	}

	var clause string
	if err := db.QueryRowContext(ctx, "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS"+
		" WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'chk_qty'").Scan(&clause); err != nil {
		t.Fatalf("read check clause: %v", err)
	}
	// MySQL CHECK_CLAUSE re-emits identifier refs with backticks
	// (e.g. "qty > 0" → "(`qty` > 0)"). Strip backticks before the
	// substring assertion so the test pins the structural shape, not
	// the catalog's re-quoting cosmetic.
	normalized := strings.ReplaceAll(clause, "`", "")
	if !strings.Contains(normalized, "qty > 0") {
		t.Errorf("check clause = %q (normalized %q), want to contain 'qty > 0'", clause, normalized)
	}
}

func TestShardConsolidationProber_MySQL_AddCheck(t *testing.T) {
	for _, c := range mysqlCheckExprMatrix() {
		c := c
		t.Run(c.family, func(t *testing.T) {
			dsn, cleanup := startMySQLForApplier(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			if _, err := db.ExecContext(ctx, "CREATE TABLE `chk_probe` ("+
				"id INT PRIMARY KEY, qty INT, payload JSON, start_date DATE, end_date DATE)"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			constraintName := "chk_existing_" + c.family
			if _, err := db.ExecContext(ctx, "ALTER TABLE `chk_probe` ADD CONSTRAINT `"+constraintName+"` CHECK ("+c.expr+")"); err != nil {
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
			table := &ir.Table{Name: "chk_probe"}

			outcome, err := applier.ProbeAddCheck(ctx, table,
				[]*ir.CheckConstraint{{Name: constraintName}})
			if err != nil {
				t.Fatalf("ProbeAddCheck (present): %v", err)
			}
			if outcome != ir.ProbeOutcomeApplied {
				t.Errorf("outcome = %v, want Applied", outcome)
			}

			outcome, err = applier.ProbeAddCheck(ctx, table,
				[]*ir.CheckConstraint{{Name: "chk_no_such"}})
			if err != nil {
				t.Fatalf("ProbeAddCheck (absent): %v", err)
			}
			if outcome != ir.ProbeOutcomeNotApplied {
				t.Errorf("outcome = %v, want NotApplied", outcome)
			}
		})
	}
}

func TestShardConsolidationProber_MySQL_ModifyCheck(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `chk_probe_mod` ("+
		"id INT PRIMARY KEY, qty INT, CONSTRAINT chk_old CHECK (qty >= 0))"); err != nil {
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
	table := &ir.Table{Name: "chk_probe_mod"}
	newChk := &ir.CheckConstraint{Name: "chk_new", Expr: "qty > 0", ExprDialect: "mysql"}

	outcome, err := applier.ProbeModifyCheck(ctx, table, "chk_old", newChk)
	if err != nil {
		t.Fatalf("ProbeModifyCheck (pre): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("pre-modify outcome = %v, want NotApplied", outcome)
	}

	if _, err := db.ExecContext(ctx, "ALTER TABLE `chk_probe_mod` DROP CHECK chk_old"); err != nil {
		t.Fatalf("drop chk_old: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE `chk_probe_mod` ADD CONSTRAINT chk_new CHECK (qty > 0)"); err != nil {
		t.Fatalf("add chk_new: %v", err)
	}

	outcome, err = applier.ProbeModifyCheck(ctx, table, "chk_old", newChk)
	if err != nil {
		t.Fatalf("ProbeModifyCheck (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("post-modify outcome = %v, want Applied", outcome)
	}

	wrongChk := &ir.CheckConstraint{Name: "chk_new", Expr: "qty < 999", ExprDialect: "mysql"}
	outcome, err = applier.ProbeModifyCheck(ctx, table, "chk_old", wrongChk)
	if err == nil {
		t.Fatal("expected error on expression mismatch")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("wrong-expr outcome = %v, want Inconsistent", outcome)
	}
}

// TestAlterAddCheck_CrossEngineRefusesLoudly_MySQL pins the safety
// floor on the MySQL side: a PG-tagged Expr with `->>` refuses
// BEFORE issuing SQL.
func TestAlterAddCheck_CrossEngineRefusesLoudly_MySQL(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `chk_xeng` (id INT PRIMARY KEY, payload JSON)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()
	mysw := sw.(*SchemaWriter)
	table := &ir.Table{Name: "chk_xeng"}

	chk := &ir.CheckConstraint{
		Name:        "chk_json_xeng",
		Expr:        "(payload->>'k') = 'v'",
		ExprDialect: "postgres",
	}
	err = mysw.AlterAddCheck(ctx, table, []*ir.CheckConstraint{chk})
	if err == nil {
		t.Fatal("expected cross-engine refuse-loudly, got nil")
	}
	if !strings.Contains(err.Error(), "refuse loudly") {
		t.Errorf("error should be a refuse-loudly: %v", err)
	}
	// Bug 77 v0.85.1: the recovery hint is "drop the CHECK on the
	// source" — NOT --expr-override, which only targets generated
	// columns and never applied to CHECK constraints.
	if !strings.Contains(err.Error(), "drop the CHECK on the source") {
		t.Errorf("error should mention the drop-source-CHECK recovery: %v", err)
	}

	var present bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.CHECK_CONSTRAINTS"+
		" WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'chk_json_xeng')").Scan(&present); err != nil {
		t.Fatalf("verify nothing landed: %v", err)
	}
	if present {
		t.Errorf("constraint should NOT have been added on refuse-loudly")
	}
}
