//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine integration test for the schema-diff orchestrator
// (ADR-0029). Pairs a Postgres source with a MySQL target so the
// "expected" side starts with PG-native IR types (UUID, Inet, Array);
// the diff's cross-engine retarget pass (translate.RetargetForEngine)
// rewrites those to the MySQL-storage IR shapes (Char, Varchar, JSON)
// before comparison, so the only drift the diff should surface is the
// deliberately-injected drift: email narrowed, created_at missing,
// legacy_audit extra. Without the retarget pass every translated
// column would flip to a noisy type mismatch — see
// internal/translate/retarget.go for the rule set.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestDiff_PostgresToMySQL boots a PG source seeded with PG-native
// columns (uuid, inet, text[]) and a MySQL target seeded with the
// shapes sluice would land them on (CHAR(36), VARCHAR(45), JSON) plus
// deliberate drift (narrowed email, missing created_at, extra table).
// Asserts the diff applies the cross-engine retarget so the translated
// columns DO NOT flag as drift, and only the in-band drift signals
// surface.
func TestDiff_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const sourceDDL = `
		CREATE TABLE accounts (
			id           BIGINT       NOT NULL PRIMARY KEY,
			email        VARCHAR(255) NOT NULL,
			account_uuid UUID         NOT NULL,
			client_ip    INET         NOT NULL,
			tags         TEXT[]       NULL,
			created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`
	// Target shape: the MySQL-storage form sluice would emit for the
	// PG types above (CHAR(36)/VARCHAR(45)/JSON), but with three
	// drift signals layered in — email narrowed, created_at dropped,
	// extra legacy_audit table.
	const targetDDL = `
		CREATE TABLE accounts (
			id           BIGINT       NOT NULL PRIMARY KEY,
			email        VARCHAR(100) NOT NULL,
			account_uuid CHAR(36)     NOT NULL,
			client_ip    VARCHAR(45)  NOT NULL,
			tags         JSON         NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE legacy_audit (
			id BIGINT NOT NULL PRIMARY KEY
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyPGDDL(t, pgSource, sourceDDL)
	applyMySQLDDL(t, mysqlTarget, targetDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("json captures only in-band drift after retarget", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:    pgEng,
			Target:    mysqlEng,
			SourceDSN: pgSource,
			TargetDSN: mysqlTarget,
			Format:    "json",
			Out:       &buf,
		}
		diff, err := d.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if diff == nil || !diff.HasChanges() {
			t.Fatalf("expected drift; got %+v", diff)
		}

		var got DiffJSON
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
		}
		if got.SourceEngine != "postgres" {
			t.Errorf("source_engine = %q; want postgres", got.SourceEngine)
		}
		if got.TargetEngine != "mysql" {
			t.Errorf("target_engine = %q; want mysql", got.TargetEngine)
		}
		if got.Summary.TablesExtra != 1 {
			t.Errorf("tables_extra = %d; want 1 (legacy_audit)", got.Summary.TablesExtra)
		}
		if got.Summary.ColumnsMissing < 1 {
			t.Errorf("columns_missing = %d; want >=1 (created_at)", got.Summary.ColumnsMissing)
		}
		// Post-retarget the only column-level drift is the deliberately
		// narrowed email column. UUID/Inet/Array are rewritten to the
		// MySQL-storage IR (Char(36)/Varchar(45)/JSON[binary]) before
		// the diff runs and so match the actual target storage.
		if got.Summary.ColumnsMismatched != 1 {
			t.Errorf("columns_mismatched = %d; want 1 (email size only)", got.Summary.ColumnsMismatched)
		}

		acc := findTableDiff(got.SchemaDiff, "accounts")
		if acc == nil {
			t.Fatalf("accounts table missing from diff: %+v", got.TablesMismatched)
		}

		// The retargeted columns must NOT show up as drift.
		for _, retargeted := range []string{"account_uuid", "client_ip", "tags"} {
			if cd := findColumnDiff(acc.ColumnsMismatched, retargeted); cd != nil {
				t.Errorf("accounts.%s flagged as drift after retarget: expected=%q actual=%q",
					retargeted, cd.ExpectedType, cd.ActualType)
			}
		}

		// The deliberately narrowed email column should still be flagged
		// with its width drift.
		emailDrift := findColumnDiff(acc.ColumnsMismatched, "email")
		if emailDrift == nil {
			t.Fatalf("expected email width drift; got %+v", acc.ColumnsMismatched)
		}
		if emailDrift.ExpectedType != "Varchar(255)" || emailDrift.ActualType != "Varchar(100)" {
			t.Errorf("accounts.email expected/actual = %q/%q; want %q/%q",
				emailDrift.ExpectedType, emailDrift.ActualType, "Varchar(255)", "Varchar(100)")
		}

		if !containsString(acc.ColumnsMissing, "created_at") {
			t.Errorf("expected created_at in columns_missing; got %v", acc.ColumnsMissing)
		}
		if !containsString(got.TablesExtra, "legacy_audit") {
			t.Errorf("expected legacy_audit in tables_extra; got %v", got.TablesExtra)
		}
	})

	t.Run("text reports drift sections", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:    pgEng,
			Target:    mysqlEng,
			SourceDSN: pgSource,
			TargetDSN: mysqlTarget,
			Format:    "text",
			Out:       &buf,
		}
		if _, err := d.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "accounts (mismatched)") {
			t.Errorf("missing accounts mismatch section:\n%s", out)
		}
		if !strings.Contains(out, "legacy_audit (extra on target)") {
			t.Errorf("missing legacy_audit extra section:\n%s", out)
		}
		// Backtick quoting confirms the renderer picked the MySQL
		// idiom for the target identifiers.
		if !strings.Contains(out, "`accounts`") {
			t.Errorf("expected MySQL backtick-quoted table name in suggestions:\n%s", out)
		}
		// The retargeted columns must NOT show up as type-mismatch lines.
		// If they did, the renderer would print their names alongside
		// the expected/actual IR strings.
		for _, retargeted := range []string{"account_uuid", "client_ip", "tags"} {
			if strings.Contains(out, retargeted) {
				t.Errorf("retargeted column %q surfaced in text output:\n%s", retargeted, out)
			}
		}
	})

	t.Run("ignore-extras suppresses extra-table diff", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:       pgEng,
			Target:       mysqlEng,
			SourceDSN:    pgSource,
			TargetDSN:    mysqlTarget,
			IgnoreExtras: true,
			Format:       "json",
			Out:          &buf,
		}
		if _, err := d.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		var got DiffJSON
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
		}
		if got.Summary.TablesExtra != 0 {
			t.Errorf("expected no extras under IgnoreExtras; got %d", got.Summary.TablesExtra)
		}
		// Column-level drifts should still surface — IgnoreExtras
		// only affects extra-on-target entries.
		if got.Summary.ColumnsMissing < 1 {
			t.Errorf("expected created_at still in columns_missing; got %d", got.Summary.ColumnsMissing)
		}
		if got.Summary.ColumnsMismatched != 1 {
			t.Errorf("expected only email drift to surface; got %d", got.Summary.ColumnsMismatched)
		}
	})
}

// findTableDiff returns the TableDiff for name, or nil if not present.
func findTableDiff(d ir.SchemaDiff, name string) *ir.TableDiff {
	for i := range d.TablesMismatched {
		if d.TablesMismatched[i].Name == name {
			return &d.TablesMismatched[i]
		}
	}
	return nil
}

// findColumnDiff returns the ColumnDiff for name, or nil if not present.
func findColumnDiff(cds []ir.ColumnDiff, name string) *ir.ColumnDiff {
	for i := range cds {
		if cds[i].Name == name {
			return &cds[i]
		}
	}
	return nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
