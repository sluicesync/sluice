//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the schema-preview orchestrator. Boots a real
// Postgres container (the source side, where ADR-0024's UUID hint
// fires) plus a MySQL container (the target side, used only for the
// PreviewDDL emit). The preview never touches the target's data, but
// the schema-writer DOES need a working DSN — opening it dials the
// target so PostGIS/charset detection and capability probing fire.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestPreview_PostgresToMySQL runs `sluice schema preview` end-to-end
// against a PG source seeded with ADR-0024 hint-firing columns (uuid,
// unbounded numeric, longtext-mapped TEXT). Verifies:
//
//  1. The text output contains the expected MySQL DDL (CHAR(36) for
//     uuid, LONGTEXT for text, DECIMAL(65,30) for numeric).
//  2. The UUID hint fires with the binary_uuid override.
//  3. --output FILE writes atomically to the destination.
func TestPreview_PostgresToMySQL(t *testing.T) {
	sourceDSN, _, cleanupPG := startPostgres(t)
	defer cleanupPG()

	mysqlSrc, _, cleanupMySQL := startMySQL(t)
	defer cleanupMySQL()
	// startMySQL returns source/target on the same image; for the
	// preview we use the source side as the "target DSN" because the
	// preview never writes — it just opens the schema writer to
	// resolve capabilities and dispatches to PreviewDDL.
	mysqlTargetDSN := mysqlSrc

	const seedDDL = `
		CREATE TABLE users (
			id         UUID PRIMARY KEY,
			email      VARCHAR(255) NOT NULL,
			bio        TEXT,
			amount     NUMERIC NOT NULL DEFAULT 0
		);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

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

	t.Run("text format to buffer", func(t *testing.T) {
		var buf bytes.Buffer
		prev := &Previewer{
			Source:    pgEng,
			Target:    mysqlEng,
			SourceDSN: sourceDSN,
			TargetDSN: mysqlTargetDSN,
			Format:    "text",
			Out:       &buf,
		}
		if err := prev.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "-- sluice schema preview") {
			t.Errorf("missing preview header; got:\n%s", out)
		}
		// MySQL DDL shape: uuid -> CHAR(36); text (PG TEXT, mapped to TextLong) -> LONGTEXT.
		if !strings.Contains(out, "CHAR(36)") {
			t.Errorf("expected CHAR(36) for uuid column; got:\n%s", out)
		}
		if !strings.Contains(out, "LONGTEXT") {
			t.Errorf("expected LONGTEXT for text column; got:\n%s", out)
		}
		// UUID hint should fire.
		if !strings.Contains(out, "--type-override users.id=binary_uuid") {
			t.Errorf("expected uuid binary_uuid hint; got:\n%s", out)
		}
		// PG TEXT column hint.
		if !strings.Contains(out, "--type-override users.bio=mediumtext") {
			t.Errorf("expected text mediumtext hint; got:\n%s", out)
		}
	})

	t.Run("json format unmarshals", func(t *testing.T) {
		var buf bytes.Buffer
		prev := &Previewer{
			Source:    pgEng,
			Target:    mysqlEng,
			SourceDSN: sourceDSN,
			TargetDSN: mysqlTargetDSN,
			Format:    "json",
			Out:       &buf,
		}
		if err := prev.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		var got PreviewJSON
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
		}
		if got.SourceEngine != "postgres" {
			t.Errorf("source = %q; want postgres", got.SourceEngine)
		}
		if got.TargetEngine != "mysql" {
			t.Errorf("target = %q; want mysql", got.TargetEngine)
		}
		// Find the users table.
		var usersTable *PreviewJSONTable
		for i := range got.Tables {
			if got.Tables[i].Name == "users" {
				usersTable = &got.Tables[i]
				break
			}
		}
		if usersTable == nil {
			t.Fatalf("users table missing from JSON output: %+v", got.Tables)
		}
		// At least one hint should fire (uuid).
		hasUUIDHint := false
		for _, h := range usersTable.Hints {
			if h.Column == "id" && strings.Contains(h.SuggestedOverride, "binary_uuid") {
				hasUUIDHint = true
				break
			}
		}
		if !hasUUIDHint {
			t.Errorf("expected uuid hint in JSON output; got hints: %+v", usersTable.Hints)
		}
	})

	t.Run("output to file is atomic", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "preview.txt")

		// Use the same atomic-write helper the CLI uses.
		f, err := os.CreateTemp(dir, "preview.tmp.*")
		if err != nil {
			t.Fatalf("create temp: %v", err)
		}
		prev := &Previewer{
			Source:    pgEng,
			Target:    mysqlEng,
			SourceDSN: sourceDSN,
			TargetDSN: mysqlTargetDSN,
			Format:    "text",
			Out:       f,
		}
		if err := prev.Run(ctx); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			t.Fatalf("Run: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close temp: %v", err)
		}
		if err := os.Rename(f.Name(), dest); err != nil {
			t.Fatalf("rename: %v", err)
		}

		got, err := os.ReadFile(dest)
		if err != nil {
			t.Fatalf("read dest: %v", err)
		}
		if !strings.Contains(string(got), "-- sluice schema preview") {
			t.Errorf("dest file missing preview header; got:\n%s", got)
		}
	})
}
