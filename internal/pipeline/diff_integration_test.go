//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the schema-diff orchestrator. Boots a Postgres
// container, applies the same source DDL twice (once into source_db,
// once into target_db with deliberate drift), and asserts the diff
// captures the drift correctly. Verifies both the JSON shape (stable
// for CI consumers) and that the orchestrator survives a real
// SchemaReader on each side.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestDiff_PostgresToPostgres seeds source_db with the canonical
// shape, then seeds target_db with a drifted shape (one column
// missing, one type narrowed, one extra table) and confirms the diff
// catches every variant.
func TestDiff_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const sourceDDL = `
		CREATE TABLE users (
			id          BIGINT      NOT NULL PRIMARY KEY,
			email       VARCHAR(255) NOT NULL,
			created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`
	const targetDDL = `
		CREATE TABLE users (
			id     BIGINT       NOT NULL PRIMARY KEY,
			email  VARCHAR(100) NOT NULL
		);
		CREATE TABLE deprecated_log (
			id BIGINT NOT NULL PRIMARY KEY
		);
	`
	applyPGDDL(t, sourceDSN, sourceDDL)
	applyPGDDL(t, targetDSN, targetDDL)

	pg, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("text format reports drift", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:    pg,
			Target:    pg,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN,
			Format:    "text",
			Out:       &buf,
		}
		diff, err := d.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if diff == nil || !diff.HasChanges() {
			t.Fatalf("expected drift; got %+v", diff)
		}
		out := buf.String()
		if !strings.Contains(out, "users (mismatched)") {
			t.Errorf("expected users mismatch section:\n%s", out)
		}
		if !strings.Contains(out, "deprecated_log (extra on target)") {
			t.Errorf("expected deprecated_log extra section:\n%s", out)
		}
	})

	t.Run("json summary counts drift correctly", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:    pg,
			Target:    pg,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN,
			Format:    "json",
			Out:       &buf,
		}
		if _, err := d.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		var got DiffJSON
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode JSON: %v\noutput:\n%s", err, buf.String())
		}
		if got.Summary.TablesExtra != 1 {
			t.Errorf("tables_extra = %d; want 1", got.Summary.TablesExtra)
		}
		if got.Summary.ColumnsMissing < 1 {
			t.Errorf("columns_missing = %d; want >=1", got.Summary.ColumnsMissing)
		}
		if got.Summary.ColumnsMismatched < 1 {
			t.Errorf("columns_mismatched = %d; want >=1 (varchar 255 vs 100)", got.Summary.ColumnsMismatched)
		}
	})

	t.Run("ignore-extras suppresses extra-table diff", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:       pg,
			Target:       pg,
			SourceDSN:    sourceDSN,
			TargetDSN:    targetDSN,
			IgnoreExtras: true,
			Format:       "json",
			Out:          &buf,
		}
		if _, err := d.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		var got DiffJSON
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
		if got.Summary.TablesExtra != 0 {
			t.Errorf("expected no extras under IgnoreExtras; got %d", got.Summary.TablesExtra)
		}
		// Drift on `users` (missing column + type mismatch) should
		// still surface — IgnoreExtras only affects extra-on-target
		// entries, not missing-from-target ones.
		if got.Summary.ColumnsMissing < 1 {
			t.Errorf("expected missing column under IgnoreExtras; got %d", got.Summary.ColumnsMissing)
		}
	})

	t.Run("bad target DSN surfaces as error", func(t *testing.T) {
		var buf bytes.Buffer
		d := &Differ{
			Source:    pg,
			Target:    pg,
			SourceDSN: sourceDSN,
			TargetDSN: "postgres://nope:nope@127.0.0.1:1/nope?sslmode=disable",
			Out:       &buf,
		}
		_, err := d.Run(ctx)
		if err == nil {
			t.Fatal("expected error for unreachable target DSN")
		}
	})
}
