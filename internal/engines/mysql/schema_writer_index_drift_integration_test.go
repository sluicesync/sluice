//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration ground truth for the MED-D0-8 definition-drift advisory:
// the unit pins script the information_schema.statistics wire shape, so
// this file re-derives it against a REAL MySQL catalog — per-column
// COLLATION 'D' for DESC keys, SUB_PART for prefixes, NULL COLUMN_NAME
// for functional key parts, and the SPATIAL index's catalog-noise
// SUB_PART (which reports 32 even though no prefix is buildable) — the
// normalization the advisory depends on to not false-WARN.
//
// Matrix: {same definition incl. prefix+desc, functional same,
// spatial same} → NO WARN; {different columns, different uniqueness}
// → WARN, while the skip itself (no rebuild, no error) is unchanged.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestIndexDrift_RealCatalog(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Pre-create the table WITH its indexes — the "operator got here
	// first" shape the detect-then-skip path meets on resume. Three
	// indexes match the intended definitions exactly (incl. a prefix, a
	// DESC key, a functional key part, and a SPATIAL index); two share
	// only the NAME with the intent.
	stmts := []string{
		"CREATE TABLE drift_t (" +
			"id BIGINT NOT NULL, a BIGINT, b VARCHAR(64), email VARCHAR(128), pt POINT NOT NULL SRID 0, " +
			"PRIMARY KEY (id))",
		"ALTER TABLE drift_t ADD INDEX idx_same (a, b(10) DESC)",
		"ALTER TABLE drift_t ADD INDEX idx_func ((lower(email)))",
		"ALTER TABLE drift_t ADD SPATIAL INDEX idx_geo (pt)",
		"ALTER TABLE drift_t ADD INDEX idx_cols (b)", // intended: (a)
		"ALTER TABLE drift_t ADD INDEX idx_uq (a)",   // intended: UNIQUE (a)
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "drift_t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Varchar{Length: 64}},
			{Name: "email", Type: ir.Varchar{Length: 128}},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "idx_same", Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b", Length: 10, Desc: true}}},
			{Name: "idx_func", Columns: []ir.IndexColumn{{Expression: "lower(`email`)", ExpressionDialect: "mysql"}}},
			{Name: "idx_geo", Kind: ir.IndexKindSpatial, Columns: []ir.IndexColumn{{Column: "pt"}}},
			{Name: "idx_cols", Columns: []ir.IndexColumn{{Column: "a"}}},
			{Name: "idx_uq", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}}},
		},
	}}}

	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := swHandle.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes over pre-existing indexes must skip, not fail: %v", err)
	}

	logged := buf.String()

	// The matching definitions — including the prefix+DESC composite,
	// the functional key part, and the SPATIAL index whose catalog
	// reports a SUB_PART no DDL asked for — must NOT be flagged.
	for _, quiet := range []string{"idx_same", "idx_func", "idx_geo"} {
		if strings.Contains(logged, quiet) {
			t.Errorf("false drift WARN for matching index %s:\n%s", quiet, logged)
		}
	}
	// The two name-only matches must be flagged, with the uniqueness
	// divergence naming the duplicate-acceptance hazard.
	if !strings.Contains(logged, "idx_cols") || !strings.Contains(logged, "DIFFERENT definition") {
		t.Errorf("missing the different-columns WARN for idx_cols:\n%s", logged)
	}
	if !strings.Contains(logged, "idx_uq") || !strings.Contains(logged, "DIFFERENT UNIQUENESS") ||
		!strings.Contains(logged, "duplicate writes") {
		t.Errorf("missing the uniqueness WARN for idx_uq:\n%s", logged)
	}

	// The skip is advisory-only: nothing was rebuilt, so the existing
	// divergent definitions are still in place on the target.
	var nonUnique int64
	row := db.QueryRowContext(ctx,
		"SELECT non_unique FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'drift_t' AND index_name = 'idx_uq'")
	if err := row.Scan(&nonUnique); err != nil {
		t.Fatalf("re-read idx_uq: %v", err)
	}
	if nonUnique != 1 {
		t.Errorf("idx_uq non_unique = %d; the advisory must not have rebuilt it as UNIQUE", nonUnique)
	}
}
