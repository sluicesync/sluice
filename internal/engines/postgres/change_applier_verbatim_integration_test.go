// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func bug97ProbeDSN() string {
	if v := os.Getenv("PG_PROBE_DSN"); v != "" {
		return v
	}
	return ""
}

// TestLoadColumnTypes_Bug97VerbatimEligibleTypes pins the v0.92.2
// closure of Bug 97's applier-side gap. Pre-fix the applier's
// loadColumnTypes call didn't fetch pg_catalog.format_type and didn't
// set columnMeta.VerbatimEligible, so translateType hit the generic
// loud refusal on the first DML touching ADR-0051/-0070 verbatim-
// carry types (money / xml / tsvector / int4range / pg_lsn / etc.).
// The CDC stream silently diverged and warm-resume hit the same wall.
//
// Per Bug 74's family-dispatch lesson, the pin matrix covers each
// verbatim-eligible family — not one representative — so a future
// drift in the allowlist surfaces here.
func TestLoadColumnTypes_Bug97VerbatimEligibleTypes(t *testing.T) {
	dsn := bug97ProbeDSN()
	if dsn == "" {
		t.Skip("PG_PROBE_DSN not set; export e.g. postgres://postgres:postgres@localhost:5443/postgres")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS bug97_verbatim_pin"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	createCols := []string{
		"id INT PRIMARY KEY",
		// Stage 1 (ADR-0051).
		"tsv TSVECTOR",
		"tsq TSQUERY",
		"r4 INT4RANGE",
		"r8 INT8RANGE",
		"nr NUMRANGE",
		"dr DATERANGE",
		// Stage 2 (ADR-0070, v0.90.0 promotion).
		"x XML",
		"m MONEY",
		"lsn PG_LSN",
		"txs TXID_SNAPSHOT",
		"psn PG_SNAPSHOT",
	}
	wantCols := []string{"tsv", "tsq", "r4", "r8", "nr", "dr", "x", "m", "lsn", "txs", "psn"}
	create := "CREATE TABLE bug97_verbatim_pin (" + strings.Join(createCols, ", ") + ")"
	if _, err := db.ExecContext(ctx, create); err != nil {
		t.Fatalf("CREATE bug97_verbatim_pin: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS bug97_verbatim_pin") }()

	cols, err := loadColumnTypes(ctx, db, "public", "bug97_verbatim_pin")
	if err != nil {
		t.Fatalf("loadColumnTypes: %v — pre-v0.92.2 this is the applier-side Bug 97 failure path", err)
	}

	for _, col := range wantCols {
		c, ok := cols[col]
		if !ok {
			t.Errorf("loadColumnTypes did not return type for column %q", col)
			continue
		}
		v, isVerbatim := c.Type.(ir.VerbatimType)
		if !isVerbatim {
			t.Errorf("col %q: type %T; expected ir.VerbatimType (the verbatim-carry path)", col, c.Type)
			continue
		}
		if v.Definition == "" {
			t.Errorf("col %q: ir.VerbatimType.Definition is empty — pg_catalog.format_type join didn't supply the canonical type name", col)
		}
	}
}
