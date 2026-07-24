//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-constraint attribute fidelity (v0.100 readiness C3, roadmap
// "UNIQUE-constraint attribute fidelity"). A PG-15 `UNIQUE NULLS NOT
// DISTINCT` or a DEFERRABLE UNIQUE constraint lands on every target —
// including same-engine PG — as a plain UNIQUE: silently WEAKENED (the
// target admits duplicate NULLs the source rejected / enforces
// immediately what the source deferred). The reader now reads the
// pg_constraint attributes, carries them on the IR, and WARNs loudly
// once per affected constraint at read time — the single chokepoint
// every consumer path (migrate, sync, backup, preview) flows through.
// Faithful same-engine carry is the filed follow-up; this pins the
// honest-WARN floor on real PG 16.
//
// PG 18's `WITHOUT OVERLAPS` (`conperiod`) cell is version-gated in the
// catalog read and covered by the unit matrix only — the rig images are
// PG 16, where the column does not exist (the read must not 42703).

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaReader_UniqueConstraintAttrs_WarnOncePerConstraint(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE attr_holders (
			id    BIGINT PRIMARY KEY,
			ref   TEXT,
			code  TEXT,
			plain TEXT,
			CONSTRAINT attr_ref_nnd   UNIQUE NULLS NOT DISTINCT (ref),
			CONSTRAINT attr_code_def  UNIQUE (code) DEFERRABLE,
			CONSTRAINT attr_plain_uni UNIQUE (plain)
		);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	buf := captureSlog(t)
	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	out := buf.String()

	// The NULLS NOT DISTINCT constraint gets a WARN naming the
	// attribute and the weaker landing, exactly once.
	if !strings.Contains(out, "attr_ref_nnd") || !strings.Contains(out, "NULLS NOT DISTINCT") {
		t.Errorf("missing WARN for the NULLS NOT DISTINCT constraint:\n%s", out)
	}
	if n := strings.Count(out, "attr_ref_nnd"); n > 1 {
		t.Errorf("NULLS NOT DISTINCT constraint warned %d times; want exactly once:\n%s", n, out)
	}

	// The DEFERRABLE constraint gets its own WARN, exactly once.
	if !strings.Contains(out, "attr_code_def") || !strings.Contains(out, "DEFERRABLE") {
		t.Errorf("missing WARN for the DEFERRABLE constraint:\n%s", out)
	}
	if n := strings.Count(out, "attr_code_def"); n > 1 {
		t.Errorf("DEFERRABLE constraint warned %d times; want exactly once:\n%s", n, out)
	}

	// The plain UNIQUE constraint carries no attribute — no WARN.
	if strings.Contains(out, "attr_plain_uni") {
		t.Errorf("plain UNIQUE constraint must not WARN:\n%s", out)
	}

	// A WARN, not a refusal: the read completed and all three
	// constraints are carried as constraint-backed unique indexes.
	tbl := findTable(schema, "attr_holders")
	if tbl == nil {
		t.Fatalf("attr_holders missing from schema; have %v", tableNames(schema))
	}
	byName := map[string]*ir.Index{}
	for _, idx := range tbl.Indexes {
		byName[idx.Name] = idx
	}
	for _, name := range []string{"attr_ref_nnd", "attr_code_def", "attr_plain_uni"} {
		idx, ok := byName[name]
		if !ok {
			t.Fatalf("constraint %q missing from Indexes; have %v", name, indexNamesOf(tbl))
		}
		if !idx.Unique || !idx.ConstraintBacked {
			t.Errorf("constraint %q: Unique=%v ConstraintBacked=%v; want both true", name, idx.Unique, idx.ConstraintBacked)
		}
	}

	// The attributes are carried on the IR (metadata-only; the emitters
	// still land plain UNIQUE — that is exactly what the WARN names).
	if idx := byName["attr_ref_nnd"]; !idx.ConstraintNullsNotDistinct || idx.ConstraintDeferrable || idx.ConstraintWithoutOverlaps {
		t.Errorf("attr_ref_nnd carry = {nnd:%v def:%v overlaps:%v}; want {true false false}",
			idx.ConstraintNullsNotDistinct, idx.ConstraintDeferrable, idx.ConstraintWithoutOverlaps)
	}
	if idx := byName["attr_code_def"]; !idx.ConstraintDeferrable || idx.ConstraintNullsNotDistinct || idx.ConstraintWithoutOverlaps {
		t.Errorf("attr_code_def carry = {nnd:%v def:%v overlaps:%v}; want {false true false}",
			idx.ConstraintNullsNotDistinct, idx.ConstraintDeferrable, idx.ConstraintWithoutOverlaps)
	}
	if idx := byName["attr_plain_uni"]; idx.ConstraintDeferrable || idx.ConstraintNullsNotDistinct || idx.ConstraintWithoutOverlaps {
		t.Errorf("attr_plain_uni must carry no attribute flags; got {nnd:%v def:%v overlaps:%v}",
			idx.ConstraintNullsNotDistinct, idx.ConstraintDeferrable, idx.ConstraintWithoutOverlaps)
	}
}

func indexNamesOf(tbl *ir.Table) []string {
	names := make([]string, 0, len(tbl.Indexes))
	for _, idx := range tbl.Indexes {
		names = append(names, idx.Name)
	}
	return names
}
