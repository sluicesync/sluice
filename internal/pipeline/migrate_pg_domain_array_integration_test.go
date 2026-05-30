//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for PG DOMAIN-type-as-array round-trip through
// pipeline.Migrator (broader-mining gap #4 —
// docs/dev/notes/test-gap-mining-broader.md).
//
// PG DOMAIN types wrap a base type with optional CHECK constraints —
// `CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)`. The
// challenging shape this pin targets is **an array column whose
// element type is a DOMAIN** — e.g. `positive_int[]`. The source
// catalog reports the column as the array of the DOMAIN; sluice's
// SchemaReader has minimal DOMAIN awareness (a single comment in
// schema_reader.go, no concrete typing path), so the open question
// is whether the migrate:
//
//	(a) Preserves the DOMAIN on the target (creates the DOMAIN first,
//	    then the column with type `positive_int[]`). Same-engine PG→PG
//	    correctness baseline.
//	(b) Silently flattens to the base type (`INTEGER[]` on target).
//	    Loses the CHECK constraint — partial silent-loss class.
//	(c) Refuses loudly at schema-read or schema-write time.
//	    Acceptable per the loud-failure tenet.
//	(d) Crashes mid-migrate with an obscure pgx error. Not loud,
//	    not pretty, but at least visible — still better than (b).
//
// The test runs PG→PG (the simplest baseline) and documents which
// outcome sluice produces today. On (a) it's a forward regression
// guard. On (b) it fails loudly with the CHECK-constraint-loss shape
// named; the operator decides the policy.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_DomainTypeAsArrayElement is the
// regression pin for gap #4. PG DOMAIN over INTEGER + a table with
// an array of that DOMAIN. Migrate must either preserve the DOMAIN
// (option a) or refuse loudly (option c); silent flatten (option b)
// fails the test with a CHECK-constraint-loss diagnostic.
func TestMigrate_PostgresToPostgres_DomainTypeAsArrayElement(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);

		CREATE TABLE inventory (
			id        BIGINT PRIMARY KEY,
			label     VARCHAR(64) NOT NULL,
			-- The load-bearing shape: array of the DOMAIN.
			quantities positive_int[] NOT NULL
		);

		INSERT INTO inventory (id, label, quantities) VALUES
			(1, 'widgets', '{1, 5, 12}'),
			(2, 'gadgets', '{99}');
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	runErr := mig.Run(ctx)
	if runErr != nil {
		// Path (c) or (d) — refuse / crash. Acceptable per the tenet
		// IF the error names the DOMAIN-array shape so the operator
		// knows what to do (refuse-loudly is the right policy until a
		// dedicated DOMAIN-aware writer lands).
		errStr := runErr.Error()
		t.Logf("Migrator.Run returned an error on the DOMAIN-as-array shape: %v", runErr)
		hasContext := false
		for _, want := range []string{"domain", "positive_int", "type", "DOMAIN"} {
			if strings.Contains(errStr, want) {
				hasContext = true
				break
			}
		}
		if !hasContext {
			t.Errorf("Migrator.Run failed but the error doesn't name the DOMAIN-array shape; "+
				"operators reading CI output need a hint that the DOMAIN is the cause.\n"+
				"got: %v", runErr)
		}
		// Acceptable path — return without asserting target state.
		return
	}

	// Path (a) or (b): migrate succeeded. Now check the target's
	// column type to distinguish them.
	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	// Did the DOMAIN itself land on the target?
	var domainExists bool
	const domainQ = `
		SELECT EXISTS (
			SELECT 1 FROM pg_type t
			JOIN pg_namespace n ON t.typnamespace = n.oid
			WHERE t.typname = 'positive_int' AND t.typtype = 'd'
		)`
	if err := target.QueryRowContext(ctx, domainQ).Scan(&domainExists); err != nil {
		t.Fatalf("query pg_type for DOMAIN: %v", err)
	}

	// What's the array element type on the target's `quantities` column?
	// pg_attribute.atttypid → pg_type.typname tells us whether it's
	// `_positive_int` (DOMAIN array, path a) or `_int4` (silently
	// flattened to INTEGER array, path b).
	var arrayTypname string
	const colQ = `
		SELECT t.typname FROM pg_attribute a
		JOIN pg_class c     ON a.attrelid = c.oid
		JOIN pg_type t      ON a.atttypid = t.oid
		WHERE c.relname = 'inventory' AND a.attname = 'quantities' AND a.attnum > 0
	`
	if err := target.QueryRowContext(ctx, colQ).Scan(&arrayTypname); err != nil {
		t.Fatalf("query pg_attribute for quantities column type: %v", err)
	}

	switch {
	case domainExists && arrayTypname == "_positive_int":
		// Path (a) — DOMAIN preserved. The correctness baseline.
		t.Logf("DOMAIN positive_int preserved on target + column typed as _positive_int (path a — correctness baseline)")

	case !domainExists && arrayTypname == "_int4":
		// Path (b) — silent flatten to base type. The CHECK constraint
		// is GONE; an INSERT of `{-1}` would succeed on the target where
		// it would fail on the source. That's the silent-loss-of-
		// constraint class.
		violationErr := tryInsertConstraintViolation(target, ctx, "inventory", "(3, 'check-loss-probe', '{-1}')")
		if violationErr == nil {
			t.Errorf("SILENT-LOSS-OF-CONSTRAINT: target DOMAIN flattened to _int4 AND `INSERT … {-1}` succeeded " +
				"(source's CHECK VALUE > 0 is gone). Loud-failure tenet violation. " +
				"Recommend: schema writer should CREATE DOMAIN on target OR refuse loudly with a " +
				"\"--unsupported-domain-flatten\" opt-in for operators who explicitly want the lossy path.")
		} else if !strings.Contains(violationErr.Error(), "violates check constraint") {
			t.Logf("target rejected the violating INSERT but not via CHECK — got: %v", violationErr)
		} else {
			t.Errorf("inconsistent state: domain DROPPED on target but a CHECK constraint somehow "+
				"still rejected `{-1}`. err=%v", violationErr)
		}

	default:
		// Unanticipated landing — surface for triage.
		t.Errorf("unexpected target state: domainExists=%v, quantities_typname=%q\n"+
			"(neither path a `_positive_int`+DOMAIN nor path b `_int4`+no-DOMAIN). "+
			"Investigate the schema writer's DOMAIN handling.",
			domainExists, arrayTypname)
	}

	// Common to (a) and (b): the row data should be present. Loss of
	// rows alongside the type change would be a separate bug.
	var nRows int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM inventory`).Scan(&nRows); err != nil {
		t.Fatalf("count target inventory rows: %v", err)
	}
	if nRows != 2 {
		t.Errorf("target inventory rows = %d; want 2 (the seed)", nRows)
	}
}

// tryInsertConstraintViolation attempts a row insert that would
// violate the source's DOMAIN CHECK constraint. Returns the error
// (which can then be inspected for CHECK-constraint-rejection vs
// success or another shape). Test-only helper.
func tryInsertConstraintViolation(db *sql.DB, ctx context.Context, table, valuesTuple string) error {
	stmt := `INSERT INTO ` + table + ` (id, label, quantities) VALUES ` + valuesTuple
	_, err := db.ExecContext(ctx, stmt)
	if err == nil {
		return nil
	}
	// Wrap in an extra layer so callers can distinguish a true
	// constraint rejection from a transient connection issue.
	return errors.New("constraint-probe failed: " + err.Error())
}
