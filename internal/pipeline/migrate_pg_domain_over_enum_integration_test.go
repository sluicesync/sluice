//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for Bug 255 HALF 1: a PG→PG migrate of a column whose
// type is a DOMAIN over an ENUM base.
//
// A PG enum is a NAMED type — `CREATE DOMAIN d AS <enum>` requires the enum
// type to exist first. Before the fix this died at create-tables with
// `emitCreateDomainType: ... Enum DDL emission requires column context`,
// because the enum-COLUMN path names the enum type from table+column and a
// CREATE DOMAIN has no column. The fix (ddl_emit.emitCreateDomainType +
// schema_writer Phase 1a') names the enum type from the DOMAIN, emits its
// CREATE TYPE first, then the CREATE DOMAIN referencing it by name.
//
// This pin drives the real Migrator end to end and ground-truths on the
// target catalog: the enum type + the domain both land, the column is typed
// as the domain, the enum VALUE ORDER is preserved, the domain's CHECK
// carries, and the rows copy value-exact (src==dst).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestMigrate_PostgresToPostgres_DomainOverEnum(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// Non-alphabetical enum value order (happy, sad, excited) so a pin that
	// only checked set-membership would miss an ordering regression.
	const seedDDL = `
		CREATE TYPE mood AS ENUM ('happy', 'sad', 'excited');
		CREATE DOMAIN dom_mood AS mood CHECK (VALUE <> 'excited');

		CREATE TABLE feelings (
			id  BIGINT PRIMARY KEY,
			m   dom_mood NOT NULL,
			m2  dom_mood            -- nullable, exercises a NULL copy
		);

		INSERT INTO feelings (id, m, m2) VALUES
			(1, 'happy', 'sad'),
			(2, 'sad',   NULL);
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

	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run over a domain-over-enum column failed (Bug 255 HALF 1): %v", err)
	}

	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	// (1) The enum TYPE landed on the target as a real enum (typtype='e').
	var enumKind string
	if err := target.QueryRowContext(ctx,
		`SELECT typtype FROM pg_type WHERE typname = 'mood'`).Scan(&enumKind); err != nil {
		t.Fatalf("query pg_type for the enum base 'mood': %v", err)
	}
	if enumKind != "e" {
		t.Errorf("target type 'mood' typtype = %q; want 'e' (enum)", enumKind)
	}

	// (2) The enum VALUE ORDER is preserved (happy, sad, excited).
	rows, err := target.QueryContext(ctx, `
		SELECT e.enumlabel
		FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid
		WHERE t.typname = 'mood'
		ORDER BY e.enumsortorder`)
	if err != nil {
		t.Fatalf("query pg_enum order: %v", err)
	}
	var labels []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			t.Fatalf("scan enum label: %v", err)
		}
		labels = append(labels, l)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate enum labels: %v", err)
	}
	want := []string{"happy", "sad", "excited"}
	if len(labels) != len(want) {
		t.Fatalf("enum labels = %v; want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Errorf("enum label[%d] = %q; want %q (value order not preserved)", i, labels[i], want[i])
		}
	}

	// (3) The DOMAIN landed (typtype='d') with the enum as its base type.
	var baseType string
	if err := target.QueryRowContext(ctx, `
		SELECT bt.typname
		FROM pg_type d JOIN pg_type bt ON bt.oid = d.typbasetype
		WHERE d.typname = 'dom_mood' AND d.typtype = 'd'`).Scan(&baseType); err != nil {
		t.Fatalf("query pg_type for DOMAIN dom_mood base: %v", err)
	}
	if baseType != "mood" {
		t.Errorf("DOMAIN dom_mood base type = %q; want 'mood'", baseType)
	}

	// (4) The column is typed as the DOMAIN (not flattened to the enum).
	var colTypeName string
	if err := target.QueryRowContext(ctx, `
		SELECT t.typname
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_type  t ON a.atttypid  = t.oid
		WHERE c.relname = 'feelings' AND a.attname = 'm' AND a.attnum > 0`).Scan(&colTypeName); err != nil {
		t.Fatalf("query pg_attribute for feelings.m type: %v", err)
	}
	if colTypeName != "dom_mood" {
		t.Errorf("feelings.m column type = %q; want 'dom_mood' (domain flattened?)", colTypeName)
	}

	// (5) Rows copied value-exact (src==dst), NULL included.
	var m1, m2sql sql.NullString
	if err := target.QueryRowContext(ctx,
		`SELECT m::text, m2::text FROM feelings WHERE id = 1`).Scan(&m1, &m2sql); err != nil {
		t.Fatalf("read feelings id=1: %v", err)
	}
	if !m1.Valid || m1.String != "happy" || !m2sql.Valid || m2sql.String != "sad" {
		t.Errorf("feelings id=1 = (%v,%v); want (happy,sad)", m1, m2sql)
	}
	if err := target.QueryRowContext(ctx,
		`SELECT m::text, m2::text FROM feelings WHERE id = 2`).Scan(&m1, &m2sql); err != nil {
		t.Fatalf("read feelings id=2: %v", err)
	}
	if !m1.Valid || m1.String != "sad" || m2sql.Valid {
		t.Errorf("feelings id=2 = (%v, valid=%v); want (sad, NULL)", m1, m2sql.Valid)
	}

	var nRows int
	if err := target.QueryRowContext(ctx, `SELECT count(*) FROM feelings`).Scan(&nRows); err != nil {
		t.Fatalf("count feelings: %v", err)
	}
	if nRows != 2 {
		t.Errorf("target feelings rows = %d; want 2", nRows)
	}

	// (6) The DOMAIN's CHECK carried: an 'excited' value must be refused on
	// the target exactly as on the source (domain wrapper is not just a
	// type alias — the constraint is load-bearing).
	if _, err := target.ExecContext(ctx,
		`INSERT INTO feelings (id, m) VALUES (3, 'excited')`); err == nil {
		t.Error("target accepted m='excited'; the DOMAIN CHECK (VALUE <> 'excited') was lost")
	}
}
