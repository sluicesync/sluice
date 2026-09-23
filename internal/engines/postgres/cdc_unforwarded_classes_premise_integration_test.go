//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestRelationFacts_DiffPremisesOnARealServer grounds the unforwarded-class
// door's exemption rules in what a real PostgreSQL actually writes, rather
// than in fixture values chosen to agree with the code (the 2026-09-23
// pre-tag review found one such fixture: a retype "re-rendering" a default
// as `0::bigint`, which PG never produces). Each step reads the facts
// through readRelationFacts — the production reader — and diffs them with
// diffRelationFacts:
//
//  1. RENAME COLUMN referenced by a policy and a CHECK: no delta (the
//     stored node trees name columns by attnum).
//  2. ALTER POLICY in the same window as another rename: a delta (the
//     rename must not mask it — the retired anyRenamed exemption did).
//  3. ALTER TYPE int→bigint with SET DEFAULT in one statement: a delta
//     (a retype does not re-render the default, so it must not exempt it).
//  4. ALTER TYPE int→bigint alone on a defaulted column: no delta.
//  5. DROP of an EXCLUDE with an expression element: a delta (conkey
//     carries attnum 0 for the expression).
//  6. A CHECK replaced, and an FK's ON DELETE changed, in the same window
//     as a rename of a column they read: a delta each (second-pass review
//     finding 2 — the rename exemption had still reached both kinds).
func TestRelationFacts_DiffPremisesOnARealServer(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE EXTENSION IF NOT EXISTS btree_gist`)
	exec(`CREATE TABLE prem_parent (id INT PRIMARY KEY)`)
	exec(`CREATE TABLE prem (
		id INT PRIMARY KEY,
		tenant TEXT NOT NULL,
		amt INT DEFAULT 0,
		parent_id INT CONSTRAINT prem_parent_fk REFERENCES prem_parent(id) ON DELETE CASCADE,
		s TIMESTAMP, e TIMESTAMP,
		CONSTRAINT prem_amt_pos CHECK (amt >= 0),
		CONSTRAINT prem_no_overlap EXCLUDE USING gist (tenant WITH =, tsrange(s, e) WITH &&))`)
	exec(`ALTER TABLE prem ENABLE ROW LEVEL SECURITY`)
	exec(`CREATE POLICY prem_iso ON prem USING (tenant = current_user)`)
	var oid uint32
	if err := db.QueryRowContext(ctx, `SELECT 'prem'::regclass::oid`).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	read := func() *pgRelationFacts {
		t.Helper()
		facts, err := readRelationFacts(ctx, db, oid)
		if err != nil {
			t.Fatal(err)
		}
		if facts[oid] == nil {
			t.Fatal("readRelationFacts returned no facts for the table")
		}
		return facts[oid]
	}
	step := func(name, ddl string, wantDelta string) {
		t.Helper()
		prior := read()
		exec(ddl)
		deltas := diffRelationFacts(prior, read())
		joined := strings.Join(deltas, "; ")
		switch {
		case wantDelta == "" && len(deltas) != 0:
			t.Errorf("%s: deltas = %q; want none", name, deltas)
		case wantDelta != "" && !strings.Contains(joined, wantDelta):
			t.Errorf("%s: deltas = %q; want one containing %q", name, deltas, wantDelta)
		}
	}

	step("rename referenced by a policy and a CHECK", `ALTER TABLE prem RENAME COLUMN tenant TO org`, "")
	step("rename referenced by a CHECK", `ALTER TABLE prem RENAME COLUMN amt TO amount`, "")
	step("policy edit in the same window as a rename",
		`ALTER TABLE prem RENAME COLUMN org TO owner; ALTER POLICY prem_iso ON prem USING (true)`,
		`ALTER POLICY "prem_iso"`)
	step("retype alone on a defaulted column", `ALTER TABLE prem ALTER COLUMN amount TYPE bigint`, "")
	step("retype plus SET DEFAULT in one statement",
		`ALTER TABLE prem ALTER COLUMN amount TYPE numeric, ALTER COLUMN amount SET DEFAULT 7`,
		`ALTER COLUMN "amount" SET DEFAULT 7`)
	step("CHECK replaced beside a rename of the column it reads",
		`ALTER TABLE prem RENAME COLUMN amount TO amt2; ALTER TABLE prem DROP CONSTRAINT prem_amt_pos, ADD CONSTRAINT prem_amt_pos CHECK (amt2 > 100)`,
		`CONSTRAINT "prem_amt_pos" changed`)
	step("FK action changed beside a rename of its column",
		`ALTER TABLE prem RENAME COLUMN parent_id TO parent; ALTER TABLE prem DROP CONSTRAINT prem_parent_fk, ADD CONSTRAINT prem_parent_fk FOREIGN KEY (parent) REFERENCES prem_parent(id) ON DELETE RESTRICT`,
		`CONSTRAINT "prem_parent_fk" changed`)
	step("drop of an EXCLUDE with an expression element",
		`ALTER TABLE prem DROP CONSTRAINT prem_no_overlap`,
		`DROP CONSTRAINT "prem_no_overlap"`)
}
