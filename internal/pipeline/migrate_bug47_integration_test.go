//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 47 (MySQL writer corrupts empty JSON object
// `{}` to empty JSON array `[]` on bulk copy).
//
// Pre-fix shape: a MySQL JSON source column carrying `{}` round-tripped
// through sluice and landed on a MySQL target as `[]`. The MySQL row
// writer's `convertArrayLikeToJSON` `[]byte` branch routed
// `[]byte("{}")` through the PG-array parser, which empties the
// literal to `[]`. Pre-existing back to v0.20.0; not a v0.29.0
// regression.
//
// The fix: distinguish the two real-world paths that converge at
// `prepareValue([]byte("{}"), ir.JSON{...})`:
//   - Bug 47 path: MySQL JSON source with value `{}`. Column has no
//     `SourceColumnType` (no override fired). Must emit `{}`.
//   - Bug 14 path: PG `text[]` source with `--type-override=jsonb`
//     and empty array value. Column carries `SourceColumnType =
//     ir.Array{...}` after translate.ApplyMappings recorded the
//     pre-override type. Must emit `[]`.
//
// Both paths are exercised: this test covers Bug 47 (no override);
// `migrate_bug14_integration_test.go` covers Bug 14 (override).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestMigrate_MySQLToMySQL_PreservesEmptyJSONObject pins the Bug 47
// closure: an empty JSON object `{}` in a MySQL JSON source column
// round-trips through sluice and lands on a MySQL target as `{}`
// (object, not array). Mirrors the canonical BUG-CATALOG repro.
func TestMigrate_MySQLToMySQL_PreservesEmptyJSONObject(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE attrs_t (
			id    BIGINT AUTO_INCREMENT PRIMARY KEY,
			email VARCHAR(120) NOT NULL UNIQUE,
			attrs JSON DEFAULT NULL
		) ENGINE=InnoDB;
		INSERT INTO attrs_t (email, attrs) VALUES
			('a@example.com', '{"role":"admin"}'),
			('b@example.com', '{}'),
			('c@example.com', '[]'),
			('d@example.com', '[1,2,3]'),
			('e@example.com', 'null'),
			('f@example.com', '"hello"');
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Read back each row's JSON value AND its JSON_TYPE so the
	// OBJECT-vs-ARRAY distinction (the load-bearing semantic in
	// Bug 47) is observable to the assertion.
	type row struct {
		email string
		attrs string
		jt    string
	}
	want := []row{
		{"a@example.com", `{"role": "admin"}`, "OBJECT"},
		{"b@example.com", `{}`, "OBJECT"},
		{"c@example.com", `[]`, "ARRAY"},
		{"d@example.com", `[1, 2, 3]`, "ARRAY"},
		{"e@example.com", `null`, "NULL"},
		{"f@example.com", `"hello"`, "STRING"},
	}

	db, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		"SELECT email, attrs, JSON_TYPE(attrs) FROM attrs_t ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.email, &r.attrs, &r.jt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.email != w.email {
			t.Errorf("row[%d] email: got %q; want %q", i, g.email, w.email)
		}
		// JSON_TYPE is the load-bearing assertion: Bug 47's signature
		// is OBJECT silently flipping to ARRAY on the target. Compare
		// types strictly; compare attrs textually with whitespace-
		// tolerance via normaliseJSON (defined in
		// migrate_bug14_integration_test.go) so MySQL version-to-
		// version formatting differences don't trip the test.
		if g.jt != w.jt {
			t.Errorf("row[%d] (%s) JSON_TYPE: got %q; want %q (Bug 47 signature: OBJECT→ARRAY corruption)",
				i, g.email, g.jt, w.jt)
		}
		if normaliseJSON(g.attrs) != normaliseJSON(w.attrs) {
			t.Errorf("row[%d] (%s) attrs: got %q; want %q",
				i, g.email, g.attrs, w.attrs)
		}
	}
}
