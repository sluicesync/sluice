//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestTableFacts_DiffPremisesOnARealServer grounds two of the binlog
// door's rules in what a real MySQL writes (the 2026-09-23 pre-tag review
// found both resting on assumed renderings). Each step reads facts through
// the production readTableFacts and diffs them with diffTableFacts:
//
//  1. BINARY/VARBINARY default changed AFTER the first NUL byte: a delta.
//     information_schema reads both sides as the same truncated "0x61";
//     only the SHOW CREATE recovery sees the change.
//  2. INT DEFAULT 0 retyped to DECIMAL(5,2): no delta (the default reads
//     back `0.00` — a spelling change, not a value change).
//  3. A retype that also changes the default value: a delta (a Postgres
//     target's forwarded ALTER TYPE carries no DEFAULT).
func TestTableFacts_DiffPremisesOnARealServer(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var schema string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`DROP TABLE IF EXISTS prem`)
	exec(`CREATE TABLE prem (
		id INT PRIMARY KEY,
		b BINARY(3) DEFAULT 0x610000,
		vb VARBINARY(4) DEFAULT 0x6100FF,
		amt INT DEFAULT 0,
		s VARCHAR(10) DEFAULT '007',
		vb2 VARBINARY(4) DEFAULT 0x61
	) ENGINE=InnoDB`)
	qn := schema + ".prem"
	read := func() *mysqlTableFacts {
		t.Helper()
		facts, err := readTableFacts(ctx, db, FlavorVanilla, schema, "prem", nil)
		if err != nil {
			t.Fatal(err)
		}
		if facts[qn] == nil {
			t.Fatalf("readTableFacts returned no facts for %s (got %v)", qn, facts)
		}
		return facts[qn]
	}
	step := func(name, ddl, wantDelta string) {
		t.Helper()
		prior := read()
		exec(ddl)
		deltas := diffTableFacts(prior, read(), nil)
		joined := strings.Join(deltas, "; ")
		switch {
		case wantDelta == "" && len(deltas) != 0:
			t.Errorf("%s: deltas = %q; want none", name, deltas)
		case wantDelta != "" && !strings.Contains(joined, wantDelta):
			t.Errorf("%s: deltas = %q; want one containing %q", name, deltas, wantDelta)
		}
	}

	step("BINARY default changed after its first NUL", `ALTER TABLE prem ALTER COLUMN b SET DEFAULT 0x6100FF`, `ALTER COLUMN "b" SET DEFAULT 0x6100FF`)
	step("VARBINARY default changed after its first NUL", `ALTER TABLE prem ALTER COLUMN vb SET DEFAULT 0x610001`, `ALTER COLUMN "vb" SET DEFAULT 0x610001`)
	step("retype re-renders the default spelling only", `ALTER TABLE prem MODIFY amt DECIMAL(5,2) DEFAULT 0`, "")
	step("retype that also changes the default value", `ALTER TABLE prem MODIFY amt BIGINT DEFAULT 7`, `ALTER COLUMN "amt" SET DEFAULT 7`)
	step("text retype that changes 007 to 7", `ALTER TABLE prem MODIFY s VARCHAR(20) DEFAULT '7'`, `ALTER COLUMN "s" SET DEFAULT 7`)
	step("binary retype that changes 0x61 to 0x0061", `ALTER TABLE prem MODIFY vb2 VARBINARY(8) DEFAULT 0x0061`, `ALTER COLUMN "vb2" SET DEFAULT 0x0061`)
}
