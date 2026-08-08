// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"database/sql"
	"strings"
	"testing"
)

// TestPremise_SQLiteForbidsAGeneratedPrimaryKey ground-truths the fact
// this engine's exemption from the generated-identity roster
// (internal/engines/generated_identity_roster_test.go) rests on.
//
// The generated-PRIMARY-KEY identity class — a key column the change
// stream cannot carry, so the applier's WHERE narrows to a non-unique
// prefix or to nothing — is refused in the MySQL binlog reader and the
// Postgres pgoutput reader. The trigger engines are exempt on the
// grounds that SQLite CANNOT EXPRESS the shape. That is a claim about
// SQLite, not about sluice, so it gets a check rather than a sentence
// (the premise-naming step): if a future SQLite ever allows it, this
// fails and the exemption has to be re-argued instead of quietly
// becoming wrong.
//
// Covers both generation kinds and both key shapes, because "generated
// columns cannot be part of the PRIMARY KEY" is one rule and a
// single-column pin would not show that.
//
// The same engine code backs `d1-trigger` (the D1 transport is a
// different wire, the same SQLite grammar), so this covers it too.
func TestPremise_SQLiteForbidsAGeneratedPrimaryKey(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	cases := []struct {
		name string
		ddl  string
	}{
		{
			name: "STORED, sole primary key",
			ddl:  `CREATE TABLE a (x INT NOT NULL, g INT GENERATED ALWAYS AS (x*2) STORED PRIMARY KEY)`,
		},
		{
			name: "STORED, one member of a composite primary key",
			ddl:  `CREATE TABLE b (x INT NOT NULL, g INT GENERATED ALWAYS AS (x*2) STORED, PRIMARY KEY (x, g))`,
		},
		{
			name: "VIRTUAL, one member of a composite primary key",
			ddl:  `CREATE TABLE c (x INT NOT NULL, g INT GENERATED ALWAYS AS (x*2) VIRTUAL, PRIMARY KEY (x, g))`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(t.Context(), tc.ddl)
			if err == nil {
				t.Fatalf(
					"SQLite ACCEPTED a generated PRIMARY KEY column — the premise the trigger engines' "+
						"generated-identity exemption rests on no longer holds; re-derive the exemption in "+
						"internal/engines/generated_identity_roster_test.go. DDL: %s", tc.ddl,
				)
			}
			// The refusal must be the STRUCTURAL one, not an incidental
			// syntax error that would also "pass" a check for non-nil.
			if !strings.Contains(err.Error(), "generated columns cannot be part of the PRIMARY KEY") {
				t.Fatalf("refused for the wrong reason (%v); want the generated-PK structural refusal", err)
			}
			t.Logf("PROVEN: SQLite refuses %q with %v", tc.name, err)
		})
	}
}
