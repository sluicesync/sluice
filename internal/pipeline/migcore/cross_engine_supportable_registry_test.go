// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Registry-parity pin for migcore.IsMySQLFamilyEngine (the mariadb-miss
// class fix): the cross-engine target-family check must match, for every
// registered engine, whether that engine declares ir.DDLDialectMySQL.
//
// The prior hand-kept list EXCLUDED mariadb, so a PG→mariadb migrate
// computed pgToMySQL = false and silently skipped every PG-native
// refusal (EXCLUDE constraints, standalone sequences, PostGIS opclasses)
// — the mariadb writer then dropped an EXCLUDE constraint with no error.
// IsMySQLFamilyEngine now delegates to translate.IsMySQLFamily; this test
// makes registering a new MySQL-dialect engine without covering it fail
// CI rather than silently reopening that silent-loss vector.
//
// External test package so it can import the engine implementations
// (which import translate/ir) without a cycle — the same pattern as
// internal/translate/mysql_family_registry_test.go.

package migcore_test

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestIsMySQLFamilyEngine_MatchesRegistryDDLDialect(t *testing.T) {
	names := engines.Names()
	if len(names) == 0 {
		t.Fatal("no engines registered — the blank imports above should have registered them")
	}
	sawMariaDB := false
	for _, name := range names {
		eng, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) failed for a name engines.Names() returned", name)
		}
		wantFamily := eng.Capabilities().DDLDialect == ir.DDLDialectMySQL
		if got := migcore.IsMySQLFamilyEngine(name); got != wantFamily {
			t.Errorf("IsMySQLFamilyEngine(%q) = %v; registry declares DDLDialectMySQL = %v — a MySQL-dialect target that misses this branch silently skips every PG-native cross-engine refusal",
				name, got, wantFamily)
		}
		if name == "mariadb" {
			sawMariaDB = true
			if !migcore.IsMySQLFamilyEngine(name) {
				t.Error("IsMySQLFamilyEngine(\"mariadb\") = false — the exact regression this test guards")
			}
		}
	}
	if !sawMariaDB {
		t.Error("mariadb engine not registered — the blank import should have registered it")
	}

	// Non-registered / non-MySQL negatives (not covered by the registry loop).
	for _, name := range []string{"postgres", "postgres-trigger", "", "future"} {
		if migcore.IsMySQLFamilyEngine(name) {
			t.Errorf("IsMySQLFamilyEngine(%q) = true; want false", name)
		}
	}
}
