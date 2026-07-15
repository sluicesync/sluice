// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Registry-parity pin for IsMySQLFamily (Bug 186's class fix): the
// helper's name list must match, in BOTH directions, the set of
// registered engines that declare ir.DDLDialectMySQL. Registering a
// new MySQL-dialect engine (a flavor, a dump reader) without adding it
// to the helper fails here instead of silently missing every
// cross-engine notice; listing an engine the registry doesn't know as
// MySQL-dialect fails too. This is an external test package so it can
// import the engine implementations (which themselves import
// translate) without a cycle.

package translate_test

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
	"sluicesync.dev/sluice/internal/translate"
)

func TestIsMySQLFamily_MatchesRegistryDDLDialect(t *testing.T) {
	names := engines.Names()
	if len(names) == 0 {
		t.Fatal("no engines registered — the blank imports above should have registered them")
	}
	for _, name := range names {
		eng, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) failed for a name engines.Names() returned", name)
		}
		wantFamily := eng.Capabilities().DDLDialect == ir.DDLDialectMySQL
		if got := translate.IsMySQLFamily(name); got != wantFamily {
			t.Errorf("IsMySQLFamily(%q) = %v; registry declares DDLDialectMySQL = %v — keep the helper and the engine Capabilities in sync (Bug 186)",
				name, got, wantFamily)
		}
	}
}
