// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Registry roster for the shape-compare index-NAME rule (Bug 234).
// External test package so it can import the engine implementations
// (which themselves import translate) without a cycle — the
// [TestIsMySQLFamily_MatchesRegistryDDLDialect] idiom.

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

// indexNamingRoster records, for every registered engine used as a
// TARGET, whether its SchemaWriter creates a secondary index under the
// source's own name or under a transformed one — and therefore whether
// the expected side of a shape comparison has to transform it too.
//
// true  = the writer TRANSFORMS the name (today: table-prefixing, because
//
//	the engine's index namespace is schema-scoped rather than
//	table-scoped).
//
// false = verbatim.
//
// # What this gate does and does not prove
//
// It proves that a DECISION exists for every registered engine: adding an
// engine without classifying it fails the build, which is the sibling-
// sweep step made mechanical. It does NOT independently prove the
// classification is right — the roster and the rule are both written by
// the same hand, so a wrong entry here would agree with a wrong rule.
// The independent evidence is behavioural and lives elsewhere:
// TestSchemaDiffAfterMigrate_* in internal/pipeline migrates a table
// across a real engine pair and then asks `schema diff` whether the
// target it just created has drifted. That test compares against the
// TARGET CATALOG's read-back, which nothing here can fake.
var indexNamingRoster = map[string]bool{
	// Index names are schema-scoped on Postgres, so two same-named
	// indexes on different tables would collide; the writer prefixes
	// with the table name (translate.PGEffectiveIndexName).
	"postgres":         true,
	"postgres-trigger": true,

	// MySQL-family: index names are TABLE-scoped, so no disambiguation
	// is needed and internal/engines/mysql.emitCreateIndex quotes
	// idx.Name as-is.
	"mysql":       false,
	"mariadb":     false,
	"planetscale": false,
	"vitess":      false,

	// SQLite-family: internal/engines/sqlite.emitCreateIndex quotes
	// idx.Name as-is.
	"sqlite":         false,
	"sqlite-trigger": false,
	"d1":             false,
	"d1-trigger":     false,

	// mydumper is a dump READER; it has no SchemaWriter and can never be
	// a diff target. Classified verbatim so an accidental use is inert.
	"mydumper": false,
}

func TestIndexNameRuleRosterCoversEveryRegisteredEngine(t *testing.T) {
	names := engines.Names()
	if len(names) == 0 {
		t.Fatal("no engines registered — the blank imports above should have registered them")
	}

	var transforms, verbatim int
	for _, name := range names {
		want, classified := indexNamingRoster[name]
		if !classified {
			t.Errorf("engine %q is registered but not classified in indexNamingRoster — decide whether its SchemaWriter "+
				"creates a secondary index under the source's own name (false) or a transformed one (true); an unclassified "+
				"engine silently defaults to verbatim and reports phantom index drift on every `schema diff` if that is wrong",
				name)
			continue
		}

		// Behavioural, not a re-read of the roster: run the real entry
		// point and see whether the name moved.
		s := &ir.Schema{Tables: []*ir.Table{{
			Name:    "orders",
			Indexes: []*ir.Index{{Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email"}}}},
		}}}
		got := translate.RetargetForShapeCompare(s, "mysql", name).Tables[0].Indexes[0].Name
		transformed := got != "idx_email"
		if transformed != want {
			t.Errorf("RetargetForShapeCompare renamed %q's index to %q (transformed=%v); roster says transformed=%v",
				name, got, transformed, want)
		}
		if want {
			transforms++
		} else {
			verbatim++
		}
	}

	// Anti-vacuity, both directions: a roster that classified everything
	// the same way could not tell the two classes apart, and would pass
	// against a rule that fired never or always.
	if transforms == 0 {
		t.Error("no registered engine transforms index names; the rule is grading nothing")
	}
	if verbatim == 0 {
		t.Error("every registered engine transforms index names; the gate cannot distinguish the two classes")
	}
}

// TestIndexNamingRosterHasNoStaleEntries is the other direction: an
// entry for an engine the registry no longer knows is a claim nobody
// checks.
func TestIndexNamingRosterHasNoStaleEntries(t *testing.T) {
	registered := make(map[string]bool, len(engines.Names()))
	for _, name := range engines.Names() {
		registered[name] = true
	}
	for name := range indexNamingRoster {
		if !registered[name] {
			t.Errorf("indexNamingRoster classifies %q, which is not a registered engine — drop the entry", name)
		}
	}
}
