// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Fail-by-default roster for the PG-source classification (audit backlog C-2).
//
// `migcore.IsPGSourceEngine` decides whether a source engine can carry
// PG-native shapes — PostGIS geometry, EXCLUDE constraints, standalone
// sequences, opclass indexes — that a MySQL-family target cannot hold. It is a
// two-name literal, and the risk is not that the two names are wrong: it is
// that a NEW engine registers and is classified "not PG" by omission, silently
// skipping every cross-engine refusal that protects it.
//
// # Why this is a roster and not a derivation
//
// The classification cannot be derived from the engine object, and a previous
// attempt was backed out for trying. At RESTORE time the source engine is a
// lineage string recorded in a backup manifest, and the engine it names may not
// be registered in this process at all — so the NAMES are the durable record,
// not a view of some richer runtime fact. What can be mechanised is the
// DECISION: every registered engine must be classified deliberately, and a new
// one fails the build until someone says which side it is on.
//
// # Why this lives in its own package
//
// The prior attempt's blocker was enumeration. Source-scanning finds 2 of 14,
// because `engines.Register` takes an `ir.Engine` STRUCT rather than a name, so
// the names live in `Name()` implementations and flavor tables. `package
// engines` cannot import the engine packages — they import it, so it would
// cycle. Blank-importing them from a test, exactly as `cmd/sluice/main.go`
// does, makes `engines.Names()` the REAL registry.
//
// It is a separate package because `internal/engines`' own registry_test.go
// calls the package-private reset(), which empties the registry inside that
// test binary — the roster then saw 0 engines and its anti-vacuity floor fired,
// which is the floor earning its place rather than a nuisance. Its own binary
// keeps the registry intact.
package engineroster_test

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// pgSourceRoster classifies every registered engine. The value is the REASON,
// and an entry without one is indistinguishable from an oversight.
var pgSourceRoster = map[string]struct {
	isPGSource bool
	reason     string
}{
	"postgres": {true, "the reference PG engine; its schema surface carries every PG-native shape"},
	"postgres-trigger": {true, "ADR-0066 trigger-based capture for slot-less managed PG. Its schema " +
		"surface DELEGATES to the vanilla postgres engine, so it can carry PostGIS geometry, EXCLUDE " +
		"constraints and opclass indexes identically — treating it as non-PG would silently skip every " +
		"PG-native refusal for those sources"},

	"mysql":       {false, "MySQL family: no PG-native type surface"},
	"mariadb":     {false, "MySQL family (item 73)"},
	"planetscale": {false, "MySQL family, Vitess-backed"},
	"vitess":      {false, "MySQL family, self-hosted Vitess"},
	"mydumper":    {false, "a MySQL dump-format READER; source-only and MySQL-shaped"},

	"sqlite":         {false, "SQLite has no PG-native type surface; ir.Domain and friends are refused at emit"},
	"sqlite-trigger": {false, "trigger-CDC over SQLite; same schema surface as sqlite"},
	"d1":             {false, "Cloudflare D1 is SQLite over HTTP; same schema surface as sqlite"},
	"d1-trigger":     {false, "trigger-CDC over D1; same schema surface as sqlite"},

	"csv":    {false, "flat-file reader; no database type surface at all"},
	"tsv":    {false, "flat-file reader"},
	"ndjson": {false, "flat-file reader"},
}

// TestPGSourceRosterCoversEveryRegisteredEngine holds the classification to the
// LIVE registry, in both directions.
func TestPGSourceRosterCoversEveryRegisteredEngine(t *testing.T) {
	names := engines.Names()

	// Anti-vacuity floor. The prior attempt's source scan discovered 2 of 14
	// and would have passed while seeing almost nothing; its floor is what
	// caught that, and it is kept here for the same reason. A shrunken registry
	// means the enumeration broke, not that sluice lost engines.
	if len(names) < 12 {
		t.Fatalf("registry reports %d engines; want >= 12 — the enumeration is broken and this roster "+
			"would pass while checking almost nothing (the prior C-2 attempt discovered 2 of 14)", len(names))
	}

	for _, name := range names {
		entry, listed := pgSourceRoster[name]
		if !listed {
			t.Errorf("engine %q is registered but absent from pgSourceRoster.\n"+
				"  Classify it: can it carry PG-native shapes (PostGIS geometry, EXCLUDE constraints, "+
				"standalone sequences, opclass indexes) that a MySQL-family target cannot hold?\n"+
				"  Defaulting to \"not PG\" by omission silently skips every cross-engine refusal that "+
				"protects such a source (audit backlog C-2).", name)
			continue
		}
		if entry.reason == "" {
			t.Errorf("engine %q is classified with an EMPTY reason; an entry without one is "+
				"indistinguishable from an oversight", name)
		}
		if got := migcore.IsPGSourceEngine(name); got != entry.isPGSource {
			t.Errorf("engine %q: migcore.IsPGSourceEngine = %v, roster says %v (%s) — the predicate and "+
				"the roster disagree, so one of them is wrong", name, got, entry.isPGSource, entry.reason)
		}
	}

	// A roster entry for an engine that no longer registers is stale, and a
	// stale entry is how a roster starts describing a tree that moved.
	for name := range pgSourceRoster {
		if _, ok := engines.Get(name); !ok {
			t.Errorf("pgSourceRoster lists %q, which is not registered — drop the entry or restore the engine", name)
		}
	}

	// Anti-vacuity on the POSITIVE class: if nothing classifies as a PG source,
	// the predicate has been emptied and every cross-engine refusal it gates is
	// inert while this test stays green.
	pg := 0
	for _, name := range names {
		if migcore.IsPGSourceEngine(name) {
			pg++
		}
	}
	if pg < 2 {
		t.Errorf("only %d registered engine(s) classify as a PG source; want >= 2 (postgres and "+
			"postgres-trigger) — the predicate is returning false for everything and the refusals it "+
			"gates are inert", pg)
	}
}
