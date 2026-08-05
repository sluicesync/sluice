// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// engineFamilyRoster classifies EVERY registered engine name for cross-engine
// supportability. It is curated on purpose — see the test below for why this
// cannot be derived from a capability — and it is fail-by-default: a newly
// registered engine that appears in no bucket fails the build until someone
// decides which one it belongs in.
//
// The buckets mean:
//
//	pg      — a Postgres-family SOURCE, whose PG-only shapes (PostGIS geometry,
//	          extension opclasses, EXCLUDE constraints) must be refused when the
//	          target cannot carry them. isPGSourceEngine must return true.
//	mysql   — a MySQL-dialect engine with no PG-native type surface.
//	          IsMySQLFamilyEngine must return true.
//	neither — everything else. Both predicates must return false.
var engineFamilyRoster = map[string]string{
	"postgres":         "pg",
	"postgres-trigger": "pg",

	"mysql":       "mysql",
	"planetscale": "mysql",
	"vitess":      "mysql",
	"mariadb":     "mysql",
	"mydumper":    "mysql",

	"sqlite":         "neither",
	"sqlite-trigger": "neither",
	"d1":             "neither",
	"d1-trigger":     "neither",
	"flatfile":       "neither",
}

// engineRegistrationPattern matches an engine self-registration, e.g.
//
//	engines.Register("postgres-trigger", Engine{})
//	engines.Register(EngineName, ...)
//
// Only the string-literal form is captured; a constant-named registration is
// picked up by the companion constant pattern below.
var (
	engineRegistrationPattern = regexp.MustCompile(`engines\.Register\(\s*"([a-z0-9-]+)"`)
	engineNameConstPattern    = regexp.MustCompile(`EngineName\s*=\s*"([a-z0-9-]+)"`)
)

// TestEngineFamilyRoster_CoversEveryRegisteredEngine is the gate the
// hand-kept family lists never had (audit 2026-08-05 C-2).
//
// # Why a curated roster and not a capability read
//
// [isPGSourceEngine] and [IsMySQLFamilyEngine] classify by NAME, and that is
// correct: the engine identity they are handed is a lineage-recorded string
// from a backup manifest, so at restore time the source database is not
// connected and its engine need not even be registered in this binary. The
// names ARE the durable record; a capability declaration is not resolvable.
//
// So the classification cannot be derived. What CAN be derived is the set of
// names that must be classified — and that set is exactly what drifted. Both
// functions carried a comment saying "keep this list in lock-step with the
// engine registrations" and nothing checked it. That is the same hand-kept
// list with a lock-step comment that produced the mariadb accident on the
// other operand, and PG flavors have already been announced.
//
// This test does not decide which family a new engine is in. It refuses to let
// anyone register one without deciding.
func TestEngineFamilyRoster_CoversEveryRegisteredEngine(t *testing.T) {
	registered := discoverRegisteredEngines(t)

	// Anti-vacuity floor. Eleven engines are registered today; a scan that
	// finds far fewer has stopped matching how engines register, and a silent
	// near-zero is how a derived gate rots into a green no-op.
	if len(registered) < 10 {
		t.Fatalf("discovered only %d registered engine name(s) %v; expected at least 10.\n\n"+
			"The scanner no longer matches how engines self-register — fix the scanner rather "+
			"than lowering the floor, or this gate silently checks nothing.",
			len(registered), registered)
	}

	for _, name := range registered {
		family, classified := engineFamilyRoster[name]
		if !classified {
			t.Errorf("engine %q is registered but not classified in engineFamilyRoster.\n\n"+
				"Cross-engine supportability refuses PG-only shapes by ENGINE NAME (the identity a "+
				"backup manifest records), so an unclassified engine silently gets the "+
				"neither-family answer: no PG refusals, no MySQL-family refusals. Add it to the "+
				"roster with the family it belongs to.", name)
			continue
		}
		gotPG := isPGSourceEngine(name)
		gotMySQL := IsMySQLFamilyEngine(name)

		wantPG := family == "pg"
		wantMySQL := family == "mysql"
		if gotPG != wantPG {
			t.Errorf("isPGSourceEngine(%q) = %v; roster says family %q so want %v", name, gotPG, family, wantPG)
		}
		if gotMySQL != wantMySQL {
			t.Errorf("IsMySQLFamilyEngine(%q) = %v; roster says family %q so want %v", name, gotMySQL, family, wantMySQL)
		}
	}

	// The other direction: a roster entry naming an engine that no longer
	// registers is stale, and a stale entry is how the roster stops being a
	// description of reality.
	reg := map[string]bool{}
	for _, n := range registered {
		reg[n] = true
	}
	var stale []string
	for name := range engineFamilyRoster {
		if !reg[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("engineFamilyRoster names engines that are not registered anywhere: %v\n\n"+
			"Either the engine was removed and the entry should go, or it was renamed and the "+
			"family predicates are now classifying a name nothing produces.", stale)
	}
}

// discoverRegisteredEngines scans the engine packages for self-registrations.
// Source-scanning rather than importing the registry keeps the archgate
// layering intact — this package must never import a specific engine package,
// and a test that did would make that boundary untestable.
func discoverRegisteredEngines(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "engines")
	found := map[string]bool{}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		src := string(b)
		for _, m := range engineRegistrationPattern.FindAllStringSubmatch(src, -1) {
			found[m[1]] = true
		}
		for _, m := range engineNameConstPattern.FindAllStringSubmatch(src, -1) {
			found[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk engines/: %v", err)
	}

	out := make([]string, 0, len(found))
	for n := range found {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
