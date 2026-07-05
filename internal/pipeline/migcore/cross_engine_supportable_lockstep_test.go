// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines/postgres"
)

// TestIsCrossEngineTranslatablePGExtension_LockStepWithCatalog mechanically
// pins the pipeline's hand-written mirror against the postgres engine's ACTUAL
// catalog declaration. isCrossEngineTranslatablePGExtension duplicates
// `crossEngineDefaultTranslatedExtensions` because the orchestrator's prod
// code must not import engine packages (engine-neutral rule) — but this TEST
// may, so catalog drift in EITHER direction fails here instead of surfacing at
// a user's migrate:
//
//   - under-inclusion (an extension gains a default translator, the mirror
//     isn't updated) → spurious loud refusal of a translatable extension;
//   - over-inclusion (the mirror claims a translator the catalog dropped or
//     never had) → the preflight waves through a column the MySQL side then
//     cannot faithfully hold.
//
// The probe universe is the engine's full recognised catalog plus
// never-recognised names, so the hand-enumerated pin in
// TestIsCrossEngineTranslatablePGExtension stays as documentation while THIS
// test is the drift gate.
func TestIsCrossEngineTranslatablePGExtension_LockStepWithCatalog(t *testing.T) {
	translated := map[string]bool{}
	for _, name := range postgres.CrossEngineDefaultTranslatedExtensionNames() {
		translated[name] = true
	}
	if len(translated) == 0 {
		t.Fatal("postgres catalog declares no default-translated extensions — the accessor (or the catalog) broke; the lock-step test would be vacuous")
	}

	universe := postgres.RecognisedPGExtensionNames()
	if len(universe) <= len(translated) {
		t.Fatalf("recognised catalog (%d) not larger than the translated set (%d) — the over-inclusion direction would be untested", len(universe), len(translated))
	}
	// Every translated extension must be a recognised one (a translator for an
	// extension the catalog doesn't know is unreachable via
	// --enable-pg-extension).
	for name := range translated {
		found := false
		for _, u := range universe {
			if u == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("catalog declares a default translator for %q but does not recognise it", name)
		}
	}
	// Names outside the catalog: both sides must say false.
	universe = append(universe, "unknown", "not_an_extension", "")

	for _, name := range universe {
		if got, want := isCrossEngineTranslatablePGExtension(name), translated[name]; got != want {
			t.Errorf("isCrossEngineTranslatablePGExtension(%q) = %v; postgres catalog says %v — the pipeline mirror drifted from crossEngineDefaultTranslatedExtensions (keep them in lock-step)",
				name, got, want)
		}
	}
}
