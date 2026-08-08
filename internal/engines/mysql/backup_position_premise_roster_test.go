// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The flavor roster behind [binlogEnabledQuery]'s premise.
//
// The premise — "an ordinary connection can read `@@GLOBAL.log_bin`" —
// was written down for two flavors and applied to four. `mysql` and
// `mariadb` were ground-truthed against real servers; `planetscale` and
// `vitess` terminate at vtgate, which serves its own partial
// system-variable surface, and nothing had measured them. That gap is
// what this file exists to make un-repeatable: a new flavor cannot land
// without saying, in code, how the premise was measured for it and which
// test re-measures it.
//
// SCOPE, stated so the name cannot be read as broader than the truth:
// this gate checks that an ENTRY EXISTS for every registered flavor and
// that the test it cites is a real test function in this package. It does
// NOT check that the entry is true — the truth is the cited tests, which
// need real servers and carry build tags. What it closes is the two ways
// this kind of citation rots: a flavor added with no evidence at all, and
// evidence citing a test that has been renamed or deleted.

package mysql

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// binlogPremiseEvidence records, per flavor, HOW "this server serves
// `@@GLOBAL.log_bin` to an ordinary connection" was measured, and the
// test that re-measures it. Prose lives on [binlogEnabledQuery]; this is
// the machine-checkable half.
var binlogPremiseEvidence = map[Flavor]struct {
	measured string
	pin      string
}{
	FlavorVanilla: {
		measured: "MySQL 8.0.46 booted --skip-log-bin reads 0 (2026-08-07); binlog-ON control reads 1",
		pin:      "TestCaptureBackupPosition_NoBinlog",
	},
	FlavorMariaDB: {
		measured: "MariaDB 11.4 booted --skip-log-bin reads 0 (2026-08-07); binlog-ON control reads 1",
		pin:      "TestCaptureBackupPosition_NoBinlog",
	},
	FlavorPlanetScale: {
		measured: "vtgate ROUTES the read to a tablet and answers 1 — measured 2026-08-07 on a real Vitess 24.0.1 cluster",
		pin:      "TestVStream_BackupPositionProbesAnswerThroughVtgate",
	},
	FlavorVitess: {
		measured: "same vtgate surface as planetscale (ADR-0073(a) shares the engine code verbatim); measured on the same cluster",
		pin:      "TestVitessCluster_BackupPositionProbesAnswerThroughVtgate",
	},
}

// TestBinlogEnabledPremise_FlavorRosterIsComplete fails when a registered
// flavor has no recorded evidence for the [binlogEnabledQuery] premise,
// when the roster carries an entry for a flavor that no longer exists, or
// when the cited pin is not a test function in this package.
func TestBinlogEnabledPremise_FlavorRosterIsComplete(t *testing.T) {
	roster := registeredFlavors()

	// Anti-vacuity floor: the four flavors that exist today must all be
	// discovered, or the discovery below is what broke — not the roster.
	wantNames := map[string]bool{"mysql": false, "planetscale": false, "vitess": false, "mariadb": false}
	for _, f := range roster {
		if _, ok := wantNames[f.String()]; ok {
			wantNames[f.String()] = true
		}
	}
	for name, found := range wantNames {
		if !found {
			t.Fatalf("flavor discovery did not find %q — registeredFlavors() is broken, so every "+
				"assertion below would pass vacuously", name)
		}
	}

	for _, f := range roster {
		ev, ok := binlogPremiseEvidence[f]
		if !ok {
			t.Errorf("flavor %q has no binlogPremiseEvidence entry: binlogEnabled() runs on EVERY "+
				"MySQL-family flavor and its failure fails the backup, so a new flavor owes a "+
				"measurement of whether that server serves @@GLOBAL.log_bin at all — record it "+
				"here and on the binlogEnabledQuery doc", f)
			continue
		}
		if strings.TrimSpace(ev.measured) == "" || strings.TrimSpace(ev.pin) == "" {
			t.Errorf("flavor %q: binlogPremiseEvidence entry is incomplete (measured=%q pin=%q)",
				f, ev.measured, ev.pin)
		}
	}

	// Symmetric: a stale entry is residue from a removed flavor and would
	// make the roster read as broader coverage than it has.
	inRoster := make(map[Flavor]bool, len(roster))
	for _, f := range roster {
		inRoster[f] = true
	}
	for f := range binlogPremiseEvidence {
		if !inRoster[f] {
			t.Errorf("binlogPremiseEvidence carries flavor %d, which is not registered — drop the "+
				"stale entry", f)
		}
	}

	// A flavor can only hide from the String()-based scan by skipping step
	// 2 of the "Adding a new flavor" checklist while doing step 3. Cross-
	// check against the capabilities map so that combination is caught too.
	for f := range flavorCapabilities {
		if !inRoster[f] {
			t.Errorf("flavor %d has a flavorCapabilities entry but no Flavor.String() case, so the "+
				"premise roster cannot see it", f)
		}
	}

	// The citations must point at tests that exist. A pin naming a
	// renamed or deleted test is the "wearing a citation to a test that
	// asserts something else" defect, one step earlier.
	funcs := testFuncNamesInPackage(t)
	for f, ev := range binlogPremiseEvidence {
		if !funcs[ev.pin] {
			t.Errorf("flavor %q cites pin %q, which is not a test function in this package — the "+
				"citation rotted (rename/removal); repoint it at the test that actually re-measures "+
				"the premise", f, ev.pin)
		}
	}
}

// registeredFlavors derives the complete set of registered [Flavor]
// values from [Flavor.String], whose default case is the "not a
// registered flavor" marker. The whole uint8 range is scanned rather than
// stopping at the first gap, so a flavor added out of iota order is still
// found.
func registeredFlavors() []Flavor {
	var out []Flavor
	for i := 0; i <= 255; i++ {
		f := Flavor(i)
		if strings.HasPrefix(f.String(), "flavor(") {
			continue
		}
		out = append(out, f)
	}
	return out
}

// testFuncNamesInPackage returns every `func TestX(t *testing.T)` name
// declared in this package's directory, INCLUDING build-tagged files —
// which is the point: the pins this roster cites are integration/vstream/
// vitesscluster-tagged, so a compile-time reference is impossible and a
// source scan is the only way to keep the citation honest.
func testFuncNamesInPackage(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	decl := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]*)\(`)
	names := make(map[string]bool)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		for _, m := range decl.FindAllStringSubmatch(string(src), -1) {
			names[m[1]] = true
		}
	}
	if len(names) == 0 {
		t.Fatal("no test functions discovered in the package directory — the scan is broken, so " +
			"every citation check below would pass vacuously")
	}
	return names
}
