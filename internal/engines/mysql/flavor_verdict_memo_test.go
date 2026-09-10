// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"testing"
)

// A PROBE THAT COULD NOT RUN IS NOT A VERDICT, ON EITHER ARM.
//
// `flavor_memo.go` states that invariant in those words and the memo
// depends on it: [Engine.checkServerFlavor] caches whatever
// [Engine.flavorVerdict] reports as REACHED, keyed per (server, flavor),
// and every later door then short-circuits on the cache without probing.
// Cache a nil verdict that came from a failed probe and the refusal is
// disabled for the life of the process.
//
// # Why this test exists
//
// The pre-tag value-fidelity review found the non-MariaDB arm returning
// `true, nil` unconditionally, so a `SELECT VERSION()` that errored WAS
// cached as "no refusal". That arm carries
// refuseVitessUnderNonVStreamFlavor, which is a silent-loss guard: the
// vanilla flavor's full scans run without `set workload=olap` and Vitess
// truncates them at its OLTP row cap, so the run completes short at exit
// 0. A `migrate` against a self-hosted vtgate that met one transient at
// phase 1.75 would lose the guard for the whole run.
//
// It was a regression introduced by the memo itself — before it, each
// door probed independently and a transient at one door was recovered by
// the next — and the memo's own doc-comment asserted the property it had
// just broken. That is the 2026-07-28 shape: an invariant is a hypothesis
// until a test fails when it breaks.
func TestFlavorVerdict_AFailedProbeIsNeverAVerdict(t *testing.T) {
	probeErr := errors.New("read tcp 10.0.0.1:3306: connection reset by peer")

	for _, tc := range []struct {
		name   string
		flavor Flavor
	}{
		// Both arms, deliberately. The MariaDB arm always had this
		// right; grading only the broken one would let a future edit
		// swap which arm is wrong without failing anything.
		{"vanilla arm — carries the Vitess silent-loss refusal", FlavorVanilla},
		{"mariadb arm", FlavorMariaDB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := Engine{Flavor: tc.flavor}
			reached, _ := e.flavorVerdict("", probeErr)
			if reached {
				t.Errorf("flavorVerdict reported a REACHED verdict for a probe that returned %v.\n\n"+
					"checkServerFlavor caches every reached verdict per (server, flavor), and every later "+
					"door short-circuits on that cache without probing. Caching a verdict derived from a "+
					"failed probe disables refuseVitessUnderNonVStreamFlavor — a silent-loss guard — for "+
					"the life of the process, which is what one transient at one door would then cost. "+
					"flavor_memo.go says a probe that could not RUN is never cached; make that true.",
					probeErr)
			}
		})
	}
}

// TestFlavorVerdict_ASuccessfulProbeIsAVerdict is the anti-vacuity half.
// Without it, `return false, nil` on every path satisfies the test above
// and disables the memo entirely — every door would re-probe, which is
// correct but is not what the memo is for, and nothing would say so.
func TestFlavorVerdict_ASuccessfulProbeIsAVerdict(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flavor  Flavor
		version string
		wantErr bool
	}{
		{"vanilla against an ordinary MySQL", FlavorVanilla, "8.0.46", false},
		{"mariadb driver against a real MariaDB", FlavorMariaDB, "11.4.13-MariaDB", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := Engine{Flavor: tc.flavor}
			reached, verdict := e.flavorVerdict(tc.version, nil)
			if !reached {
				t.Fatalf("a SUCCESSFUL probe of %q reported no verdict; the memo then caches nothing and "+
					"every door re-probes, which defeats the memo this test's sibling protects",
					tc.version)
			}
			if gotErr := verdict != nil; gotErr != tc.wantErr {
				t.Errorf("flavorVerdict(%q) verdict = %v; wantErr %v", tc.version, verdict, tc.wantErr)
			}
		})
	}
}
