// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The import-boundary gate (audit 2026-07-26 ARCH-2, gate G-16).
//
// This package's doc declares it "imports NO engine package and NOT
// internal/ir". That claim was FALSE when the audit checked it: http.go
// imported internal/diagnose for one six-line helper, and diagnose's
// transitive closure includes internal/ir. The prose was the only thing
// holding the boundary, and it had already been breached — which is the
// general problem with an invariant that lives in a comment.
//
// The helper now lives in internal/safeerr (a genuine leaf), so the claim is
// true again. This test is what keeps it true: it walks the REAL transitive
// closure via `go list -deps`, so it sees an indirect violation — the shape
// that actually happened — rather than only a direct import.
package telemetrysink

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// forbidden are packages this one must not reach, directly or transitively,
// with the reason each boundary exists.
var forbidden = map[string]string{
	"sluicesync.dev/sluice/internal/ir":               "the sink takes a plain Record; the caller maps its telemetry view into it. Depending on the IR would couple a durable output format to the schema model and make every IR change a potential sink-format change",
	"sluicesync.dev/sluice/internal/engines/mysql":    "no engine package: the sink is engine-neutral by construction",
	"sluicesync.dev/sluice/internal/engines/postgres": "no engine package, as above",
	"sluicesync.dev/sluice/internal/pipeline":         "the pipeline is a CONSUMER of this package; depending back on it would be a cycle in intent even where the compiler allows it",
}

func TestTelemetrySinkStaysALeaf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-deps", "sluicesync.dev/sluice/internal/telemetrysink").Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}
	deps := strings.Fields(string(out))

	// Anti-vacuity: a `go list` that returns almost nothing (a toolchain
	// hiccup, a wrong package path) would make every assertion below pass.
	if len(deps) < 20 {
		t.Fatalf("go list -deps returned only %d packages; the query is not resolving this package's closure "+
			"and the gate is vacuous", len(deps))
	}

	depSet := make(map[string]bool, len(deps))
	for _, d := range deps {
		depSet[d] = true
	}

	for pkg, why := range forbidden {
		if depSet[pkg] {
			t.Errorf("internal/telemetrysink now depends on %s (directly or transitively), which its package doc "+
				"says it does not.\n  why the boundary exists: %s\n\n"+
				"Either drop the dependency — hoisting the needed helper into a leaf package, as was done for "+
				"safeerr.SafeParseError — or, if the dependency is genuinely warranted, change the package doc "+
				"in the same commit so the claim and the code agree. A stale boundary claim is worse than no "+
				"claim: it is the thing people trust instead of checking.", pkg, why)
		}
	}
}
