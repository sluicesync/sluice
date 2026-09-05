// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// orderabilityVerdict records, per engine package, the two facts that decide
// whether laneapply's marker-less checkpoint path could assert token
// monotonicity: whether the engine emits transaction markers, and whether it
// can order two positions.
type orderabilityVerdict struct {
	emitsTxMarkers  bool // emits ir.TxCommit → takes laneapply's MARKER path
	positionOrderer bool // implements ir.PositionOrderer
	why             string
}

// TestPositionOrdererRosterMatchesTheMarkerlessPath pins the premise behind a
// deliberate NON-decision in internal/laneapply.
//
// THE CLAIM IT GUARDS. `Orchestrator.writeCheckpoint` is monotone in the
// orchestrator's own arrival counter, not in the position TOKEN, so a reader
// delivering changes out of order persists a token lower than one already
// applied. That is Bug 268 exactly (`{"last_id":9}` written after ids up to 42
// had been applied). The obvious guard — assert non-decreasing tokens at
// noteBoundary — was costed and deliberately NOT built, because the engines
// that REACH noteBoundary cannot order their positions, and the engines that
// can, do not reach it. A gate built on ir.PositionOrderer would have missed
// the two engines that produced the defect while reading as though it covered
// the marker-less path.
//
// That reasoning is a hypothesis about the engine roster, and this repo's rule
// is that an invariant nobody checks is indistinguishable from one that holds.
// So: if the roster ever changes — a trigger engine gains an orderer, or a new
// marker-less engine arrives — this fails, and the laneapply decision gets
// revisited rather than silently inherited.
//
// WHAT IT REACHES: the engine packages under internal/engines, graded by
// source inspection (this package cannot import them — they import it, and
// register via init()). It grades DECLARATION, not behaviour: that a package
// declares PositionAtOrAfter, not that the implementation is correct.
func TestPositionOrdererRosterMatchesTheMarkerlessPath(t *testing.T) {
	// Fail-by-default divergence map. Every engine package must appear, and
	// its two facts must match. A new package is a failure until classified.
	expected := map[string]orderabilityVerdict{
		"mysql": {
			emitsTxMarkers: true, positionOrderer: true,
			why: "BOTH modes in one package: binlog emits Tx markers and takes laneapply's marker path, " +
				"while VStream is marker-less and DOES reach noteBoundary. It is the only engine that is " +
				"both marker-less and orderable, which is why a PositionOrderer-based assertion would " +
				"have covered VStream alone",
		},
		"postgres": {
			emitsTxMarkers: true, positionOrderer: true,
			why: "pgoutput emits Tx markers, so noteBoundary is never reached; the orderer exists for the " +
				"ADR-0049 schema-history resolution, not for checkpointing",
		},
		"pgtrigger": {
			emitsTxMarkers: false, positionOrderer: false,
			why: "marker-less change-log source, no orderer — reaches noteBoundary and cannot be asserted " +
				"generically. Its poll is nonetheless SAFE by inspection: pollQuery selects `id` unaliased " +
				"in both the CTE and the outer query, so nothing shadows the bigint, and its watermark is " +
				"the end of the contiguous committed run rather than the max id fetched",
		},
		"sqlite-trigger": {
			emitsTxMarkers: false, positionOrderer: false,
			why: "marker-less change-log source, no orderer — and the engine that produced Bug 266/268. " +
				"Its ordering is guarded READER-side by the CHANGE-LOG-PAGE-UNORDERED refusal, which is " +
				"the only layer that knows the token means an integer",
		},
		"d1-trigger": {
			emitsTxMarkers: false, positionOrderer: false,
			why: "registration shim over the sqlite-trigger engine; same token shape, same reader-side guard",
		},
		"sqlite": {
			emitsTxMarkers: false, positionOrderer: false,
			why: "bulk-copy engine with no CDC reader of its own, so it reaches neither path",
		},
		"flatfile": {why: "export target only; no CDC"},
		"mydumper": {why: "dump reader only; no CDC"},
		"internal": {why: "shared engine-side helpers, not an engine"},
		"testdata": {why: "fixtures"},
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read engines dir: %v", err)
	}

	var pkgs []string
	for _, e := range entries {
		if e.IsDir() {
			pkgs = append(pkgs, e.Name())
		}
	}
	sort.Strings(pkgs)

	// Anti-vacuity: the walk must actually find the engine packages. Six is
	// under the real count and bites only on a broken scan.
	if len(pkgs) < 6 {
		t.Fatalf("found only %d engine package(s) %v; the roster derives its universe from the "+
			"directory listing, so a broken walk makes every assertion below vacuous", len(pkgs), pkgs)
	}

	for _, pkg := range pkgs {
		want, ok := expected[pkg]
		if !ok {
			t.Errorf("engine package %q is not classified in this roster.\n\n"+
				"Record whether it emits ir.TxCommit (→ laneapply's marker path) and whether it "+
				"implements ir.PositionOrderer. If it is marker-less AND orderable, the laneapply "+
				"token-monotonicity assertion that was deliberately skipped becomes buildable with "+
				"real coverage — revisit the decision recorded at noteBoundary.", pkg)
			continue
		}
		gotMarkers := pkgDeclares(t, pkg, "ir.TxCommit{")
		// Receiver-AGNOSTIC on purpose. The first cut of this gate matched
		// `func (e Engine) PositionAtOrAfter` and reported postgres as having
		// no orderer, because postgres spells it `func (Engine)` with an
		// unnamed receiver. The gate caught its own false premise on the
		// first run, which is the cheapest possible time; a needle keyed on a
		// receiver NAME under-reports silently.
		gotOrderer := pkgDeclares(t, pkg, ") PositionAtOrAfter(")

		if gotMarkers != want.emitsTxMarkers {
			t.Errorf("engine %q: emits Tx markers = %v, roster says %v.\n\nWhy this matters: %s",
				pkg, gotMarkers, want.emitsTxMarkers, want.why)
		}
		if gotOrderer != want.positionOrderer {
			t.Errorf("engine %q: implements ir.PositionOrderer = %v, roster says %v.\n\n"+
				"If a MARKER-LESS engine just gained an orderer, that is the trigger to revisit "+
				"laneapply's skipped token-monotonicity assertion — it now has coverage it lacked. "+
				"Recorded reason: %s", pkg, gotOrderer, want.positionOrderer, want.why)
		}
	}
}

// pkgDeclares reports whether any non-test .go file in the engine package
// contains needle. Source inspection rather than reflection: this package is
// imported BY the engine packages (they self-register in init()), so it cannot
// import them back.
func pkgDeclares(t *testing.T, pkg, needle string) bool {
	t.Helper()
	found := false
	err := filepath.Walk(pkg, func(path string, info os.FileInfo, werr error) error {
		if werr != nil || info.IsDir() || found {
			return werr
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(b), needle) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", pkg, err)
	}
	return found
}
