// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// The CDC-APPLY target engine list, kept honest against the code — the
// G-10 residual [TestTargetEngineListMatchesTheCode] names at its own
// definition.
//
// # Why this exists
//
// That gate grades the two cold-copy writer doors (`OpenSchemaWriter`,
// `OpenRowWriter`) and deliberately not `OpenChangeApplier`, because CDC
// apply is a genuinely different question: `sqlite` is a migrate target
// and refuses the change stream. Its scope note said, in so many words,
// that production-readiness.md's neighbouring "CDC apply targets" sentence
// was therefore prose bound to nothing. This gate is that binding.
//
// # What the marker holds
//
// The engine set an operator can point a continuous `sync` (or a
// rotated-chain restore — [backup.ChainRestore.Run] opens the same door)
// at as a target. It is derived exactly as the sibling gates derive it:
// twice, from evidence the two halves do not share —
//
//   - STRUCTURE: [cdcApplyEnginesByStructure], the AST pass asking whether
//     each engine type's `OpenChangeApplier` body is a bare
//     `return nil, <sentinel>` refusal stub.
//   - BEHAVIOUR: [cdcApplyEnginesByBehaviour], calling the door with an
//     empty DSN and classifying the error an operator would actually see.
//
// Both helpers live in chain_restore_idempotent_writer_test.go, whose gate
// asks a different question of the same set (are these engines idempotent
// writers); this one holds the OPERATOR DOC to it. The derivation is run
// and floor-checked here independently, so neither gate's verdict rides
// the other's.
//
// # Which documents it reaches
//
// One: `docs/production-readiness.md`, the only home of the claim (the
// README and the CDC guide name apply behaviour but not the target SET —
// re-grep before assuming that held; if the claim grows a second home,
// widen this to (file, marker) pairs the way the psdb gate did).
func TestCDCApplyTargetListMatchesTheCode(t *testing.T) {
	fromStructure := cdcApplyEnginesByStructure(t)
	fromBehaviour := cdcApplyEnginesByBehaviour(t)
	if !equalStringSets(fromStructure, fromBehaviour) {
		t.Fatalf("the two derivations of the CDC-apply target set disagree — resolve that before trusting "+
			"either.\n  structure (AST: OpenChangeApplier is not a bare `return nil, <sentinel>`): %s\n"+
			"  behaviour (the door called with an empty DSN did not answer \"not implemented\"): %s",
			strings.Join(fromStructure, ", "), strings.Join(fromBehaviour, ", "))
	}
	fromCode := fromStructure

	// Anti-vacuity floors. An empty set, or a set that swallowed every
	// registered engine, would agree with a doc marker saying anything.
	if len(fromCode) == 0 {
		t.Fatalf("no registered engine classifies as a CDC-apply target — the derivation broke; this gate "+
			"cannot be allowed to pass on an empty set (%d engines registered)", len(engines.Names()))
	}
	if len(fromCode) == len(engines.Names()) {
		t.Fatalf("every one of the %d registered engines classifies as a CDC-apply target, so the derivation "+
			"separates nothing. sluice ships engines that refuse OpenChangeApplier (sqlite, d1, flatfile, "+
			"mydumper); if that genuinely changed, delete this gate — do not delete this check",
			len(engines.Names()))
	}
	// The split this marker exists to state: an engine can be a migrate
	// target and NOT a CDC-apply target. If sqlite ever classifies as one,
	// the doc's "sqlite is a migrate-only target" sentence — and this
	// gate's reason to exist separately from the target-engines gate —
	// both need rewriting in the same commit.
	for _, name := range fromCode {
		if name == "sqlite" {
			t.Fatalf("%q classified as a CDC-apply target. It is the engine whose migrate-target/CDC-apply "+
				"split is the reason this claim needs its own marker; if a sqlite change applier has "+
				"genuinely shipped, update docs/production-readiness.md's migrate-only sentence IN THE SAME "+
				"COMMIT as this check's removal", name)
		}
	}

	docPath := filepath.Join("..", "..", "docs", "production-readiness.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*cdc-apply-targets:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/production-readiness.md carries no `<!-- cdc-apply-targets: … -->` marker. Its "+
			"\"CDC apply targets\" sentence was prose bound to nothing (the G-10 residual); add the marker "+
			"beside it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's CDC-apply target list disagrees with the code.\n"+
			"  code (engines implementing OpenChangeApplier): %s\n"+
			"  doc  (marker):                                 %s\n\n"+
			"An engine absent from this list refuses OpenChangeApplier and cannot take a CDC stream — not in "+
			"the docs, not in release notes. Update the marker AND the prose beside it, and take any \"which "+
			"apply targets does this affect\" sentence from this list rather than from the shape of the fix.",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
