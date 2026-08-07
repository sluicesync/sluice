// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The idempotent-copy engine list, held to the code (Bug 222).
//
// `--restart-from-scratch` and the automatic re-snapshot behave differently
// depending on whether the source's snapshot reader is IDEMPOTENT: a
// non-idempotent reader's target is cleared before the re-copy (its plain
// INSERT would dup-key otherwise), while an idempotent one's was left in
// place to be absorbed by the upsert. Which engines fall on which side is
// therefore an operator-visible fact, and it is decided by exactly one thing:
// who pins ir.IdempotentCopyReader.
//
// The v0.109.0 release notes said the target was left in place for
// "PlanetScale, Vitess and Postgres". Postgres was never on that path — the
// capability has a single implementor, the VStream snapshot reader — so the
// published prose told Postgres operators they had been silently accumulating
// deleted rows when they had not. The fix underneath was correct; the sentence
// describing who it applied to was not.
//
// That is the fifth time an engine-set claim has been the wrong part of an
// otherwise-correct release, which is what CLAUDE.md's rule is about: a claim
// that names an engine set belongs behind a marker, not in prose alone. Prose
// stays prose; the marker is what fails.
//
//	<!-- idempotent-copy-engine-packages: mysql -->
//
// # Why this one marker names PACKAGES when its siblings name engines
//
// This is the deliberate exception among the three capability markers in
// this package, and the marker key says so in its own name. The other two
// ([TestFilteredSyncEngineListMatchesTheCode],
// [TestIndexVerifyEngineListMatchesTheCode]) derive REGISTERED ENGINE NAMES,
// because their pins sit on types every flavor of the package reaches. This
// one cannot: the pin belongs to `*vstreamSnapshotRows`, reachable only from
// the PlanetScale and Vitess flavors. A native MySQL binlog snapshot is NOT
// idempotent and is on the cleared side, in the same package — so a
// package→names mapping would emit `mariadb, mysql, planetscale, vitess` and
// state the opposite of the truth for two of them.
//
// The key was `idempotent-copy-engines` until the 2026-08-06 sweep, which is
// a name that reads as an engine list — and dangerously so here, since
// `mysql` is BOTH this package's directory AND the name of the one flavor
// that LACKS the capability. Renamed so it cannot be read as broader, or
// other, than the truth. The marker answers "which package holds an
// idempotent reader"; the prose beside it must say which FLAVORS that reader
// serves — the one thing this gate cannot check for you.

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestIdempotentCopyEngineListMatchesTheCode(t *testing.T) {
	const capability = "IdempotentCopyReader"

	fromCode := enginePackagesImplementing(t, capability)
	if len(fromCode) == 0 {
		t.Fatalf("no engine package declares a `var _ ir.%s` pin; either the capability was renamed or the "+
			"scan broke — this gate must not pass on an empty set, because an empty set would agree with "+
			"any marker", capability)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*idempotent-copy-engine-packages:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/cdc-streaming.md carries no `<!-- idempotent-copy-engine-packages: … -->` "+
			"marker. It is the checkable form of the claim Bug 222 filed against the v0.109.0 notes; add it "+
			"listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's idempotent-copy engine-PACKAGE list disagrees with the code.\n"+
			"  code (packages pinning ir.%s): %s\n"+
			"  doc  (marker):                 %s\n\n"+
			"This list decides who is affected by the --restart-from-scratch clearing change and by the "+
			"automatic re-snapshot's merge residual. An engine NOT on this list already cleared its target "+
			"before v0.109.0 and is unchanged by it — saying otherwise tells those operators they lost data "+
			"when they did not. Update the marker AND the prose beside it, and use this list when writing "+
			"release notes rather than reasoning about which engines lack the capability.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
