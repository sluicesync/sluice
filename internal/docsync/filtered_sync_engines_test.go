// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The filtered-continuous-sync engine list, kept honest against the code.
//
// # Why this exists
//
// Three consecutive releases shipped a wrong claim about WHICH ENGINES a
// filtered-sync behaviour applied to. The worst of them told SQLite and
// trigger-CDC operators their targets "have been diverging silently ever
// since" — for a mode those engines refuse at preflight and can never have
// entered. Sending someone to audit for a problem they cannot have had is its
// own kind of harm, and it is a harm no amount of care reliably prevents,
// because the mistake is always the same shape: reasoning about which engines
// LACK a capability instead of checking which ones HAVE it.
//
// There is a mechanical source of truth for that question. A filtered
// continuous sync needs the source's CDC reader to deliver full before-images,
// declared by a compile-time pin — `var _ ir.FullBeforeImageSetter = …` — in
// the implementing engine's capabilities_assert.go. This test derives the
// engine set from those pins and holds the operator doc to it.
//
// # The marker
//
// Prose should stay prose, so the doc carries a machine-checkable marker that
// this test owns:
//
//	<!-- filtered-sync-engines: mariadb, mysql, planetscale, postgres, vitess -->
//
// The visible sentence next to it is for humans; the marker is what fails.
// Anyone writing release notes about filtered sync now has one place to look
// whose answer is derived rather than remembered.
//
// # The marker lists ENGINE NAMES, and for two years it could not
//
// This gate shipped deriving PACKAGE DIRECTORY names, which for a
// multi-flavor package is not the set an operator can type. It could only
// ever emit `mysql, postgres`, so the marker was correct by construction and
// the prose beside it was bound to nothing — and the whole time MariaDB
// could run a filtered continuous sync (its binlog `*CDCReader` carries the
// pin) and appeared in no engine list sluice published. The derivation now
// goes through the registry; see engine_roster_test.go for why the two
// questions need two helpers, and for the flavor-uniformity precondition
// this capability satisfies (every mysql flavor reaches a pinned reader:
// binlog `*CDCReader` for mysql/mariadb, `*vstreamCDCReader` +
// `*vstreamSnapshotChanges` for planetscale/vitess).
func TestFilteredSyncEngineListMatchesTheCode(t *testing.T) {
	const capability = "FullBeforeImageSetter"

	fromCode := registeredEnginesImplementing(t, capability)
	if len(fromCode) == 0 {
		t.Fatalf("no engine package declares a `var _ ir.%s` pin; either the capability was renamed or the "+
			"scan broke — this gate cannot be allowed to pass on an empty set", capability)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "filtered-subset-migration.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*filtered-sync-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/filtered-subset-migration.md carries no `<!-- filtered-sync-engines: … -->` "+
			"marker. It is the checkable form of a claim this project has gotten wrong three releases running; "+
			"add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's filtered-sync engine list disagrees with the code.\n"+
			"  code (engines pinning ir.%s): %s\n"+
			"  doc  (marker):                %s\n\n"+
			"A filtered continuous sync requires the source CDC reader to deliver full before-images; an engine "+
			"without that pin refuses at preflight and CANNOT run one. Update the marker AND the prose beside "+
			"it — and if you are about to describe this in release notes, use this list rather than reasoning "+
			"about which engines lack the capability.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
