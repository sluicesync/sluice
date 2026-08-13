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
	"sluicesync.dev/sluice/internal/ir"
)

// The incremental-backup / broker engine list, kept honest against the
// registry (audit 2026-08-11 D-2).
//
// production-readiness.md claimed encrypted chains "(full + incremental)
// … and a continuous broker work on every engine that migrates". Only
// `backup full` is engine-neutral: IncrementalBackup.validate and
// StreamBackup.validate both refuse a CDC-less source (an incremental
// captures the changes SINCE the parent, and capturing changes is what
// CDC is), and six registered engines migrate with no CDC mechanism.
// The cookbook recipe states this correctly; the two docs disagreed.
//
//	<!-- incremental-backup-engines: … -->
//
// Derived from the same registry read as the cdc-modes marker in the
// same file: an engine is in this list iff its declared CDC mechanism
// is not "none".
func TestIncrementalBackupEngineListMatchesTheRegistry(t *testing.T) {
	var fromCode []string
	sawNone := false
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missing an engine its own Names() listed", name)
		}
		if e.Capabilities().CDC == ir.CDCNone {
			sawNone = true
			continue
		}
		fromCode = append(fromCode, name)
	}
	// Non-vacuity floors: a partial registry must not quietly agree with
	// a marker about a smaller world, and the CDC/CDC-less DISTINCTION
	// must be present — if no engine were CDC-less the list would equal
	// "every engine that migrates" and the gate would hold the doc to
	// the very claim it exists to refute.
	if len(fromCode) < 3 {
		t.Fatalf("only %d engines declare a CDC mechanism (%s) — a blank import is missing or the registry "+
			"did not populate", len(fromCode), strings.Join(fromCode, ", "))
	}
	if !sawNone {
		t.Fatal("every registered engine declares a CDC mechanism — the CDC-less class this gate distinguishes " +
			"is empty, so either the registry is partial or the incremental/broker validate gates lost their reason to exist; " +
			"re-derive before trusting this marker")
	}

	docPath := filepath.Join("..", "..", "docs", "production-readiness.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*incremental-backup-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/production-readiness.md carries no `<!-- incremental-backup-engines: … -->` marker; "+
			"add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the incremental-backup/broker engine list disagrees with the registry.\n"+
			"  code (CDC mechanism declared): %s\n"+
			"  doc  (marker):                 %s\n\n"+
			"`backup incremental` and the broker refuse a CDC-less source at validate; update the marker AND "+
			"the prose beside it.",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
