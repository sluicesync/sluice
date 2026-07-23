// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Blank-imported for their init() self-registration, the same set
	// cmd/sluice/main.go links — this test's whole point is to iterate
	// the REAL registry, so a new engine package added there must be
	// added here too (forgetting it fails the count belt below, not
	// silently).
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// The CDC operator guide lagged the engine registry for three audits
// running: the mariadb flavor shipped first-class CDC in v0.99.268 and
// was still absent from docs/operator/cdc-streaming.md at the
// 2026-07-23 audit (DOC-6, 3rd carry). This is the ratchet: every
// registered engine that DECLARES a CDC capability must at least be
// named in the guide, so the next flavor can't ship undocumented.
//
// Deliberately a name-presence check, not content equality — the guide
// is prose, and the gate's job is "the operator can find their engine
// here", not to freeze wording.
func TestCDCGuideCoversEveryCDCCapableEngine(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md"))
	if err != nil {
		t.Fatalf("read CDC guide: %v", err)
	}
	guide := strings.ToLower(string(raw))

	var cdcCapable int
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Get(%q) missing an engine its own Names() listed", name)
		}
		if e.Capabilities().CDC == ir.CDCNone {
			continue
		}
		cdcCapable++
		if !strings.Contains(guide, strings.ToLower(name)) {
			t.Errorf("engine %q declares CDC (%s) but is never mentioned in docs/operator/cdc-streaming.md — add an operator-facing note for it (the DOC-6 class)", name, e.Capabilities().CDC)
		}
	}

	// Belt against a vacuous pass: if the blank-import set above rots
	// (a new engine package not linked here), the loop can green while
	// checking nothing new. 8 is today's CDC-capable roster (mysql,
	// planetscale, vitess, mariadb, postgres, postgres-trigger,
	// sqlite-trigger, d1-trigger); a shrink means a missing import, a
	// growth means update this count alongside the new guide section.
	const wantCDCCapable = 8
	if cdcCapable != wantCDCCapable {
		t.Fatalf("registry lists %d CDC-capable engines, want %d — if an engine package was added/removed, update the blank imports and this count", cdcCapable, wantCDCCapable)
	}
}
