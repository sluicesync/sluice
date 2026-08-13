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

// The `migrate --where` engine list, kept honest against the code (audit
// 2026-08-11 D-1).
//
// The operator doc claimed the one-shot `migrate --where` "works on every
// engine that supports `migrate`" — and `migcore.ApplyRowFilters` refuses
// any reader that does not implement [ir.RowFilterSetter], which the
// SQLite/D1, flat-file, and mydumper readers do not: six engines that
// migrate and refuse `--where`, told they wouldn't. Same claim-surface
// shape as the filtered-sync sentence three releases running, same cure:
// derive the engine set from the compile-time pins and hold the doc's
// marker to it.
//
//	<!-- migrate-where-engines: … -->
//
// The `postgres-trigger` entry is the one that needs its own sentence:
// its bulk-copy reader DELEGATES to the postgres engine, so the pin the
// derivation finds lives in the pgtrigger package asserting the delegated
// type (see its capabilities_assert.go), and the runtime truth behind it
// is pinned behaviourally by TestPostgresTriggerRowReaderHonorsWhereFilters
// (internal/pipeline).
func TestMigrateWhereEngineListMatchesTheCode(t *testing.T) {
	const capability = "RowFilterSetter"

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

	marker := regexp.MustCompile(`<!--\s*migrate-where-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/filtered-subset-migration.md carries no `<!-- migrate-where-engines: … -->` "+
			"marker; add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's migrate --where engine list disagrees with the code.\n"+
			"  code (engines pinning ir.%s): %s\n"+
			"  doc  (marker):                %s\n\n"+
			"migcore.ApplyRowFilters refuses any reader without the setter, so an engine outside the pin set "+
			"CANNOT run `migrate --where`. Update the marker AND the prose beside it.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
