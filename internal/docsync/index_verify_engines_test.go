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

// The post-index-build verification engine list, kept honest against the
// code.
//
// # Why this exists
//
// `ir.IndexVerifier`'s own doc said the net "runs for ALL targets that
// implement it" — true, and it read as a statement of coverage while only
// ONE engine implemented it. The gap sat there for releases on the engine
// that needed it most: Postgres emits `CREATE INDEX IF NOT EXISTS`, so a
// build that does not happen there is a SILENT no-op rather than an error,
// and there was nothing behind it. The error-codes row for
// `SLUICE-E-INDEX-MISSING` now names which targets the check runs on, and
// a named engine set in operator-facing prose is precisely the claim shape
// this package exists to hold to the code (see
// [TestFilteredSyncEngineListMatchesTheCode] for the same gate over
// filtered sync).
//
// # The marker
//
// Prose stays prose; the doc carries a machine-checkable marker this test
// owns, on its own line BELOW the error-code table:
//
//	<!-- index-verify-engines: mariadb, mysql, planetscale, postgres, vitess -->
//
// The source of truth is the compile-time pin — `var _ ir.IndexVerifier =
// …` — in each engine's capabilities_assert.go. An engine without that pin
// is skipped by the orchestrator's type-assert and genuinely has no net.
//
// The marker lists ENGINE NAMES, not package directories, because "which
// targets get the net" is answered by what an operator passes to
// `--target-driver`. This is the name-level derivation, and the capability
// meets its precondition: the pin is on each engine's `*SchemaWriter`, and
// `OpenSchemaWriter` returns that one type for every flavor of its package
// (mysql's carries the flavor as a field). See engine_roster_test.go.
//
// # The marker used to be flavor-blind
//
// It derived PACKAGE names, so it could only ever emit `mysql, postgres` —
// correct by construction, and silently dropping mariadb / planetscale /
// vitess from a claim about which targets are protected.
func TestIndexVerifyEngineListMatchesTheCode(t *testing.T) {
	const capability = "IndexVerifier"

	fromCode := registeredEnginesImplementing(t, capability)
	if len(fromCode) == 0 {
		t.Fatalf("no engine package declares a `var _ ir.%s` pin; either the capability was renamed or the "+
			"scan broke — this gate cannot be allowed to pass on an empty set", capability)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "error-codes.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*index-verify-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/error-codes.md carries no `<!-- index-verify-engines: … -->` marker in the "+
			"SLUICE-E-INDEX-MISSING row. It is the checkable form of a claim about which targets get the "+
			"silent-index-no-op safety net; add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's index-verification engine list disagrees with the code.\n"+
			"  code (engines pinning ir.%s): %s\n"+
			"  doc  (marker):                %s\n\n"+
			"The orchestrator dispatches this net by type-assertion, so an engine without the pin silently "+
			"gets NO post-index-phase verification. Update the marker AND the SLUICE-E-INDEX-MISSING row's "+
			"prose — and note that the row's cells are mirrored byte-equal in internal/sluicecode/docrows.go "+
			"(TestRegistryDocSync_ContentEquality), so a prose edit moves both homes. The marker itself sits "+
			"OUTSIDE the table, so it is not part of that mirror.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}
