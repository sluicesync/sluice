// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_LineageDispatchRoster is this package's instantiation
// of the Bug 233 gate (audit A-3): every column-type dispatch either reads the
// STORAGE type through ir.UnwrapDomain or carries a written, code-verified
// reason.
//
// The package's one dispatch site — VerbatimExtensionColumnsIn's verbatim-column
// detection — was a REAL Bug 233 instance: a DOMAIN-over-verbatim (extension)
// column matched no arm, so the lineage catalog never recorded the
// verbatim-extension marker and the restore-time refuseVerbatim* gate could not
// fire for it. It now reads the STORAGE type (the fix), so the roster is empty;
// the correctness proof is TestPin_DomainOverVerbatim_MarkerRecorded in
// domain_gate_pin_test.go.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_LineageDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "pipeline/lineage",
		// 1 dispatch site, now reading storage via UnwrapDomain (still counts
		// toward the floor); this holds the shape against a refactor.
		MinSites: 1,
		Allowed:  map[string]string{},
	})
}
