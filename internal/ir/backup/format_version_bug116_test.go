// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChooseFormatVersion_Bug116 pins the v0.94.1 Bug 116 closure:
// manifests whose schema carries security-relevant fields older
// binaries would silently drop (RLS, RLSForced, Policies, EXCLUDE
// constraints) are stamped FormatVersion=2; manifests whose schema
// has none of those fields stay on FormatVersion=1 for backward
// compatibility with v0.94.0-and-earlier restores.
//
// The matrix is the bug-116 closure surface — each "ON" trigger
// independently flips the version, and no other field does.
func TestChooseFormatVersion_Bug116(t *testing.T) {
	cases := []struct {
		name   string
		schema *ir.Schema
		want   int
	}{
		{
			name:   "nil schema → legacy (degenerate stays compatible)",
			schema: nil,
			want:   FormatVersionLegacy,
		},
		{
			name:   "empty schema → legacy",
			schema: &ir.Schema{},
			want:   FormatVersionLegacy,
		},
		{
			name: "innocent table (no security fields) → legacy",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
				},
			},
			want: FormatVersionLegacy,
		},
		{
			name: "table with RLSEnabled → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "tenants", RLSEnabled: true},
				},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "table with RLSForced (but not Enabled) → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "audit", RLSForced: true},
				},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "table with Policies → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "events", Policies: []*ir.Policy{{Name: "tenant_isolation"}}},
				},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "table with ExcludeConstraints → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "schedule", ExcludeConstraints: []*ir.ExcludeConstraint{{Name: "no_overlap"}}},
				},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "multi-table: one innocent, one with RLS → security-metadata (first hit wins)",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "users"},
					{Name: "tenants", RLSEnabled: true},
				},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "nil-element-tolerance: nil *Table in slice is skipped",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "users"},
					nil,
				},
			},
			want: FormatVersionLegacy,
		},
		{
			name: "security-metadata is the FINALIZED-manifest ceiling (FormatVersionProgressSidecar is in-progress-only and never chosen here)",
			schema: &ir.Schema{
				Tables: []*ir.Table{{Name: "x", RLSEnabled: true}},
			},
			want: FormatVersionSecurityMetadata,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := chooseFormatVersion(c.schema)
			if got != c.want {
				t.Errorf("chooseFormatVersion = %d; want %d", got, c.want)
			}
			// FormatVersionFor is the exported wrapper; must agree.
			if exported := FormatVersionFor(c.schema); exported != got {
				t.Errorf("FormatVersionFor disagrees with chooseFormatVersion: %d vs %d", exported, got)
			}
		})
	}
}

// TestBackupFormatVersion_Bumped pins the version ladder: the build
// ceiling is the ADR-0086 in-progress sidecar version, the Bug 116
// security-metadata version stays the FINALIZED ceiling, and the
// legacy value is frozen. If a future change reorders these without
// updating the chooseFormatVersion / sidecar contracts, this test
// catches the regression at build time.
func TestBackupFormatVersion_Bumped(t *testing.T) {
	if BackupFormatVersion != FormatVersionProgressSidecar {
		t.Errorf("BackupFormatVersion = %d; want FormatVersionProgressSidecar=%d (ADR-0086 ceiling)",
			BackupFormatVersion, FormatVersionProgressSidecar)
	}
	if FormatVersionLegacy != 1 {
		t.Errorf("FormatVersionLegacy = %d; must stay 1 (load-bearing for older-binary preflight semantics)", FormatVersionLegacy)
	}
	if FormatVersionSecurityMetadata <= FormatVersionLegacy {
		t.Errorf("FormatVersionSecurityMetadata (%d) must be strictly greater than FormatVersionLegacy (%d)",
			FormatVersionSecurityMetadata, FormatVersionLegacy)
	}
	if FormatVersionProgressSidecar <= FormatVersionSecurityMetadata {
		t.Errorf("FormatVersionProgressSidecar (%d) must be strictly greater than FormatVersionSecurityMetadata (%d) — older binaries refuse the in-progress sidecar layout via the version gate",
			FormatVersionProgressSidecar, FormatVersionSecurityMetadata)
	}
}
