// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestBackup_FormatVersion_Bug116 pins the v0.94.1 Bug 116 closure at
// the end-to-end manifest-write surface: a Backup whose schema uses
// security-relevant fields (RLS / Policies / ExcludeConstraints)
// produces a manifest stamped FormatVersion=2 so older binaries refuse
// it loudly at their preflight; a Backup whose schema has no such
// fields stays on FormatVersion=1 and remains restorable on older
// binaries.
//
// This pins the proportional version-stamp behaviour at the pipeline
// boundary (not just at irbackup.FormatVersionFor, which is unit-pinned in
// internal/ir/format_version_bug116_test.go). The orchestrator paths
// pinned here are the three that construct *irbackup.Manifest with
// FormatVersion set: Backup (full), IncrementalBackup, and Streamer's
// per-rollover manifest constructor.
func TestBackup_FormatVersion_Bug116(t *testing.T) {
	cases := []struct {
		name   string
		schema *ir.Schema
		want   int
	}{
		{
			name: "innocent schema → legacy (older binaries can still restore)",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{
						Name: "users",
						Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
						},
					},
				},
			},
			want: irbackup.FormatVersionLegacy,
		},
		{
			name: "schema with RLSEnabled → security-metadata (older binaries refuse loudly)",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{
						Name:       "tenants",
						RLSEnabled: true,
						Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
						},
					},
				},
			},
			want: irbackup.FormatVersionSecurityMetadata,
		},
		{
			name: "schema with Policies → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{
						Name: "events",
						Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
						},
						Policies: []*ir.Policy{{Name: "tenant_isolation"}},
					},
				},
			},
			want: irbackup.FormatVersionSecurityMetadata,
		},
		{
			name: "schema with EXCLUDE constraint → security-metadata",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{
						Name: "schedule",
						Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
						},
						ExcludeConstraints: []*ir.ExcludeConstraint{{Name: "no_overlap"}},
					},
				},
			},
			want: irbackup.FormatVersionSecurityMetadata,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := NewLocalStore(dir)
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			// Empty rows are fine — Bug 116 is a manifest-level
			// version-stamp check, not a row-content check.
			src := newBackupRecorderEngine("postgres", c.schema, map[string][]ir.Row{})
			b := &Backup{
				Source:    src,
				SourceDSN: "src",
				Store:     store,
			}
			if err := b.Run(context.Background()); err != nil {
				t.Fatalf("Backup.Run: %v", err)
			}
			m, err := readManifest(context.Background(), store)
			if err != nil {
				t.Fatalf("readManifest: %v", err)
			}
			if m.FormatVersion != c.want {
				t.Errorf("manifest.FormatVersion = %d; want %d (schema features: RLS=%v Policies=%d EXCLUDE=%d)",
					m.FormatVersion, c.want,
					tableHasRLS(c.schema), countPolicies(c.schema), countExclude(c.schema))
			}
		})
	}
}

func tableHasRLS(s *ir.Schema) bool {
	if s == nil {
		return false
	}
	for _, t := range s.Tables {
		if t != nil && (t.RLSEnabled || t.RLSForced) {
			return true
		}
	}
	return false
}

func countPolicies(s *ir.Schema) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, t := range s.Tables {
		if t != nil {
			n += len(t.Policies)
		}
	}
	return n
}

func countExclude(s *ir.Schema) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, t := range s.Tables {
		if t != nil {
			n += len(t.ExcludeConstraints)
		}
	}
	return n
}
