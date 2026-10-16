// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestReadManifest_RefusesAnUnstampedHiddenCollation is the reverse door of
// FormatVersionHiddenCollation (sixth review F3/F4): a manifest recorded
// below 15 that carries a functional key's hidden-column collation was not
// written by any binary, so both manifest reads refuse it rather than let
// the edit decide how a restored key compares — including an in-progress
// manifest at the sidecar tier, whose replay normalization would otherwise
// re-derive version 15 from the injected schema and wave it through. The
// same manifest at 15, and one at 14 carrying nothing, read.
func TestReadManifest_RefusesAnUnstampedHiddenCollation(t *testing.T) {
	ctx := context.Background()
	schema := func(hidden string) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name:       "kd",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{{Name: "fk", Unique: true, Columns: []ir.IndexColumn{
				{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden},
			}}},
		}}}
	}
	for _, tc := range []struct {
		name    string
		m       *irbackup.Manifest
		refuses bool
	}{
		{"a v4-signed-era full (format 6) with the field injected", &irbackup.Manifest{Kind: irbackup.BackupKindFull, FormatVersion: irbackup.FormatVersionSignedManifest, Schema: schema("utf8mb4_bin")}, true},
		{"format 14 with the field injected", &irbackup.Manifest{Kind: irbackup.BackupKindFull, FormatVersion: irbackup.FormatVersionKeyCollationUnestablished, Schema: schema("utf8mb4_bin")}, true},
		{"an incremental whose delta carries it, unstamped", &irbackup.Manifest{Kind: irbackup.BackupKindIncremental, FormatVersion: 1, SchemaDelta: []*irbackup.SchemaDeltaEntry{
			{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: schema("").Tables[0], After: schema("utf8mb4_bin").Tables[0]},
		}}, true},
		{
			"an in-progress sidecar-tier full with the field injected",
			&irbackup.Manifest{
				Kind: irbackup.BackupKindFull, FormatVersion: irbackup.FormatVersionProgressSidecar, Schema: schema("utf8mb4_bin"),
				PartialState: irbackup.BackupStateInProgress, ProgressSidecar: &irbackup.ProgressSidecarRef{File: "manifest.progress.jsonl"},
			},
			true,
		},
		{"format 15 carrying it (what this binary writes)", &irbackup.Manifest{Kind: irbackup.BackupKindFull, FormatVersion: irbackup.FormatVersionHiddenCollation, Schema: schema("utf8mb4_bin")}, false},
		{"format 14 carrying nothing", &irbackup.Manifest{Kind: irbackup.BackupKindFull, FormatVersion: irbackup.FormatVersionKeyCollationUnestablished, Schema: schema("")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			if err := WriteManifest(ctx, store, tc.m); err != nil {
				t.Fatal(err)
			}
			const at = "manifests/incr-0001.json"
			if err := WriteManifestAt(ctx, store, at, tc.m); err != nil {
				t.Fatal(err)
			}
			_, errRoot := ReadManifest(ctx, store)
			_, errAt := ReadManifestAt(ctx, store, at)
			for name, err := range map[string]error{"ReadManifest": errRoot, "ReadManifestAt": errAt} {
				if tc.refuses != (err != nil) {
					t.Fatalf("%s: err %v; want refused %v", name, err, tc.refuses)
				}
				if err != nil && !strings.Contains(err.Error(), "hidden-column collation") {
					t.Errorf("%s refused, but not by this door: %v", name, err)
				}
			}
		})
	}
}
