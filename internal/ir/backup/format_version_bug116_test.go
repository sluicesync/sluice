// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestFormatVersionFor_HiddenCollation is the FormatVersionHiddenCollation
// tier's proportionality, cell by cell (sixth review F3/F4): only a schema
// or delta that CARRIES a functional key's hidden-column collation moves
// to 15; an expression key without one, a plain key, and every other tier's
// trigger keep their version. The reverse door: a manifest below 15 that
// carries the field refuses, one at 15 or with nothing carried does not.
func TestFormatVersionFor_HiddenCollation(t *testing.T) {
	table := func(name, hidden string) *ir.Table {
		return &ir.Table{
			Name:       name,
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{{Name: "fk", Unique: true, Columns: []ir.IndexColumn{
				{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden},
			}}},
		}
	}
	for _, tc := range []struct {
		name   string
		schema *ir.Schema
		want   int
	}{
		{"functional key, hidden collation read", &ir.Schema{Tables: []*ir.Table{table("a", ""), table("b", "utf8mb4_bin")}}, FormatVersionHiddenCollation},
		{"functional key, hidden collation not read", &ir.Schema{Tables: []*ir.Table{table("a", "")}}, FormatVersionLegacy},
		{"with sequences too: the higher tier wins", &ir.Schema{Tables: []*ir.Table{table("a", "utf8mb4_bin")}, Sequences: []*ir.Sequence{{Name: "s"}}}, FormatVersionHiddenCollation},
		{"sequences, no hidden collation", &ir.Schema{Tables: []*ir.Table{table("a", "")}, Sequences: []*ir.Sequence{{Name: "s"}}}, FormatVersionStandaloneSequences},
		{"on the primary key", &ir.Schema{Tables: []*ir.Table{{Name: "p", PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Expression: "lower(k)", HiddenCollation: "latin1_bin"}}}}}}, FormatVersionHiddenCollation},
	} {
		if got := FormatVersionFor(tc.schema); got != tc.want {
			t.Errorf("%s: FormatVersionFor = %d; want %d", tc.name, got, tc.want)
		}
	}

	// The CDC segment's stamp: a delta side or the swapped-in schema.
	for _, tc := range []struct {
		name string
		m    *Manifest
		want int
	}{
		{"incremental, nothing carried", &Manifest{Kind: BackupKindIncremental, FormatVersion: 9, Schema: &ir.Schema{Tables: []*ir.Table{table("a", "")}}}, 9},
		{"incremental, schema carries", &Manifest{Kind: BackupKindIncremental, FormatVersion: 9, Schema: &ir.Schema{Tables: []*ir.Table{table("a", "utf8mb4_bin")}}}, FormatVersionHiddenCollation},
		{"incremental, delta after carries", &Manifest{Kind: BackupKindIncremental, FormatVersion: 1, SchemaDelta: []*SchemaDeltaEntry{{Kind: SchemaDeltaAlterTable, Table: "a", Before: table("a", ""), After: table("a", "utf8mb4_bin")}}}, FormatVersionHiddenCollation},
		{"incremental, delta before carries", &Manifest{Kind: BackupKindIncremental, FormatVersion: 1, SchemaDelta: []*SchemaDeltaEntry{{Kind: SchemaDeltaAlterTable, Table: "a", Before: table("a", "utf8mb4_bin"), After: table("a", "")}}}, FormatVersionHiddenCollation},
		{"full, schema carries", &Manifest{Kind: BackupKindFull, FormatVersion: 1, Schema: &ir.Schema{Tables: []*ir.Table{table("a", "utf8mb4_bin")}}}, FormatVersionHiddenCollation},
		{"nil manifest", nil, 0},
	} {
		StampHiddenCollation(tc.m)
		got := 0
		if tc.m != nil {
			got = tc.m.FormatVersion
		}
		if got != tc.want {
			t.Errorf("%s: StampHiddenCollation → %d; want %d", tc.name, got, tc.want)
		}
	}

	carrying := &Manifest{Kind: BackupKindFull, Schema: &ir.Schema{Tables: []*ir.Table{table("a", "utf8mb4_bin")}}}
	for fv, wantErr := range map[int]bool{1: true, 9: true, FormatVersionKeyCollationUnestablished: true, FormatVersionHiddenCollation: false} {
		carrying.FormatVersion = fv
		if err := RefuseUnstampedHiddenCollation(carrying); (err != nil) != wantErr {
			t.Errorf("format %d carrying the field: err %v; want refused %v", fv, err, wantErr)
		}
	}
	plain := &Manifest{Kind: BackupKindFull, FormatVersion: 1, Schema: &ir.Schema{Tables: []*ir.Table{table("a", "")}}}
	if err := RefuseUnstampedHiddenCollation(plain); err != nil {
		t.Errorf("an unstamped manifest carrying nothing refused: %v", err)
	}

	// A history entry carrying the field below 15 is the same edited shape:
	// the key-collation guard takes a history version's table as its source
	// shape, so an injected value there must be refused too.
	historyEntry := func(hidden string) *SchemaHistoryEntry {
		b, err := ir.MarshalTable(table("a", hidden))
		if err != nil {
			t.Fatalf("marshal history table: %v", err)
		}
		return &SchemaHistoryEntry{TableJSON: b}
	}
	inHistory := &Manifest{
		Kind: BackupKindIncremental, FormatVersion: FormatVersionKeyCollationUnestablished,
		SchemaHistory: []*SchemaHistoryEntry{historyEntry("utf8mb4_bin")},
	}
	if err := RefuseUnstampedHiddenCollation(inHistory); err == nil {
		t.Error("a format-14 manifest whose schema history carries a hidden collation was accepted")
	}
	plainHistory := &Manifest{
		Kind: BackupKindIncremental, FormatVersion: FormatVersionKeyCollationUnestablished,
		SchemaHistory: []*SchemaHistoryEntry{historyEntry("")},
	}
	if err := RefuseUnstampedHiddenCollation(plainHistory); err != nil {
		t.Errorf("a history entry carrying nothing refused: %v", err)
	}

	// WithoutHiddenCollation clears every one and mutates nothing.
	s := &ir.Schema{Tables: []*ir.Table{table("a", "utf8mb4_bin"), table("b", "")}}
	cleared := WithoutHiddenCollation(s)
	if FormatVersionFor(cleared) != FormatVersionLegacy || s.Tables[0].Indexes[0].Columns[0].HiddenCollation != "utf8mb4_bin" {
		t.Errorf("WithoutHiddenCollation: cleared → %d, caller's %q", FormatVersionFor(cleared), s.Tables[0].Indexes[0].Columns[0].HiddenCollation)
	}
	if cleared.Tables[1] != s.Tables[1] {
		t.Error("WithoutHiddenCollation copied a table that carried nothing")
	}
}

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
			name: "security-metadata is the sequence-less FINALIZED ceiling (FormatVersionProgressSidecar is in-progress-only and never chosen here)",
			schema: &ir.Schema{
				Tables: []*ir.Table{{Name: "x", RLSEnabled: true}},
			},
			want: FormatVersionSecurityMetadata,
		},
		{
			name: "standalone sequence → standalone-sequences (older binaries would silently drop Schema.Sequences)",
			schema: &ir.Schema{
				Tables:    []*ir.Table{{Name: "orders"}},
				Sequences: []*ir.Sequence{{Name: "order_number_seq", Start: 1000, Increment: 5}},
			},
			want: FormatVersionStandaloneSequences,
		},
		{
			name: "standalone sequence wins over security-metadata (highest tier needed)",
			schema: &ir.Schema{
				Tables:    []*ir.Table{{Name: "tenants", RLSEnabled: true}},
				Sequences: []*ir.Sequence{{Name: "s"}},
			},
			want: FormatVersionStandaloneSequences,
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
// ceiling is the key-collation-unestablished version, the
// key-collation-delta, exact-numbers, positionless-full, redaction-marker, injective-chunk-AAD, standalone-sequences, ADR-0086 in-progress
// sidecar and Bug 116 security-metadata versions keep their historical
// slots, and the legacy value is frozen. If a future change reorders
// these without updating the chooseFormatVersion / sidecar /
// chunk-binding contracts, this test catches the regression at build
// time.
func TestBackupFormatVersion_Bumped(t *testing.T) {
	if BackupFormatVersion != FormatVersionHiddenCollation {
		t.Errorf("BackupFormatVersion = %d; want FormatVersionHiddenCollation=%d (current ceiling)",
			BackupFormatVersion, FormatVersionHiddenCollation)
	}
	// Above the key-collation-unestablished tier: a 15-stamped manifest
	// keeps every lower `>=` fold, and none changes between 9 and 15.
	if FormatVersionHiddenCollation <= FormatVersionKeyCollationUnestablished {
		t.Errorf("FormatVersionHiddenCollation (%d) must be strictly greater than FormatVersionKeyCollationUnestablished (%d)",
			FormatVersionHiddenCollation, FormatVersionKeyCollationUnestablished)
	}
	// Above the key-collation-delta tier, for the same reason: a 14-stamped
	// segment keeps every lower `>=` fold, and none changes between 9 and 14.
	if FormatVersionKeyCollationUnestablished <= FormatVersionKeyCollationDelta {
		t.Errorf("FormatVersionKeyCollationUnestablished (%d) must be strictly greater than FormatVersionKeyCollationDelta (%d)",
			FormatVersionKeyCollationUnestablished, FormatVersionKeyCollationDelta)
	}
	// Above the exact-numbers tier, for the reason that tier sits above the
	// positionless one: a 13-stamped segment keeps every lower `>=` fold.
	if FormatVersionKeyCollationDelta <= FormatVersionExactNumbers {
		t.Errorf("FormatVersionKeyCollationDelta (%d) must be strictly greater than FormatVersionExactNumbers (%d)",
			FormatVersionKeyCollationDelta, FormatVersionExactNumbers)
	}
	// Above the positionless tier: a 12-stamped segment keeps every lower
	// `>=` fold, and the AAD encoding does not change between 11 and 12
	// (which is what makes the mid-capture stamp safe for sealed chunks).
	if FormatVersionExactNumbers <= FormatVersionPositionlessFull {
		t.Errorf("FormatVersionExactNumbers (%d) must be strictly greater than FormatVersionPositionlessFull (%d)",
			FormatVersionExactNumbers, FormatVersionPositionlessFull)
	}
	// The ladder is strictly ascending — the AAD gates are `>=` comparisons,
	// so a reordered constant would silently reroute a whole tier's chunks
	// to the wrong encoding.
	if FormatVersionInjectiveChunkAAD <= FormatVersionCDCPositionBinding {
		t.Errorf("FormatVersionInjectiveChunkAAD (%d) must be strictly greater than FormatVersionCDCPositionBinding (%d)",
			FormatVersionInjectiveChunkAAD, FormatVersionCDCPositionBinding)
	}
	if FormatVersionRedaction <= FormatVersionInjectiveChunkAAD {
		t.Errorf("FormatVersionRedaction (%d) must be strictly greater than FormatVersionInjectiveChunkAAD (%d)",
			FormatVersionRedaction, FormatVersionInjectiveChunkAAD)
	}
	// Above the redaction tier, not beside it: an 11-stamped manifest must
	// still take the redaction fold in ComputeBackupID (a redacted
	// positionless full carries both), which the `>=` gate only gives it
	// from above.
	if FormatVersionPositionlessFull <= FormatVersionRedaction {
		t.Errorf("FormatVersionPositionlessFull (%d) must be strictly greater than FormatVersionRedaction (%d)",
			FormatVersionPositionlessFull, FormatVersionRedaction)
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
	if FormatVersionStandaloneSequences <= FormatVersionProgressSidecar {
		t.Errorf("FormatVersionStandaloneSequences (%d) must be strictly greater than FormatVersionProgressSidecar (%d) — sequence-bearing manifests must refuse on every pre-sequence binary, including sidecar-capable ones",
			FormatVersionStandaloneSequences, FormatVersionProgressSidecar)
	}
	if FormatVersionEncryptedChunkBinding <= FormatVersionStandaloneSequences {
		t.Errorf("FormatVersionEncryptedChunkBinding (%d) must be strictly greater than FormatVersionStandaloneSequences (%d) — chunk-binding manifests must refuse on every pre-binding binary, whose nil-AAD decrypt would fail with a MISLEADING bare auth error",
			FormatVersionEncryptedChunkBinding, FormatVersionStandaloneSequences)
	}
	if FormatVersionSignedManifest <= FormatVersionEncryptedChunkBinding {
		t.Errorf("FormatVersionSignedManifest (%d) must be strictly greater than FormatVersionEncryptedChunkBinding (%d) — a signed manifest asserts a signature that pre-signing binaries cannot verify, so they must refuse it at the version gate (ADR-0154)",
			FormatVersionSignedManifest, FormatVersionEncryptedChunkBinding)
	}
	if FormatVersionChunkTableBinding <= FormatVersionSignedManifest {
		t.Errorf("FormatVersionChunkTableBinding (%d) must be strictly greater than FormatVersionSignedManifest (%d) — a table-bound row chunk decrypts against an AAD pre-v7 binaries cannot reproduce, so they must refuse it at the version gate (ADR-0154 SEC-F1 / SEC-1)",
			FormatVersionChunkTableBinding, FormatVersionSignedManifest)
	}
	if FormatVersionCDCPositionBinding <= FormatVersionChunkTableBinding {
		t.Errorf("FormatVersionCDCPositionBinding (%d) must be strictly greater than FormatVersionChunkTableBinding (%d) — a manifest that folds CDCPositionCommitsAfterRows into its BackupID recompute-mismatches on any pre-8 binary (which computes the id without the folded field), so they must refuse it at the version gate (item 57 fold)",
			FormatVersionCDCPositionBinding, FormatVersionChunkTableBinding)
	}
}
