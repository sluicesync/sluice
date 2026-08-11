// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The Bug 243 door pins. The mangled expression is the MEASURED
// recording a pre-v0.120.0 reader produced for `CHECK (name <>
// 'o''brien')` — a literal that never closes — taken verbatim from the
// catalog's fixture chain. Both doors that REFUSE must report
// SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED before anything touches the
// target or the chunks; pre-fix, verify returned rc=0 over exactly this
// manifest and restore died in the target's parser mid-create.

const bug243MangledExpr = `name <> 'o\\'brien'`

func bug243Manifest(backupID string, schema *ir.Schema) *irbackup.Manifest {
	return &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionLegacy,
		CreatedAt:     time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "mysql", Kind: irbackup.BackupKindFull, BackupID: backupID,
		Schema: schema,
		Tables: []*irbackup.TableManifest{
			{Name: "ck", Chunks: []*irbackup.ChunkInfo{}},
		},
	}
}

func bug243Schema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "ck",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "name", Type: ir.Varchar{Length: 40}},
			},
			CheckConstraints: []*ir.CheckConstraint{{Name: "ck_name", Expr: bug243MangledExpr}},
		},
		{
			Name:    "plain",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}},
		},
	}}
}

// TestVerifyBackup_MalformedRecordedSchema_Refuses is the verify door:
// every chunk of this chain is "intact" (there are none to fail), and
// pre-fix that was exactly the problem — verify passed a chain restore
// cannot start on.
func TestVerifyBackup_MalformedRecordedSchema_Refuses(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := bug243Manifest("full0001", bug243Schema())
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.DefaultCodec)

	_, _, err := VerifyBackupWith(ctx, store, VerifyOptions{})
	if err == nil {
		t.Fatal("backup verify passed a chain whose recorded schema cannot be emitted — the Bug 243 shape")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupRecordedSchemaMalformed, err)
	}
	for _, want := range []string{`"ck"`, `CHECK "ck_name"`, "never closes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %s", err, want)
		}
	}
}

// TestRestoreRun_MalformedRecordedSchema_RefusesPreDDL is the restore
// door: the coded refusal fires from the manifest alone — the stub
// target proves no engine call happens first (it panics on use), which
// IS the pre-DDL claim.
func TestRestoreRun_MalformedRecordedSchema_RefusesPreDDL(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := bug243Manifest("full0001", bug243Schema())
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.DefaultCodec)

	r := &Restore{Store: store, Target: stubEngine{}, TargetDSN: "target-dsn"}
	err := r.Run(ctx)
	if err == nil {
		t.Fatal("want the coded refusal, got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupRecordedSchemaMalformed, err)
	}
	if !strings.Contains(ce.Hint, "--exclude-table") {
		t.Errorf("refusal hint %q does not carry the salvage remedy", ce.Hint)
	}
}

// TestRestoreRun_MalformedRecordedSchema_ExcludeTableIsAWorkingRemedy pins
// the filter-awareness the remedy depends on: excluding the affected
// table must get PAST this gate (the run then proceeds toward the target
// preflights — the stub engine's panic there is proof the gate released
// it, and a real run would restore the surviving tables).
func TestRestoreRun_MalformedRecordedSchema_ExcludeTableIsAWorkingRemedy(t *testing.T) {
	problems := filteredSchemaLexProblems(bug243Schema(), migcore.TableFilter{})
	if len(problems) != 1 {
		t.Fatalf("unfiltered problems = %d; want 1", len(problems))
	}
	filter, err := migcore.NewTableFilter(nil, []string{"ck"})
	if err != nil {
		t.Fatal(err)
	}
	if problems := filteredSchemaLexProblems(bug243Schema(), filter); len(problems) != 0 {
		t.Fatalf("--exclude-table=ck still reports %v — the documented remedy re-refuses", problems)
	}
	// The include-form scoped AWAY from the affected table is equally a remedy.
	inc, err := migcore.NewTableFilter([]string{"plain"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if problems := filteredSchemaLexProblems(bug243Schema(), inc); len(problems) != 0 {
		t.Fatalf("--include-table=plain still reports %v", problems)
	}
}

// TestManifestRecordedSchemaProblems_CoversSchemaDeltas pins the delta
// door's input: a mangled expression arriving via a SchemaDelta ADD (the
// chain-evolution path, applied unfiltered at restore) is a problem too.
func TestManifestRecordedSchemaProblems_CoversSchemaDeltas(t *testing.T) {
	m := bug243Manifest("incr0001", nil)
	m.Kind = irbackup.BackupKindIncremental
	m.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
		Kind:  irbackup.SchemaDeltaAddTable,
		Table: "ck2",
		After: &ir.Table{
			Name:             "ck2",
			Columns:          []*ir.Column{{Name: "name", Type: ir.Varchar{Length: 40}}},
			CheckConstraints: []*ir.CheckConstraint{{Name: "c", Expr: bug243MangledExpr}},
		},
	}}
	problems := manifestRecordedSchemaProblems(m)
	if len(problems) != 1 || !strings.Contains(problems[0], `"ck2"`) {
		t.Fatalf("delta problems = %v; want exactly the ck2 CHECK", problems)
	}
	if got := manifestRecordedSchemaProblems(nil); got != nil {
		t.Errorf("nil manifest = %v; want nil", got)
	}
}
