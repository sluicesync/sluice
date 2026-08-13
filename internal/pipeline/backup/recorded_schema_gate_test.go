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
	problems := filteredSchemaLexProblems(nil, bug243Schema(), migcore.TableFilter{})
	if len(problems) != 1 {
		t.Fatalf("unfiltered problems = %d; want 1", len(problems))
	}
	filter, err := migcore.NewTableFilter(nil, []string{"ck"})
	if err != nil {
		t.Fatal(err)
	}
	if problems := filteredSchemaLexProblems(nil, bug243Schema(), filter); len(problems) != 0 {
		t.Fatalf("--exclude-table=ck still reports %v — the documented remedy re-refuses", problems)
	}
	// The include-form scoped AWAY from the affected table is equally a remedy.
	inc, err := migcore.NewTableFilter([]string{"plain"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if problems := filteredSchemaLexProblems(nil, bug243Schema(), inc); len(problems) != 0 {
		t.Fatalf("--include-table=plain still reports %v", problems)
	}
}

// TestRefuseChainRecordedSchemaMalformed_BrokerDoor pins the exported
// fifth door (audit 2026-08-11 BRK-1, consumed by the broker's
// --reset-target-data cold start BEFORE its destructive drop): a clean
// chain passes, a mangled full refuses with the shared code, and a
// mangle arriving only via an incremental's schema delta refuses too —
// the whole-chain scan, one detector, one renderer.
func TestRefuseChainRecordedSchemaMalformed_BrokerDoor(t *testing.T) {
	clean := bug243Manifest("full0001", &ir.Schema{Tables: []*ir.Table{
		{Name: "plain", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}},
	}})
	if err := RefuseChainRecordedSchemaMalformed("broker", []*irbackup.Manifest{clean, nil}); err != nil {
		t.Fatalf("clean chain refused: %v", err)
	}

	mangledFull := bug243Manifest("full0002", bug243Schema())
	err := RefuseChainRecordedSchemaMalformed("broker", []*irbackup.Manifest{mangledFull})
	if err == nil {
		t.Fatal("mangled full passed the chain door — the broker would drop the target's tables and THEN refuse")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupRecordedSchemaMalformed, err)
	}

	incr := bug243Manifest("incr0001", nil)
	incr.Kind = irbackup.BackupKindIncremental
	incr.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
		Kind:  irbackup.SchemaDeltaAddTable,
		Table: "ck2",
		After: &ir.Table{
			Name:             "ck2",
			Columns:          []*ir.Column{{Name: "name", Type: ir.Varchar{Length: 40}}},
			CheckConstraints: []*ir.CheckConstraint{{Name: "c", Expr: bug243MangledExpr}},
		},
	}}
	err = RefuseChainRecordedSchemaMalformed("broker", []*irbackup.Manifest{clean, incr})
	if err == nil {
		t.Fatal("a mangle arriving only via a schema delta passed the chain door")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("delta arm: want %s, got %v", sluicecode.CodeBackupRecordedSchemaMalformed, err)
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
	problems := ManifestRecordedSchemaProblems(m)
	if len(problems) != 1 || !strings.Contains(problems[0], `"ck2"`) {
		t.Fatalf("delta problems = %v; want exactly the ck2 CHECK", problems)
	}
	if got := ManifestRecordedSchemaProblems(nil); got != nil {
		t.Errorf("nil manifest = %v; want nil", got)
	}
}

// ---- The Bug 243 RESIDUE arm: structurally valid, silently wrong ----
//
// A pre-v0.120.0 MySQL-family recording spells a literal backslash in
// MySQL's DOUBLED form (`'a\d'` meaning ONE backslash). It lexes
// cleanly — the structural arm above never fires — but emits wrong on
// every current target: PG/SQLite read two characters, and the
// post-v0.120.0 MySQL emitter re-doubles (the premise pin lives in
// internal/engines/mysql, TestEscapeExprLiteralBackslashes_
// PreV0120DoubledRecordingReDoubles_Premise). The arm is keyed on the
// manifest's (SourceEngine, SluiceVersion).

const bug243ResidueExpr = `code <> 'a\d'`

func bug243ResidueManifest(sourceEngine, version string) *irbackup.Manifest {
	m := bug243Manifest("full0001", bug243ResidueSchema())
	m.SourceEngine = sourceEngine
	m.SluiceVersion = version
	// A real content-derived BackupID: the clean direction of the verify
	// pin runs PAST the recorded-schema door into the BackupID check.
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

func bug243ResidueSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "ck",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "code", Type: ir.Varchar{Length: 40}},
			},
			CheckConstraints: []*ir.CheckConstraint{{Name: "ck_code", Expr: bug243ResidueExpr}},
		},
		{
			Name:    "plain",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}},
		},
	}}
}

// TestRecordedByPreEscapeFixMySQLReader pins the era key: MySQL-family
// source + parseable version < 0.120.0. The unparseable rows ("dev",
// "") are the STATED coverage boundary, not an accident — treating them
// as old would refuse chains a from-source build writes today, with a
// remedy (fresh backup, also stamped "dev") that could never release
// it: the Bug 245/246/247 unrunnable-remedy class.
func TestRecordedByPreEscapeFixMySQLReader(t *testing.T) {
	cases := []struct {
		engine, version string
		want            bool
	}{
		{"mysql", "0.119.3", true},
		{"mysql", "0.99.292", true},
		{"mysql", "v0.119.0", true},
		{"mariadb", "0.119.3", true},
		{"mydumper", "0.99.292", true},
		{"planetscale", "0.119.3", true},
		{"vitess", "0.119.3", true},
		{"mysql", "0.120.0", false},
		{"mysql", "0.124.1", false},
		{"mysql", "1.0.0", false},
		{"mysql", "dev", false},
		{"mysql", "", false},
		{"postgres", "0.119.3", false},
		{"sqlite", "0.119.3", false},
	}
	for _, tc := range cases {
		m := bug243ResidueManifest(tc.engine, tc.version)
		if got := recordedByPreEscapeFixMySQLReader(m); got != tc.want {
			t.Errorf("(%s, %q) = %v; want %v", tc.engine, tc.version, got, tc.want)
		}
	}
	if recordedByPreEscapeFixMySQLReader(nil) {
		t.Error("nil manifest must not gate")
	}
}

// TestVerifyBackup_DoubledBackslashResidue is the verify door on the
// residue arm, both directions: an old-era manifest refuses with the
// shared code and a message naming the era and the doubled spelling; a
// byte-identical schema recorded by v0.120.0+ passes clean.
func TestVerifyBackup_DoubledBackslashResidue(t *testing.T) {
	ctx := context.Background()

	run := func(version string) error {
		store := newMemStore()
		m := bug243ResidueManifest("mysql", version)
		if err := lineage.WriteManifest(ctx, store, m); err != nil {
			t.Fatal(err)
		}
		_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.DefaultCodec)
		_, _, err := VerifyBackupWith(ctx, store, VerifyOptions{})
		return err
	}

	err := run("0.119.3")
	if err == nil {
		t.Fatal("verify passed a pre-v0.120.0 chain whose literal backslashes restore silently wrong")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupRecordedSchemaMalformed, err)
	}
	for _, want := range []string{`"ck"`, `CHECK "ck_code"`, "doubled backslash spelling", "0.119.3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %s", err, want)
		}
	}
	if !strings.Contains(ce.Hint, "--exclude-table") {
		t.Errorf("refusal hint %q does not carry the salvage remedy", ce.Hint)
	}

	if err := run("0.120.0"); err != nil {
		t.Fatalf("the SAME schema recorded by v0.120.0 (bare contract) must pass verify; got %v", err)
	}
}

// TestRestoreRun_DoubledBackslashResidue_RefusesPreDDL is the restore
// door: the coded refusal fires from the manifest alone — the stub
// target proves no engine call happens first (it panics on use).
func TestRestoreRun_DoubledBackslashResidue_RefusesPreDDL(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := bug243ResidueManifest("mysql", "0.99.292")
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
}

// TestFilteredSchemaLexProblems_DoubledBackslashResidue pins the
// filter-awareness of the residue arm (the salvage remedy) and the era
// keying at the restore door's own collector: exclude-table and the
// include-form both release the gate, and the same schema under a
// post-fix or absent manifest reports nothing.
func TestFilteredSchemaLexProblems_DoubledBackslashResidue(t *testing.T) {
	old := bug243ResidueManifest("mysql", "0.119.3")
	if got := filteredSchemaLexProblems(old, old.Schema, migcore.TableFilter{}); len(got) != 1 {
		t.Fatalf("unfiltered problems = %d; want 1 (the residue site)", len(got))
	}
	filter, err := migcore.NewTableFilter(nil, []string{"ck"})
	if err != nil {
		t.Fatal(err)
	}
	if got := filteredSchemaLexProblems(old, old.Schema, filter); len(got) != 0 {
		t.Fatalf("--exclude-table=ck still reports %v — the documented remedy re-refuses", got)
	}
	inc, err := migcore.NewTableFilter([]string{"plain"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := filteredSchemaLexProblems(old, old.Schema, inc); len(got) != 0 {
		t.Fatalf("--include-table=plain still reports %v", got)
	}

	fresh := bug243ResidueManifest("mysql", "0.120.0")
	if got := filteredSchemaLexProblems(fresh, fresh.Schema, migcore.TableFilter{}); len(got) != 0 {
		t.Fatalf("a v0.120.0 recording reported %v; the bare contract is correct", got)
	}
	if got := filteredSchemaLexProblems(nil, old.Schema, migcore.TableFilter{}); len(got) != 0 {
		t.Fatalf("a nil manifest (no era evidence) reported %v", got)
	}
}

// TestManifestRecordedSchemaProblems_ResidueDeltaArm pins that the
// doubled-backslash arm reaches SCHEMA-DELTA tables exactly like the
// structural arm — an incremental's AddTable/AlterTable carries
// expressions a restore emits too.
func TestManifestRecordedSchemaProblems_ResidueDeltaArm(t *testing.T) {
	m := bug243ResidueManifest("mysql", "0.119.3")
	m.Schema = nil
	m.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
		Table: "ck",
		After: bug243ResidueSchema().Tables[0],
	}}
	got := ManifestRecordedSchemaProblems(m)
	if len(got) != 1 || !strings.Contains(got[0], "doubled backslash spelling") {
		t.Fatalf("delta residue problems = %v; want the one described site", got)
	}
	m.SluiceVersion = "0.124.0"
	if got := ManifestRecordedSchemaProblems(m); len(got) != 0 {
		t.Fatalf("post-fix delta reported %v", got)
	}
}
