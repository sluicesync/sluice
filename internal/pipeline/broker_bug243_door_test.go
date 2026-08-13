// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The BRK-1 ordering pin (audit 2026-08-11): the broker's
// --reset-target-data cold start must refuse a Bug 243 malformed-schema
// chain BEFORE its Bug 40a table drop, while the target still holds its
// data. ChainRestore.Run carries its own doors, but they fire after the
// broker has already dropped every table named in the cached manifest —
// pre-fix, a refusable chain cost the operator their target's tables and
// THEN refused.
package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestSyncFromBackup_ColdStartReset_RefusesMalformedChainBeforeDrop drives
// coldStartReset against a chain whose full carries the MEASURED Bug 243
// mangle (the same fixture bytes as the backup package's door pins). The
// ordering claim rides the stub target: dropExistingTargetTables opens a
// row writer, and stubTargetEngine's OpenRowWriter returns a plain "stub"
// error — so the CODED refusal arriving instead proves the door fired
// before any drop was attempted. If the door were missing, this test
// would see the row-writer error, not the code.
func TestSyncFromBackup_ColdStartReset_RefusesMalformedChainBeforeDrop(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema.Tables[0].CheckConstraints = []*ir.CheckConstraint{
		// The measured pre-v0.120.0 recording of CHECK (name <> 'o''brien')
		// — a literal that never closes (the backup package's
		// bug243MangledExpr, re-spelled here because the fixture is
		// package-scoped; Go raw strings are the only trustworthy channel
		// for escape bytes).
		{Name: "ck_name", Expr: `name <> 'o\\'brien'`},
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	b := &SyncFromBackup{
		Target: stubTargetEngine{}, TargetDSN: "x", Store: store, StreamID: "s",
		ResetTargetData: true,
	}
	_, err = b.coldStartReset(ctx, nil)
	if err == nil {
		t.Fatal("coldStartReset accepted a chain whose restore must refuse — " +
			"the target's tables would have been dropped for nothing")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRecordedSchemaMalformed {
		t.Fatalf("want %s BEFORE any drop (a row-writer 'stub' error here means the drop ran first), got: %v",
			sluicecode.CodeBackupRecordedSchemaMalformed, err)
	}
	if !strings.Contains(err.Error(), "--reset-target-data") {
		t.Errorf("refusal does not name the broker door: %v", err)
	}
}
