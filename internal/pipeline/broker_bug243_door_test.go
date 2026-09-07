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

// TestSyncFromBackup_ColdStartReset_RefusesIntegrityFailuresBeforeDrop is
// the RCN-1 half (audit 2026-09-06): BRK-1 hoisted ONE door of seven, and
// the six left below the drop include all three integrity gates.
//
// Same discriminator as the pin above, which is what makes these cells
// mean anything: stubTargetEngine.OpenRowWriter returns a plain "stub"
// error, so a plain row-writer error proves the drop was attempted FIRST,
// while a coded refusal proves a door fired first. Measured pre-fix, all
// three of these returned `broker: --reset-target-data: open row writer:
// stub` — the drop ran, then the restore refused, and the operator was
// left with neither a target nor a usable chain.
//
// These are the gates that fire when a chain is CORRUPT OR TAMPERED,
// i.e. exactly when there may be nothing else to restore from. That is
// why the ordering matters more here than for an ordinary refusal.
func TestSyncFromBackup_ColdStartReset_RefusesIntegrityFailuresBeforeDrop(t *testing.T) {
	newBroker := func(t *testing.T, mutate func(m *irbackup.Manifest)) *SyncFromBackup {
		t.Helper()
		ctx := context.Background()
		dir := t.TempDir()
		store, err := blobcodec.NewLocalStore(dir)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
		full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
		full.BackupID = irbackup.ComputeBackupID(full)
		mutate(full)
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("write full: %v", err)
		}
		return &SyncFromBackup{
			Target: stubTargetEngine{}, TargetDSN: "x", Store: store, StreamID: "s",
			ResetTargetData: true,
		}
	}

	// The control first: it proves the discriminator is live in THIS
	// harness. A clean chain must reach the row writer, i.e. produce the
	// plain "stub" error. Without it, every cell below could be passing
	// because the broker fails early for some unrelated reason.
	t.Run("control: a clean chain reaches the row writer", func(t *testing.T) {
		b := newBroker(t, func(*irbackup.Manifest) {})
		_, err := b.coldStartReset(context.Background(), nil)
		if err == nil {
			t.Fatal("the stub target should have failed at the row writer; this harness cannot " +
				"discriminate 'a door fired' from 'the drop ran' if a clean chain succeeds")
		}
		if _, coded := sluicecode.FromError(err); coded {
			t.Fatalf("a CLEAN chain raised a coded refusal, so the cells below cannot distinguish a "+
				"door from a false refusal: %v", err)
		}
		if !strings.Contains(err.Error(), "stub") {
			t.Fatalf("control did not reach the row writer; got: %v", err)
		}
	})

	t.Run("backup-id mismatch refuses before the drop", func(t *testing.T) {
		b := newBroker(t, func(m *irbackup.Manifest) { m.BackupID = "deadbeefdeadbeef" })
		assertColdStartResetRefusedBeforeDrop(t, b, "backup-id mismatch")
	})

	t.Run("schema-hash mismatch refuses before the drop", func(t *testing.T) {
		b := newBroker(t, func(m *irbackup.Manifest) {
			m.SchemaHash = "0000000000000000000000000000000000000000000000000000000000000000"
			m.BackupID = irbackup.ComputeBackupID(m)
		})
		assertColdStartResetRefusedBeforeDrop(t, b, "schema-hash mismatch")
	})

	t.Run("--require-signature over an unsigned chain refuses before the drop", func(t *testing.T) {
		b := newBroker(t, func(*irbackup.Manifest) {})
		b.RequireSignature = true
		assertColdStartResetRefusedBeforeDrop(t, b, "require-signature over an unsigned chain")
	})
}

// assertColdStartResetRefusedBeforeDrop runs the cold start and requires
// that it failed with something OTHER than the stub row-writer error —
// which, given the control above, means a door fired before the drop.
func assertColdStartResetRefusedBeforeDrop(t *testing.T, b *SyncFromBackup, shape string) {
	t.Helper()
	_, err := b.coldStartReset(context.Background(), nil)
	if err == nil {
		t.Fatalf("%s: coldStartReset accepted a chain the restore must refuse", shape)
	}
	if strings.Contains(err.Error(), "open row writer") {
		t.Fatalf("%s: the DROP ran first — the refusal arrived only after the broker had dropped every "+
			"table in the cached tail manifest. These gates fire on a corrupt or tampered chain, so this "+
			"turns 'bad chain, target intact' into 'bad chain, target gone'. Got: %v", shape, err)
	}
}
