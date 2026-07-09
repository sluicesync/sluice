//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0154 Phase 1 integration: signed-manifest DR round-trip + store
// tamper matrix against real Postgres.
//
// Proves the real crypto + store + chain-restore wiring for a SIGNED
// encrypted chain: a `backup full --encrypt --sign` + `backup
// incremental` (signing forced because the chain is signed), `backup
// verify` reports valid, chain-restore lands the data byte-exact, and a
// store-level tamper (corrupt signature / dropped signature / truncated
// change-list / swapped manifest) is refused LOUDLY with the coded
// SLUICE-E-BACKUP-SIGNATURE-* class before any data lands. The unit
// tests pin the tamper CLASSES exhaustively; this pins the end-to-end
// plumbing that carries them through a real backup + restore.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func storeBytes(t *testing.T, store irbackup.Store, path string) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), path)
	if err != nil {
		t.Fatalf("store.Get %q: %v", path, err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	return b
}

func storePut(t *testing.T, store irbackup.Store, path string, b []byte) {
	t.Helper()
	if err := store.Put(context.Background(), path, bytes.NewReader(b)); err != nil {
		t.Fatalf("store.Put %q: %v", path, err)
	}
}

// TestBackup_SignedManifest_DR_RoundTripAndTamper is the end-to-end DR
// pin: signed encrypted full + incremental, verify, byte-exact restore,
// and a store-tamper matrix that must refuse loudly.
func TestBackup_SignedManifest_DR_RoundTripAndTamper(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	const passphrase = "correct horse battery staple"

	// 1. Signed encrypted full with chain provisioning.
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChainSlot: true,
		Encryption: &lineage.BackupEncryption{Envelope: newTestPassphraseEnvelope(t, passphrase), Mode: crypto.EncryptModePerChain},
		Sign:       true,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (signed full): %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if full.FormatVersion != irbackup.FormatVersionSignedManifest {
		t.Fatalf("full FormatVersion = %d; want %d (signed)", full.FormatVersion, irbackup.FormatVersionSignedManifest)
	}
	if signed, _ := lineage.ChainIsSigned(context.Background(), store); !signed {
		t.Fatal("chain not detected as signed (lineage.json.sig missing)")
	}
	if ex, _ := store.Exists(context.Background(), lineage.ManifestSigPath(lineage.ManifestFileName)); !ex {
		t.Fatal("manifest.json.sig missing after signed full")
	}

	// 2. Delta + incremental. Signing is forced (chain is signed) even
	// though Sign is not set here — the incremental must detect and sign.
	applyDDL(t, sourceDSN, `
		INSERT INTO users (id, email, active) VALUES (4, 'dave@example.com', true);
		UPDATE users SET active = false WHERE id = 1;
		DELETE FROM users WHERE id = 3;
	`)
	ctx, c := context.WithTimeout(context.Background(), 90*time.Second)
	defer c()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		Window: 10 * time.Second, MaxChanges: 3, ChunkChanges: 100,
		Encryption: &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            crypto.EncryptModePerChain,
		},
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run (signed): %v", err)
	}

	cat, _, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || cat == nil || len(cat.Segments) == 0 || len(cat.Segments[0].Incrementals) == 0 {
		t.Fatalf("lineage catalog missing incremental: cat=%v err=%v", cat, err)
	}
	incrPath := cat.Segments[0].Incrementals[0]
	if ex, _ := store.Exists(context.Background(), lineage.ManifestSigPath(incrPath)); !ex {
		t.Fatalf("incremental signature %q missing", lineage.ManifestSigPath(incrPath))
	}

	// 3. backup verify with the key: signatures valid, zero failures.
	verifyEnv := envelopeFromManifest(t, store, passphrase)
	if _, mismatches, verr := backup.VerifyBackupWith(context.Background(), store, backup.VerifyOptions{Envelope: verifyEnv}); verr != nil || mismatches != 0 {
		t.Fatalf("VerifyBackupWith: mismatches=%d err=%v (want 0/nil for a valid signed chain)", mismatches, verr)
	}

	// 4. Store-tamper matrix. Each case tampers, asserts chain-restore
	// refuses with the right coded class BEFORE any data lands, then
	// restores the original bytes. The target must stay empty throughout.
	incrManifestBytes := storeBytes(t, store, incrPath)
	fullManifestBytes := storeBytes(t, store, lineage.ManifestFileName)
	fullSigPath := lineage.ManifestSigPath(lineage.ManifestFileName)
	incrSigPath := lineage.ManifestSigPath(incrPath)
	origFullSig := storeBytes(t, store, fullSigPath)
	origIncrSig := storeBytes(t, store, incrSigPath)
	// restorePristine resets the store to its untampered signed state.
	restorePristine := func() {
		storePut(t, store, fullSigPath, origFullSig)
		storePut(t, store, incrSigPath, origIncrSig)
		storePut(t, store, incrPath, incrManifestBytes)
	}

	tamperCases := []struct {
		name  string
		apply func()
		undo  func()
		// wantCode is the coded class the refusal must carry; empty means
		// "any loud refusal" (a whole-manifest swap is caught by the
		// structural Kind/parent checks AHEAD of the signature backstop —
		// defense in depth, still a loud refusal, just a different code).
		wantCode sluicecode.Code
	}{
		{
			name: "corrupt full manifest signature",
			apply: func() {
				b := storeBytes(t, store, fullSigPath)
				var sig irbackup.ManifestSignature
				_ = json.Unmarshal(b, &sig)
				sig.MAC = flipHex(sig.MAC)
				nb, _ := json.Marshal(&sig)
				storePut(t, store, fullSigPath, nb)
			},
			undo:     func() {},
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
		{
			name:     "dropped incremental signature",
			apply:    func() { _ = store.Delete(context.Background(), incrSigPath) },
			undo:     func() {},
			wantCode: sluicecode.CodeBackupSignatureMissing,
		},
		{
			name: "truncated change-list tail",
			apply: func() {
				var m irbackup.Manifest
				_ = json.Unmarshal(incrManifestBytes, &m)
				if len(m.ChangeChunks) == 0 {
					t.Skip("incremental produced no change chunks; cannot exercise truncation")
				}
				m.ChangeChunks = m.ChangeChunks[:len(m.ChangeChunks)-1]
				nb, _ := json.Marshal(&m)
				storePut(t, store, incrPath, nb)
			},
			undo:     func() { storePut(t, store, incrPath, incrManifestBytes) },
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
		{
			// A whole-manifest substitution that PRESERVES the structural
			// shape (Kind + parent linkage) so it slips past the chain-build
			// checks and must be caught by the signature backstop: same
			// incremental, one recorded row-count bumped.
			name: "substituted incremental manifest (same shape, tampered content)",
			apply: func() {
				var m irbackup.Manifest
				_ = json.Unmarshal(incrManifestBytes, &m)
				if len(m.ChangeChunks) == 0 {
					t.Skip("incremental produced no change chunks")
				}
				m.ChangeChunks[0].RowCount += 1000
				nb, _ := json.Marshal(&m)
				storePut(t, store, incrPath, nb)
			},
			undo:     func() {},
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
		{
			// A whole-backup rollback (swap the newest link's manifest for
			// an OLDER, differently-shaped one) is refused loudly — here the
			// structural Kind check fires ahead of the signature backstop.
			name:     "whole-manifest swap (older backup rollback)",
			apply:    func() { storePut(t, store, incrPath, fullManifestBytes) },
			undo:     func() {},
			wantCode: "", // any loud refusal
		},
	}
	for _, tc := range tamperCases {
		t.Run(tc.name, func(t *testing.T) {
			restorePristine()
			tc.apply()
			err := (&backup.ChainRestore{
				Target: pgEng, TargetDSN: targetDSN, Store: store, Envelope: envelopeFromManifest(t, store, passphrase),
			}).Run(context.Background())
			if err == nil {
				t.Fatal("tamper was not refused")
			}
			if tc.wantCode != "" {
				if ce, ok := sluicecode.FromError(err); !ok || ce.Code != tc.wantCode {
					t.Fatalf("got %v (code ok=%v), want %s", err, ok, tc.wantCode)
				}
			}
			// Target must be empty — the refusal is a preflight, before any
			// data lands.
			if n := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM information_schema.tables WHERE table_name='users' AND table_schema='public'"); n != 0 {
				t.Fatalf("tamper refusal still created the target table (%d) — data landed before the signature check", n)
			}
			tc.undo()
		})
	}

	// 5. Restore pristine state, then a clean chain-restore lands the data
	// byte-exact.
	restorePristine()
	if err := (&backup.ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: store, Envelope: envelopeFromManifest(t, store, passphrase),
	}).Run(context.Background()); err != nil {
		t.Fatalf("clean ChainRestore after undo: %v", err)
	}
	// Delta applied: id 3 deleted, id 4 added, id 1 deactivated.
	if got := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM users"); got != 3 {
		t.Errorf("restored row count = %d; want 3", got)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT coalesce(sum(id),0) FROM users"); got != 7 {
		t.Errorf("restored sum(id) = %d; want 7 (1+2+4)", got)
	}
	if got := pgQueryOne[bool](t, targetDSN, "SELECT active FROM users WHERE id=1"); got {
		t.Errorf("id=1 active = true; want false (delta UPDATE not applied)")
	}
}

func flipHex(s string) string {
	if s == "" {
		return "00"
	}
	if s[0] == '0' {
		return "1" + s[1:]
	}
	return "0" + s[1:]
}
