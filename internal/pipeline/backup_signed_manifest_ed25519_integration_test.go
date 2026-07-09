//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0154 Phase 2 integration: Ed25519 (asymmetric) signed-manifest DR
// round-trip against real Postgres, covering the two capabilities Phase 2
// unlocks over Phase 1's HMAC-off-KEK:
//
//   - PLAINTEXT signing (Phase 1 refused it — no KEK to key off): a
//     `backup full --sign-key` with NO --encrypt, verified + restored with
//     only the PUBLIC key.
//   - Key-separated verification: the restore/verify side holds ONLY the
//     public key, never a signing secret.
//
// It also exercises an ENCRYPTED + Ed25519-signed chain (full +
// incremental) — proving Ed25519 signing is orthogonal to the encryption
// keystore — verify, byte-exact chain-restore, a store-tamper refusal, a
// wrong-verify-key refusal, and the DR-safe no-verify-key warn-and-proceed
// policy. The unit tests pin the tamper + scheme-confusion CLASSES; this
// pins the end-to-end plumbing carrying them through a real backup/restore.

package pipeline

import (
	"context"
	"encoding/json"
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

// TestBackup_SignedManifest_Ed25519_DR is the Phase 2 end-to-end pin.
func TestBackup_SignedManifest_Ed25519_DR(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	pub, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		t.Fatal(err)
	}
	otherPub, _, _ := crypto.GenerateEd25519Keypair()

	// ============================================================
	// Phase A — PLAINTEXT Ed25519 signing (the headline capability).
	// ============================================================
	storeA, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: storeA,
		SluiceVersion: "test",
		Ed25519Signer: lineage.NewEd25519Signer(priv),
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (plaintext ed25519 full): %v", err)
	}
	fullA, err := lineage.ReadManifest(context.Background(), storeA)
	if err != nil {
		t.Fatal(err)
	}
	if fullA.FormatVersion != irbackup.FormatVersionSignedManifest {
		t.Fatalf("plaintext signed full FormatVersion = %d; want %d", fullA.FormatVersion, irbackup.FormatVersionSignedManifest)
	}
	if fullA.ChainEncryption != nil {
		t.Fatal("plaintext backup unexpectedly carries ChainEncryption")
	}
	if signed, _ := lineage.ChainIsSigned(context.Background(), storeA); !signed {
		t.Fatal("plaintext signed chain not detected as signed")
	}

	// Verify with ONLY the public key (no envelope, no signing secret).
	if _, mism, verr := backup.VerifyBackupWith(context.Background(), storeA, backup.VerifyOptions{VerifyKey: pub}); verr != nil || mism != 0 {
		t.Fatalf("VerifyBackupWith (plaintext ed25519): mism=%d err=%v", mism, verr)
	}

	// Restore with the public key → byte-exact.
	if err := (&backup.Restore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeA, VerifyKey: pub,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore (plaintext ed25519, with verify key): %v", err)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM users"); got != 3 {
		t.Fatalf("plaintext restore row count = %d; want 3", got)
	}
	applyDDL(t, targetDSN, "DROP TABLE users;")

	// Wrong verify key → refuse loudly (INVALID), no data lands.
	werr := (&backup.Restore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeA, VerifyKey: otherPub,
	}).Run(context.Background())
	if ce, ok := sluicecode.FromError(werr); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("wrong verify key: got %v, want SIGNATURE-INVALID", werr)
	}
	if n := pgUsersTableCount(t, targetDSN); n != 0 {
		t.Fatalf("wrong-key refusal still created the target table (%d)", n)
	}

	// No verify key + no --require-signature → DR-safe warn-and-proceed.
	if err := (&backup.Restore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeA,
	}).Run(context.Background()); err != nil {
		t.Fatalf("no verify key (default policy) should warn-and-proceed, got %v", err)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM users"); got != 3 {
		t.Fatalf("warn-and-proceed restore row count = %d; want 3", got)
	}
	applyDDL(t, targetDSN, "DROP TABLE users;")

	// No verify key + --require-signature → refuse MISSING.
	rerr := (&backup.Restore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeA, RequireSignature: true,
	}).Run(context.Background())
	if ce, ok := sluicecode.FromError(rerr); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("no verify key + --require-signature: got %v, want SIGNATURE-MISSING", rerr)
	}
	applyDDL(t, targetDSN, "DROP TABLE IF EXISTS users;")

	// ============================================================
	// Phase B — ENCRYPTED + Ed25519 signing (orthogonal keystores).
	// ============================================================
	const passphrase = "correct horse battery staple"
	storeB, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: storeB,
		SluiceVersion: "test", ChainSlot: true,
		Encryption:    &lineage.BackupEncryption{Envelope: newTestPassphraseEnvelope(t, passphrase), Mode: crypto.EncryptModePerChain},
		Ed25519Signer: lineage.NewEd25519Signer(priv),
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (encrypted ed25519 full): %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	fullB, err := lineage.ReadManifest(context.Background(), storeB)
	if err != nil {
		t.Fatal(err)
	}
	if fullB.ChainEncryption == nil {
		t.Fatal("encrypted backup missing ChainEncryption")
	}
	if fullB.FormatVersion != irbackup.FormatVersionSignedManifest {
		t.Fatalf("encrypted signed full FormatVersion = %d; want %d", fullB.FormatVersion, irbackup.FormatVersionSignedManifest)
	}

	// Delta + incremental (Ed25519 signer supplied; chain is Ed25519-signed).
	applyDDL(t, sourceDSN, `
		INSERT INTO users (id, email, active) VALUES (4, 'dave@example.com', true);
		UPDATE users SET active = false WHERE id = 1;
		DELETE FROM users WHERE id = 3;
	`)
	ctx, c := context.WithTimeout(context.Background(), 90*time.Second)
	defer c()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: storeB,
		Window: 10 * time.Second, MaxChanges: 3, ChunkChanges: 100,
		Encryption: &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            crypto.EncryptModePerChain,
		},
		Ed25519Signer: lineage.NewEd25519Signer(priv),
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run (encrypted ed25519): %v", err)
	}

	cat, _, err := lineage.LoadLineageCatalog(context.Background(), storeB)
	if err != nil || cat == nil || len(cat.Segments) == 0 || len(cat.Segments[0].Incrementals) == 0 {
		t.Fatalf("lineage catalog missing incremental: cat=%v err=%v", cat, err)
	}
	incrPath := cat.Segments[0].Incrementals[0]

	// backup verify: needs BOTH the envelope (decrypt probe) and the
	// verify key (Ed25519 signatures) — the KEK alone does NOT verify them.
	verifyEnv := envelopeFromManifest(t, storeB, passphrase)
	if _, mism, verr := backup.VerifyBackupWith(context.Background(), storeB, backup.VerifyOptions{Envelope: verifyEnv, VerifyKey: pub}); verr != nil || mism != 0 {
		t.Fatalf("VerifyBackupWith (encrypted ed25519): mism=%d err=%v", mism, verr)
	}

	// Store-tamper: corrupt the incremental signature → refuse INVALID.
	incrSigPath := lineage.ManifestSigPath(incrPath)
	origIncrSig := storeBytes(t, storeB, incrSigPath)
	var sig irbackup.ManifestSignature
	_ = json.Unmarshal(origIncrSig, &sig)
	sig.MAC = flipHex(sig.MAC)
	nb, _ := json.Marshal(&sig)
	storePut(t, storeB, incrSigPath, nb)
	terr := (&backup.ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeB,
		Envelope: envelopeFromManifest(t, storeB, passphrase), VerifyKey: pub,
	}).Run(context.Background())
	if ce, ok := sluicecode.FromError(terr); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("corrupt ed25519 incr sig: got %v, want SIGNATURE-INVALID", terr)
	}
	if n := pgUsersTableCount(t, targetDSN); n != 0 {
		t.Fatalf("tamper refusal still created the target table (%d)", n)
	}
	storePut(t, storeB, incrSigPath, origIncrSig) // restore pristine

	// Clean chain-restore with envelope + verify key → byte-exact + deltas.
	if err := (&backup.ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: storeB,
		Envelope: envelopeFromManifest(t, storeB, passphrase), VerifyKey: pub,
	}).Run(context.Background()); err != nil {
		t.Fatalf("clean encrypted ed25519 chain-restore: %v", err)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM users"); got != 3 {
		t.Errorf("restored row count = %d; want 3", got)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT coalesce(sum(id),0) FROM users"); got != 7 {
		t.Errorf("restored sum(id) = %d; want 7 (1+2+4)", got)
	}
}

func pgUsersTableCount(t *testing.T, dsn string) int64 {
	t.Helper()
	return pgQueryOne[int64](t, dsn, "SELECT count(*) FROM information_schema.tables WHERE table_name='users' AND table_schema='public'")
}
