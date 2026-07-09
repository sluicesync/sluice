//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0154 back-compat integration: a Phase-1 (v0.99.208, canon v2) signed
// chain, EXTENDED by a Phase-2 binary (canon v3), restores byte-exact on
// the Phase-2 binary under the DUAL-VERSION verifier. Guards the "newer
// sluice always reads older" manifest invariant end-to-end through real
// crypto + store + chain-restore — the unit tests pin the dual-version
// verify shape; this pins that a mixed v2/v3 chain actually RESTORES.

package pipeline

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestBackup_SignedManifest_MixedV2V3_Restores builds an HMAC-signed
// full+incremental, DOWNGRADES the full's detached signature to the exact
// Phase-1 v2 shape (as if v0.99.208 wrote it and a Phase-2 binary appended
// the v3 incremental), then chain-restores byte-exact.
func TestBackup_SignedManifest_MixedV2V3_Restores(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const passphrase = "correct horse battery staple"

	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChainSlot: true,
		Encryption: &lineage.BackupEncryption{Envelope: newTestPassphraseEnvelope(t, passphrase), Mode: crypto.EncryptModePerChain},
		Sign:       true,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (signed full): %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

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

	// Downgrade the FULL's signature to the exact Phase-1 v2 shape.
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	env := envelopeFromManifest(t, store, passphrase)
	key, err := env.(crypto.ManifestSigner).ManifestSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := irbackup.CanonicalManifestBytesForVersion(full, 0, irbackup.ManifestCanonVersionV2, "")
	if err != nil {
		t.Fatal(err)
	}
	v2sig := &irbackup.ManifestSignature{
		CanonVersion: irbackup.ManifestCanonVersionV2,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        crypto.ManifestSigKeyID(key),
		Sequence:     0,
		ChunkCount:   irbackup.ManifestChunkCount(full),
		MAC:          hex.EncodeToString(crypto.SignManifestHMAC(key, payload)),
	}
	body, err := irbackup.MarshalManifestSignature(v2sig)
	if err != nil {
		t.Fatal(err)
	}
	storePut(t, store, lineage.ManifestSigPath(lineage.ManifestFileName), body)

	// backup verify accepts the mixed chain.
	if _, mism, verr := backup.VerifyBackupWith(context.Background(), store, backup.VerifyOptions{Envelope: envelopeFromManifest(t, store, passphrase)}); verr != nil || mism != 0 {
		t.Fatalf("verify mixed v2/v3 chain: mism=%d err=%v", mism, verr)
	}

	// Chain-restore lands the data byte-exact under the dual-version verifier.
	if err := (&backup.ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: store, Envelope: envelopeFromManifest(t, store, passphrase),
	}).Run(context.Background()); err != nil {
		t.Fatalf("mixed v2/v3 chain-restore refused: %v", err)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT count(*) FROM users"); got != 3 {
		t.Errorf("restored row count = %d; want 3", got)
	}
	if got := pgQueryOne[int64](t, targetDSN, "SELECT coalesce(sum(id),0) FROM users"); got != 7 {
		t.Errorf("restored sum(id) = %d; want 7 (1+2+4)", got)
	}
}
