// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// Bug-214 unit pins. The load-bearing proof is the encrypted
// compact→restore integration round trip
// (internal/pipeline/backup_compact_encrypted_roundtrip_integration_test.go);
// these pin the pieces that a memstore can hold honestly — that the
// chain-root manifest SURVIVES a compact, and that the readability gate
// refuses exactly the states that are unrestorable and no others.

// gateChainEnc returns a passphrase-mode ChainEncryption bound to salt.
func gateChainEnc(t *testing.T, salt, wrapped []byte) *irbackup.ChainEncryption {
	t.Helper()
	return &irbackup.ChainEncryption{
		Algorithm:  crypto.AlgorithmAESGCM,
		Mode:       crypto.EncryptModePerChain,
		KEKMode:    crypto.KEKModePassphrase,
		WrappedCEK: wrapped,
		Argon2id: &irbackup.Argon2idParams{
			Salt:        salt,
			Memory:      65536,
			Iterations:  1,
			Parallelism: 1,
			KeyLen:      32,
		},
	}
}

// seedEncryptedTwoSegment seeds a compactable 2-segment lineage whose
// segments share one keyset, and returns the store.
func seedEncryptedTwoSegment(t *testing.T, now time.Time, enc *irbackup.ChainEncryption) *memStore {
	t.Helper()
	store := newMemStore()
	seedTwoSegmentLineageWithEncryption(t, store, now, enc, enc)
	return store
}

// compactedChainMissingItsRoot reproduces the exact post-Bug-214 on-disk
// state: a COMPACTED chain (segment 0 is now the merged segment under its
// own dir, so the lineage walks fine) whose chain-root manifest has been
// deleted the way the pre-fix sweep deleted it. Returns the post-compact
// catalog. Building it by compacting-then-deleting — rather than by
// hand-authoring a lineage — keeps the fixture honest: it is the shape a
// pre-fix binary actually leaves behind.
func compactedChainMissingItsRoot(t *testing.T, store *memStore, now time.Time) *lineage.Catalog {
	t.Helper()
	if _, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-gate" },
	}); err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-compact LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if err := store.Delete(context.Background(), lineage.ManifestFileName); err != nil {
		t.Fatalf("delete chain-root manifest: %v", err)
	}
	return cat
}

// TestCompactChain_EncryptedChain_KeepsChainRootManifest is the direct
// Bug-214 pin at the unit level: compacting an ENCRYPTED chain must leave
// the chain-root manifest — the ADR-0152 CEK-wrap identity and, for a
// passphrase chain, the only recorded copy of the Argon2id salt the
// restore side re-derives the KEK from — in place, with its key material
// intact. Deleting it is what made `compact` exit 0 on a chain that then
// refused at `unwrap chain cek` with zero rows.
func TestCompactChain_EncryptedChain_KeepsChainRootManifest(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("chain-salt-0123456789ab"), nil)
	store := seedEncryptedTwoSegment(t, now, enc)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-enc" },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsMerged != 1 {
		t.Fatalf("GroupsMerged = %d; want 1", res.GroupsMerged)
	}

	root, err := lineage.ReadRootManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadRootManifest: %v", err)
	}
	if root == nil {
		t.Fatal("Bug 214: compact deleted the chain-root manifest of an ENCRYPTED chain; every segment — including ones it never touched — is now unreadable")
	}
	if root.ChainEncryption == nil || root.ChainEncryption.Argon2id == nil {
		t.Fatalf("chain-root manifest survived but lost its key-derivation material: %+v", root.ChainEncryption)
	}
	if !bytes.Equal(root.ChainEncryption.Argon2id.Salt, enc.Argon2id.Salt) {
		t.Errorf("chain-root Argon2id salt = %q; want %q", root.ChainEncryption.Argon2id.Salt, enc.Argon2id.Salt)
	}
}

// TestCompactChain_DryRun_LeavesChainRootManifest pins that --dry-run is
// inert against the chain identity too — the plan-only path must not
// touch storage at all.
func TestCompactChain_DryRun_LeavesChainRootManifest(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("dry-run-salt-0123456789"), nil)
	store := seedEncryptedTwoSegment(t, now, enc)

	before, err := lineage.ReadRootManifest(context.Background(), store)
	if err != nil || before == nil {
		t.Fatalf("pre-dry-run ReadRootManifest: %v (nil=%v)", err, before == nil)
	}
	if _, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
		DryRun:      true,
		Now:         func() time.Time { return now.Add(10 * time.Hour) },
	}); err != nil {
		t.Fatalf("CompactChain --dry-run: %v", err)
	}
	after, err := lineage.ReadRootManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("post-dry-run ReadRootManifest: %v", err)
	}
	if after == nil {
		t.Fatal("--dry-run deleted the chain-root manifest; the plan-only path must not touch storage")
	}
	// The catalog is untouched too (two segments, no merged dir).
	cat, ok, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !ok || len(cat.Segments) != 2 {
		t.Errorf("--dry-run rewrote the catalog: ok=%v segments=%d; want 2", ok, len(cat.Segments))
	}
	if paths, _ := store.List(context.Background(), mergedSegmentDirPrefix); len(paths) != 0 {
		t.Errorf("--dry-run staged a merged segment dir: %v", paths)
	}
}

// TestVerifyChainReadable_PassphraseChain_MissingRoot_Refuses is the
// gate's core assertion: an encrypted passphrase chain whose root
// manifest is gone is UNRESTORABLE, and the gate must say so rather than
// let the run report success. This is the state Bug 214 shipped.
func TestVerifyChainReadable_PassphraseChain_MissingRoot_Refuses(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("missing-root-salt-01234"), nil)
	store := seedEncryptedTwoSegment(t, now, enc)

	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	// Sanity: the gate passes on the intact chain, so the refusal below
	// is attributable to the deleted identity and nothing else.
	if err := verifyChainReadable(context.Background(), store, cat, nil, "backup compact", "test"); err != nil {
		t.Fatalf("gate refused an INTACT encrypted chain: %v", err)
	}

	cat = compactedChainMissingItsRoot(t, store, now)
	err = verifyChainReadable(context.Background(), store, cat, nil, "backup compact", "test")
	if err == nil {
		t.Fatal("gate PASSED an encrypted chain with no chain-root manifest; that chain cannot be restored")
	}
	if !strings.Contains(err.Error(), "Argon2id salt") {
		t.Errorf("err = %q; want the message to name the Argon2id salt the restore side cannot re-derive the KEK without", err)
	}
}

// TestVerifyChainReadable_KMSChain_MissingRoot_Allowed pins the OTHER
// half of the class, so the gate does not grow into a false refusal: a
// KMS chain derives no key material from the chain-root manifest (the KEK
// lives in the KMS and every segment full carries the KEKRef), so a
// missing root costs it nothing and must not be refused.
func TestVerifyChainReadable_KMSChain_MissingRoot_Allowed(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	kms := &irbackup.ChainEncryption{
		Algorithm: crypto.AlgorithmAESGCM,
		Mode:      crypto.EncryptModePerChain,
		KEKMode:   crypto.KEKModeAWSKMS,
		KEKRef:    "arn:aws:kms:us-east-1:1234:key/abcd",
	}
	store := seedEncryptedTwoSegment(t, now, kms)
	cat := compactedChainMissingItsRoot(t, store, now)
	if err := verifyChainReadable(context.Background(), store, cat, nil, "backup compact", "test"); err != nil {
		t.Errorf("gate refused a KMS chain with no chain-root manifest; that chain restores fine: %v", err)
	}
}

// TestVerifyChainReadable_PlaintextChain_MissingRoot_Allowed pins the
// third cell: a plaintext chain restores without the root manifest (a
// chain compacted by a pre-fix binary is exactly this shape), so the gate
// WARNs rather than refusing chains that work.
func TestVerifyChainReadable_PlaintextChain_MissingRoot_Allowed(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)
	cat := compactedChainMissingItsRoot(t, store, now)
	if err := verifyChainReadable(context.Background(), store, cat, nil, "backup compact", "test"); err != nil {
		t.Errorf("gate refused a PLAINTEXT chain with no chain-root manifest: %v", err)
	}
}

// TestVerifyChainReadable_MissingSurvivorChunk_Refuses pins the presence
// leg (2026-08-22 invariant sweep): the structural walk reads only
// MANIFESTS, so before this leg existed the gate passed a chain whose
// DATA files a buggy sweep had eaten — the cmd-layer "never report
// success over a chain it just made unreadable" rested on delete-set
// geometry with nothing checking it. Both segment shapes are covered:
// a chunk under a rotation segment's sub-dir (resolved through the
// segment's prefixed store, exactly as restore resolves it) and a chunk
// at the chain root (Dir == ""). The intact-chain PASS control is the
// existing gate tests above, which run this leg against the seeded
// chunk files on every call — so an over-refusing mutant fails those,
// and a leg-skipping mutant fails this (both directions mutation-run).
func TestVerifyChainReadable_MissingSurvivorChunk_Refuses(t *testing.T) {
	for name, victim := range map[string]string{
		"segment-subdir chunk": "seg-1/chunks/_changes/seg1-incr1.jsonl.gz",
		"chain-root chunk":     "chunks/_changes/seg0-incr2.jsonl.gz",
	} {
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
			seedTwoSegmentLineage(t, store, now, time.Hour)
			cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
			if err != nil || !ok {
				t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
			}
			// Control first: the intact chain passes, so the refusal below is
			// attributable to the deleted chunk and nothing else.
			if err := verifyChainReadable(context.Background(), store, cat, nil, "backup prune", "test"); err != nil {
				t.Fatalf("gate refused an INTACT chain: %v", err)
			}
			if err := store.Delete(context.Background(), victim); err != nil {
				t.Fatalf("delete victim chunk: %v", err)
			}
			err = verifyChainReadable(context.Background(), store, cat, nil, "backup prune", "test")
			if err == nil {
				t.Fatal("gate PASSED a chain missing a survivor's chunk file; a restore of that chain fails at the chunk")
			}
			if !strings.Contains(err.Error(), "MISSING") || !strings.Contains(err.Error(), "jsonl.gz") {
				t.Errorf("err = %q; want the missing-chunk refusal naming the file", err)
			}
		})
	}
}

// TestVerifyChainReadable_DivergentRootSalt_Refuses pins the subtler
// unrestorable shape the identity check covers: the root manifest is
// present but records DIFFERENT key-derivation material than the chain's
// first restorable segment, so a read envelope built from the root would
// not unwrap that segment's CEK.
func TestVerifyChainReadable_DivergentRootSalt_Refuses(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := newMemStore()
	// Same keyset FINGERPRINT is not what is checked here — the salt is.
	rootEnc := gateChainEnc(t, []byte("salt-root-000000000000"), nil)
	segEnc := gateChainEnc(t, []byte("salt-seg1-111111111111"), nil)
	// Segment 0 (the chain root) carries rootEnc; segment 1 carries a
	// divergent salt. Compact refuses this at the keyset preflight; the
	// gate must refuse it too, on the read-path grounds.
	seedTwoSegmentLineageWithEncryption(t, store, now, rootEnc, segEnc)
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	cat.RestorableFromSegment = 1 // restore floor is the divergent segment

	err := verifyChainReadable(context.Background(), store, cat, nil, "backup compact", "test")
	if err == nil {
		t.Fatal("gate PASSED a chain whose root records a different Argon2id salt than its first restorable segment")
	}
	if !strings.Contains(err.Error(), "DIFFERENT Argon2id salt") {
		t.Errorf("err = %q; want the divergent-salt refusal", err)
	}
}

// TestVerifyChainReadable_UnwrapsChainCEK_WithEnvelope pins the
// BEHAVIOURAL half: given the operator's key, the gate performs the real
// chain-CEK unwrap — the exact operation Bug 214 broke — and refuses when
// the key does not open the chain.
func TestVerifyChainReadable_UnwrapsChainCEK_WithEnvelope(t *testing.T) {
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	env, err := crypto.NewPassphraseEnvelope("gate-pass", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	cek := make([]byte, 32)
	for i := range cek {
		cek[i] = byte(i)
	}

	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := newMemStore()
	// The wrap is bound to the owner manifest's identity (ADR-0152), so
	// seed first with a placeholder wrap, then re-wrap against the
	// manifest the seeder actually wrote.
	enc := gateChainEnc(t, params.Salt, nil)
	seedTwoSegmentLineageWithEncryption(t, store, now, enc, enc)

	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	seg0 := &cat.Segments[0]
	owner, err := lineage.ReadManifestAt(context.Background(), seg0.Store(store), seg0.FullManifestPath)
	if err != nil {
		t.Fatalf("read segment 0 full: %v", err)
	}
	wrapped, err := lineage.WrapChainCEK(env, cek, owner)
	if err != nil {
		t.Fatalf("WrapChainCEK: %v", err)
	}
	owner.ChainEncryption.WrappedCEK = wrapped
	if err := lineage.WriteManifestAt(context.Background(), seg0.Store(store), seg0.FullManifestPath, owner); err != nil {
		t.Fatalf("rewrite segment 0 full: %v", err)
	}

	if err := verifyChainReadable(context.Background(), store, cat, env, "backup compact", "test"); err != nil {
		t.Fatalf("gate refused a chain whose CEK unwraps under the supplied key: %v", err)
	}

	wrongEnv, err := crypto.NewPassphraseEnvelope("not-the-pass", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope(wrong): %v", err)
	}
	err = verifyChainReadable(context.Background(), store, cat, wrongEnv, "backup compact", "test")
	if err == nil {
		t.Fatal("gate PASSED with a key that does not unwrap the chain CEK")
	}
	if !strings.Contains(err.Error(), "does not unwrap") {
		t.Errorf("err = %q; want the unwrap refusal", err)
	}
}
