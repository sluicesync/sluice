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
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Roadmap item 95 unit pins — the prune twin of the Bug-214 compaction
// pins above. The load-bearing proof is the encrypted prune→restore
// integration round trip
// (internal/pipeline/backup_prune_encrypted_roundtrip_integration_test.go);
// these pin the pieces a memstore can hold honestly, and — the half that
// matters just as much — that the gate does NOT grow into a false refusal
// on the chains that restore fine without a root manifest.
//
// Every one of them drives the REAL [PruneChain], never the guard
// directly: a fix that is present but unreachable is the failure mode this
// project keeps re-learning.

// seedThreeSegmentPrunable seeds a 3-segment lineage (2 incrementals per
// segment) with the given per-segment chain encryption, so a prune can
// drop leading segments WHOLE — the shape that deletes the chain-root
// manifest, and the only restore-safe retention shape prune has (see the
// within-segment-trim note in the item-95 report).
func seedThreeSegmentPrunable(t *testing.T, now time.Time, enc *irbackup.ChainEncryption) *memStore {
	t.Helper()
	store := newMemStore()
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{
		encPerSegment: []*irbackup.ChainEncryption{enc, enc, enc},
	})
	return store
}

// prunedChainMissingItsRoot reproduces the on-disk state a PRE-FIX binary
// leaves behind: a chain whose leading segment has been pruned away AND
// whose chain-root manifest went with it. Built by pruning first and then
// deleting — rather than hand-authoring — so the fixture is the shape the
// bug actually produced, and so segment 0 of the surviving catalog is a
// `seg-*` dir (which is what makes the chain still WALK with no root
// manifest, exactly as it did in the field).
func prunedChainMissingItsRoot(t *testing.T, store *memStore) {
	t.Helper()
	if _, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 4}); err != nil {
		t.Fatalf("first PruneChain: %v", err)
	}
	if err := store.Delete(context.Background(), lineage.ManifestFileName); err != nil {
		t.Fatalf("delete chain-root manifest: %v", err)
	}
}

// TestPruneChain_EncryptedChain_KeepsChainRootManifest is the direct
// item-95 pin: pruning segment 0 of an ENCRYPTED chain retires the
// segment's data but KEEPS the chain-root manifest — the ADR-0152
// CEK-wrap identity and the only recorded copy of the Argon2id salt the
// restore side re-derives the KEK from. Deleting it is what made `prune`
// exit 0 on a chain that then refused at `unwrap chain cek`, on a
// retention schedule rather than as occasional maintenance.
func TestPruneChain_EncryptedChain_KeepsChainRootManifest(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("prune-chain-salt-01234"), nil)
	store := seedThreeSegmentPrunable(t, now, enc)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 4})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Fatalf("SegmentsDropped = %d; want 1 (nothing dropped ⇒ this pin is vacuous)", res.SegmentsDropped)
	}

	root, err := lineage.ReadRootManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadRootManifest: %v", err)
	}
	if root == nil {
		t.Fatal("item 95: prune deleted the chain-root manifest of an ENCRYPTED chain; every REMAINING segment is now unreadable")
	}
	if root.ChainEncryption == nil || root.ChainEncryption.Argon2id == nil {
		t.Fatalf("chain-root manifest survived but lost its key-derivation material: %+v", root.ChainEncryption)
	}
	if !bytes.Equal(root.ChainEncryption.Argon2id.Salt, enc.Argon2id.Salt) {
		t.Errorf("chain-root Argon2id salt = %q; want %q", root.ChainEncryption.Argon2id.Salt, enc.Argon2id.Salt)
	}
	// The retired segment's DATA is genuinely gone — keeping the identity
	// header must not turn prune into a no-op.
	if ex, _ := store.Exists(context.Background(), "manifests/incr-00001-seg0-1.json"); ex {
		t.Error("prune kept the retired root segment's incrementals; only the identity header should survive")
	}
}

// TestPruneChain_DryRun_LeavesChainRootManifest pins that `--dry-run` is
// inert against the chain identity: the plan-only path must not touch
// storage at all.
func TestPruneChain_DryRun_LeavesChainRootManifest(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("prune-dryrun-salt-0123"), nil)
	store := seedThreeSegmentPrunable(t, now, enc)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 4, DryRun: true})
	if err != nil {
		t.Fatalf("PruneChain --dry-run: %v", err)
	}
	if len(res.Pruned) == 0 {
		t.Fatal("--dry-run planned nothing; this pin would be vacuous")
	}
	if root, _ := lineage.ReadRootManifest(context.Background(), store); root == nil {
		t.Fatal("--dry-run deleted the chain-root manifest")
	}
	cat, ok, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !ok || len(cat.Segments) != 3 {
		t.Errorf("--dry-run rewrote the catalog: ok=%v segments=%d; want 3", ok, len(cat.Segments))
	}
}

// TestPruneChain_PassphraseChain_MissingRoot_RefusesCoded drives the
// gate's PRE-COMMIT leg through the real prune path on the state a
// pre-fix binary left behind, and pins both halves of the refusal
// contract: the machine-matchable code + exit 3 (item 96), and the fact
// that a refusal that fires before the commit deleted nothing.
func TestPruneChain_PassphraseChain_MissingRoot_RefusesCoded(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	enc := gateChainEnc(t, []byte("prune-missing-root-012"), nil)
	store := seedThreeSegmentPrunable(t, now, enc)
	prunedChainMissingItsRoot(t, store)

	before, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	_, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err == nil {
		t.Fatal("prune reported SUCCESS over an encrypted chain with no identity header — the whole defect")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("refusal is not %s (no script can branch on it): %v", sluicecode.CodeBackupChainUnreadable, err)
	}
	if coded.ExitCode() != sluicecode.ExitRefusal {
		t.Errorf("ExitCode() = %d; want %d", coded.ExitCode(), sluicecode.ExitRefusal)
	}
	if !strings.Contains(err.Error(), "Argon2id salt") {
		t.Errorf("err = %q; want the message to name the salt the restore side cannot re-derive the KEK without", err)
	}
	if !strings.Contains(err.Error(), "NO files were removed") {
		t.Errorf("err = %q; want the pre-sweep decoration that tells the operator nothing was destroyed", err)
	}
	after, ok2, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !ok2 || len(after.Segments) != len(before.Segments) {
		t.Errorf("refused prune mutated the catalog: %d segments, want %d", len(after.Segments), len(before.Segments))
	}
}

// TestPruneChain_KMSChain_MissingRoot_StillPrunes is the must-NOT-refuse
// half, and it carries equal weight: a KMS chain derives no key material
// from the chain-root manifest (the KEK lives in the KMS and every segment
// full carries the KEKRef), so a missing root costs it nothing. An
// over-refusal here would break working retention.
func TestPruneChain_KMSChain_MissingRoot_StillPrunes(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	kms := &irbackup.ChainEncryption{
		Algorithm: crypto.AlgorithmAESGCM,
		Mode:      crypto.EncryptModePerChain,
		KEKMode:   crypto.KEKModeAWSKMS,
		KEKRef:    "arn:aws:kms:us-east-1:1234:key/abcd",
	}
	store := seedThreeSegmentPrunable(t, now, kms)
	prunedChainMissingItsRoot(t, store)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("prune refused a KMS chain with no chain-root manifest; that chain restores fine: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("SegmentsDropped = %d; want 1", res.SegmentsDropped)
	}
}

// TestPruneChain_PlaintextChain_MissingRoot_StillPrunes is the third
// must-NOT-refuse cell: a plaintext chain restores without the root
// manifest (nothing derives a key from it), so the gate WARNs rather than
// refusing a chain that works.
func TestPruneChain_PlaintextChain_MissingRoot_StillPrunes(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := seedThreeSegmentPrunable(t, now, nil)
	prunedChainMissingItsRoot(t, store)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("prune refused a PLAINTEXT chain with no chain-root manifest: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("SegmentsDropped = %d; want 1", res.SegmentsDropped)
	}
}

// TestPruneChain_PlaintextChain_KeepsChainRootManifest pins that the keep
// is UNCONDITIONAL, not "when the chain is encrypted". A keep-it-when rule
// is one future encryption mode, one future reader, or one future `if`
// away from the same outage, and nobody would notice until a restore —
// which is the reasoning Bug 214 was made of.
func TestPruneChain_PlaintextChain_KeepsChainRootManifest(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := seedThreeSegmentPrunable(t, now, nil)

	if _, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 4}); err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); !ex {
		t.Error("prune deleted a PLAINTEXT chain's root manifest; the keep must not be conditional on encryption")
	}
}
