// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for healStaleLineageSignatures — the resignIfSigned crash window
// (batch C, 2026-08-23). Compact and prune commit their catalog restructure
// FIRST and re-sign SECOND; a crash between the two leaves a restructured
// chain under stale signatures, and the re-run the SIGNATURE-MISSING remedy
// prescribes ("re-run the maintenance step with the chain's key material")
// used to return through a no-op door BEFORE resignIfSigned — never healing
// anything. These tests drive every no-op door with the crash-shaped store
// state and pin: the heal fires exactly there, only with a signer, never on
// a dry-run, and never churns .sig objects on a healthy chain.

package backup

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// sigCountingStore wraps a Store and counts Puts of detached `.sig`
// objects — the observable for "the heal (or anything else) re-signed".
// HMAC signatures are deterministic, so byte-comparing sig files cannot
// distinguish "not rewritten" from "rewritten identically"; counting the
// writes can.
type sigCountingStore struct {
	irbackup.Store
	sigPuts int
}

func (s *sigCountingStore) Put(ctx context.Context, path string, r io.Reader) error {
	if strings.HasSuffix(path, ".sig") {
		s.sigPuts++
	}
	return s.Store.Put(ctx, path, r)
}

// mustSigner rebuilds the chain's signer from its envelope.
func mustSigner(t *testing.T, env crypto.EnvelopeEncryption) *lineage.Signer {
	t.Helper()
	signer, ok, err := lineage.NewSigner(env)
	if err != nil || !ok {
		t.Fatalf("new signer: ok=%v err=%v", ok, err)
	}
	return signer
}

// staleifyCatalog reproduces the crash window's on-disk shape: the catalog
// is restructured (its newest catalogued incremental dropped — exactly what
// a prune commit does) WITHOUT the re-sign that a completed run would have
// performed. lineage.json.sig now signs a catalog that no longer exists.
func staleifyCatalog(t *testing.T, store irbackup.Store) {
	t.Helper()
	ctx := context.Background()
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("load catalog: ok=%v err=%v", ok, err)
	}
	seg := &cat.Segments[len(cat.Segments)-1]
	if len(seg.Incrementals) == 0 {
		t.Fatal("staleifyCatalog needs a segment with at least one incremental")
	}
	seg.Incrementals = seg.Incrementals[:len(seg.Incrementals)-1]
	if err := lineage.WriteLineageCatalog(ctx, store, cat); err != nil {
		t.Fatalf("write restructured catalog: %v", err)
	}
}

// requireLineageSigStale asserts the chain is in the pre-heal state, so a
// later "healed" assertion cannot pass vacuously.
func requireLineageSigStale(t *testing.T, store irbackup.Store, signer *lineage.Signer) {
	t.Helper()
	ctx := context.Background()
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("load catalog: ok=%v err=%v", ok, err)
	}
	if err := lineage.VerifyLineage(ctx, store, cat, signer); err == nil {
		t.Fatal("setup failed: the lineage signature still verifies — the staleify did not reproduce the crash shape")
	}
}

// requireLineageHealed asserts the whole signed surface verifies: the
// lineage catalog signature AND every catalogued manifest at its flat
// position (via the real restore-side verifier, strict mode).
func requireLineageHealed(t *testing.T, store irbackup.Store, env crypto.EnvelopeEncryption, signer *lineage.Signer) {
	t.Helper()
	ctx := context.Background()
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("load catalog: ok=%v err=%v", ok, err)
	}
	if err := lineage.VerifyLineage(ctx, store, cat, signer); err != nil {
		t.Fatalf("lineage signature still stale after the no-op maintenance run: %v", err)
	}
	links, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, true); err != nil {
		t.Fatalf("chain signatures do not verify after the heal: %v", err)
	}
}

// addSignedIncremental appends one more incremental to a buildSignedChain
// store (mirroring its incr-0001 shape), catalogues it, and re-signs the
// whole lineage so the chain is longer but still fully verified.
func addSignedIncremental(t *testing.T, store irbackup.Store, signer *lineage.Signer, backupID, parentID, path string) {
	t.Helper()
	ctx := context.Background()
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.FormatVersionSignedManifest,
		CreatedAt:      time.Date(2026, 7, 9, 2, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Kind:           irbackup.BackupKindIncremental,
		BackupID:       backupID,
		ParentBackupID: parentID,
		SchemaHash:     "fh",
		ChangeChunks: []*irbackup.ChunkInfo{
			{File: "chunks/_changes/c-9.jsonl.gz", RowCount: 1, SHA256: "cs9"},
		},
	}
	if err := lineage.WriteManifestAt(ctx, store, path, incr); err != nil {
		t.Fatalf("write incremental %s: %v", backupID, err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, path, blobcodec.DefaultCodec)
	if err := lineage.ResignLineage(ctx, store, signer); err != nil {
		t.Fatalf("re-sign after adding %s: %v", backupID, err)
	}
}

// TestMaintenanceNoOp_HealsStaleLineageSignatures drives the crash-shaped
// store through every maintenance no-op door with the signing key supplied
// and asserts each one heals. The door roster is stated, not implied:
// compact has two no-op returns (fewer-than-2-segments, no-merge-groups)
// and prune has two (keep >= count, keep-duration-all-newer); the dry-run
// returns are exempt by the read-only contract and pinned separately below.
func TestMaintenanceNoOp_HealsStaleLineageSignatures(t *testing.T) {
	ctx := context.Background()

	t.Run("compact door: fewer than 2 retained segments", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		staleifyCatalog(t, store)
		requireLineageSigStale(t, store, signer)

		res, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, Signer: signer})
		if err != nil {
			t.Fatalf("CompactChain: %v", err)
		}
		if res.GroupsMerged != 0 {
			t.Fatalf("GroupsMerged = %d; want 0 (the run must be a no-op — the heal re-signs, it never restructures)", res.GroupsMerged)
		}
		requireLineageHealed(t, store, env, signer)
	})

	t.Run("compact door: no merge groups within the window", func(t *testing.T) {
		store := newMemStore()
		base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
		seedTwoSegmentLineage(t, store, base, 48*time.Hour)
		kek := make([]byte, crypto.KEKLen)
		for i := range kek {
			kek[i] = 0x5a
		}
		env := sigFakeEnv{kek: kek}
		signer := mustSigner(t, env)
		if err := lineage.ResignLineage(ctx, store, signer); err != nil {
			t.Fatalf("sign seeded chain: %v", err)
		}
		staleifyCatalog(t, store)
		requireLineageSigStale(t, store, signer)

		res, err := CompactChain(ctx, store, CompactOpts{
			MergeWindow: time.Hour, // far below the 48h gap → two size-1 groups → no-op
			Now:         func() time.Time { return base.Add(100 * time.Hour) },
			Signer:      signer,
		})
		if err != nil {
			t.Fatalf("CompactChain: %v", err)
		}
		if res.GroupsMerged != 0 {
			t.Fatalf("GroupsMerged = %d; want 0", res.GroupsMerged)
		}
		requireLineageHealed(t, store, env, signer)
	})

	t.Run("prune door: keep >= incremental count", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		staleifyCatalog(t, store)
		requireLineageSigStale(t, store, signer)

		if _, err := PruneChain(ctx, store, PruneOpts{KeepIncrementals: 5, Signer: signer}); err != nil {
			t.Fatalf("PruneChain: %v", err)
		}
		requireLineageHealed(t, store, env, signer)
	})

	t.Run("prune door: all incrementals newer than keep-duration", func(t *testing.T) {
		// This door needs a catalogued incremental LEFT after the staleify
		// (dropN == 0 is only reachable with flat non-empty under
		// KeepDuration), so build the chain with TWO incrementals and let
		// the staleify drop the newest — the exact prune-crash shape.
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		addSignedIncremental(t, store, signer, "incr0002", "incr0001", "manifests/incr-0002.json")
		staleifyCatalog(t, store)
		requireLineageSigStale(t, store, signer)

		// A century-long retention keeps every 2026-dated incremental →
		// dropN == 0 → the keep-duration no-op door.
		if _, err := PruneChain(ctx, store, PruneOpts{KeepDuration: 100 * 365 * 24 * time.Hour, Signer: signer}); err != nil {
			t.Fatalf("PruneChain: %v", err)
		}
		requireLineageHealed(t, store, env, signer)
	})
}

// TestMaintenanceNoOp_HealBoundaries pins the three deliberate boundaries:
// keyless runs skip the heal silently (today's exit-0 keyless no-op stays),
// dry-runs never write, and a healthy signed chain's no-op run never churns
// a single .sig object (the heal verifies first).
func TestMaintenanceNoOp_HealBoundaries(t *testing.T) {
	ctx := context.Background()

	t.Run("no signer: no heal, no error, still stale", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		staleifyCatalog(t, store)

		cs := &sigCountingStore{Store: store}
		if _, err := CompactChain(ctx, cs, CompactOpts{MergeWindow: time.Hour}); err != nil {
			t.Fatalf("keyless no-op CompactChain: %v", err)
		}
		if cs.sigPuts != 0 {
			t.Fatalf("keyless run wrote %d .sig object(s); want 0", cs.sigPuts)
		}
		requireLineageSigStale(t, store, signer)
	})

	t.Run("dry-run: no heal writes even with the key", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		staleifyCatalog(t, store)

		cs := &sigCountingStore{Store: store}
		if _, err := CompactChain(ctx, cs, CompactOpts{MergeWindow: time.Hour, DryRun: true, Signer: signer}); err != nil {
			t.Fatalf("dry-run CompactChain: %v", err)
		}
		if cs.sigPuts != 0 {
			t.Fatalf("dry-run wrote %d .sig object(s); want 0 (a dry-run must not write)", cs.sigPuts)
		}
		requireLineageSigStale(t, store, signer)
	})

	t.Run("healthy chain: verify-first, zero sig churn", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)

		cs := &sigCountingStore{Store: store}
		if _, err := PruneChain(ctx, cs, PruneOpts{KeepIncrementals: 5, Signer: signer}); err != nil {
			t.Fatalf("PruneChain: %v", err)
		}
		if cs.sigPuts != 0 {
			t.Fatalf("no-op prune of a HEALTHY signed chain wrote %d .sig object(s); want 0 (the heal must verify before it re-signs)", cs.sigPuts)
		}
		requireLineageHealed(t, store, env, signer)
	})
}

// wrongKeySigner builds a signing-capable HMAC-off-KEK signer from a KEK
// that is NOT buildSignedChain's — the "mistyped --encrypt passphrase"
// shape. A different KEK derives a different manifest-HMAC key, so its
// KeyID fingerprint differs from the chain's recorded one.
func wrongKeySigner(t *testing.T) *lineage.Signer {
	t.Helper()
	kek := make([]byte, crypto.KEKLen)
	for i := range kek {
		kek[i] = 0xa7 // buildSignedChain uses 0x5a
	}
	return mustSigner(t, sigFakeEnv{kek: kek})
}

// requireWrongKeyHealRefusal asserts the SEC-1 refusal shape: a loud,
// coded error naming the key mismatch — never a silent re-sign.
func requireWrongKeyHealRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("no-op maintenance with the WRONG key succeeded; want the wrong-key heal refusal (a silent success here is exactly the SEC-1 re-key footgun)")
	}
	if !strings.Contains(err.Error(), "not this chain's signing key") {
		t.Fatalf("refusal does not name the key mismatch: %v", err)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupEncryptionMismatch {
		t.Fatalf("want %s coded refusal; got %v", sluicecode.CodeBackupEncryptionMismatch, err)
	}
}

// TestMaintenanceNoOp_HealRefusesWrongKey is the SEC-1 cell the heal test
// family lacked (audit 2026-08-26): the heal must distinguish
// crash-stale-same-key (recorded KeyID == supplied KeyID → heals; pinned
// by TestMaintenanceNoOp_HealsStaleLineageSignatures above) from
// wrong-key (fingerprints differ → refuses, zero .sig writes). Pre-fix, a
// mistyped --encrypt passphrase on a no-op compact of a healthy signed
// chain silently re-keyed every .sig at exit 0, after which the chain
// reported SIGNATURE-INVALID under the CORRECT passphrase.
//
// The guard lives in healStaleLineageSignatures itself — the single
// chokepoint every no-op door (compact ×2 via compactNoOpReturn, prune ×2
// via pruneNoOpReturn) funnels through — so one door per maintenance verb
// exercises every door's path through the guard.
func TestMaintenanceNoOp_HealRefusesWrongKey(t *testing.T) {
	ctx := context.Background()

	t.Run("compact no-op, healthy chain: refuses, zero .sig writes, chain intact under the correct key", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		correct := mustSigner(t, env)

		cs := &sigCountingStore{Store: store}
		_, err := CompactChain(ctx, cs, CompactOpts{MergeWindow: time.Hour, Signer: wrongKeySigner(t)})
		requireWrongKeyHealRefusal(t, err)
		if cs.sigPuts != 0 {
			t.Fatalf("wrong-key run wrote %d .sig object(s); want 0 (the refusal must precede any re-sign)", cs.sigPuts)
		}
		requireLineageHealed(t, store, env, correct) // nothing was re-keyed
	})

	t.Run("prune no-op, healthy chain: refuses, zero .sig writes, chain intact under the correct key", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		correct := mustSigner(t, env)

		cs := &sigCountingStore{Store: store}
		_, err := PruneChain(ctx, cs, PruneOpts{KeepIncrementals: 5, Signer: wrongKeySigner(t)})
		requireWrongKeyHealRefusal(t, err)
		if cs.sigPuts != 0 {
			t.Fatalf("wrong-key run wrote %d .sig object(s); want 0 (the refusal must precede any re-sign)", cs.sigPuts)
		}
		requireLineageHealed(t, store, env, correct)
	})

	t.Run("wrong key leaves no heal evidence artifacts (nothing healed)", func(t *testing.T) {
		store, _, _ := buildSignedChain(t)
		_, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, Signer: wrongKeySigner(t)})
		requireWrongKeyHealRefusal(t, err)
		requireNoHealEvidence(t, store)
	})

	t.Run("crash-stale chain + wrong key: still refuses (stale is not this key's to heal)", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		correct := mustSigner(t, env)
		staleifyCatalog(t, store)
		requireLineageSigStale(t, store, correct)

		cs := &sigCountingStore{Store: store}
		_, err := CompactChain(ctx, cs, CompactOpts{MergeWindow: time.Hour, Signer: wrongKeySigner(t)})
		requireWrongKeyHealRefusal(t, err)
		if cs.sigPuts != 0 {
			t.Fatalf("wrong-key run wrote %d .sig object(s); want 0", cs.sigPuts)
		}
		// Still crash-stale — and still healable by the CORRECT key,
		// exactly as the published remedy prescribes.
		requireLineageSigStale(t, store, correct)
		if _, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, Signer: correct}); err != nil {
			t.Fatalf("correct-key heal after the wrong-key refusal: %v", err)
		}
		requireLineageHealed(t, store, env, correct)
	})
}

// storeReadAll reads a store object's raw bytes.
func storeReadAll(t *testing.T, store irbackup.Store, path string) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), path)
	if err != nil {
		t.Fatalf("get %q: %v", path, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	return body
}

// preservedSigPaths lists the pre-heal preserved lineage.json.sig copies.
func preservedSigPaths(t *testing.T, store irbackup.Store) []string {
	t.Helper()
	paths, err := store.List(context.Background(), lineage.PreHealLineageSigPrefix)
	if err != nil {
		t.Fatalf("list pre-heal sigs: %v", err)
	}
	return paths
}

// requireNoHealEvidence asserts neither heal artifact exists — the
// boundary paths (keyless, dry-run, healthy, wrong-key) must not write
// evidence for a heal that never ran.
func requireNoHealEvidence(t *testing.T, store irbackup.Store) {
	t.Helper()
	if paths := preservedSigPaths(t, store); len(paths) != 0 {
		t.Fatalf("found %d preserved pre-heal signature(s) %v; want none (no heal ran)", len(paths), paths)
	}
	recs, err := lineage.ReadHealRecords(context.Background(), store)
	if err != nil {
		t.Fatalf("read heal records: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("found %d heal record(s); want none (no heal ran)", len(recs))
	}
}

// TestMaintenanceHeal_PreservesForensicEvidence is the audit 2026-08-27 A3
// pin: a same-key heal must not DESTROY the evidence of what it healed.
// Before this, ResignLineage overwrote the non-verifying lineage.json.sig
// (the only artifact distinguishing crash-stale from
// tampered-with-sigs-left-in-place) and the transient WARN was the sole
// record — after the heal, `backup verify` reported all-valid forever.
// Now the heal preserves the pre-heal signature verbatim
// (lineage.json.sig.pre-heal-<ts>) and appends a durable
// maintenance-heal.log record BEFORE re-signing, and both survive
// repeated heals (append, never overwrite).
func TestMaintenanceHeal_PreservesForensicEvidence(t *testing.T) {
	ctx := context.Background()

	store, env, _ := buildSignedChain(t)
	signer := mustSigner(t, env)
	staleifyCatalog(t, store)
	requireLineageSigStale(t, store, signer)
	preHealSig := storeReadAll(t, store, lineage.LineageSigFileName)

	if _, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, Signer: signer}); err != nil {
		t.Fatalf("CompactChain heal run: %v", err)
	}
	requireLineageHealed(t, store, env, signer)

	preserved := preservedSigPaths(t, store)
	if len(preserved) != 1 {
		t.Fatalf("preserved pre-heal signatures = %v; want exactly 1", preserved)
	}
	if got := storeReadAll(t, store, preserved[0]); !bytes.Equal(got, preHealSig) {
		t.Errorf("preserved pre-heal signature bytes differ from the actual pre-heal lineage.json.sig — the evidence must survive VERBATIM")
	}
	// The heal must have actually replaced the live signature (the
	// preserved copy is evidence, not the current state).
	if live := storeReadAll(t, store, lineage.LineageSigFileName); bytes.Equal(live, preHealSig) {
		t.Error("live lineage.json.sig is byte-identical to the pre-heal one — the heal did not re-sign")
	}

	recs, err := lineage.ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("read heal records: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("heal records = %d; want exactly 1", len(recs))
	}
	rec := recs[0]
	if rec.Operation != "backup compact" {
		t.Errorf("record.Operation = %q; want %q", rec.Operation, "backup compact")
	}
	if rec.KeyID != signer.KeyID {
		t.Errorf("record.KeyID = %q; want the signing key's %q", rec.KeyID, signer.KeyID)
	}
	if rec.VerifyFailure == "" {
		t.Error("record.VerifyFailure is empty; want the verification error that triggered the heal")
	}
	if rec.PreservedSig != preserved[0] {
		t.Errorf("record.PreservedSig = %q; want the preserved copy's path %q", rec.PreservedSig, preserved[0])
	}
	if rec.HealedAt.IsZero() {
		t.Error("record.HealedAt is zero")
	}

	// SECOND heal (this time through a prune door): the log APPENDS and a
	// second preserved copy lands — repeated heals never overwrite prior
	// evidence. The first staleify consumed the chain's only catalogued
	// incremental, so re-grow the chain before staleifying again.
	addSignedIncremental(t, store, signer, "incr0002", "full0001", "manifests/incr-0002.json")
	staleifyCatalog(t, store)
	requireLineageSigStale(t, store, signer)
	if _, err := PruneChain(ctx, store, PruneOpts{KeepIncrementals: 5, Signer: signer}); err != nil {
		t.Fatalf("PruneChain second heal run: %v", err)
	}
	if got := preservedSigPaths(t, store); len(got) != 2 {
		t.Fatalf("preserved pre-heal signatures after second heal = %v; want 2", got)
	}
	recs, err = lineage.ReadHealRecords(ctx, store)
	if err != nil {
		t.Fatalf("read heal records after second heal: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("heal records after second heal = %d; want 2 (append, never overwrite)", len(recs))
	}
	if recs[1].Operation != "prune" {
		t.Errorf("second record.Operation = %q; want %q", recs[1].Operation, "prune")
	}
}

// TestMaintenanceHeal_BoundariesLeaveNoEvidence pins that the three no-heal
// boundaries (keyless, dry-run, healthy chain) write neither evidence
// artifact — a heal record asserting a heal that never ran would be its own
// kind of false evidence.
func TestMaintenanceHeal_BoundariesLeaveNoEvidence(t *testing.T) {
	ctx := context.Background()

	t.Run("keyless stale run", func(t *testing.T) {
		store, _, _ := buildSignedChain(t)
		staleifyCatalog(t, store)
		if _, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour}); err != nil {
			t.Fatalf("keyless CompactChain: %v", err)
		}
		requireNoHealEvidence(t, store)
	})

	t.Run("dry-run stale run with the key", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		staleifyCatalog(t, store)
		if _, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, DryRun: true, Signer: mustSigner(t, env)}); err != nil {
			t.Fatalf("dry-run CompactChain: %v", err)
		}
		requireNoHealEvidence(t, store)
	})

	t.Run("healthy chain no-op run", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		if _, err := PruneChain(ctx, store, PruneOpts{KeepIncrementals: 5, Signer: mustSigner(t, env)}); err != nil {
			t.Fatalf("healthy PruneChain: %v", err)
		}
		requireNoHealEvidence(t, store)
	})
}

// TestBackupVerify_SurfacesMaintenanceHeals pins the verify-side half of
// A3: after a heal, every signature legitimately verifies, so `backup
// verify`'s heal-provenance line is the ONLY signal that the signatures
// were regenerated. Informational, never a failure — and absent on a
// never-healed chain.
func TestBackupVerify_SurfacesMaintenanceHeals(t *testing.T) {
	ctx := context.Background()

	t.Run("healed chain: informational line, zero failures", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		signer := mustSigner(t, env)
		staleifyCatalog(t, store)
		if _, err := CompactChain(ctx, store, CompactOpts{MergeWindow: time.Hour, Signer: signer}); err != nil {
			t.Fatalf("CompactChain heal run: %v", err)
		}
		records, err := lineage.ListAllSegmentManifests(ctx, store)
		if err != nil {
			t.Fatalf("list manifests: %v", err)
		}

		buf := captureMaintenanceSlog(t)
		failed := verifyBackupSignatures(ctx, store, records, VerifyOptions{Envelope: env})
		if failed != 0 {
			t.Fatalf("verifyBackupSignatures on a healed chain = %d failure(s); want 0 (the heal record is informational)\nlog:\n%s", failed, buf.String())
		}
		out := buf.String()
		for _, want := range []string{"regenerated by a no-op maintenance heal", "backup compact", "pre-heal"} {
			if !strings.Contains(out, want) {
				t.Errorf("verify output missing %q — the heal record's presence was not surfaced\nlog:\n%s", want, out)
			}
		}
	})

	t.Run("never-healed chain: no heal line", func(t *testing.T) {
		store, env, _ := buildSignedChain(t)
		records, err := lineage.ListAllSegmentManifests(ctx, store)
		if err != nil {
			t.Fatalf("list manifests: %v", err)
		}
		buf := captureMaintenanceSlog(t)
		if failed := verifyBackupSignatures(ctx, store, records, VerifyOptions{Envelope: env}); failed != 0 {
			t.Fatalf("verifyBackupSignatures on a healthy chain = %d failure(s); want 0", failed)
		}
		if strings.Contains(buf.String(), "maintenance heal") {
			t.Errorf("verify output claims a maintenance heal on a never-healed chain:\n%s", buf.String())
		}
	})
}

// TestMaintenanceHeal_MalformedLogRefusesLoudly pins ReadHealRecords'
// refuse-loudly contract (VF review 2026-08-27): a torn or garbage line
// in maintenance-heal.log must surface as the "heal provenance unknown"
// WARN in backup verify — never a silent partial read.
func TestMaintenanceHeal_MalformedLogRefusesLoudly(t *testing.T) {
	ctx := context.Background()
	store, env, _ := buildSignedChain(t)
	if err := store.Put(ctx, lineage.MaintenanceHealLogFileName,
		strings.NewReader("{\"healed_at\":\"2026-08-27T00:00:00Z\"\nnot json at all\xff")); err != nil {
		t.Fatalf("plant malformed heal log: %v", err)
	}
	if _, err := lineage.ReadHealRecords(ctx, store); err == nil {
		t.Fatal("ReadHealRecords accepted a malformed log; want a loud decode error")
	}
	records, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	buf := captureMaintenanceSlog(t)
	if failed := verifyBackupSignatures(ctx, store, records, VerifyOptions{Envelope: env}); failed != 0 {
		t.Fatalf("verifyBackupSignatures = %d failure(s); want 0 (the heal log is informational)", failed)
	}
	if !strings.Contains(buf.String(), "heal provenance unknown") {
		t.Errorf("verify output missing the 'heal provenance unknown' WARN on a malformed heal log:\n%s", buf.String())
	}
}
