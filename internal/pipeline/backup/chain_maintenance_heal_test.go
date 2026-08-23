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
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
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
