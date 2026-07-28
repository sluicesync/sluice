//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 95 — the encrypted `backup prune` → `restore` round trip,
// which had never existed either.
//
// Item 94 (Bug 214) was `backup compact` deleting the chain-root
// `manifest.json` — the ADR-0152 CEK-wrap identity and the only recorded
// copy of a passphrase chain's Argon2id salt. `pruneWholeSegments` deleted
// the SAME file through `seg.Store(store)` whenever retention dropped the
// ROOT segment, and its reach is larger: compaction is occasional
// maintenance, prune is scheduled retention.
//
// So this file restores, for the same reason the compaction one does:
// asserting prune's return value would reproduce the original mistake,
// because the defect IS an operation reporting success over an unreadable
// artifact. The matrix is the two encryption modes that fail DIFFERENTLY
// plus the plaintext control that was never affected — and it deliberately
// prunes at SEGMENT granularity (see [pruneKeepDroppingRootSegmentWhole]),
// which is the shape that deletes the root manifest.
//
// The restore-side envelope is [bug214ReadEnvelope]: the production CLI's
// buildReadEnvelope, silent fresh-salt fallback included. Reusing the
// backup-side envelope object would go green straight over the bug.

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pruneKeepDroppingRootSegmentWhole returns the --keep-incrementals count
// that makes prune drop segment 0 WHOLE and trim nothing inside the new
// floor segment: keep exactly the incrementals held by segments 1..N.
//
// Segment granularity is deliberate. Prune's OTHER shape — trimming
// leading incrementals INSIDE the floor segment — leaves the segment full
// anchored at S with its first surviving incremental starting at some
// S' > S, which is a genuine event gap the restore path refuses on
// purpose; that is a separate, pre-existing defect (see the report), not
// the one this file pins.
func pruneKeepDroppingRootSegmentWhole(t *testing.T, store *blobcodec.LocalStore) int {
	t.Helper()
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) < 2 {
		t.Fatalf("chain has %d segments; need >= 2 for a whole-segment prune", len(cat.Segments))
	}
	keep := 0
	for _, seg := range cat.Segments[1:] {
		keep += len(seg.Incrementals)
	}
	if keep == 0 {
		t.Fatal("segments 1..N hold no incrementals; a keep-0 prune would not exercise the whole-segment path")
	}
	if len(cat.Segments[0].Incrementals) == 0 {
		t.Fatal("segment 0 holds no incrementals; nothing would be dropped")
	}
	return keep
}

// TestItem95_EncryptedPrune_RestoresRowExact is THE pin: an encrypted
// multi-segment chain whose ROOT segment is pruned away still RESTORES
// row-exact, in per-chain AND per-chunk mode, with the plaintext control
// alongside.
//
// Pre-fix, the two encrypted legs both fail — per-chain at `unwrap chain
// cek`, per-chunk on the first chunk's CEK resolve — while the plaintext
// leg passes, which is precisely the asymmetry that kept Bug 214's twin
// hidden here.
func TestItem95_EncryptedPrune_RestoresRowExact(t *testing.T) {
	const passphrase = "prune-round-trip-passphrase"
	legs := []struct {
		name string
		mode string // "" = plaintext control
	}{
		{"per-chain", crypto.EncryptModePerChain},
		{"per-chunk", crypto.EncryptModePerChunk},
		{"plaintext-control", ""},
	}
	for _, leg := range legs {
		t.Run(leg.name, func(t *testing.T) {
			sourceDSN, targetDSN, store := bug214SeedEncryptedRotatedChain(t, leg.mode, passphrase)

			pre, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
			if err != nil || !ok {
				t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
			}
			if len(pre.Segments) < 2 {
				t.Fatalf("pre-prune segments = %d; want >= 2 (rotation didn't materialize)", len(pre.Segments))
			}
			oracle := bug214Read(t, sourceDSN)
			if oracle.n < 2 {
				t.Fatalf("source oracle has %d rows; the churn produced nothing", oracle.n)
			}

			// Prune runs the way the CLI runs it for an operator who did NOT
			// pass --encrypt (the common case for a retention cron, and the
			// one that shipped the bug): no envelope, so the readability gate
			// has only the chain's identity to check.
			res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{
				KeepIncrementals: pruneKeepDroppingRootSegmentWhole(t, store),
			})
			if err != nil {
				t.Fatalf("PruneChain: %v", err)
			}
			if res.SegmentsDropped < 1 {
				t.Fatalf("SegmentsDropped = %d; want >= 1 (no whole segment dropped, so this pin would be vacuous)", res.SegmentsDropped)
			}
			post, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
			if len(post.Segments) >= len(pre.Segments) {
				t.Fatalf("post-prune segments = %d; want < %d", len(post.Segments), len(pre.Segments))
			}
			// The load-bearing artifact, asserted directly as well as through
			// the restore below: prune retired segment 0 and KEPT the chain's
			// identity header.
			if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); !ex {
				t.Error("prune deleted the chain-root manifest (item 95): the chain's identity is gone")
			}

			// THE assertion: restore the pruned chain into a fresh target
			// through the production read path.
			var envRestore crypto.EnvelopeEncryption
			if leg.mode != "" {
				envRestore = bug214ReadEnvelope(t, store, passphrase)
			}
			eng, _ := engines.Get("postgres")
			if err := (&backup.ChainRestore{
				Target: eng, TargetDSN: targetDSN, Store: store, Envelope: envRestore,
			}).Run(context.Background()); err != nil {
				t.Fatalf("item 95: ChainRestore of the PRUNED %s chain failed — prune reported success over an unreadable chain: %v", leg.name, err)
			}
			got := bug214Read(t, targetDSN)
			if got != oracle {
				t.Fatalf("restored pruned %s chain != source oracle: got %+v want %+v", leg.name, got, oracle)
			}
			t.Logf("item-95 %s pin PROVEN: %d-segment chain pruned to %d (root segment retired) and restored row-exact (%+v)",
				leg.name, len(pre.Segments), len(post.Segments), oracle)
		})
	}
}

// TestItem95_EncryptedPrune_DryRunIsInert pins that `--dry-run` does not
// touch the chain identity (it never did — and it must not start to). A
// dry run followed by a real restore proves the chain is exactly as
// restorable as it was.
func TestItem95_EncryptedPrune_DryRunIsInert(t *testing.T) {
	const passphrase = "prune-dry-run-inert-passphrase"
	sourceDSN, targetDSN, store := bug214SeedEncryptedRotatedChain(t, crypto.EncryptModePerChain, passphrase)

	pre, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	oracle := bug214Read(t, sourceDSN)

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{
		KeepIncrementals: pruneKeepDroppingRootSegmentWhole(t, store),
		DryRun:           true,
	})
	if err != nil {
		t.Fatalf("PruneChain --dry-run: %v", err)
	}
	if len(res.Pruned) == 0 {
		t.Fatal("--dry-run planned nothing; this pin would be vacuous")
	}

	post, ok, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !ok || len(post.Segments) != len(pre.Segments) {
		t.Errorf("--dry-run rewrote the catalog: %d segments, want %d", len(post.Segments), len(pre.Segments))
	}
	if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); !ex {
		t.Fatal("--dry-run deleted the chain-root manifest")
	}

	eng, _ := engines.Get("postgres")
	if err := (&backup.ChainRestore{
		Target: eng, TargetDSN: targetDSN, Store: store,
		Envelope: bug214ReadEnvelope(t, store, passphrase),
	}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore after --dry-run: %v", err)
	}
	if got := bug214Read(t, targetDSN); got != oracle {
		t.Errorf("restore after --dry-run != source oracle: got %+v want %+v", got, oracle)
	}
}

// TestItem95_PruneRefusesLoudlyOverAnUnreadableChain drives the gate's
// PRE-COMMIT leg through the real prune path and pins both halves of the
// refusal contract: the coded, exit-3 class (item 96), and the fact that
// the refusal left the catalog untouched.
//
// The chain is put in exactly the state Bug 214 left one in — an
// ENCRYPTED chain whose identity header is already gone, i.e. one pruned
// or compacted by a pre-fix binary — because that is the state an
// operator plausibly runs the next scheduled prune against.
func TestItem95_PruneRefusesLoudlyOverAnUnreadableChain(t *testing.T) {
	const passphrase = "prune-refusal-passphrase"
	_, _, store := bug214SeedEncryptedRotatedChain(t, crypto.EncryptModePerChain, passphrase)

	// Simulate the pre-fix state: an encrypted chain whose identity header
	// is already gone (a chain pruned or compacted by sluice <= v0.104.2).
	if err := store.Delete(context.Background(), lineage.ManifestFileName); err != nil {
		t.Fatalf("delete chain-root manifest: %v", err)
	}
	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{
		KeepIncrementals: pruneKeepDroppingRootSegmentWhole(t, store),
	})
	if err == nil {
		t.Fatal("prune reported SUCCESS over a chain with no identity header — the whole defect")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("refusal is not %s (a script cannot branch on it): %v", sluicecode.CodeBackupChainUnreadable, err)
	}
	if coded.ExitCode() != sluicecode.ExitRefusal {
		t.Errorf("ExitCode() = %d; want %d (refusal)", coded.ExitCode(), sluicecode.ExitRefusal)
	}
	// Pre-commit leg: it fired BEFORE anything was deleted, so every
	// pre-prune segment must still be catalogued.
	cat, okCat, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !okCat || len(cat.Segments) < 2 {
		t.Fatalf("prune mutated the catalog before refusing: %+v", cat)
	}
	t.Logf("item-96 pin PROVEN: %s / exit %d — %v", coded.Code, coded.ExitCode(), err)
}
