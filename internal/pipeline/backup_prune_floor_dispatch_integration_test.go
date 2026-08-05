//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The prune-to-floor chain-shaping cell (audit G-7), across
// {plaintext, encrypted, signed}.
//
// # The shape
//
// `backup prune --keep-duration` with a cutoff older than every incremental
// in a ROTATED chain retires each earlier segment WHOLE and leaves the
// newest segment holding only its full. It is a supported retention outcome
// — the ordinary result of a cron running against a chain whose writes have
// stopped — and it is the ONLY shape in which the one restorable segment is
// a single segment that does not live at the conventional root paths.
//
// That is what made it a blind spot. [Restore.Run]'s dispatch predicate
// asked "one segment, no incrementals?" and, on yes, took the fast path
// that resolves `manifest.json` at the lineage ROOT by convention. After
// this prune that file is the RETIRED segment-0 full, kept deliberately for
// the chain's identity (its CEK wrap + Argon2id salt, roadmap item 95) —
// while `backup verify` walked the catalog and read the floor segment's
// full. Two commands, one directory, different manifests: verify-green /
// restore-chunk-missing on an intact chain, or a SILENT STALE RESTORE of
// older data when the retired segment's chunk deletes had failed.
//
// # Why this file rather than a row in the existing matrix
//
// [TestChainShaping_EncryptedRoundTripMatrix] already prunes and restores.
// It could not have caught this, for two reasons worth stating because both
// are gate-scope defects rather than coverage gaps:
//
//  1. Its `prune` cell uses `--keep-incrementals`, which can never produce
//     a floor segment with ZERO incrementals (the flag refuses when its
//     rounded keep-count would retain everything, and every boundary count
//     it accepts keeps at least one). Only a `--keep-duration` cutoff past
//     the whole chain reaches the shape.
//  2. Its restore leg called [backup.ChainRestore] DIRECTLY, which is the
//     branch the dispatch would have chosen anyway. The defect lived in the
//     choosing. (That leg now goes through [backup.Restore] — the entry
//     point the CLI uses — so every cell of that matrix exercises the
//     dispatch too.)
//
// # The independent expected value
//
// Stated plainly, per the 2026-08-01 rule: the expected value is the SOURCE
// database, read with SQL after the archive is closed. It is never derived
// from the manifest, the catalog, or a second read of the archive — which
// is the trap here, since a `backup verify` that rehashes what the manifest
// lists is internally consistent and says NOTHING about whether the
// dispatch picked the right manifest.
//
// Making that comparison EXACT is what the fixture is for: every write
// happens BEFORE the stream starts, so each rotation-born segment full is a
// snapshot of the already-final source. The floor segment's full therefore
// holds exactly the source, and "restored == source" is a real equality
// rather than a subset check. The discriminator is the retired root: its
// snapshot is the single seed row, so a restore that reads it lands 1 row
// against a source of ~25 — asserted as a non-vacuity precondition so the
// cell cannot pass by the two snapshots happening to agree.
package pipeline

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// floorPruneMode is one cell of the {plaintext, encrypted, signed} axis.
//
// The signed cell is the reason the axis is not just {plaintext,
// encrypted}: C-8 (a retired manifest keeping a signature that still
// verifies) can only be seen on a chain that HAS signatures, and a
// plaintext-only test skips it silently rather than failing.
//
// It signs AFTER the chain is built, through the same
// [lineage.ResignLineage] `prune`/`compact` use on a signed chain. That is
// not a shortcut around the writers: [BackupStream] REFUSES to extend a
// signed chain (refuseSignedChain), so a signed MULTI-segment lineage
// cannot be produced by streaming at all, and re-signing a rotated chain is
// exactly how one comes to exist.
type floorPruneMode struct {
	name string
	mode string // crypto.EncryptMode*; "" = plaintext
	sign bool
}

func floorPruneModes() []floorPruneMode {
	return []floorPruneMode{
		{name: "plaintext", mode: ""},
		{name: "encrypted", mode: crypto.EncryptModePerChain},
		{name: "signed", mode: "", sign: true},
	}
}

// TestPruneToFloorFull_RestoreDispatchMatrix is audit G-7's cell: prune a
// rotated chain down to its floor segment's full, then require that
// `restore` — through the production [backup.Restore] entry point, dispatch
// and all — lands the FLOOR's rows, that `backup verify` agrees, and that
// maintenance left no orphaned signature behind.
func TestPruneToFloorFull_RestoreDispatchMatrix(t *testing.T) {
	const passphrase = "prune-to-floor-dispatch-passphrase"
	for _, m := range floorPruneModes() {
		t.Run(m.name, func(t *testing.T) {
			ctx := context.Background()
			f := seedQuiescedRotatedChainFixture(t, m.mode, passphrase)

			// The source is final and closed; this is THE independent
			// expected value for everything below.
			oracle := bug214Read(t, f.sourceDSN)
			if oracle.n < 5 {
				t.Fatalf("source oracle holds %d rows; the fixture produced nothing to distinguish "+
					"snapshots with", oracle.n)
			}

			var signer *lineage.Signer
			var verifyPub ed25519.PublicKey
			if m.sign {
				pub, priv, err := crypto.GenerateEd25519Keypair()
				if err != nil {
					t.Fatalf("GenerateEd25519Keypair: %v", err)
				}
				verifyPub, signer = pub, lineage.NewEd25519Signer(priv)
				if err := lineage.ResignLineage(ctx, f.store, signer); err != nil {
					t.Fatalf("ResignLineage: %v", err)
				}
				if signed, _ := lineage.ChainIsSigned(ctx, f.store); !signed {
					t.Fatal("the chain is not detected as signed after re-signing; the signed cell would " +
						"be a duplicate of the plaintext one")
				}
			}

			// PRECONDITION (non-vacuity, and the discriminator): the chain
			// ROOT's snapshot must differ from the floor's, or "restore
			// read the wrong manifest" and "restore read the right one"
			// produce the same rows and this cell proves nothing.
			rootFull, err := lineage.ReadRootManifest(ctx, f.store)
			if err != nil || rootFull == nil {
				t.Fatalf("read chain-root manifest: %v", err)
			}
			rootRows := manifestRowCount(rootFull)
			if rootRows >= oracle.n {
				t.Fatalf("the chain root's snapshot holds %d row(s) and the source holds %d — a restore "+
					"of the RETIRED root would be indistinguishable from a restore of the floor, so this "+
					"cell cannot detect the defect it exists for", rootRows, oracle.n)
			}

			// ---- the maintenance under test ----
			preSegments := f.segments(t)
			res, err := backup.PruneChain(ctx, f.store, backup.PruneOpts{
				KeepDuration: time.Second,
				Now:          func() time.Time { return time.Now().Add(365 * 24 * time.Hour) },
				Signer:       signer,
				Envelope:     f.readEnvelope(t),
			})
			if err != nil {
				t.Fatalf("PruneChain (--keep-duration past the whole chain): %v", err)
			}
			cat, ok, err := lineage.LoadLineageCatalog(ctx, f.store)
			if err != nil || !ok {
				t.Fatalf("post-prune LoadLineageCatalog: ok=%v err=%v", ok, err)
			}
			if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 0 || cat.Segments[0].Dir == "" {
				t.Fatalf("the prune did not produce the floor-full-only shape this cell is about "+
					"(%d segment(s) from %d, dir=%q, %d incremental(s), %d dropped); retune the "+
					"fixture's rotation thresholds",
					len(cat.Segments), preSegments, cat.Segments[0].Dir,
					len(cat.Segments[0].Incrementals), res.SegmentsDropped)
			}
			// item 95: the chain identity survives (its key material is what
			// the encrypted cell needs to read anything at all).
			if ex, _ := f.store.Exists(ctx, lineage.ManifestFileName); !ex {
				t.Fatal("prune deleted the chain-root manifest — the chain's identity, not segment 0's data")
			}

			// ---- audit C-8: no signature outlives its manifest ----
			if orphans := sigOrphans(t, f.store); len(orphans) > 0 {
				t.Errorf("audit C-8: %d signature(s) survived the manifest(s) they sign:\n  %s\n"+
					"Each still VERIFIES, so an operator checking signatures to decide what to trust gets "+
					"a green answer about a manifest that is no longer part of this chain — including the "+
					"chain-identity manifest.json prune keeps for its key material.",
					len(orphans), strings.Join(orphans, "\n  "))
			}

			// ---- audit B-6: the dispatch ----
			verifyRep, verifyErr := backup.VerifyBackupCodedReport(ctx, f.store, backup.VerifyOptions{
				Envelope: f.readEnvelope(t), VerifyKey: verifyPub,
			})
			restoreErr := (&backup.Restore{
				Target: mustEngine(t, "postgres"), TargetDSN: f.targetDSN, Store: f.store,
				Envelope: f.readEnvelope(t), VerifyKey: verifyPub,
			}).Run(ctx)

			var got bug214Sums
			if restoreErr == nil {
				got = bug214Read(t, f.targetDSN)
			}
			rowsMatch := restoreErr == nil && got == oracle

			// GATE 1 — verify and restore must agree. Reported first so the
			// message names the trap rather than the symptom.
			if verifyErr == nil && !rowsMatch {
				t.Errorf("VERIFY/RESTORE DISAGREEMENT — `backup verify` returned rc=0 (chunks=%d decrypted=%d) "+
					"over a pruned-to-floor chain restore reads differently.\n"+
					"  restore:  %v\n  rows:     got %+v want %+v (source, read by SQL)\n"+
					"verify walks the catalog; restore dispatched somewhere else. On this chain shape that "+
					"somewhere else is the RETIRED chain-root manifest (audit B-6).",
					verifyRep.Chunks, verifyRep.Authenticated, restoreErr, got, oracle)
			}

			// GATE 2 — the round trip, against the source.
			if restoreErr != nil {
				t.Fatalf("restore of a pruned-to-floor chain FAILED: %v\n"+
					"The floor segment's full is a self-contained snapshot; if this reports missing chunks "+
					"they are the RETIRED segment's, which means the restore resolved the retired chain-root "+
					"manifest instead of the catalog's floor (audit B-6).", restoreErr)
			}
			if got != oracle {
				extra := ""
				if got.n == rootRows {
					extra = fmt.Sprintf("\nThe restored row count equals the RETIRED chain root's snapshot (%d) "+
						"EXACTLY: this is a silent stale restore — older data, everything reporting success "+
						"(audit B-6).", rootRows)
				}
				t.Fatalf("restored rows != the source: got %+v want %+v%s", got, oracle, extra)
			}
			if verifyErr != nil {
				t.Fatalf("`backup verify` failed on a chain that restored row-exact: %v", verifyErr)
			}
			if f.encrypted() && verifyRep.Authenticated == 0 {
				t.Errorf("`backup verify` authenticated 0 of %d chunks on an ENCRYPTED chain — the decrypt "+
					"probe is vacuous", verifyRep.Chunks)
			}
			t.Logf("OK [%s]: %d → 1 segment(s) (floor %q), %d manifests pruned; restored %+v from the floor "+
				"(the retired root's snapshot holds %d row(s)); verify chunks=%d decrypted=%d",
				m.name, preSegments, cat.Segments[0].Dir, len(res.Pruned), got, rootRows,
				verifyRep.Chunks, verifyRep.Authenticated)
		})
	}
}

// mustEngine resolves a registered engine or fails the test.
func mustEngine(t *testing.T, name string) ir.Engine {
	t.Helper()
	eng, ok := engines.Get(name)
	if !ok {
		t.Fatalf("engine %q is not registered", name)
	}
	return eng
}

// manifestRowCount sums the recorded row counts across a full manifest's
// chunks — the size of the snapshot that manifest describes.
func manifestRowCount(m *irbackup.Manifest) int64 {
	var n int64
	for _, tbl := range m.Tables {
		for _, ch := range tbl.Chunks {
			n += ch.RowCount
		}
	}
	return n
}

// sigOrphans returns every `.sig` object in the store that the CURRENT
// catalog does not account for — a signature whose manifest is no longer a
// link of the chain. lineage.json's own signature is always legitimate.
//
// It sweeps the whole store rather than the paths the maintenance code
// touches, deliberately: a check that only looks where the fix looks cannot
// report the sibling the fix missed.
func sigOrphans(t *testing.T, store irbackup.Store) []string {
	t.Helper()
	ctx := context.Background()
	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	qualify := func(dir, p string) string {
		if dir == "" {
			return p
		}
		return dir + "/" + p
	}
	legit := map[string]bool{lineage.LineageSigFileName: true}
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		legit[qualify(seg.Dir, lineage.ManifestSigPath(seg.FullManifestPath))] = true
		for _, ip := range seg.Incrementals {
			legit[qualify(seg.Dir, lineage.ManifestSigPath(ip))] = true
		}
	}
	var out []string
	for _, p := range all {
		if strings.HasSuffix(p, irbackup.SignatureFileSuffix) && !legit[p] {
			out = append(out, p)
		}
	}
	return out
}

// seedQuiescedRotatedChainFixture builds a multi-segment lineage whose
// every rotation-born full snapshots an ALREADY-FINAL source.
//
// It differs from [seedRotatedChainFixture] in one deliberate way, and that
// way is the whole point: all writes land BEFORE the stream starts. The
// stream then drains an existing backlog of changes, rolling over and
// rotating, and each rotation's full — an ordinary `backup full` against
// the live source — captures the source exactly as it now stands. So the
// FLOOR segment's full equals the source table, and the cell can assert an
// exact equality against SQL instead of a subset relation.
//
// The seed full (the chain root, one row) is taken before any of that, so
// the retired root's snapshot is measurably different from the floor's —
// which is what lets the cell tell a floor restore from a stale one.
func seedQuiescedRotatedChainFixture(t *testing.T, mode, passphrase string) *shapingFixture {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)

	applyDDL(t, sourceDSN, `
		CREATE TABLE ledger (
			id      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			memo    VARCHAR(255) NOT NULL,
			balance BIGINT NOT NULL DEFAULT 0
		);
		ALTER TABLE ledger REPLICA IDENTITY FULL;
		INSERT INTO ledger (memo, balance) VALUES ('seed', 1);
	`)
	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	t.Cleanup(func() { dropPGLogicalSlot(t, sourceDSN, "sluice_slot") })

	eng, _ := engines.Get("postgres")
	store, _ := blobcodec.NewLocalStore(t.TempDir())
	f := &shapingFixture{sourceDSN: sourceDSN, targetDSN: targetDSN, store: store, mode: mode, pass: passphrase}

	var seedEnc *lineage.BackupEncryption
	if mode != "" {
		seedEnc = &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            mode,
		}
	}
	if err := (&backup.Backup{
		Source: eng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChunkRows: 1,
		Encryption: seedEnc,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("read seed full: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "postgres",
		Token:  fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, slotLSN),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite seed full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.DefaultCodec)

	// EVERY write, before the stream exists. The slot has been holding WAL
	// since it was created, so the stream still sees all of it as changes to
	// roll over — but the TABLE is final from here on.
	const churn = 24
	f.insert(t, churn)

	var streamEnc *lineage.BackupEncryption
	if mode != "" {
		streamEnc = &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            mode,
		}
	}
	stream := &BackupStream{
		Source:                    eng,
		SourceDSN:                 sourceDSN,
		Store:                     store,
		ParentRef:                 full.BackupID,
		RolloverWindow:            900 * time.Millisecond,
		RolloverMaxChanges:        6,
		RolloverMaxBytes:          1 << 30,
		ChunkChanges:              50,
		RetainRotateAtChainLength: 2,
		SluiceVersion:             "test",
		Encryption:                streamEnc,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	time.Sleep(12 * time.Second)
	cancel()
	select {
	case err := <-streamErr:
		if err != nil && !strings.Contains(err.Error(), "context") {
			t.Fatalf("stream.Run = %v; want clean exit", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream.Run did not exit within 30s of cancel")
	}

	if segs := f.segments(t); segs < 2 {
		t.Fatalf("seeded segments = %d; want >= 2 (rotation didn't materialize, so there is no "+
			"sub-dir floor segment and this cell cannot reach the B-6 shape)", segs)
	}
	if incs := f.totalIncrementals(t); incs == 0 {
		t.Fatal("the seeded chain holds no incrementals, so a --keep-duration prune has nothing to " +
			"retire and would report `nothing to prune`")
	}
	return f
}

// totalIncrementals counts the incrementals across the whole lineage — the
// set a --keep-duration cutoff past the chain retires.
func (f *shapingFixture) totalIncrementals(t *testing.T) int {
	t.Helper()
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), f.store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	n := 0
	for i := range cat.Segments {
		n += len(cat.Segments[i].Incrementals)
	}
	return n
}
