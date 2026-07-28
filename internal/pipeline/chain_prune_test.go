// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestPruneLineage_WithinSegmentTrimIsRefusedBeforeAnythingIsDeleted
// pins a defect the item-95 readability gate SURFACED (it did not create
// it), and it is why this test replaces the old
// `TestPruneLineage_KeepIncrementalsDropsOldest`, which asserted the
// trim succeeded.
//
// Prune's other retention shape — dropping LEADING incrementals inside
// the floor segment — leaves that segment's full anchored at S with its
// first surviving incremental starting at some S' > S. The events in
// (S, S'] are simply gone, so the chain no longer restores: the restore
// path's own `lineage.BuildLineageChain` refuses it, first on the
// severed parent link and then (with a real comparator) on the forward
// position gap. Ground-truthed on a REAL rotated Postgres chain, in
// PLAINTEXT, on the pre-fix binary: prune exited 0 and the subsequent
// `ChainRestore` failed with `build lineage: … does not chain off
// preceding link … — branching/mis-stitched lineage`.
//
// Item 100's settled contract is segment-granular retention: prune
// rounds a keep-count UP to the nearest segment boundary and retires
// only whole segments (see
// [TestPruneLineage_KeepCountRoundsUpToTheSegmentBoundary]). This is the
// one shape rounding cannot rescue — a SINGLE-segment chain has no
// boundary, so rounding up retains everything and the run would delete
// nothing. Reporting that as a successful prune is the silent no-op the
// contract exists to avoid, so it stays a refusal, and this test holds
// that refusal to naming the shape, the inertness, and the retention
// that IS available on THIS chain.
func TestPruneLineage_WithinSegmentTrimIsRefusedBeforeAnythingIsDeleted(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 5)

	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 2})
	if err == nil {
		t.Fatal("prune reported success over a chain whose restore path refuses to walk")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeBackupChainUnreadable, err)
	}
	if coded.ExitCode() != sluicecode.ExitRefusal {
		t.Errorf("ExitCode() = %d; want %d — a re-run will not help", coded.ExitCode(), sluicecode.ExitRefusal)
	}
	if coded.Hint == "" {
		t.Error("the refusal carries no standalone hint — the machine-readable remedy is the half item 100 was filed for")
	}
	// The three jobs of the purpose-built prose. Each substring is a
	// promise to the operator, not a formatting detail: what happened,
	// that nothing was destroyed, and what to do instead.
	for _, want := range []string{
		// The SHAPE, in the operator's own invocation.
		"refusing --keep-incrementals=2",
		"within-segment incremental trim severs the chain",
		"still records parent",
		// The inertness promise.
		"NOTHING WAS DELETED",
		"exactly as restorable as it was before this run",
		// The remedy — and, on a never-rotated chain, the fact that the
		// flag they used cannot express it (item 100's reach, derived
		// from the lineage rather than asserted in prose).
		"No --keep-incrementals value lands on a segment boundary",
		"never-rotated (SINGLE-segment) chain none ever can",
		"--keep-duration",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal is missing %q — it is the unactionable generic prose again:\n  %v", want, err)
		}
	}
	// The promise has to be true: catalog and manifests untouched.
	cat, okCat, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !okCat {
		t.Fatalf("post-refusal lineage.LoadLineageCatalog: ok=%v err=%v", okCat, err)
	}
	if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 5 {
		t.Errorf("refused prune mutated the catalog: %+v", cat.Segments)
	}
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("refused prune left the chain unwalkable: %v", err)
	}
}

// TestPruneLineage_KeepDuration drops incrementals older than the
// threshold. The threshold retires the WHOLE segment's incrementals
// (leaving its full as the restore base) rather than trimming leading
// ones — the segment-granular shape that stays restorable; see
// [TestPruneLineage_WithinSegmentTrimIsRefusedBeforeAnythingIsDeleted]
// for why the partial-trim variant this used to assert cannot.
func TestPruneLineage_KeepDuration(t *testing.T) {
	store := newMemStore()
	base := seedLineageChain(t, store, 5)
	now := func() time.Time { return base.Add(100 * time.Hour) }

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepDuration: 2 * time.Hour, Now: now})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	// Incrementals at base+1h..base+5h; now=base+100h; nothing is younger
	// than 2h, so all 5 go and the segment full remains the restore base.
	if len(res.Pruned) != 5 {
		t.Errorf("Pruned = %d; want 5 (older-than-2h)", len(res.Pruned))
	}
	// The must-NOT-change direction: retiring a segment's incrementals
	// ENTIRELY is boundary-aligned already (the floor keeps only its full,
	// so there is no first-incremental boundary to violate), so item
	// 100's rounding leaves it alone.
	if res.RequestedIncrementals != 0 || res.IncrementalsRetained != 0 {
		t.Errorf("requested/retained = %d/%d; want 0/0 — a cutoff past every incremental needs no rounding",
			res.RequestedIncrementals, res.IncrementalsRetained)
	}
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-prune lineage.LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 0 {
		t.Errorf("post-prune segment = %+v; want 1 segment with 0 incrementals", cat.Segments)
	}
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-prune chain does not walk: %v", err)
	}
}

// TestPruneLineage_KeepDurationOlderThanEveryIncrementalIsANoOp is the
// other must-NOT-change direction for item 100: a retention window that
// reaches back past every incremental retires none of them, and that
// stays the documented no-op rather than becoming a refusal. Rounding
// never enters it — there is no drop to round.
func TestPruneLineage_KeepDurationOlderThanEveryIncrementalIsANoOp(t *testing.T) {
	store := newMemStore()
	base := seedLineageChain(t, store, 5)
	now := func() time.Time { return base.Add(6 * time.Hour) }

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepDuration: 1000 * time.Hour, Now: now})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 0 || res.SegmentsDropped != 0 {
		t.Errorf("Pruned=%d SegmentsDropped=%d; want 0,0", len(res.Pruned), res.SegmentsDropped)
	}
	if res.RequestedIncrementals != 5 || res.IncrementalsRetained != 5 {
		t.Errorf("requested/retained = %d/%d; want 5/5", res.RequestedIncrementals, res.IncrementalsRetained)
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 5 {
		t.Errorf("incrementals = %d; want 5 (unchanged)", len(cat.Segments[0].Incrementals))
	}
}

// TestPruneLineage_KeepAllNoOp: keep >= count → nothing pruned.
func TestPruneLineage_KeepAllNoOp(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 3)
	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 10})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Errorf("Pruned = %d; want 0", len(res.Pruned))
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 3 {
		t.Errorf("incrementals = %d; want 3 (unchanged)", len(cat.Segments[0].Incrementals))
	}
}

// TestPruneLineage_DryRunNoSideEffects reports the would-prune set
// without mutating the lineage or deleting chunks — and reports the
// ROUNDED plan, not the requested one. A dry run that describes a prune
// the real run would not perform is worse than no dry run, so the plan
// is compared against the real run's own result rather than against a
// hand-computed expectation.
func TestPruneLineage_DryRunNoSideEffects(t *testing.T) {
	store := newMemStore()
	seedTwoSegmentLineage(t, store)

	// keep=1 asks for one incremental inside seg-1; segment granularity
	// rounds it up to 2 (seg-1's pair), so the plan is seg0 WHOLE.
	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1, DryRun: true})
	if err != nil {
		t.Fatalf("PruneChain dry-run: %v", err)
	}
	if res.ChunksDeleted != 0 {
		t.Errorf("dry-run ChunksDeleted = %d; want 0", res.ChunksDeleted)
	}
	if res.RequestedIncrementals != 1 || res.IncrementalsRetained != 2 {
		t.Errorf("dry-run requested/retained = %d/%d; want 1/2 — the dry run must report the ROUNDED retention",
			res.RequestedIncrementals, res.IncrementalsRetained)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("dry-run SegmentsDropped = %d; want 1 (whole seg0)", res.SegmentsDropped)
	}
	for _, p := range res.Pruned {
		if strings.HasPrefix(p, "seg-1/") {
			t.Errorf("dry-run planned to drop %q inside the surviving floor segment; rounding up must leave it whole", p)
		}
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 || len(cat.Segments[0].Incrementals) != 2 {
		t.Errorf("post-dry-run catalog = %+v; want the seeded 2 segments untouched", cat.Segments)
	}

	// …and the real run does exactly what the dry run said.
	live, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1})
	if err != nil {
		t.Fatalf("PruneChain (real): %v", err)
	}
	if len(live.Pruned) != len(res.Pruned) || live.SegmentsDropped != res.SegmentsDropped {
		t.Errorf("the real run diverged from the dry run: dropped %d manifests / %d segments, dry run said %d / %d",
			len(live.Pruned), live.SegmentsDropped, len(res.Pruned), res.SegmentsDropped)
	}
}

// TestPruneLineage_DryRunRefusesWhatTheRealRunRefuses is the other half
// of "a dry run must not lie": on the shape rounding cannot rescue, the
// plan-only path refuses identically instead of enumerating a prune the
// real run would decline.
//
// This assertion is INVERTED from what it pinned before item 100's
// contract landed. `--dry-run` used to return ahead of the readability
// gate, so it reported "would drop 3" on a chain whose real prune then
// refused — which was tolerable only while the refusal came from a gate
// the dry run deliberately skipped. The refusal is now a property of the
// retention arithmetic, which --dry-run computes too.
func TestPruneLineage_DryRunRefusesWhatTheRealRunRefuses(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 4)

	dry, dryErr := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1, DryRun: true})
	if dryErr == nil {
		t.Fatalf("--dry-run reported a plan (%d manifests) the real run refuses", len(dry.Pruned))
	}
	coded, ok := sluicecode.FromError(dryErr)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("dry-run refusal is not %s: %v", sluicecode.CodeBackupChainUnreadable, dryErr)
	}
	_, realErr := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1})
	if realErr == nil || realErr.Error() != dryErr.Error() {
		t.Errorf("dry run and real run disagree:\n  dry:  %v\n  real: %v", dryErr, realErr)
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 4 {
		t.Errorf("post-dry-run incrementals = %d; want 4 (unchanged)", len(cat.Segments[0].Incrementals))
	}
}

func TestPruneLineage_RefusesBothFlags(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2)
	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1, KeepDuration: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err = %v; want mutual-exclusion", err)
	}
}

func TestPruneLineage_RefusesNeitherFlag(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2)
	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err = %v; want at-least-one", err)
	}
}

func TestPruneLineage_RefusesWhenCatalogAbsent(t *testing.T) {
	store := newMemStore()
	mustWriteManifest(t, store, lineage.ManifestFileName, &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
		BackupID: "full000", Kind: irbackup.BackupKindFull, CreatedAt: time.Now().UTC(),
	})
	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1})
	if err == nil || !strings.Contains(err.Error(), "lineage.json not found") {
		t.Errorf("err = %v; want lineage.json-not-found refusal", err)
	}
}

// TestPruneLineage_MultiSegmentDropsLeadingWholeSegment: a 2-segment
// lineage (seg0 capped w/ 2 incrs, seg1 open w/ 2 incrs); keep 2 →
// the whole seg0 (full + its 2 incrs) is dropped, seg1's full is the
// new restore base; restore-after-prune stays correct (the segment
// full is a self-contained snapshot).
func TestPruneLineage_MultiSegmentDropsLeadingWholeSegment(t *testing.T) {
	store := newMemStore()
	seedTwoSegmentLineage(t, store)

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("SegmentsDropped = %d; want 1 (whole seg0)", res.SegmentsDropped)
	}
	// The must-NOT-change direction for item 100: a keep-count that
	// ALREADY lands on a segment boundary is not rounded, and behaves
	// exactly as it did before segment-granular retention landed.
	if res.RequestedIncrementals != 2 || res.IncrementalsRetained != 2 {
		t.Errorf("requested/retained = %d/%d; want 2/2 — a boundary keep-count must not be rounded",
			res.RequestedIncrementals, res.IncrementalsRetained)
	}
	got, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(got.Segments) != 1 || got.Segments[0].Dir != "seg-1" {
		t.Fatalf("post-prune segments = %+v; want only seg-1", got.Segments)
	}
	if got.RestorableFromSegment != 0 {
		t.Errorf("RestorableFromSegment = %d; want 0 (re-based to seg-1)", got.RestorableFromSegment)
	}
	// seg0's DATA is gone, but the chain-root manifest SURVIVES.
	//
	// This assertion is INVERTED from what it pinned before (roadmap item
	// 95). It encoded a plaintext-era assumption: that the root segment's
	// full is just segment 0's manifest, redundant once segment 0 is
	// retired. Encryption changed what "redundant" means. That file is the
	// CHAIN'S identity — ADR-0152 binds the chain CEK's wrap to it, and a
	// passphrase chain's Argon2id salt is recorded ONLY there — so deleting
	// it revoked readability for every REMAINING segment while the operator
	// held a correct passphrase. Prune fires on a retention schedule, so its
	// reach was larger than Bug 214's compaction twin. What survives is a
	// small dangling identity header for a legitimately retired root
	// segment; that is the right trade, and it is unconditional rather than
	// encryption-gated because "we only need this file sometimes" is exactly
	// the reasoning that produced Bug 214.
	if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); !ex {
		t.Error("item 95: prune deleted the chain-root manifest — the chain's identity, not segment 0's data")
	}
	// …and the retirement is real: seg0's incrementals ARE gone, so keeping
	// the header did not quietly turn prune into a no-op.
	if ex, _ := store.Exists(context.Background(), "manifests/incr-01.json"); ex {
		t.Error("seg0 incrementals survived a whole-segment prune")
	}
	if ex, _ := store.Exists(context.Background(), "seg-1/manifest.json"); !ex {
		t.Error("seg-1 full must survive (it is the new restore base)")
	}
	// Restore-after-prune correctness: the surviving lineage still
	// validates (the kept segment full is a contiguous base).
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("restore-after-prune lineage.BuildLineageChain: %v; want valid", err)
	}
}

// seedTwoSegmentLineage writes the canonical rotated fixture: seg0
// (capped, full + 2 incrementals) and seg1 (open, full + 2). Contiguous
// throughout — each link's StartPosition == the preceding link's
// EndPosition, and seg1.full.Start == seg0.lastIncr.End, the
// inter-segment boundary the rotation FSM guarantees — so any refusal a
// prune of it produces is about the PRUNE, never about the seed.
func seedTwoSegmentLineage(t *testing.T, store irbackup.Store) {
	t.Helper()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	f0 := seedFull(t, store, "", "0/000", "0/100", now)
	i01 := seedIncr(t, store, "", "incr01", f0.BackupID, "0/100", "0/200", now.Add(time.Hour))
	i02 := seedIncr(t, store, "", "incr02", i01.BackupID, "0/200", "0/300", now.Add(2*time.Hour))
	f1 := seedFull(t, store, "seg-1", "0/300", "0/400", now.Add(3*time.Hour))
	i11 := seedIncr(t, store, "seg-1", "incr11", f1.BackupID, "0/400", "0/500", now.Add(4*time.Hour))
	i12 := seedIncr(t, store, "seg-1", "incr12", i11.BackupID, "0/500", "0/600", now.Add(5*time.Hour))

	capt := now.Add(3 * time.Hour)
	cat := &lineage.Catalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []lineage.Segment{
			{
				SegmentID: f0.BackupID, Dir: "", FullManifestPath: lineage.ManifestFileName,
				Incrementals:  []string{"manifests/incr-01.json", "manifests/incr-02.json"},
				StartPosition: f0.EndPosition, EndPosition: i02.EndPosition,
				CappedAt: &capt, CapReason: rotationReasonAge, Codec: blobcodec.CodecGzip,
			},
			{
				SegmentID: f1.BackupID, Dir: "seg-1", FullManifestPath: lineage.ManifestFileName,
				Incrementals:  []string{"manifests/incr-11.json", "manifests/incr-12.json"},
				StartPosition: f1.EndPosition, EndPosition: i12.EndPosition, Codec: blobcodec.CodecGzip,
			},
		},
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
}

// TestPruneLineage_KeepCountRoundsUpToTheSegmentBoundary is item 100's
// contract, on the shape it was decided for.
//
// This test's first half is INVERTED from what it pinned as
// `TestPruneLineage_WithinSegmentTrimNamesTheBoundaryKeepCounts`, which
// asserted that `--keep-incrementals=1` on this fixture REFUSED and
// named the boundary counts. Refusing was the interim behaviour while
// the retention contract was open; the contract is now "round the
// retention UP to a segment boundary", so this keep-count succeeds and
// retains 2. The refusal's boundary-naming half did not disappear with
// it — see
// [TestPruneLineage_NoBoundaryAtOrAboveTheRequestRefusesAndNamesTheCounts],
// which pins that prose on the same fixture at the keep-count rounding
// still cannot rescue.
//
// The fixture holds 4 incrementals split 2/2 across two segments, so the
// only boundary is at keep=2 (retire seg0 whole).
func TestPruneLineage_KeepCountRoundsUpToTheSegmentBoundary(t *testing.T) {
	store := newMemStore()
	seedTwoSegmentLineage(t, store)

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	// Retained MORE than asked, in the safe direction, and said so.
	if res.RequestedIncrementals != 1 || res.IncrementalsRetained != 2 {
		t.Errorf("requested/retained = %d/%d; want 1/2 — retention rounds UP to the segment boundary",
			res.RequestedIncrementals, res.IncrementalsRetained)
	}
	// …and it is a real prune, not an over-retention that dropped nothing.
	if res.SegmentsDropped != 1 {
		t.Fatalf("SegmentsDropped = %d; want 1 (whole seg0) — rounding up must not turn prune into a no-op", res.SegmentsDropped)
	}
	got, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(got.Segments) != 1 || got.Segments[0].Dir != "seg-1" {
		t.Fatalf("post-prune segments = %+v; want only seg-1", got.Segments)
	}
	// The load-bearing half: the surviving floor segment was not trimmed
	// in place. Whole segments dropped, nothing severed.
	if n := len(got.Segments[0].Incrementals); n != 2 {
		t.Errorf("floor segment kept %d of its 2 incrementals; segment-granular retention must leave it whole", n)
	}
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Fatalf("the rounded-up prune left a chain that does not walk: %v", err)
	}
	// Idempotence in the must-NOT-change direction: asking for the count
	// the rounding produced behaves identically (there is nothing left to
	// drop, so it is the documented no-op).
	again, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("re-running at the rounded keep-count refuses: %v", err)
	}
	if len(again.Pruned) != 0 {
		t.Errorf("re-run Pruned = %d; want 0", len(again.Pruned))
	}
}

// TestPruneLineage_NoBoundaryAtOrAboveTheRequestRefusesAndNamesTheCounts
// is the ROTATED half of item 100's refusal, and the reason the refusal
// survived the contract decision at all: rounding UP can only reach a
// boundary that EXISTS at or above the request. Ask to keep 3 of this
// fixture's 4 incrementals and the only boundary (2) is below it, so
// honouring the request segment-granularly means retaining all 4 and
// deleting nothing.
//
// The refusal must not just say "prune at segment granularity" — it must
// compute and name the `--keep-incrementals` values that work on THIS
// chain, so the operator's next invocation is a copy-paste rather than a
// derivation from the lineage catalog.
func TestPruneLineage_NoBoundaryAtOrAboveTheRequestRefusesAndNamesTheCounts(t *testing.T) {
	store := newMemStore()
	seedTwoSegmentLineage(t, store)

	_, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 3})
	if err == nil {
		t.Fatal("a keep-count above every segment boundary retains everything; reporting that as a prune is the silent no-op")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeBackupChainUnreadable, err)
	}
	for _, want := range []string{
		"refusing --keep-incrementals=3",
		"within-segment incremental trim severs the chain",
		"the --keep-incrementals counts that land on a segment boundary are 2",
		"NOTHING WAS DELETED",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal is missing %q:\n  %v", want, err)
		}
	}
	// Inert, and the named remedy actually WORKS — a computed remedy
	// nobody ran is the same unactionable refusal with more words.
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Fatalf("the refusal mutated the catalog: %+v", cat.Segments)
	}
	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("the boundary keep-count the refusal named still refuses: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("SegmentsDropped = %d; want 1 (the remedy must actually prune, not no-op)", res.SegmentsDropped)
	}
}

// TestPruneLineage_LeadingIncrementalLessSegmentStillDrops pins the
// reason item 100's refusal asks TWO questions ("did rounding leave
// nothing to drop?" AND "has the restore floor not moved?") rather than
// just the first.
//
// A rotated chain whose leading segment holds no incrementals of its own
// contributes no segment boundary, so a keep-count rounds up to "retain
// every incremental" — and yet there is still a whole segment to retire.
// Refusing here would deny an operator a prune that is available and
// perfectly safe, on the strength of an incremental count.
func TestPruneLineage_LeadingIncrementalLessSegmentStillDrops(t *testing.T) {
	store := newMemStore()
	seedLeadingIncrementalLessSegmentLineage(t, store)

	res, err := backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 1})
	if err != nil {
		t.Fatalf("PruneChain refused a chain with a droppable leading segment: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Fatalf("SegmentsDropped = %d; want 1 (the incremental-less seg0)", res.SegmentsDropped)
	}
	if res.RequestedIncrementals != 1 || res.IncrementalsRetained != 2 {
		t.Errorf("requested/retained = %d/%d; want 1/2 (rounded up to the whole surviving segment)",
			res.RequestedIncrementals, res.IncrementalsRetained)
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 1 || cat.Segments[0].Dir != "seg-1" || len(cat.Segments[0].Incrementals) != 2 {
		t.Fatalf("post-prune segments = %+v; want only seg-1 with both incrementals", cat.Segments)
	}
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-prune chain does not walk: %v", err)
	}
}

// seedLeadingIncrementalLessSegmentLineage writes a 2-segment lineage
// whose ROOT segment received no incrementals before rotating (the
// Bug-139 stamp-less shape) and whose second segment holds two.
func seedLeadingIncrementalLessSegmentLineage(t *testing.T, store irbackup.Store) {
	t.Helper()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	f0 := seedFull(t, store, "", "0/000", "0/100", now)
	f1 := seedFull(t, store, "seg-1", "0/100", "0/200", now.Add(time.Hour))
	i11 := seedIncr(t, store, "seg-1", "incr11", f1.BackupID, "0/200", "0/300", now.Add(2*time.Hour))
	i12 := seedIncr(t, store, "seg-1", "incr12", i11.BackupID, "0/300", "0/400", now.Add(3*time.Hour))

	capt := now.Add(time.Hour)
	cat := &lineage.Catalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []lineage.Segment{
			{
				SegmentID: f0.BackupID, Dir: "", FullManifestPath: lineage.ManifestFileName,
				StartPosition: f0.EndPosition, EndPosition: f0.EndPosition,
				CappedAt: &capt, CapReason: rotationReasonAge, Codec: blobcodec.CodecGzip,
			},
			{
				SegmentID: f1.BackupID, Dir: "seg-1", FullManifestPath: lineage.ManifestFileName,
				Incrementals:  []string{"manifests/incr-11.json", "manifests/incr-12.json"},
				StartPosition: f1.EndPosition, EndPosition: i12.EndPosition, Codec: blobcodec.CodecGzip,
			},
		},
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
}

// TestPruneLineage_UnreadableChainKeepsTheGenericRefusal is the
// SPECIFICITY pin for item 100's message: the readability gate catches
// more than the within-segment trim, and those conditions need their own
// prose (an identity/key failure's recovery is `cp`-ing a surviving
// header back, which has nothing to do with retention granularity). A
// blanket "if prune fails, blame the trim" would have been the wrong
// fix, so this asserts the generic decoration is still what a
// non-trim refusal carries.
//
// The shape: a whole-segment prune (keep exactly seg1's pair — no
// in-place trim at all) of a chain whose surviving segment claims
// passphrase encryption while the chain-ROOT manifest records none, i.e.
// the state a pre-fix binary left an encrypted chain in.
func TestPruneLineage_UnreadableChainKeepsTheGenericRefusal(t *testing.T) {
	store := newMemStore()
	seedTwoSegmentLineage(t, store)

	// Stamp encryption on the surviving segment's full only. The walk
	// still succeeds — this fails at the gate's IDENTITY leg, which is
	// the whole point of the pin.
	segStore := lineage.NewPrefixedStore(store, "seg-1")
	f1, err := lineage.ReadManifestAt(context.Background(), segStore, lineage.ManifestFileName)
	if err != nil {
		t.Fatalf("read seg-1 full: %v", err)
	}
	f1.ChainEncryption = &irbackup.ChainEncryption{
		Algorithm: "AES-256-GCM", Mode: crypto.EncryptModePerChain,
		KEKMode:  crypto.KEKModePassphrase,
		Argon2id: &irbackup.Argon2idParams{Salt: []byte("0123456789abcdef")},
	}
	if err := lineage.WriteManifestAt(context.Background(), segStore, lineage.ManifestFileName, f1); err != nil {
		t.Fatalf("rewrite seg-1 full: %v", err)
	}

	_, err = backup.PruneChain(context.Background(), store, backup.PruneOpts{KeepIncrementals: 2})
	if err == nil {
		t.Fatal("prune reported success over a chain whose identity header records no encryption")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeBackupChainUnreadable {
		t.Fatalf("refusal is not %s: %v", sluicecode.CodeBackupChainUnreadable, err)
	}
	if !strings.Contains(err.Error(), "records no chain-encryption metadata") {
		t.Errorf("the identity refusal lost its own prose:\n  %v", err)
	}
	if !strings.Contains(err.Error(), "NO files were removed") {
		t.Errorf("err = %q; want the generic pre-sweep decoration", err)
	}
	if strings.Contains(err.Error(), "within-segment incremental trim") {
		t.Errorf("a NON-trim readability failure was reported as item 100's trim — the detection is not specific:\n  %v", err)
	}
}

// --- seed helpers ---

func seedFull(t *testing.T, root irbackup.Store, dir, startLSN, lsn string, created time.Time) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
		Kind: irbackup.BackupKindFull, CreatedAt: created,
		StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + startLSN + `"}`},
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifestAt(context.Background(), lineage.NewPrefixedStore(root, dir), lineage.ManifestFileName, m); err != nil {
		t.Fatalf("seed full: %v", err)
	}
	return m
}

func seedIncr(t *testing.T, root irbackup.Store, dir, _id, parent, startLSN, lsn string, created time.Time) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
		Kind: irbackup.BackupKindIncremental, CreatedAt: created, ParentBackupID: parent,
		StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + startLSN + `"}`},
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	p := "manifests/incr-" + strings.TrimPrefix(_id, "incr") + ".json"
	if err := lineage.WriteManifestAt(context.Background(), lineage.NewPrefixedStore(root, dir), p, m); err != nil {
		t.Fatalf("seed incr: %v", err)
	}
	return m
}

// seedLineageChain writes a one-segment lineage (full + N
// incrementals) via the production lineage hooks so lineage.json is
// well-formed. Returns the base time (incrementals at base+1h..+Nh).
// stubOrderer is a totally-ordered string-based orderer used by the
// SchemaHistoryRetentionFloor unit tests. Avoids a real engine's
// JSON-position parsing while still exercising the partial-order
// branches (PositionAtOrAfter both-true, both-false-incomparable).
// "incomparable:X" tokens are NEVER at-or-after anything except
// themselves, modelling a partial-order edge.
type stubOrderer struct{}

func (stubOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	if p.Token == "" || anchor.Token == "" {
		return false, errors.New("stubOrderer: empty token")
	}
	if strings.HasPrefix(p.Token, "incomparable:") || strings.HasPrefix(anchor.Token, "incomparable:") {
		// Two "incomparable:N" tokens are incomparable unless they
		// share the exact same token (reflexive).
		return p.Token == anchor.Token, nil
	}
	return p.Token >= anchor.Token, nil
}

// TestSchemaHistoryRetentionFloor_PicksOlder_LiveOlder confirms the
// helper returns the live safe-point when it is OLDER than the oldest
// backup resume position (DP-2: min(live, oldest-backup) — live wins
// when it pulls the floor backward).
func TestSchemaHistoryRetentionFloor_PicksOlder_LiveOlder(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2) // backup oldest = 0/100

	live := ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/050"}`}
	// Use a stub orderer that treats string-compare as the order (matches
	// the seeded lineage's tokens). 0/050 < 0/100, so live is older.
	floor, ok, err := backup.SchemaHistoryRetentionFloor(context.Background(), store, live, stubOrderer{})
	if err != nil || !ok {
		t.Fatalf("expected ok floor; got ok=%v err=%v", ok, err)
	}
	if floor.Token != live.Token {
		t.Errorf("want live floor %q; got %q", live.Token, floor.Token)
	}
}

// TestSchemaHistoryRetentionFloor_PicksOlder_BackupOlder confirms the
// helper returns the backup floor when it is OLDER than the live
// safe-point.
func TestSchemaHistoryRetentionFloor_PicksOlder_BackupOlder(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2) // backup oldest token = `{"slot":"s","lsn":"0/100"}`

	live := ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/999"}`}
	floor, ok, err := backup.SchemaHistoryRetentionFloor(context.Background(), store, live, stubOrderer{})
	if err != nil || !ok {
		t.Fatalf("expected ok floor; got ok=%v err=%v", ok, err)
	}
	if floor.Token != `{"slot":"s","lsn":"0/100"}` {
		t.Errorf("want backup floor 0/100; got %q", floor.Token)
	}
}

// TestSchemaHistoryRetentionFloor_NoBackup_NoLive returns ok=false
// (the caller must skip compaction; no floor → deleting everything
// would defeat the loud-floor sentinel).
func TestSchemaHistoryRetentionFloor_NoBackup_NoLive(t *testing.T) {
	store := newMemStore()
	// No lineage seeded.
	floor, ok, err := backup.SchemaHistoryRetentionFloor(context.Background(), store, ir.Position{}, stubOrderer{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if ok {
		t.Errorf("ok must be false when neither floor is available; got floor=%+v", floor)
	}
}

// TestSchemaHistoryRetentionFloor_Incomparable refuses LOUDLY when
// live and backup-oldest are incomparable under the partial order
// (Bug-74 class: never guess a min for unordered candidates).
func TestSchemaHistoryRetentionFloor_Incomparable(t *testing.T) {
	store := newMemStore()
	// Custom lineage with an "incomparable:A" token.
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
		Kind: irbackup.BackupKindFull, CreatedAt: time.Now(),
		EndPosition:  ir.Position{Engine: "postgres", Token: "incomparable:A"},
		PartialState: irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	mustWriteManifest(t, store, lineage.ManifestFileName, full)
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	live := ir.Position{Engine: "postgres", Token: "incomparable:B"}
	_, _, err := backup.SchemaHistoryRetentionFloor(context.Background(), store, live, stubOrderer{})
	if err == nil {
		t.Fatal("incomparable positions must refuse LOUDLY; got nil err")
	}
	if !strings.Contains(err.Error(), "incomparable") {
		t.Errorf("err must mention incomparable; got %v", err)
	}
}

func seedLineageChain(t *testing.T, store irbackup.Store, incrementals int) time.Time {
	t.Helper()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
		Kind: irbackup.BackupKindFull, CreatedAt: base,
		EndPosition:  ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		PartialState: irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	mustWriteManifest(t, store, lineage.ManifestFileName, full)
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
	parent := full.BackupID
	for i := 1; i <= incrementals; i++ {
		m := &irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion, SourceEngine: "postgres",
			Kind: irbackup.BackupKindIncremental, ParentBackupID: parent,
			CreatedAt:     base.Add(time.Duration(i) * time.Hour),
			StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
			EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
			PartialState:  irbackup.BackupStateComplete,
		}
		m.BackupID = irbackup.ComputeBackupID(m)
		path := "manifests/incr-000" + string(rune('0'+i)) + ".json"
		mustWriteManifest(t, store, path, m)
		_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, m, path, blobcodec.CodecGzip)
		parent = m.BackupID
	}
	return base
}
