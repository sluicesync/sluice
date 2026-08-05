// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit B-6 + C-8 unit pins — the prune-to-floor chain shape.
//
// `backup prune` with a `--keep-duration` cutoff older than every
// incremental in a ROTATED chain retires each earlier segment WHOLE and
// leaves the newest segment holding only its full. That is a supported,
// operator-reachable shape (it is what a retention cron produces on a chain
// whose writes stopped), and it is the one shape where the surviving
// segment is a single segment that is NOT at the conventional root paths.
//
// The load-bearing proof that the right ROWS come back is the integration
// cell TestPruneToFloorFull_RestoreDispatchMatrix (internal/pipeline),
// across {plaintext, encrypted, signed}. These pin the pieces a memstore
// can hold honestly — and, like their item-95 neighbours, every one of them
// drives the REAL [PruneChain] / [Restore.Run] rather than the predicate in
// isolation, because a fix that is present but unreachable is the failure
// mode this project keeps re-learning.

// pruneToFloorFull retires every incremental in the chain by running the
// real prune with a cutoff past all of them, leaving the newest segment's
// full as the only restorable thing. Returns the post-prune catalog.
func pruneToFloorFull(t *testing.T, store irbackup.Store, base time.Time, signer *lineage.Signer) *lineage.Catalog {
	t.Helper()
	ctx := context.Background()
	res, err := PruneChain(ctx, store, PruneOpts{
		KeepDuration: time.Hour,
		Now:          func() time.Time { return base.Add(1000 * time.Hour) },
		Signer:       signer,
	})
	if err != nil {
		t.Fatalf("PruneChain (cutoff past every incremental): %v", err)
	}
	if res.SegmentsDropped < 1 {
		t.Fatalf("SegmentsDropped = %d; the prune retired no segment, so this fixture is not the "+
			"prune-to-floor shape and every assertion below would be vacuous", res.SegmentsDropped)
	}
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("post-prune LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 0 || cat.Segments[0].Dir == "" {
		t.Fatalf("post-prune catalog is not the floor-full-only shape: %d segment(s), dir=%q, %d incremental(s)",
			len(cat.Segments), cat.Segments[0].Dir, len(cat.Segments[0].Incrementals))
	}
	return cat
}

// TestVerifyLineageNeedsWalk_SegmentShapes is the dispatch predicate's own
// table. It is the single predicate `restore`, `backup verify` and
// `export-as-parquet` all ask, so a shape misclassified here diverges all
// three at once — which is precisely how B-6 presented (verify walked the
// catalog floor, restore read the retired root).
//
// The third row is the one that was wrong.
func TestVerifyLineageNeedsWalk_SegmentShapes(t *testing.T) {
	cases := []struct {
		name string
		segs []lineage.Segment
		want bool
		why  string
	}{
		{
			name: "one root segment, no incrementals",
			segs: []lineage.Segment{{Dir: "", FullManifestPath: lineage.ManifestFileName}},
			want: false,
			why:  "a never-rotated bare full: the single-manifest path resolves exactly this file",
		},
		{
			name: "one root segment with incrementals",
			segs: []lineage.Segment{{Dir: "", FullManifestPath: lineage.ManifestFileName, Incrementals: []string{"manifests/incr-1.json"}}},
			want: true,
			why:  "incrementals must be replayed, so the walk is required",
		},
		{
			name: "one SUB-DIR segment, no incrementals (the prune-to-floor shape)",
			segs: []lineage.Segment{{Dir: "seg-1700000000000", FullManifestPath: lineage.ManifestFileName}},
			want: true,
			why: "audit B-6: the surviving full lives under seg-*/, so the single-manifest path would " +
				"read the RETIRED chain-identity manifest.json at the root instead",
		},
		{
			name: "two segments",
			segs: []lineage.Segment{
				{Dir: "", FullManifestPath: lineage.ManifestFileName},
				{Dir: "seg-1700000000000", FullManifestPath: lineage.ManifestFileName},
			},
			want: true,
			why:  "more than one segment always walks",
		},
		{
			name: "one root segment whose full is at a non-conventional path",
			segs: []lineage.Segment{{Dir: "", FullManifestPath: "fulls/manifest.json"}},
			want: true,
			why: "the single-manifest path resolves ManifestFileName by convention and would read the " +
				"wrong file; no release writes this shape, and taking the walk fails loudly rather " +
				"than silently resolving elsewhere",
		},
		{
			name: "one root segment with no recorded full path",
			segs: []lineage.Segment{{Dir: "", FullManifestPath: ""}},
			want: true,
			why:  "a catalog that cannot say where its full is must fail in the walk, not resolve to the root file",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: tc.segs}
			if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
				t.Fatalf("WriteLineageCatalog: %v", err)
			}
			got, err := verifyLineageNeedsWalk(context.Background(), store)
			if err != nil {
				t.Fatalf("verifyLineageNeedsWalk: %v", err)
			}
			if got != tc.want {
				t.Errorf("verifyLineageNeedsWalk = %v; want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// erroringEngine is an [ir.Engine] whose Open* methods FAIL instead of
// panicking, so a test can drive [Restore.Run] to completion on either
// dispatch branch and read the error it returns. It reports the seeded
// chains' own source engine name, so no cross-engine branch is taken.
type erroringEngine struct{ stubEngine }

var errTargetUnreachable = errors.New("test target unreachable")

func (erroringEngine) Name() string { return "postgres" }

func (erroringEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errTargetUnreachable
}

func (erroringEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errTargetUnreachable
}

// TestPruneToFloorFull_RestoreReadsTheFloorNotTheRetiredRoot drives the
// REAL [Restore.Run] over a real prune-to-floor chain and proves it never
// opens the retired chain-identity manifest.
//
// The discriminator is a booby-trapped root: after the prune, the kept
// `manifest.json` — segment 0's retired full — is rewritten with a null
// structural element, which [validateManifestStructure] refuses with a
// coded SLUICE-E-BACKUP-SIGNATURE-INVALID. That refusal can only be raised
// by a reader that actually LOADED that file. Pre-fix Restore.Run took the
// single-manifest path and raised exactly it; post-fix it dispatches to the
// lineage walk, reads the floor segment's full (which is intact), and fails
// later at the unreachable target instead.
//
// A restore that reports the target error is a restore that was reading the
// floor. It is a proxy for "the right rows landed" — the rows themselves
// are the integration cell's job — but it is the proxy that fails when the
// dispatch regresses, which is what a unit pin is for.
func TestPruneToFloorFull_RestoreReadsTheFloorNotTheRetiredRoot(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := newMemStore()
	seedSegmentsWithGapsOpts(t, store, base, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{})

	pruneToFloorFull(t, store, base, nil)

	// The chain-identity manifest is still there (item 95) — and it is now
	// the trap.
	root, err := lineage.ReadRootManifest(ctx, store)
	if err != nil || root == nil {
		t.Fatalf("prune deleted the chain-root identity manifest (item 95): err=%v", err)
	}
	root.Tables = []*irbackup.TableManifest{nil}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, root); err != nil {
		t.Fatalf("booby-trap the retired root: %v", err)
	}

	rerr := (&Restore{Target: erroringEngine{}, TargetDSN: "target://x", Store: store}).Run(ctx)
	if rerr == nil {
		t.Fatal("Restore.Run succeeded against an unreachable target; the fixture is not exercising a real run")
	}
	if ce, ok := sluicecode.FromError(rerr); ok && ce.Code == sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("audit B-6: Restore.Run LOADED the retired chain-root manifest — the file `backup prune` "+
			"keeps for the chain's identity after retiring segment 0 — instead of the catalog's floor "+
			"segment full. On an intact chain that restores the wrong (older) snapshot or fails on chunks "+
			"the prune deleted, while `backup verify` walks the floor and reports green. Error: %v", rerr)
	}
	if !errors.Is(rerr, errTargetUnreachable) {
		t.Errorf("Restore.Run = %v; want the target-open failure, which is how far a floor-dispatched "+
			"restore gets in this fixture", rerr)
	}

	// The other half of the same contract: `backup verify` and restore must
	// agree about which shape this is. They share verifyLineageNeedsWalk
	// precisely so they cannot drift.
	needsWalk, err := verifyLineageNeedsWalk(ctx, store)
	if err != nil {
		t.Fatalf("verifyLineageNeedsWalk: %v", err)
	}
	if !needsWalk {
		t.Error("verifyLineageNeedsWalk = false on a pruned-to-floor chain; verify and restore would " +
			"read different manifests for one directory")
	}
}

// signChain signs every manifest in the seeded lineage plus the catalog,
// through the REAL [lineage.ResignLineage] the maintenance paths use, and
// returns the signer the subsequent prune/compact must be given. Real
// signatures rather than placeholder bytes: the sibling objects these pins
// are about are the ones the production signing path actually writes.
func signChain(t *testing.T, store irbackup.Store) *lineage.Signer {
	t.Helper()
	_, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		t.Fatalf("GenerateEd25519Keypair: %v", err)
	}
	signer := lineage.NewEd25519Signer(priv)
	if err := lineage.ResignLineage(context.Background(), store, signer); err != nil {
		t.Fatalf("ResignLineage: %v", err)
	}
	return signer
}

// pathIn qualifies a segment-relative path against the lineage root.
func pathIn(dir, p string) string {
	if dir == "" {
		return p
	}
	return dir + "/" + p
}

// orphanSigs returns every `.sig` object in store that the catalog does not
// account for — i.e. one whose manifest is no longer a link of the chain.
// The lineage catalog's own signature is always legitimate.
//
// This is deliberately a whole-STORE sweep rather than a check of the paths
// the maintenance code touched: a gate that only looks where the fix looks
// cannot report the sibling the fix missed.
func orphanSigs(t *testing.T, store irbackup.Store) []string {
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
	legit := map[string]bool{lineage.LineageSigFileName: true}
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		legit[pathIn(seg.Dir, lineage.ManifestSigPath(seg.FullManifestPath))] = true
		for _, ip := range seg.Incrementals {
			legit[pathIn(seg.Dir, lineage.ManifestSigPath(ip))] = true
		}
	}
	var out []string
	for _, p := range all {
		if strings.HasSuffix(p, irbackup.SignatureFileSuffix) && !legit[p] {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// TestPruneRetiresSignatureSiblings is audit C-8 on the prune path: a
// manifest the prune retires must not leave a detached signature behind.
//
// A `.sig` is a claim that its manifest is link N of this chain. Nothing in
// the signature covers whether the catalog still names the file, so a
// signature left beside a retired manifest keeps VERIFYING — and the one it
// matters most for is the chain-identity `manifest.json`, which prune keeps
// on purpose (item 95). Its body is key material; its signature was a green
// answer about a manifest that had left the chain, handed to whoever checks
// signatures to decide what to trust.
func TestPruneRetiresSignatureSiblings(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := newMemStore()
	seedSegmentsWithGapsOpts(t, store, base, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{})
	signer := signChain(t, store)

	// Non-vacuity: the fixture must actually hold the signature that the
	// prune is supposed to retire.
	rootSig := lineage.ManifestSigPath(lineage.ManifestFileName)
	if ex, _ := store.Exists(ctx, rootSig); !ex {
		t.Fatalf("fixture did not sign %q; this pin would be vacuous", rootSig)
	}

	pruneToFloorFull(t, store, base, signer)

	if orphans := orphanSigs(t, store); len(orphans) > 0 {
		t.Errorf("audit C-8: %d signature(s) survived the manifest(s) they sign:\n  %s\n"+
			"Each still verifies, so a signature check reports GREEN for a manifest that is no longer "+
			"part of this chain — including the chain-identity manifest.json prune keeps for its key "+
			"material, which is exactly the artifact a mis-dispatched restore would have read.",
			len(orphans), strings.Join(orphans, "\n  "))
	}
	// The surviving floor segment keeps ITS signature: retiring the orphans
	// must not strip the chain that remains.
	cat, _, _ := lineage.LoadLineageCatalog(ctx, store)
	kept := pathIn(cat.Segments[0].Dir, lineage.ManifestSigPath(cat.Segments[0].FullManifestPath))
	if ex, _ := store.Exists(ctx, kept); !ex {
		t.Errorf("prune deleted the SURVIVING floor segment's signature %q; the retirement sweep must "+
			"only reach manifests the catalog no longer names", kept)
	}
	if ex, _ := store.Exists(ctx, lineage.LineageSigFileName); !ex {
		t.Error("prune deleted lineage.json.sig; the catalog's own signature is not a manifest sibling")
	}
	// item 95 still holds: the identity BODY survives even though its
	// signature does not.
	if ex, _ := store.Exists(ctx, lineage.ManifestFileName); !ex {
		t.Error("prune deleted the chain-root manifest body; only its signature should have been retired")
	}
}

// TestCompactRetiresSignatureSiblings is the same class on `backup
// compact`'s two sweeps — and the reason it asserts BOTH is the sibling
// step: the root-segment sweep deletes named files (it had the defect), the
// sub-dir sweep deletes a whole prefix (it did not). The exemption written
// at [sweepSegmentSubdir] is checked here rather than believed.
func TestCompactRetiresSignatureSiblings(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	store := newMemStore()
	seedSegmentsWithGapsOpts(t, store, base, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{})
	signer := signChain(t, store)

	pre, _, _ := lineage.LoadLineageCatalog(ctx, store)
	res, err := CompactChain(ctx, store, CompactOpts{MergeWindow: 24 * time.Hour, Signer: signer})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsMerged < 1 {
		t.Fatalf("compaction merged nothing (%d segments before); this pin would be vacuous", len(pre.Segments))
	}
	if orphans := orphanSigs(t, store); len(orphans) > 0 {
		t.Errorf("audit C-8 (compact): %d signature(s) survived the manifest(s) they sign:\n  %s",
			len(orphans), strings.Join(orphans, "\n  "))
	}
	if ex, _ := store.Exists(ctx, lineage.ManifestFileName); !ex {
		t.Error("compact deleted the chain-root manifest body (Bug 214); only its signature should have been retired")
	}
}

// TestPruneChain_OrphanSweepDeleteFailure_Warns is audit F-8: a delete
// failure in prune's post-commit sweep must reach a WARN naming the path,
// never vanish behind `_ =`.
//
// It is not a run failure — the catalog commit already succeeded and the
// leftovers are uncatalogued, so the chain is correctly pruned and the cost
// is disk. It is not nothing either: silence here is what turned B-6's
// "restore fails on a missing chunk" into "restore silently returns older
// data", because a mis-dispatched read found the retired segment's chunks
// still sitting where the failed sweep had left them. Prune's posture now
// matches compaction's, whose identical pin is
// TestCompactChain_OrphanSweepDeleteFailure_Warns.
func TestPruneChain_OrphanSweepDeleteFailure_Warns(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := context.Background()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	mem := newMemStore()
	seedSegmentsWithGapsOpts(t, mem, base, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{})
	// Fail every delete under the retired segment 0's conventional paths —
	// the store an expired credential, an object-lock/WORM retention
	// policy, or a read-only mount produces.
	store := &failingDeleteStore{memStore: mem, failPrefixes: []string{"chunks/", "manifests/"}}

	res, err := PruneChain(ctx, store, PruneOpts{
		KeepDuration: time.Hour,
		Now:          func() time.Time { return base.Add(1000 * time.Hour) },
	})
	if err != nil {
		t.Fatalf("PruneChain: %v — a failed orphan delete must not fail the run; the catalog commit "+
			"already succeeded", err)
	}
	if res.SegmentsDropped < 1 {
		t.Fatal("the prune dropped nothing; this pin would be vacuous")
	}
	logged := logBuf.String()
	if !strings.Contains(logged, "orphan sweep incomplete") {
		t.Errorf("audit F-8: prune discarded every delete failure — no WARN reached the log.\n"+
			"A retention run that reports segments dropped while the bytes stay is how a bounded "+
			"archive quietly stops being bounded.\nlog:\n%s", logged)
	}
	if !strings.Contains(logged, "chunks/") && !strings.Contains(logged, "manifests/") {
		t.Errorf("the WARN names no failing path, so an operator has nothing to look at:\n%s", logged)
	}
}

// TestRetireManifest_KeepBodyRetiresOnlyTheSignature is the helper's own
// two-branch pin — the asymmetry the chain-identity root depends on.
func TestRetireManifest_KeepBodyRetiresOnlyTheSignature(t *testing.T) {
	ctx := context.Background()
	for _, keepBody := range []bool{true, false} {
		t.Run(fmt.Sprintf("keepBody=%v", keepBody), func(t *testing.T) {
			store := newMemStore()
			const p = "manifest.json"
			if err := store.Put(ctx, p, strings.NewReader(`{}`)); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, lineage.ManifestSigPath(p), strings.NewReader(`{}`)); err != nil {
				t.Fatal(err)
			}
			if err := retireManifest(ctx, store, p, keepBody); err != nil {
				t.Fatalf("retireManifest: %v", err)
			}
			body, _ := store.Exists(ctx, p)
			if body != keepBody {
				t.Errorf("manifest body present = %v; want %v", body, keepBody)
			}
			if sig, _ := store.Exists(ctx, lineage.ManifestSigPath(p)); sig {
				t.Error("the detached signature survived the retirement; it still verifies, for a " +
					"manifest that is no longer a link of this chain (audit C-8)")
			}
		})
	}
}
