// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestChainRestore_TamperedBackupIDCoveredField_Refused pins audit item 57:
// editing a BackupID-covered manifest field (created_at / source_engine / kind
// / EndPosition) WITHOUT recomputing the recorded BackupID is refused at restore
// (verifyBackupIDs) with SLUICE-E-BACKUP-MANIFEST-INVALID, before any data lands
// — the corruption / lazy-tamper backstop, the BackupID twin of the schema-hash
// check. A valid chain restores clean; a legacy full with an empty BackupID is
// skipped (nothing recorded to verify).
func TestChainRestore_TamperedBackupIDCoveredField_Refused(t *testing.T) {
	ctx := context.Background()
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}}
	newStore := func(t *testing.T, mutate func(*irbackup.Manifest)) irbackup.Store {
		t.Helper()
		dir := t.TempDir()
		store, _ := blobcodec.NewLocalStore(dir)
		full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
		full.Schema = schema
		full.BackupID = irbackup.ComputeBackupID(full)
		mutate(full) // tamper a covered field AFTER stamping the id, or clear it
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("write full: %v", err)
		}
		_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
		return store
	}
	run := func(store irbackup.Store) error {
		tgt := &chainRestoreRecorderEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
		return (&backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
	}
	t.Run("valid chain restores clean", func(t *testing.T) {
		if err := run(newStore(t, func(*irbackup.Manifest) {})); err != nil {
			t.Fatalf("valid chain refused: %v", err)
		}
	})
	t.Run("tampered created_at (covered field) without recomputing BackupID — REFUSED", func(t *testing.T) {
		store := newStore(t, func(m *irbackup.Manifest) { m.CreatedAt = m.CreatedAt.Add(time.Hour) })
		assertCoded(t, run(store), sluicecode.CodeBackupManifestInvalid)
	})
	t.Run("legacy full with empty BackupID — skipped (restores clean)", func(t *testing.T) {
		if err := run(newStore(t, func(m *irbackup.Manifest) { m.BackupID = "" })); err != nil {
			t.Fatalf("legacy empty-BackupID full refused: %v", err)
		}
	})
	t.Run("FV8 fold: flipping CDCPositionCommitsAfterRows without recomputing — REFUSED (item 57 fold)", func(t *testing.T) {
		// The item-57 fold makes CDCPositionCommitsAfterRows a BackupID-covered
		// field for a FormatVersion-8 manifest, so an unsigned flip of it — the
		// flag that decides whether restore trusts a schema anchor at
		// EndPosition (Bug 184) — is caught the same way a created_at edit is.
		// (A full fixture is used for setup simplicity; the fold binds the flag
		// for any FV8 manifest regardless of kind.)
		dir := t.TempDir()
		store, _ := blobcodec.NewLocalStore(dir)
		full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
		full.Schema = schema
		full.CDCPositionCommitsAfterRows = true
		irbackup.StampCDCPositionBinding(full) // FormatVersion -> 8
		full.BackupID = irbackup.ComputeBackupID(full)
		full.CDCPositionCommitsAfterRows = false // tamper: flip WITHOUT recomputing
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("write full: %v", err)
		}
		_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
		assertCoded(t, run(store), sluicecode.CodeBackupManifestInvalid)
	})
}

// makeManifest returns a manifest with deterministic CreatedAt and
// position for chain-walk test fixtures.
func makeManifest(t *testing.T, kind string, parent *irbackup.Manifest, lsn string) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
		Kind:          kind,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"` + lsn + `"}`},
	}
	if parent != nil {
		m.ParentBackupID = lineage.ManifestBackupID(parent)
		m.StartPosition = parent.EndPosition
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

// seedSegment writes a single segment's full + incrementals into a
// per-segment store and returns the lineage.Segment describing it. dir
// == "" is the root (one-segment) layout.
func seedSegment(t *testing.T, root irbackup.Store, dir string, full *irbackup.Manifest, incrs []*irbackup.Manifest, codec blobcodec.Codec) lineage.Segment {
	t.Helper()
	ss := lineage.NewPrefixedStore(root, dir)
	if err := lineage.WriteManifestAt(context.Background(), ss, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write seg full: %v", err)
	}
	seg := lineage.Segment{
		SegmentID:        lineage.ManifestBackupID(full),
		Dir:              dir,
		FullManifestPath: lineage.ManifestFileName,
		StartPosition:    full.EndPosition,
		EndPosition:      full.EndPosition,
		Codec:            codec,
	}
	for i, m := range incrs {
		p := "manifests/incr-" + fmt.Sprintf("%04d", i) + ".json"
		if err := lineage.WriteManifestAt(context.Background(), ss, p, m); err != nil {
			t.Fatalf("write seg incr: %v", err)
		}
		seg.Incrementals = append(seg.Incrementals, p)
		seg.EndPosition = m.EndPosition
	}
	return seg
}

func TestBuildLineageChain_SingleSegmentNoIncrementals(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	_ = lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full)
	// No lineage.json — lineage.ResolveLineage synthesises a one-segment
	// lineage; behaviour byte-identical to a pre-ADR single full.
	chain, err := lineage.BuildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("lineage.BuildLineageChain: %v", err)
	}
	if len(chain) != 1 || lineage.CanonicalKind(chain[0].Manifest.Kind) != irbackup.BackupKindFull {
		t.Errorf("chain = %+v; want one full link", chain)
	}
}

func TestBuildLineageChain_LinearOK(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	incr1 := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")
	incr2 := makeManifest(t, irbackup.BackupKindIncremental, incr1, "0/300")
	seg := seedSegment(t, store, "", full, []*irbackup.Manifest{incr1, incr2}, blobcodec.CodecGzip)
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{seg}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	chain, err := lineage.BuildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("lineage.BuildLineageChain: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain len = %d; want 3", len(chain))
	}
	if lineage.ManifestBackupID(chain[2].Manifest) != lineage.ManifestBackupID(incr2) {
		t.Errorf("chain[2] is not incr2")
	}
}

// TestBuildLineageChain_MultiSegmentBoundaryOK proves a 3-segment
// lineage walks end-to-end when seg[i].end == seg[i+1].start.
func TestBuildLineageChain_MultiSegmentBoundaryOK(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	f0 := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, irbackup.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, store, "", f0, []*irbackup.Manifest{i0}, blobcodec.CodecGzip)

	// seg1 full's StartPosition == seg0.end (0/200). makeManifest sets
	// EndPosition from the lsn arg; force StartPosition = prior end.
	f1 := makeManifest(t, irbackup.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = irbackup.ComputeBackupID(f1)
	i1 := makeManifest(t, irbackup.BackupKindIncremental, f1, "0/400")
	s1 := seedSegment(t, store, "seg-1", f1, []*irbackup.Manifest{i1}, blobcodec.CodecNone)

	f2 := makeManifest(t, irbackup.BackupKindFull, nil, "0/500")
	f2.StartPosition = i1.EndPosition
	f2.BackupID = irbackup.ComputeBackupID(f2)
	s2 := seedSegment(t, store, "seg-2", f2, nil, blobcodec.CodecZstd)

	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	s1.CappedAt, s1.CapReason = &capt, rotationReasonChainLength
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{s0, s1, s2}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	chain, err := lineage.BuildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("lineage.BuildLineageChain (valid 3-segment): %v", err)
	}
	// f0,i0,f1,i1,f2 = 5 links.
	if len(chain) != 5 {
		t.Fatalf("chain len = %d; want 5", len(chain))
	}
}

// TestBuildBrokerChain_MultiSegmentFollows pins the post-Round-D
// closure of the Phase 4.5 multi-segment-broker deferral. Pre-fix
// lineage.BuildBrokerChain refused loudly on any chain with >1 segment with
// the documented "Broker following a multi-segment lineage is deferred
// (ADR-0046 Phase 4.5); ..." error. Post-fix, the broker walks the
// full lineage via lineage.BuildLineageChain — same code path sluice restore
// uses for multi-segment chains.
//
// The broker's apply loop (broker.go::replayNewIncrementals) skips
// any link whose Kind is BackupKindFull, so segment-N+1's rotation
// snapshot is auto-skipped and the broker continues with the new
// segment's incremental tail. ADR-0067's born-contiguous rotation
// guarantees that tail's first incremental covers the (P_N, S]
// overlap from the prior segment's end; ADR-0010's idempotent
// applier handles the brief re-application of any changes that
// landed between the broker's last advance and the rotation moment.
//
// This test pins the CHAIN ASSEMBLY shape (no refusal, all manifests
// from all segments present in chain order). The end-to-end apply-
// across-rotation invariant is pinned by the broker integration
// tests (TestSyncFromBackup_Postgres_HappyPath + the Round D soak
// re-run against the fixed binary).
func TestBuildBrokerChain_MultiSegmentFollows(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	// 3-segment lineage, same shape as
	// TestBuildLineageChain_MultiSegmentBoundaryOK.
	f0 := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, irbackup.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, store, "", f0, []*irbackup.Manifest{i0}, blobcodec.CodecGzip)

	f1 := makeManifest(t, irbackup.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = irbackup.ComputeBackupID(f1)
	i1 := makeManifest(t, irbackup.BackupKindIncremental, f1, "0/400")
	s1 := seedSegment(t, store, "seg-1", f1, []*irbackup.Manifest{i1}, blobcodec.CodecNone)

	f2 := makeManifest(t, irbackup.BackupKindFull, nil, "0/500")
	f2.StartPosition = i1.EndPosition
	f2.BackupID = irbackup.ComputeBackupID(f2)
	s2 := seedSegment(t, store, "seg-2", f2, nil, blobcodec.CodecZstd)

	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	s1.CappedAt, s1.CapReason = &capt, rotationReasonChainLength
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{s0, s1, s2}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}

	chain, err := lineage.BuildBrokerChain(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.BuildBrokerChain (3-segment): unexpected refusal: %v", err)
	}
	// f0,i0,f1,i1,f2 = 5 links across 3 segments.
	if len(chain) != 5 {
		t.Fatalf("chain len = %d; want 5 (f0,i0,f1,i1,f2)", len(chain))
	}

	// Verify chain ordering: full → incrementals within each segment,
	// segments in lineage order.
	expectedKinds := []string{
		irbackup.BackupKindFull, irbackup.BackupKindIncremental, // seg0
		irbackup.BackupKindFull, irbackup.BackupKindIncremental, // seg1
		irbackup.BackupKindFull, // seg2 (no incrementals)
	}
	for i, want := range expectedKinds {
		got := lineage.CanonicalKind(chain[i].Manifest.Kind)
		if got != want {
			t.Errorf("chain[%d].Kind = %q; want %q", i, got, want)
		}
	}
}

// TestBuildBrokerChain_DeferralRemoved asserts the literal Phase 4.5
// refusal message is no longer emitted on multi-segment lineages. The
// prior message was load-bearing for operator-visible behavior; this
// test pins that it's gone (anyone re-introducing the refusal trips
// this assertion). Single-segment behavior is byte-identical to the
// pre-deferral-removed code path.
func TestBuildBrokerChain_DeferralRemoved(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	// Minimal 2-segment lineage to trigger the prior multi-segment path.
	f0 := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	s0 := seedSegment(t, store, "", f0, nil, blobcodec.CodecGzip)
	f1 := makeManifest(t, irbackup.BackupKindFull, nil, "0/200")
	f1.StartPosition = f0.EndPosition
	f1.BackupID = irbackup.ComputeBackupID(f1)
	s1 := seedSegment(t, store, "seg-1", f1, nil, blobcodec.CodecNone)
	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{s0, s1}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}

	_, err := lineage.BuildBrokerChain(context.Background(), store)
	if err != nil {
		// Any error here is unexpected — the chain is well-formed.
		t.Fatalf("lineage.BuildBrokerChain on valid 2-segment lineage: unexpected error %v", err)
	}
}

// TestValidateBoundary_SameCodePathIntraAndInter proves the SINGLE
// boundary validator is the SAME function for an intra-segment
// incremental boundary (exact=true, contiguous) AND a
// segment-to-segment boundary (exact=false, monotonic) — one check
// site, ADR-0046 §3. A token-ranking comparator stands in for the
// engine's PositionMonotonicChecker.
func TestValidateBoundary_SameCodePathIntraAndInter(t *testing.T) {
	prevEnd := ir.Position{Engine: "postgres", Token: "0/200"}
	eq := ir.Position{Engine: "postgres", Token: "0/200"}
	ahead := ir.Position{Engine: "postgres", Token: "0/300"}
	behind := ir.Position{Engine: "postgres", Token: "0/199"}
	cmp := &fakeMonotonicEngine{order: map[string]int{"0/199": 199, "0/200": 200, "0/300": 300}}

	// SAME lineage.ValidateBoundary, intra-segment (exact=true): contiguous OK,
	// any non-equality (gap OR regression) is a loud mismatch.
	if err := lineage.ValidateBoundary(cmp, prevEnd, eq, true, "seg0 link1", "seg0 incrX"); err != nil {
		t.Errorf("intra contiguous: err = %v; want nil", err)
	}
	if err := lineage.ValidateBoundary(cmp, prevEnd, ahead, true, "seg0 link1", "seg0 incrX"); err == nil ||
		!strings.Contains(err.Error(), "lineage boundary mismatch") {
		t.Errorf("intra forward-gap: err = %v; want loud mismatch (exact)", err)
	}
	if err := lineage.ValidateBoundary(cmp, prevEnd, behind, true, "seg0 link1", "seg0 incrX"); err == nil {
		t.Errorf("intra regression: err = nil; want loud mismatch")
	}
	// SAME lineage.ValidateBoundary, inter-segment (exact=false): equal OR
	// ahead OK (S >= P_N); only a REGRESSION is a loud refusal.
	if err := lineage.ValidateBoundary(cmp, prevEnd, eq, false, "seg0 last", "seg1 start"); err != nil {
		t.Errorf("inter equal: err = %v; want nil", err)
	}
	if err := lineage.ValidateBoundary(cmp, prevEnd, ahead, false, "seg0 last", "seg1 start"); err != nil {
		t.Errorf("inter S>P_N (ahead): err = %v; want nil (monotonic OK)", err)
	}
	if err := lineage.ValidateBoundary(cmp, prevEnd, behind, false, "seg0 last", "seg1 start"); err == nil ||
		!strings.Contains(err.Error(), "REGRESSION") {
		t.Errorf("inter regression: err = %v; want loud REGRESSION refusal", err)
	}
	// Empty prevEnd tolerance (legacy v0.16 full) — skip either mode.
	if err := lineage.ValidateBoundary(cmp, ir.Position{}, behind, true, "p", "c"); err != nil {
		t.Errorf("empty-prev tolerance: err = %v; want nil", err)
	}
}

// TestBuildLineageChain_SegmentBoundaryRegressionRefuses: a
// position-regression across a segment boundary is a LOUD refusal
// (DR data — never a silent partial assemble).
func TestBuildLineageChain_SegmentBoundaryRegressionRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	f0 := makeManifest(t, irbackup.BackupKindFull, nil, "END0")
	s0 := seedSegment(t, store, "", f0, nil, blobcodec.CodecGzip)
	f1 := makeManifest(t, irbackup.BackupKindFull, nil, "END1")
	s1 := seedSegment(t, store, "seg-1", f1, nil, blobcodec.CodecGzip)
	// seg1's RECORDED StartPosition REGRESSES before seg0's end
	// (a tampered / corrupt lineage.json — DR data).
	s1.StartPosition = ir.Position{Engine: "postgres", Token: "BEFORE0"}
	capt := time.Now().UTC()
	s0.CappedAt = &capt
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{s0, s1}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	// Ranking comparator: f0.End ("END0") ranks AFTER the regressed
	// seg1 start ("BEFORE0") -> inter-segment monotonic check fails.
	cmp := &fakeMonotonicEngine{order: map[string]int{
		`{"slot":"sluice_slot","lsn":"END0"}`: 200,
		"BEFORE0":                             100,
		`{"slot":"sluice_slot","lsn":"END1"}`: 300,
	}}
	_, err := lineage.BuildLineageChain(context.Background(), store, cmp)
	if err == nil || !strings.Contains(err.Error(), "REGRESSION") {
		t.Errorf("err = %v; want loud segment-boundary REGRESSION refusal", err)
	}
}

func TestBuildLineageChain_IntraSegmentMismatchRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	tampered := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")
	tampered.StartPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"WRONG"}`}
	tampered.BackupID = irbackup.ComputeBackupID(tampered)
	seg := seedSegment(t, store, "", full, []*irbackup.Manifest{tampered}, blobcodec.CodecGzip)
	cat := &lineage.Catalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []lineage.Segment{seg}}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	_, err := lineage.BuildLineageChain(context.Background(), store, nil)
	if err == nil || !strings.Contains(err.Error(), "lineage boundary mismatch") {
		t.Errorf("err = %v; want intra-segment boundary refusal", err)
	}
}

// TestBuildLineageChain_MissingFullRefuses: a segment whose recorded
// full manifest is absent is a loud refusal.
func TestBuildLineageChain_MissingFullRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	cat := &lineage.Catalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []lineage.Segment{{SegmentID: "s0", Dir: "", FullManifestPath: lineage.ManifestFileName, Codec: blobcodec.CodecGzip}},
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	_, err := lineage.BuildLineageChain(context.Background(), store, nil)
	if err == nil || !strings.Contains(err.Error(), "full") {
		t.Errorf("err = %v; want missing-full refusal", err)
	}
}

func TestDetectAmbiguousDeltas_RenameRefuses(t *testing.T) {
	deltas := []*irbackup.SchemaDeltaEntry{
		{
			Kind:  irbackup.SchemaDeltaAlterTable,
			Table: "users",
			Before: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "username", Type: ir.Varchar{Length: 50}},
			}},
			After: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "login", Type: ir.Varchar{Length: 50}}, // rename!
			}},
		},
	}
	if err := lineage.DetectAmbiguousDeltas(deltas); err == nil {
		t.Errorf("lineage.DetectAmbiguousDeltas: nil; want refusal on rename ambiguity")
	}
}

func TestDetectAmbiguousDeltas_AddOnlyOK(t *testing.T) {
	deltas := []*irbackup.SchemaDeltaEntry{
		{
			Kind:  irbackup.SchemaDeltaAlterTable,
			Table: "users",
			Before: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			}},
			After: &ir.Table{Name: "users", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
			}},
		},
	}
	if err := lineage.DetectAmbiguousDeltas(deltas); err != nil {
		t.Errorf("lineage.DetectAmbiguousDeltas: %v; want clean", err)
	}
}

// chainRestoreRecorderEngine: a target engine that records every
// schema-write phase and every applier event so chain-restore tests
// can assert ordering and content.
type chainRestoreRecorderEngine struct {
	*restoreRecorderEngine
	mu       sync.Mutex
	applied  []ir.Change
	applierC *chainRestoreRecordingApplier
}

func (e *chainRestoreRecorderEngine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	if e.applierC == nil {
		e.applierC = &chainRestoreRecordingApplier{owner: e}
	}
	return e.applierC, nil
}

type chainRestoreRecordingApplier struct {
	owner *chainRestoreRecorderEngine
}

func (a *chainRestoreRecordingApplier) EnsureControlTable(_ context.Context) error { return nil }
func (a *chainRestoreRecordingApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *chainRestoreRecordingApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *chainRestoreRecordingApplier) Apply(_ context.Context, _ string, changes <-chan ir.Change) error {
	for c := range changes {
		a.owner.mu.Lock()
		a.owner.applied = append(a.owner.applied, c)
		a.owner.mu.Unlock()
	}
	return nil
}

func (a *chainRestoreRecordingApplier) RequestStop(context.Context, string) error { return nil }

func (a *chainRestoreRecordingApplier) ClearStopRequested(context.Context, string) error { return nil }

func (a *chainRestoreRecordingApplier) Close() error { return nil }

// TestChainRestore_FullPlusOneIncremental_RoundTrip is the load-bearing
// end-to-end test for Phase 3.2 acceptance criterion 2: write a full
// + one incremental, restore the chain, and verify the applied
// change events match what was streamed into the incremental.
func TestChainRestore_FullPlusOneIncremental_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	// Full backup via the existing Backup pipeline.
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	})
	if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	// Patch the full's manifest with an EndPosition + BackupID so the
	// incremental can chain off it. (The full backup pipeline
	// doesn't yet record EndPosition for fulls — Phase 3.3 work.)
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	// Incremental: feed a recorded set of changes through the
	// IncrementalBackup orchestrator.
	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`},
				Table:    "users",
				Row:      ir.Row{"id": int64(3)},
			},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
	}
	ib := &IncrementalBackup{
		Source:    cdc,
		SourceDSN: "src",
		Store:     store,
		ParentRef: full.BackupID,
		Window:    5 * time.Minute,
	}
	if err := ib.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// Restore via the chain path. Override Restore.Run's chain
	// dispatch by calling ChainRestore directly to keep the test
	// independent of the dispatcher.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	// Phase ordering: the full's CreateTablesWithoutConstraints must
	// have run before any change-apply.
	phases, _ := tgt.snapshot()
	if len(phases) == 0 || phases[0] != "CreateTablesWithoutConstraints" {
		t.Errorf("phase[0] = %v; want CreateTablesWithoutConstraints first", phases)
	}

	// Applier received the 3 incremental changes (tx_begin + insert
	// + tx_commit).
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("applied changes = %d; want 3 (got %v)", len(got), got)
	}
	if _, ok := got[0].(ir.TxBegin); !ok {
		t.Errorf("got[0] = %T; want TxBegin", got[0])
	}
	ins, ok := got[1].(ir.Insert)
	if !ok {
		t.Errorf("got[1] = %T; want Insert", got[1])
	} else if ins.Table != "users" || !valuesEquivalent(ins.Row["id"], int64(3)) {
		t.Errorf("got[1] = %+v; want users.id=3", ins)
	}
	if _, ok := got[2].(ir.TxCommit); !ok {
		t.Errorf("got[2] = %T; want TxCommit", got[2])
	}
}

// TestChainRestore_SchemaHistoryReplayed pins ADR-0049 Chunk D's
// restore-side contract: SchemaHistory entries on the incremental
// manifest are replayed onto the applier as ir.SchemaSnapshot events
// BEFORE the row-shaped changes (so a subsequent sync resume at
// backup.EndPosition finds the post-DDL schema version in the target's
// sluice_cdc_schema_history, NOT the loud cold-start floor).
//
// Idempotency: re-running the chain restore must NOT duplicate the
// snapshots fed to the applier (the engine's writeSchemaVersion is
// UPSERT-on-PK; this test verifies the orchestrator-side count, the
// engine's idempotency is covered by the engine integration tests).
func TestChainRestore_SchemaHistoryReplayed(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	postDDL := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
		},
	}
	postDDLPayload, err := ir.MarshalTable(postDDL)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}

	// Full manifest at 0/100.
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	// Incremental: carries SchemaHistory + a single Insert event. EndPosition
	// matches the single change's position (0/180) — the real writer invariant
	// the F1 tail-truncation backstop relies on.
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/180")
	incr.Schema = &ir.Schema{Tables: []*ir.Table{postDDL}}
	incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{
		{
			StreamID:       "",
			Schema:         "",
			Table:          "users",
			AnchorPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/150"}`},
			TableJSON:      postDDLPayload,
		},
	}
	// Build a single-Insert change chunk via the writer.
	buf := &bytes.Buffer{}
	cw, err := blobcodec.NewChangeChunkWriter(buf, nil, blobcodec.CodecGzip, nil)
	if err != nil {
		t.Fatalf("newChangeChunkWriter: %v", err)
	}
	row := ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/180"}`},
		Schema:   "",
		Table:    "users",
		Row:      ir.Row{"id": int64(7), "email": "x@example.com"},
	}
	if err := cw.WriteChange(row); err != nil {
		t.Fatalf("WriteChange: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("cw.Close: %v", err)
	}
	chunkPath := "chunks/_changes/test/changes-0.jsonl.gz"
	if err := store.Put(context.Background(), chunkPath, buf); err != nil {
		t.Fatalf("store.Put: %v", err)
	}
	incr.ChangeChunks = []*irbackup.ChunkInfo{{
		File:     chunkPath,
		RowCount: 1,
		SHA256:   cw.Hash(),
	}}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, blobcodec.CodecGzip)

	// Run restore.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	// Applier MUST have seen the SchemaSnapshot first, then the Insert.
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("applied changes = %d; want 2 (1 SchemaSnapshot + 1 Insert); got %v", len(got), got)
	}
	snap, ok := got[0].(ir.SchemaSnapshot)
	if !ok {
		t.Fatalf("got[0] = %T; want SchemaSnapshot (must precede the row event)", got[0])
	}
	if snap.Table != "users" {
		t.Errorf("snap.Table = %q; want users", snap.Table)
	}
	if snap.IR == nil || len(snap.IR.Columns) != 2 {
		t.Errorf("snap.IR shape mismatch; got %+v", snap.IR)
	}
	wantAnchor := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/150"}`}
	if snap.Position != wantAnchor {
		t.Errorf("snap.Position = %+v; want %+v", snap.Position, wantAnchor)
	}
	if _, ok := got[1].(ir.Insert); !ok {
		t.Errorf("got[1] = %T; want Insert", got[1])
	}
}

// TestChainRestore_SchemaHistoryOnlyManifestReplayed pins the
// "SchemaHistory present but ChangeChunks empty" branch: a window
// observed a DDL but no DML, so the incremental carries only schema
// history. The applier must still receive the synthetic SchemaSnapshot
// (previously the early-return on empty ChangeChunks would skip).
func TestChainRestore_SchemaHistoryOnlyManifestReplayed(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	postDDL := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
		},
	}
	postDDLPayload, _ := ir.MarshalTable(postDDL)

	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")
	incr.Schema = &ir.Schema{Tables: []*ir.Table{postDDL}}
	// Model the ground-truth legit DDL-only window (item60_anchor_schemadelta_*
	// integration tests, real PG + MySQL): a pure DDL-only window emits its
	// snapshot with an EMPTY EndPosition, so the F1 completeness guard is skipped
	// (posBearing false) and the synthetic SchemaSnapshot is still delivered —
	// the branch this test pins. (Before audit-2026-07-12 this fixture modeled an
	// anchor AT an advancing EndPosition, which ground truth shows is only ever a
	// store adversary's emptied-DATA forgery, not a legit shape.)
	incr.EndPosition = ir.Position{}
	incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{
		{
			Schema:    "",
			Table:     "users",
			TableJSON: postDDLPayload,
		},
	}
	incr.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
		Kind:  irbackup.SchemaDeltaAlterTable,
		Table: "users",
		After: postDDL,
	}}
	incr.ChangeChunks = nil // no DML
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, blobcodec.CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("applied changes = %d; want 1 (schema-history-only replay); got %v", len(got), got)
	}
	if _, ok := got[0].(ir.SchemaSnapshot); !ok {
		t.Errorf("got[0] = %T; want SchemaSnapshot", got[0])
	}
}

// TestChainRestore_SchemaHistoryDecodeFailureIsLoud pins the loud-
// failure contract for a corrupt SchemaHistory entry: a nil table-
// JSON decode produces a clear refusal (NOT a silent skip — that's
// the exact silent-mis-decode class ADR-0049 exists to kill).
func TestChainRestore_SchemaHistoryDecodeFailureIsLoud(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/200")
	// Corrupt SchemaHistory entry: TableJSON is "null" → UnmarshalTable
	// returns (nil, nil); orchestrator must refuse loudly.
	incr.SchemaHistory = []*irbackup.SchemaHistoryEntry{
		{
			Schema:         "",
			Table:          "users",
			AnchorPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/150"}`},
			TableJSON:      []byte("null"),
		},
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, blobcodec.CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	err := chain.Run(context.Background())
	if err == nil {
		t.Fatal("corrupt SchemaHistory must refuse LOUDLY; got nil err")
	}
	if !strings.Contains(err.Error(), "schema") || !strings.Contains(err.Error(), "nil table") {
		t.Errorf("err must mention schema-history + nil-table; got %v", err)
	}
}

// TestChainRestore_PreChunkDManifest_BackwardCompat pins that a
// manifest WITHOUT SchemaHistory (pre-Chunk-D shape) restores cleanly:
// the orchestrator must NOT require the new field. The applier just
// gets the row events with no preceding SchemaSnapshot — the
// documented pre-D state.
func TestChainRestore_PreChunkDManifest_BackwardCompat(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	// EndPosition matches the single change's position (0/180) per the real
	// writer invariant the F1 tail-truncation backstop relies on.
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/180")
	incr.Schema = schema
	// SchemaHistory deliberately nil (pre-Chunk-D shape).
	incr.SchemaHistory = nil
	buf := &bytes.Buffer{}
	cw, _ := blobcodec.NewChangeChunkWriter(buf, nil, blobcodec.CodecGzip, nil)
	_ = cw.WriteChange(ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/180"}`},
		Table:    "users",
		Row:      ir.Row{"id": int64(9)},
	})
	_ = cw.Close()
	chunkPath := "chunks/_changes/preD/changes-0.jsonl.gz"
	_ = store.Put(context.Background(), chunkPath, buf)
	incr.ChangeChunks = []*irbackup.ChunkInfo{{
		File:     chunkPath,
		RowCount: 1,
		SHA256:   cw.Hash(),
	}}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	_ = lineage.WriteManifestAt(context.Background(), store, incrPath, incr)
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, blobcodec.CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
	}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("pre-Chunk-D manifest must restore cleanly: %v", err)
	}
	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("applied changes = %d; want 1 (Insert only, no synthetic snapshot); got %v", len(got), got)
	}
	if _, ok := got[0].(ir.Insert); !ok {
		t.Errorf("got[0] = %T; want Insert", got[0])
	}
}

// TestChainRestore_CrossEngineWithIncrementalsSucceeds verifies that
// Phase 5's cross-engine routing lifts the v0.17.0–v0.20.x refusal:
// a chain whose source engine differs from the target engine now
// runs through the restore + applier pipeline. Same shape, different
// outcome: the test that used to assert refusal now asserts the
// no-incremental-data path completes cleanly. Cross-engine acceptance
// criteria 1–4 are validated end-to-end by the integration tests in
// chain_restore_cross_integration_test.go.
func TestChainRestore_CrossEngineWithIncrementalsSucceeds(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	// Full manifest: source_engine=postgres.
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.SourceEngine = "postgres"
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	// A no-op incremental (no change chunks): its EndPosition does NOT advance
	// past StartPosition — the real writer records the last change's position,
	// and a 0-change window never advances it (Bug 183: a 0-chunk incremental
	// with an ADVANCED EndPosition is the emptied-list attack shape and is
	// refused).
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/100")
	incr.SourceEngine = "postgres"
	incr.BackupID = irbackup.ComputeBackupID(incr)
	if err := lineage.WriteManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	// Target engine = mysql, distinct from the chain's source. The
	// recorder engine handles every phase as a no-op + record; the
	// incremental has no change chunks so the applier is exercised
	// only via OpenChangeApplier + EnsureControlTable.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	chain := &backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("chain restore: %v", err)
	}
	phases, _ := tgt.snapshot()
	if len(phases) == 0 || phases[0] != "CreateTablesWithoutConstraints" {
		t.Errorf("phase[0] = %v; want CreateTablesWithoutConstraints first", phases)
	}
}

// TestChainRestore_CrossEnginePostGISNowSupported documents the
// post-ADR-0035 behaviour: PG → MySQL geometry round-trips with SRID
// preserved (Bug 26 closed in v0.28.0). The unit-level refusal-shape
// test for the remaining unsupportable cross-engine type
// (ir.ExtensionType, ADR-0032 pgvector / hstore) lives in
// cross_engine_supportable_test.go where it doesn't need to round-
// trip a manifest (ExtensionType isn't representable in the backup-
// manifest envelope today). This test stays as a placeholder so the
// chunk's acceptance-criteria list maps 1:1 to a test name.
func TestChainRestore_CrossEnginePostGISNowSupported(t *testing.T) {
	t.Skip("PostGIS cross-engine geometry support landed in v0.28.0 (ADR-0035); see TestCheckCrossEngineSupportable_PGtoMySQL_ExtensionTypeRefuses for the remaining unsupportable-shape refusal.")
}

// TestChainRestore_DispatchFromRestore_Run verifies the existing
// Restore.Run delegates to ChainRestore when incrementals are
// present.
func TestChainRestore_DispatchFromRestore_Run(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"users": {{"id": int64(1)}}})
	_ = (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background())

	// Patch the full with an EndPosition.
	full, _ := lineage.ReadManifest(context.Background(), store)
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/100"}`}
	full.BackupID = irbackup.ComputeBackupID(full)
	_ = lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full)

	// Add an empty incremental so the dispatch fires.
	cdc := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema, schema},
		cdcChanges:     []ir.Change{},
	}
	if err := (&IncrementalBackup{
		Source: cdc, SourceDSN: "src", Store: store,
		ParentRef: full.BackupID,
		Window:    1 * time.Millisecond, // close immediately
	}).Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup: %v", err)
	}

	// Now Restore.Run with the standard recorder should detect the
	// incremental and dispatch.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	r := &backup.Restore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	// Successful chain restore implies the dispatch fired (otherwise
	// the recorder's applier would never have been called and we'd
	// surface a different error).
	phases, _ := tgt.snapshot()
	if len(phases) == 0 {
		t.Errorf("no phases recorded; restore must have dispatched to chain")
	}
}

// writeTestChangeChunk serialises changes into one change chunk at
// path within store and returns its manifest ChunkInfo. Shared fixture
// for the multi-chunk replay pins below.
func writeTestChangeChunk(t *testing.T, store irbackup.Store, path string, changes []ir.Change) *irbackup.ChunkInfo {
	t.Helper()
	buf := &bytes.Buffer{}
	cw, err := blobcodec.NewChangeChunkWriter(buf, nil, blobcodec.CodecGzip, nil)
	if err != nil {
		t.Fatalf("newChangeChunkWriter: %v", err)
	}
	for _, c := range changes {
		if err := cw.WriteChange(c); err != nil {
			t.Fatalf("WriteChange: %v", err)
		}
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("cw.Close: %v", err)
	}
	info := &irbackup.ChunkInfo{File: path, RowCount: int64(len(changes)), SHA256: cw.Hash()}
	if err := store.Put(context.Background(), path, buf); err != nil {
		t.Fatalf("store.Put(%s): %v", path, err)
	}
	return info
}

// seedMultiChunkIncremental writes a full + one incremental carrying
// nine ordered Inserts split across three change chunks (3-3-3), and
// returns the incremental's manifest. Fixture for the one-chunk
// read-ahead pins.
func seedMultiChunkIncremental(t *testing.T, store irbackup.Store, schema *ir.Schema) *irbackup.Manifest {
	t.Helper()
	full := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	// EndPosition MUST equal the LAST change's position — the real
	// incremental/stream writer sets manifest.EndPosition = lastPos (the
	// last position-bearing change). The last change here is id=9 →
	// "0/209"; the F1 tail-truncation backstop asserts the replayed tail
	// reaches EndPosition, so a fixture with an EndPosition ahead of its
	// last change would (correctly) be refused as a short tail.
	incr := makeManifest(t, irbackup.BackupKindIncremental, full, "0/209")
	incr.Schema = schema
	for chunkIdx := 0; chunkIdx < 3; chunkIdx++ {
		var changes []ir.Change
		for j := 0; j < 3; j++ {
			id := chunkIdx*3 + j + 1
			changes = append(changes, ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"slot":"sluice_slot","lsn":"0/2%02d"}`, id)},
				Table:    "users",
				Row:      ir.Row{"id": int64(id)},
			})
		}
		incr.ChangeChunks = append(incr.ChangeChunks,
			writeTestChangeChunk(t, store, fmt.Sprintf("chunks/_changes/test/changes-%d.jsonl.gz", chunkIdx), changes))
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := lineage.WriteManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, blobcodec.CodecGzip)
	return incr
}

// TestChainRestore_MultiChunkReplay_PrefetchPreservesOrder pins the
// one-chunk read-ahead in the incremental replay (perf-parity matrix
// gap 4): with an incremental split across three change chunks, the
// applier must see every change in exact manifest order — the fetcher
// goroutine overlaps chunk N+1's fetch with chunk N's apply but the
// unbuffered handoff keeps the apply strictly sequential.
func TestChainRestore_MultiChunkReplay_PrefetchPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	seedMultiChunkIncremental(t, store, schema)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	tgt.mu.Lock()
	got := append([]ir.Change(nil), tgt.applied...)
	tgt.mu.Unlock()
	if len(got) != 9 {
		t.Fatalf("applied changes = %d; want 9 (3 chunks x 3 inserts); got %v", len(got), got)
	}
	for i, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("got[%d] = %T; want Insert", i, c)
		}
		if !valuesEquivalent(ins.Row["id"], int64(i+1)) {
			t.Errorf("got[%d].id = %v; want %d (apply order must match manifest chunk order exactly)", i, ins.Row["id"], i+1)
		}
	}
}

// TestChainRestore_CorruptChangeChunk_FailsLoud pins the read-ahead's
// error path: a change chunk whose stored bytes no longer match the
// manifest SHA must fail the restore LOUDLY, naming the chunk, after
// the ADR-0117 bounded re-fetch attempts — and must not deadlock the
// fetcher/consumer pair (a hang here would time the test out).
func TestChainRestore_CorruptChangeChunk_FailsLoud(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	incr := seedMultiChunkIncremental(t, store, schema)

	// Corrupt the MIDDLE chunk so the failure surfaces while chunk 0
	// is already applying — the read-ahead's in-flight case.
	corrupt := incr.ChangeChunks[1].File
	if err := store.Put(context.Background(), corrupt, strings.NewReader("not the recorded bytes")); err != nil {
		t.Fatalf("corrupt chunk: %v", err)
	}

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	err := chain.Run(context.Background())
	if err == nil {
		t.Fatal("ChainRestore.Run succeeded on a corrupt change chunk; want a loud hash-mismatch failure")
	}
	if !strings.Contains(err.Error(), corrupt) || !strings.Contains(err.Error(), "open chunk") {
		t.Errorf("error should name the corrupt chunk %q via the open-chunk path; got: %v", corrupt, err)
	}
}

// TestChainRestore_IdentitySequencesSyncedAtTail_Dispatch pins the
// chain-tail identity re-sync wiring (roadmap "Open bugs", filed
// 2026-07-03): a chain whose schema carries an identity column must
// invoke SyncIdentitySequences TWICE — once inside the base full's
// restore (restore.go Phase 3) and once at the chain tail, AFTER the
// incremental links applied their rows. Pre-fix the tail call did not
// exist and the count was 1. The real 23505 shape is pinned by
// TestBackup_ChainRestore_IdentitySequenceSyncedAtTail (integration).
func TestChainRestore_IdentitySequencesSyncedAtTail_Dispatch(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}},
	}}}
	seedMultiChunkIncremental(t, store, schema)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &backup.ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
	if err := chain.Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	phases, _ := tgt.snapshot()
	syncs := 0
	for _, p := range phases {
		if p == "SyncIdentitySequences" {
			syncs++
		}
	}
	if syncs != 2 {
		t.Errorf("SyncIdentitySequences ran %d time(s); want 2 (base full + chain tail); phases=%v", syncs, phases)
	}
	if len(phases) == 0 || phases[len(phases)-1] != "SyncIdentitySequences" {
		t.Errorf("last phase = %v; want the chain-tail SyncIdentitySequences (must run AFTER the links applied rows)", phases)
	}
}
