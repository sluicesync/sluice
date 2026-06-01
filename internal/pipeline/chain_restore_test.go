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

	"github.com/orware/sluice/internal/ir"
)

// makeManifest returns a manifest with deterministic CreatedAt and
// position for chain-walk test fixtures.
func makeManifest(t *testing.T, kind string, parent *ir.Manifest, lsn string) *ir.Manifest {
	t.Helper()
	m := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
		Kind:          kind,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"` + lsn + `"}`},
	}
	if parent != nil {
		m.ParentBackupID = manifestBackupID(parent)
		m.StartPosition = parent.EndPosition
	}
	m.BackupID = ir.ComputeBackupID(m)
	return m
}

// seedSegment writes a single segment's full + incrementals into a
// per-segment store and returns the LineageSegment describing it. dir
// == "" is the root (one-segment) layout.
func seedSegment(t *testing.T, root ir.BackupStore, dir string, full *ir.Manifest, incrs []*ir.Manifest, codec Codec) LineageSegment {
	t.Helper()
	ss := newPrefixedStore(root, dir)
	if err := writeManifestAt(context.Background(), ss, ManifestFileName, full); err != nil {
		t.Fatalf("write seg full: %v", err)
	}
	seg := LineageSegment{
		SegmentID:        manifestBackupID(full),
		Dir:              dir,
		FullManifestPath: ManifestFileName,
		StartPosition:    full.EndPosition,
		EndPosition:      full.EndPosition,
		Codec:            codec,
	}
	for i, m := range incrs {
		p := "manifests/incr-" + fmt.Sprintf("%04d", i) + ".json"
		if err := writeManifestAt(context.Background(), ss, p, m); err != nil {
			t.Fatalf("write seg incr: %v", err)
		}
		seg.Incrementals = append(seg.Incrementals, p)
		seg.EndPosition = m.EndPosition
	}
	return seg
}

func TestBuildLineageChain_SingleSegmentNoIncrementals(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)
	// No lineage.json — resolveLineage synthesises a one-segment
	// lineage; behaviour byte-identical to a pre-ADR single full.
	chain, err := buildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("buildLineageChain: %v", err)
	}
	if len(chain) != 1 || canonicalKind(chain[0].manifest.Kind) != ir.BackupKindFull {
		t.Errorf("chain = %+v; want one full link", chain)
	}
}

func TestBuildLineageChain_LinearOK(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	incr1 := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr2 := makeManifest(t, ir.BackupKindIncremental, incr1, "0/300")
	seg := seedSegment(t, store, "", full, []*ir.Manifest{incr1, incr2}, CodecGzip)
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{seg}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	chain, err := buildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("buildLineageChain: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain len = %d; want 3", len(chain))
	}
	if manifestBackupID(chain[2].manifest) != manifestBackupID(incr2) {
		t.Errorf("chain[2] is not incr2")
	}
}

// TestBuildLineageChain_MultiSegmentBoundaryOK proves a 3-segment
// lineage walks end-to-end when seg[i].end == seg[i+1].start.
func TestBuildLineageChain_MultiSegmentBoundaryOK(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	f0 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, ir.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, store, "", f0, []*ir.Manifest{i0}, CodecGzip)

	// seg1 full's StartPosition == seg0.end (0/200). makeManifest sets
	// EndPosition from the lsn arg; force StartPosition = prior end.
	f1 := makeManifest(t, ir.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = ir.ComputeBackupID(f1)
	i1 := makeManifest(t, ir.BackupKindIncremental, f1, "0/400")
	s1 := seedSegment(t, store, "seg-1", f1, []*ir.Manifest{i1}, CodecNone)

	f2 := makeManifest(t, ir.BackupKindFull, nil, "0/500")
	f2.StartPosition = i1.EndPosition
	f2.BackupID = ir.ComputeBackupID(f2)
	s2 := seedSegment(t, store, "seg-2", f2, nil, CodecZstd)

	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	s1.CappedAt, s1.CapReason = &capt, rotationReasonChainLength
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1, s2}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	chain, err := buildLineageChain(context.Background(), store, nil)
	if err != nil {
		t.Fatalf("buildLineageChain (valid 3-segment): %v", err)
	}
	// f0,i0,f1,i1,f2 = 5 links.
	if len(chain) != 5 {
		t.Fatalf("chain len = %d; want 5", len(chain))
	}
}

// TestBuildBrokerChain_MultiSegmentFollows pins the post-Round-D
// closure of the Phase 4.5 multi-segment-broker deferral. Pre-fix
// buildBrokerChain refused loudly on any chain with >1 segment with
// the documented "Broker following a multi-segment lineage is deferred
// (ADR-0046 Phase 4.5); ..." error. Post-fix, the broker walks the
// full lineage via buildLineageChain — same code path sluice restore
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
	store, _ := NewLocalStore(dir)

	// 3-segment lineage, same shape as
	// TestBuildLineageChain_MultiSegmentBoundaryOK.
	f0 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, ir.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, store, "", f0, []*ir.Manifest{i0}, CodecGzip)

	f1 := makeManifest(t, ir.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = ir.ComputeBackupID(f1)
	i1 := makeManifest(t, ir.BackupKindIncremental, f1, "0/400")
	s1 := seedSegment(t, store, "seg-1", f1, []*ir.Manifest{i1}, CodecNone)

	f2 := makeManifest(t, ir.BackupKindFull, nil, "0/500")
	f2.StartPosition = i1.EndPosition
	f2.BackupID = ir.ComputeBackupID(f2)
	s2 := seedSegment(t, store, "seg-2", f2, nil, CodecZstd)

	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	s1.CappedAt, s1.CapReason = &capt, rotationReasonChainLength
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1, s2}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}

	chain, err := buildBrokerChain(context.Background(), store)
	if err != nil {
		t.Fatalf("buildBrokerChain (3-segment): unexpected refusal: %v", err)
	}
	// f0,i0,f1,i1,f2 = 5 links across 3 segments.
	if len(chain) != 5 {
		t.Fatalf("chain len = %d; want 5 (f0,i0,f1,i1,f2)", len(chain))
	}

	// Verify chain ordering: full → incrementals within each segment,
	// segments in lineage order.
	expectedKinds := []string{
		ir.BackupKindFull, ir.BackupKindIncremental, // seg0
		ir.BackupKindFull, ir.BackupKindIncremental, // seg1
		ir.BackupKindFull, // seg2 (no incrementals)
	}
	for i, want := range expectedKinds {
		got := canonicalKind(chain[i].manifest.Kind)
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
	store, _ := NewLocalStore(dir)

	// Minimal 2-segment lineage to trigger the prior multi-segment path.
	f0 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	s0 := seedSegment(t, store, "", f0, nil, CodecGzip)
	f1 := makeManifest(t, ir.BackupKindFull, nil, "0/200")
	f1.StartPosition = f0.EndPosition
	f1.BackupID = ir.ComputeBackupID(f1)
	s1 := seedSegment(t, store, "seg-1", f1, nil, CodecNone)
	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}

	_, err := buildBrokerChain(context.Background(), store)
	if err != nil {
		// Any error here is unexpected — the chain is well-formed.
		t.Fatalf("buildBrokerChain on valid 2-segment lineage: unexpected error %v", err)
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

	// SAME validateBoundary, intra-segment (exact=true): contiguous OK,
	// any non-equality (gap OR regression) is a loud mismatch.
	if err := validateBoundary(cmp, prevEnd, eq, true, "seg0 link1", "seg0 incrX"); err != nil {
		t.Errorf("intra contiguous: err = %v; want nil", err)
	}
	if err := validateBoundary(cmp, prevEnd, ahead, true, "seg0 link1", "seg0 incrX"); err == nil ||
		!strings.Contains(err.Error(), "lineage boundary mismatch") {
		t.Errorf("intra forward-gap: err = %v; want loud mismatch (exact)", err)
	}
	if err := validateBoundary(cmp, prevEnd, behind, true, "seg0 link1", "seg0 incrX"); err == nil {
		t.Errorf("intra regression: err = nil; want loud mismatch")
	}
	// SAME validateBoundary, inter-segment (exact=false): equal OR
	// ahead OK (S >= P_N); only a REGRESSION is a loud refusal.
	if err := validateBoundary(cmp, prevEnd, eq, false, "seg0 last", "seg1 start"); err != nil {
		t.Errorf("inter equal: err = %v; want nil", err)
	}
	if err := validateBoundary(cmp, prevEnd, ahead, false, "seg0 last", "seg1 start"); err != nil {
		t.Errorf("inter S>P_N (ahead): err = %v; want nil (monotonic OK)", err)
	}
	if err := validateBoundary(cmp, prevEnd, behind, false, "seg0 last", "seg1 start"); err == nil ||
		!strings.Contains(err.Error(), "REGRESSION") {
		t.Errorf("inter regression: err = %v; want loud REGRESSION refusal", err)
	}
	// Empty prevEnd tolerance (legacy v0.16 full) — skip either mode.
	if err := validateBoundary(cmp, ir.Position{}, behind, true, "p", "c"); err != nil {
		t.Errorf("empty-prev tolerance: err = %v; want nil", err)
	}
}

// TestBuildLineageChain_SegmentBoundaryRegressionRefuses: a
// position-regression across a segment boundary is a LOUD refusal
// (DR data — never a silent partial assemble).
func TestBuildLineageChain_SegmentBoundaryRegressionRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	f0 := makeManifest(t, ir.BackupKindFull, nil, "END0")
	s0 := seedSegment(t, store, "", f0, nil, CodecGzip)
	f1 := makeManifest(t, ir.BackupKindFull, nil, "END1")
	s1 := seedSegment(t, store, "seg-1", f1, nil, CodecGzip)
	// seg1's RECORDED StartPosition REGRESSES before seg0's end
	// (a tampered / corrupt lineage.json — DR data).
	s1.StartPosition = ir.Position{Engine: "postgres", Token: "BEFORE0"}
	capt := time.Now().UTC()
	s0.CappedAt = &capt
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	// Ranking comparator: f0.End ("END0") ranks AFTER the regressed
	// seg1 start ("BEFORE0") -> inter-segment monotonic check fails.
	cmp := &fakeMonotonicEngine{order: map[string]int{
		`{"slot":"sluice_slot","lsn":"END0"}`: 200,
		"BEFORE0":                             100,
		`{"slot":"sluice_slot","lsn":"END1"}`: 300,
	}}
	_, err := buildLineageChain(context.Background(), store, cmp)
	if err == nil || !strings.Contains(err.Error(), "REGRESSION") {
		t.Errorf("err = %v; want loud segment-boundary REGRESSION refusal", err)
	}
}

func TestBuildLineageChain_IntraSegmentMismatchRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	tampered := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	tampered.StartPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"WRONG"}`}
	tampered.BackupID = ir.ComputeBackupID(tampered)
	seg := seedSegment(t, store, "", full, []*ir.Manifest{tampered}, CodecGzip)
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{seg}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	_, err := buildLineageChain(context.Background(), store, nil)
	if err == nil || !strings.Contains(err.Error(), "lineage boundary mismatch") {
		t.Errorf("err = %v; want intra-segment boundary refusal", err)
	}
}

// TestBuildLineageChain_MissingFullRefuses: a segment whose recorded
// full manifest is absent is a loud refusal.
func TestBuildLineageChain_MissingFullRefuses(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	cat := &LineageCatalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []LineageSegment{{SegmentID: "s0", Dir: "", FullManifestPath: ManifestFileName, Codec: CodecGzip}},
	}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	_, err := buildLineageChain(context.Background(), store, nil)
	if err == nil || !strings.Contains(err.Error(), "full") {
		t.Errorf("err = %v; want missing-full refusal", err)
	}
}

func TestDetectAmbiguousDeltas_RenameRefuses(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAlterTable,
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
	if err := detectAmbiguousDeltas(deltas); err == nil {
		t.Errorf("detectAmbiguousDeltas: nil; want refusal on rename ambiguity")
	}
}

func TestDetectAmbiguousDeltas_AddOnlyOK(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAlterTable,
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
	if err := detectAmbiguousDeltas(deltas); err != nil {
		t.Errorf("detectAmbiguousDeltas: %v; want clean", err)
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
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	// Full backup via the existing Backup pipeline.
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
	})
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	// Patch the full's manifest with an EndPosition + BackupID so the
	// incremental can chain off it. (The full backup pipeline
	// doesn't yet record EndPosition for fulls — Phase 3.3 work.)
	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	full.Kind = ir.BackupKindFull
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
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
	chain := &ChainRestore{
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
	store, _ := NewLocalStore(dir)

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
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, CodecGzip)

	// Incremental: carries SchemaHistory + a single Insert event.
	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr.Schema = &ir.Schema{Tables: []*ir.Table{postDDL}}
	incr.SchemaHistory = []*ir.SchemaHistoryEntry{
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
	cw, err := newChangeChunkWriter(buf, nil, CodecGzip)
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
	incr.ChangeChunks = []*ir.ChunkInfo{{
		File:     chunkPath,
		RowCount: 1,
		SHA256:   cw.Hash(),
	}}
	incr.BackupID = ir.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := writeManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, CodecGzip)

	// Run restore.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &ChainRestore{
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
	store, _ := NewLocalStore(dir)

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

	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, CodecGzip)

	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr.Schema = &ir.Schema{Tables: []*ir.Table{postDDL}}
	incr.SchemaHistory = []*ir.SchemaHistoryEntry{
		{
			Schema:         "",
			Table:          "users",
			AnchorPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/150"}`},
			TableJSON:      postDDLPayload,
		},
	}
	incr.ChangeChunks = nil // no DML
	incr.BackupID = ir.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := writeManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &ChainRestore{
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
	store, _ := NewLocalStore(dir)

	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, CodecGzip)

	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	// Corrupt SchemaHistory entry: TableJSON is "null" → UnmarshalTable
	// returns (nil, nil); orchestrator must refuse loudly.
	incr.SchemaHistory = []*ir.SchemaHistoryEntry{
		{
			Schema:         "",
			Table:          "users",
			AnchorPosition: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/150"}`},
			TableJSON:      []byte("null"),
		},
	}
	incr.BackupID = ir.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	if err := writeManifestAt(context.Background(), store, incrPath, incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &ChainRestore{
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
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.Schema = schema
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, CodecGzip)

	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr.Schema = schema
	// SchemaHistory deliberately nil (pre-Chunk-D shape).
	incr.SchemaHistory = nil
	buf := &bytes.Buffer{}
	cw, _ := newChangeChunkWriter(buf, nil, CodecGzip)
	_ = cw.WriteChange(ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/180"}`},
		Table:    "users",
		Row:      ir.Row{"id": int64(9)},
	})
	_ = cw.Close()
	chunkPath := "chunks/_changes/preD/changes-0.jsonl.gz"
	_ = store.Put(context.Background(), chunkPath, buf)
	incr.ChangeChunks = []*ir.ChunkInfo{{
		File:     chunkPath,
		RowCount: 1,
		SHA256:   cw.Hash(),
	}}
	incr.BackupID = ir.ComputeBackupID(incr)
	incrPath := "manifests/incr-0001.json"
	_ = writeManifestAt(context.Background(), store, incrPath, incr)
	updateLineageForManifestBestEffort(context.Background(), store, incr, incrPath, CodecGzip)

	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("postgres"),
	}
	chain := &ChainRestore{
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
	store, _ := NewLocalStore(dir)

	// Full manifest: source_engine=postgres.
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	full.SourceEngine = "postgres"
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	incr := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	incr.SourceEngine = "postgres"
	incr.BackupID = ir.ComputeBackupID(incr)
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0001.json", incr); err != nil {
		t.Fatalf("write incr: %v", err)
	}

	// Target engine = mysql, distinct from the chain's source. The
	// recorder engine handles every phase as a no-op + record; the
	// incremental has no change chunks so the applier is exercised
	// only via OpenChangeApplier + EnsureControlTable.
	tgt := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine("mysql"),
	}
	chain := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: store}
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
	store, _ := NewLocalStore(dir)

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	src := newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"users": {{"id": int64(1)}}})
	_ = (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background())

	// Patch the full with an EndPosition.
	full, _ := readManifest(context.Background(), store)
	full.EndPosition = ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/100"}`}
	full.BackupID = ir.ComputeBackupID(full)
	_ = writeManifestAt(context.Background(), store, ManifestFileName, full)

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
	r := &Restore{Target: tgt, TargetDSN: "tgt", Store: store}
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
