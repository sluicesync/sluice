// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestLineage_LoadAbsent: a store without lineage.json reports
// (nil, false, nil); ResolveLineage synthesises a single root
// segment over the conventional layout (the pre-ADR one-segment
// shape — strict generalization).
func TestLineage_LoadAbsent(t *testing.T) {
	store := newMemStore()
	cat, ok, err := LoadLineageCatalog(context.Background(), store)
	if err != nil || ok || cat != nil {
		t.Fatalf("LoadLineageCatalog(absent) = (%v,%v,%v); want (nil,false,nil)", cat, ok, err)
	}

	// With a conventional full present, ResolveLineage synthesises a
	// one-segment lineage (Dir == "", DefaultCodec).
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindFull,
		EndPosition:  ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		PartialState: irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	mustWriteManifest(t, store, ManifestFileName, full)
	rl, err := ResolveLineage(context.Background(), store)
	if err != nil {
		t.Fatalf("ResolveLineage: %v", err)
	}
	if len(rl.Segments) != 1 || rl.Segments[0].Dir != "" || rl.Segments[0].CodecOrDefault() != blobcodec.DefaultCodec {
		t.Fatalf("synthetic lineage = %+v; want one root segment, Dir='', codec=%s", rl.Segments, blobcodec.DefaultCodec)
	}
}

// TestLineage_RoundTrip_MixedCodec pins the on-disk JSON shape and
// proves a mixed-codec multi-segment lineage (de)serialises with each
// segment's codec preserved (ADR-0046 §5: recorded, never inferred).
func TestLineage_RoundTrip_MixedCodec(t *testing.T) {
	store := newMemStore()
	cap0 := time.Date(2026, 5, 16, 1, 0, 0, 0, time.UTC)
	cap1 := time.Date(2026, 5, 16, 2, 0, 0, 0, time.UTC)
	cat := &Catalog{
		LineageID:    "lin1",
		SourceEngine: "postgres",
		CreatedAt:    time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC),
		Segments: []Segment{
			{
				SegmentID: "s0", Dir: "", FullManifestPath: ManifestFileName,
				Incrementals:  []string{"manifests/incr-1.json"},
				StartPosition: ir.Position{Engine: "postgres", Token: "0/100"},
				EndPosition:   ir.Position{Engine: "postgres", Token: "0/200"},
				CappedAt:      &cap0, CapReason: "retain-rotate-at", Codec: blobcodec.CodecNone,
			},
			{
				SegmentID: "s1", Dir: "seg-1", FullManifestPath: ManifestFileName,
				StartPosition: ir.Position{Engine: "postgres", Token: "0/200"},
				EndPosition:   ir.Position{Engine: "postgres", Token: "0/300"},
				CappedAt:      &cap1, CapReason: "retain-rotate-at-chain-length", Codec: blobcodec.CodecGzip,
			},
			{
				SegmentID: "s2", Dir: "seg-2", FullManifestPath: ManifestFileName,
				StartPosition: ir.Position{Engine: "postgres", Token: "0/300"},
				EndPosition:   ir.Position{Engine: "postgres", Token: "0/300"},
				Codec:         blobcodec.CodecZstd,
			},
		},
	}
	if err := WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("WriteLineageCatalog: %v", err)
	}
	got, ok, err := LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: (%v,%v)", ok, err)
	}
	if len(got.Segments) != 3 {
		t.Fatalf("segments = %d; want 3", len(got.Segments))
	}
	wantCodecs := []blobcodec.Codec{blobcodec.CodecNone, blobcodec.CodecGzip, blobcodec.CodecZstd}
	for i, wc := range wantCodecs {
		if got.Segments[i].CodecOrDefault() != wc {
			t.Errorf("segment %d codec = %q; want %q", i, got.Segments[i].Codec, wc)
		}
	}
	if !got.Segments[0].Open() && got.Segments[2].Open() {
		// expected: seg0 capped, seg2 open
	} else {
		t.Errorf("open() wrong: seg0.open=%v seg2.open=%v; want false,true",
			got.Segments[0].Open(), got.Segments[2].Open())
	}
	// JSON shape: format_version + segments present; capped_at absent
	// on the open segment.
	raw, _ := store.Get(context.Background(), LineageCatalogFileName)
	body, _ := io.ReadAll(raw)
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["format_version"]; !ok {
		t.Error("lineage.json missing format_version")
	}
	if _, ok := m["segments"]; !ok {
		t.Error("lineage.json missing segments")
	}
	segs := m["segments"].([]any)
	openSeg := segs[2].(map[string]any)
	if _, present := openSeg["capped_at"]; present {
		t.Error("open segment must not serialise capped_at")
	}
}

// TestUpdateLineageForManifest_RecordsVerbatimMarker proves the
// ADR-0047 backup capability marker is recorded on the open segment
// from a full manifest whose schema carries ir.VerbatimType columns,
// and is ABSENT (nil; omitempty in JSON) for a non-verbatim full
// (legacy/common backups unaffected — additive, no format bump).
func TestUpdateLineageForManifest_RecordsVerbatimMarker(t *testing.T) {
	t.Run("verbatim full → marker recorded", func(t *testing.T) {
		store := newMemStore()
		m := &irbackup.Manifest{
			SourceEngine: "postgres",
			Kind:         irbackup.BackupKindFull,
			Schema: &ir.Schema{Tables: []*ir.Table{{
				Schema: "public", Name: "docs",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "path", Type: ir.VerbatimType{Definition: "ltree"}},
				},
			}}},
		}
		if err := UpdateLineageForManifest(context.Background(), store, m, ManifestFileName, blobcodec.CodecZstd); err != nil {
			t.Fatalf("UpdateLineageForManifest: %v", err)
		}
		cat, ok, err := LoadLineageCatalog(context.Background(), store)
		if err != nil || !ok {
			t.Fatalf("LoadLineageCatalog: (%v,%v)", ok, err)
		}
		seg := cat.Segments[len(cat.Segments)-1]
		if !seg.HasVerbatimExtensionColumns() {
			t.Fatal("expected open segment to carry the verbatim marker")
		}
		want := "public.docs.path"
		if len(seg.VerbatimExtensionColumns) != 1 || seg.VerbatimExtensionColumns[0] != want {
			t.Errorf("VerbatimExtensionColumns = %v; want [%q]", seg.VerbatimExtensionColumns, want)
		}
	})

	t.Run("non-verbatim full → marker absent (omitempty)", func(t *testing.T) {
		store := newMemStore()
		m := &irbackup.Manifest{
			SourceEngine: "postgres",
			Kind:         irbackup.BackupKindFull,
			Schema: &ir.Schema{Tables: []*ir.Table{{
				Name:    "users",
				Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			}}},
		}
		if err := UpdateLineageForManifest(context.Background(), store, m, ManifestFileName, blobcodec.CodecZstd); err != nil {
			t.Fatalf("UpdateLineageForManifest: %v", err)
		}
		raw, _ := store.Get(context.Background(), LineageCatalogFileName)
		body, _ := io.ReadAll(raw)
		if strings.Contains(string(body), "verbatim_extension_columns") {
			t.Errorf("non-verbatim lineage.json must omit verbatim_extension_columns; got %s", body)
		}
	})
}

// TestLineage_FormatVersionGate: a newer format_version is a loud
// refusal (forward-incompat — upgrade sluice).
func TestLineage_FormatVersionGate(t *testing.T) {
	store := newMemStore()
	store.data[LineageCatalogFileName] = []byte(`{"format_version":99,"segments":[{"segment_id":"x","full_manifest_path":"manifest.json"}]}`)
	if _, _, err := LoadLineageCatalog(context.Background(), store); err == nil ||
		!strings.Contains(err.Error(), "newer than this build supports") {
		t.Fatalf("err = %v; want forward-version refusal", err)
	}
}

// TestLineage_ZeroSegmentsRefused: a lineage with no segments is
// corrupt DR data — loud refusal, never silent continue.
func TestLineage_ZeroSegmentsRefused(t *testing.T) {
	store := newMemStore()
	store.data[LineageCatalogFileName] = []byte(`{"format_version":1,"segments":[]}`)
	if _, _, err := LoadLineageCatalog(context.Background(), store); err == nil ||
		!strings.Contains(err.Error(), "zero segments") {
		t.Fatalf("err = %v; want zero-segments refusal", err)
	}
}

// TestUpdateLineageForManifest_SeedsAndAppends: a full seeds segment
// 0; subsequent incrementals append to the open segment with its
// pinned codec.
func TestUpdateLineageForManifest_SeedsAndAppends(t *testing.T) {
	store := newMemStore()
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindFull,
		EndPosition:  ir.Position{Engine: "postgres", Token: "0/100"},
		PartialState: irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	UpdateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, blobcodec.CodecZstd)
	cat, ok, _ := LoadLineageCatalog(context.Background(), store)
	if !ok || len(cat.Segments) != 1 || cat.Segments[0].CodecOrDefault() != blobcodec.CodecZstd {
		t.Fatalf("after full: %+v; want one zstd segment", cat)
	}

	incr := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: time.Now().UTC(),
		SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  ir.Position{Engine: "postgres", Token: "0/100"},
		EndPosition:    ir.Position{Engine: "postgres", Token: "0/200"},
		PartialState:   irbackup.BackupStateComplete,
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	UpdateLineageForManifestBestEffort(context.Background(), store, incr, "manifests/incr-1.json", blobcodec.CodecZstd)
	cat, _, _ = LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 1 ||
		cat.Segments[0].Incrementals[0] != "manifests/incr-1.json" {
		t.Fatalf("after incr: %+v; want one incremental appended", cat.Segments[0])
	}
	if cat.Segments[0].EndPosition.Token != "0/200" {
		t.Errorf("open segment EndPosition = %q; want 0/200", cat.Segments[0].EndPosition.Token)
	}
	// Dedup: re-writing the same path doesn't double-append.
	UpdateLineageForManifestBestEffort(context.Background(), store, incr, "manifests/incr-1.json", blobcodec.CodecZstd)
	cat, _, _ = LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 1 {
		t.Errorf("re-write incrementals = %d; want 1 (deduped)", len(cat.Segments[0].Incrementals))
	}
}

// TestValidateRecordedCodec_UnknownRefused: an unknown recorded codec
// is a loud refusal — codec is recorded, never inferred (ADR-0046 §5).
func TestValidateRecordedCodec_UnknownRefused(t *testing.T) {
	if err := blobcodec.ValidateRecordedCodec(blobcodec.Codec("snappy")); err == nil ||
		!strings.Contains(err.Error(), "unknown compression codec") {
		t.Fatalf("err = %v; want unknown-codec refusal", err)
	}
	for _, c := range []blobcodec.Codec{"", blobcodec.CodecNone, blobcodec.CodecGzip, blobcodec.CodecZstd} {
		if err := blobcodec.ValidateRecordedCodec(c); err != nil {
			t.Errorf("validateRecordedCodec(%q) = %v; want nil", c, err)
		}
	}
}

// TestParseCompression covers the CLI codec parse (loud on unknown).
// It also pins the v0.67.0 gzip→zstd default flip: empty resolves to
// the zstd default (clean break, zero-users tenet — no back-compat
// gzip-default shim). A regression here silently changes every new
// backup's codec.
func TestParseCompression(t *testing.T) {
	if blobcodec.DefaultCodec != blobcodec.CodecZstd {
		t.Fatalf("DefaultCodec = %q; want zstd (v0.67.0 flip)", blobcodec.DefaultCodec)
	}
	for in, want := range map[string]blobcodec.Codec{"": blobcodec.CodecZstd, "gzip": blobcodec.CodecGzip, "none": blobcodec.CodecNone, "zstd": blobcodec.CodecZstd} {
		got, err := blobcodec.ParseCompression(in)
		if err != nil || got != want {
			t.Errorf("ParseCompression(%q) = (%q,%v); want (%q,nil)", in, got, err, want)
		}
	}
	if got := blobcodec.ResolveCodec(""); got != blobcodec.CodecZstd {
		t.Errorf("resolveCodec(\"\") = %q; want zstd (the operator-unset write default)", got)
	}
	if _, err := blobcodec.ParseCompression("lz4"); err == nil {
		t.Error("ParseCompression(lz4) = nil; want unknown-codec error")
	}
}

func mustWriteManifest(t *testing.T, store irbackup.Store, path string, m *irbackup.Manifest) {
	t.Helper()
	if err := WriteManifestAt(context.Background(), store, path, m); err != nil {
		t.Fatalf("WriteManifestAt(%q): %v", path, err)
	}
}

// pgIncr builds a complete incremental manifest chained off parent.
func pgIncr(parent, startLSN, endLSN string) *irbackup.Manifest {
	m := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Now().UTC(),
		SourceEngine:   "postgres",
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: parent,
		StartPosition:  ir.Position{Engine: "postgres", Token: startLSN},
		EndPosition:    ir.Position{Engine: "postgres", Token: endLSN},
		PartialState:   irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

// TestReconcileOpenSegmentCatalog_HealsHeadOrphan pins the fix for the
// ADR-0046 crash-injection mis-stitch: a rotation-opened segment's first
// (P_N, S] overlap incremental is durable on disk but was orphaned from
// lineage.json (its best-effort catalog append was lost to a crash/cancel).
// On resume the catalog's first recorded incremental then parents off the
// orphan instead of the segment full, and restore refuses the segment as
// "branching/mis-stitched lineage" though the on-disk chain is complete.
// The heal must re-catalogue the orphan AT THE HEAD in chain order and fix
// the derived coverage start / end.
func TestReconcileOpenSegmentCatalog_HealsHeadOrphan(t *testing.T) {
	ctx := context.Background()
	root := newMemStore()

	// seg0: capped root segment (Dir == "").
	full0 := bug66Manifest(irbackup.BackupKindFull, "0/100")
	mustWriteManifest(t, root, ManifestFileName, full0)

	// seg1: rotation-opened sub-dir segment. Anchor S = 0/900; the first
	// incremental A starts at the prior segment's end P_N = 0/250 (the
	// kept overlap), so A.start != seg1.StartPosition.
	seg1 := NewPrefixedStore(root, "seg-1")
	full1 := bug66Manifest(irbackup.BackupKindFull, "0/900")
	mustWriteManifest(t, seg1, ManifestFileName, full1)
	a := pgIncr(full1.BackupID, "0/250", "0/300")
	b := pgIncr(a.BackupID, "0/300", "0/400")
	c := pgIncr(b.BackupID, "0/400", "0/500")
	pathA := "manifests/incr-1000000000001-aaaaaaaa.json"
	pathB := "manifests/incr-1000000000002-bbbbbbbb.json"
	pathC := "manifests/incr-1000000000003-cccccccc.json"
	mustWriteManifest(t, seg1, pathA, a) // orphan: ON DISK, missing from catalog
	mustWriteManifest(t, seg1, pathB, b)
	mustWriteManifest(t, seg1, pathC, c)

	capped := time.Now().UTC()
	cat := &Catalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  "postgres",
		SluiceVersion: "test",
		Segments: []Segment{
			{Dir: "", SegmentID: full0.BackupID, FullManifestPath: ManifestFileName, StartPosition: full0.EndPosition, EndPosition: full0.EndPosition, Codec: blobcodec.CodecZstd, CappedAt: &capped},
			// The bug shape: A is missing from Incrementals, so the list
			// HEAD (B) parents off the orphan A, and the recorded coverage
			// start is B.start (wrong) instead of A.start.
			{Dir: "seg-1", SegmentID: full1.BackupID, FullManifestPath: ManifestFileName, StartPosition: full1.EndPosition, Incrementals: []string{pathB, pathC}, IncrementalCoverageStart: b.StartPosition, EndPosition: c.EndPosition, Codec: blobcodec.CodecZstd},
		},
	}
	if err := WriteLineageCatalog(ctx, root, cat); err != nil {
		t.Fatalf("seed catalog: %v", err)
	}

	if err := ReconcileOpenSegmentCatalog(ctx, root, seg1); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	healed, ok, err := LoadLineageCatalog(ctx, root)
	if err != nil || !ok {
		t.Fatalf("reload catalog: ok=%v err=%v", ok, err)
	}
	got := healed.Segments[1]
	if want := []string{pathA, pathB, pathC}; !slicesEqualStr(got.Incrementals, want) {
		t.Fatalf("healed Incrementals = %v; want %v (orphan re-catalogued at head, chain order)", got.Incrementals, want)
	}
	if got.IncrementalCoverageStart.Token != "0/250" {
		t.Errorf("healed IncrementalCoverageStart = %q; want 0/250 (the true first link A's start)", got.IncrementalCoverageStart.Token)
	}
	if got.EndPosition.Token != "0/500" {
		t.Errorf("healed EndPosition = %q; want 0/500 (last link C's end)", got.EndPosition.Token)
	}
	// seg0 must be untouched.
	if len(healed.Segments[0].Incrementals) != 0 || healed.Segments[0].CappedAt == nil {
		t.Errorf("seg0 mutated by heal: %+v", healed.Segments[0])
	}

	// Idempotent: a second reconcile is a no-op.
	if err := ReconcileOpenSegmentCatalog(ctx, root, seg1); err != nil {
		t.Fatalf("reconcile (2nd): %v", err)
	}
	again, _, _ := LoadLineageCatalog(ctx, root)
	if !slicesEqualStr(again.Segments[1].Incrementals, []string{pathA, pathB, pathC}) {
		t.Errorf("second reconcile changed the catalog: %v", again.Segments[1].Incrementals)
	}
}

// TestReconcileOpenSegmentCatalog_NoOpWhenConsistent: a catalog that
// already matches disk is left byte-identical (no spurious rewrite).
func TestReconcileOpenSegmentCatalog_NoOpWhenConsistent(t *testing.T) {
	ctx := context.Background()
	root := newMemStore()
	full := bug66Manifest(irbackup.BackupKindFull, "0/100")
	mustWriteManifest(t, root, ManifestFileName, full)
	a := pgIncr(full.BackupID, "0/100", "0/200")
	pathA := "manifests/incr-1000000000001-aaaaaaaa.json"
	mustWriteManifest(t, root, pathA, a)
	cat := &Catalog{
		FormatVersion: lineageCatalogFormatVersion, SourceEngine: "postgres", SluiceVersion: "test",
		Segments: []Segment{{Dir: "", SegmentID: full.BackupID, FullManifestPath: ManifestFileName, StartPosition: full.EndPosition, Incrementals: []string{pathA}, EndPosition: a.EndPosition, Codec: blobcodec.CodecZstd}},
	}
	if err := WriteLineageCatalog(ctx, root, cat); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := ReconcileOpenSegmentCatalog(ctx, root, root); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _, _ := LoadLineageCatalog(ctx, root)
	if !slicesEqualStr(got.Segments[0].Incrementals, []string{pathA}) {
		t.Errorf("consistent catalog mutated: %v", got.Segments[0].Incrementals)
	}
}

// TestReconcileOpenSegmentCatalog_RefusesToGuessOnBranch: when the
// on-disk set is NOT a single clean linear chain off the full (here two
// incrementals share a parent — a real branch), the heal refuses to
// guess and leaves the catalog untouched for restore's strict check.
func TestReconcileOpenSegmentCatalog_RefusesToGuessOnBranch(t *testing.T) {
	ctx := context.Background()
	root := newMemStore()
	full := bug66Manifest(irbackup.BackupKindFull, "0/100")
	mustWriteManifest(t, root, ManifestFileName, full)
	// Two children both parent off the full -> branch.
	b1 := pgIncr(full.BackupID, "0/100", "0/200")
	b2 := pgIncr(full.BackupID, "0/100", "0/250")
	mustWriteManifest(t, root, "manifests/incr-1000000000001-aaaaaaaa.json", b1)
	mustWriteManifest(t, root, "manifests/incr-1000000000002-bbbbbbbb.json", b2)
	cat := &Catalog{
		FormatVersion: lineageCatalogFormatVersion, SourceEngine: "postgres", SluiceVersion: "test",
		Segments: []Segment{{Dir: "", SegmentID: full.BackupID, FullManifestPath: ManifestFileName, StartPosition: full.EndPosition, Codec: blobcodec.CodecZstd}},
	}
	if err := WriteLineageCatalog(ctx, root, cat); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := ReconcileOpenSegmentCatalog(ctx, root, root); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _, _ := LoadLineageCatalog(ctx, root)
	if len(got.Segments[0].Incrementals) != 0 {
		t.Errorf("branch was healed (%v); want untouched (strict restore surfaces it)", got.Segments[0].Incrementals)
	}
}

func slicesEqualStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// memStore is a minimal in-memory BackupStore for catalog/lineage
// tests. The real LocalStore + BlobStore have integration coverage;
// the lineage behaviour is store-agnostic.
type memStore struct {
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (s *memStore) Put(_ context.Context, path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.data[path] = b
	return nil
}

func (s *memStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, &storeNotFoundErr{path: path}
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}

func (s *memStore) List(_ context.Context, prefix string) ([]string, error) {
	out := make([]string, 0)
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *memStore) Delete(_ context.Context, path string) error {
	delete(s.data, path)
	return nil
}

func (s *memStore) Exists(_ context.Context, path string) (bool, error) {
	_, ok := s.data[path]
	return ok, nil
}

type storeNotFoundErr struct{ path string }

func (e *storeNotFoundErr) Error() string { return "memstore: not found: " + e.path }

// bug66Manifest builds a minimal complete full/incremental manifest for
// the ResolveLineage missing-catalog pins.
func bug66Manifest(kind, lsn string) *irbackup.Manifest {
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Kind:          kind,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

// TestResolveLineage_MissingCatalogMultiSegmentRefused pins Bug 66: a
// ROTATED multi-segment backup whose lineage.json is absent (e.g. a
// pre-v0.67.0 chain.json-shaped backup, or a lost catalog) must LOUD-
// REFUSE — never silently restore only the root segment. Pre-fix this
// silently synthesised a one-segment lineage and dropped every
// rotation-opened seg-* segment with exit 0 (~90% data loss observed
// in the v0.67.0 regression cycle). The unreadable-lineage path was
// already loud; only this absent+multi-segment branch fell back.
func TestResolveLineage_MissingCatalogMultiSegmentRefused(t *testing.T) {
	store := newMemStore()
	// Root (segment 0) conventional layout.
	mustWriteManifest(t, store, ManifestFileName, bug66Manifest(irbackup.BackupKindFull, "0/100"))
	mustWriteManifest(t, store, "manifests/incr-0000000000001-aaaa.json", bug66Manifest(irbackup.BackupKindIncremental, "0/200"))
	// Rotation-opened segment evidence (seg-<unix-millis>/...), but NO
	// lineage.json — the catalog that is the only structural record of
	// the rotated segments.
	mustWriteManifest(t, store, RotationSegmentDirPrefix+"0000000000002/manifest.json", bug66Manifest(irbackup.BackupKindFull, "0/300"))

	_, err := ResolveLineage(context.Background(), store)
	if err == nil {
		t.Fatal("ResolveLineage: nil error for missing lineage.json on a multi-segment backup; want a loud refusal (Bug 66 — silent root-only partial is DR data loss)")
	}
	msg := err.Error()
	if !strings.Contains(msg, RotationSegmentDirPrefix) || !strings.Contains(msg, "lineage.json") {
		t.Errorf("refusal message = %q; want it to name the seg-* evidence and the missing lineage.json", msg)
	}
}

// TestResolveLineage_MissingCatalogLegacySingleSegmentStillResolves is
// the companion regression guard for the Bug 66 fix: a GENUINE
// never-rotated / pre-ADR backup (manifest.json + manifests/incr-*, NO
// seg-* dirs, NO lineage.json) MUST still synthesize a one-segment
// lineage and restore — the strict-generalization behaviour the fix
// must not break.
func TestResolveLineage_MissingCatalogLegacySingleSegmentStillResolves(t *testing.T) {
	store := newMemStore()
	mustWriteManifest(t, store, ManifestFileName, bug66Manifest(irbackup.BackupKindFull, "0/100"))
	mustWriteManifest(t, store, "manifests/incr-0000000000001-bbbb.json", bug66Manifest(irbackup.BackupKindIncremental, "0/200"))

	cat, err := ResolveLineage(context.Background(), store)
	if err != nil {
		t.Fatalf("ResolveLineage (legacy single-segment, no seg-*): unexpected error %v — never-rotated backups must still resolve", err)
	}
	if len(cat.Segments) != 1 || cat.Segments[0].Dir != "" {
		t.Fatalf("synthesised lineage = %+v; want exactly one root segment with Dir=\"\"", cat.Segments)
	}
}
