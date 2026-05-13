// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestChainCatalog_LoadAbsent covers the legacy-chain detection path:
// a store without chain.json must report (nil, false, nil) so callers
// fall through to the directory walk. Returning an error here would
// break v0.46.0-and-earlier chains the moment v0.47.0 reads them.
func TestChainCatalog_LoadAbsent(t *testing.T) {
	store := newMemStore()
	cat, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil {
		t.Fatalf("loadChainCatalog(empty): err = %v; want nil", err)
	}
	if ok {
		t.Errorf("loadChainCatalog(empty): present = true; want false")
	}
	if cat != nil {
		t.Errorf("loadChainCatalog(empty): cat = %v; want nil", cat)
	}
}

// TestChainCatalog_RoundTrip pins the write/read symmetry. A catalog
// written by [writeChainCatalog] must come back via [loadChainCatalog]
// with every field intact.
func TestChainCatalog_RoundTrip(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC().Truncate(time.Millisecond)
	original := &ChainCatalog{
		FormatVersion:     chainCatalogFormatVersion,
		SluiceVersion:     "v0.47.0-test",
		SourceEngine:      "postgres",
		ChainRootBackupID: "root123",
		CreatedAt:         now,
		UpdatedAt:         now,
		Entries: []ChainCatalogEntry{
			{
				BackupID:     "root123",
				Kind:         ir.BackupKindFull,
				ManifestPath: ManifestFileName,
				CreatedAt:    now,
				FileCount:    7,
			},
			{
				BackupID:       "incr001",
				Kind:           ir.BackupKindIncremental,
				ParentBackupID: "root123",
				ManifestPath:   "manifests/incr-0001.json",
				CreatedAt:      now.Add(time.Minute),
				FileCount:      1,
			},
		},
	}
	if err := writeChainCatalog(context.Background(), store, original); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}
	got, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil {
		t.Fatalf("loadChainCatalog: %v", err)
	}
	if !ok {
		t.Fatalf("loadChainCatalog: present = false; want true")
	}
	if got.FormatVersion != chainCatalogFormatVersion {
		t.Errorf("FormatVersion = %d; want %d", got.FormatVersion, chainCatalogFormatVersion)
	}
	if got.ChainRootBackupID != "root123" {
		t.Errorf("ChainRootBackupID = %q; want %q", got.ChainRootBackupID, "root123")
	}
	if len(got.Entries) != 2 {
		t.Fatalf("len(Entries) = %d; want 2", len(got.Entries))
	}
	if got.Entries[1].ParentBackupID != "root123" {
		t.Errorf("Entries[1].ParentBackupID = %q; want %q", got.Entries[1].ParentBackupID, "root123")
	}
}

// TestChainCatalog_FormatVersionGate covers the forward-incompat
// refusal: a v0.47.0 reader against a chain.json with
// format_version > 1 must refuse loudly rather than silently parsing
// fields it might not understand. Operator-actionable: "upgrade
// sluice".
func TestChainCatalog_FormatVersionGate(t *testing.T) {
	store := newMemStore()
	future := &ChainCatalog{FormatVersion: chainCatalogFormatVersion + 99}
	if err := writeChainCatalog(context.Background(), store, future); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}
	_, _, err := loadChainCatalog(context.Background(), store)
	if err == nil {
		t.Fatal("loadChainCatalog: err = nil; want format-version refusal")
	}
	if !strings.Contains(err.Error(), "upgrade sluice") {
		t.Errorf("loadChainCatalog: err = %v; want operator-actionable upgrade hint", err)
	}
}

// TestChainCatalog_AppendDeduplicatesByBackupID pins the per-chunk-
// checkpoint replay path. Within a single backup-full run, the
// orchestrator writes the same BackupID's manifest multiple times as
// chunks complete. updateChainCatalog must replace by BackupID, not
// accumulate duplicates — otherwise the catalog grows linearly with
// chunk count for a single backup, defeating the O(1) goal.
func TestChainCatalog_AppendDeduplicatesByBackupID(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()
	m := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "abc",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	// First write — appended.
	if err := updateChainCatalog(context.Background(), store, m, ManifestFileName, 1); err != nil {
		t.Fatalf("updateChainCatalog 1: %v", err)
	}
	// Same BackupID, different FileCount (simulating mid-run checkpoint).
	if err := updateChainCatalog(context.Background(), store, m, ManifestFileName, 5); err != nil {
		t.Fatalf("updateChainCatalog 2: %v", err)
	}
	cat, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("loadChainCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Entries) != 1 {
		t.Errorf("len(Entries) = %d; want 1 (dedup by BackupID)", len(cat.Entries))
	}
	if cat.Entries[0].FileCount != 5 {
		t.Errorf("Entries[0].FileCount = %d; want 5 (latest write should win)", cat.Entries[0].FileCount)
	}
}

// TestChainCatalog_RebuildFromLegacyChain covers the lazy-rebuild
// path: a chain produced by pre-v0.47.0 sluice (full + incrementals
// on disk, no chain.json) gets a catalog seeded with every historical
// entry the first time v0.47.0 writes to the chain.
func TestChainCatalog_RebuildFromLegacyChain(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()

	// Stage a v0.46.0-shape chain: full + two incrementals, no chain.json.
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full000",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	incr1 := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SourceEngine:   "postgres",
		BackupID:       "incr001",
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: "full000",
		CreatedAt:      now.Add(time.Minute),
	}
	incr2 := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SourceEngine:   "postgres",
		BackupID:       "incr002",
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: "incr001",
		CreatedAt:      now.Add(2 * time.Minute),
	}
	mustWriteManifest(t, store, ManifestFileName, full)
	mustWriteManifest(t, store, "manifests/incr-0001-incr001.json", incr1)
	mustWriteManifest(t, store, "manifests/incr-0002-incr002.json", incr2)

	// Simulate v0.47.0 writing a new incremental — first updateChainCatalog
	// triggers the lazy rebuild.
	incr3 := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SourceEngine:   "postgres",
		BackupID:       "incr003",
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: "incr002",
		CreatedAt:      now.Add(3 * time.Minute),
	}
	if err := updateChainCatalog(context.Background(), store, incr3, "manifests/incr-0003-incr003.json", 1); err != nil {
		t.Fatalf("updateChainCatalog: %v", err)
	}
	cat, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("loadChainCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Entries) != 4 {
		t.Fatalf("len(Entries) = %d; want 4 (3 historical + 1 new)", len(cat.Entries))
	}
	// First entry must be the full (chain order).
	if cat.Entries[0].BackupID != "full000" {
		t.Errorf("Entries[0].BackupID = %q; want %q (full first)", cat.Entries[0].BackupID, "full000")
	}
	// New incremental appended at the end.
	last := cat.Entries[len(cat.Entries)-1]
	if last.BackupID != "incr003" {
		t.Errorf("last entry BackupID = %q; want %q", last.BackupID, "incr003")
	}
	if cat.ChainRootBackupID != "full000" {
		t.Errorf("ChainRootBackupID = %q; want %q", cat.ChainRootBackupID, "full000")
	}
}

// TestChainCatalog_FilterTombstoned pins the v0.47.0-reader / v0.48.0+
// -writer forward-compat insurance: an entry marked Tombstoned must be
// skipped by [readManifestsFromCatalog] so a future compaction's
// tombstones don't surface compacted-out manifests in a v0.47.0
// restore.
func TestChainCatalog_FilterTombstoned(t *testing.T) {
	entries := []ChainCatalogEntry{
		{BackupID: "a", ManifestPath: "manifest.json"},
		{BackupID: "b", ManifestPath: "manifests/incr-0001.json", Tombstoned: true},
		{BackupID: "c", ManifestPath: "manifests/incr-0002.json"},
	}
	active := filterActiveEntries(entries)
	if len(active) != 2 {
		t.Fatalf("len(active) = %d; want 2", len(active))
	}
	if active[0].BackupID != "a" || active[1].BackupID != "c" {
		t.Errorf("active = [%q, %q]; want [a, c]", active[0].BackupID, active[1].BackupID)
	}
}

// TestListAllManifests_PrefersCatalog asserts the integration: when
// chain.json is present, listAllManifests reads from it rather than
// walking the directory. We prove this by writing the catalog with
// only a subset of manifests' worth of entries and confirming the
// returned slice matches the catalog, not the on-disk directory.
func TestListAllManifests_PrefersCatalog(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	incr := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SourceEngine:   "postgres",
		BackupID:       "incr",
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: "full",
		CreatedAt:      now.Add(time.Minute),
	}
	mustWriteManifest(t, store, ManifestFileName, full)
	mustWriteManifest(t, store, "manifests/incr-0001.json", incr)

	// Catalog references only the full — list should mirror that
	// even though incr is on disk.
	partial := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     now,
		UpdatedAt:     now,
		Entries: []ChainCatalogEntry{
			{BackupID: "full", Kind: ir.BackupKindFull, ManifestPath: ManifestFileName, CreatedAt: now},
		},
	}
	if err := writeChainCatalog(context.Background(), store, partial); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}
	records, err := listAllManifests(context.Background(), store)
	if err != nil {
		t.Fatalf("listAllManifests: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("len(records) = %d; want 1 (catalog wins)", len(records))
	}
}

// TestListAllManifests_FallsBackToWalk asserts the legacy path: a
// chain without chain.json must still be readable via the directory
// walk.
func TestListAllManifests_FallsBackToWalk(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	mustWriteManifest(t, store, ManifestFileName, full)
	records, err := listAllManifests(context.Background(), store)
	if err != nil {
		t.Fatalf("listAllManifests: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d; want 1", len(records))
	}
	if records[0].manifest.BackupID != "full" {
		t.Errorf("records[0].BackupID = %q; want %q", records[0].manifest.BackupID, "full")
	}
}

// mustWriteManifest is a test helper that writes a manifest at the
// given path, bypassing the chain-catalog hook so the test controls
// catalog state explicitly.
func mustWriteManifest(t *testing.T, store ir.BackupStore, path string, m *ir.Manifest) {
	t.Helper()
	if err := writeManifestAt(context.Background(), store, path, m); err != nil {
		t.Fatalf("writeManifestAt(%q): %v", path, err)
	}
}

// memStore is a minimal in-memory BackupStore for catalog tests. The
// real LocalStore + BlobStore have integration coverage; the catalog
// behaviour is store-agnostic.
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

// assertCatalogJSONShape is a small belt-and-braces check that the
// on-disk JSON envelope matches the schema docs. Catches accidental
// json-tag drift on the catalog types.
func assertCatalogJSONShape(t *testing.T, body []byte) {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal catalog body: %v", err)
	}
	if _, ok := raw["format_version"]; !ok {
		t.Errorf("catalog json missing required field: format_version")
	}
	if _, ok := raw["entries"]; !ok {
		t.Errorf("catalog json missing required field: entries")
	}
}

// TestChainCatalog_JSONShape pins the on-disk envelope so future
// json-tag renames must trip a test rather than silently break
// readers (or older sluice's forward-compat ignore-unknown behaviour).
func TestChainCatalog_JSONShape(t *testing.T) {
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		Entries:       []ChainCatalogEntry{{BackupID: "x"}},
	}
	body, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertCatalogJSONShape(t, body)
}
