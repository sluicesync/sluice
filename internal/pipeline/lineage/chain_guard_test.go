// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// Pins for the ADR-0160 chain concurrent-writer guard: the lineage
// catalog read-modify-write is a compare-and-swap on stores with the
// [irbackup.ConditionalPutter] capability — the losing writer refuses
// loudly with the coded SLUICE-E-BACKUP-CHAIN-CONFLICT refusal and
// writes NO catalog change — while stores without the capability keep
// the pre-guard last-write-wins behavior, pinned here so the degrade
// is deliberate, not accidental.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// casMemStore extends the plain memStore with the conditional-create
// capability, plus two fault-injection knobs the pins use to produce a
// deterministic interleave (no goroutines — the conflict is a state
// machine, not a scheduler race).
type casMemStore struct {
	*memStore

	// beforePutIfAbsent, when set, runs before each claim attempt —
	// the deterministic "another writer got here first" injection.
	beforePutIfAbsent func(path string)

	// putIfAbsentErr, when set, fails every claim with a NON-conflict
	// error (the S3-compatible-without-conditional-PUT shape).
	putIfAbsentErr error
}

func (s *casMemStore) PutIfAbsent(ctx context.Context, path string, r io.Reader) error {
	if s.beforePutIfAbsent != nil {
		s.beforePutIfAbsent(path)
	}
	if s.putIfAbsentErr != nil {
		return s.putIfAbsentErr
	}
	if _, taken := s.data[path]; taken {
		return fmt.Errorf("casmem: %q: %w", path, irbackup.ErrPathExists)
	}
	return s.Put(ctx, path, r)
}

// guardFixtureCatalog returns a minimal single-segment catalog.
func guardFixtureCatalog(engine string) *Catalog {
	return &Catalog{
		FormatVersion: lineageCatalogFormatVersion,
		SourceEngine:  engine,
		Segments: []Segment{{
			SegmentID:        "seg0",
			FullManifestPath: ManifestFileName,
		}},
	}
}

// requireChainConflict asserts err carries the coded refusal.
func requireChainConflict(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil; want the SLUICE-E-BACKUP-CHAIN-CONFLICT refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("err = %v; carries no sluicecode", err)
	}
	if ce.Code != sluicecode.CodeBackupChainConflict {
		t.Fatalf("code = %s; want %s", ce.Code, sluicecode.CodeBackupChainConflict)
	}
	if info, registered := sluicecode.Describe(ce.Code); !registered || info.Class != sluicecode.ClassRefusal {
		t.Errorf("code %s: registered=%v class=%v; want a registered refusal", ce.Code, registered, info.Class)
	}
}

// TestChainGuard_MarkerPathRoundTrip pins the marker naming: zero-
// padded (lexical == numeric order), round-trips through the parser,
// and malformed names are ignored (under-observing can only produce a
// spurious — safe — conflict).
func TestChainGuard_MarkerPathRoundTrip(t *testing.T) {
	for _, n := range []uint64{0, 1, 7, 12345, 1<<63 + 42} {
		p := chainGenMarkerPath(n)
		if !strings.HasPrefix(p, ChainGenPrefix) {
			t.Errorf("marker path %q lacks prefix %q", p, ChainGenPrefix)
		}
		got, ok := parseChainGenMarker(p)
		if !ok || got != n {
			t.Errorf("parse(%q) = (%d, %v); want (%d, true)", p, got, ok, n)
		}
	}
	if a, b := chainGenMarkerPath(9), chainGenMarkerPath(10); a >= b {
		t.Errorf("marker paths must sort lexically == numerically: %q >= %q", a, b)
	}
	for _, bad := range []string{
		"lineage.gen/", "lineage.gen/g-", "lineage.gen/g-xyz",
		"lineage.gen/other", "manifests/incr-1.json",
	} {
		if n, ok := parseChainGenMarker(bad); ok {
			t.Errorf("parse(%q) = (%d, true); want ignored", bad, n)
		}
	}
}

// TestChainGuard_InterleavedWritersConflict is the core CAS pin: two
// writers load-for-update the same catalog; the first write wins, the
// second refuses loudly with the coded conflict and writes NOTHING.
func TestChainGuard_InterleavedWritersConflict(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{memStore: newMemStore()}

	seed := guardFixtureCatalog("postgres")
	stampChainGuard(seed, chainGuardStamp{gen: 0, observed: true})
	if err := WriteLineageCatalog(ctx, store, seed); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	a, okA, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil || !okA {
		t.Fatalf("writer A load: ok=%v err=%v", okA, err)
	}
	b, okB, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil || !okB {
		t.Fatalf("writer B load: ok=%v err=%v", okB, err)
	}

	a.Segments[0].Incrementals = []string{"manifests/incr-a.json"}
	if err := WriteLineageCatalog(ctx, store, a); err != nil {
		t.Fatalf("writer A write: %v", err)
	}

	b.Segments[0].Incrementals = []string{"manifests/incr-b.json"}
	requireChainConflict(t, WriteLineageCatalog(ctx, store, b))

	// The loser wrote nothing: the durable catalog is A's.
	got, ok, err := LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("reload: ok=%v err=%v", ok, err)
	}
	if len(got.Segments[0].Incrementals) != 1 || got.Segments[0].Incrementals[0] != "manifests/incr-a.json" {
		t.Errorf("catalog after conflict = %v; want writer A's update intact", got.Segments[0].Incrementals)
	}
}

// TestChainGuard_LocalFS_InterleavedWriters runs the same interleave
// against the REAL local-FS store (O_EXCL claim + directory-walk
// observation) — the deterministic two-writer scenario on the backend
// operators actually point --output-dir at. Sequenced calls, no
// goroutines: the conflict is arbitrated by marker state, not timing.
func TestChainGuard_LocalFS_InterleavedWriters(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	seed := guardFixtureCatalog("mysql")
	stampChainGuard(seed, chainGuardStamp{gen: 0, observed: true})
	if err := WriteLineageCatalog(ctx, store, seed); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	a, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("writer A load: %v", err)
	}
	b, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("writer B load: %v", err)
	}
	a.Segments[0].Incrementals = []string{"manifests/incr-a.json"}
	if err := WriteLineageCatalog(ctx, store, a); err != nil {
		t.Fatalf("writer A write: %v", err)
	}
	b.Segments[0].Incrementals = []string{"manifests/incr-b.json"}
	requireChainConflict(t, WriteLineageCatalog(ctx, store, b))

	got, ok, err := LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("reload: ok=%v err=%v", ok, err)
	}
	if got.Segments[0].Incrementals[0] != "manifests/incr-a.json" {
		t.Errorf("catalog after conflict = %v; want writer A's update", got.Segments[0].Incrementals)
	}

	// A third writer recovers cleanly after the refusal (the "re-run"
	// remediation the error message promises).
	c, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("writer C load: %v", err)
	}
	c.Segments[0].Incrementals = append(c.Segments[0].Incrementals, "manifests/incr-c.json")
	if err := WriteLineageCatalog(ctx, store, c); err != nil {
		t.Fatalf("writer C (post-conflict re-run) write: %v", err)
	}
}

// TestChainGuard_UpdateLineageForManifest_Conflict drives the conflict
// through the PRODUCTION writer (the full/incremental/rollover catalog
// append) and pins that UpdateLineageForManifestBestEffort — which
// swallows ordinary catalog hiccups by design — REFUSES to swallow the
// concurrent-writer conflict.
func TestChainGuard_UpdateLineageForManifest_Conflict(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{memStore: newMemStore()}

	full := bug66Manifest(irbackup.BackupKindFull, "0/A0")
	if err := UpdateLineageForManifest(ctx, store, full, ManifestFileName, blobcodec.CodecNone); err != nil {
		t.Fatalf("seed full: %v", err)
	}

	// Interleave injection: just before this writer's claim, "another
	// writer" takes the slot.
	raced := false
	store.beforePutIfAbsent = func(path string) {
		if raced {
			return
		}
		raced = true
		store.data[path] = []byte(`{"claimed_at":"2026-07-14T00:00:00Z","host":"other-cron"}`)
	}

	incr := bug66Manifest(irbackup.BackupKindIncremental, "0/B0")
	incr.ParentBackupID = ManifestBackupID(full)

	requireChainConflict(t, UpdateLineageForManifest(ctx, store, incr, "manifests/incr-1.json", blobcodec.CodecNone))

	// BestEffort propagates the conflict rather than WARN-swallowing it.
	raced = false
	requireChainConflict(t, UpdateLineageForManifestBestEffort(ctx, store, incr, "manifests/incr-1.json", blobcodec.CodecNone))

	// The refused appends wrote nothing: the catalog still has no
	// incrementals.
	got, ok, err := LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("reload: ok=%v err=%v", ok, err)
	}
	if n := len(got.Segments[0].Incrementals); n != 0 {
		t.Errorf("catalog has %d incrementals after refused appends; want 0", n)
	}
}

// TestChainGuard_SeedRaceConflicts pins the first-write race: two
// writers racing to SEED lineage.json on a fresh chain conflict rather
// than silently clobbering each other's root segment.
func TestChainGuard_SeedRaceConflicts(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{memStore: newMemStore()}
	raced := false
	store.beforePutIfAbsent = func(path string) {
		if raced {
			return
		}
		raced = true
		store.data[path] = []byte(`{}`)
	}
	full := bug66Manifest(irbackup.BackupKindFull, "0/A0")
	requireChainConflict(t, UpdateLineageForManifest(ctx, store, full, ManifestFileName, blobcodec.CodecNone))
	if _, taken := store.data[LineageCatalogFileName]; taken {
		t.Error("refused seed still wrote lineage.json")
	}
}

// TestChainGuard_BestEffortStillSwallowsOrdinaryFailures pins the
// pre-existing best-effort contract: a NON-conflict catalog failure
// stays a WARN, not an error (the manifest file is authoritative for
// the one-segment shape).
func TestChainGuard_BestEffortStillSwallowsOrdinaryFailures(t *testing.T) {
	ctx := context.Background()
	store := &failingCatalogPutStore{memStore: newMemStore()}
	full := bug66Manifest(irbackup.BackupKindFull, "0/A0")
	if err := UpdateLineageForManifestBestEffort(ctx, store, full, ManifestFileName, blobcodec.CodecNone); err != nil {
		t.Fatalf("BestEffort returned an ordinary failure: %v; want WARN-swallowed nil", err)
	}
}

// failingCatalogPutStore fails every Put of lineage.json (an ordinary
// transient store failure, NOT a conflict).
type failingCatalogPutStore struct{ *memStore }

func (s *failingCatalogPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	if path == LineageCatalogFileName {
		return errors.New("injected transient store failure")
	}
	return s.memStore.Put(ctx, path, r)
}

// TestChainGuard_NoCapabilityStoreUnchanged pins the graceful degrade:
// a store WITHOUT the ConditionalPutter capability keeps today's
// unguarded last-write-wins behavior — no markers, no refusal. This is
// the deliberate compatibility posture, recorded in ADR-0160.
func TestChainGuard_NoCapabilityStoreUnchanged(t *testing.T) {
	ctx := context.Background()
	store := newMemStore() // plain — no PutIfAbsent

	seed := guardFixtureCatalog("postgres")
	if err := WriteLineageCatalog(ctx, store, seed); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	a, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("writer A load: %v", err)
	}
	b, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("writer B load: %v", err)
	}
	a.Segments[0].Incrementals = []string{"manifests/incr-a.json"}
	if err := WriteLineageCatalog(ctx, store, a); err != nil {
		t.Fatalf("writer A write: %v", err)
	}
	b.Segments[0].Incrementals = []string{"manifests/incr-b.json"}
	if err := WriteLineageCatalog(ctx, store, b); err != nil {
		t.Fatalf("writer B write on a no-CAS store: %v; want unguarded success (pre-guard behavior)", err)
	}
	for k := range store.data {
		if strings.HasPrefix(k, ChainGenPrefix) {
			t.Errorf("no-capability store grew a claim marker %q", k)
		}
	}
}

// TestChainGuard_OrphanedClaimDoesNotBrick pins liveness: a writer
// that claimed a slot and CRASHED before its catalog Put leaves an
// orphaned marker, and the next writer observes past it — no stale
// lock, no manual cleanup, no TTL heuristics.
func TestChainGuard_OrphanedClaimDoesNotBrick(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{memStore: newMemStore()}

	seed := guardFixtureCatalog("postgres")
	if err := WriteLineageCatalog(ctx, store, seed); err != nil { // unstamped fixture write
		t.Fatalf("seed write: %v", err)
	}
	// A crashed writer's residue: generation 5 claimed, catalog never
	// written.
	store.data[chainGenMarkerPath(5)] = []byte(`{}`)

	cat, _, err := LoadLineageCatalogForUpdate(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cat.Segments[0].Incrementals = []string{"manifests/incr-1.json"}
	if err := WriteLineageCatalog(ctx, store, cat); err != nil {
		t.Fatalf("write after an orphaned claim: %v; want success at the next generation", err)
	}
	if _, ok := store.data[chainGenMarkerPath(6)]; !ok {
		t.Error("write did not claim generation 6 (5's successor)")
	}
}

// TestChainGuard_DegradeOnNonConflictClaimError pins the availability
// tradeoff for backends whose conditional-PUT support fails at runtime
// (an S3-compatible predating conditional writes): the claim's NON-
// conflict failure degrades to an unguarded write with a WARN instead
// of bricking the chain. Named wart; ADR-0160 "runtime degradation".
func TestChainGuard_DegradeOnNonConflictClaimError(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{
		memStore:       newMemStore(),
		putIfAbsentErr: errors.New("501 NotImplemented: If-None-Match unsupported"),
	}
	seed := guardFixtureCatalog("postgres")
	stampChainGuard(seed, chainGuardStamp{gen: 0, observed: true})
	if err := WriteLineageCatalog(ctx, store, seed); err != nil {
		t.Fatalf("write on a claim-degraded store: %v; want unguarded success", err)
	}
	if _, ok := store.data[LineageCatalogFileName]; !ok {
		t.Error("degraded write did not land the catalog")
	}
}

// TestChainGuard_MarkerGCKeepsTrailingWindow pins the marker GC: after
// N guarded writes only the trailing window survives — bounded litter,
// while keeping enough history that a slot is never re-opened under a
// merely-slow writer.
func TestChainGuard_MarkerGCKeepsTrailingWindow(t *testing.T) {
	ctx := context.Background()
	store := &casMemStore{memStore: newMemStore()}

	full := bug66Manifest(irbackup.BackupKindFull, "0/A0")
	if err := UpdateLineageForManifest(ctx, store, full, ManifestFileName, blobcodec.CodecNone); err != nil {
		t.Fatalf("seed: %v", err)
	}
	const writes = 12 // total generations claimed (seed = 1)
	for i := 2; i <= writes; i++ {
		cat, _, err := LoadLineageCatalogForUpdate(ctx, store)
		if err != nil {
			t.Fatalf("load %d: %v", i, err)
		}
		cat.UpdatedAt = cat.UpdatedAt.Add(1) // any mutation
		if err := WriteLineageCatalog(ctx, store, cat); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	var gens []uint64
	for k := range store.data {
		if n, ok := parseChainGenMarker(k); ok {
			gens = append(gens, n)
		}
	}
	// Floor = writes - chainGenKeepTrailing; markers below it are GC'd.
	wantFloor := uint64(writes - chainGenKeepTrailing)
	wantCount := chainGenKeepTrailing + 1 // [floor, writes]
	if len(gens) != wantCount {
		t.Errorf("marker count = %d (%v); want %d", len(gens), gens, wantCount)
	}
	for _, g := range gens {
		if g < wantFloor {
			t.Errorf("marker generation %d survived GC; floor is %d", g, wantFloor)
		}
	}
	if _, ok := store.data[chainGenMarkerPath(writes)]; !ok {
		t.Errorf("newest marker g-%d missing", writes)
	}
}
