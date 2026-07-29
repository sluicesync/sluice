// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The overlapping-cron chain-FORK gate.
//
// Reproduced 2026-07-28 against real AWS S3 and a local directory: two
// `sluice backup incremental` runs against the same chain both exited
// rc=0, both recorded the same `parent_backup_id`, and the lineage
// committed both as if sequential. From that moment `backup verify` and
// `restore` refused the chain permanently — and it kept ACCEPTING
// incrementals, so the damage banks silently until a recovery.
//
// ADR-0160's generation CAS did not catch it and was never going to: it
// serialises catalog WRITES, and both writes were serialised correctly
// (the `lineage.gen/` markers advanced 1→2→3 with no clobber). The
// missing precondition is on the LINK, not the write — an incremental
// may only be appended while its parent is still the chain's tip.
//
// These are the unit pins for that precondition, driving REAL
// [IncrementalBackup.Run] values through a fake CDC source so the race
// is exercised end to end without a database. The encryption-mode matrix
// and the "does it still RESTORE row-exact afterwards" half live in the
// integration cell `concurrent-incremental-fork` in
// backup_chain_shaping_encrypted_matrix_integration_test.go — a
// plaintext cell must never stand in for the encrypted ones (Bug 215).

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// gatedCDCEngine is a [fakeCDCEngine] whose CDC stream does not open
// until the test releases it. That is what lets two writers reach the
// state the field repro reaches: BOTH have resolved the same parent
// (resolveParent runs before the window opens — it is where the window's
// StartPosition comes from) while NEITHER has committed.
type gatedCDCEngine struct {
	*fakeCDCEngine

	opened chan struct{} // closed once StreamChanges has been entered
	gate   chan struct{} // StreamChanges returns only after this closes
	once   sync.Once
}

func (e *gatedCDCEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return &gatedCDCReader{engine: e}, nil
}

type gatedCDCReader struct {
	engine *gatedCDCEngine
}

func (r *gatedCDCReader) StreamChanges(ctx context.Context, from ir.Position) (<-chan ir.Change, error) {
	r.engine.once.Do(func() { close(r.engine.opened) })
	select {
	case <-r.engine.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	inner := &fakeCDCReader{engine: r.engine.fakeCDCEngine}
	return inner.StreamChanges(ctx, from)
}

func (*gatedCDCReader) Close() error { return nil }

// forkRaceWriter is one of the two competing `backup incremental` runs.
type forkRaceWriter struct {
	eng *gatedCDCEngine
	b   *IncrementalBackup
	err error
}

// newForkRaceWriter builds a writer whose window carries one distinct
// transaction, pinned to a distinct CreatedAt so the two runs cannot
// collide on manifest path, chunk namespace, or BackupID.
func newForkRaceWriter(store *blobcodec.LocalStore, schema *ir.Schema, lsn string, createdAt time.Time) *forkRaceWriter {
	eng := &gatedCDCEngine{
		fakeCDCEngine: &fakeCDCEngine{
			name:           "postgres",
			schemaSequence: []*ir.Schema{schema},
			cdcChanges: []ir.Change{
				ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/1` + lsn + `"}`}},
				ir.Insert{
					Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/2` + lsn + `"}`},
					Table:    "users",
					Row:      ir.Row{"id": int64(1), "name": "row-" + lsn},
				},
				ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/3` + lsn + `"}`}},
			},
			cdcExpectedFromOK: true,
		},
		opened: make(chan struct{}),
		gate:   make(chan struct{}),
	}
	return &forkRaceWriter{
		eng: eng,
		b: &IncrementalBackup{
			Source:        eng,
			SourceDSN:     "src",
			Store:         store,
			Window:        5 * time.Minute,
			ChunkChanges:  10,
			SluiceVersion: "test",
			Now:           func() time.Time { return createdAt },
			clockNow:      func() time.Time { return createdAt },
		},
	}
}

// seedForkRaceChain writes a chain root and catalogues it.
func seedForkRaceChain(t *testing.T) (*blobcodec.LocalStore, *ir.Schema, *irbackup.Manifest) {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	writeParentFullManifest(t, store, full)
	if err := lineage.UpdateLineageForManifest(context.Background(), store, full, lineage.ManifestFileName, blobcodec.DefaultCodec); err != nil {
		t.Fatalf("seed lineage: %v", err)
	}
	return store, schema, full
}

// assertSingleLinearChain requires the chain to hold exactly one
// incremental and to still build — i.e. the fork never landed.
func assertSingleLinearChain(t *testing.T, store *blobcodec.LocalStore, full *irbackup.Manifest) {
	t.Helper()
	ctx := context.Background()
	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if n := len(cat.Segments); n != 1 {
		t.Fatalf("segments = %d; want 1", n)
	}
	if n := len(cat.Segments[0].Incrementals); n != 1 {
		t.Fatalf("lineage.json records %d incrementals off one parent; want exactly 1. "+
			"Two entries IS the fork: both carry parent %q and every later `backup verify`/`restore` "+
			"refuses the chain permanently. incrementals=%v",
			n, full.BackupID, cat.Segments[0].Incrementals)
	}
	links, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatalf("BuildLineageChain over the surviving chain: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("chain links = %d; want 2 (full + 1 incremental)", len(links))
	}
}

// assertForkRefusal requires err to be the loud, coded refusal — not a
// generic failure, and certainly not nil.
func assertForkRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the losing writer returned nil: it committed a SIBLING incremental off an already-extended " +
			"parent. That is the fork — both runs report success and the chain is unrestorable from here on.")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupChainConflict {
		t.Fatalf("loser failed with %v; want a coded %s refusal so the operator sees the cause "+
			"(and so `sluice` exits with the refusal class rather than a generic error)",
			err, sluicecode.CodeBackupChainConflict)
	}
}

// TestIncrementalBackup_ConcurrentExtenders_LoserRefusesInsteadOfForking
// is the ordered replay of the field repro: two writers, both resolve
// the same parent, then one commits and the other tries to.
//
// The ordering is imposed rather than raced so the pin is deterministic
// and names ONE outcome — but the state it produces is exactly the
// field's: two live runs holding the same resolved parent.
func TestIncrementalBackup_ConcurrentExtenders_LoserRefusesInsteadOfForking(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)

	winner := newForkRaceWriter(store, schema, "10", time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC))
	loser := newForkRaceWriter(store, schema, "20", time.Date(2026, 7, 28, 11, 5, 0, 0, time.UTC))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, w := range []*forkRaceWriter{winner, loser} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.err = w.b.Run(ctx)
		}()
	}

	// Both writers are now past resolveParent and parked at the stream
	// open — the overlapping-cron state.
	for _, w := range []*forkRaceWriter{winner, loser} {
		select {
		case <-w.eng.opened:
		case <-ctx.Done():
			t.Fatal("writers did not both reach the CDC stream open")
		}
	}

	// Let the winner run to completion, THEN release the loser.
	close(winner.eng.gate)
	waitForChainLength(t, ctx, store, 1)
	close(loser.eng.gate)
	wg.Wait()

	if winner.err != nil {
		t.Fatalf("the first writer must succeed; got %v", winner.err)
	}
	assertForkRefusal(t, loser.err)
	assertSingleLinearChain(t, store, full)

	// The loser must not have left a manifest behind: it is refused
	// BEFORE the manifest write, which is what "writes nothing durable"
	// means for the common (ordered) case.
	recs, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("manifests on disk = %d; want 2 (full + the winner's incremental) — the refused writer "+
			"wrote a manifest it should have refused ahead of", len(recs))
	}
}

// TestIncrementalBackup_SimultaneousExtenders_NeverForkTheChain releases
// both writers at once, collapsing the whole race into the sub-
// millisecond window between one writer's catalog READ and its Put.
//
// What is pinned here is deliberately WEAKER than the ordered test above,
// and the difference is worth stating rather than papering over. In this
// window both writers can legitimately observe the same tip — that is
// ADR-0160's documented claim→Put residual, and the generation CAS does
// not close it either (a writer that observes the OTHER writer's marker
// claims the generation AFTER it and also "wins"). So "exactly one run
// exits non-zero" is not assertable without rearchitecting the claim, and
// asserting it would make this a flaky test rather than a strong one.
//
// What IS guaranteed, and is the DR-relevant property: the lineage never
// FORKS. At most one incremental per parent is ever recorded, and the
// chain still builds. The losing writer's link becomes inert debris
// instead of a sibling, and because both siblings START at the same
// parent position, whichever one survives leaves the chain contiguous —
// the next incremental resumes from its EndPosition and re-reads
// whatever the dropped window covered. That is a lost update; the
// pre-fix behaviour was an unrestorable chain.
func TestIncrementalBackup_SimultaneousExtenders_NeverForkTheChain(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)

	a := newForkRaceWriter(store, schema, "10", time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC))
	b := newForkRaceWriter(store, schema, "20", time.Date(2026, 7, 28, 11, 5, 0, 0, time.UTC))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, w := range []*forkRaceWriter{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.err = w.b.Run(ctx)
		}()
	}
	for _, w := range []*forkRaceWriter{a, b} {
		select {
		case <-w.eng.opened:
		case <-ctx.Done():
			t.Fatal("writers did not both reach the CDC stream open")
		}
	}
	close(a.eng.gate)
	close(b.eng.gate)
	wg.Wait()

	switch {
	case a.err != nil && b.err != nil:
		t.Fatalf("BOTH writers failed; at least one must commit. a=%v b=%v", a.err, b.err)
	case a.err != nil:
		assertForkRefusal(t, a.err)
	case b.err != nil:
		assertForkRefusal(t, b.err)
	default:
		// Both landed inside the claim→Put residual. Legal, and the
		// assertion below is what makes it survivable.
		t.Log("both writers exited 0 (the ADR-0160 claim→Put residual); the chain must still be unforked")
	}
	assertSingleLinearChain(t, store, full)
}

// TestIncrementalBackup_StaleParentRef_RefusesToForkTheChain is the same
// precondition reached the other way — the operator (or a retrying
// scheduler) naming an explicit `--parent` that is no longer the tip.
// No concurrency at all, which makes it the cheap always-on pin: a
// regression that removed the guard would turn it red without depending
// on goroutine scheduling.
func TestIncrementalBackup_StaleParentRef_RefusesToForkTheChain(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)

	first := newForkRaceWriter(store, schema, "10", time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC))
	close(first.eng.gate)
	if err := first.b.Run(context.Background()); err != nil {
		t.Fatalf("first incremental: %v", err)
	}

	// The chain tip is now the first incremental; ask to extend the FULL.
	second := newForkRaceWriter(store, schema, "20", time.Date(2026, 7, 28, 11, 5, 0, 0, time.UTC))
	second.b.ParentRef = full.BackupID
	close(second.eng.gate)
	assertForkRefusal(t, second.b.Run(context.Background()))
	assertSingleLinearChain(t, store, full)
}

// TestUpdateLineageForManifest_RefusesASiblingAppend is the durable
// gate's own pin, independent of any orchestrator: hand
// [lineage.UpdateLineageForManifest] a well-formed incremental whose
// parent is not the tip and it must refuse rather than record it.
//
// Without this, the guard would be pinned only through Run, so a later
// refactor that moved the pre-write check without keeping the durable
// one would still look green.
func TestUpdateLineageForManifest_RefusesASiblingAppend(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)
	ctx := context.Background()

	link := func(parentID, lsn string, at time.Time) (*irbackup.Manifest, string) {
		m := &irbackup.Manifest{
			FormatVersion:  irbackup.BackupFormatVersion,
			CreatedAt:      at,
			SourceEngine:   "postgres",
			Schema:         schema,
			Kind:           irbackup.BackupKindIncremental,
			ParentBackupID: parentID,
			StartPosition:  full.EndPosition,
			EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/` + lsn + `"}`},
			PartialState:   irbackup.BackupStateComplete,
		}
		m.BackupID = irbackup.ComputeBackupID(m)
		p := "manifests/incr-" + lsn + ".json"
		if err := lineage.WriteManifestAt(ctx, store, p, m); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		return m, p
	}

	first, firstPath := link(full.BackupID, "200", time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC))
	if err := lineage.UpdateLineageForManifest(ctx, store, first, firstPath, blobcodec.DefaultCodec); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Re-appending the SAME path must stay the idempotent no-op the
	// per-chunk checkpoint and the signing path rely on — the tip is then
	// this very manifest, and a guard that did not skip recorded paths
	// would refuse its own link.
	if err := lineage.UpdateLineageForManifest(ctx, store, first, firstPath, blobcodec.DefaultCodec); err != nil {
		t.Fatalf("idempotent re-append refused: %v", err)
	}

	sibling, siblingPath := link(full.BackupID, "300", time.Date(2026, 7, 28, 11, 5, 0, 0, time.UTC))
	err := lineage.UpdateLineageForManifest(ctx, store, sibling, siblingPath, blobcodec.DefaultCodec)
	assertForkRefusal(t, err)
	assertSingleLinearChain(t, store, full)

	// The best-effort wrapper must NOT swallow it — swallowing turns the
	// refusal back into the silent sibling commit.
	err = lineage.UpdateLineageForManifestBestEffort(ctx, store, sibling, siblingPath, blobcodec.DefaultCodec)
	assertForkRefusal(t, err)
	assertSingleLinearChain(t, store, full)
}

// TestIncrementalBackup_CrashOrphanedLink_IsHealedNotRefused is the
// guard's false-positive pin, and it exists because the guard compares
// against the CATALOG's tip.
//
// A crash between an incremental's manifest write and its best-effort
// lineage.json append leaves the link durable on disk and absent from the
// catalog. resolveParent then walks to the ON-DISK tail while the catalog
// still names the previous link — so a naive tip check would refuse a
// chain that is merely un-catalogued, and refuse it forever. The
// reconcile [IncrementalBackup.openSegment] runs (the same one `backup
// stream`'s resume runs) heals that first, which is what keeps the
// refusal meaning "a second writer" rather than "a crash happened once".
//
// Pre-guard this shape did NOT refuse — it silently appended, producing a
// catalog whose first incremental parents off a link the catalog does not
// list, i.e. exactly the mis-stitched lineage restore refuses. So this
// cell is a strict improvement in both directions.
func TestIncrementalBackup_CrashOrphanedLink_IsHealedNotRefused(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)
	ctx := context.Background()

	// The crash-orphan: a durable incremental with NO catalog entry.
	orphan := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 7, 28, 10, 30, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         schema,
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  full.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/150"}`},
		PartialState:   irbackup.BackupStateComplete,
	}
	orphan.BackupID = irbackup.ComputeBackupID(orphan)
	if err := lineage.WriteManifestAt(ctx, store, "manifests/incr-0000000000150.json", orphan); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	next := newForkRaceWriter(store, schema, "60", time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))
	close(next.eng.gate)
	if err := next.b.Run(ctx); err != nil {
		t.Fatalf("extending a chain whose tail link was crash-orphaned from lineage.json must SUCCEED "+
			"(the reconcile heals the catalog first); got %v", err)
	}

	cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if n := len(cat.Segments[0].Incrementals); n != 2 {
		t.Fatalf("catalog records %d incrementals; want 2 (the healed orphan + the new link): %v",
			n, cat.Segments[0].Incrementals)
	}
	links, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatalf("BuildLineageChain after the heal: %v", err)
	}
	if len(links) != 3 {
		t.Fatalf("chain links = %d; want 3 (full + orphan + new)", len(links))
	}
}

// TestIncrementalBackup_NoCatalog_GuardHasNoOpinion pins the guard's one
// named carve-out: when lineage.json is ABSENT there is no recorded tip,
// so the guard must not speak. The seeded catalog
// [lineage.UpdateLineageForManifest] invents in that branch has an empty
// incrementals list, so its "tip" is the segment full — and a chain whose
// on-disk tail is an incremental would be refused with a message blaming
// a concurrent writer that does not exist. Misattribution is precisely
// what this change set set out to remove, so the guard declines instead.
//
// What this does NOT claim: that the resulting chain is healthy. A chain
// with manifests on disk and no lineage.json is the pre-existing
// lineage.json-lost hazard — the catalog seeded here lists only the new
// link while that link parents off an incremental the catalog never
// mentions, and restore refuses it. `backup verify --rebuild-catalog` is
// what re-derives a truthful catalog. The assertion below is therefore
// narrow on purpose: whatever happens, it must not be a chain-conflict
// refusal.
func TestIncrementalBackup_NoCatalog_GuardHasNoOpinion(t *testing.T) {
	store, schema, full := seedForkRaceChain(t)
	ctx := context.Background()

	stray := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 7, 28, 10, 30, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         schema,
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  full.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/150"}`},
		PartialState:   irbackup.BackupStateComplete,
	}
	stray.BackupID = irbackup.ComputeBackupID(stray)
	if err := lineage.WriteManifestAt(ctx, store, "manifests/incr-0000000000150.json", stray); err != nil {
		t.Fatalf("write stray: %v", err)
	}
	// Lose the catalog entirely.
	if err := store.Delete(ctx, lineage.LineageCatalogFileName); err != nil {
		t.Fatalf("delete lineage.json: %v", err)
	}

	next := newForkRaceWriter(store, schema, "60", time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC))
	close(next.eng.gate)
	err := next.b.Run(ctx)
	if ce, coded := sluicecode.FromError(err); coded && ce.Code == sluicecode.CodeBackupChainConflict {
		t.Fatalf("the chain-tip guard fired on a chain with NO lineage.json, blaming a concurrent writer that "+
			"cannot be inferred from an absent catalog: %v", err)
	}
}

// waitForChainLength blocks until the open segment records n
// incrementals.
func waitForChainLength(t *testing.T, ctx context.Context, store *blobcodec.LocalStore, n int) {
	t.Helper()
	for {
		cat, ok, err := lineage.LoadLineageCatalog(ctx, store)
		if err == nil && ok && len(cat.Segments) > 0 && len(cat.Segments[len(cat.Segments)-1].Incrementals) >= n {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("lineage never reached %d incremental(s): %v", n, errors.Join(err, ctx.Err()))
		case <-time.After(10 * time.Millisecond):
		}
	}
}
