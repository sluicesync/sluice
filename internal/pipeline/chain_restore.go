// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Chain-aware restore orchestrator. Phase 3.2 of the logical-backup
// feature (`docs/dev/design-logical-backups-phase-3.md`):
// `sluice restore --from=<chain-url>` walks the chain
// [full, incr_1, incr_2, …, incr_N] in order, applying the full first
// and then replaying each incremental's serialised [Change] events
// through the existing applier path.
//
// Chain walk:
//
//   1. List every manifest in the store.
//   2. Identify the full (Kind == "full" or empty for legacy v0.16.x).
//   3. Build the chain by walking ParentBackupID links from each
//      incremental back to the full root. Validate it's a single
//      linear chain (no branching, no cycles, no missing links).
//   4. Apply the full via the existing [Restore] path.
//   5. For each incremental in order: apply schema deltas, then
//      stream the change chunks through [ir.ChangeApplier.ApplyBatch].
//
// Idempotency: per ADR-0010, the applier path is idempotent (Insert
// uses ON CONFLICT, Update / Delete tolerate zero-rows-affected). A
// re-run of chain restore against an already-restored target is
// safe; the applier replays without divergence.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// ChainRestoreStreamID is the stream identifier used by chain
// restore when calling [ir.ChangeApplier.ApplyBatch]. The id is
// stable across runs so a re-run-on-failure doesn't lose its
// idempotency floor.
const ChainRestoreStreamID = "sluice_chain_restore"

// ChainRestore runs a Phase 3 chain-aware restore from Store into
// Target / TargetDSN. Constructs an in-flight chain on Run() and
// applies it; concurrent calls on the same value are not supported.
type ChainRestore struct {
	// Target is the engine the target DSN belongs to. Required.
	Target ir.Engine

	// TargetDSN is the target-engine-native connection string. Required.
	TargetDSN string

	// Store is the [ir.BackupStore] the chain lives in. Required.
	Store ir.BackupStore

	// Filter selects which tables from the chain participate.
	Filter TableFilter

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory. Same semantics as [Migrator.MaxBufferBytes].
	MaxBufferBytes int64

	// ApplyBatchSize is the upper bound on changes per target
	// transaction during incremental replay. Same shape as
	// [Streamer.ApplyBatchSize]. Zero falls back to 100 — chain
	// restore wants throughput; the per-change conservative default
	// (1) would make even modest chains painfully slow.
	ApplyBatchSize int
}

// DefaultChainRestoreBatchSize is the default value of
// [ChainRestore.ApplyBatchSize] when left zero.
const DefaultChainRestoreBatchSize = 100

// Run executes the chain restore. Returns nil on success.
func (r *ChainRestore) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	// 1. Build the chain.
	chain, err := buildChain(ctx, r.Store)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: build chain: %w", err))
	}
	if len(chain) == 0 {
		return errors.New("chain restore: store contains no manifests")
	}

	// 2. Cross-engine routing (Phase 5). When the chain's source
	//    engine differs from the target engine, schema deltas in
	//    incrementals get translated via [translate.RetargetForEngine]
	//    before they're applied; change-event row payloads are shaped
	//    by the engine appliers' existing live-CDC value-translation
	//    path (the applier looks up target column types and routes
	//    each value through `prepareValue`). Pre-flight the chain's
	//    full schema for unsupportable types (PostGIS, hstore — same
	//    refusal the full cross-engine restore already surfaces) so
	//    the operator gets a clear refusal before any work happens.
	full := chain[0]
	crossEngine := full.manifest.SourceEngine != r.Target.Name() && full.manifest.SourceEngine != ""
	if crossEngine {
		if err := checkCrossEngineSupportable(
			full.manifest.Schema,
			full.manifest.SourceEngine, r.Target.Name(),
			fmt.Sprintf("chain restore: full %s", manifestBackupID(full.manifest)),
		); err != nil {
			return err
		}
		// Pre-flight every incremental's After-shape too. Schema
		// deltas may introduce a new table or column whose type is
		// unsupportable; surfacing it here means the operator's
		// recovery hint (--exclude-table) names the right table.
		for _, link := range chain[1:] {
			if err := checkCrossEngineDeltaSupportable(
				link.manifest.SchemaDelta,
				full.manifest.SourceEngine, r.Target.Name(),
				manifestBackupID(link.manifest),
			); err != nil {
				return err
			}
		}
		slog.InfoContext(ctx, "chain restore: cross-engine mode",
			slog.String("source_engine", full.manifest.SourceEngine),
			slog.String("target_engine", r.Target.Name()),
			slog.Int("incrementals", len(chain)-1),
		)
	}

	// 3. Apply the full via the existing Restore path.
	slog.InfoContext(ctx, "chain restore: applying full",
		slog.String("manifest_path", full.path),
		slog.String("backup_id", manifestBackupID(full.manifest)),
		slog.Int("incrementals", len(chain)-1),
	)
	if err := r.applyFull(ctx, full); err != nil {
		return wrapWithHint(PhaseBulkCopy, fmt.Errorf("chain restore: apply full: %w", err))
	}

	// 4. Apply each incremental in chain order.
	if len(chain) == 1 {
		slog.InfoContext(ctx, "chain restore: chain has no incrementals; full restore complete")
		return nil
	}

	applier, err := r.Target.OpenChangeApplier(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: open change applier: %w", err))
	}
	defer closeIf(applier)
	applyMaxBufferBytes(applier, r.MaxBufferBytes)
	if err := applier.EnsureControlTable(ctx); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("chain restore: ensure control table: %w", err))
	}

	batchSize := r.ApplyBatchSize
	if batchSize <= 0 {
		batchSize = DefaultChainRestoreBatchSize
	}

	for i, link := range chain[1:] {
		slog.InfoContext(ctx, "chain restore: applying incremental",
			slog.Int("index", i+1),
			slog.Int("total", len(chain)-1),
			slog.String("manifest_path", link.path),
			slog.String("backup_id", manifestBackupID(link.manifest)),
			slog.Int("change_chunks", len(link.manifest.ChangeChunks)),
			slog.Int("schema_deltas", len(link.manifest.SchemaDelta)),
		)
		if err := r.applyIncremental(ctx, link, applier, batchSize); err != nil {
			return wrapWithHint(PhaseCDC, fmt.Errorf("chain restore: incremental %s: %w",
				manifestBackupID(link.manifest), err))
		}
	}

	slog.InfoContext(ctx, "chain restore complete",
		slog.Int("manifests_applied", len(chain)),
	)
	return nil
}

// validate sanity-checks required fields.
func (r *ChainRestore) validate() error {
	switch {
	case r.Target == nil:
		return errors.New("chain restore: Target engine is nil")
	case r.TargetDSN == "":
		return errors.New("chain restore: TargetDSN is empty")
	case r.Store == nil:
		return errors.New("chain restore: Store is nil")
	}
	return nil
}

// applyFull delegates to the existing [Restore] path. The full's
// manifest lives at the conventional path ([ManifestFileName]); for a
// chain whose full was stored under that name (the only supported
// shape today), this is a one-line wrapper.
func (r *ChainRestore) applyFull(ctx context.Context, full *manifestRecord) error {
	if full.path != ManifestFileName {
		return fmt.Errorf("chain restore: full manifest is at %q; expected %q (Phase 3.1 only supports the standard layout)",
			full.path, ManifestFileName)
	}
	rest := &Restore{
		Target:            r.Target,
		TargetDSN:         r.TargetDSN,
		Store:             r.Store,
		Filter:            r.Filter,
		MaxBufferBytes:    r.MaxBufferBytes,
		SkipChainDispatch: true,
	}
	return rest.Run(ctx)
}

// applyIncremental applies one incremental's schema deltas and
// streams its change chunks through the applier.
func (r *ChainRestore) applyIncremental(
	ctx context.Context,
	link *manifestRecord,
	applier ir.ChangeApplier,
	batchSize int,
) error {
	// 1. Schema deltas first. Phase 3.2 same-engine: AddTable goes
	//    through CreateTablesWithoutConstraints (the table has no
	//    rows yet on the target — they arrive via the change
	//    stream). Drop / Alter surface as a clear refusal for v1
	//    unless the target is the same engine as the source AND the
	//    delta is a column-add we can cleanly express (the common
	//    case for chain restore).
	if len(link.manifest.SchemaDelta) > 0 {
		if err := r.applySchemaDeltas(ctx, link); err != nil {
			return fmt.Errorf("apply schema deltas: %w", err)
		}
	}

	// 2. Stream the change chunks through the applier. Use a derived
	//    context the producer side honours: if the applier errors out
	//    mid-chunk, cancelling the producer's context unblocks its
	//    `out <- change` send so the goroutine exits. Without this, a
	//    failed apply would leave the producer hung forever (the
	//    applier no longer drains the channel).
	if len(link.manifest.ChangeChunks) == 0 {
		slog.InfoContext(ctx, "chain restore: incremental has no change chunks; schema deltas only",
			slog.String("backup_id", manifestBackupID(link.manifest)),
		)
		return nil
	}
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	changesCh := make(chan ir.Change)
	errCh := make(chan error, 1)
	go func() {
		defer close(changesCh)
		errCh <- r.streamIncrementalChanges(streamCtx, link, changesCh)
	}()

	if batched, ok := applier.(ir.BatchedChangeApplier); ok {
		if err := batched.ApplyBatch(ctx, ChainRestoreStreamID, changesCh, batchSize); err != nil {
			streamCancel()
			<-errCh
			return fmt.Errorf("apply changes (batched): %w", err)
		}
	} else {
		if err := applier.Apply(ctx, ChainRestoreStreamID, changesCh); err != nil {
			streamCancel()
			<-errCh
			return fmt.Errorf("apply changes: %w", err)
		}
	}
	if err := <-errCh; err != nil {
		return fmt.Errorf("stream chunks: %w", err)
	}
	return nil
}

// applySchemaDeltas applies the manifest's SchemaDelta entries to
// the target. Phase 3.2 strategy: AddTable creates the new table
// (no rows yet — they arrive via subsequent change events);
// AlterTable applies via CreateTablesWithoutConstraints if the
// target supports IF NOT EXISTS / additive ALTER, otherwise refuses
// loudly; DropTable is a no-op for v1 (the chain might also include
// inserts into a table being dropped — out-of-order; the
// conservative thing is to keep the table around and let the
// operator drop manually after restore).
//
// The IR schema writers don't currently expose a "merge alter"
// surface; they take a whole schema and emit "create everything".
// For Phase 3.2 we lean on this by re-applying the Schema field's
// CreateTables — it's idempotent (engine writers IF NOT EXISTS) so
// a re-apply against an existing table is a no-op for the original
// columns. New tables get created. Column additions (the common
// alter case) are NOT picked up by this path; we surface a clear
// log line and document the limitation.
func (r *ChainRestore) applySchemaDeltas(ctx context.Context, link *manifestRecord) error {
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return fmt.Errorf("open schema writer: %w", err)
	}
	defer closeIf(sw)

	// Bucket the deltas by kind for clear logging + clean strategy
	// dispatch.
	var (
		adds, drops, alters int
	)
	for _, d := range link.manifest.SchemaDelta {
		switch d.Kind {
		case ir.SchemaDeltaAddTable:
			adds++
		case ir.SchemaDeltaDropTable:
			drops++
		case ir.SchemaDeltaAlterTable:
			alters++
		default:
			return fmt.Errorf("unknown schema delta kind %q on table %q", d.Kind, d.Table)
		}
	}

	// Detect unsupportable shapes per the design doc's Schema-evolution
	// failure modes section. The "column dropped + new column with same
	// name" pattern across deltas in a single manifest indicates ambiguous
	// intent; for v1, refuse rather than risk wrong-shape application.
	if err := detectAmbiguousDeltas(link.manifest.SchemaDelta); err != nil {
		return fmt.Errorf(
			"unsupportable schema delta in incremental %s: %w. "+
				"Force a fresh full + new chain to recover",
			manifestBackupID(link.manifest), err)
	}

	// AddTable: build a partial schema containing only the new tables
	// and call CreateTablesWithoutConstraints (idempotent).
	if adds > 0 {
		newTables := make([]*ir.Table, 0, adds)
		for _, d := range link.manifest.SchemaDelta {
			if d.Kind == ir.SchemaDeltaAddTable && d.After != nil {
				newTables = append(newTables, d.After)
			}
		}
		s := translate.RetargetForEngine(&ir.Schema{Tables: newTables}, link.manifest.SourceEngine, r.Target.Name())
		if err := sw.CreateTablesWithoutConstraints(ctx, s); err != nil {
			return fmt.Errorf("create added tables: %w", err)
		}
		slog.InfoContext(ctx, "chain restore: schema delta — added tables",
			slog.Int("count", adds),
		)
	}

	// AlterTable: emit ADD COLUMN for newly-added columns via the
	// engine's optional [ir.SchemaDeltaApplier] surface. Both PG and
	// MySQL implement it as of v0.17.0. Engines without the surface
	// fall through to the legacy "rely on change-stream
	// reconciliation" path, which works for additive INSERT shapes
	// on some engines but not all — the loud-failure tenet means
	// that's the operator's signal to take a fresh full.
	if alters > 0 {
		applier, _ := sw.(ir.SchemaDeltaApplier)
		for _, d := range link.manifest.SchemaDelta {
			if d.Kind != ir.SchemaDeltaAlterTable || d.Before == nil || d.After == nil {
				continue
			}
			added := addedColumns(d.Before, d.After)
			if len(added) == 0 {
				continue
			}
			if applier == nil {
				slog.WarnContext(ctx, "chain restore: schema delta — altered table with added columns; engine has no SchemaDeltaApplier; replay will rely on the applier's column-list reconciliation. If inserts fail, force a fresh full + new chain.",
					slog.String("table", d.Table),
					slog.Int("added_columns", len(added)),
				)
				continue
			}
			// Retarget the After-shape so cross-engine column types
			// (UUID → CHAR(36) etc.) get rewritten before emit.
			retargetSchema := translate.RetargetForEngine(
				&ir.Schema{Tables: []*ir.Table{d.After}},
				link.manifest.SourceEngine, r.Target.Name())
			retargetTable := retargetSchema.Tables[0]
			retargetAdded := addedColumns(d.Before, retargetTable)
			if err := applier.AlterAddColumn(ctx, retargetTable, retargetAdded); err != nil {
				return fmt.Errorf("alter add column on %s: %w", d.Table, err)
			}
			slog.InfoContext(ctx, "chain restore: schema delta — applied ADD COLUMN",
				slog.String("table", d.Table),
				slog.Int("added_columns", len(added)),
			)
		}
	}

	if drops > 0 {
		// Don't auto-drop in v1. The chain might carry inserts into a
		// table being dropped (out-of-order semantics under chain
		// replay); silently dropping would risk losing data.
		slog.WarnContext(ctx, "chain restore: schema delta — dropped tables encountered; v1 does NOT auto-DROP on the target. Drop manually after restore if the operator intent is to remove the table.",
			slog.Int("count", drops),
		)
	}

	return nil
}

// streamIncrementalChanges opens each change chunk in turn and
// pushes events onto out, verifying SHA-256 along the way.
func (r *ChainRestore) streamIncrementalChanges(
	ctx context.Context,
	link *manifestRecord,
	out chan<- ir.Change,
) error {
	for chunkIdx, chunk := range link.manifest.ChangeChunks {
		if err := r.streamOneChangeChunk(ctx, chunk, out); err != nil {
			return fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)
		}
		slog.DebugContext(ctx, "chain restore: chunk verified and streamed",
			slog.String("backup_id", manifestBackupID(link.manifest)),
			slog.Int("chunk", chunkIdx),
			slog.Int64("changes", chunk.RowCount),
		)
	}
	return nil
}

// streamOneChangeChunk reads chunk's events into out via the
// change-chunk reader.
func (r *ChainRestore) streamOneChangeChunk(
	ctx context.Context,
	chunk *ir.ChunkInfo,
	out chan<- ir.Change,
) error {
	src, err := r.Store.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	cr, err := newChangeChunkReader(src, chunk.SHA256)
	if err != nil {
		return fmt.Errorf("open chunk reader: %w", err)
	}
	for {
		change, err := cr.ReadChange()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cr.Close()
			return fmt.Errorf("read change: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return ctx.Err()
		case out <- change:
		}
	}
	return cr.Close()
}

// buildChain constructs the linear chain of manifests in the store,
// validating that the chain is unambiguous (single full root, every
// incremental's parent resolves, no cycles, no branching).
func buildChain(ctx context.Context, store ir.BackupStore) ([]*manifestRecord, error) {
	records, err := listAllManifests(ctx, store)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	// Find the full. Pre-Phase-3 manifests carry empty Kind which we
	// treat as full. Multiple fulls in one store is an unsupported
	// shape — fail loud.
	var (
		fulls        []*manifestRecord
		incrementals []*manifestRecord
	)
	for i := range records {
		rec := &records[i]
		switch rec.manifest.Kind {
		case ir.BackupKindFull, "":
			fulls = append(fulls, rec)
		case ir.BackupKindIncremental:
			incrementals = append(incrementals, rec)
		default:
			return nil, fmt.Errorf("manifest %q has unknown kind %q", rec.path, rec.manifest.Kind)
		}
	}
	if len(fulls) == 0 {
		return nil, errors.New("no full backup manifest found in store")
	}
	if len(fulls) > 1 {
		paths := make([]string, len(fulls))
		for i, f := range fulls {
			paths[i] = f.path
		}
		return nil, fmt.Errorf("store contains %d full manifests; chain restore requires exactly one (paths: %s)",
			len(fulls), joinComma(paths))
	}
	full := fulls[0]
	fullID := manifestBackupID(full.manifest)

	if len(incrementals) == 0 {
		return []*manifestRecord{full}, nil
	}

	// Detect duplicate IDs early — would surface as a branching
	// chain otherwise, but a more specific error here helps the
	// operator track the cause.
	seen := make(map[string]string, len(incrementals))
	for _, inc := range incrementals {
		id := manifestBackupID(inc.manifest)
		if prevPath, dup := seen[id]; dup {
			return nil, fmt.Errorf("duplicate incremental BackupID %q (paths %q and %q)",
				id, prevPath, inc.path)
		}
		seen[id] = inc.path
	}

	// Find children of each ID. A clean chain has exactly one child
	// per non-terminal node.
	children := make(map[string][]*manifestRecord, len(incrementals)+1)
	for _, inc := range incrementals {
		parent := inc.manifest.ParentBackupID
		if parent == "" {
			return nil, fmt.Errorf("incremental %q has empty ParentBackupID", inc.path)
		}
		children[parent] = append(children[parent], inc)
	}

	// Walk: full → child → child → ... until no more.
	chain := []*manifestRecord{full}
	visited := map[string]bool{fullID: true}
	currentID := fullID
	for {
		kids := children[currentID]
		if len(kids) == 0 {
			break
		}
		if len(kids) > 1 {
			paths := make([]string, len(kids))
			for i, k := range kids {
				paths[i] = k.path
			}
			return nil, fmt.Errorf("chain branches at %q: multiple children %s — chain restore requires a single linear chain",
				currentID, joinComma(paths))
		}
		kid := kids[0]
		kidID := manifestBackupID(kid.manifest)
		if visited[kidID] {
			return nil, fmt.Errorf("chain has a cycle at %q", kidID)
		}
		visited[kidID] = true
		chain = append(chain, kid)
		currentID = kidID
	}

	// Validate: every incremental in the store appears in the chain.
	if len(chain) != 1+len(incrementals) {
		// Find the orphans.
		var orphans []string
		for _, inc := range incrementals {
			id := manifestBackupID(inc.manifest)
			if !visited[id] {
				orphans = append(orphans, fmt.Sprintf("%s (%s, parent=%s)",
					id, inc.path, inc.manifest.ParentBackupID))
			}
		}
		return nil, fmt.Errorf("store contains %d incremental(s) but only %d are reachable from the full; orphans: %s",
			len(incrementals), len(chain)-1, joinComma(orphans))
	}

	// Validate: each incremental's StartPosition matches the previous
	// link's EndPosition. Surfaces tampering / mis-stitched chains.
	for i := 1; i < len(chain); i++ {
		prev := chain[i-1].manifest
		cur := chain[i].manifest
		// Allow empty StartPosition only when the previous link is a
		// pre-Phase-3 full with no recorded EndPosition.
		if prev.EndPosition.Engine == "" && prev.EndPosition.Token == "" {
			continue
		}
		if cur.StartPosition != prev.EndPosition {
			return nil, fmt.Errorf(
				"chain link mismatch at incremental %s: StartPosition %+v does not equal parent's EndPosition %+v (parent: %s)",
				manifestBackupID(cur), cur.StartPosition, prev.EndPosition, manifestBackupID(prev))
		}
	}

	return chain, nil
}

// detectAmbiguousDeltas returns a non-nil error when the slice
// contains a clearly unsupportable pattern (today: drop+add of the
// same column name within a single incremental, which the design
// doc names as ambiguous and recommends "force fresh full").
func detectAmbiguousDeltas(deltas []*ir.SchemaDeltaEntry) error {
	for _, d := range deltas {
		if d.Kind != ir.SchemaDeltaAlterTable {
			continue
		}
		if d.Before == nil || d.After == nil {
			continue
		}
		// Build column-name sets for before / after.
		bef := make(map[string]bool, len(d.Before.Columns))
		for _, c := range d.Before.Columns {
			bef[c.Name] = true
		}
		aft := make(map[string]bool, len(d.After.Columns))
		for _, c := range d.After.Columns {
			aft[c.Name] = true
		}
		// "Drop+add of the same name" wouldn't show up at the
		// incremental boundary (the diff only sees the start and end
		// shape). The genuine ambiguous case is a column-rename: a
		// before column missing from after AND an after column missing
		// from before, with different names. Surface that as
		// ambiguous so the operator can disambiguate.
		var dropped, added []string
		for name := range bef {
			if !aft[name] {
				dropped = append(dropped, name)
			}
		}
		for name := range aft {
			if !bef[name] {
				added = append(added, name)
			}
		}
		// Single drop + single add is the rename ambiguity. Multiple
		// of either is a more complex shape that's still
		// data-dependent; for v1 we stay conservative and refuse just
		// the rename pattern.
		if len(dropped) == 1 && len(added) == 1 {
			sort.Strings(dropped)
			sort.Strings(added)
			return fmt.Errorf(
				"table %q has dropped column %q and added column %q within one incremental window; ambiguous (rename vs independent edits)",
				d.Table, dropped[0], added[0])
		}
	}
	return nil
}

// addedColumns returns the columns in after but not in before,
// preserving after's declaration order.
func addedColumns(before, after *ir.Table) []*ir.Column {
	if after == nil {
		return nil
	}
	beforeNames := map[string]bool{}
	if before != nil {
		for _, c := range before.Columns {
			beforeNames[c.Name] = true
		}
	}
	out := make([]*ir.Column, 0, len(after.Columns))
	for _, c := range after.Columns {
		if !beforeNames[c.Name] {
			out = append(out, c)
		}
	}
	return out
}

// manifestBackupID returns m's stored or computed BackupID. Pre-
// Phase-3 manifests have an empty BackupID; we compute it on demand
// so chain code can identify links uniformly.
func manifestBackupID(m *ir.Manifest) string {
	if m == nil {
		return ""
	}
	if m.BackupID != "" {
		return m.BackupID
	}
	return ir.ComputeBackupID(m)
}
