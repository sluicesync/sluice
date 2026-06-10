// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Chain-aware restore orchestrator. Phase 3.2 of the logical-backup
// feature (`docs/dev/design/logical-backups-phase-3.md`):
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

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
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

	// Envelope, when non-nil, is the [crypto.EnvelopeEncryption] used
	// to unwrap CEKs from encrypted manifests. Required for encrypted
	// chains. See [Restore.Envelope].
	Envelope crypto.EnvelopeEncryption

	// TargetSchema is the per-source target-schema namespace override
	// (ADR-0031). See [Restore.TargetSchema] for the design. Threaded
	// through to the chain's full-application step (via Restore) and
	// to the per-incremental ChangeApplier so user-data DDL +
	// INSERT/UPDATE/DELETE land in the named schema.
	TargetSchema string

	// chainCEK caches the chain-level CEK after the full's preflight.
	// Reused for every change-chunk decrypt across the incremental
	// walk so Argon2id (passphrase mode) runs once per chain restore.
	chainCEK []byte
}

// DefaultChainRestoreBatchSize is the default value of
// [ChainRestore.ApplyBatchSize] when left zero.
const DefaultChainRestoreBatchSize = 100

// Run executes the lineage-walk restore (ADR-0046 §3). Returns nil on
// success.
//
// The lineage is walked segment-by-segment in order. Each segment is:
// its full (idempotent bulk-copy / schema apply via the Restore path,
// scoped to the segment's Dir + codec) followed by its incrementals in
// chain order. The ONE boundary-monotonicity invariant
// (`prev.end <= cur.start`) is validated by [validateBoundary] — the
// SAME code path for intra-segment incremental boundaries and for
// segment-to-segment boundaries (ADR-0046 §3: no bimodal "is it
// rotated" branch, no SucceededBy chase). A malformed lineage
// (out-of-order, position regression, missing full, cyclic) is a LOUD
// refusal — never a silent partial assemble (DR data).
func (r *ChainRestore) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	// 1. Build the lineage chain: ordered segments, each with its
	//    full + incrementals as a flat link list, validated by the
	//    single boundary-monotonicity invariant. The target engine is
	//    a valid SOURCE-position comparator only for a same-engine
	//    restore (positions are engine-native); cross-engine restore
	//    passes nil and relies on the rotation FSM's write-time
	//    S>=P_N hard-fail + the structural same-engine guarantee.
	cmp := sameEngineComparator(ctx, r.Store, r.Target)
	links, err := buildLineageChain(ctx, r.Store, cmp)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: build lineage: %w", err))
	}
	if len(links) == 0 {
		return errors.New("chain restore: store contains no manifests")
	}

	root := links[0]
	incrementalCount := 0
	for _, l := range links {
		if canonicalKind(l.manifest.Kind) == ir.BackupKindIncremental {
			incrementalCount++
		}
	}

	// 1.9. ADR-0047 verbatim-extension restore-time engine gate. A
	//      segment carrying the recorded PG-restore-only marker
	//      restored to a non-PG target is a LOUD refusal before any
	//      data moves (the load-bearing safety pin; same severity as
	//      Bug 66 / the ADR-0035 PostGIS-absent refusal). The marker
	//      is read from lineage.json — the authoritative structural
	//      record — never sniffed from chunk bytes.
	if cat, err := resolveLineage(ctx, r.Store); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: %w", err))
	} else if err := refuseVerbatimRestoreToNonPG(cat, r.Target); err != nil {
		return wrapWithHint(PhaseConnect, err)
	}

	// 2. Cross-engine routing (Phase 5). Pre-flight the root full's
	//    schema + every link's delta for unsupportable types.
	crossEngine := root.manifest.SourceEngine != r.Target.Name() && root.manifest.SourceEngine != ""
	if crossEngine {
		if err := checkCrossEngineSupportable(
			root.manifest.Schema,
			root.manifest.SourceEngine, r.Target.Name(),
			fmt.Sprintf("chain restore: full %s", manifestBackupID(root.manifest)),
		); err != nil {
			return err
		}
		for _, link := range links[1:] {
			if err := checkCrossEngineDeltaSupportable(
				link.manifest.SchemaDelta,
				root.manifest.SourceEngine, r.Target.Name(),
				manifestBackupID(link.manifest),
			); err != nil {
				return err
			}
		}
		slog.InfoContext(
			ctx, "chain restore: cross-engine mode",
			slog.String("source_engine", root.manifest.SourceEngine),
			slog.String("target_engine", r.Target.Name()),
			slog.Int("incrementals", incrementalCount),
		)
	}

	// 2.5. Encryption pre-flight at the lineage root.
	if err := r.preflightEncryption(root.manifest); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: %w", err))
	}

	// 2.6. Mixed-mode encryption refusal across the whole lineage.
	if err := r.checkMixedModeChain(links); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: %w", err))
	}

	applier, err := r.Target.OpenChangeApplier(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("chain restore: open change applier: %w", err))
	}
	defer closeIf(applier)
	applyMaxBufferBytes(applier, r.MaxBufferBytes)
	applyTargetSchema(applier, r.TargetSchema)
	if err := applier.EnsureControlTable(ctx); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("chain restore: ensure control table: %w", err))
	}

	batchSize := r.ApplyBatchSize
	if batchSize <= 0 {
		batchSize = DefaultChainRestoreBatchSize
	}

	// 3. Walk every link in lineage order. A full link bulk-copies its
	//    snapshot (idempotent upsert over prior state — correct
	//    because seg[i].end <= seg[i+1].start, so a later segment's
	//    full carries strictly-newer-or-equal state); an incremental
	//    link replays its change chunks.
	firstFullApplied := false
	for i := range links {
		link := &links[i]
		switch canonicalKind(link.manifest.Kind) {
		case ir.BackupKindFull:
			// Segment 0's full establishes the schema + indexes; every
			// LATER segment full is a fresh snapshot of the same
			// (DDL-evolved) schema and must NOT re-run the
			// non-idempotent index/constraint phases — it refreshes
			// rows via an idempotent upsert (ADR-0046 §3).
			dataOnly := firstFullApplied
			slog.InfoContext(
				ctx, "chain restore: applying segment full",
				slog.String("segment_dir", link.segment.Dir),
				slog.String("manifest_path", link.path),
				slog.String("backup_id", manifestBackupID(link.manifest)),
				slog.String("codec", string(link.segment.codecOrDefault())),
				slog.Bool("data_only", dataOnly),
			)
			if err := r.applyFull(ctx, link, dataOnly); err != nil {
				return wrapWithHint(PhaseBulkCopy, fmt.Errorf("chain restore: apply segment full %s: %w",
					manifestBackupID(link.manifest), err))
			}
			firstFullApplied = true
		case ir.BackupKindIncremental:
			slog.InfoContext(
				ctx, "chain restore: applying incremental",
				slog.Int("link", i),
				slog.String("manifest_path", link.path),
				slog.String("backup_id", manifestBackupID(link.manifest)),
				slog.Int("change_chunks", len(link.manifest.ChangeChunks)),
				slog.Int("schema_deltas", len(link.manifest.SchemaDelta)),
			)
			if err := r.applyIncremental(ctx, link, applier, batchSize); err != nil {
				return wrapWithHint(PhaseCDC, fmt.Errorf("chain restore: incremental %s: %w",
					manifestBackupID(link.manifest), err))
			}
		default:
			return fmt.Errorf("chain restore: link %d (%s) has unknown kind %q",
				i, link.path, link.manifest.Kind)
		}
	}

	slog.InfoContext(
		ctx, "chain restore complete",
		slog.Int("manifests_applied", len(links)),
		slog.Int("incrementals", incrementalCount),
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

// applyFull delegates to the existing [Restore] path, scoped to the
// segment's per-Dir store and recorded codec. The full's manifest
// lives at the segment's FullManifestPath (== [ManifestFileName]
// within the segment store — the segment-root layout). For a
// one-segment lineage with Dir == "" this is byte-identical to the
// pre-ADR single-full restore.
//
// SkipChainDispatch=true prevents Restore from re-detecting the
// lineage and recursing. dataOnly is false for segment 0 (establishes
// schema + indexes) and true for every later segment (idempotent
// upsert refresh only — re-running CreateIndexes/Constraints on the
// already-built schema would error). Converges on the snapshot-at-S_n
// view because seg[i].end <= seg[i+1].start (no gap).
func (r *ChainRestore) applyFull(ctx context.Context, full *segmentRecord, dataOnly bool) error {
	if full.path != full.segment.FullManifestPath {
		return fmt.Errorf("chain restore: segment full manifest is at %q; segment records %q",
			full.path, full.segment.FullManifestPath)
	}
	if full.segment.FullManifestPath != ManifestFileName {
		return fmt.Errorf("chain restore: segment full manifest path %q; expected %q (v0.67.0 segment-root layout)",
			full.segment.FullManifestPath, ManifestFileName)
	}
	if err := validateRecordedCodec(full.segment.Codec); err != nil {
		return err
	}
	rest := &Restore{
		Target:            r.Target,
		TargetDSN:         r.TargetDSN,
		Store:             full.segment.store(r.Store),
		Filter:            r.Filter,
		MaxBufferBytes:    r.MaxBufferBytes,
		SkipChainDispatch: true,
		DataOnly:          dataOnly,
		Envelope:          r.Envelope,
		TargetSchema:      r.TargetSchema,
		segCodec:          full.segment.codecOrDefault(),
	}
	return rest.Run(ctx)
}

// preflightEncryption validates the chain root's encryption metadata
// and caches the chain-level CEK on r.chainCEK. Mirrors
// [Restore.preflightEncryption] but the cached CEK is consumed by the
// incremental change-chunk walk rather than the full's bulk-copy
// path.
func (r *ChainRestore) preflightEncryption(rootManifest *ir.Manifest) error {
	if rootManifest == nil || rootManifest.ChainEncryption == nil {
		return nil
	}
	enc := rootManifest.ChainEncryption
	if r.Envelope == nil {
		return fmt.Errorf("encrypted chain (algorithm=%q kek_mode=%q kek_ref=%q) requires --encrypt + a passphrase / KMS reference; no key was supplied",
			enc.Algorithm, enc.KEKMode, enc.KEKRef)
	}
	if enc.KEKMode != "" && r.Envelope.Mode() != enc.KEKMode {
		return fmt.Errorf("encryption envelope mode %q does not match chain's recorded kek_mode %q",
			r.Envelope.Mode(), enc.KEKMode)
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(enc.WrappedCEK) == 0 {
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty")
		}
		cek, err := r.Envelope.UnwrapCEK(enc.WrappedCEK)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase / KMS key?): %w", err)
		}
		r.chainCEK = cek
	}
	return nil
}

// checkMixedModeChain rejects a lineage where a segment's full and one
// of that segment's incrementals disagree on encryption shape (one
// encrypted, one not). Per ADR-0046 the per-chain encryption rule is
// now per-SEGMENT (each segment's full carries its own
// ChainEncryption); the uniformity is asserted within each segment.
// Mixed-mode strongly suggests a tampered / mis-stitched lineage —
// refuse loudly (DR data).
func (r *ChainRestore) checkMixedModeChain(chain []segmentRecord) error {
	if len(chain) < 2 {
		return nil
	}
	segEnc := false // current segment's full's encryption shape
	var segFullID string
	for _, link := range chain {
		if canonicalKind(link.manifest.Kind) == ir.BackupKindFull {
			segEnc = link.manifest.ChainEncryption != nil
			segFullID = manifestBackupID(link.manifest)
			continue
		}
		incrHasChunkEnc := false
		for _, c := range link.manifest.ChangeChunks {
			if c != nil && c.Encryption != nil {
				incrHasChunkEnc = true
				break
			}
		}
		if segEnc && !incrHasChunkEnc && len(link.manifest.ChangeChunks) > 0 {
			return fmt.Errorf("mixed-mode lineage: segment full %s is encrypted but incremental %s has plaintext change chunks; encryption must be uniform within a segment",
				segFullID, manifestBackupID(link.manifest))
		}
		if !segEnc && incrHasChunkEnc {
			return fmt.Errorf("mixed-mode lineage: segment full %s is plaintext but incremental %s has encrypted change chunks; encryption must be uniform within a segment",
				segFullID, manifestBackupID(link.manifest))
		}
	}
	return nil
}

// applyIncremental applies one incremental's schema deltas and
// streams its change chunks through the applier.
func (r *ChainRestore) applyIncremental(
	ctx context.Context,
	link *segmentRecord,
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
	//
	//    ADR-0049 Chunk D: a manifest may carry SchemaHistory entries
	//    even when ChangeChunks is empty (a window observed a DDL but
	//    no DML, then closed). Replay the schema-history regardless so
	//    a resumed stream finds the post-DDL version at backup.EndPosition.
	if len(link.manifest.ChangeChunks) == 0 && len(link.manifest.SchemaHistory) == 0 {
		slog.InfoContext(
			ctx, "chain restore: incremental has no change chunks; schema deltas only",
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

	// ADR-0049 Chunk D bug fix (CI 26132654611 ground truth): the
	// applier's case ir.SchemaSnapshot dispatch writes via
	// writeSchemaVersion(ctx, tx, a.streamID, ...) — using the FIELD
	// `a.streamID`, set only by SetStreamID, NOT the Apply/ApplyBatch
	// arg. migrate.go calls SetStreamID before its applier path
	// (line 1297-1298); chain restore did not, so a.streamID stayed
	// "" and the synthetic-SchemaSnapshot replay (Chunk D restore
	// path) wrote schema-history rows under stream_id="" instead of
	// ChainRestoreStreamID — defeating the operator value-prop
	// (resume-after-DDL without full re-snapshot). Mirror migrate.go's
	// pattern. Follow-up (Chunk F / v0.70.1): the dispatch's use of
	// a.streamID vs the Apply arg is fragile in general — should be
	// refactored to take the streamID from the arg consistently so any
	// future non-migrate sync path is not silently mis-keyed.
	if setter, ok := applier.(ir.StreamIDSetter); ok {
		setter.SetStreamID(ChainRestoreStreamID)
	}

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
func (r *ChainRestore) applySchemaDeltas(ctx context.Context, link *segmentRecord) error {
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
			manifestBackupID(link.manifest), err,
		)
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
		slog.InfoContext(
			ctx, "chain restore: schema delta — added tables",
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
				slog.WarnContext(
					ctx, "chain restore: schema delta — altered table with added columns; engine has no SchemaDeltaApplier; replay will rely on the applier's column-list reconciliation. If inserts fail, force a fresh full + new chain.",
					slog.String("table", d.Table),
					slog.Int("added_columns", len(added)),
				)
				continue
			}
			// Retarget the After-shape so cross-engine column types
			// (UUID → CHAR(36) etc.) get rewritten before emit.
			retargetSchema := translate.RetargetForEngine(
				&ir.Schema{Tables: []*ir.Table{d.After}},
				link.manifest.SourceEngine, r.Target.Name(),
			)
			retargetTable := retargetSchema.Tables[0]
			retargetAdded := addedColumns(d.Before, retargetTable)
			if err := applier.AlterAddColumn(ctx, retargetTable, retargetAdded); err != nil {
				return fmt.Errorf("alter add column on %s: %w", d.Table, err)
			}
			slog.InfoContext(
				ctx, "chain restore: schema delta — applied ADD COLUMN",
				slog.String("table", d.Table),
				slog.Int("added_columns", len(added)),
			)
		}
	}

	if drops > 0 {
		// Don't auto-drop in v1. The chain might carry inserts into a
		// table being dropped (out-of-order semantics under chain
		// replay); silently dropping would risk losing data.
		slog.WarnContext(
			ctx, "chain restore: schema delta — dropped tables encountered; v1 does NOT auto-DROP on the target. Drop manually after restore if the operator intent is to remove the table.",
			slog.Int("count", drops),
		)
	}

	return nil
}

// streamIncrementalChanges opens each change chunk in turn and
// pushes events onto out, verifying SHA-256 along the way.
//
// ADR-0049 Chunk D: prepends the manifest's SchemaHistory entries as
// [ir.SchemaSnapshot] events on the channel before the row events.
// The applier processes them through its normal dispatch path (engine
// SchemaSnapshot case → writeSchemaVersion in the SAME target tx as
// the ADR-0007 position write, locked decision #4a), seeding the
// target's sluice_cdc_schema_history table under [ChainRestoreStreamID].
// A subsequent `sluice sync start` using the same stream-id resumes
// at the backup's EndPosition with the post-DDL schema in effect —
// closing the previously documented pre-Chunk-D cold-start hazard.
//
// Idempotent: re-running chain restore replays the same snapshots; the
// engine's writeSchemaVersion is UPSERT-on-PK (MySQL ON DUPLICATE KEY
// UPDATE / PG ON CONFLICT DO UPDATE), so re-applies are value-identical
// no-ops.
func (r *ChainRestore) streamIncrementalChanges(
	ctx context.Context,
	link *segmentRecord,
	out chan<- ir.Change,
) error {
	if err := r.streamSchemaHistorySnapshots(ctx, link, out); err != nil {
		return fmt.Errorf("apply schema history: %w", err)
	}
	segStore := link.segment.store(r.Store)
	codec := link.segment.codecOrDefault()
	for chunkIdx, chunk := range link.manifest.ChangeChunks {
		if err := r.streamOneChangeChunk(ctx, segStore, codec, chunk, out); err != nil {
			return fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)
		}
		slog.DebugContext(
			ctx, "chain restore: chunk verified and streamed",
			slog.String("backup_id", manifestBackupID(link.manifest)),
			slog.Int("chunk", chunkIdx),
			slog.Int64("changes", chunk.RowCount),
		)
	}
	return nil
}

// streamSchemaHistorySnapshots converts each SchemaHistoryEntry on
// link.manifest.SchemaHistory into a synthetic [ir.SchemaSnapshot] and
// pushes it onto out. The applier's SchemaSnapshot dispatch persists
// the version into sluice_cdc_schema_history (locked decision #4a:
// same target tx as the position write — engine-side handled).
//
// A nil-table entry is loud (not silently skipped): a corrupt manifest
// would otherwise silently degrade future resume across the boundary
// (the exact silent-mis-decode class ADR-0049 exists to kill). This
// composes with the engine applier's own nil-IR guard.
func (r *ChainRestore) streamSchemaHistorySnapshots(
	ctx context.Context,
	link *segmentRecord,
	out chan<- ir.Change,
) error {
	if len(link.manifest.SchemaHistory) == 0 {
		return nil
	}
	for i, entry := range link.manifest.SchemaHistory {
		if entry == nil {
			return fmt.Errorf("schema-history entry %d: nil", i)
		}
		tbl, err := ir.UnmarshalTable(entry.TableJSON)
		if err != nil {
			return fmt.Errorf("schema-history entry %d (%s.%s): decode table: %w",
				i, entry.Schema, entry.Table, err)
		}
		if tbl == nil {
			return fmt.Errorf("schema-history entry %d (%s.%s) at %+v decoded to a nil table — corrupt manifest",
				i, entry.Schema, entry.Table, entry.AnchorPosition)
		}
		snap := ir.SchemaSnapshot{
			Position: entry.AnchorPosition,
			Schema:   entry.Schema,
			Table:    entry.Table,
			IR:       tbl,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- snap:
		}
	}
	slog.DebugContext(
		ctx, "chain restore: schema-history replayed",
		slog.String("backup_id", manifestBackupID(link.manifest)),
		slog.Int("entries", len(link.manifest.SchemaHistory)),
	)
	return nil
}

// streamOneChangeChunk reads chunk's events into out via the
// change-chunk reader. segStore is the chunk's segment store
// (Dir-prefixed) and codec is that segment's RECORDED codec (never
// sniffed from the bytes — ADR-0046 §5).
func (r *ChainRestore) streamOneChangeChunk(
	ctx context.Context,
	segStore ir.BackupStore,
	codec Codec,
	chunk *ir.ChunkInfo,
	out chan<- ir.Change,
) error {
	src, err := segStore.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	cek, err := r.changeChunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return fmt.Errorf("resolve change chunk cek: %w", err)
	}
	cr, err := newChangeChunkReader(src, chunk.SHA256, cek, codec)
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

// validateBoundary is THE single boundary-monotonicity invariant
// (ADR-0046 §3). It is called by [buildLineageChain] for BOTH an
// intra-segment incremental boundary (prev link → next link within a
// segment) AND a segment-to-segment boundary (prior segment's last
// link → next segment's full) — the SAME code path, proving the
// "one invariant, one check site" simplification. The invariant is
// `prev.EndPosition == cur.StartPosition` (exact match: a backup chain
// is a contiguous log, not merely non-decreasing — a gap loses data, a
// regression duplicates / corrupts; either is a loud refusal, never a
// silent partial assemble — DR data).
//
// The sole tolerance: an empty prev.EndPosition (a pre-Phase-3 / v0.16
// full that recorded no position) skips the check, exactly as the
// pre-ADR intra-chain validator did — preserving the legacy
// one-segment behaviour byte-for-byte.
// The exact flag selects the relation: intra-segment incremental
// boundaries are CONTIGUOUS (curStart == prevEnd, by writer
// construction — a gap loses events); segment-to-segment boundaries
// are MONOTONIC (prevEnd <= curStart — the new full's snapshot anchor
// S may legitimately be at-or-ahead of the prior segment's P_N, the
// (P_N, S] window being covered by the new full). Both are the SAME
// no-regression safety property checked at the SAME site; `exact`
// only tightens the intra-segment case to also forbid a forward gap.
// Monotonicity (the load-bearing property) is enforced via the
// engine's [ir.PositionMonotonicChecker] when available (PG + MySQL),
// the SAME mechanism as the rotation FSM's S>=P_N hard-fail.
func validateBoundary(cmp ir.PositionMonotonicChecker, prevEnd, curStart ir.Position, exact bool, prevLabel, curLabel string) error {
	if prevEnd.Engine == "" && prevEnd.Token == "" {
		return nil // legacy v0.16 full with no recorded position
	}
	if exact {
		if curStart != prevEnd {
			return fmt.Errorf(
				"lineage boundary mismatch at %s: StartPosition %+v does not equal preceding %s EndPosition %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
				curLabel, curStart, prevLabel, prevEnd,
			)
		}
		return nil
	}
	// Inter-segment: prevEnd must precede-or-equal curStart (no
	// regression). Exact contiguity is NOT required (S >= P_N).
	if curStart == prevEnd {
		return nil
	}
	if cmp != nil {
		le, err := cmp.PrecedesOrEqual(prevEnd, curStart)
		if err != nil {
			return fmt.Errorf("lineage boundary at %s: cannot prove monotonic vs preceding %s (DR data, refusing): %w",
				curLabel, prevLabel, err)
		}
		if !le {
			return fmt.Errorf(
				"lineage boundary REGRESSION at %s: StartPosition %+v precedes preceding %s EndPosition %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
				curLabel, curStart, prevLabel, prevEnd,
			)
		}
		return nil
	}
	// No comparator: fall back to the structural same-engine guarantee
	// (the rotation FSM already hard-asserted S>=P_N at write time).
	if curStart.Engine != prevEnd.Engine {
		return fmt.Errorf("lineage boundary at %s: engine %q != preceding %s engine %q (DR data, refusing)",
			curLabel, curStart.Engine, prevLabel, prevEnd.Engine)
	}
	return nil
}

// validateFirstIncrementalBoundary validates a segment's full ->
// first-incremental boundary, tolerating the ADR-0067 overlap. A
// rotation-opened segment KEEPS the (P_N, S] window in its incrementals,
// so its first incremental legitimately starts at P_N, which PRECEDES
// the full's anchor (fullEnd == S). Two properties are checked:
//
//  1. INTEGRITY: the first incremental must start exactly at the
//     segment's recorded coverage start (coverageStart ==
//     IncrementalCoverageStart, or StartPosition when unset). This is
//     what lets the no-comparator path trust the overlap: the rotation
//     FSM hard-asserted P_N <= S when it recorded coverageStart = P_N at
//     write time, so a first incremental that matches coverageStart is
//     known-good even when restore can't re-order positions. It also
//     catches a tampered/corrupt first incremental. Prune keeps
//     coverageStart in sync when it trims leading incrementals (see
//     PruneChain), so this does not spuriously fire post-prune.
//  2. NO FORWARD GAP: the first incremental must start at-or-before the
//     full's end -- the (firstStart, fullEnd] overlap re-applies
//     idempotently on restore (ADR-0010); a first incremental AFTER the
//     full's end would leave (fullEnd, firstStart) uncovered (a silent
//     data gap -> loud refusal). For a never-rotated segment firstStart
//     == fullEnd and this is the historical exact match.
func validateFirstIncrementalBoundary(cmp ir.PositionMonotonicChecker, fullEnd, recordedCoverage, firstStart ir.Position, segLabel string) error {
	if fullEnd.Engine == "" && fullEnd.Token == "" {
		return nil // legacy full with no recorded position (historical tolerance)
	}
	// Where the first incremental must start:
	//   - rotated segment (recordedCoverage set): the kept-overlap start
	//     P_N (which precedes the full's anchor fullEnd == S);
	//   - never-rotated segment (recordedCoverage unset): the full's own
	//     end — the historical contiguous chain. We compare against the
	//     full MANIFEST's EndPosition (authoritative), NOT the catalog's
	//     StartPosition, which legacy/rebuilt catalogs may leave unset.
	rotated := recordedCoverage.Engine != "" || recordedCoverage.Token != ""
	expected := fullEnd
	if rotated {
		expected = recordedCoverage
	}
	if firstStart != expected {
		return fmt.Errorf(
			"lineage boundary mismatch at %s: first incremental StartPosition %+v does not equal the expected start %+v — refusing to silently assemble a gapped/regressed restore (DR data)",
			segLabel, firstStart, expected,
		)
	}
	if !rotated {
		return nil // exact match against the full's end — historical behavior
	}
	// Rotated: the kept (recordedCoverage, fullEnd] overlap re-applies
	// idempotently on restore (ADR-0010). Require no FORWARD gap — the
	// coverage start must be at-or-before the full's end; a coverage start
	// AHEAD of the full would leave (fullEnd, recordedCoverage) uncovered.
	if recordedCoverage == fullEnd {
		return nil
	}
	if cmp != nil {
		le, err := cmp.PrecedesOrEqual(recordedCoverage, fullEnd)
		if err != nil {
			return fmt.Errorf("lineage %s: cannot prove first incremental start %+v <= full end %+v (DR data, refusing): %w",
				segLabel, recordedCoverage, fullEnd, err)
		}
		if !le {
			return fmt.Errorf(
				"lineage boundary mismatch at %s: first incremental StartPosition %+v is AHEAD of the segment full's end %+v — a forward gap would lose events between them (DR data, refusing)",
				segLabel, recordedCoverage, fullEnd,
			)
		}
		return nil
	}
	// No comparator: the rotation FSM hard-asserted P_N <= S from the live
	// source at write time when it recorded recordedCoverage; require the
	// same engine as the structural guarantee.
	if recordedCoverage.Engine != fullEnd.Engine {
		return fmt.Errorf("lineage %s: first incremental engine %q != full engine %q (DR data, refusing)",
			segLabel, recordedCoverage.Engine, fullEnd.Engine)
	}
	return nil
}

// buildLineageChain walks the lineage segment-by-segment and returns a
// flat ordered link list (each segment's full followed by its
// incrementals in chain order), validated by the SINGLE
// [validateBoundary] invariant at every adjacency — intra-segment and
// segment-to-segment alike. A malformed lineage (missing full,
// out-of-order, position regression/gap across any boundary, branching,
// cyclic) is a LOUD refusal — never a silent partial (ADR-0046 §3 /
// loud-failure tenet).
// cmp, when non-nil, is a [ir.PositionMonotonicChecker] for the
// lineage's SOURCE engine (typically the target engine when restoring
// same-engine — positions are then comparable). It enforces the
// inter-segment no-regression check; nil degrades to the structural
// same-engine guarantee (the rotation FSM already hard-asserted
// S>=P_N at write time via the live source engine).
func buildLineageChain(ctx context.Context, store ir.BackupStore, cmp ir.PositionMonotonicChecker) ([]segmentRecord, error) {
	cat, err := resolveLineage(ctx, store)
	if err != nil {
		return nil, err
	}
	if cat.RestorableFromSegment < 0 || cat.RestorableFromSegment >= len(cat.Segments) {
		return nil, fmt.Errorf("lineage restorable_from_segment=%d out of range [0,%d) — corrupt lineage",
			cat.RestorableFromSegment, len(cat.Segments))
	}

	var out []segmentRecord
	var prevLink *segmentRecord // last link of the previously-walked segment
	for si := cat.RestorableFromSegment; si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		if err := validateRecordedCodec(seg.Codec); err != nil {
			return nil, err
		}
		ss := seg.store(store)

		// Segment full.
		fm, err := readManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil {
			return nil, fmt.Errorf("segment %d (%s) full %q: %w", si, seg.SegmentID, seg.FullManifestPath, err)
		}
		if k := canonicalKind(fm.Kind); k != ir.BackupKindFull {
			return nil, fmt.Errorf("segment %d (%s) full manifest %q has kind %q; expected full",
				si, seg.SegmentID, seg.FullManifestPath, fm.Kind)
		}
		fullRec := segmentRecord{
			manifestRecord: manifestRecord{path: seg.FullManifestPath, manifest: fm},
			segment:        seg,
		}
		// Segment-to-segment boundary (MONOTONIC, not exact): the
		// prior segment's last link EndPosition (P_N) must
		// precede-or-equal this segment's recorded StartPosition (S
		// from lineage.json — the rotation anchor, NOT the full
		// manifest's empty StartPosition field). SAME validator as the
		// intra-segment boundary below, `exact=false`.
		if prevLink != nil {
			if err := validateBoundary(cmp, prevLink.manifest.EndPosition, seg.incrementalCoverageStartOrStart(), false,
				fmt.Sprintf("segment %d last link %s", si-1, manifestBackupID(prevLink.manifest)),
				fmt.Sprintf("segment %d (%s) incremental coverage start", si, seg.SegmentID)); err != nil {
				return nil, err
			}
		}
		out = append(out, fullRec)
		prevLink = &out[len(out)-1]

		// Intra-segment incremental chain. The lineage records the
		// ordered incremental paths; validate the parent-link + the
		// SAME boundary invariant against the running prev link.
		seenInc := make(map[string]string, len(seg.Incrementals))
		parentID := manifestBackupID(fm)
		for ii, ip := range seg.Incrementals {
			im, err := readManifestAt(ctx, ss, ip)
			if err != nil {
				return nil, fmt.Errorf("segment %d (%s) incremental %q: %w", si, seg.SegmentID, ip, err)
			}
			if k := canonicalKind(im.Kind); k != ir.BackupKindIncremental {
				return nil, fmt.Errorf("segment %d incremental %q has kind %q; expected incremental",
					si, ip, im.Kind)
			}
			id := manifestBackupID(im)
			if prevPath, dup := seenInc[id]; dup {
				return nil, fmt.Errorf("segment %d duplicate incremental BackupID %q (paths %q and %q)",
					si, id, prevPath, ip)
			}
			seenInc[id] = ip
			if im.ParentBackupID != "" && im.ParentBackupID != parentID {
				return nil, fmt.Errorf("segment %d incremental %q parent %q does not chain off preceding link %q — branching/mis-stitched lineage",
					si, ip, im.ParentBackupID, parentID)
			}
			if ii == 0 {
				// Full -> first-incremental boundary. ADR-0067: a
				// rotation-opened segment KEEPS the (P_N, S] overlap, so
				// its first incremental starts at IncrementalCoverageStart
				// (P_N), which PRECEDES the full's anchor (full.End == S).
				// Tolerate that backward overlap (it re-applies
				// idempotently on restore); refuse only a FORWARD gap
				// (first incremental starting AFTER the full's end would
				// lose events). For a never-rotated segment the coverage
				// start == full.End and this is the historical exact match.
				if err := validateFirstIncrementalBoundary(cmp, fm.EndPosition, seg.IncrementalCoverageStart, im.StartPosition,
					fmt.Sprintf("segment %d (%s)", si, seg.SegmentID)); err != nil {
					return nil, err
				}
			} else if err := validateBoundary(cmp, prevLink.manifest.EndPosition, im.StartPosition, true,
				fmt.Sprintf("segment %d link %d", si, ii),
				fmt.Sprintf("segment %d incremental %s", si, id)); err != nil {
				return nil, err
			}
			out = append(out, segmentRecord{
				manifestRecord: manifestRecord{path: ip, manifest: im},
				segment:        seg,
			})
			prevLink = &out[len(out)-1]
			parentID = id
		}
	}
	return out, nil
}

// sameEngineComparator returns eng as an [ir.PositionMonotonicChecker]
// IFF eng implements it AND eng.Name() matches the lineage's recorded
// source engine (positions are engine-native — a MySQL target cannot
// order PG LSNs). Otherwise nil (the inter-segment check degrades to
// the structural guarantee; the write-time S>=P_N hard-fail remains
// the authoritative monotonicity gate). Best-effort: a lineage-read
// hiccup yields nil rather than failing the restore here (the
// subsequent buildLineageChain surfaces real lineage errors).
func sameEngineComparator(ctx context.Context, store ir.BackupStore, eng ir.Engine) ir.PositionMonotonicChecker {
	chk, ok := eng.(ir.PositionMonotonicChecker)
	if !ok {
		return nil
	}
	cat, err := resolveLineage(ctx, store)
	if err != nil || cat.SourceEngine == "" || cat.SourceEngine != eng.Name() {
		return nil
	}
	return chk
}

// buildBrokerChain is the backup-broker (Phase 4.5) entry point. It
// walks the full lineage — single OR multi-segment — and returns the
// flat link list. The broker's apply loop skips every link whose
// manifest Kind is BackupKindFull (`broker.go::replayNewIncrementals`),
// so segment-N+1's rotation full is auto-skipped and the broker
// continues with the new segment's incremental tail. ADR-0067's
// born-contiguous rotation guarantees the new segment's first
// incremental covers the `(P_N, S]` overlap from the prior segment's
// end position; ADR-0010's idempotent applier handles the brief
// re-application of changes that landed before the broker last
// advanced its `last_applied_backup_id`. Phase 4.5 originally deferred
// multi-segment broker following pending validation that the existing
// chain-walker + idempotent-applier infrastructure actually covered
// the seam; Round D's soak (2026-05-31) characterized the gap and
// this commit closes it. Same comparator semantics as the single-
// segment path — nil is fine because the rotation FSM's write-time
// S>=P_N hard-assert is the authoritative monotonicity gate.
// Broker call sites reach this through [brokerChainCache], which
// memoizes the walk across ticks so an idle tick is O(1) store GETs
// instead of O(chain-length).
func buildBrokerChain(ctx context.Context, store ir.BackupStore) ([]segmentRecord, error) {
	return buildLineageChain(ctx, store, nil)
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
				d.Table, dropped[0], added[0],
			)
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

// changeChunkCEK resolves the CEK for a change chunk: per-chain mode
// returns the cached r.chainCEK; per-chunk mode unwraps the chunk's
// own wrapped CEK; plaintext returns nil.
func (r *ChainRestore) changeChunkCEK(chunk *ir.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		return nil, nil
	}
	if len(chunk.Encryption.WrappedCEK) > 0 {
		if r.Envelope == nil {
			return nil, errors.New("per-chunk encrypted change chunk encountered without envelope")
		}
		cek, err := r.Envelope.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap change chunk cek: %w", err)
		}
		return cek, nil
	}
	if r.chainCEK == nil {
		return nil, errors.New("encrypted change chunk encountered but chain CEK is unset")
	}
	return r.chainCEK, nil
}
