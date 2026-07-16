// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

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
	stdcrypto "crypto"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/sluicecode"
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

	// Store is the [irbackup.Store] the chain lives in. Required.
	Store irbackup.Store

	// Filter selects which tables from the chain participate.
	Filter migcore.TableFilter

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory. Same semantics as [Migrator.MaxBufferBytes].
	MaxBufferBytes int64

	// TableParallelism caps how many tables bulk-apply CONCURRENTLY
	// during each segment full's chunk restore (ADR-0084 restore
	// side). Threaded into the re-entrant [Restore] for every segment
	// full; the incremental change-replay path is untouched (ordered
	// by construction). Same semantics as [Restore.TableParallelism].
	TableParallelism int

	// ChunkParallelism caps how many of a single table's chunks
	// bulk-apply CONCURRENTLY during each segment full's chunk restore
	// — the within-table axis (ADR-0112). Threaded into the re-entrant
	// [Restore] for every segment full; the incremental change-replay
	// path is untouched (ordered by construction). Composes with
	// TableParallelism under the same connection-budget bound. Same
	// semantics as [Restore.ChunkParallelism].
	ChunkParallelism int

	// Summary is threaded into the re-entrant [Restore] for every
	// segment full so the CLI's `--format json` envelope sees chain
	// restores too. Same semantics as [Restore.Summary]; nil disables.
	Summary *migcore.RunSummary

	// ApplyBatchSize is the upper bound on changes per target
	// transaction during incremental replay. Same shape as
	// [Streamer.ApplyBatchSize]. Zero falls back to 100 — chain
	// restore wants throughput; the per-change conservative default
	// (1) would make even modest chains painfully slow.
	ApplyBatchSize int

	// ApplyConcurrency is the key-hash concurrent-apply LANE count for
	// the chain's incremental replay (ADR-0104/0105). Without it the
	// chain-restore incremental apply runs through the single-stream
	// ADR-0092 pipelined applier — RTT-bound on a high-latency /
	// cross-region target, so a chain carrying a large incremental
	// stalls exactly as the broker's polling path did before its
	// concurrent-apply fix (live Track-C finding, 2026-06-24: a
	// cold-start absorbed the full backup then wedged at ~1 change/s
	// applying an 8M-change incremental into cross-region PlanetScale-PG).
	// This is the chain-restore analog of that fix: the full-restore COPY
	// is already parallel (ADR-0112); this closes the incremental-apply
	// leg. Resolved through [migcore.ResolveReplayApplyConcurrency] (ADR-0106:
	// 0 = auto:N fast default, 1 = serial opt-out, N > 1 honored), so the
	// zero value gets the fast default (no zero-value-safe-default trap).
	ApplyConcurrency int

	// Envelope, when non-nil, is the [crypto.EnvelopeEncryption] used
	// to unwrap CEKs from encrypted manifests. Required for encrypted
	// chains. See [Restore.Envelope].
	Envelope crypto.EnvelopeEncryption

	// VerifyKey, when non-nil, is the asymmetric PUBLIC key (`--verify-key`
	// — Ed25519 / ECDSA / RSA) that verifies an ADR-0154 Phase 2/3 signed
	// chain (Ed25519 or KMS scheme). Required to verify such a chain;
	// orthogonal to Envelope. See [Restore.VerifyKey].
	VerifyKey stdcrypto.PublicKey

	// RequireSignature makes the ADR-0154 policy strict-always: a v6
	// (signed) chain that cannot be verified (no matching verify key)
	// refuses instead of WARN-and-proceeding. The default (false) never
	// fails a legitimate DR restore for a signature it cannot check; an
	// INVALID signature always refuses regardless.
	RequireSignature bool

	// TargetSchema is the per-source target-schema namespace override
	// (ADR-0031). See [Restore.TargetSchema] for the design. Threaded
	// through to the chain's full-application step (via Restore) and
	// to the per-incremental ChangeApplier so user-data DDL +
	// INSERT/UPDATE/DELETE land in the named schema.
	TargetSchema string

	// IndexBuildFallback is the optional ADR-0148 deploy-request
	// index-build fallback, threaded to the segment-0 full's Restore (the
	// segment that builds indexes; later DataOnly segments skip the schema
	// surface). See [Restore.IndexBuildFallback]. nil (the zero value)
	// leaves the direct index build byte-identical.
	IndexBuildFallback ir.IndexBuildFallback

	// chainCEK caches the chain-level CEK after the full's preflight.
	// Reused for every change-chunk decrypt across the incremental
	// walk so Argon2id (passphrase mode) runs once per chain restore.
	chainCEK []byte

	// chainEncrypted records whether the chain root carries
	// [irbackup.ChainEncryption] (per-chain OR per-chunk mode). Set in
	// preflightEncryption. When true, a change chunk with no ChunkEncryption
	// is a plaintext splice and is refused (BRK-3 parity with the broker;
	// mirrors SyncFromBackup.chainEncrypted). NOT derivable from chainCEK,
	// which stays nil in per-chunk mode even on an encrypted chain.
	chainEncrypted bool

	// Progress is the ADR-0155 presentation sink threaded from the
	// dispatching [Restore]. nil is the [progress.Nop] default. The chain
	// walk drives a coarse Schema/Data/Constraints checklist (the
	// per-segment Restores it constructs keep Progress nil, so the
	// checklist isn't double-emitted across segments).
	Progress progress.Sink
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
	sink := sinkOrNop(r.Progress)
	sink.PhaseStarted(restorePhaseSchema)

	// 1. Build the lineage chain: ordered segments, each with its
	//    full + incrementals as a flat link list, validated by the
	//    single boundary-monotonicity invariant. The target engine is
	//    a valid SOURCE-position comparator only for a same-engine
	//    restore (positions are engine-native); cross-engine restore
	//    passes nil and relies on the rotation FSM's write-time
	//    S>=P_N hard-fail + the structural same-engine guarantee.
	cmp := lineage.SameEngineComparator(ctx, r.Store, r.Target)
	links, err := lineage.BuildLineageChain(ctx, r.Store, cmp)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: build lineage: %w", err))
	}
	if len(links) == 0 {
		return errors.New("chain restore: store contains no manifests")
	}
	// Bug 182 (restore-path half): reject a tampered/bit-rotted manifest with a
	// null structural element up front, before any traversal nil-derefs it and
	// crashes the restore — the coded refusal `backup verify` already raises.
	for i := range links {
		if verr := validateManifestStructure(links[i].Manifest); verr != nil {
			return sluicecode.Wrap(sluicecode.CodeBackupSignatureInvalid,
				"the backup manifest is structurally invalid (tampered or corrupt) — restore from a known-good chain",
				fmt.Errorf("chain restore: manifest %q: %w", links[i].Path, verr))
		}
	}

	root := links[0]
	incrementalCount := 0
	for _, l := range links {
		if lineage.CanonicalKind(l.Manifest.Kind) == irbackup.BackupKindIncremental {
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
	if cat, err := lineage.ResolveLineage(ctx, r.Store); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: %w", err))
	} else if err := refuseVerbatimRestoreToNonPG(cat, r.Target); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}

	// 2. Cross-engine routing (Phase 5). Pre-flight the root full's
	//    schema + every link's delta for unsupportable types.
	crossEngine := root.Manifest.SourceEngine != r.Target.Name() && root.Manifest.SourceEngine != ""
	if crossEngine {
		if err := migcore.CheckCrossEngineSupportable(
			root.Manifest.Schema,
			root.Manifest.SourceEngine, r.Target.Name(),
			fmt.Sprintf("chain restore: full %s", lineage.ManifestBackupID(root.Manifest)),
		); err != nil {
			return err
		}
		for _, link := range links[1:] {
			if err := migcore.CheckCrossEngineDeltaSupportable(
				link.Manifest.SchemaDelta,
				root.Manifest.SourceEngine, r.Target.Name(),
				lineage.ManifestBackupID(link.Manifest),
			); err != nil {
				return err
			}
		}
		slog.InfoContext(
			ctx, "chain restore: cross-engine mode",
			slog.String("source_engine", root.Manifest.SourceEngine),
			slog.String("target_engine", r.Target.Name()),
			slog.Int("incrementals", incrementalCount),
		)
	}

	// 2.5. Encryption pre-flight at the lineage root.
	if err := r.preflightEncryption(root.Manifest); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: %w", err))
	}

	// 2.6. Mixed-mode encryption refusal across the whole lineage.
	if err := checkMixedModeChain(links); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: %w", err))
	}

	// 2.7. Schema-fingerprint corruption check across every link
	// (ADR-0152 — the check [irbackup.Manifest.SchemaHash] documents),
	// before anything lands on the target.
	if err := verifySchemaHashes(ctx, links); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}

	// 2.7b. BackupID recompute check (audit item 57): the schema-hash twin
	// for the four ComputeBackupID-covered fields (created_at/source_engine/
	// kind/EndPosition). Catches bit-rot / a truncated rewrite / a lazy edit
	// of one of those that forgot to recompute the id, before anything lands.
	if err := verifyBackupIDs(links); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}

	// 2.8. ADR-0154 whole-manifest signature + freshness verification.
	// A signed (v6) chain refuses loudly on a missing/invalid/rolled-back
	// signature, a truncated change-list, or a dropped-newest-link BEFORE
	// anything lands on the target. Pre-v6 chains are a no-op.
	if err := verifyChainSignatures(ctx, r.Store, links, verifyMaterial{env: r.Envelope, verifyPub: r.VerifyKey}, r.RequireSignature); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: %w", err))
	}

	applier, err := r.Target.OpenChangeApplier(ctx, r.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: open change applier: %w", err))
	}
	defer migcore.CloseIf(applier)
	migcore.ApplyMaxBufferBytes(applier, r.MaxBufferBytes)
	migcore.ApplyTargetSchema(applier, r.TargetSchema)
	migcore.ApplyApplyConcurrency(applier, migcore.ResolveReplayApplyConcurrency(r.ApplyConcurrency))
	if err := applier.EnsureControlTable(ctx); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("chain restore: ensure control table: %w", err))
	}

	batchSize := r.ApplyBatchSize
	if batchSize <= 0 {
		batchSize = DefaultChainRestoreBatchSize
	}

	sink.PhaseCompleted(restorePhaseSchema)
	sink.PhaseStarted(restorePhaseData)

	// 3. Walk every link in lineage order. A full link bulk-copies its
	//    snapshot (idempotent upsert over prior state — correct
	//    because seg[i].end <= seg[i+1].start, so a later segment's
	//    full carries strictly-newer-or-equal state); an incremental
	//    link replays its change chunks.
	firstFullApplied := false
	for i := range links {
		link := &links[i]
		switch lineage.CanonicalKind(link.Manifest.Kind) {
		case irbackup.BackupKindFull:
			// Segment 0's full establishes the schema + indexes; every
			// LATER segment full is a fresh snapshot of the same
			// (DDL-evolved) schema and must NOT re-run the
			// non-idempotent index/constraint phases — it refreshes
			// rows via an idempotent upsert (ADR-0046 §3).
			dataOnly := firstFullApplied
			slog.InfoContext(
				ctx, "chain restore: applying segment full",
				slog.String("segment_dir", link.Segment.Dir),
				slog.String("manifest_path", link.Path),
				slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
				slog.String("codec", string(link.Segment.CodecOrDefault())),
				slog.Bool("data_only", dataOnly),
			)
			if err := r.applyFull(ctx, link, dataOnly); err != nil {
				return migcore.WrapWithHint(migcore.PhaseBulkCopy, fmt.Errorf("chain restore: apply segment full %s: %w",
					lineage.ManifestBackupID(link.Manifest), err))
			}
			firstFullApplied = true
		case irbackup.BackupKindIncremental:
			slog.InfoContext(
				ctx, "chain restore: applying incremental",
				slog.Int("link", i),
				slog.String("manifest_path", link.Path),
				slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
				slog.Int("change_chunks", len(link.Manifest.ChangeChunks)),
				slog.Int("schema_deltas", len(link.Manifest.SchemaDelta)),
			)
			if err := r.applyIncremental(ctx, link, applier, batchSize); err != nil {
				return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("chain restore: incremental %s: %w",
					lineage.ManifestBackupID(link.Manifest), err))
			}
		default:
			return fmt.Errorf("chain restore: link %d (%s) has unknown kind %q",
				i, link.Path, link.Manifest.Kind)
		}
	}
	sink.PhaseCompleted(restorePhaseData)
	sink.PhaseStarted(restorePhaseConstraints)

	// 4. Chain-tail standalone-sequence re-prime (item 51, delta
	//    review finding #2). The base full primed each standalone
	//    sequence at the BASE manifest's captured position; the
	//    incremental links then applied rows that consumed LATER
	//    values, so without this pass the restored sequence silently
	//    re-issues every number the links consumed. Forward-only by
	//    construction (the engine primitive only advances a lagging
	//    sequence), sourced from the NEWEST link that carries a
	//    schema snapshot.
	if err := r.reprimeStandaloneSequences(ctx, links); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("chain restore: re-prime standalone sequences: %w", err))
	}

	// 5. Chain-tail identity-sequence re-sync (roadmap "Open bugs",
	//    filed 2026-07-03; live-reproduced as a 23505 on the first
	//    post-restore insert). The base full's restore ran
	//    SyncIdentitySequences at ITS tail (restore.go Phase 3), but
	//    the incremental links then applied rows with HIGHER ids
	//    straight through the change applier — leaving every identity
	//    column's sequence at the base full's max: a loud 23505 on
	//    constrained tables, a SILENT duplicate on keyless ones.
	//    Data-derived (MAX+1 read from the restored target), idempotent,
	//    safe to run unconditionally — the identity analogue of the
	//    standalone-sequence re-prime above.
	if err := r.syncIdentitySequencesAtTail(ctx, links); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("chain restore: sync identity sequences: %w", err))
	}

	slog.InfoContext(
		ctx, "chain restore complete",
		slog.Int("manifests_applied", len(links)),
		slog.Int("incrementals", incrementalCount),
	)
	sink.PhaseCompleted(restorePhaseConstraints)
	// ADR-0155 summary panel (TTY only; Nop ignores it). Chain restore's
	// natural rollup is the chain shape (manifests + incrementals applied)
	// plus the restored table/row totals from the shared RunSummary.
	chainFields := []progress.Field{
		{Label: "Manifests", Value: progress.HumanCount(int64(len(links)))},
		{Label: "Incrementals", Value: progress.HumanCount(int64(incrementalCount))},
	}
	if r.Summary != nil {
		stats := r.Summary.Tables()
		var rows int64
		for i := range stats {
			if stats[i].Rows != nil {
				rows += *stats[i].Rows
			}
		}
		chainFields = append(chainFields,
			progress.Field{Label: "Tables", Value: progress.HumanCount(int64(len(stats)))},
			progress.Field{Label: "Rows", Value: progress.HumanCount(rows)})
	}
	sink.Summary(progress.Result{Fields: chainFields})
	return nil
}

// sequenceReprimer is the optional engine surface the chain-restore
// tail uses to forward-only re-prime standalone sequences (item 51).
// Implemented by the postgres SchemaWriter; declared here (like
// [RawDefaultReader]) so the orchestrator stays engine-neutral.
type sequenceReprimer interface {
	ReprimeSequences(ctx context.Context, s *ir.Schema) error
}

// reprimeStandaloneSequences advances the target's standalone
// sequences to the NEWEST captured position in the chain. The newest
// link carrying a schema snapshot wins. Incremental manifests carry
// sequence positions refreshed at their window END (see
// schemaWithRefreshedSequences in incremental.go), so the residual
// exposure is only source advancement AFTER the final link's
// end-of-window read — the chain's ordinary row RPO, documented in
// docs/type-mapping.md "Sequences and serial columns". No-op when no
// link carries standalone sequences.
//
// A target engine without the re-prime surface is a loud error here,
// not a skip: reaching this point with sequences means the earlier
// cross-engine refusals were bypassed, and silently leaving a stale
// position is the exact class finding #2 closed.
func (r *ChainRestore) reprimeStandaloneSequences(ctx context.Context, links []lineage.SegmentRecord) error {
	var schema *ir.Schema
	for i := len(links) - 1; i >= 0; i-- {
		if s := links[i].Manifest.Schema; s != nil {
			schema = s
			break
		}
	}
	if schema == nil || len(schema.Sequences) == 0 {
		return nil
	}
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return fmt.Errorf("open target schema writer: %w", err)
	}
	defer migcore.CloseIf(sw)
	migcore.ApplyTargetSchema(sw, r.TargetSchema)
	reprimer, ok := sw.(sequenceReprimer)
	if !ok {
		return fmt.Errorf(
			"target engine %q cannot re-prime standalone sequence %q (no sequence surface) — "+
				"the restored sequence position would silently lag the applied rows",
			r.Target.Name(), schema.Sequences[0].Name,
		)
	}
	if err := reprimer.ReprimeSequences(ctx, schema); err != nil {
		return err
	}
	slog.InfoContext(ctx, "chain restore: standalone sequences re-primed from newest link schema",
		slog.Int("sequences", len(schema.Sequences)))
	return nil
}

// syncIdentitySequencesAtTail re-syncs every identity column's sequence
// from the restored rows after the chain's incremental links applied
// ids past the base full's max. Unlike the standalone re-prime above no
// optional engine surface is needed: SyncIdentitySequences is a
// required [ir.SchemaWriter] method (a documented no-op on engines
// whose counters auto-bump on direct INSERT — MySQL, SQLite), so the
// call is engine-neutral by construction and the broker's
// --reset-target-data ChainRestore inherits it for free. The table set
// comes from the NEWEST link carrying a schema snapshot (same
// resolution as the re-prime), retargeted and filtered exactly as the
// base full's restore was so the MAX reads only touch tables that
// exist on the target. Wrapped in the ADR-0114 reparent retry,
// mirroring restore.go's Phase 3 call shape.
func (r *ChainRestore) syncIdentitySequencesAtTail(ctx context.Context, links []lineage.SegmentRecord) error {
	var schema *ir.Schema
	var sourceEngine string
	for i := len(links) - 1; i >= 0; i-- {
		if s := links[i].Manifest.Schema; s != nil {
			schema = s
			sourceEngine = links[i].Manifest.SourceEngine
			break
		}
	}
	if schema == nil {
		return nil
	}
	// Retarget mirrors the full-restore path (identity for same-engine).
	// The filter trims into a FRESH slice — the newest link's manifest
	// schema must not be mutated (migcore.ApplyTableFilter trims in place, and
	// same-engine retarget returns the input pointer).
	schema = translate.RetargetForEngine(schema, sourceEngine, r.Target.Name())
	tables := schema.Tables
	if !r.Filter.IsEmpty() {
		kept := make([]*ir.Table, 0, len(tables))
		for _, t := range tables {
			if t != nil && r.Filter.Allows(t.Name) {
				kept = append(kept, t)
			}
		}
		tables = kept
	}
	if !hasIdentityColumn(tables) {
		return nil
	}
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return fmt.Errorf("open target schema writer: %w", err)
	}
	defer migcore.CloseIf(sw)
	migcore.ApplyTargetSchema(sw, r.TargetSchema)
	syncSchema := &ir.Schema{Tables: tables}
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
		return sw.SyncIdentitySequences(ctx, syncSchema)
	}); err != nil {
		return err
	}
	slog.InfoContext(ctx, "chain restore: identity sequences re-synced from the restored rows",
		slog.Int("tables", len(tables)))
	return nil
}

// hasIdentityColumn reports whether any table carries an identity /
// auto-increment integer column — the cheap pre-scan that lets the
// chain tail skip opening a schema writer when there is nothing to
// sync.
func hasIdentityColumn(tables []*ir.Table) bool {
	for _, t := range tables {
		if t == nil {
			continue
		}
		for _, c := range t.Columns {
			if intT, ok := c.Type.(ir.Integer); ok && intT.AutoIncrement {
				return true
			}
		}
	}
	return false
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
// lives at the segment's FullManifestPath (== [lineage.ManifestFileName]
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
func (r *ChainRestore) applyFull(ctx context.Context, full *lineage.SegmentRecord, dataOnly bool) error {
	if full.Path != full.Segment.FullManifestPath {
		return fmt.Errorf("chain restore: segment full manifest is at %q; segment records %q",
			full.Path, full.Segment.FullManifestPath)
	}
	if full.Segment.FullManifestPath != lineage.ManifestFileName {
		return fmt.Errorf("chain restore: segment full manifest path %q; expected %q (v0.67.0 segment-root layout)",
			full.Segment.FullManifestPath, lineage.ManifestFileName)
	}
	if err := blobcodec.ValidateRecordedCodec(full.Segment.Codec); err != nil {
		return err
	}
	rest := &Restore{
		Target:             r.Target,
		TargetDSN:          r.TargetDSN,
		Store:              full.Segment.Store(r.Store),
		Filter:             r.Filter,
		MaxBufferBytes:     r.MaxBufferBytes,
		TableParallelism:   r.TableParallelism,
		ChunkParallelism:   r.ChunkParallelism,
		Summary:            r.Summary,
		SkipChainDispatch:  true,
		DataOnly:           dataOnly,
		Envelope:           r.Envelope,
		TargetSchema:       r.TargetSchema,
		IndexBuildFallback: r.IndexBuildFallback,
		segCodec:           full.Segment.CodecOrDefault(),
	}
	return rest.Run(ctx)
}

// preflightEncryption validates the chain root's encryption metadata
// and caches the chain-level CEK on r.chainCEK. Mirrors
// [Restore.preflightEncryption] but the cached CEK is consumed by the
// incremental change-chunk walk rather than the full's bulk-copy
// path.
func (r *ChainRestore) preflightEncryption(rootManifest *irbackup.Manifest) error {
	if rootManifest == nil || rootManifest.ChainEncryption == nil {
		// SEC-MIRROR follow-up: a supplied key against a chain that claims
		// PLAINTEXT is refused, not silently ignored. On an unsigned chain a
		// store adversary can strip the root ChainEncryption marker and forge
		// every chunk as plaintext (a whole-chain downgrade); an operator who
		// passes --encrypt EXPECTS encryption, so "you gave me a key but this
		// backup says it is unencrypted" is the loud signal that catches it.
		if r.Envelope != nil {
			return sluicecode.Wrap(sluicecode.CodeBackupChunkAuthFailed,
				"remove --encrypt if this backup is genuinely unencrypted; if it should be encrypted, its chain-encryption marker was stripped (tampered/downgraded) — sign chains (--sign + --require-signature) to make this tamper-evident",
				errors.New("restore: an encryption key was supplied but this backup is not encrypted (no chain-encryption metadata) — refusing to restore a plaintext-claiming chain under a key"))
		}
		return nil
	}
	// The chain is encrypted (per-chain or per-chunk): every legitimate chunk
	// carries ChunkEncryption, so a plaintext chunk in the walk is a splice.
	r.chainEncrypted = true
	enc := rootManifest.ChainEncryption
	if r.Envelope == nil {
		return sluicecode.Wrap(sluicecode.CodeBackupEncryptionMismatch,
			"pass --encrypt with the chain's key material (the message names its kek_mode/kek_ref)",
			fmt.Errorf("encrypted chain (algorithm=%q kek_mode=%q kek_ref=%q) requires --encrypt + a passphrase / KMS reference; no key was supplied",
				enc.Algorithm, enc.KEKMode, enc.KEKRef))
	}
	if enc.KEKMode != "" && r.Envelope.Mode() != enc.KEKMode {
		return sluicecode.Wrap(sluicecode.CodeBackupEncryptionMismatch,
			"supply the key material matching the chain's recorded kek_mode (the passphrase for kek_mode=passphrase, the KMS reference for a KMS mode)",
			fmt.Errorf("encryption envelope mode %q does not match chain's recorded kek_mode %q",
				r.Envelope.Mode(), enc.KEKMode))
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(enc.WrappedCEK) == 0 {
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty")
		}
		// ADR-0152 chokepoint: identity-bound unwrap for v5+ roots,
		// legacy unwrap below; plus the Azure wrap-time key-version
		// retarget (audit N-9).
		cek, err := lineage.UnwrapChainCEK(r.Envelope, enc.WrappedCEK, rootManifest)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase / KMS key?): %w", err)
		}
		r.chainCEK = cek
		return nil
	}
	// Per-chunk mode: no chain-level CEK, but the envelope still needs
	// the Azure version retarget before the per-chunk unwraps run.
	lineage.RebindEnvelopeKEK(r.Envelope, rootManifest)
	return nil
}

// verifySchemaHashes recomputes every link's schema fingerprint and
// compares it against the manifest-recorded [irbackup.Manifest.SchemaHash]
// BEFORE anything lands on the target — the check the SchemaHash field
// doc promised and, pre-ADR-0152, nothing performed (audit N-8 item 4).
// This is CORRUPTION detection (bit-rot, truncated rewrite, mangled
// schema JSON), not tamper-proofing: the hash lives in the same
// unsigned manifest as the schema.
//
// Links without a recorded hash (fulls written before ADR-0152,
// pre-Phase-3 manifests) are skipped. One named carve-out: a pre-v5
// manifest carrying STANDALONE sequences can legitimately mismatch —
// older writers re-stamped the recorded Schema with end-of-window
// sequence state without re-stamping the hash when sequence OPTIONS
// changed inside a no-DDL window (the wart the incremental writer's
// item-51 comment predicted a "future hash-verifying chain walker"
// would trip on; current writers re-hash after the swap). That shape
// WARNs instead of refusing — refusing legitimate old chains on the DR
// path would be worse than the narrow miss.
func verifySchemaHashes(ctx context.Context, links []lineage.SegmentRecord) error {
	for i := range links {
		m := links[i].Manifest
		if m == nil || m.SchemaHash == "" {
			continue
		}
		got, err := irbackup.ComputeSchemaHash(m.Schema)
		if err != nil {
			return fmt.Errorf("chain restore: link %s: recompute schema hash: %w", links[i].Path, err)
		}
		if got == m.SchemaHash {
			continue
		}
		if len(m.Schema.Sequences) > 0 && m.FormatVersion < irbackup.FormatVersionEncryptedChunkBinding {
			slog.WarnContext(
				ctx, "chain restore: manifest's recorded schema hash does not match its schema; tolerated on this manifest shape (pre-v5 manifest with standalone sequences — older writers re-stamped sequence state without re-hashing when sequence options changed mid-window). Verify the restored sequences' options against the source",
				slog.String("manifest", links[i].Path),
				slog.String("recorded", m.SchemaHash),
				slog.String("recomputed", got),
			)
			continue
		}
		return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid,
			"restore from an untampered copy, or sign the chain (--sign/--sign-key + --require-signature) so a manifest edit is caught at verify time",
			fmt.Errorf("chain restore: manifest %s (backup %s) schema hash mismatch: recorded %s, recomputed %s — the manifest's schema does not match the fingerprint written with it (corrupted or partially-rewritten manifest); refusing before any data lands",
				links[i].Path, lineage.ManifestBackupID(m), m.SchemaHash, got))
	}
	return nil
}

// verifyBackupIDs recomputes every link's deterministic BackupID
// ([irbackup.ComputeBackupID]) and refuses on a mismatch with the recorded
// [irbackup.Manifest.BackupID] BEFORE anything lands — the BackupID twin of
// [verifySchemaHashes] (audit item 57). ComputeBackupID covers created_at,
// source_engine, kind, and EndPosition, so this catches bit-rot, a truncated
// rewrite, or a LAZY edit of one of those fields that forgot to recompute the
// id (corruption or tamper). Like the schema-hash check it is a corruption
// backstop, not tamper-PROOFING: a fully-coherent edit that also recomputes
// BackupID and fixes the ParentBackupID chain is signing-closed (ADR-0154 /
// --require-signature). Unlike the schema-hash case there is no legitimate-
// drift carve-out: the 2026-07-10 BackupID-coherence audit confirmed no
// manifest-mutating op re-stamps a covered field in place, so any mismatch on
// a non-empty recorded id is genuine.
//
// Links with an empty recorded BackupID (pre-Phase-3 fulls) are skipped — the
// walker computes those on demand; there is no recorded value to verify. That
// skip is safe ONLY for fulls: a CDC segment (incremental / streaming) has
// carried a recorded BackupID since Phase 3 introduced them, so an empty one is
// never writer-legitimate — it is a store adversary blanking the id to slip the
// recompute check (and, on an FV8 VStream segment, to un-bind the folded
// CDCPositionCommitsAfterRows flag). Refuse it rather than skip.
//
// The skip is keyed on STRUCTURE as well as the recorded Kind (audit-2026-07-12
// LOW): Kind is itself a manifest field, so an adversary blanking BackupID
// could also relabel the segment `full` to take this skip. A manifest carrying
// ChangeChunks IS a CDC segment whatever its label says, so it never skips. The
// conservative residual: evading BOTH keys means relabeling to full AND
// stripping the ChangeChunks — which degrades the tamper to the emptied-window
// shape the chain walk refuses via the EndPosition-reached backstop
// (Bug 183/184) and breaks the segment structure the lineage walk derives from
// fulls. Full tamper-proofing remains signing (--require-signature).
func verifyBackupIDs(links []lineage.SegmentRecord) error {
	for i := range links {
		m := links[i].Manifest
		if m == nil {
			continue
		}
		if m.BackupID == "" {
			if m.Kind == irbackup.BackupKindIncremental || len(m.ChangeChunks) > 0 {
				return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid,
					"restore from an untampered copy, or sign the chain (--sign/--sign-key + --require-signature) so a manifest edit is caught at verify time",
					fmt.Errorf("chain restore: manifest %s carries an empty BackupID on a CDC segment (kind %q, %d change chunk(s)) — a CDC segment always records one, so this is a corrupt or blanked-to-evade-verification manifest; refusing before any data lands",
						links[i].Path, lineage.CanonicalKind(m.Kind), len(m.ChangeChunks)))
			}
			continue
		}
		if got := irbackup.ComputeBackupID(m); got != m.BackupID {
			return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid,
				"restore from an untampered copy, or sign the chain (--sign/--sign-key + --require-signature) so a manifest edit is caught at verify time",
				fmt.Errorf("chain restore: manifest %s: recorded BackupID %s does not match its content (recomputed %s) — a BackupID-covered field (created_at/source_engine/kind/EndPosition) was edited without recomputing the id, or the manifest is corrupt; refusing before any data lands",
					links[i].Path, m.BackupID, got))
		}
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
func checkMixedModeChain(chain []lineage.SegmentRecord) error {
	if len(chain) < 2 {
		return nil
	}
	segEnc := false // current segment's full's encryption shape
	var segFullID string
	for _, link := range chain {
		if lineage.CanonicalKind(link.Manifest.Kind) == irbackup.BackupKindFull {
			segEnc = link.Manifest.ChainEncryption != nil
			segFullID = lineage.ManifestBackupID(link.Manifest)
			continue
		}
		incrHasChunkEnc := false
		for _, c := range link.Manifest.ChangeChunks {
			if c != nil && c.Encryption != nil {
				incrHasChunkEnc = true
				break
			}
		}
		if segEnc && !incrHasChunkEnc && len(link.Manifest.ChangeChunks) > 0 {
			return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid,
				"restore from an untampered copy of the chain, or sign it (--sign/--sign-key + --require-signature) so a mis-stitched/tampered lineage is caught at verify time",
				fmt.Errorf("mixed-mode lineage: segment full %s is encrypted but incremental %s has plaintext change chunks; encryption must be uniform within a segment",
					segFullID, lineage.ManifestBackupID(link.Manifest)))
		}
		if !segEnc && incrHasChunkEnc {
			return sluicecode.Wrap(sluicecode.CodeBackupManifestInvalid,
				"restore from an untampered copy of the chain, or sign it (--sign/--sign-key + --require-signature) so a mis-stitched/tampered lineage is caught at verify time",
				fmt.Errorf("mixed-mode lineage: segment full %s is plaintext but incremental %s has encrypted change chunks; encryption must be uniform within a segment",
					segFullID, lineage.ManifestBackupID(link.Manifest)))
		}
	}
	return nil
}

// applyIncremental applies one incremental's schema deltas and
// streams its change chunks through the applier.
func (r *ChainRestore) applyIncremental(
	ctx context.Context,
	link *lineage.SegmentRecord,
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
	if len(link.Manifest.SchemaDelta) > 0 {
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
	if len(link.Manifest.ChangeChunks) == 0 && len(link.Manifest.SchemaHistory) == 0 {
		// Bug 183/184: this branch is a 0-chunk AND 0-schema-history window, so
		// it can carry NO snapshot anchored at EndPosition. The only legitimate
		// shapes that reach here did NOT advance a position-bearing EndPosition:
		// a no-op window (EndPosition == StartPosition) or a schema-delta-only
		// window whose DDL produced no position advance (e.g. a DROP — no
		// Relation snapshot, so EndPosition stays the zero position and
		// posBearing is false). A posBearing EndPosition that ADVANCES beyond
		// StartPosition with neither chunks nor a schema snapshot is an emptied
		// change-chunk list — its events are gone while EndPosition overstates,
		// poisoning a later resume. Refuse.
		//
		// Bug 184 CORRECTION: the old `SchemaDelta == 0` carve-out was unsound.
		// A schema delta is an out-of-band before/after diff; it never advances
		// EndPosition on its own (only a written position-bearing change does),
		// so leaving SchemaDelta behind must not exempt an emptied data+DDL
		// window. The posBearing gate already excludes the legit DROP-only case.
		if end := link.Manifest.EndPosition; (end.Engine != "" || end.Token != "") &&
			end != link.Manifest.StartPosition {
			return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
				"restore from an untampered copy, or sign the chain so an emptied change-list is caught at verify time",
				fmt.Errorf("incremental %s: manifest records EndPosition %+v (StartPosition %+v) with no change chunks and no schema content — the change-chunk list was emptied; refusing to report success with dropped events",
					lineage.ManifestBackupID(link.Manifest), end, link.Manifest.StartPosition))
		}
		slog.InfoContext(
			ctx, "chain restore: incremental has no change chunks; schema deltas only",
			slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
		)
		return nil
	}
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	// Bounded buffer (see [migcore.RowChanBuffer]) so chunk decode and target
	// apply overlap instead of rendezvous-alternating — the replay
	// analog of migrate's bulk-copy hop discipline (perf-parity matrix
	// gap 2). Positions stay exact: the applier persists a position
	// only AFTER consuming the changes ahead of it, so buffered-but-
	// unapplied changes can never advance the durable position.
	changesCh := make(chan ir.Change, migcore.RowChanBuffer)
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
func (r *ChainRestore) applySchemaDeltas(ctx context.Context, link *lineage.SegmentRecord) error {
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return fmt.Errorf("open schema writer: %w", err)
	}
	defer migcore.CloseIf(sw)

	// Bucket the deltas by kind for clear logging + clean strategy
	// dispatch.
	var (
		adds, drops, alters int
	)
	for _, d := range link.Manifest.SchemaDelta {
		switch d.Kind {
		case irbackup.SchemaDeltaAddTable:
			adds++
		case irbackup.SchemaDeltaDropTable:
			drops++
		case irbackup.SchemaDeltaAlterTable:
			alters++
		default:
			return fmt.Errorf("unknown schema delta kind %q on table %q", d.Kind, d.Table)
		}
	}

	// Detect unsupportable shapes per the design doc's Schema-evolution
	// failure modes section. The "column dropped + new column with same
	// name" pattern across deltas in a single manifest indicates ambiguous
	// intent; for v1, refuse rather than risk wrong-shape application.
	if err := lineage.DetectAmbiguousDeltas(link.Manifest.SchemaDelta); err != nil {
		return fmt.Errorf(
			"unsupportable schema delta in incremental %s: %w. "+
				"Force a fresh full + new chain to recover",
			lineage.ManifestBackupID(link.Manifest), err,
		)
	}

	// AddTable: build a partial schema containing only the new tables
	// and call CreateTablesWithoutConstraints (idempotent).
	if adds > 0 {
		newTables := make([]*ir.Table, 0, adds)
		for _, d := range link.Manifest.SchemaDelta {
			if d.Kind == irbackup.SchemaDeltaAddTable && d.After != nil {
				newTables = append(newTables, d.After)
			}
		}
		s := translate.RetargetForEngine(&ir.Schema{Tables: newTables}, link.Manifest.SourceEngine, r.Target.Name())
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
		for _, d := range link.Manifest.SchemaDelta {
			if d.Kind != irbackup.SchemaDeltaAlterTable || d.Before == nil || d.After == nil {
				continue
			}
			added := lineage.AddedColumns(d.Before, d.After)
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
				link.Manifest.SourceEngine, r.Target.Name(),
			)
			retargetTable := retargetSchema.Tables[0]
			retargetAdded := lineage.AddedColumns(d.Before, retargetTable)
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
	link *lineage.SegmentRecord,
	out chan<- ir.Change,
) error {
	if err := r.streamSchemaHistorySnapshots(ctx, link, out); err != nil {
		return fmt.Errorf("apply schema history: %w", err)
	}
	segStore := link.Segment.Store(r.Store)
	codec := link.Segment.CodecOrDefault()

	// One-chunk read-ahead (perf-parity matrix gap 4): the fetcher
	// goroutine GETs + SHA-verifies chunk N+1 while chunk N's changes
	// decode and apply downstream, hiding the object-store round-trip
	// behind the apply. The handoff channel is UNBUFFERED — the fetcher
	// holds at most ONE verified chunk while waiting, so exactly one
	// chunk of read-ahead, never an N-deep pipeline (fetchChunkVerified
	// already buffers a whole chunk in memory; this at most doubles
	// that, bounded). Apply ORDER is strictly preserved (single fetcher,
	// single consumer, FIFO handoff) and the ADR-0117 per-chunk fetch
	// retry is untouched — it lives inside fetchChunkVerified, which
	// still runs once per chunk, just one chunk early.
	type fetchedChunk struct {
		idx   int
		chunk *irbackup.ChunkInfo
		src   io.ReadCloser
		err   error
	}
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel() // unblocks the fetcher if the decode below fails early
	fetchCh := make(chan fetchedChunk)
	go func() {
		defer close(fetchCh)
		for chunkIdx, chunk := range link.Manifest.ChangeChunks {
			src, err := blobcodec.FetchChunkVerified(fetchCtx, segStore, chunk.File, chunk.SHA256)
			select {
			case fetchCh <- fetchedChunk{idx: chunkIdx, chunk: chunk, src: src, err: err}:
			case <-fetchCtx.Done():
				if src != nil {
					_ = src.Close()
				}
				return
			}
			if err != nil {
				return
			}
		}
	}()
	// lastApplied tracks the position of the last position-bearing change
	// emitted across every chunk — the input to the F1 tail-truncation
	// backstop below. Confined to this producer goroutine.
	var lastApplied ir.Position
	for f := range fetchCh {
		if f.err != nil {
			// A SHA-256 mismatch (tampered/corrupt stored bytes) surfaces here
			// before decryption → coded SLUICE-E-BACKUP-CHUNK-CORRUPT.
			return lineage.CodeChunkHashError(fmt.Errorf("chunk %d (%s): open chunk: %w", f.idx, f.chunk.File, f.err))
		}
		if err := r.streamOneChangeChunk(ctx, link, codec, f.idx, f.chunk, f.src, out, &lastApplied); err != nil {
			return fmt.Errorf("chunk %d (%s): %w", f.idx, f.chunk.File, err)
		}
		slog.DebugContext(
			ctx, "chain restore: chunk verified and streamed",
			slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
			slog.Int("chunk", f.idx),
			slog.Int64("changes", f.chunk.RowCount),
		)
	}
	// F1 (SLUICE-E-BACKUP-INCOMPLETE): change-chunk tail-truncation backstop.
	// The window writer sets manifest.EndPosition to the position of the LAST
	// change it wrote (incremental.go: `lastPos = pos`; stream.go: same). A
	// store adversary who deletes the tail entries of an unsigned incremental's
	// ChangeChunks list leaves every survivor with an intact list ordinal (so
	// the GCM AAD still validates) but the replayed tail now falls SHORT of
	// EndPosition — a silent exit-0 with fewer events and an EndPosition that
	// overstates the data, poisoning a subsequent CDC resume. Assert we reached
	// EndPosition and refuse loudly. A fully-coherent edit that also lowers
	// EndPosition matches here and stays the documented, recoverable
	// whole-backup rollback.
	//
	// Bug 183/184: assert the replay actually REACHED EndPosition. A store
	// adversary who TRUNCATES or EMPTIES an unsigned incremental's ChangeChunks
	// leaves the survivors' AAD ordinals intact (so they still decrypt) but the
	// replayed content now falls SHORT of EndPosition — a silent exit-0 with
	// fewer events and an EndPosition that overstates the data, poisoning a
	// subsequent CDC resume. Refuse loudly. A fully-coherent edit that also
	// lowers EndPosition (and any matching anchor) matches as reached and stays
	// the documented, recoverable whole-backup rollback; signing closes that
	// residue.
	end := link.Manifest.EndPosition
	posBearing := end.Engine != "" || end.Token != ""
	claimsAdvance := end != link.Manifest.StartPosition
	// "Reached" = the last applied change-chunk position equals EndPosition.
	// A schema-history snapshot anchored at EndPosition is NOT trusted as proof
	// of completeness (audit-2026-07-12). Ground truth on real Postgres and
	// MySQL (item60_anchor_schemadelta_{pg,mysql} integration tests, both
	// engines) shows a legitimate window never presents a schema anchor at a
	// position-bearing EndPosition: a DDL-only window emits its snapshot with an
	// EMPTY EndPosition (posBearing false → this guard is skipped), and a data
	// window reaches EndPosition through its change-chunk tail. The only
	// producer of "anchor == EndPosition with a short/empty chunk tail" is a
	// store adversary who empties an unsigned window's chunks and re-anchors its
	// routine first-touch snapshot — and the anchor/SchemaDelta fields it edits
	// are outside every signing-independent cover (not the BackupID, not the
	// schema hash, not chunk AAD), so gating anchor-trust on those fields was a
	// bar-raise, not a closure (the item-57 lesson recurring: audit-2026-07-11
	// facet c / roadmap item 60). Resting completeness solely on the chunk tail
	// closes the PG/MySQL anchor-forge AND the VStream shared-position case
	// (Bug 184, where a snapshot could share a data row's position) at once,
	// signing-independently. --require-signature remains the belt-and-suspenders
	// for the whole unsigned manifest-edit class.
	reachedEnd := lastApplied == end
	if posBearing && claimsAdvance && !reachedEnd {
		return sluicecode.Wrap(sluicecode.CodeBackupIncomplete,
			"restore from an untampered copy, or sign the chain so a truncated/emptied change-list is caught at verify time",
			fmt.Errorf("incremental %s: replay reached position %+v but the manifest records EndPosition %+v (StartPosition %+v) with no change chunk or schema snapshot at EndPosition — the change-chunk list is truncated or emptied (fewer events than recorded); refusing to report success with a short tail",
				lineage.ManifestBackupID(link.Manifest), lastApplied, end, link.Manifest.StartPosition))
	}
	return nil
}

// streamSchemaHistorySnapshots converts each SchemaHistoryEntry on
// link.Manifest.SchemaHistory into a synthetic [ir.SchemaSnapshot] and
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
	link *lineage.SegmentRecord,
	out chan<- ir.Change,
) error {
	if len(link.Manifest.SchemaHistory) == 0 {
		return nil
	}
	for i, entry := range link.Manifest.SchemaHistory {
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
		slog.String("backup_id", lineage.ManifestBackupID(link.Manifest)),
		slog.Int("entries", len(link.Manifest.SchemaHistory)),
	)
	return nil
}

// streamOneChangeChunk decodes chunk's events from src — the
// already-fetched, SHA-verified chunk body handed over by
// [ChainRestore.streamIncrementalChanges]'s read-ahead fetcher — and
// pushes them into out. codec is the chunk's segment's RECORDED codec
// (never sniffed from the bytes — ADR-0046 §5).
func (r *ChainRestore) streamOneChangeChunk(
	ctx context.Context,
	link *lineage.SegmentRecord,
	codec blobcodec.Codec,
	chunkIdx int,
	chunk *irbackup.ChunkInfo,
	src io.ReadCloser,
	out chan<- ir.Change,
	lastApplied *ir.Position,
) error {
	cek, err := r.changeChunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return fmt.Errorf("resolve change chunk cek: %w", err)
	}
	// ADR-0152: the position binding comes from the chunk's OWNING
	// manifest — its recorded FormatVersion gates bound (v5+) vs the
	// legacy nil-AAD shape, per link, so a mixed chain (old full + new
	// incrementals) reads each side correctly. The list ordinal rides
	// in the binding because change-REPLAY order is semantic.
	cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, cek, codec, irbackup.ChangeChunkAADFor(link.Manifest, chunk, chunkIdx))
	if err != nil {
		// A change chunk decrypts at open; a tampered/spliced encrypted
		// change chunk fails its GCM auth tag here → coded refusal (SEC-1).
		return lineage.CodeChunkAuthError(fmt.Errorf("open chunk reader: %w", err))
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
		// F1 backstop bookkeeping: remember the last POSITION-BEARING change
		// we emit so [streamIncrementalChanges] can assert the replayed tail
		// reaches the manifest's EndPosition. Non-position-bearing changes
		// (e.g. a bare TxBegin/TxCommit without a source position) don't
		// advance it, mirroring the window writer's `lastPos` rule.
		if p := change.Pos(); p.Engine != "" || p.Token != "" {
			*lastApplied = p
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return ctx.Err()
		case out <- change:
		}
	}
	// A change-chunk SHA-256 mismatch surfaces at Close → coded
	// SLUICE-E-BACKUP-CHUNK-CORRUPT (a non-hash Close error passes through).
	return lineage.CodeChunkHashError(cr.Close())
}

// changeChunkCEK resolves the CEK for a change chunk: per-chain mode
// returns the cached r.chainCEK; per-chunk mode unwraps the chunk's
// own wrapped CEK; plaintext returns nil.
func (r *ChainRestore) changeChunkCEK(chunk *irbackup.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		if r.chainEncrypted {
			// BRK-3 parity: refuse a plaintext chunk spliced into an encrypted
			// chain rather than opening it as attacker cleartext. checkMixedModeChain
			// keys on ANY-chunk-encrypted per incremental, so it misses a single
			// plaintext chunk among encrypted siblings; this catches it.
			return nil, lineage.PlaintextChunkSplicedError(chunk.File)
		}
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
