// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Restore orchestrator. The inverse of [Backup]: read manifest, apply
// the schema (with cross-engine retargeting if needed), bulk-copy
// every chunk's rows into the target with per-chunk SHA-256
// verification, then create indexes / constraints / views.
//
// The restore-phase order mirrors [Migrator]:
//
//   1. CreateTablesWithoutConstraints
//   2. Bulk-copy rows from chunks (per-chunk SHA-256 verified)
//   3. SyncIdentitySequences
//   4. CreateIndexes
//   5. CreateConstraints
//   6. CreateViews
//
// Cross-engine restore is supported via [translate.RetargetForEngine]:
// a PG-source backup restoring into MySQL gets PG-native types
// rewritten to their MySQL-storage equivalents (UUID → CHAR(36), etc.)
// before the schema is applied. Same shape `sluice schema diff` uses.

import (
	"context"
	stdcrypto "crypto"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/translate"
)

// Restore runs a single Phase 1 full restore from Store into Target /
// TargetDSN. Construct the value, then call Run.
type Restore struct {
	// Target is the engine the target DSN belongs to. Required.
	// May differ from the backup's source engine — the orchestrator
	// runs `translate.RetargetForEngine` to bridge type differences.
	Target ir.Engine

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// Store is the [irbackup.Store] to read manifest + chunks from.
	// Required.
	Store irbackup.Store

	// Filter selects which tables from the manifest participate.
	// Empty (zero value) restores every table.
	Filter migcore.TableFilter

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory in the row writer. Same semantics as [Migrator.MaxBufferBytes].
	// Zero means "no cap".
	MaxBufferBytes int64

	// TableParallelism caps how many tables bulk-apply CONCURRENTLY
	// during the chunk-restore phase (ADR-0084 restore side). 0 = auto
	// (4, pgcopydb --table-jobs parity); 1 = serial (the pre-ADR-0084
	// behaviour). Engine-generic: parallel restore needs no shared
	// snapshot — each concurrent table writes through its own dedicated
	// row-writer connection, so it engages for EVERY target (PG and
	// MySQL). The resolved value is bounded by the target's measured
	// connection budget and clamped to the table count.
	TableParallelism int

	// ChunkParallelism caps how many of a SINGLE table's chunks
	// bulk-apply CONCURRENTLY during that table's restore — the
	// within-table axis (ADR-0112), the restore-side analog of
	// migrate's BulkParallelism (ADR-0019). 0 = auto (min(8, NumCPU));
	// 1 = serial (the pre-ADR-0112 single-stream behaviour). When the
	// resolved value P > 1 AND a table has >= 2 chunks, restoreTable
	// fans that table's chunk list across P workers, each writing
	// through its OWN dedicated row-writer connection (via the
	// openTargetRowWriter factory) and streaming its disjoint subset of
	// chunks through one WriteRows call. Snapshot chunks are a disjoint
	// partition of the table's rows, so parallel INSERT cannot collide
	// on a PK on a cold target (ADR-0112 §Correctness). The two axes
	// MULTIPLY (table × chunk) and are bounded at the SAME connection-
	// budget chokepoint migrate uses (migcore.ResolveTargetCopyParallelism +
	// migcore.ResolveCopyParallelismBudget, ADR-0076): within-table is satisfied
	// first, the table axis takes the remainder, the product never
	// exceeds the target's measured CopyBudget. Targets without a budget
	// prober (MySQL) pass through unclamped.
	ChunkParallelism int

	// Summary, when non-nil, collects the end-of-run per-table facts
	// the CLI's `--format json` result envelope renders — the rows
	// each [Restore.restoreTable] applied (accumulated, so a chain
	// restore that re-applies a table across segments sums up; the
	// incremental change-replay leg is not counted). nil (the zero
	// value) disables the bookkeeping. See [migcore.RunSummary].
	Summary *migcore.RunSummary

	// ApplyConcurrency is the key-hash concurrent-apply LANE count used
	// ONLY when this restore targets a multi-incremental chain and so
	// dispatches to [ChainRestore] (the incremental-replay leg). It has no
	// effect on a single-full restore, whose row load is the bulk-copy
	// COPY governed by TableParallelism × ChunkParallelism (ADR-0112).
	// Threaded into the dispatched ChainRestore so a chain carrying a
	// large incremental doesn't replay through the single-stream applier
	// and stall on a high-latency / cross-region target (the chain-restore
	// analog of the broker's concurrent-replay fix). Resolved through
	// [migcore.ResolveReplayApplyConcurrency] (ADR-0106: 0 = auto:N, 1 = serial,
	// N > 1 honored) inside ChainRestore.
	ApplyConcurrency int

	// TargetTelemetry, when non-nil, is an advisory control-plane health
	// provider (ADR-0107) the restore consults at parallelism-resolve time
	// to clamp the AUTO bulk×table fan-out by the target's LIVE CPU/memory
	// headroom (ADR-0115 / item 40). This is the PlanetScale-correct bound:
	// connections are abundant there (vtgate fronts a large pool) but CPU is
	// the scarce resource on small tiers, and the connection-budget split
	// only clamps engines with a budget prober (Postgres) — so a MySQL/
	// PlanetScale auto fan-out otherwise passes through unbounded onto a hot
	// instance. Advisory and degrades exactly like every other telemetry
	// consumer: nil (the default, e.g. the cold-copy grow-gate's signal-only
	// path) ⇒ no clamp, the pre-ADR-0115 behaviour. It never RAISES the
	// resolved parallelism and never clamps an explicitly-pinned axis.
	TargetTelemetry ir.TargetTelemetry

	// SkipChainDispatch, when true, suppresses the chain-detection
	// branch in [Restore.Run]. Used internally by [ChainRestore] so
	// that re-entering Restore for the full-application step doesn't
	// loop back into another chain restore. End-users leave this
	// false; the public single-manifest restore path detects chain
	// shape and dispatches.
	SkipChainDispatch bool

	// DataOnly, when true, restores ONLY the manifest's row data via an
	// idempotent upsert bulk-copy and skips CreateTables / CreateIndexes
	// / CreateConstraints / CreateViews. Set by [ChainRestore] for
	// every segment full AFTER the first in a multi-segment lineage
	// (ADR-0046 §3): segment 0 establishes the schema + indexes; a
	// later rotation segment's full is a fresh snapshot of the SAME
	// (possibly DDL-evolved) schema, so re-running the non-idempotent
	// index/constraint phases would error ("relation already exists"),
	// while the rows still need refreshing. Idempotent because the
	// later segment's snapshot is at S >= the prior segment's end, so
	// an upsert converges on the correct state. End-users leave this
	// false (a single-manifest restore always builds the schema).
	DataOnly bool

	// TargetSchema is the per-source target-schema namespace override
	// (ADR-0031), mirroring [Migrator.TargetSchema] /
	// [Streamer.TargetSchema] / [Previewer.TargetSchema]. When set,
	// the target schema writer / row writer / change applier route
	// user-data DDL + INSERTs into the named schema. Engines that
	// don't expose [ir.SchemaSetter] surface a flat-namespace refusal
	// at validate time (today: MySQL, since schemas == databases on
	// MySQL — operators use a different --target DSN database
	// instead). Empty preserves the DSN-derived default schema (the
	// pre-v0.56.0 shape). v0.56.0 / GitHub UX-gap closure flagged by
	// the v0.55.0 cycle subagent.
	TargetSchema string

	// Envelope, when non-nil, is the [crypto.EnvelopeEncryption] used
	// to unwrap CEKs from encrypted manifests. Required when the
	// chain's full manifest carries [irbackup.ChainEncryption]. A nil
	// Envelope against an encrypted chain produces a clear refusal
	// at chain-walk time naming the missing key — no partial data
	// lands on the target.
	Envelope crypto.EnvelopeEncryption

	// VerifyKey, when non-nil, is the asymmetric PUBLIC key (`--verify-key`
	// — Ed25519 / ECDSA / RSA) that verifies an ADR-0154 Phase 2/3 signed
	// chain (Ed25519 or KMS scheme). It is REQUIRED to verify such a chain —
	// the KEK does not verify an asymmetric signature — and is orthogonal to
	// [Restore.Envelope] (a chain may be encrypted AND asymmetrically
	// signed). Absent for HMAC-off-KEK chains.
	VerifyKey stdcrypto.PublicKey

	// RequireSignature makes the ADR-0154 signature policy strict-always
	// (see [ChainRestore.RequireSignature]). Threaded into the chain
	// dispatch. Default false — a legitimate DR restore never fails for a
	// signature it cannot check; an INVALID signature always refuses.
	RequireSignature bool

	// chainCEK caches the chain-level CEK after first unwrap so
	// per-chain mode pays the unwrap cost (Argon2id, KMS Decrypt)
	// exactly once per Run. Internal — set by Run on the first
	// encrypted-chunk read.
	chainCEK []byte

	// segCodec is the codec recorded for the segment being restored on
	// the single-manifest path (the root segment for a public restore;
	// the specific segment ChainRestore is applying when it re-enters
	// with SkipChainDispatch). Recorded, never sniffed (ADR-0046 §5).
	// Set by Run / by ChainRestore.applyFull.
	segCodec blobcodec.Codec

	// growGate is the run's shared coordinated cold-copy grow-window pause
	// (ADR-0110), constructed unconditionally in Run and applied to EVERY
	// restore writer via [openTargetRowWriter] (the single construction
	// path). Without it, a concurrent restore (ADR-0112 table×chunk
	// fan-out) hammer-retries a storage-growing target independently per
	// worker and can outrun the target's replication during a grow/
	// reparent — the silent under-copy found on the live PS-10 A/B (Track
	// C, 2026-06-23). With the gate, the first classified grow-transient
	// on any worker quiesces ALL workers together so writes don't outrun
	// replication across the reparent, matching the migrate cold-copy that
	// is byte-perfect through grows. Signal-driven (recovered=nil) — the
	// restore path wires no telemetry provider, so it relies on the
	// universal signal floor, exactly as the migrate cold-copy does.
	growGate ir.GrowGate

	// reparentTracker collects the set of tables a writer reported as
	// reparent-touched (ADR-0113). The grow-gate calms the target but
	// cannot recover rows a PlanetScale storage-grow reparent dropped
	// BEFORE the first transient was seen; after the bulk copy the restore
	// re-derives exactly these tables from their immutable chunks (TRUNCATE
	// + serial redo, or idempotent re-apply for a chain segment) so each
	// matches the manifest regardless of what the reparent dropped. nil ⇒
	// no tracking (direct unit-test callers that bypass Run).
	reparentTracker *migcore.ReparentTracker

	// manifest is the manifest being restored, kept for the chunk-read
	// integrity layer (ADR-0152): its recorded FormatVersion + identity
	// derive each chunk's expected GCM position binding
	// ([irbackup.ChunkAAD]). Set by Run after the manifest read.
	manifest *irbackup.Manifest

	// chunkColumns maps manifestTableKey → the SOURCE schema's
	// non-generated column names — exactly the header column list the
	// backup writer pinned into every chunk. streamChunkRows compares
	// each chunk's header against it (set equality) so a chunk written
	// against a different schema version (renamed / added / removed
	// column) refuses loudly instead of mis-keying rows — the check the
	// chunk-header doc promised and, pre-ADR-0152, nothing performed
	// (audit N-8 item 3). Built from the PRE-retarget manifest schema:
	// the writer's column list is source-shaped, and cross-engine
	// retarget preserves column names but not necessarily other
	// attributes. Set by Run after the manifest read.
	chunkColumns map[string][]string
}

// reparentMark returns the observer callback to wire onto each writer, or
// nil when no tracker is constructed (so migcore.ApplyReparentObserver no-ops).
func (r *Restore) reparentMark() func(string) {
	if r.reparentTracker == nil {
		return nil
	}
	return r.reparentTracker.Mark
}

// lineageNeedsWalk reports whether the store's lineage requires the
// lineage-walk restore path: more than one segment, or a single
// segment carrying incrementals. A one-segment-no-incrementals lineage
// (== a pre-ADR single full) returns false so the single-manifest path
// handles it with byte-identical behaviour.
func (r *Restore) lineageNeedsWalk(ctx context.Context) (bool, error) {
	cat, err := lineage.ResolveLineage(ctx, r.Store)
	if err != nil {
		return false, err
	}
	if len(cat.Segments) > 1 {
		return true, nil
	}
	return len(cat.Segments[0].Incrementals) > 0, nil
}

// rootSegmentCodec returns the codec recorded for segment 0 (the root
// segment). Absent lineage → gzip (pre-ADR default). The codec is
// recorded, NEVER inferred from chunk bytes.
func (r *Restore) rootSegmentCodec(ctx context.Context) (blobcodec.Codec, error) {
	cat, err := lineage.ResolveLineage(ctx, r.Store)
	if err != nil {
		return "", err
	}
	seg := &cat.Segments[0]
	if err := blobcodec.ValidateRecordedCodec(seg.Codec); err != nil {
		return "", err
	}
	return seg.CodecOrDefault(), nil
}

// Run executes the restore. Returns nil on success; a wrapped error
// pointing at the failed phase otherwise.
//
// Phase 3 (v0.17.0+): when the store contains incremental manifests
// in addition to the full, Run delegates to [ChainRestore] which
// walks the chain in order. The single-manifest path remains
// unchanged for backups produced by `sluice backup full` alone.
func (r *Restore) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	// 0. Detect lineage shape (ADR-0046). A multi-segment lineage, OR
	//    a one-segment lineage with incrementals, dispatches to the
	//    lineage-walk restore. A one-segment-no-incrementals lineage
	//    (== a pre-ADR single full) takes the single-manifest path
	//    below — byte-identical behaviour to before.
	//
	// SkipChainDispatch is set internally by ChainRestore when it
	// re-enters the single-manifest path to apply ONE segment's full
	// alone; without it the two would mutually recurse.
	if !r.SkipChainDispatch {
		multi, err := r.lineageNeedsWalk(ctx)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: detect lineage: %w", err))
		}
		if multi {
			chain := &ChainRestore{
				Target:           r.Target,
				TargetDSN:        r.TargetDSN,
				Store:            r.Store,
				Filter:           r.Filter,
				MaxBufferBytes:   r.MaxBufferBytes,
				TableParallelism: r.TableParallelism,
				ChunkParallelism: r.ChunkParallelism,
				Summary:          r.Summary,
				ApplyConcurrency: r.ApplyConcurrency,
				Envelope:         r.Envelope,
				VerifyKey:        r.VerifyKey,
				RequireSignature: r.RequireSignature,
				TargetSchema:     r.TargetSchema,
			}
			return chain.Run(ctx)
		}
	}

	// 1. Read manifest. Single-manifest path: this is the root
	//    segment's full at the conventional lineage.ManifestFileName.
	manifest, err := lineage.ReadManifest(ctx, r.Store)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: %w", err))
	}
	// The root segment's recorded codec governs the full's chunks.
	// Recorded, never sniffed (ADR-0046). Absent lineage → gzip.
	if r.segCodec == "" {
		r.segCodec, err = r.rootSegmentCodec(ctx)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: %w", err))
		}
	}
	slog.InfoContext(
		ctx, "restore: loaded manifest",
		slog.Int("format_version", manifest.FormatVersion),
		slog.String("source_engine", manifest.SourceEngine),
		slog.String("target_engine", r.Target.Name()),
		slog.Int("tables", len(manifest.Tables)),
		slog.Time("created_at", manifest.CreatedAt),
	)

	if manifest.Schema == nil {
		return errors.New("restore: manifest carries no schema")
	}

	// 1.4. ADR-0047 verbatim-extension restore-time engine gate. A
	//      backup carrying verbatim (uncatalogued) PG extension-typed
	//      columns is PG-restore-only; restoring it to a non-PG target
	//      is a LOUD refusal before any data moves (never a silent
	//      drop/mangle). The single-manifest path gates on the
	//      manifest schema directly — the same schema the lineage
	//      marker is derived from, so the two restore paths agree.
	if err := refuseVerbatimManifestRestoreToNonPG(manifest.Schema, r.Target); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}

	// 1.45. Cross-engine supportability gate (Bug 134). The chain
	//       restore path has gated PG-native constructs (EXCLUDE
	//       constraints, extension opclasses, PostGIS metadata, …)
	//       since Phase 5, but this single-manifest branch never
	//       called the gate — so a full-only PG backup restored to a
	//       MySQL-family target exited 0 with an EXCLUDE constraint
	//       SILENTLY downgraded to a plain non-unique KEY (semantic-
	//       invariant loss; every row still arrives, which is exactly
	//       why nothing else caught it). Same recorded-SourceEngine vs
	//       target-name dispatch as chain_restore.go step 2, gated
	//       BEFORE the retarget so the refusal sees the source-true
	//       schema, and BEFORE the table filter for path-consistency
	//       with the chain (which also gates its root's full schema).
	if manifest.SourceEngine != "" && manifest.SourceEngine != r.Target.Name() {
		if err := migcore.CheckCrossEngineSupportable(
			manifest.Schema,
			manifest.SourceEngine, r.Target.Name(),
			fmt.Sprintf("restore: full %s", lineage.ManifestBackupID(manifest)),
		); err != nil {
			return err
		}
		slog.InfoContext(
			ctx, "restore: cross-engine mode",
			slog.String("source_engine", manifest.SourceEngine),
			slog.String("target_engine", r.Target.Name()),
		)
	}

	// 1.5. Encryption pre-flight. If the chain root manifest carries
	// [irbackup.ChainEncryption], the operator MUST have supplied an
	// envelope that can unwrap the chain's CEK. A missing envelope
	// against an encrypted chain refuses up-front so no partial data
	// lands on the target.
	if err := r.preflightEncryption(manifest); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: %w", err))
	}

	// 1.6. ADR-0154 signature verification for the single-manifest path
	// (a lone signed full, sequence 0). When SkipChainDispatch is set,
	// [ChainRestore] already verified every segment's signatures at its
	// walked position — re-verifying here would use the wrong sequence
	// for a later segment's full, so skip.
	if !r.SkipChainDispatch {
		if err := verifyManifestSignaturePolicy(ctx, r.Store, lineage.ManifestFileName, manifest, 0, verifyMaterial{env: r.Envelope, verifyPub: r.VerifyKey}, r.RequireSignature); err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: %w", err))
		}
	}

	// 2. Filter tables — both at the schema level (so unwanted
	//    tables never get created) and at the manifest-table level
	//    (so unwanted chunks never get streamed).
	if err := migcore.ApplyTableFilter(ctx, manifest.Schema, r.Filter); err != nil {
		return err
	}
	manifest.Tables = filterManifestTables(manifest.Tables, r.Filter)

	// 2.5. Chunk-read integrity inputs (ADR-0152): the manifest whose
	//      recorded FormatVersion + identity derive each chunk's GCM
	//      position binding, and the SOURCE-schema column sets each
	//      chunk's header is validated against. Captured BEFORE the
	//      retarget — the backup writer's column list is source-shaped.
	r.manifest = manifest
	r.chunkColumns = sourceChunkColumns(manifest.Schema)

	// 3. Cross-engine retarget (identity for same-engine).
	schema := translate.RetargetForEngine(manifest.Schema, manifest.SourceEngine, r.Target.Name())

	// 4. Open target writers.
	if err := migcore.ValidateTargetSchema(r.Target, r.TargetSchema); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: %w", err))
	}
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: open target schema writer: %w", err))
	}
	migcore.ApplyTargetSchema(sw, r.TargetSchema)
	defer migcore.CloseIf(sw)

	// Construct the run's shared coordinated grow-gate (ADR-0110) BEFORE
	// opening any row writer, so every writer openTargetRowWriter hands out
	// — the primary here, plus the cross-table-pool and within-table
	// chunk-worker writers (ADR-0112) — takes its grow-aware flush path.
	// Signal-driven (recovered=nil), matching the migrate cold-copy: the
	// first classified grow-transient on any worker quiesces ALL workers so
	// a concurrent restore can't outrun the target's replication across a
	// storage-grow reparent (the Track-C live silent-under-copy fix).
	r.growGate = migcore.GrowGateOrNil(migcore.NewGrowGate(ctx, nil))
	// ADR-0113: the reparent tracker collects tables that hit a grow/reparent
	// transient during apply, so the reconciliation phase below can re-derive
	// exactly those from their chunks (recovering rows the reparent dropped
	// that the gate cannot). Constructed before any writer opens so every
	// writer receives the observer through openTargetRowWriter.
	r.reparentTracker = migcore.NewReparentTracker()

	rw, err := r.openTargetRowWriter(ctx)
	if err != nil {
		return err
	}
	defer migcore.CloseIf(rw)

	// 5. Phase 1: tables. Skipped in DataOnly mode (a later
	//    rotation-segment full — schema already established by
	//    segment 0; CreateTables IF NOT EXISTS would be a no-op
	//    anyway, but we skip the whole schema surface for clarity).
	if !r.DataOnly {
		if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("restore: create tables: %w", err))
		}
		slog.InfoContext(ctx, "restore: tables created", slog.Int("count", len(schema.Tables)))
	}

	// 6. Phase 2: bulk-copy from chunks, fanned across a bounded
	//    cross-table writer pool (ADR-0084; tableParallelism=1 runs the
	//    same pool serially — the pre-ADR behaviour). DataOnly uses an
	//    idempotent upsert so re-applying a later segment's snapshot
	//    over the prior segment's restored state converges (no
	//    PK-collision); the writer selection is per-worker.
	tablesByName := indexManifestTables(manifest.Tables)
	tasks := make([]restoreTableTask, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		entry, ok := tablesByName[key]
		if !ok {
			slog.InfoContext(ctx, "restore: table not in manifest; skipping bulk-copy",
				slog.String("table", table.Name))
			continue
		}
		tasks = append(tasks, restoreTableTask{table: table, entry: entry})
	}
	tableParallelism, chunkParallelism, err := r.resolveRestoreParallelism(ctx, len(tasks))
	if err != nil {
		return err
	}
	// A dedicated-writer factory is needed when EITHER axis fans out:
	// the cross-table pool opens one per concurrent peer table, and a
	// within-table chunk fan-out opens one per chunk-group worker
	// (ADR-0112). Both come through openTargetRowWriter — the single
	// construction path — so buffer cap + target-schema routing can't
	// drift across either axis.
	var factory restoreWriterFactory
	if tableParallelism > 1 || chunkParallelism > 1 {
		factory = r.openTargetRowWriter
	}
	if err := r.runRestoreTablePool(ctx, tasks, rw, factory, tableParallelism, chunkParallelism); err != nil {
		return err
	}

	// ADR-0113: reconcile any reparent-touched table. The grow-gate calms a
	// storage-growing target but cannot recover rows its reparent dropped
	// before the first transient was seen (PlanetScale promotes a new primary
	// behind the async-acked window). Re-derive each touched table from its
	// immutable chunks so it exactly matches the manifest. No-op when no
	// reparent occurred.
	if err := r.reconcileReparentTouched(ctx, rw, tasks); err != nil {
		return migcore.WrapWithHint(migcore.PhaseBulkCopy, err)
	}

	if r.DataOnly {
		slog.InfoContext(ctx, "restore: data-only segment refresh complete",
			slog.Int("tables", len(schema.Tables)))
		return nil
	}

	// 7. Phase 3: identity-sequence sync. Each post-copy DDL phase is
	// wrapped in the ADR-0114 reparent retry so a PlanetScale storage-grow
	// reparent landing on the DDL phase (the index build is the textbook
	// case — Track-C live finding) rides out instead of aborting the whole
	// restore after a byte-perfect data copy. All four phases are idempotent
	// on re-run (see migcore.RunDDLPhaseWithReparentRetry's header).
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
		return sw.SyncIdentitySequences(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("restore: sync identity sequences: %w", err))
	}

	// 8. Phase 4: indexes.
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "indexes", sw, func(ctx context.Context) error {
		return sw.CreateIndexes(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseIndexes, fmt.Errorf("restore: create indexes: %w", err))
	}

	// 9. Phase 5: constraints.
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "constraints", sw, func(ctx context.Context) error {
		return sw.CreateConstraints(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConstraints, fmt.Errorf("restore: create constraints: %w", err))
	}

	// 10. Phase 6: views. migcore.RunViewsPhase wraps its own per-view DDL in the
	// reparent retry (ADR-0114) atop its dependency-pass retry.
	if err := migcore.RunViewsPhase(ctx, schema, sw); err != nil {
		return migcore.WrapWithHint(migcore.PhaseViews, err)
	}

	slog.InfoContext(ctx, "restore complete", slog.Int("tables", len(schema.Tables)))
	return nil
}

// validate sanity-checks required fields.
func (r *Restore) validate() error {
	switch {
	case r.Target == nil:
		return errors.New("restore: Target engine is nil")
	case r.TargetDSN == "":
		return errors.New("restore: TargetDSN is empty")
	case r.Store == nil:
		return errors.New("restore: Store is nil")
	}
	return nil
}

// openTargetRowWriter opens one fully-configured row writer against
// the target: OpenRowWriter + the buffer cap + the target-schema
// routing. The SINGLE construction point for restore row writers —
// Run's primary writer and the pool's dedicated per-table writers
// (ADR-0084) both come through here, so the two setups can never
// drift.
func (r *Restore) openTargetRowWriter(ctx context.Context) (ir.RowWriter, error) {
	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("restore: open target row writer: %w", err))
	}
	migcore.ApplyMaxBufferBytes(rw, r.MaxBufferBytes)
	migcore.ApplyTargetSchema(rw, r.TargetSchema)
	// Wire the run's shared grow-gate (ADR-0110) onto every writer so the
	// MySQL writer's flushWithReparentRetry awaits/trips it — coordinating
	// all concurrent restore workers through a storage-grow reparent instead
	// of independently hammering (the Track-C silent-under-copy fix). nil
	// gate (direct unit-test callers that don't go through Run) is a no-op.
	migcore.ApplyGrowGate(rw, r.growGate)
	// ADR-0113: wire the run's reparent observer so this writer reports any
	// table it sees hit a grow/reparent transient — the reconciliation
	// phase re-derives those tables. nil observer (no tracker / non-restore
	// callers) is a no-op.
	migcore.ApplyReparentObserver(rw, r.reparentMark())
	return rw, nil
}

// reconcileReparentTouched re-derives every reparent-touched table from its
// chunks (ADR-0113) so it exactly matches the manifest, recovering rows a
// storage-grow reparent dropped that the grow-gate could not. Each redo runs
// through the SAME reparent-retry + observer, so a redo that itself hits a
// reparent re-marks its table for the next round; the loop ends when a full
// pass observes no new touches (the sound proxy for "no reparent ⇒ no loss",
// since a reparent is the only loss vector). No-op when no reparent occurred.
func (r *Restore) reconcileReparentTouched(ctx context.Context, rw ir.RowWriter, tasks []restoreTableTask) error {
	if r.reparentTracker == nil {
		return nil
	}
	byName := make(map[string]restoreTableTask, len(tasks))
	for _, t := range tasks {
		byName[t.table.Name] = t
	}
	for round := 1; ; round++ {
		touched := r.reparentTracker.Drain()
		if len(touched) == 0 {
			return nil
		}
		if round > migcore.ReconcileMaxRounds {
			return fmt.Errorf(
				"restore: reparent reconciliation did not converge after %d rounds — the target keeps reparenting during the serial redo (still-touched: %v); re-run with --bulk-parallelism 1 or restore into a pre-sized / Metal target",
				migcore.ReconcileMaxRounds, touched,
			)
		}
		slog.WarnContext(
			ctx, "restore: reconciling reparent-touched tables (ADR-0113) — re-deriving each from its chunks to recover any rows the target's storage-grow reparent dropped",
			slog.Int("round", round),
			slog.Int("tables", len(touched)),
			slog.Any("table_names", touched),
		)
		for _, name := range touched {
			task, ok := byName[name]
			if !ok {
				// Touched a table outside this run's task set (e.g. filtered
				// out after the mark) — nothing to re-derive.
				continue
			}
			if err := r.reapplyTableForReconcile(ctx, rw, task.table, task.entry); err != nil {
				return fmt.Errorf("reconcile table %q: %w", name, err)
			}
		}
	}
}

// reapplyTableForReconcile re-derives one table from its chunks (ADR-0113).
// Non-DataOnly cold restore: TRUNCATE then redo SERIALLY (chunkParallelism=1
// — the pace that never outruns replication) into the now-empty table; no
// primary-key/UPSERT needed, and indexes/constraints are later phases so the
// TRUNCATE is clean and cheap. DataOnly (chain rotation segment): skip the
// truncate (it would wipe a prior segment) and re-apply idempotently — the
// idempotent writer restoreTable selects converges. The serial redo reuses
// the supplied primary writer (which carries the reparent observer), so a
// reparent during the redo re-marks the table for another round.
func (r *Restore) reapplyTableForReconcile(ctx context.Context, rw ir.RowWriter, table *ir.Table, entry *irbackup.TableManifest) error {
	if !r.DataOnly {
		truncator, ok := rw.(ir.TableTruncator)
		if !ok {
			return fmt.Errorf(
				"target engine %q cannot TRUNCATE for reconciliation of %q; re-run with --bulk-parallelism 1",
				r.Target.Name(), table.Name,
			)
		}
		if err := truncator.TruncateTable(ctx, table); err != nil {
			return fmt.Errorf("truncate before redo: %w", err)
		}
	}
	return r.restoreTable(ctx, rw, table, entry, 1, nil)
}

// restoreTable bulk-copies a table's chunks into the target, verifying
// each chunk's SHA-256 along the way.
//
// When the resolved within-table parallelism P > 1 AND the table has
// >= 2 chunks, the chunk list is partitioned across P workers (ADR-0112,
// the within-table axis): each worker acquires its OWN row-writer
// connection (worker 0 reuses the supplied primary; peers open dedicated
// writers via factory), streams its disjoint, contiguous subset of the
// table's chunks through ONE channel into ONE WriteRows call (so
// per-worker batch continuity is preserved — batching does not reset per
// chunk), and returns its rows-applied count. The orchestrator sums the
// per-worker counts for the layer-2 row-count check, so the manifest
// cross-check is exactly as strong as the serial path. Snapshot chunks
// are a disjoint partition of the table's rows, so parallel INSERT
// cannot collide on a PK on a cold target.
//
// P <= 1, or a single-chunk table, runs the original single-stream path
// through the SAME worker code (one group covering every chunk) with a
// loud INFO naming why the within-table fan-out didn't engage when it
// was requested (ADR-0079: never a silent no-op).
//
// Per-chunk SHA-256 verification is the load-bearing layer-1 check of
// the proto-ADR's "100% confidence" story, unchanged and still
// per-chunk. A mismatch is a hard failure — silent corruption is not
// acceptable.
func (r *Restore) restoreTable(
	ctx context.Context,
	rw ir.RowWriter,
	table *ir.Table,
	entry *irbackup.TableManifest,
	chunkParallelism int,
	factory restoreWriterFactory,
) error {
	if len(entry.Chunks) == 0 {
		slog.InfoContext(ctx, "restore: empty table; no chunks to apply",
			slog.String("table", table.Name))
		r.Summary.RecordTableRows(table.Schema, table.Name, 0)
		return nil
	}

	groups := partitionChunks(entry.Chunks, r.resolveTableChunkParallelism(ctx, table, len(entry.Chunks), chunkParallelism))

	// errgroup-derived ctx so the first worker's failure cancels its
	// siblings' producers (the Bug-40b cancel-on-writer-error shape,
	// replicated per worker — without it a peer producer blocked on
	// `rowCh <- row` would hang). The serial path (one group) is the
	// degenerate case of this same code.
	wg, wctx := errgroup.WithContext(ctx)
	var rowsApplied atomic.Int64
	for groupIdx, group := range groups {
		groupIdx, group := groupIdx, group
		wg.Go(func() error {
			// Worker 0 reuses the supplied primary writer; peers open
			// their own dedicated connection via factory — the SAME
			// construction path, so buffer cap + target-schema routing
			// can't drift across the within-table axis.
			worker := rw
			if groupIdx > 0 {
				if factory == nil {
					return errRestoreChunkPoolNoFactory
				}
				w, err := factory(wctx)
				if err != nil {
					return err
				}
				defer migcore.CloseIf(w)
				worker = w
			}
			n, err := r.restoreChunkGroup(wctx, worker, table, group)
			if err != nil {
				return err
			}
			rowsApplied.Add(n)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}

	// Layer-2 row-count check: the EXACT sum of ACTUALLY-decoded rows
	// across workers compared to the manifest's RowCount — byte-for-byte
	// as strong as the serial path. A mismatch stays a HARD failure (no
	// silent corruption).
	if entry.RowCount > 0 && rowsApplied.Load() != entry.RowCount {
		return fmt.Errorf("layer-2 row-count mismatch on table %q: manifest says %d, streamed %d",
			table.Name, entry.RowCount, rowsApplied.Load())
	}
	slog.InfoContext(
		ctx, "restore: table complete",
		slog.String("table", table.Name),
		slog.Int64("rows", entry.RowCount),
		slog.Int("chunks", len(entry.Chunks)),
		slog.Int("chunk_workers", len(groups)),
	)
	// Envelope bookkeeping: same number the line above announces.
	r.Summary.RecordTableRows(table.Schema, table.Name, entry.RowCount)
	return nil
}

// restoreChunkGroup streams one worker's contiguous subset of a table's
// chunks through ONE channel into ONE WriteRows call, returning the
// rows applied by this worker (for the orchestrator's layer-2 sum).
//
// This is the per-worker body of the ADR-0112 within-table fan-out; the
// serial path is the degenerate one-group case. Each worker owns its
// own writer (the caller decides reuse-primary vs dedicated), so the
// channel + producer goroutine + Bug-40b cancel below are entirely
// worker-local — no cross-worker channel sharing.
func (r *Restore) restoreChunkGroup(
	ctx context.Context,
	rw ir.RowWriter,
	table *ir.Table,
	group []*irbackup.ChunkInfo,
) (int64, error) {
	// Bug 40b fix (v0.20.1): wrap the producer goroutine in a derived
	// context so a writer-side failure can cancel the producer.
	// Pre-fix: the producer pushed rows into an unbuffered channel
	// that the writer stopped draining the moment WriteRows returned
	// an error; the producer then blocked forever on `out <- row`,
	// and `<-errCh` deadlocked. Net effect: a target-side
	// "column does not exist" turned into a silent hang with idle
	// PG connections in ClientRead state and operators having to
	// Stop-Process to recover. The streamCtx + streamCancel pattern
	// below mirrors the one [ChainRestore.applyIncremental] uses for
	// change-chunk streaming.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	// Bounded buffer (see [migcore.RowChanBuffer]) so chunk decode and target
	// write overlap instead of rendezvous-alternating — the restore
	// analog of migrate's bulk-copy hop discipline (perf-parity matrix
	// gap 2). The Bug-40b cancel path below is unaffected: a writer
	// error still cancels the producer, which unblocks from a full
	// buffer via streamChunkRows' <-ctx.Done() arm exactly as it did
	// from the unbuffered rendezvous.
	rowCh := make(chan ir.Row, migcore.RowChanBuffer)
	// The producer reports either an error OR the count of rows it
	// actually decoded+streamed for this group (the ACTUAL count, not
	// the manifest sum — so the orchestrator's layer-2 cross-check is
	// exactly as strong as the serial path even for chunks whose
	// manifest RowCount is unrecorded). Buffered-1 so the producer never
	// blocks reporting.
	type groupResult struct {
		rows int64
		err  error
	}
	resCh := make(chan groupResult, 1)

	go func() {
		defer close(rowCh)
		var rowsApplied int64
		for chunkIdx, chunk := range group {
			// streamChunkRows verifies each chunk's SHA-256 AND
			// cross-checks the decoded count against the chunk's manifest
			// RowCount (the per-chunk layer-2 check); a mismatch on either
			// is a hard failure surfaced here.
			chunkRows, err := r.streamChunkRows(streamCtx, table, chunk, rowCh)
			if err != nil {
				resCh <- groupResult{err: fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)}
				return
			}
			rowsApplied += chunkRows
			slog.DebugContext(
				ctx, "restore: chunk verified and streamed",
				slog.String("table", table.Name),
				slog.Int("chunk", chunkIdx),
				slog.Int64("rows", chunkRows),
			)
		}
		resCh <- groupResult{rows: rowsApplied}
	}()

	// DataOnly (later rotation-segment full): use the idempotent
	// upsert writer when the engine exposes it so re-applying the
	// snapshot over the prior segment's restored rows converges
	// (ON CONFLICT / ON DUPLICATE KEY UPDATE). Each worker type-asserts
	// its OWN writer, so the dispatch decision is independent and
	// idempotent upsert is order- and concurrency-independent for the
	// disjoint rows of a snapshot. Engines without the surface, or
	// no-PK tables, fall back to plain WriteRows — the caller's lineage
	// invariant (S_n >= prior end) means the rows are at-or-ahead, so a
	// plain insert only collides on a PK the upsert would have updated
	// to the same value; the idempotent path is the correct one and
	// shipping engines (PG, MySQL) implement it.
	writeFn := rw.WriteRows
	if r.DataOnly {
		if iw, ok := rw.(ir.IdempotentRowWriter); ok {
			writeFn = iw.WriteRowsIdempotent
		}
	}
	if err := writeFn(ctx, table, rowCh); err != nil {
		// Bug 40b: cancel the producer's context so a goroutine
		// blocked on `rowCh <- row` unblocks via the streamChunkRows
		// `<-ctx.Done()` arm. Without this, `<-resCh` below would
		// deadlock — the silent-hang shape from Bug 40.
		slog.ErrorContext(
			ctx, "restore: write rows failed; cancelling chunk producer",
			slog.String("table", table.Name),
			slog.String("err", err.Error()),
		)
		streamCancel()
		<-resCh
		return 0, fmt.Errorf("write rows for table %q: %w", table.Name, err)
	}
	res := <-resCh
	if res.err != nil {
		return 0, res.err
	}
	return res.rows, nil
}

// errRestoreChunkPoolNoFactory is the loud precondition guard for the
// within-table chunk fan-out: a peer worker (groupIdx > 0) needs a
// dedicated writer, which the orchestrator only configures together with
// a writer factory. Reaching it with a nil factory is a programming
// error, surfaced rather than silently deref'd. Mirrors
// [errRestorePoolNoFactory].
var errRestoreChunkPoolNoFactory = errors.New("pipeline: restore chunk fan-out: dedicated writer needed but no writer factory configured")

// partitionChunks splits chunks into p contiguous, disjoint groups so
// each within-table worker streams ONE run of chunks through ONE
// WriteRows call (preserving per-worker batch continuity). The split is
// near-even: the first (len%p) groups get one extra chunk. Ordering
// within a group is the manifest's chunk order, unchanged. p <= 1, or
// fewer chunks than p, collapses to the appropriate group count (never
// an empty group). Returns at least one group whenever chunks is
// non-empty.
func partitionChunks(chunks []*irbackup.ChunkInfo, p int) [][]*irbackup.ChunkInfo {
	if p < 1 {
		p = 1
	}
	if p > len(chunks) {
		p = len(chunks)
	}
	if p <= 1 {
		return [][]*irbackup.ChunkInfo{chunks}
	}
	groups := make([][]*irbackup.ChunkInfo, 0, p)
	base := len(chunks) / p
	rem := len(chunks) % p
	start := 0
	for i := 0; i < p; i++ {
		size := base
		if i < rem {
			size++
		}
		groups = append(groups, chunks[start:start+size])
		start += size
	}
	return groups
}

// resolveTableChunkParallelism decides the effective within-table
// worker count for ONE table: the already-budget-resolved
// chunkParallelism, but collapsed to serial (1) when there are fewer
// than 2 chunks to spread. When the operator requested the fan-out
// (chunkParallelism > 1) but a table can't use it, that's logged loudly
// (ADR-0112 / ADR-0079: never a silent no-op) naming the reason —
// mirroring resolveRestoreTableParallelism's serialReason pattern.
func (r *Restore) resolveTableChunkParallelism(ctx context.Context, table *ir.Table, chunkCount, chunkParallelism int) int {
	if chunkParallelism <= 1 {
		return 1
	}
	if chunkCount < 2 {
		slog.InfoContext(
			ctx, "restore: within-table chunk parallel apply not engaged; applying chunks serially",
			slog.String("table", table.Name),
			slog.String("reason", "table has fewer than 2 chunks"),
			slog.Int("requested_chunk_parallelism", chunkParallelism),
			slog.Int("chunks", chunkCount),
		)
		return 1
	}
	effective := chunkParallelism
	if effective > chunkCount {
		effective = chunkCount // never spawn more workers than chunks
	}
	slog.InfoContext(
		ctx, "restore: within-table chunk parallel apply engaged (ADR-0112)",
		slog.String("table", table.Name),
		slog.Int("chunk_parallelism", effective),
		slog.Int("chunks", chunkCount),
	)
	return effective
}

// streamChunkRows opens chunk in the store, validates its header
// against table's source column set, decodes every row, sends each
// into rowCh, and verifies the SHA-256 on close. Returns the row count
// read from this chunk, which the caller compares against the manifest
// entry's RowCount for layer-2 verification.
func (r *Restore) streamChunkRows(
	ctx context.Context,
	table *ir.Table,
	chunk *irbackup.ChunkInfo,
	rowCh chan<- ir.Row,
) (int64, error) {
	src, err := blobcodec.FetchChunkVerified(ctx, r.Store, chunk.File, chunk.SHA256)
	if err != nil {
		return 0, fmt.Errorf("open chunk: %w", err)
	}
	cek, err := r.chunkCEK(chunk)
	if err != nil {
		_ = src.Close()
		return 0, fmt.Errorf("resolve chunk cek: %w", err)
	}
	cr, err := blobcodec.NewChunkReader(src, chunk.SHA256, cek, r.segCodec, irbackup.ChunkAADFor(r.manifest, chunk, table.Schema, table.Name))
	if err != nil {
		return 0, fmt.Errorf("open chunk reader: %w", err)
	}
	// Chunk-header ↔ schema cross-check (ADR-0152, audit N-8 item 3):
	// the header pins the column list the chunk was written against; a
	// mismatch against the manifest schema's column set means the chunk
	// does not belong to this schema version — rows would silently
	// mis-key — so refuse loudly before emitting any.
	if err := r.checkChunkHeaderColumns(table, chunk, cr.Header().Columns); err != nil {
		_ = cr.Close()
		return 0, err
	}

	var rows int64
	for {
		row, err := cr.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cr.Close()
			return rows, fmt.Errorf("read row: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return rows, ctx.Err()
		case rowCh <- row:
			rows++
		}
	}
	if err := cr.Close(); err != nil {
		// SHA-256 mismatch surfaces here as a wrapped
		// ErrChunkHashMismatch; loud-failure tenet means we surface
		// it directly rather than continuing.
		return rows, err
	}
	if chunk.RowCount > 0 && rows != chunk.RowCount {
		return rows, fmt.Errorf("layer-2 chunk row-count mismatch on %s: manifest says %d, decoded %d",
			chunk.File, chunk.RowCount, rows)
	}
	return rows, nil
}

// sourceChunkColumns maps every manifest-schema table to its
// non-generated column-name list — the exact set [blobcodec.ChunkWriter]
// pins into each chunk's header (generated columns are never backed
// up). Keys are [manifestTableKey]-shaped, matching how restore pairs
// schema tables with their manifest entries.
func sourceChunkColumns(schema *ir.Schema) map[string][]string {
	out := make(map[string][]string, len(schema.Tables))
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		cols := nonGeneratedTableColumns(t)
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = c.Name
		}
		out[manifestTableKey(t.Schema, t.Name)] = names
	}
	return out
}

// checkChunkHeaderColumns compares a chunk's header column list
// against the manifest schema's expected set for table, as SETS —
// order-insensitive, because the header records declaration order
// while the guarantee that matters is "same columns". Any missing or
// unexpected column is a loud refusal naming both sides: the chunk was
// written against a DIFFERENT schema version than the manifest carries
// (a renamed column being the canonical case), and streaming it would
// silently mis-key rows. ADR-0152 (audit N-8 item 3) — this is the
// enforcement the chunk-header format doc had promised since Phase 1.
//
// Runs for every chunk, plaintext and encrypted alike (the header is
// inside the codec stream, so it is covered by GCM on encrypted
// chunks and only by SHA-256 on plaintext ones — either way the
// cross-check against the manifest schema is what catches the
// mis-assembled pairing).
func (r *Restore) checkChunkHeaderColumns(table *ir.Table, chunk *irbackup.ChunkInfo, headerCols []string) error {
	key := manifestTableKey(table.Schema, table.Name)
	expected, ok := r.chunkColumns[key]
	if !ok {
		// Programming error: every restored table came out of the same
		// manifest schema the map was built from. Loud, not skipped.
		return fmt.Errorf("restore: internal: no source column set for table %q (chunk %s)", key, chunk.File)
	}
	missing, extra := diffColumnSets(expected, headerCols)
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	return fmt.Errorf("restore: chunk %s does not match table %q's schema: header is missing columns %v and carries unexpected columns %v — the chunk was written against a different schema version than this manifest records (renamed/altered column, or a mis-assembled/tampered backup store); refusing before any row lands",
		chunk.File, key, missing, extra)
}

// diffColumnSets returns expected-minus-got (missing) and
// got-minus-expected (extra), each in first-seen order.
func diffColumnSets(expected, got []string) (missing, extra []string) {
	gotSet := make(map[string]struct{}, len(got))
	for _, c := range got {
		gotSet[c] = struct{}{}
	}
	expSet := make(map[string]struct{}, len(expected))
	for _, c := range expected {
		expSet[c] = struct{}{}
		if _, ok := gotSet[c]; !ok {
			missing = append(missing, c)
		}
	}
	for _, c := range got {
		if _, ok := expSet[c]; !ok {
			extra = append(extra, c)
		}
	}
	return missing, extra
}

// filterManifestTables filters the manifest's table list against the
// supplied filter, mirroring the schema-side filtering. Empty filter
// returns the input unchanged.
func filterManifestTables(in []*irbackup.TableManifest, filter migcore.TableFilter) []*irbackup.TableManifest {
	if filter.IsEmpty() {
		return in
	}
	out := in[:0]
	for _, t := range in {
		if t == nil {
			continue
		}
		if filter.Allows(t.Name) {
			out = append(out, t)
		}
	}
	return out
}

// indexManifestTables returns a "schema.name" → entry map. Used by
// [Restore.Run] to look up each schema-table's manifest entry in O(1).
func indexManifestTables(tables []*irbackup.TableManifest) map[string]*irbackup.TableManifest {
	out := make(map[string]*irbackup.TableManifest, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		out[manifestTableKey(t.Schema, t.Name)] = t
	}
	return out
}

func manifestTableKey(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// preflightEncryption inspects the manifest for [irbackup.ChainEncryption]
// and, when present, validates that an envelope is supplied and that
// it can unwrap the chain's CEK. Caches the chain-level CEK on
// r.chainCEK for per-chain mode so subsequent chunk reads pay no
// further unwrap cost.
//
// On a plaintext chain this is a no-op; on an encrypted chain with no
// envelope, it returns an operator-actionable error naming the chain's
// KEKMode and (where relevant) KEKRef so the operator knows what they
// need to supply.
func (r *Restore) preflightEncryption(manifest *irbackup.Manifest) error {
	if manifest == nil || manifest.ChainEncryption == nil {
		return nil
	}
	enc := manifest.ChainEncryption
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
			return errors.New("encrypted chain in per-chain mode but ChainEncryption.WrappedCEK is empty (manifest corrupted?)")
		}
		// ADR-0152 chokepoint: identity-bound unwrap for v5+ manifests,
		// legacy unwrap below; plus the Azure wrap-time key-version
		// retarget (audit N-9).
		cek, err := lineage.UnwrapChainCEK(r.Envelope, enc.WrappedCEK, manifest)
		if err != nil {
			return fmt.Errorf("unwrap chain cek (wrong passphrase / KMS key?): %w", err)
		}
		r.chainCEK = cek
		return nil
	}
	// Per-chunk mode: no chain-level CEK, but the envelope still needs
	// the Azure version retarget before the per-chunk unwraps run.
	lineage.RebindEnvelopeKEK(r.Envelope, manifest)
	return nil
}

// chunkCEK returns the per-chunk CEK for chunk based on the chunk's
// recorded [irbackup.ChunkEncryption]. Per-chain mode returns r.chainCEK;
// per-chunk mode unwraps the chunk's own [irbackup.ChunkEncryption.WrappedCEK]
// via the envelope.
//
// Returns nil for plaintext chunks (Encryption == nil) — caller passes
// nil cek to newChunkReader for the legacy plaintext read path.
func (r *Restore) chunkCEK(chunk *irbackup.ChunkInfo) ([]byte, error) {
	if chunk.Encryption == nil {
		return nil, nil
	}
	// Per-chunk wrap takes precedence: if the chunk carries its own
	// wrapped CEK, unwrap it.
	if len(chunk.Encryption.WrappedCEK) > 0 {
		if r.Envelope == nil {
			return nil, errors.New("per-chunk encrypted chunk encountered without envelope")
		}
		cek, err := r.Envelope.UnwrapCEK(chunk.Encryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap chunk cek: %w", err)
		}
		return cek, nil
	}
	// Per-chain mode: use the cached chain CEK (preflight already
	// unwrapped it).
	if r.chainCEK == nil {
		return nil, errors.New("encrypted chunk encountered but chain CEK is unset (preflight skipped?)")
	}
	return r.chainCEK, nil
}

// VerifyOptions controls [VerifyBackup]'s verification surface. The
// zero value performs only the byte-level SHA-256 check (the historical
// behavior, preserved for backward compatibility).
type VerifyOptions struct {
	// Envelope, when non-nil, enables decrypt-probe verification on
	// every encrypted chunk. Closes Bug 117 (v0.94.1): pre-fix,
	// per-chunk-mode chains accepted a passphrase rotation
	// mid-chain silently — `backup verify` was SHA-only, the
	// later chunks' SHAs still matched (the bytes on disk DID
	// hash correctly), and the divergence only surfaced at restore
	// time as a partial-fail with no rollback. Post-fix, the
	// envelope is used to unwrap each per-chunk WrappedCEK; an
	// unwrap failure (wrong passphrase / wrong KMS key for that
	// chunk) is reported as a verify failure with a clear
	// chunk-naming error so the operator can identify the rotation
	// point before attempting restore.
	//
	// Per-chain mode is also probed (unwrap the chain-level CEK
	// once up-front) so a fully-wrong passphrase fails fast with a
	// single clear error rather than 0 verify failures + a
	// restore-time surprise.
	//
	// Must match the chain root's recorded KEKMode when set; a
	// mismatch returns an irrecoverable error from VerifyBackup.
	Envelope crypto.EnvelopeEncryption

	// VerifyKey, when non-nil, is the asymmetric PUBLIC key (`--verify-key`
	// — Ed25519 / ECDSA / RSA) that verifies an ADR-0154 Phase 2/3 signed
	// chain (Ed25519 or KMS scheme). Required to verify such a chain;
	// orthogonal to Envelope.
	VerifyKey stdcrypto.PublicKey

	// RequireSignature makes an UNVERIFIABLE signed (v6) chain — one with
	// no matching verify key — a verify failure rather than a WARN. An
	// INVALID signature is always a failure regardless. Default false.
	RequireSignature bool
}

// VerifyBackup walks every chunk referenced by the manifest in store,
// rehashes each chunk's bytes, and reports any mismatches. Used by
// `sluice backup verify` for "is my backup still intact?" cron probes
// against archived backups — no decoding of rows, just byte-level
// hash comparison against the manifest.
//
// Returns the total number of chunks, the count of failed chunks, and
// an error. A non-nil error pinpoints an irrecoverable problem
// (manifest unreadable, ctx cancelled); per-chunk hash failures are
// reported via slog at error level AND counted in the failed return,
// so the caller can report "N of M chunks failed verification"
// without needing the full list.
//
// Phase 3 (v0.17.0+): when the store contains a backup chain (a full
// plus one or more incremental manifests), VerifyBackup walks every
// manifest's chunks — both the row chunks of the full and the
// change chunks of each incremental.
//
// VerifyBackup is the historical SHA-only entrypoint preserved for
// backward compatibility (all existing tests rely on this signature).
// Operators wanting the Bug 117 decrypt probe pass a non-nil
// VerifyOptions.Envelope via [VerifyBackupWith].
func VerifyBackup(ctx context.Context, store irbackup.Store) (total, failed int, err error) {
	return VerifyBackupWith(ctx, store, VerifyOptions{})
}

// VerifyBackupWith is the options-bearing form of [VerifyBackup]. When
// opts.Envelope is non-nil, every encrypted chunk's WrappedCEK is
// unwrapped against the supplied envelope so a passphrase rotation
// mid-chain (Bug 117) surfaces at verify-time instead of partial-failing
// the restore.
func VerifyBackupWith(ctx context.Context, store irbackup.Store, opts VerifyOptions) (total, failed int, err error) {
	records, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: %w", err)
	}
	if len(records) == 0 {
		return 0, 0, errors.New("verify: no manifests found in store")
	}
	// Bug 117 closure: when an envelope is supplied AND the chain
	// root records ChainEncryption, validate the chain-level
	// envelope eagerly so the operator gets a single clear "wrong
	// passphrase / wrong KMS key" error up front. For per-chain
	// mode this is also the only decrypt probe per verify run; for
	// per-chunk mode it confirms the operator's envelope is
	// well-formed before per-chunk probes run in the chunk loop.
	if opts.Envelope != nil {
		if rootEnc := records[0].Manifest.ChainEncryption; rootEnc != nil {
			if rootEnc.KEKMode != "" && opts.Envelope.Mode() != rootEnc.KEKMode {
				return 0, 0, fmt.Errorf(
					"verify: envelope mode %q does not match chain's recorded kek_mode %q",
					opts.Envelope.Mode(), rootEnc.KEKMode,
				)
			}
			if len(rootEnc.WrappedCEK) > 0 {
				// ADR-0152 chokepoint: bound unwrap for v5+ roots +
				// the Azure key-version retarget (audit N-9).
				if _, uerr := lineage.UnwrapChainCEK(opts.Envelope, rootEnc.WrappedCEK, records[0].Manifest); uerr != nil {
					return 0, 0, fmt.Errorf(
						"verify: unwrap chain cek (wrong passphrase / KMS key?): %w", uerr,
					)
				}
			} else {
				// Per-chunk mode: retarget the envelope's key version
				// before the per-chunk probes in the loop below.
				lineage.RebindEnvelopeKEK(opts.Envelope, records[0].Manifest)
			}
		}
	}
	// ADR-0154: report + verify the whole-manifest signatures. Reports
	// signed/valid, signed/invalid, or unsigned per manifest; an invalid
	// (or, under RequireSignature, an unverifiable) signature counts as a
	// verify failure so `backup verify` exits non-zero.
	failed += verifyBackupSignatures(ctx, store, records, opts)

	for _, rec := range records {
		manifest := rec.Manifest
		// Chunk files are addressed relative to the segment's store
		// (Dir-prefixed). verify only rehashes bytes — codec is
		// irrelevant for a byte-level SHA check.
		segStore := rec.Segment.Store(store)
		// Row chunks (full backups).
		for _, table := range manifest.Tables {
			for _, chunk := range table.Chunks {
				total++
				if err := verifyChunk(ctx, segStore, chunk); err != nil {
					failed++
					slog.ErrorContext(
						ctx, "verify: chunk failed",
						slog.String("manifest", rec.Path),
						slog.String("table", table.Name),
						slog.String("file", chunk.File),
						slog.String("error", err.Error()),
					)
					continue
				}
				if perr := lineage.ProbeChunkDecrypt(opts.Envelope, chunk); perr != nil {
					failed++
					slog.ErrorContext(
						ctx, "verify: chunk decrypt probe failed",
						slog.String("manifest", rec.Path),
						slog.String("table", table.Name),
						slog.String("file", chunk.File),
						slog.String("error", perr.Error()),
					)
					continue
				}
				slog.DebugContext(
					ctx, "verify: chunk OK",
					slog.String("manifest", rec.Path),
					slog.String("table", table.Name),
					slog.String("file", chunk.File),
				)
			}
		}
		// Change chunks (incremental backups).
		for _, chunk := range manifest.ChangeChunks {
			total++
			if err := verifyChunk(ctx, segStore, chunk); err != nil {
				failed++
				slog.ErrorContext(
					ctx, "verify: change chunk failed",
					slog.String("manifest", rec.Path),
					slog.String("file", chunk.File),
					slog.String("error", err.Error()),
				)
				continue
			}
			if perr := lineage.ProbeChunkDecrypt(opts.Envelope, chunk); perr != nil {
				failed++
				slog.ErrorContext(
					ctx, "verify: change chunk decrypt probe failed",
					slog.String("manifest", rec.Path),
					slog.String("file", chunk.File),
					slog.String("error", perr.Error()),
				)
				continue
			}
			slog.DebugContext(
				ctx, "verify: change chunk OK",
				slog.String("manifest", rec.Path),
				slog.String("file", chunk.File),
			)
		}
	}
	return total, failed, nil
}

// verifyChunk fetches a chunk and recomputes its SHA-256, returning
// nil on match or a wrapped [ErrChunkHashMismatch] on mismatch. It routes
// through [fetchChunkVerified] so a transient short read is retried rather
// than reported as a false mismatch (the same robustness the restore
// chunk-read path gained); a genuine at-rest corruption persists across
// the retries and still surfaces loudly.
func verifyChunk(ctx context.Context, store irbackup.Store, chunk *irbackup.ChunkInfo) error {
	rc, err := blobcodec.FetchChunkVerified(ctx, store, chunk.File, chunk.SHA256)
	if err != nil {
		return err
	}
	return rc.Close()
}
