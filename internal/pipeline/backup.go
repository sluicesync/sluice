// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Backup orchestrator. Phase 1 of the logical-backup feature
// (`docs/dev/design/logical-backups.md`): full snapshot to a
// [ir.BackupStore], one chunk file per N rows per table, plus a
// JSON manifest that lists schema + chunks + per-chunk SHA-256.
//
// Shape mirrors [Migrator]:
//
//   - Construct the value with engine + DSN + filter + chunk size.
//   - Call [Backup.Run] with a context.
//   - Errors are wrapped with phase names so a failed run pinpoints
//     where it failed without parsing strings.
//
// The row sweep fans out across tables through a bounded worker pool
// (ADR-0084, `backup_table_pool.go`) when the source surfaces a
// SHAREABLE exported snapshot (Postgres); MySQL's per-session snapshot
// and the v0.17.x non-snapshot fallback stay sequential, each with a
// loud INFO naming the reason.
//
// # Resumable backups (Phase 2)
//
// A re-run of `sluice backup full` against the same destination is
// resumable: the orchestrator reads any pre-existing manifest at the
// destination and decides whether to start fresh, resume, or refuse.
//
//   - If no manifest exists, the run starts fresh.
//   - If the prior manifest is `partial_state == "complete"` (a
//     successful prior run), the new run refuses unless
//     [Backup.ForceOverwrite] is set. Operators trigger this from the
//     CLI with `--force-overwrite`, mirroring the friction tier of
//     `migrate --reset-target-data` (ADR-0023).
//   - If the prior manifest is `partial_state == "in_progress"`, the
//     new run resumes at TABLE granularity: tables already fully
//     written in the prior run are kept verbatim (their chunks are
//     HEAD-checked for presence); partially-written tables are
//     RE-STREAMED FROM SCRATCH — their prior chunks are never reused,
//     because chunk contents depend on scan order, which is not
//     repeatable across runs (Bug 135; see [Backup.backupTable]).
//     Newly-produced chunks whose bytes match what is already at the
//     same path skip the upload (content-addressed); mismatches
//     overwrite (treating the prior bytes as stale or corrupt).
//
// The manifest is committed to the store after every table completes,
// so a crashed run leaves at most tableParallelism tables' worth of
// in-flight work to redo (one table's, when the sweep runs serial).
//
// # Resume anchor adoption (task #42, ADR-0085)
//
// A resumed run keeps the prior attempt's completed tables VERBATIM —
// those chunks are exact as-of the PRIOR attempt's snapshot anchor. So
// the chain-handoff position the resumed run records (EndPosition)
// must be that prior anchor, never the fresh snapshot's: writes that
// landed on kept tables between the two anchors are in neither the
// kept chunks nor a next-incremental window opened at the new anchor —
// a silent chain gap with exit 0. The in-progress manifest therefore
// carries the anchor from its FIRST write, and a resume ADOPTS it
// (min-anchor rule); the resumed run's fresh snapshot serves only read
// consistency for tables streamed this run. Re-streamed tables overlap
// the chain's replay window — sound for keyed tables (the chain
// appliers are idempotent, ADR-0010), refused loudly for truly keyless
// ones. Pre-fix in-progress manifests (no recorded anchor) re-stream
// every table instead.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// BackupEncryption is the chunk-writer-side encryption configuration
// shared by [Backup], [IncrementalBackup], and [BackupStream]. Nil
// means plaintext (the v0.16.x..v0.21.x shape, preserved for backward
// compatibility); non-nil means every chunk written by this run is
// encrypted under the supplied envelope.
//
// The orchestrator generates the per-chain CEK on first use (per-chain
// mode; the default), wraps it via the envelope, and records the
// wrapped CEK + Argon2id params (passphrase mode) in the chain
// manifest's [ir.ChainEncryption] field. Per-chunk mode generates a
// fresh CEK + wrap per chunk; the wrapped CEK lands in
// [ir.ChunkEncryption.WrappedCEK].
type BackupEncryption struct {
	// Envelope is the [crypto.EnvelopeEncryption] implementation used
	// to wrap CEKs. Phase 6.1: a *crypto.PassphraseEnvelope. Required
	// when the parent struct's encryption is enabled.
	//
	// Cold-start path: the orchestrator uses Envelope as-is to wrap a
	// fresh chain CEK and stamps the envelope's params on the chain
	// root's [ir.ChainEncryption].
	//
	// Chain-extension path: when the orchestrator detects an existing
	// chain root (or in-progress full's prior manifest) carrying
	// recorded [ir.Argon2idParams], it rebuilds the envelope via
	// [BackupEncryption.RebuildForChain] (when supplied) so the KEK
	// derives against the chain's salt rather than a freshly-minted
	// one. Without RebuildForChain, the orchestrator uses Envelope
	// as-is — correct for tests that build envelopes with a known
	// salt, broken for production CLI calls that mint fresh salts.
	// Bug 43 (v0.22.1): closes the gap by routing CLI passphrase
	// envelopes through RebuildForChain.
	Envelope crypto.EnvelopeEncryption

	// RebuildForChain, when non-nil, is called by the orchestrator
	// when extending an existing encrypted chain (incremental / stream
	// against a chain with recorded Argon2id params, or backup-full
	// resume against an in-progress encrypted manifest). The supplied
	// params are the chain root's recorded [ir.Argon2idParams] (the
	// salt that was used to derive the chain's KEK). Implementations
	// should rebuild a [crypto.EnvelopeEncryption] tied to that salt
	// + the operator's passphrase / KMS key.
	//
	// Returning a non-nil error aborts the orchestrator's startup
	// loudly (e.g. wrong passphrase shape).
	//
	// Phase 6.1: passphrase mode populates this with a closure over
	// the operator's passphrase. KMS modes (Phase 6.2/6.3) leave it
	// nil — KMS unwrap doesn't depend on a chain-recorded salt.
	RebuildForChain func(parentParams *ir.Argon2idParams) (crypto.EnvelopeEncryption, error)

	// Mode is "per-chain" (default) or "per-chunk". See
	// `docs/dev/design/logical-backups-phase-6.md` for the trade-off.
	Mode string

	// KEKRef is the operator-visible reference recorded in
	// [ir.ChainEncryption.KEKRef]. Empty for passphrase mode (the
	// salt + Argon2id params are the reference); KMS modes record the
	// key ARN / resource name.
	KEKRef string
}

// rebindForChain rebuilds the encryption envelope against the parent
// chain's recorded Argon2id params and swaps it onto the receiver. A
// no-op when params are nil or RebuildForChain is unset; callers fall
// through to the cold-start envelope in that case.
//
// Bug 43 fix: the write-side previously built the envelope with a
// fresh-minted Argon2id salt, so unwrapping the parent chain's
// WrappedCEK (which was sealed under the parent's salt) failed with
// `aes-gcm open: cipher: message authentication failed`. This helper
// is the load-bearing mirror of the read-side
// [EncryptionFlags.buildReadEnvelope] pattern: detect chain extension
// via recorded Argon2id params, rebuild the envelope tied to those
// params before any CEK unwrap.
func (e *BackupEncryption) rebindForChain(parentParams *ir.Argon2idParams) error {
	if e == nil || parentParams == nil || e.RebuildForChain == nil {
		return nil
	}
	env, err := e.RebuildForChain(parentParams)
	if err != nil {
		return err
	}
	e.Envelope = env
	return nil
}

// DefaultBackupChunkRows is the per-chunk row count when [Backup]'s
// ChunkRows is left at zero. 100,000 rows is the proto-ADR default;
// big enough to amortise per-chunk JSON-Lines + gzip overhead, small
// enough that a single chunk fits comfortably in memory at restore
// time on commodity hardware. Operators tune via --chunk-size.
const DefaultBackupChunkRows = 100_000

// ManifestFileName is the filename of the manifest within a backup
// directory. Convention; restore looks here first.
const ManifestFileName = "manifest.json"

// Backup runs a single Phase 1 full backup against Source / SourceDSN
// and writes to Store. Construct the value, then call Run.
//
// Backup does not retain state between Run calls. Concurrent calls
// on the same value are not supported.
type Backup struct {
	// Source is the engine the source DSN belongs to. Required.
	Source ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// Store is the [ir.BackupStore] backup chunks and manifest are
	// written to. Required.
	Store ir.BackupStore

	// Filter selects which source tables participate in the backup.
	// Empty filter (zero value) keeps every table the schema reader
	// returns.
	Filter TableFilter

	// ChunkRows is the per-chunk row count. Zero falls back to
	// [DefaultBackupChunkRows]. The writer rolls over to a new chunk
	// file whenever the current one hits this row count.
	ChunkRows int

	// SluiceVersion is the build identifier of the running binary,
	// recorded in the manifest. Optional — empty leaves the field
	// blank in the manifest.
	SluiceVersion string

	// SlotName is the source-side replication-slot name to record on
	// the manifest's [ir.Manifest.EndPosition] for engines with a slot
	// concept (Postgres). Phase 3.3: the captured EndPosition pairs the
	// slot name with the source's current LSN at end-of-backup so a
	// subsequent incremental can resume CDC from that LSN against a
	// slot of the same name. Empty falls back to the engine's default
	// (`sluice_slot` on PG); engines without slots (MySQL) ignore the
	// field. The slot need not exist at backup time — Phase 3.3's
	// `sluice sync start --position-from-manifest` pre-flights slot
	// state before resuming CDC.
	SlotName string

	// ChainSlot, when true, asks the engine to provision the chain
	// prerequisites at backup time: the PERSISTENT chain slot (named
	// SlotName) is created and used as the snapshot anchor — its
	// consistent point IS the recorded EndPosition — and kept once
	// the backup completes, so `backup incremental` chains with zero
	// gap by construction. Postgres also ensures the publication the
	// chain's CDC decodes through exists before the anchor (pgoutput
	// resolves publications with a historic catalog snapshot, so a
	// later-created publication cannot decode the chain's first
	// window). Engines without a slot concept (MySQL) log a loud
	// no-op. The slot is dropped only when the run fails BEFORE its
	// in-progress manifest durably records the anchor; after that it
	// is kept even across a failure — it is the WAL-retention
	// guarantee a resumed run adopts (task #42, ADR-0085). See
	// [ir.BackupSnapshotOptions].
	ChainSlot bool

	// TableParallelism caps how many tables stream CONCURRENTLY during
	// the row sweep (ADR-0084, the backup sibling of migrate's
	// --table-parallelism / ADR-0076). 0 = auto (4, pgcopydb
	// --table-jobs parity); 1 = serial (the pre-ADR-0084 behaviour).
	// Only engages when the source surfaces a shareable exported
	// snapshot AND implements [ir.SnapshotImporterOpener]
	// ([backupParallelEligible]); otherwise the sweep stays serial with
	// a loud INFO naming the reason. The resolved value is further
	// bounded by the SOURCE's measured connection budget, reserving one
	// slot for the snapshot's slot-creation replication conn.
	TableParallelism int

	// ForceOverwrite, when true, lets a re-run replace a previously-
	// completed backup at the same destination. Without it, finding a
	// `partial_state == "complete"` manifest at the destination is an
	// operator-actionable error. This is the analog of
	// `migrate --reset-target-data` for the backup verb. In-progress
	// manifests resume by default; with this flag set they are
	// DISCARDED and the run starts fresh — the escape hatch the resume
	// guards (schema drift, keyless re-stream, chain-slot preflight;
	// task #42 / ADR-0085) name in their refusals.
	ForceOverwrite bool

	// Encryption, when non-nil, encrypts every chunk this run writes.
	// See [BackupEncryption]. Empty (nil) preserves the plaintext
	// shape — the v0.16.x..v0.21.x default.
	Encryption *BackupEncryption

	// Redactor, when non-nil and non-empty, applies operator-
	// configured PII redaction to every row before it's written to a
	// chunk (PII Phase 1.5, roadmap item 15a follow-on, v0.55.0).
	// Same redact.Registry shape used by [Migrator.Redactor] /
	// [Streamer.Redactor]. nil/empty is the no-redactions hot path
	// (zero-cost passthrough). Closes the Phase 1.5 backup-stream
	// gap so backups stored on disk are PII-clean when the operator
	// supplies --redact flags to `sluice backup full` / `backup
	// stream run`.
	Redactor *redact.Registry

	// Codec is the per-segment compression codec (ADR-0046 §5) every
	// chunk this run writes is compressed with. Recorded on the
	// lineage's segment 0; restore reads it from there, never sniffs
	// it. Empty resolves to [DefaultCodec] (zstd, v0.67.0+). A
	// one-segment never-rotated lineage takes the same single-segment
	// restore path as a pre-ADR single chain (codec aside).
	Codec Codec

	// Now, when set, overrides the wall-clock-time source for
	// [Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time
}

// Run executes the backup. Returns nil on success; a wrapped error on
// any phase failure.
//
// On success: the Store contains exactly one `manifest.json` (with
// `partial_state == "complete"`) and one chunk file per (table,
// chunk-index) in the source schema.
func (b *Backup) Run(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}

	// Engine-default exclusions (Bug 22): merge in PlanetScale's
	// `_vt_*` shadow tables when the source signals them via the
	// optional [ir.DefaultTableExcluder] surface.
	if eff, added := effectiveTableFilter(b.Filter, b.Source, b.SourceDSN); len(added) > 0 {
		slog.InfoContext(
			ctx, "backup: applying engine-default table exclusions",
			slog.String("engine", b.Source.Name()),
			slog.Any("patterns", added),
		)
		b.Filter = eff
	}

	// 0. Resume detection + anchor adoption: decide whether to
	// fresh-start, resume (adopting the prior attempt's anchor), or
	// refuse — and preflight a --chain-slot adoption — before anything
	// new is opened on the source.
	prior, resumeAnchor, err := b.resolveResumeState(ctx)
	if err != nil {
		return err
	}
	adopting := resumeAnchor.Engine != "" || resumeAnchor.Token != ""

	// 1. Read source schema.
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: open source schema reader: %w", err))
	}
	defer closeIf(sr)

	// ADR-0047 tier (b): a PG-source backup may carry uncatalogued
	// extension types verbatim. The restore-target engine is unknown
	// at backup time, so this only enables CAPTURE — the PG-restore-
	// only constraint is enforced later by the recorded lineage marker
	// (verbatimExtensionColumnsIn → LineageSegment) + the loud
	// restore-time engine gate. A non-PG source never enables it.
	applyVerbatimExtensionPassthrough(sr, verbatimBackupSourcePG(b.Source))

	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (b.Filter already has engine defaults merged above).
	applyTableScope(sr, b.Filter)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "backup: source schema has no tables; manifest with empty table list will be written")
	}

	// 2. Apply table filter.
	if err := applyTableFilter(ctx, schema, b.Filter); err != nil {
		// applyTableFilter errors when the filter excludes everything.
		// For backups that's still a valid intent in some workflows
		// (e.g. "snapshot-time-only"), but matching the migrate shape
		// — surface the error so the operator notices.
		return err
	}

	// 2.5. Schema-stability guard for anchored resume (task #42): the
	// adopted anchor claims "kept chunks + CDC replay from the anchor
	// converge to source state", which DDL between the two attempts
	// breaks — the kept chunks and the new schema disagree, and the
	// replay window carries events shaped by a schema the manifest
	// never recorded. Refuse before the snapshot opens.
	if adopting {
		if err := refuseAnchoredResumeOnSchemaDrift(schema, prior.Schema); err != nil {
			return err
		}
	}

	// 3. Open the row reader. v0.18.0: prefer the snapshot-anchored
	// path that captures EndPosition at snapshot START and pins all
	// table reads to a cross-table consistent view, closing the
	// during-backup write-window gap that v0.17.x's basic OpenRowReader
	// path leaves open. Engines that don't implement
	// [ir.BackupSnapshotOpener] — OR engines that implement it but
	// whose OpenBackupSnapshot returns an error (e.g. PG without
	// `wal_level=logical`) — fall through to the v0.17.x shape with a
	// soft warning so operators know the chain rooted in this full will
	// carry the during-backup write-window gap. One-shot full backups
	// without CDC are still legitimate; chain correctness is the only
	// thing that needs the snapshot path.
	//
	// On an anchored resume the persistent chain slot is NOT re-created
	// (it already exists at the adopted anchor — see the adoption
	// preflight above): the snapshot opens in the temporary-anchor
	// shape, serving only read consistency for tables streamed THIS run.
	rr, snapshotPos, snap, snapshotCloser, err := b.openSnapshotOrFallback(ctx, schema, b.ChainSlot && !adopting)
	if err != nil {
		return err
	}
	defer snapshotCloser()

	// 4. Stream each table to chunk file(s).
	chunkRows := b.ChunkRows
	if chunkRows <= 0 {
		chunkRows = DefaultBackupChunkRows
	}

	now := time.Now
	if b.Now != nil {
		now = b.Now
	}

	// Pre-build the in-progress manifest with the schema. Tables get
	// appended (or copied from the prior run) as they finish; the
	// manifest is committed after each table completes.
	manifest := &ir.Manifest{
		// Bug 116 closure: stamp the smallest format version safe for
		// this schema. Schemas using RLS / policies / exclude
		// constraints get FormatVersion=2 so older binaries refuse
		// rather than silently drop those fields; innocent schemas
		// stay on FormatVersion=1 for max backward compatibility.
		FormatVersion: ir.FormatVersionFor(schema),
		SluiceVersion: b.SluiceVersion,
		CreatedAt:     now().UTC(),
		SourceEngine:  b.Source.Name(),
		Schema:        schema,
		Tables:        make([]*ir.TableManifest, 0, len(schema.Tables)),
		PartialState:  ir.BackupStateInProgress,
		Kind:          ir.BackupKindFull,
	}
	if prior != nil {
		// Preserve the original CreatedAt across resume so the
		// "when was this backup taken?" answer is the snapshot point,
		// not the resume point.
		manifest.CreatedAt = prior.CreatedAt
	}
	// Task #42 (ADR-0085): stamp the chain anchor on the IN-PROGRESS
	// manifest from its first write, so a crashed run leaves the anchor
	// a future resume must adopt. A resumed run stamps the ADOPTED
	// prior-attempt anchor — never this run's fresh snapshot position.
	// The non-snapshot fallback path has no anchor yet and keeps its
	// post-sweep capture (step 4.5).
	switch {
	case adopting:
		manifest.EndPosition = resumeAnchor
	case snapshotPos != nil:
		manifest.EndPosition = *snapshotPos
	}

	// Phase 6.1: when encryption is enabled, generate the chain-level
	// CEK (per-chain mode) up-front, wrap it via the envelope, and
	// stamp the manifest's [ir.ChainEncryption] header. Per-chunk
	// mode leaves the chain-level WrappedCEK empty; each chunk
	// generates its own CEK at write time.
	chainCEK, err := b.setupChainEncryption(manifest, prior)
	if err != nil {
		return fmt.Errorf("backup: setup encryption: %w", err)
	}

	priorTables := indexManifestTables(priorResumableTables(prior))
	if len(priorTables) > 0 && !adopting && snapshotPos != nil {
		// Pre-anchor-adoption in-progress manifest (no recorded
		// EndPosition) under a snapshot-anchored run: keeping its
		// completed tables verbatim would pair old-anchor chunks with
		// this run's NEW anchor — exactly the silent chain gap ADR-0085
		// closes. Re-stream everything instead; the content-addressed
		// same-path upload skip still avoids re-uploading identical
		// bytes. The v0.17.x non-snapshot fallback is deliberately NOT
		// in scope: it records no snapshot anchor (post-sweep capture,
		// documented during-backup gap), so kept tables introduce no
		// NEW gap there and table-level resume stays.
		slog.WarnContext(
			ctx, "backup: prior in-progress manifest carries no anchor (written by a pre-anchor-adoption binary); its completed tables cannot be kept safely — re-streaming every table from this run's snapshot",
			slog.String("reason", "kept tables would be exact as-of an unknown earlier anchor; recording this run's anchor over them would silently gap the chain (ADR-0085)"),
		)
		priorTables = nil
	}

	// Bug 34a: Once the schema is in hand, fan out the resume decision
	// per-table so operators can see exactly what's being resumed and
	// what's being re-run. Decisions are surface-level only here; the
	// actual skip / per-chunk-resume happens inside the loop below.
	//
	// A prior entry with Partial=false (or omitted, for backward compat
	// with pre-v0.16.1 manifests) AND all its chunks still on the store
	// is a "fully complete" table (skip whole table). Partial=true with
	// some chunks present falls into the per-chunk resume path.
	if prior != nil && prior.PartialState == ir.BackupStateInProgress {
		var alreadyComplete, toResume []string
		for _, table := range schema.Tables {
			key := manifestTableKey(table.Schema, table.Name)
			existing, ok := priorTables[key]
			if !ok {
				toResume = append(toResume, table.Name)
				continue
			}
			full, err := tableManifestFullyComplete(ctx, b.Store, existing)
			if err != nil {
				return fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if full {
				alreadyComplete = append(alreadyComplete, table.Name)
			} else {
				toResume = append(toResume, table.Name)
			}
		}
		slog.InfoContext(
			ctx, "resume plan",
			slog.Int("tables_already_complete", len(alreadyComplete)),
			slog.Any("tables_to_resume", toResume),
		)
	}

	// Pre-stage every table's manifest entry in schema order, then sweep
	// the non-complete ones through the bounded cross-table pool
	// (ADR-0084). Pre-staging keeps the manifest's table order
	// deterministic (== schema order) regardless of worker completion
	// order; the committer serializes every entry mutation + manifest
	// Put. Per-chunk + per-table checkpoints live inside backupTable /
	// the committer now — a crash leaves at most tableParallelism tables
	// with partial chunk lists to redo.
	committer := &manifestCommitter{store: b.Store, manifest: manifest}
	tasks, err := b.stageBackupTables(ctx, committer, schema, priorTables)
	if err != nil {
		return err
	}

	// Task #42 guard: on an anchored resume, every (re-)streamed table's
	// chunks are read at THIS run's later snapshot, so they overlap the
	// chain's replay window (adopted anchor, stop]. The overlap converges
	// only because the chain appliers are idempotent on a key (ADR-0010);
	// a truly keyless table falls back to plain INSERT and would
	// duplicate the overlapping rows — refuse loudly. Kept tables may be
	// keyless: they are exact at the adopted anchor, no overlap.
	if adopting {
		if err := refuseKeylessRestreamOnAnchoredResume(tasks); err != nil {
			return err
		}
	}

	// Task #42 (ADR-0085): make the anchor-stamped in-progress manifest
	// durable BEFORE the sweep, so every crash from here on leaves a
	// resumable record carrying the anchor. For a fresh --chain-slot run
	// this is also the moment the chain slot becomes load-bearing: once
	// a durable manifest references the anchor, a later failure must NOT
	// drop the slot (it is the WAL-retention guarantee a resume adopts),
	// so the snapshot is committed NOW rather than after the final
	// manifest write. A failure before this point still drops the slot
	// via the deferred Close — nothing references it yet. On a resumed
	// run (temporary-anchor shape) and on non-chain-slot runs Commit is
	// a no-op.
	if err := committer.commit(ctx); err != nil {
		return fmt.Errorf("backup: write in-progress manifest: %w", err)
	}
	if err := snap.Commit(ctx); err != nil {
		return fmt.Errorf("backup: persist chain resources: %w", err)
	}

	snapshotName := ""
	if snap != nil {
		snapshotName = snap.SnapshotName
	}
	tableParallelism, err := b.resolveBackupTableParallelism(ctx, snapshotName, len(tasks))
	if err != nil {
		return err
	}
	factory, factoryCleanup, err := b.openBackupReaderFactory(ctx, snapshotName, tableParallelism)
	if err != nil {
		return err
	}
	defer factoryCleanup()

	if err := b.runBackupTablePool(ctx, tasks, rr, factory, tableParallelism, chunkRows, committer, chainCEK); err != nil {
		return err
	}

	// 4.5. Record EndPosition. Three paths:
	//
	//   - anchored resume (task #42, ADR-0085): the manifest keeps the
	//     ADOPTED prior-attempt anchor stamped at creation. This run's
	//     fresh snapshot position must NOT overwrite it — kept tables
	//     are exact at the adopted anchor, and CDC from there covers
	//     everything after it (re-streamed tables' overlap converges
	//     under the idempotent chain appliers).
	//   - v0.18.0 snapshot-anchored: snapshotPos was captured at
	//     snapshot START before the row sweep. The row sweep's reads
	//     all observe the source AS-OF this position, and CDC from
	//     this position forward covers every write after the snapshot.
	//     The during-backup window gap is closed.
	//   - v0.17.x fallback: the engine doesn't implement
	//     BackupSnapshotOpener so we capture the position now,
	//     post-sweep, via the optional [ir.BackupPositionCapturer].
	//     This is the v0.17.2 shape with the documented during-backup
	//     write-window gap; the openSnapshotOrFallback step has
	//     already logged a WARN line so operators know.
	switch {
	case adopting:
		manifest.EndPosition = resumeAnchor // re-assert: nothing may overwrite the adopted anchor
		thisRunToken := "<none — v0.17.x fallback read>"
		if snapshotPos != nil {
			thisRunToken = snapshotPos.Token
		}
		slog.WarnContext(
			ctx, "backup: resumed run recorded the interrupted attempt's anchor as EndPosition (adoption); this run's snapshot served only read consistency for re-streamed tables",
			slog.String("engine", manifest.SourceEngine),
			slog.String("adopted_position_token", resumeAnchor.Token),
			slog.String("this_run_snapshot_token", thisRunToken),
		)
	case snapshotPos != nil:
		manifest.EndPosition = *snapshotPos
		slog.InfoContext(
			ctx, "backup: recorded end position (snapshot-anchored)",
			slog.String("engine", manifest.SourceEngine),
			slog.String("position_token", snapshotPos.Token),
		)
	default:
		if err := b.captureEndPosition(ctx, manifest); err != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("backup: capture end position: %w", err))
		}
	}

	// Compute BackupID once EndPosition is known. The id is used by
	// Phase 3 incrementals to link via ParentBackupID; pre-Phase-3
	// fulls leave it empty, which the chain-restore walker tolerates by
	// computing on demand. Filling it in here means the full's manifest
	// carries the same id the incremental would compute when chaining.
	manifest.BackupID = ir.ComputeBackupID(manifest)

	// 5. Final manifest write — flip to complete. Routed through the
	// committer like every other manifest Put this run (the pool has
	// drained, so this is single-threaded; one code path regardless).
	manifest.PartialState = ir.BackupStateComplete
	if err := committer.commit(ctx); err != nil {
		return fmt.Errorf("backup: write final manifest: %w", err)
	}
	// Chain resources (--chain-slot: the slot anchored at EndPosition)
	// were already persisted by the pre-sweep snap.Commit above — they
	// must survive a mid-sweep failure so a resume can adopt them
	// (task #42, ADR-0085).
	// ADR-0046: seed / update lineage.json. A full backup is segment 0
	// of a one-segment lineage at the conventional root (Dir == "").
	// Best-effort — the manifest file is authoritative for the
	// one-segment shape; lineage.json is the O(1) segment-shape +
	// recorded-codec accelerator.
	updateLineageForManifestBestEffort(ctx, b.Store, manifest, ManifestFileName, resolveCodec(b.Codec))

	totalRows := int64(0)
	totalChunks := 0
	for _, t := range manifest.Tables {
		totalRows += t.RowCount
		totalChunks += len(t.Chunks)
	}
	slog.InfoContext(
		ctx, "backup complete",
		slog.Int("tables", len(manifest.Tables)),
		slog.Int64("rows", totalRows),
		slog.Int("chunks", totalChunks),
	)
	return nil
}

// resolveResumeState is Run's step 0: inspect any pre-existing manifest
// at the destination and decide whether to fresh-start (prior == nil),
// resume (prior != nil; resumeAnchor carries the interrupted attempt's
// anchor when it recorded one), or refuse.
//
// Resume anchor adoption (task #42, ADR-0085): the prior attempt's
// completed tables are kept verbatim — exact as-of the PRIOR anchor —
// so the position the resumed run records must be that prior anchor
// (min-anchor rule), never this run's fresh snapshot position.
// resumeAnchor stays zero when there is nothing to adopt: a fresh run,
// or a prior manifest written by a pre-fix binary (those never stamped
// EndPosition while in progress; Run's re-stream-everything fallback
// handles them).
//
// A --chain-slot resume is an ADOPTION, not a creation: the prior
// attempt's persistent chain slot already stands at the adopted anchor
// and IS the chain's WAL-retention guarantee. It is preflighted here,
// BEFORE anything opens on the source — a slot that is missing or has
// advanced past the anchor cannot serve the gap WAL, and resuming
// anyway would silently skip it. The adopted slot is never created,
// committed, or dropped by the resumed run: its snapshot opens in the
// temporary-anchor shape (PersistChainSlot=false), so even a failed
// resume leaves the chain slot standing.
func (b *Backup) resolveResumeState(ctx context.Context) (prior *ir.Manifest, resumeAnchor ir.Position, err error) {
	prior, err = readManifestIfPresent(ctx, b.Store)
	if err != nil {
		return nil, ir.Position{}, fmt.Errorf("backup: inspect existing manifest: %w", err)
	}
	if prior == nil {
		return nil, ir.Position{}, nil
	}
	switch prior.PartialState {
	case ir.BackupStateInProgress:
		if b.ForceOverwrite {
			// Task #42 (ADR-0085): --force-overwrite now also discards
			// an in-progress prior — it is the escape hatch the resume
			// guards (schema drift, keyless re-stream, chain-slot
			// preflight) point operators at, so it must actually
			// produce a fresh start.
			slog.InfoContext(
				ctx, "backup: --force-overwrite set; discarding in-progress prior backup and starting fresh",
				slog.Time("prior_created_at", prior.CreatedAt),
			)
			return nil, ir.Position{}, nil
		}
		// Bug 34a: emit a clear "resuming" log line so operators can
		// see resume happened. The detailed per-table fan-out (which
		// tables to skip vs resume) follows once the schema is read;
		// this is the headline.
		slog.InfoContext(
			ctx, "resuming from partial backup",
			slog.String("backup_dir", backupStoreDescriptor(b.Store)),
			slog.Int("tables_in_prior_manifest", len(prior.Tables)),
			slog.Time("prior_created_at", prior.CreatedAt),
		)
		// Bug 137: an in-progress manifest proves a prior run died
		// mid-flight. Backups crashed under pre-fix binaries left a
		// persistent anchor replication slot on the source, each one
		// silently pinning WAL until the disk fills — give the engine
		// its chance to sweep that debris now. Best-effort hygiene via
		// the optional [ir.BackupAnchorSweeper] surface: a sweep
		// failure must not fail the resume itself.
		if sweeper, ok := b.Source.(ir.BackupAnchorSweeper); ok {
			if err := sweeper.SweepOrphanedBackupAnchors(ctx, b.SourceDSN); err != nil {
				slog.WarnContext(
					ctx, "backup: orphaned-anchor sweep failed; stale anchor slots may still be retaining WAL on the source",
					slog.String("engine", b.Source.Name()),
					slog.String("err", err.Error()),
				)
			}
		}
		resumeAnchor = prior.EndPosition
	case ir.BackupStateComplete, "":
		if !b.ForceOverwrite {
			return nil, ir.Position{}, fmt.Errorf("backup: a completed backup already exists at this destination (created %s); pass --force-overwrite to replace it",
				prior.CreatedAt.UTC().Format(time.RFC3339))
		}
		slog.InfoContext(
			ctx, "backup: --force-overwrite set; replacing existing complete backup",
			slog.Time("prior_created_at", prior.CreatedAt),
		)
		return nil, ir.Position{}, nil // discard so we start from scratch
	}

	if b.ChainSlot && (resumeAnchor.Engine != "" || resumeAnchor.Token != "") {
		if err := preflightChainResume(ctx, b.Source, b.SourceDSN, resumeAnchor); err != nil {
			return nil, ir.Position{}, fmt.Errorf(
				"backup: resume --chain-slot: the chain slot cannot serve the interrupted attempt's anchor, so resuming would silently gap the chain: %w. "+
					"To deliberately start over instead, pass --force-overwrite (and drop the slot via `sluice slot drop` if it still exists)", err,
			)
		}
		slog.InfoContext(
			ctx, "backup: resume --chain-slot: adopting the interrupted attempt's chain slot; no new chain slot is created and this run never drops the adopted one",
			slog.String("adopted_position_token", resumeAnchor.Token),
		)
	}
	return prior, resumeAnchor, nil
}

// openSnapshotOrFallback returns a row reader the orchestrator drives
// the table sweep against, plus an optional snapshot-anchored
// EndPosition captured at snapshot start.
//
// v0.18.0: when the engine implements [ir.BackupSnapshotOpener], we
// open a backup-scoped consistent snapshot. The returned RowReader is
// pinned to the snapshot view (cross-table consistency holds) and the
// returned position is the snapshot's anchor LSN/GTID — recorded on
// manifest.EndPosition so the Phase 3 incremental's CDC pump from this
// position forward covers every write after the snapshot, closing the
// during-backup write-window gap that v0.17.x's basic OpenRowReader
// path leaves open.
//
// Fallback (two flavours, both end up on the v0.17.x shape):
//
//   - Engine doesn't implement BackupSnapshotOpener at all (e.g. an
//     engine that hasn't been migrated to v0.18.0 yet). We fall
//     through with a WARN naming the gap.
//   - Engine implements it but the call returned an error — e.g. PG
//     without `wal_level=logical` can't create the temporary anchor
//     slot, so OpenBackupSnapshot fails. v0.17.x's non-snapshot path
//     is still a legitimate one-shot full-backup shape; we don't fail
//     the run, we fall through with a WARN naming the underlying
//     error AND the operational implication (chain correctness needs
//     the snapshot path → operator action: enable wal_level=logical
//     or pair backups with continuous `sluice sync start`).
//
// Either way, the fallback gets a basic OpenRowReader (no shared
// snapshot, no cross-table consistency) plus a post-sweep
// [ir.BackupPositionCapturer] capture (later, in
// [Backup.captureEndPosition]).
//
// Returns (reader, snapshotPos, cleanup, err). When snapshotPos is
// nil the caller falls through to captureEndPosition; when non-nil it
// has already been captured. cleanup is always non-nil and safe to
// call. err is reserved for outright open failures of the fallback
// row reader itself — a snapshot open error is NOT propagated as
// err; it triggers the fallback instead.
// openBackupSnapshotScoped opens a backup-scoped consistent snapshot,
// preferring the table-scoped surface when a table scope is in effect.
// It is the backup-path sibling of [openSnapshotStreamScoped] (the
// cold-start dispatcher) — same selection logic, [ir.BackupSnapshot]
// shape.
//
//   - source implements [ir.TableScopedBackupSnapshotOpener] AND there
//     are filtered tables (len(tables) > 0) → OpenBackupSnapshotForTables,
//     so a PlanetScale backup scopes its VStream COPY to the included
//     tables (avoids the ADR-0071 over-stream/buffer-overflow on a large
//     unrelated keyspace table).
//   - else source implements [ir.BackupSnapshotOpener] → OpenBackupSnapshot
//     (unchanged whole-snapshot path — PG, vanilla MySQL via base).
//   - else → not implemented (ok=false); the caller takes the v0.17.x
//     non-snapshot fallback.
//
// The schema passed here is already filtered to the included tables by
// [applyTableFilter] (called earlier in Run), so its table names are the
// backup's effective scope. implemented reports whether ANY snapshot
// opener was found; err carries an open failure (the caller falls back to
// the v0.17.x path on a non-nil err exactly as the base OpenBackupSnapshot
// error path does — a scoped-open error is NOT a different failure mode).
//
// persistChainSlot is b.ChainSlot minus the anchored-resume case: a
// resume of a --chain-slot backup ADOPTS the prior attempt's standing
// chain slot (task #42, ADR-0085) and opens this run's snapshot in the
// temporary-anchor shape so the adopted slot is never re-created — and
// never dropped by this run's failure path.
func (b *Backup) openBackupSnapshotScoped(ctx context.Context, schema *ir.Schema, persistChainSlot bool) (snap *ir.BackupSnapshot, implemented bool, err error) {
	opts := ir.BackupSnapshotOptions{
		SlotName:         b.SlotName,
		PersistChainSlot: persistChainSlot,
	}
	tables := tableNamesForPublication(schema)
	if len(tables) > 0 {
		if scoped, ok := b.Source.(ir.TableScopedBackupSnapshotOpener); ok {
			snap, err = scoped.OpenBackupSnapshotForTables(ctx, b.SourceDSN, opts, tables)
			return snap, true, err
		}
	}
	if opener, ok := b.Source.(ir.BackupSnapshotOpener); ok {
		snap, err = opener.OpenBackupSnapshot(ctx, b.SourceDSN, opts)
		return snap, true, err
	}
	return nil, false, nil
}

func (b *Backup) openSnapshotOrFallback(ctx context.Context, schema *ir.Schema, persistChainSlot bool) (ir.RowReader, *ir.Position, *ir.BackupSnapshot, func(), error) {
	if snap, ok, err := b.openBackupSnapshotScoped(ctx, schema, persistChainSlot); ok {
		if err == nil {
			slog.InfoContext(
				ctx, "backup: opened snapshot-anchored consistent view",
				slog.String("engine", b.Source.Name()),
				slog.String("position_token", snap.Position.Token),
			)
			pos := snap.Position
			cleanup := func() {
				if err := snap.Close(); err != nil {
					slog.WarnContext(
						ctx, "backup: snapshot close failed; partial cleanup may have leaked resources",
						slog.String("err", err.Error()),
					)
				}
			}
			return snap.Rows, &pos, snap, cleanup, nil
		}
		// --chain-slot is an explicit request for chain provisioning;
		// the v0.17.x fallback cannot honour it (no snapshot anchor →
		// no slot to persist → the chain the operator asked for would
		// silently not exist). Refuse instead of degrading.
		if b.ChainSlot {
			return nil, nil, nil, func() {}, fmt.Errorf("backup: --chain-slot requested but the snapshot-anchored path is unavailable: %w", err)
		}
		// Snapshot open failed (e.g. PG without `wal_level=logical`
		// can't create the temporary anchor slot). v0.17.x's
		// non-snapshot path is still a legitimate one-shot full-backup
		// shape — the chain → handoff story is the only thing that
		// breaks. Don't fail the run; fall through with a WARN line
		// that names the operational implication so operators using
		// this backup as a chain root know to enable wal_level=logical
		// (or pair the backup with continuous `sluice sync start` so
		// the live stream covers the during-backup window).
		slog.WarnContext(
			ctx, "backup: snapshot-anchored consistent view unavailable; falling back to v0.17.x path",
			slog.String("engine", b.Source.Name()),
			slog.String("error", err.Error()),
			slog.String("implication", "chains rooted in this full will have a during-backup write-window gap (see v0.17.2 release notes); enable wal_level=logical for snapshot-anchored consistency, or pair backups with continuous `sluice sync start`"),
			slog.String("see_also", "v0.17.2 release notes; docs/dev/design/logical-backups-phase-3.md"),
		)
	} else {
		// Engine doesn't implement BackupSnapshotOpener at all. With
		// --chain-slot that's a refusal (chain provisioning is
		// impossible — degrading silently would betray the explicit
		// intent); without it, surface the gap loudly — operators
		// consuming the chain need to know writes during the backup
		// window are not guaranteed to be captured. The recommended
		// mitigation (pair backups with continuous `sluice sync
		// start`) is the same one the v0.17.2 release notes called
		// out.
		if b.ChainSlot {
			return nil, nil, nil, func() {}, fmt.Errorf("backup: --chain-slot requested but engine %q does not implement the snapshot-anchored backup path", b.Source.Name())
		}
		slog.WarnContext(
			ctx, "backup: engine does not implement BackupSnapshotOpener; falling back to non-snapshot row reads",
			slog.String("engine", b.Source.Name()),
			slog.String("impact", "chains rooted in this full will have a during-backup write-window gap"),
			slog.String("mitigation", "pair backups with continuous `sluice sync start` so the live stream captures every write"),
			slog.String("see_also", "v0.17.2 release notes; docs/dev/design/logical-backups-phase-3.md"),
		)
	}

	rr, err := b.Source.OpenRowReader(ctx, b.SourceDSN)
	if err != nil {
		return nil, nil, nil, func() {}, wrapWithHint(PhaseConnect, fmt.Errorf("backup: open source row reader: %w", err))
	}
	cleanup := func() { closeIf(rr) }
	return rr, nil, nil, cleanup, nil
}

// captureEndPosition queries the source for its current CDC position
// and stores it on manifest.EndPosition. v0.18.0: this path is the
// FALLBACK shape used only when the engine doesn't implement
// [ir.BackupSnapshotOpener] — engines that do (PG, MySQL in v0.18.0+)
// route through [openSnapshotOrFallback] instead and capture the
// position at snapshot START rather than post-sweep.
//
// Engines that don't support CDC (Capabilities.CDC == ir.CDCNone) skip
// the capture; engines that do but don't implement
// [ir.BackupPositionCapturer] also skip with a debug log line so the
// gap is visible to operators running with --log-level=debug.
//
// In the fallback shape the capture happens AFTER the per-table row
// sweep, so the recorded position reflects "the source has produced
// everything up to here at the moment the backup completes." Writes
// during the backup window are NOT covered by this path's row sweep
// (no shared snapshot) and NOT covered by the chain's next link's
// `--since=<full>.EndPosition` window (those LSNs are before this
// captured EndPosition) — the documented v0.17.2 caveat.
func (b *Backup) captureEndPosition(ctx context.Context, manifest *ir.Manifest) error {
	if b.Source.Capabilities().CDC == ir.CDCNone {
		slog.DebugContext(
			ctx, "backup: source does not support CDC; skipping EndPosition capture",
			slog.String("engine", b.Source.Name()),
		)
		return nil
	}
	// We need a SchemaReader-shaped surface for the optional capturer
	// (it lives there to share the engine's *sql.DB pool). Open one
	// just for the capture; the engine handles connection lifetime
	// via Close.
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return fmt.Errorf("open schema reader for position capture: %w", err)
	}
	defer closeIf(sr)

	capturer, ok := sr.(ir.BackupPositionCapturer)
	if !ok {
		slog.DebugContext(
			ctx, "backup: source SchemaReader does not implement BackupPositionCapturer; manifest EndPosition will be empty",
			slog.String("engine", b.Source.Name()),
		)
		return nil
	}
	pos, err := capturer.CaptureBackupPosition(ctx, b.SlotName)
	if err != nil {
		return fmt.Errorf("capture position: %w", err)
	}
	manifest.EndPosition = pos
	slog.InfoContext(
		ctx, "backup: recorded end position",
		slog.String("engine", manifest.SourceEngine),
		slog.String("position_token", pos.Token),
	)
	return nil
}

// validate checks that all required fields are populated.
func (b *Backup) validate() error {
	switch {
	case b.Source == nil:
		return errors.New("backup: Source engine is nil")
	case b.SourceDSN == "":
		return errors.New("backup: SourceDSN is empty")
	case b.Store == nil:
		return errors.New("backup: Store is nil")
	}
	return nil
}

// backupTable streams every row of task.table from rr through one or
// more chunk files in the Store, filling in the table's pre-staged
// manifest entry (task.entry) through the committer.
//
// Concurrency (ADR-0084): backupTable runs on a pool worker, possibly
// alongside peers streaming other tables. Everything it touches is
// worker-local — rr, the chunk writer, the row cursor — EXCEPT the
// manifest entry, whose every mutation (and every manifest checkpoint)
// is routed through the committer's mutex. The per-chunk CEK path
// ([Backup.resolveChunkCEK]) is safe concurrently: every envelope's
// WrapCEK is read-only on envelope state and crypto/rand is
// goroutine-safe.
//
// One subtle point worth flagging: the chunk-roll boundary is checked
// AFTER each row write. A table whose row count is an exact multiple
// of chunkRows ends with a fully-written chunk and no extra trailing
// chunk. Empty tables produce zero chunks (the manifest entry has
// RowCount=0 and Chunks=nil) — keeps the storage layout tidy.
//
// Partially-written tables from a prior crashed run are re-streamed
// FROM SCRATCH — their prior chunks are deliberately not reused. The
// pre-v0.99.36 per-chunk resume (Bug 34b) reused prior chunk N and
// advanced the new row stream by N×chunkRows rows, which assumed the
// two runs' row streams deliver identical order. They don't:
// [buildSelect] has no ORDER BY (a full-table ORDER BY would gut read
// throughput), so scan order is only repeatable by accident — and
// under the ADR-0084 parallel sweep the accident stopped holding,
// producing backups with duplicate AND missing rows that exit 0
// (Bug 135, CRITICAL, caught by the v0.99.35 battle-test). Whole-table
// reuse (Partial=false entries, staged verbatim by
// [Backup.stageBackupTables]) is order-independent and stays. The redo
// cost is bounded by the crash contract: at most tableParallelism
// tables are in flight. flush's same-path SHA comparison below is the
// only chunk-level reuse left — it is content-addressed (compares the
// NEWLY-PRODUCED bytes against what's already at the path), so it
// never depends on scan order.
func (b *Backup) backupTable(
	ctx context.Context,
	rr ir.RowReader,
	task backupTableTask,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
) error {
	table, entry := task.table, task.entry
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}

	cols := nonGeneratedTableColumns(table)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	var (
		writer        *chunkWriter
		buf           *bytes.Buffer
		chunkIdx      int
		rowsTotal     int64
		curWrappedCEK []byte // populated only in per-chunk mode
	)

	flush := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		chunkPath := chunkFilePath(table, chunkIdx)
		hash := writer.Hash()
		// Resumable-write defence: if a chunk file is already at this
		// path (from a prior interrupted run) AND its bytes hash to
		// the same value as the chunk we just produced, skip the
		// upload. Mismatches overwrite — a corrupted partial-upload
		// shouldn't survive a re-run.
		skip, err := chunkAlreadyMatches(ctx, b.Store, chunkPath, hash)
		if err != nil {
			return fmt.Errorf("inspect existing chunk %q: %w", chunkPath, err)
		}
		if !skip {
			if err := b.Store.Put(ctx, chunkPath, buf); err != nil {
				return fmt.Errorf("store put %q: %w", chunkPath, err)
			}
		}
		ci := &ir.ChunkInfo{
			File:     chunkPath,
			RowCount: writer.RowCount(),
			SHA256:   hash,
		}
		if b.Encryption != nil {
			ci.Encryption = &ir.ChunkEncryption{
				Algorithm:  crypto.AlgorithmAESGCM,
				NonceLen:   crypto.NonceLen,
				AuthTagLen: crypto.AuthTagLen,
				WrappedCEK: curWrappedCEK, // empty for per-chain mode
			}
		}
		writer = nil
		buf = nil
		curWrappedCEK = nil
		chunkIdx++
		// Record the chunk + per-chunk checkpoint: commit the manifest
		// after every chunk so operators (and the resume plan's log)
		// can see progress accrue. Resume no longer REUSES these
		// partial chunks (see the function comment / Bug 135) — the
		// checkpoint is observability, and it keeps the same-path SHA
		// fast-skip in flush effective across a re-run. The committer
		// serializes the entry mutation + the same-key manifest Put
		// against peer tables.
		if err := committer.appendChunk(ctx, entry, ci); err != nil {
			return fmt.Errorf("checkpoint manifest after chunk %d: %w", chunkIdx-1, err)
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, ok := <-rows:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				// Surface any sticky error captured by the reader's
				// streaming goroutine (Bug 68 loud-failure gate; now a
				// first-class [ir.RowReader.Err] surface, no longer an
				// optional type assertion).
				if err := readerStreamErr(rr, table); err != nil {
					return err
				}
				// Per-table checkpoint: flip the entry to its terminal
				// state (natural EOF) and commit. Empty tables and tables
				// whose row count is an exact chunk multiple rely on this
				// commit — their last per-chunk checkpoint (if any)
				// doesn't carry the Partial=false flip.
				if err := committer.finishTable(ctx, entry, rowsTotal); err != nil {
					return fmt.Errorf("checkpoint manifest after table: %w", err)
				}
				slog.InfoContext(
					ctx, "backup: table complete",
					slog.String("table", table.Name),
					slog.Int64("rows", rowsTotal),
					slog.Int("chunks", len(entry.Chunks)),
				)
				return nil
			}
			if writer == nil {
				buf = &bytes.Buffer{}
				cek, wrapped, err := b.resolveChunkCEK(chainCEK)
				if err != nil {
					return fmt.Errorf("resolve chunk cek: %w", err)
				}
				curWrappedCEK = wrapped
				w, err := newChunkWriter(buf, colNames, cek, b.Codec)
				if err != nil {
					return fmt.Errorf("open chunk: %w", err)
				}
				writer = w
			}
			// PII Phase 1.5: redact row before writing to the chunk so
			// backups stored on disk are PII-clean. nil/empty Registry
			// is a zero-cost passthrough.
			//
			// streamID is empty for full-backup runs (which are one-shot
			// snapshots); the per-row randomize:* seed is determined
			// purely by table + column + PK values, so re-running a
			// full backup against the same source produces the same
			// redacted values. pkColumns from the table descriptor gates
			// randomize:* on no-PK tables (the strategy refuses cleanly).
			if err := redactRow(b.Redactor, table.Schema, table.Name, row, cols, tablePKColumns(table), ""); err != nil {
				return fmt.Errorf("redact row: %w", err)
			}
			if err := writer.WriteRow(row, cols); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			rowsTotal++
			if writer.RowCount() >= int64(chunkRows) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// chunkFilePath returns the conventional path of chunk index for
// table within the backup store. Forward-slash separated, kept short
// because the path lands in the manifest verbatim.
func chunkFilePath(table *ir.Table, idx int) string {
	name := table.Name
	if table.Schema != "" {
		// Schema-qualified tables get a `<schema>__<name>` directory
		// to avoid collisions when two PG schemas have a same-named
		// table. Underscore-double rather than slash because some
		// object stores reserve specific path shapes.
		name = table.Schema + "__" + table.Name
	}
	return path.Join("chunks", name, fmt.Sprintf("%s-%d.jsonl.gz", name, idx))
}

// nonGeneratedTableColumns returns the columns of table that are NOT
// generated columns. Mirrors the engine row readers' filtering — the
// row stream omits generated values, so the chunk writer must skip
// them too or it'd emit nil for every generated column slot. The
// restore path does the same skip on insert.
func nonGeneratedTableColumns(table *ir.Table) []*ir.Column {
	out := make([]*ir.Column, 0, len(table.Columns))
	for _, c := range table.Columns {
		if c.IsGenerated() {
			continue
		}
		out = append(out, c)
	}
	return out
}

// writeManifest serialises manifest as JSON (indented for human
// readability) and writes it to the store. The manifest is the
// public contract; readability matters.
func writeManifest(ctx context.Context, store ir.BackupStore, manifest *ir.Manifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, ManifestFileName, bytes.NewReader(b))
}

// readManifestIfPresent returns the prior manifest if one exists in
// store, or (nil, nil) when no manifest is on disk. Distinct from
// [readManifest] which surfaces a NotFound as an error: resume code
// needs to distinguish "no prior backup" (fresh start) from "prior
// manifest is unreadable" (operator-actionable failure).
func readManifestIfPresent(ctx context.Context, store ir.BackupStore) (*ir.Manifest, error) {
	exists, err := store.Exists(ctx, ManifestFileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return readManifest(ctx, store)
}

// priorResumableTables returns the prior manifest's table list when
// the manifest is in a state where its entries are eligible for
// resume. Returns nil for a nil manifest.
//
// Only "in_progress" manifests carry per-table progress; "complete"
// manifests are the same shape but get rejected up-stack unless
// --force-overwrite is set, in which case the caller has already
// nilled out prior. Empty PartialState (Phase 1 manifests) is treated
// the same as "complete" — those manifests are immutable backups, not
// resume points.
//
// Renamed from priorCompletedTables in v0.16.1: per-chunk-resume needs
// partial-table entries too, not just fully-completed-tables. Callers
// must check chunk-level state themselves before reusing entries.
func priorResumableTables(prior *ir.Manifest) []*ir.TableManifest {
	if prior == nil || prior.PartialState != ir.BackupStateInProgress {
		return nil
	}
	return prior.Tables
}

// refuseAnchoredResumeOnSchemaDrift refuses an anchored resume when the
// source schema's fingerprint no longer matches the interrupted
// attempt's recorded schema (task #42, ADR-0085). Adoption claims
// "kept chunks + CDC replay from the adopted anchor converge to source
// state" — DDL between the two attempts breaks that claim: the kept
// chunks were shaped by the old schema, the manifest would record the
// new one, and the replay window carries events under a schema the
// manifest never saw. Both schemas here are post-filter, so a changed
// --tables filter between attempts trips this too — equally unsound.
func refuseAnchoredResumeOnSchemaDrift(current, prior *ir.Schema) error {
	curHash, err := manifestSchemaFingerprint(current)
	if err != nil {
		return fmt.Errorf("backup: resume: fingerprint current schema: %w", err)
	}
	priorHash, err := manifestSchemaFingerprint(prior)
	if err != nil {
		return fmt.Errorf("backup: resume: fingerprint prior schema: %w", err)
	}
	if curHash == priorHash {
		return nil
	}
	return fmt.Errorf(
		"backup: resume: the source schema changed since the interrupted attempt (fingerprint %s != recorded %s) — "+
			"resuming would pair the prior attempt's table chunks with a schema they were not read under, corrupting the chain's replay claim. "+
			"Pass --force-overwrite to discard the interrupted attempt and take a fresh full backup (use the same --tables filter as the prior attempt if the schema itself did not change)",
		curHash[:12], priorHash[:12],
	)
}

// manifestSchemaFingerprint hashes s in the MANIFEST's domain: it
// JSON-round-trips the schema before hashing. Named wart: the IR's
// decode hooks materialize concrete zero values for fields a freshly-
// read schema leaves nil (e.g. a nil Column.Default decodes to an
// explicit kind=None value that re-marshals non-empty), so
// [ir.ComputeSchemaHash] over a reader-fresh schema does NOT equal the
// hash of the same schema after it has been stored in (and re-read
// from) a manifest. The drift guard compares a fresh read against
// prior.Schema — which IS round-tripped — so both sides must be
// normalized into the round-tripped domain first or every resume would
// false-positive as drift. Pinned by
// [TestBackup_ResumeAdoptsPriorAnchor] (identical schema across
// attempts must not refuse).
func manifestSchemaFingerprint(s *ir.Schema) (string, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	var rt ir.Schema
	if err := json.Unmarshal(raw, &rt); err != nil {
		return "", fmt.Errorf("round-trip decode: %w", err)
	}
	return ir.ComputeSchemaHash(&rt)
}

// refuseKeylessRestreamOnAnchoredResume refuses an anchored resume when
// any table that must be (re-)streamed THIS run is truly keyless — no
// PRIMARY KEY and no all-NOT-NULL plain-column UNIQUE index
// ([ir.TableReplayIdempotent]). Such a table's chunks are read at this
// run's later snapshot and therefore OVERLAP the chain's replay window
// (adopted anchor, stop]; the chain appliers' keyless fallback is plain
// INSERT (ADR-0010), so the overlap would duplicate rows silently.
// Kept tables may be keyless — they are exact at the adopted anchor,
// no overlap.
func refuseKeylessRestreamOnAnchoredResume(tasks []backupTableTask) error {
	var keyless []string
	for _, task := range tasks {
		if !ir.TableReplayIdempotent(task.table) {
			keyless = append(keyless, task.table.Name)
		}
	}
	if len(keyless) == 0 {
		return nil
	}
	return fmt.Errorf(
		"backup: resume: table(s) %s must be (re-)streamed but have no PRIMARY KEY and no non-null UNIQUE index. "+
			"A resumed backup adopts the interrupted attempt's anchor, so these tables' chunks would overlap the chain's replay window — "+
			"and replaying onto a keyless table falls back to plain INSERT (ADR-0010), duplicating the overlapping rows. "+
			"Add a PRIMARY KEY or a NOT NULL UNIQUE index, exclude the table(s), or pass --force-overwrite to discard the interrupted attempt and start fresh",
		strings.Join(keyless, ", "),
	)
}

// backupStoreDescriptor returns a short human-readable identifier of
// the destination store for log lines. LocalStore reports the absolute
// root directory; BlobStore reports the (annotated) URL with credentials
// stripped. Other implementations of [ir.BackupStore] fall back to
// `<unknown-store>` so log shape is stable.
func backupStoreDescriptor(s ir.BackupStore) string {
	type rooted interface{ Root() string }
	type urled interface{ URL() string }
	if r, ok := s.(rooted); ok {
		return r.Root()
	}
	if u, ok := s.(urled); ok {
		return u.URL()
	}
	return "<unknown-store>"
}

// tableChunksAllPresent verifies every chunk listed in entry is still
// present in store. Used when resuming to confirm a "fully completed
// table from a prior run" is still backed by its bytes — operators
// who manually deleted chunks between runs trip this and the table
// gets re-streamed instead of silently appearing in the manifest with
// no actual data behind it.
func tableChunksAllPresent(ctx context.Context, store ir.BackupStore, entry *ir.TableManifest) (bool, error) {
	for _, c := range entry.Chunks {
		exists, err := store.Exists(ctx, c.File)
		if err != nil {
			return false, fmt.Errorf("exists %q: %w", c.File, err)
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// tableManifestFullyComplete reports whether a prior table entry
// represents a fully-completed table backup (so the resume can skip
// the table whole) vs a mid-stream partial state (resume per-chunk
// inside the table).
//
// Two conditions both required: (1) the entry's Partial flag is false
// (the row stream reached natural EOF when the entry was written; the
// flag is omitted in pre-v0.16.1 manifests, defaulting to false there
// too — those manifests only ever persisted entries at table-completion
// boundaries, so the default is correct); (2) every chunk listed is
// still present on the store (an operator who manually deleted chunks
// between runs forces a re-stream).
func tableManifestFullyComplete(ctx context.Context, store ir.BackupStore, entry *ir.TableManifest) (bool, error) {
	if entry.Partial {
		return false, nil
	}
	return tableChunksAllPresent(ctx, store, entry)
}

// chunkAlreadyMatches reports whether key already exists in store and
// hashes to expectedSHA256. Returns (false, nil) when the key is
// absent (the common cold-start case). Returns (false, nil) when the
// key is present but mismatches — the caller treats that as a stale
// partial-upload and overwrites.
//
// The fetch-and-hash cost is bounded to chunk size (default 100k rows,
// typically a few MB compressed); cheap relative to a full re-upload
// of the same chunk over a slow link.
func chunkAlreadyMatches(ctx context.Context, store ir.BackupStore, key, expectedSHA256 string) (bool, error) {
	exists, err := store.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get %q: %w", key, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := hashChunkBytes(ctx, rc)
	if err != nil {
		return false, fmt.Errorf("hash %q: %w", key, err)
	}
	return got == expectedSHA256, nil
}

// ReadRootManifest loads and decodes the chain-root manifest at
// [ManifestFileName]. Returns (nil, nil) when no manifest is present
// at the path (used by CLI helpers that want to inspect a chain's
// encryption header before constructing a restore-side envelope).
//
// Distinct from [readManifest] which surfaces a NotFound as an error.
func ReadRootManifest(ctx context.Context, store ir.BackupStore) (*ir.Manifest, error) {
	return readManifestIfPresent(ctx, store)
}

// chainRootEncryption returns the chain-root's [ir.ChainEncryption]
// when an extending writer (incremental / stream) needs to align its
// envelope. parent's ChainEncryption is returned directly when set
// (the common case: parent is a full carrying the chain header).
// When parent is itself an incremental (no ChainEncryption), the
// chain root manifest is read from store and its ChainEncryption is
// returned.
//
// Read errors are swallowed (returns nil) — the alignment logic
// already handles a nil ChainEncryption shape gracefully and a noisy
// store read at this point would mask the simpler "parent is
// plaintext" path.
func chainRootEncryption(ctx context.Context, store ir.BackupStore, parent *ir.Manifest) *ir.ChainEncryption {
	if parent != nil && parent.ChainEncryption != nil {
		return parent.ChainEncryption
	}
	root, err := readManifestIfPresent(ctx, store)
	if err != nil || root == nil {
		return nil
	}
	return root.ChainEncryption
}

// readManifest loads and decodes the manifest from store. Used by
// both restore and `sluice backup verify`.
func readManifest(ctx context.Context, store ir.BackupStore) (*ir.Manifest, error) {
	rc, err := store.Get(ctx, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read manifest body: %w", err)
	}
	var m ir.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if m.FormatVersion > ir.BackupFormatVersion {
		return nil, fmt.Errorf("backup: manifest format version %d is newer than this build supports (%d); upgrade sluice",
			m.FormatVersion, ir.BackupFormatVersion)
	}
	return &m, nil
}

// setupChainEncryption configures the manifest's [ir.ChainEncryption]
// header and returns the chain-level CEK (per-chain mode) or nil
// (per-chunk mode). When encryption is disabled (b.Encryption == nil),
// returns nil with no manifest mutation.
//
// Resume safety: if prior is a Phase 6 in-progress encrypted manifest,
// the chain-level CEK is unwrapped from prior.ChainEncryption.WrappedCEK
// using the supplied envelope, so per-chain mode resumes write
// additional chunks against the same CEK as the original run. This is
// the load-bearing equivalent of "open the prior CEK to keep encrypting
// consistently with the chain so far."
func (b *Backup) setupChainEncryption(manifest, prior *ir.Manifest) ([]byte, error) {
	if b.Encryption == nil {
		return nil, nil
	}
	enc := b.Encryption
	if enc.Envelope == nil {
		return nil, errors.New("backup: encryption envelope is nil")
	}
	mode := enc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	chainEnc := &ir.ChainEncryption{
		Algorithm: crypto.AlgorithmAESGCM,
		Mode:      mode,
		KEKMode:   enc.Envelope.Mode(),
		KEKRef:    enc.KEKRef,
	}
	// Passphrase mode: record the Argon2id params so a restore-side
	// envelope can re-derive the same KEK.
	if pe, ok := enc.Envelope.(*crypto.PassphraseEnvelope); ok {
		p := pe.Params()
		chainEnc.Argon2id = &ir.Argon2idParams{
			Salt:        p.Salt,
			Memory:      p.Memory,
			Iterations:  p.Iterations,
			Parallelism: p.Parallelism,
			KeyLen:      p.KeyLen,
		}
	}

	// Per-chunk mode: chain-level WrappedCEK stays empty; each chunk
	// generates its own CEK.
	if mode == crypto.EncryptModePerChunk {
		manifest.ChainEncryption = chainEnc
		return nil, nil
	}

	// Per-chain mode (default): generate a fresh CEK and wrap it.
	// On resume of an in-progress encrypted manifest we re-use the
	// prior wrap so chunks already on disk decrypt cleanly.
	var chainCEK []byte
	if prior != nil && prior.ChainEncryption != nil &&
		len(prior.ChainEncryption.WrappedCEK) > 0 &&
		prior.ChainEncryption.Mode == crypto.EncryptModePerChain {
		// Bug 43: rebuild the envelope against the prior chain's
		// recorded Argon2id salt before unwrapping. CLI envelopes
		// are minted with a fresh salt; unwrap would fail with an
		// auth-tag mismatch without this rebind.
		if err := enc.rebindForChain(prior.ChainEncryption.Argon2id); err != nil {
			return nil, fmt.Errorf("rebuild envelope for prior chain: %w", err)
		}
		// Unwrap to recover the in-flight CEK.
		cek, err := enc.Envelope.UnwrapCEK(prior.ChainEncryption.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("unwrap prior chain cek: %w", err)
		}
		chainCEK = cek
		chainEnc.WrappedCEK = prior.ChainEncryption.WrappedCEK
		// Preserve the Argon2id params from the prior manifest so the
		// salt remains stable across resume (the freshly-derived
		// envelope was built from the operator's passphrase + the
		// recorded salt; either source recovers the same KEK).
		if prior.ChainEncryption.Argon2id != nil {
			chainEnc.Argon2id = prior.ChainEncryption.Argon2id
		}
	} else {
		cek, err := crypto.GenerateCEK()
		if err != nil {
			return nil, fmt.Errorf("generate chain cek: %w", err)
		}
		wrapped, err := enc.Envelope.WrapCEK(cek)
		if err != nil {
			return nil, fmt.Errorf("wrap chain cek: %w", err)
		}
		chainCEK = cek
		chainEnc.WrappedCEK = wrapped
	}
	manifest.ChainEncryption = chainEnc
	return chainCEK, nil
}

// resolveChunkCEK returns the (cek, wrappedCEK) pair to use for the
// next chunk. Per-chain mode reuses the chain-level CEK and records an
// empty wrapped value (the ChunkEncryption.WrappedCEK field). Per-chunk
// mode generates a fresh CEK + wrap on every call.
//
// Returns (nil, nil, nil) when encryption is disabled — caller passes
// nil cek to newChunkWriter for the plaintext path.
func (b *Backup) resolveChunkCEK(chainCEK []byte) (cek, wrapped []byte, err error) {
	if b.Encryption == nil {
		return nil, nil, nil
	}
	mode := b.Encryption.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		return chainCEK, nil, nil
	}
	// Per-chunk: fresh CEK + fresh wrap.
	cek, err = crypto.GenerateCEK()
	if err != nil {
		return nil, nil, fmt.Errorf("generate chunk cek: %w", err)
	}
	wrapped, err = b.Encryption.Envelope.WrapCEK(cek)
	if err != nil {
		return nil, nil, fmt.Errorf("wrap chunk cek: %w", err)
	}
	return cek, wrapped, nil
}
