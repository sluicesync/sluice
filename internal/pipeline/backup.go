// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Backup orchestrator. Phase 1 of the logical-backup feature
// (`docs/dev/design-logical-backups.md`): full snapshot to a
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
// The orchestrator is sequential per table (Phase 2 will add
// parallel reads — same shape as the parallel bulk-copy path —
// once cloud backends with multipart upload land).
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
//     new run resumes: tables already fully written in the prior run
//     are kept verbatim (their chunks are HEAD-checked for presence),
//     and the run picks up at the next un-completed table. Within a
//     table, chunk writes that find an existing object with a
//     matching SHA-256 are skipped; mismatches overwrite (treating
//     the prior bytes as a corrupted partial upload).
//
// The manifest is committed to the store after every table completes,
// so a crashed run leaves at most one table's worth of work to redo.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
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
	// `docs/dev/design-logical-backups-phase-6.md` for the trade-off.
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

	// ForceOverwrite, when true, lets a re-run replace a previously-
	// completed backup at the same destination. Without it, finding a
	// `partial_state == "complete"` manifest at the destination is an
	// operator-actionable error. This is the analog of
	// `migrate --reset-target-data` for the backup verb. In-progress
	// manifests always resume regardless of this flag.
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

	// 0. Resume detection: if a manifest already exists in the store,
	// decide whether to fresh-start, resume, or refuse.
	prior, priorErr := readManifestIfPresent(ctx, b.Store)
	if priorErr != nil {
		return fmt.Errorf("backup: inspect existing manifest: %w", priorErr)
	}
	if prior != nil {
		switch prior.PartialState {
		case ir.BackupStateInProgress:
			// Bug 34a: emit a clear "resuming" log line so operators
			// can see resume happened. The detailed per-table fan-out
			// (which tables to skip vs resume) follows once the schema
			// is read; this is the headline.
			slog.InfoContext(
				ctx, "resuming from partial backup",
				slog.String("backup_dir", backupStoreDescriptor(b.Store)),
				slog.Int("tables_in_prior_manifest", len(prior.Tables)),
				slog.Time("prior_created_at", prior.CreatedAt),
			)
		case ir.BackupStateComplete, "":
			if !b.ForceOverwrite {
				return fmt.Errorf("backup: a completed backup already exists at this destination (created %s); pass --force-overwrite to replace it",
					prior.CreatedAt.UTC().Format(time.RFC3339))
			}
			slog.InfoContext(
				ctx, "backup: --force-overwrite set; replacing existing complete backup",
				slog.Time("prior_created_at", prior.CreatedAt),
			)
			prior = nil // discard so we start from scratch
		}
	}

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
	rr, snapshotPos, snapshotCloser, err := b.openSnapshotOrFallback(ctx)
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

	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		var priorTable *ir.TableManifest
		if existing, ok := priorTables[key]; ok {
			full, err := tableManifestFullyComplete(ctx, b.Store, existing)
			if err != nil {
				return fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if full {
				slog.InfoContext(
					ctx, "skipping table — already complete in partial backup",
					slog.String("table", table.Name),
					slog.Int64("rows", existing.RowCount),
					slog.Int("chunks", len(existing.Chunks)),
				)
				manifest.Tables = append(manifest.Tables, existing)
				continue
			}
			// Partial: pass the prior entry through so backupTable can
			// per-chunk-skip already-uploaded chunks (Bug 34b).
			priorTable = existing
			slog.InfoContext(
				ctx, "resuming table mid-stream — partial chunks present in prior backup",
				slog.String("table", table.Name),
				slog.Int("prior_chunks", len(existing.Chunks)),
				slog.Bool("prior_partial_flag", existing.Partial),
			)
		}
		// backupTable stages its returned entry into manifest.Tables
		// up-front so per-chunk checkpoints record progress as it
		// accrues (Bug 34b's per-chunk granularity). The orchestrator
		// must NOT append again here.
		if _, err := b.backupTable(ctx, rr, table, chunkRows, priorTable, manifest, chainCEK); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", table.Name, err))
		}

		// Per-table checkpoint: commit the manifest with PartialState
		// = "in_progress" after each table so a crash before the next
		// table loses at most one table of work. (Per-chunk checkpoints
		// inside backupTable already capture sub-table progress; this
		// final per-table commit ensures the manifest is up to date
		// once the table fully closes — the per-chunk checkpoint after
		// the last chunk usually has the same effect, but empty-table
		// runs and tables whose row count is an exact chunk multiple
		// rely on this final write.)
		if err := writeManifest(ctx, b.Store, manifest); err != nil {
			return fmt.Errorf("backup: checkpoint manifest after %q: %w", table.Name, err)
		}
	}

	// 4.5. Record EndPosition. Two paths:
	//
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
	if snapshotPos != nil {
		manifest.EndPosition = *snapshotPos
		slog.InfoContext(
			ctx, "backup: recorded end position (snapshot-anchored)",
			slog.String("engine", manifest.SourceEngine),
			slog.String("position_token", snapshotPos.Token),
		)
	} else if err := b.captureEndPosition(ctx, manifest); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: capture end position: %w", err))
	}

	// Compute BackupID once EndPosition is known. The id is used by
	// Phase 3 incrementals to link via ParentBackupID; pre-Phase-3
	// fulls leave it empty, which the chain-restore walker tolerates by
	// computing on demand. Filling it in here means the full's manifest
	// carries the same id the incremental would compute when chaining.
	manifest.BackupID = ir.ComputeBackupID(manifest)

	// 5. Final manifest write — flip to complete.
	manifest.PartialState = ir.BackupStateComplete
	if err := writeManifest(ctx, b.Store, manifest); err != nil {
		return fmt.Errorf("backup: write final manifest: %w", err)
	}
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
func (b *Backup) openSnapshotOrFallback(ctx context.Context) (ir.RowReader, *ir.Position, func(), error) {
	if opener, ok := b.Source.(ir.BackupSnapshotOpener); ok {
		snap, err := opener.OpenBackupSnapshot(ctx, b.SourceDSN, b.SlotName)
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
			return snap.Rows, &pos, cleanup, nil
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
			slog.String("see_also", "v0.17.2 release notes; docs/dev/design-logical-backups-phase-3.md"),
		)
	} else {
		// Engine doesn't implement BackupSnapshotOpener at all.
		// Surface the gap loudly — operators consuming the chain
		// need to know writes during the backup window are not
		// guaranteed to be captured. The recommended mitigation
		// (pair backups with continuous `sluice sync start`) is the
		// same one the v0.17.2 release notes called out.
		slog.WarnContext(
			ctx, "backup: engine does not implement BackupSnapshotOpener; falling back to non-snapshot row reads",
			slog.String("engine", b.Source.Name()),
			slog.String("impact", "chains rooted in this full will have a during-backup write-window gap"),
			slog.String("mitigation", "pair backups with continuous `sluice sync start` so the live stream captures every write"),
			slog.String("see_also", "v0.17.2 release notes; docs/dev/design-logical-backups-phase-3.md"),
		)
	}

	rr, err := b.Source.OpenRowReader(ctx, b.SourceDSN)
	if err != nil {
		return nil, nil, func() {}, wrapWithHint(PhaseConnect, fmt.Errorf("backup: open source row reader: %w", err))
	}
	cleanup := func() { closeIf(rr) }
	return rr, nil, cleanup, nil
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

// backupTable streams every row of table from rr through one or more
// chunk files in the Store, returning the manifest entry that
// describes them.
//
// One subtle point worth flagging: the chunk-roll boundary is checked
// AFTER each row write. A table whose row count is an exact multiple
// of chunkRows ends with a fully-written chunk and no extra trailing
// chunk. Empty tables produce zero chunks (the manifest entry has
// RowCount=0 and Chunks=nil) — keeps the storage layout tidy.
//
// Bug 34b: when priorTable is non-nil, its chunk entries are checked
// before each new chunk gets a writer. If chunk N's prior info exists,
// the recorded chunk is still on the store, AND its on-store SHA-256
// matches the manifest's recorded SHA-256, the chunk is skipped — the
// orchestrator advances the row cursor by chunk N's recorded RowCount
// (reading-and-discarding) without opening a writer or hitting Put.
// fullManifest, when non-nil, is the in-flight manifest to checkpoint
// after each newly-written chunk so a mid-table crash leaves a manifest
// recording exactly which chunks completed.
func (b *Backup) backupTable(
	ctx context.Context,
	rr ir.RowReader,
	table *ir.Table,
	chunkRows int,
	priorTable *ir.TableManifest,
	fullManifest *ir.Manifest,
	chainCEK []byte,
) (*ir.TableManifest, error) {
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	cols := nonGeneratedTableColumns(table)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	entry := &ir.TableManifest{
		Schema:  table.Schema,
		Name:    table.Name,
		Partial: true, // flips to false on natural EOF; per-chunk checkpoints persist it as true
	}
	// Stage the entry into the in-flight manifest now so per-chunk
	// checkpoints capture progress as it accrues. The same pointer is
	// returned to the orchestrator at the end.
	if fullManifest != nil {
		fullManifest.Tables = append(fullManifest.Tables, entry)
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
		entry.Chunks = append(entry.Chunks, ci)
		writer = nil
		buf = nil
		curWrappedCEK = nil
		chunkIdx++
		// Per-chunk checkpoint: commit the manifest now so a mid-table
		// crash (or kill) before the next table starts leaves an
		// up-to-date record of exactly which chunks completed. The cost
		// is one extra Put per chunk; benefit is the per-chunk skip on
		// resume sees the right state. Bug 34b.
		if fullManifest != nil {
			if err := writeManifest(ctx, b.Store, fullManifest); err != nil {
				return fmt.Errorf("checkpoint manifest after chunk %d: %w", chunkIdx-1, err)
			}
		}
		return nil
	}

	// trySkipChunk inspects priorTable for a recorded entry at chunk
	// chunkIdx. If the recorded entry is still on the store and its
	// on-store bytes hash to the recorded SHA-256, the chunk is reused
	// verbatim — the orchestrator advances the row cursor over chunk's
	// rows without opening a writer or issuing a Put.
	//
	// Returns (skipped, nil) on a successful skip; (false, nil) when
	// either no prior entry exists or the prior bytes mismatch (caller
	// then writes the chunk normally — overwrite-on-mismatch).
	trySkipChunk := func() (bool, error) {
		if priorTable == nil || chunkIdx >= len(priorTable.Chunks) {
			return false, nil
		}
		priorChunk := priorTable.Chunks[chunkIdx]
		if priorChunk == nil || priorChunk.SHA256 == "" {
			return false, nil
		}
		matches, err := chunkAlreadyMatches(ctx, b.Store, priorChunk.File, priorChunk.SHA256)
		if err != nil {
			return false, fmt.Errorf("inspect prior chunk %q: %w", priorChunk.File, err)
		}
		if !matches {
			// Either the chunk is missing on the store or its bytes
			// don't match the recorded SHA-256 (corrupted partial
			// upload). Surface the second case loudly per the loud-
			// failure tenet; either way the chunk gets re-written.
			exists, existsErr := b.Store.Exists(ctx, priorChunk.File)
			if existsErr == nil && exists {
				slog.WarnContext(
					ctx, "prior chunk SHA-256 mismatch — overwriting on resume",
					slog.String("table", table.Name),
					slog.Int("chunk", chunkIdx),
					slog.String("path", priorChunk.File),
				)
			}
			return false, nil
		}
		// Reuse the prior entry verbatim and advance the cursor.
		entry.Chunks = append(entry.Chunks, priorChunk)
		// Discard the rows the skipped chunk covered so the row stream
		// stays aligned with the chunk index. The chunk-row-range is
		// deterministic ([N*chunkRows, (N+1)*chunkRows) in PK order),
		// so reading-and-discarding priorChunk.RowCount rows from the
		// channel preserves alignment for the next chunk.
		discardN := priorChunk.RowCount
		for discardN > 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case _, ok := <-rows:
				if !ok {
					return false, fmt.Errorf("row stream ended early while skipping chunk %d (had %d rows of %d to discard)", chunkIdx, priorChunk.RowCount-discardN, priorChunk.RowCount)
				}
				discardN--
			}
		}
		rowsTotal += priorChunk.RowCount
		slog.InfoContext(
			ctx, "skipping chunk — already complete in partial backup",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIdx),
			slog.Int64("rows", priorChunk.RowCount),
			slog.String("sha256", priorChunk.SHA256),
		)
		chunkIdx++
		return true, nil
	}

	// Drive the per-chunk skip up-front before the row loop opens a
	// new writer for chunk chunkIdx. We loop because successive chunks
	// may all be skippable.
	skipUntilWrite := func() error {
		for {
			skipped, err := trySkipChunk()
			if err != nil {
				return err
			}
			if !skipped {
				return nil
			}
		}
	}

	if err := skipUntilWrite(); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case row, ok := <-rows:
			if !ok {
				if err := flush(); err != nil {
					return nil, err
				}
				// Surface any sticky error captured by the reader's
				// streaming goroutine (Bug 68 loud-failure gate; now a
				// first-class [ir.RowReader.Err] surface, no longer an
				// optional type assertion).
				if err := readerStreamErr(rr, table); err != nil {
					return nil, err
				}
				entry.RowCount = rowsTotal
				entry.Partial = false // table EOF reached naturally; flip the partial flag off
				slog.InfoContext(
					ctx, "backup: table complete",
					slog.String("table", table.Name),
					slog.Int64("rows", rowsTotal),
					slog.Int("chunks", len(entry.Chunks)),
				)
				return entry, nil
			}
			if writer == nil {
				buf = &bytes.Buffer{}
				cek, wrapped, err := b.resolveChunkCEK(chainCEK)
				if err != nil {
					return nil, fmt.Errorf("resolve chunk cek: %w", err)
				}
				curWrappedCEK = wrapped
				w, err := newChunkWriter(buf, colNames, cek, b.Codec)
				if err != nil {
					return nil, fmt.Errorf("open chunk: %w", err)
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
				return nil, fmt.Errorf("redact row: %w", err)
			}
			if err := writer.WriteRow(row, cols); err != nil {
				return nil, fmt.Errorf("write row: %w", err)
			}
			rowsTotal++
			if writer.RowCount() >= int64(chunkRows) {
				if err := flush(); err != nil {
					return nil, err
				}
				// After flushing, the next chunk index might also have
				// been pre-uploaded in a prior run — try to skip again.
				if err := skipUntilWrite(); err != nil {
					return nil, err
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
