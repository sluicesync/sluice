// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Backup orchestrator. Phase 1 of the logical-backup feature
// (`docs/dev/design/logical-backups.md`): full snapshot to a
// [irbackup.Store], one chunk file per N rows per table, plus a
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
// Progress is checkpointed after every chunk and every table, so a
// crashed run leaves at most tableParallelism tables' worth of
// in-flight work to redo (one table's, when the sweep runs serial).
// On append-capable stores each checkpoint is one appended line in the
// `manifest.progress.jsonl` sidecar (O(1) — ADR-0086) and the
// in-progress truth is base manifest + sidecar replay; stores without
// appends keep the legacy full-manifest rewrite per checkpoint.
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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/redact"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// DefaultBackupChunkRows is the per-chunk row count when [Backup]'s
// ChunkRows is left at zero. 100,000 rows is the proto-ADR default;
// big enough to amortise per-chunk JSON-Lines + gzip overhead, small
// enough that a single chunk fits comfortably in memory at restore
// time on commodity hardware. Operators tune via --chunk-size.
const DefaultBackupChunkRows = 100_000

// ManifestProgressFileName is the filename of the in-progress
// checkpoint sidecar next to the manifest (ADR-0086): one appended
// JSON line per chunk-finished / table-finished event, so checkpoints
// are O(1) instead of rewriting the whole manifest. Present only while
// a sidecar-mode backup is in progress — the final manifest write
// folds the progress back into [lineage.ManifestFileName] and deletes it.
const ManifestProgressFileName = "manifest.progress.jsonl"

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

	// Store is the [irbackup.Store] backup chunks and manifest are
	// written to. Required.
	Store irbackup.Store

	// Filter selects which source tables participate in the backup.
	// Empty filter (zero value) keeps every table the schema reader
	// returns.
	Filter migcore.TableFilter

	// ChunkRows is the per-chunk row count. Zero falls back to
	// [DefaultBackupChunkRows]. The writer rolls over to a new chunk
	// file whenever the current one hits this row count.
	ChunkRows int

	// SluiceVersion is the build identifier of the running binary,
	// recorded in the manifest. Optional — empty leaves the field
	// blank in the manifest.
	SluiceVersion string

	// SlotName is the source-side replication-slot name to record on
	// the manifest's [irbackup.Manifest.EndPosition] for engines with a slot
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
	// [irbackup.SnapshotOptions].
	ChainSlot bool

	// StrictFloat makes a `backup full` on a VStream (PlanetScale/Vitess)
	// source demand EXACT single-precision FLOAT or FAIL — "exact, or fail;
	// never rounded/skewed" (roadmap open-bug 2026-07-09). It still archives
	// exact via the default re-read for every repairable table under the
	// row cap; it REFUSES loudly (SLUICE-E-VSTREAM-FLOAT-LOSSY, exit 3) for
	// a FLOAT column that cannot be re-read exactly — an un-repairable
	// (keyless / float-PK-only) table refuses UPFRONT, an over-cap table
	// refuses inside the reader. OPT-IN: the default (false) instead falls
	// back to a WARN + rounded archive for those un-exactable tables. Inert
	// on every non-VStream source.
	StrictFloat bool

	// NoFloatExactReread opts OUT of the default exact FLOAT re-read on a
	// VStream `backup full` (roadmap open-bug 2026-07-09): by default the
	// backup re-reads single-precision FLOAT columns exactly and patches the
	// archived rows (exact float32, at the cost of a bounded within-row
	// temporal skew — see [floatExactPatchReader]). Set this to archive the
	// COPY's display-rounded values instead — a rounded-but-perfectly-
	// consistent snapshot, for operators who value within-row consistency
	// over FLOAT precision. OPT-OUT-named so the Go zero value (false) keeps
	// the exact re-read ON (the correct default) for every non-CLI
	// construction. Mutually distinct from --strict-float (which refuses).
	// Inert on every non-VStream source.
	NoFloatExactReread bool

	// FloatRereadMaxRows caps the per-table PK→exact-FLOAT map the default
	// backup re-read buffers, so the repair is BOUNDED-memory (a whole-table
	// buffer would OOM on a large FLOAT table — the ADR-0071 tenet). A table
	// whose re-read would exceed this many distinct rows falls back loudly
	// (default: WARN + rounded; --strict-float: refuse) rather than
	// buffering unbounded. 0 (the zero value) → [defaultFloatRereadMaxRows]
	// (2,000,000 rows ≈ a few hundred MB worst case). Inert on the sync
	// cold-start path (which is already bounded — it streams the re-read
	// cursor-paginated and UPDATEs, never buffering a map).
	FloatRereadMaxRows int

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

	// BulkParallelism caps how many PK-range readers stream ONE large
	// table concurrently (ADR-0149, the backup sibling of migrate's
	// --bulk-parallelism / ADR-0019). 0 = auto (min(8, NumCPU), then
	// budget-split); 1 = disable within-table chunking (the pre-ADR-0149
	// behaviour). Engages per table only when the snapshot is shareable
	// via the LAZY importer path ([backupWithinChunkingEligible]), the
	// table's PK shape is chunkable ([migcore.CanParallelChunkTable]) and its
	// estimated row count clears BulkParallelMinRows; everything else
	// streams single-reader with the reason logged. The
	// TableParallelism × BulkParallelism product is bounded by the
	// SOURCE's connection budget (cross-table is satisfied first; the
	// within axis gets the remainder). Orthogonal to ChunkRows, which
	// stays the rows-per-chunk-FILE roll boundary.
	BulkParallelism int

	// BulkParallelMinRows is the estimated-row-count threshold below
	// which a table streams single-reader regardless of BulkParallelism
	// — the same knob and auto-adaptation as migrate's
	// --bulk-parallel-min-rows ([migcore.ResolveBulkParallelMinRows]). 0 = auto
	// (base 80k, dialled down on many-table schemas).
	BulkParallelMinRows int64

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
	// See [lineage.BackupEncryption]. Empty (nil) preserves the plaintext
	// shape — the v0.16.x..v0.21.x default.
	Encryption *lineage.BackupEncryption

	// Sign, when true, writes a detached HMAC-SHA-256 signature over the
	// manifest (`manifest.json.sig`) and the lineage catalog
	// (`lineage.json.sig`), keyed off a key HKDF-derived from the chain
	// KEK (ADR-0154 Phase 1, Option A "HMAC-off-KEK"). Requires
	// [Backup.Encryption] (Phase 1 signs only encrypted chains — there
	// is a KEK to key the HMAC) and a passphrase-mode envelope (a
	// KMS-encrypted chain's KEK never leaves the HSM; KMS Sign is Phase
	// 3). A signed backup is stamped [irbackup.FormatVersionSignedManifest]
	// (proportional per Bug 116 — an unsigned backup stays on 5).
	Sign bool

	// Ed25519Signer, when non-nil, signs the manifest + lineage with an
	// asymmetric Ed25519 keypair (ADR-0154 Phase 2, `--sign-key`) instead
	// of the HMAC-off-KEK default that [Backup.Sign] selects. Unlike Sign,
	// it works on PLAINTEXT AND encrypted chains (the keypair is
	// independent of the encryption keystore) and on KMS-encrypted chains
	// (Ed25519 signs the manifest, KMS wraps the CEK — orthogonal). Set by
	// the CLI from the operator's private key; mutually exclusive with Sign.
	Ed25519Signer *lineage.Signer

	// Summary, when non-nil, collects the end-of-run per-table facts
	// the CLI's `--format json` result envelope renders — filled from
	// the completed manifest's per-table row counts. nil (the zero
	// value) disables the bookkeeping. See [migcore.RunSummary].
	Summary *migcore.RunSummary

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
	// it. Empty resolves to [blobcodec.DefaultCodec] (zstd, v0.67.0+). A
	// one-segment never-rotated lineage takes the same single-segment
	// restore path as a pre-ADR single chain (codec aside).
	Codec blobcodec.Codec

	// Now, when set, overrides the wall-clock-time source for
	// [Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time

	// Progress is the ADR-0155 presentation sink. nil is the
	// [progress.Nop] default: backup's own direct-slog output is the
	// byte-identical non-TTY stream, so the sink adds nothing there. The
	// CLI sets a [progress.TTYSink] only for an interactive terminal.
	Progress progress.Sink
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
	sink := sinkOrNop(b.Progress)
	sink.PhaseStarted(backupPhaseSchema)

	// Managed-host advisories (items 69a/70a). cdc=true — a full backup
	// anchors the chain's CDC position (EndPosition), so retention traps
	// like DigitalOcean's out-of-band binlog purger apply to any
	// incremental that will chain off this run.
	migcore.WarnSourceHostAdvisories(ctx, b.Source, b.SourceDSN, true)

	// Engine-default exclusions (Bug 22): merge in PlanetScale's
	// `_vt_*` shadow tables when the source signals them via the
	// optional [ir.DefaultTableExcluder] surface.
	if eff, added := migcore.EffectiveTableFilter(b.Filter, b.Source, b.SourceDSN); len(added) > 0 {
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
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: open source schema reader: %w", err))
	}
	defer migcore.CloseIf(sr)

	// ADR-0047 tier (b): a PG-source backup may carry uncatalogued
	// extension types verbatim. The restore-target engine is unknown
	// at backup time, so this only enables CAPTURE — the PG-restore-
	// only constraint is enforced later by the recorded lineage marker
	// (lineage.VerbatimExtensionColumnsIn → lineage.Segment) + the loud
	// restore-time engine gate. A non-PG source never enables it.
	migcore.ApplyVerbatimExtensionPassthrough(sr, migcore.VerbatimBackupSourcePG(b.Source))

	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (b.Filter already has engine defaults merged above).
	migcore.ApplyTableScope(sr, b.Filter)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "backup: source schema has no tables; manifest with empty table list will be written")
	}

	// 2. Apply table filter.
	if err := migcore.ApplyTableFilter(ctx, schema, b.Filter); err != nil {
		// migcore.ApplyTableFilter errors when the filter excludes everything.
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
	// [irbackup.SnapshotOpener] — OR engines that implement it but
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
	//
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

	// Pre-build the in-progress manifest with the schema. Tables get
	// appended (or copied from the prior run) as they finish; the
	// manifest is committed after each table completes.
	manifest, err := b.buildInProgressManifest(schema, prior, adopting, resumeAnchor, snapshotPos)
	if err != nil {
		return err
	}

	// Phase 6.1: when encryption is enabled, generate the chain-level
	// CEK (per-chain mode) up-front, wrap it via the envelope, and
	// stamp the manifest's [irbackup.ChainEncryption] header. Per-chunk
	// mode leaves the chain-level WrappedCEK empty; each chunk
	// generates its own CEK at write time.
	chainCEK, err := b.setupChainEncryption(manifest, prior)
	if err != nil {
		return fmt.Errorf("backup: setup encryption: %w", err)
	}
	// ADR-0154 Phase 2: a PLAINTEXT Ed25519-signed backup (--sign-key, no
	// --encrypt) is stamped the signed-manifest version here — the
	// encrypted path already stamped it inside setupChainEncryption. This
	// is the plaintext-signing enablement Phase 1 lacked (it had no key to
	// sign a plaintext manifest with).
	if b.Ed25519Signer != nil && b.Encryption == nil {
		manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionSignedManifest)
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
	if err := b.logResumePlan(ctx, schema, prior, priorTables); err != nil {
		return err
	}

	// Pre-stage every table's manifest entry in schema order, then sweep
	// the non-complete ones through the bounded cross-table pool
	// (ADR-0084). Pre-staging keeps the manifest's table order
	// deterministic (== schema order) regardless of worker completion
	// order; the committer serializes every entry mutation + manifest
	// Put. Per-chunk + per-table checkpoints live inside backupTable /
	// the committer now — a crash leaves at most tableParallelism tables
	// with partial chunk lists to redo. On append-capable stores the
	// checkpoints are O(1) sidecar deltas (ADR-0086) and the in-progress
	// manifest is stamped with the sidecar-layout format version.
	committer, err := newManifestCommitter(b.Store, manifest)
	if err != nil {
		return err
	}
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
	if err := committer.commitBase(ctx); err != nil {
		return fmt.Errorf("backup: write in-progress manifest: %w", err)
	}
	if err := snap.Commit(ctx); err != nil {
		return fmt.Errorf("backup: persist chain resources: %w", err)
	}

	tableParallelism, requestedWithin, err := b.resolveBackupTableParallelism(ctx, snap, len(tasks))
	if err != nil {
		return err
	}
	// ADR-0149: within-table read chunking — nil when the mode can't
	// supply per-range snapshot readers (eager MySQL / non-snapshot
	// fallback) or --bulk-parallelism resolved to 1.
	within := b.resolveBackupWithinTable(ctx, snap, tableParallelism, requestedWithin, len(tasks))
	withinParallelism := 1
	if within != nil {
		withinParallelism = within.parallelism
	}
	factory, factoryCleanup, err := b.openBackupReaderFactory(ctx, snap, tableParallelism, withinParallelism)
	if err != nil {
		return err
	}
	defer factoryCleanup()

	// EAGER (MySQL ADR-0088) peers seed the pool's reusable free-reader
	// channel; the LAZY (PG) path leaves them nil and mints via factory.
	// Only when parallelism actually engaged — a collapsed-to-serial
	// gate runs one reader (the primary).
	var peers []ir.RowReader
	if tableParallelism > 1 && snap != nil {
		peers = snap.ExtraReaders
	}

	sink.PhaseCompleted(backupPhaseSchema)
	sink.PhaseStarted(backupPhaseCopy)
	if err := b.runBackupTablePool(ctx, tasks, rr, peers, factory, tableParallelism, chunkRows, committer, chainCEK, within); err != nil {
		return err
	}
	sink.PhaseCompleted(backupPhaseCopy)
	sink.PhaseStarted(backupPhaseFinalize)

	// 4.5. Record EndPosition — anchored-resume adoption / snapshot-
	// anchored / v0.17.x post-sweep fallback (see recordEndPosition).
	if err := b.recordEndPosition(ctx, manifest, adopting, resumeAnchor, snapshotPos, snap); err != nil {
		return err
	}

	// Compute BackupID once EndPosition is known. The id is used by
	// Phase 3 incrementals to link via ParentBackupID; pre-Phase-3
	// fulls leave it empty, which the chain-restore walker tolerates by
	// computing on demand. Filling it in here means the full's manifest
	// carries the same id the incremental would compute when chaining.
	manifest.BackupID = irbackup.ComputeBackupID(manifest)

	// 5. Final manifest write — flip to complete. Routed through the
	// committer like every other manifest Put this run (the pool has
	// drained, so this is single-threaded; one code path regardless).
	// Sidecar mode folds the progress back into the one self-contained
	// manifest (schema-appropriate format version restored, sidecar
	// reference cleared + file deleted) so the finalized layout is the
	// pre-ADR-0086 contract restore/verify/chain tooling already reads.
	manifest.PartialState = irbackup.BackupStateComplete
	if err := committer.finalize(ctx); err != nil {
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
	// recorded-codec accelerator. The one non-best-effort outcome is the
	// ADR-0160 concurrent-writer conflict: a second writer interleaving
	// this chain fails the run loudly (the manifest above is durable;
	// only the catalog append was refused).
	if err := lineage.UpdateLineageForManifestBestEffort(ctx, b.Store, manifest, lineage.ManifestFileName, blobcodec.ResolveCodec(b.Codec)); err != nil {
		return fmt.Errorf("backup: lineage catalog: %w", err)
	}

	// ADR-0154: sign the manifest + the lineage catalog AFTER both are
	// durable. The full is segment 0's root manifest (sequence 0). Unlike
	// the best-effort lineage update, signing is fail-loud: a signed
	// chain with an unsigned artifact would refuse at restore, so a
	// signing failure must fail the backup, not leave a half-signed store.
	if err := b.signBackupArtifacts(ctx, manifest); err != nil {
		return fmt.Errorf("backup: sign artifacts: %w", err)
	}

	logBackupComplete(ctx, manifest, b.Summary)
	sink.PhaseCompleted(backupPhaseFinalize)
	sink.Summary(backupSummaryResult(manifest, b.signingRequested()))
	return nil
}

// backupSummaryResult builds the ADR-0155 TTY summary panel for a
// completed full/incremental manifest — tables, rows, chunks, and the
// encrypted/signed/EndPosition posture the operator wants confirmed after
// a backup. TTY-only; [progress.Nop] ignores it on the non-TTY path.
func backupSummaryResult(manifest *irbackup.Manifest, signed bool) progress.Result {
	totalRows := int64(0)
	totalChunks := 0
	for _, t := range manifest.Tables {
		totalRows += t.RowCount
		totalChunks += len(t.Chunks)
	}
	fields := []progress.Field{
		{Label: "Tables", Value: progress.HumanCount(int64(len(manifest.Tables)))},
		{Label: "Rows", Value: progress.HumanCount(totalRows)},
		{Label: "Chunks", Value: progress.HumanCount(int64(totalChunks))},
		{Label: "Encrypted", Value: yesNo(manifest.ChainEncryption != nil)},
		{Label: "Signed", Value: yesNo(signed)},
	}
	if tok := manifest.EndPosition.Token; tok != "" {
		fields = append(fields, progress.Field{Label: "EndPosition", Value: clipToken(tok)})
	}
	return progress.Result{Fields: fields}
}

// yesNo renders a bool as the summary panel's "yes"/"no".
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// clipToken shortens a CDC position token for the one-line summary field
// (tokens are JSON blobs that would blow past the box width).
func clipToken(tok string) string {
	const maxLen = 48
	if len(tok) <= maxLen {
		return tok
	}
	return tok[:maxLen-3] + "..."
}

// signBackupArtifacts writes the detached manifest + lineage signatures
// for a signed (--sign) run. A no-op when signing is off. The envelope's
// signability was already validated in setupChainEncryption, so a failure
// here is an I/O problem, not a misconfiguration.
func (b *Backup) signBackupArtifacts(ctx context.Context, manifest *irbackup.Manifest) error {
	if !b.signingRequested() {
		return nil
	}
	signer, err := b.buildWriteSigner()
	if err != nil {
		return err
	}
	if err := lineage.WriteManifestSig(ctx, b.Store, lineage.ManifestFileName, manifest, 0, signer); err != nil {
		return fmt.Errorf("write manifest signature: %w", err)
	}
	if err := lineage.SignLineageCatalog(ctx, b.Store, signer); err != nil {
		return fmt.Errorf("write lineage signature: %w", err)
	}
	slog.InfoContext(ctx, "backup: wrote detached signatures (ADR-0154)",
		slog.String("scheme", signer.Scheme),
		slog.String("key_id", signer.KeyID),
		slog.Int("format_version", manifest.FormatVersion))
	return nil
}

// signingRequested reports whether this run must sign — either the
// HMAC-off-KEK default ([Backup.Sign]) or an explicit Ed25519 keypair
// ([Backup.Ed25519Signer], `--sign-key`).
func (b *Backup) signingRequested() bool {
	return b.Sign || b.Ed25519Signer != nil
}

// buildWriteSigner resolves the signer for a signing run: the pre-built
// Ed25519 signer (Phase 2) takes precedence over the HMAC-off-KEK signer
// derived from the chain envelope (Phase 1). Signability was validated in
// setupChainEncryption (HMAC) / the CLI (Ed25519), so a failure here is an
// internal inconsistency, not a misconfiguration.
func (b *Backup) buildWriteSigner() (*lineage.Signer, error) {
	if b.Ed25519Signer != nil {
		return b.Ed25519Signer, nil
	}
	signer, ok, err := lineage.NewSigner(b.Encryption.Envelope)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("signing envelope became non-signable after preflight (internal inconsistency)")
	}
	return signer, nil
}

// logBackupComplete emits the terminal "backup complete" summary line
// and mirrors the completed manifest's per-table row counts into the
// optional Summary (nil-safe no-op otherwise) for the CLI's
// `--format json` envelope. Manifest table names already carry any
// namespace prefix, so the schema slot stays empty.
func logBackupComplete(ctx context.Context, manifest *irbackup.Manifest, summary *migcore.RunSummary) {
	totalRows := int64(0)
	totalChunks := 0
	for _, t := range manifest.Tables {
		totalRows += t.RowCount
		totalChunks += len(t.Chunks)
		summary.RecordTableRows("", t.Name, t.RowCount)
	}
	slog.InfoContext(
		ctx, "backup complete",
		slog.Int("tables", len(manifest.Tables)),
		slog.Int64("rows", totalRows),
		slog.Int("chunks", totalChunks),
	)
}

// buildInProgressManifest constructs the in-progress manifest Run
// checkpoints after each table completes. A resumed run preserves the
// prior CreatedAt (the "when was this backup taken?" answer is the
// snapshot point, not the resume point) and stamps the ADOPTED prior
// anchor; a fresh snapshot-anchored run stamps this run's snapshot
// position; the non-snapshot fallback leaves EndPosition unset for the
// post-sweep capture (step 4.5).
func (b *Backup) buildInProgressManifest(schema *ir.Schema, prior *irbackup.Manifest, adopting bool, resumeAnchor ir.Position, snapshotPos *ir.Position) (*irbackup.Manifest, error) {
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	// ADR-0152: fulls record the schema fingerprint too (incrementals
	// always have), extending chain-restore's recompute-and-compare
	// corruption check to the chain root.
	schemaHash, err := irbackup.ComputeSchemaHash(schema)
	if err != nil {
		return nil, fmt.Errorf("backup: hash source schema: %w", err)
	}
	manifest := &irbackup.Manifest{
		// Bug 116 closure: stamp the smallest format version safe for
		// this schema. Schemas using RLS / policies / exclude
		// constraints get FormatVersion=2 so older binaries refuse
		// rather than silently drop those fields; innocent schemas
		// stay on FormatVersion=1 for max backward compatibility.
		// (Encrypted runs are bumped further by setupChainEncryption —
		// ADR-0152.)
		FormatVersion: irbackup.FormatVersionFor(schema),
		SluiceVersion: b.SluiceVersion,
		CreatedAt:     now().UTC(),
		SourceEngine:  b.Source.Name(),
		Schema:        schema,
		SchemaHash:    schemaHash,
		Tables:        make([]*irbackup.TableManifest, 0, len(schema.Tables)),
		PartialState:  irbackup.BackupStateInProgress,
		Kind:          irbackup.BackupKindFull,
	}
	if prior != nil {
		manifest.CreatedAt = prior.CreatedAt
	}
	// Task #42 (ADR-0085): stamp the chain anchor on the IN-PROGRESS
	// manifest from its first write, so a crashed run leaves the anchor a
	// future resume must adopt. A resumed run stamps the ADOPTED
	// prior-attempt anchor — never this run's fresh snapshot position.
	switch {
	case adopting:
		manifest.EndPosition = resumeAnchor
	case snapshotPos != nil:
		manifest.EndPosition = *snapshotPos
	}
	return manifest, nil
}

// logResumePlan fans the resume decision out per-table — which prior
// tables are already fully complete (chunks still present on the store)
// vs which will be re-streamed — and logs the headline plan. Surface-
// level only: the actual skip / re-stream happens in the sweep.
// Re-validating a prior table's chunk presence can fail, which surfaces
// here as an error. A nil / non-in-progress prior is a no-op.
func (b *Backup) logResumePlan(ctx context.Context, schema *ir.Schema, prior *irbackup.Manifest, priorTables map[string]*irbackup.TableManifest) error {
	if prior == nil || prior.PartialState != irbackup.BackupStateInProgress {
		return nil
	}
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
	return nil
}

// recordEndPosition stamps manifest.EndPosition after the row sweep via
// one of three paths: an anchored resume re-asserts the ADOPTED prior
// anchor (nothing may overwrite it — kept tables are exact there and CDC
// from it covers the rest); a snapshot-anchored run uses the position
// captured at snapshot start; the v0.17.x fallback captures the position
// now, post-sweep, via the optional PositionCapturer (the documented
// during-backup write-window gap, already WARN-logged at snapshot open).
//
// snap is the snapshot opened for this run (nil on the v0.17.x non-
// snapshot fallback). On the snapshot-anchored path, if the snapshot
// carries a [irbackup.Snapshot.FinalizePositionFn] — the VStream case,
// whose terminal VGTID is only known AFTER the concurrent COPY pump
// drains (ADR-0071) — its post-sweep result is recorded instead of the
// zero-at-open snapshotPos. Engines whose open-time position is
// authoritative (Postgres, vanilla MySQL) leave FinalizePositionFn nil
// and record snapshotPos unchanged (byte-identical to before).
func (b *Backup) recordEndPosition(ctx context.Context, manifest *irbackup.Manifest, adopting bool, resumeAnchor ir.Position, snapshotPos *ir.Position, snap *irbackup.Snapshot) error {
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
		endPos := *snapshotPos
		// VStream: the open-time snapshotPos is the ZERO value because the
		// terminal VGTID is produced by the concurrent COPY pump (ADR-0071)
		// and is only readable after the sweep drains. FinalizePositionFn
		// joins the copy-completion barrier and returns the real anchor;
		// recording snapshotPos here instead would stamp an EMPTY
		// EndPosition, breaking chain-resume off a VStream full backup.
		if snap != nil && snap.FinalizePositionFn != nil {
			finalized, err := snap.FinalizePositionFn(ctx)
			if err != nil {
				return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: finalize end position: %w", err))
			}
			endPos = finalized
			slog.InfoContext(
				ctx, "backup: finalized end position after row sweep (post-COPY anchor)",
				slog.String("engine", manifest.SourceEngine),
				slog.String("open_time_token", snapshotPos.Token),
				slog.String("finalized_token", endPos.Token),
			)
		}
		manifest.EndPosition = endPos
		slog.InfoContext(
			ctx, "backup: recorded end position (snapshot-anchored)",
			slog.String("engine", manifest.SourceEngine),
			slog.String("position_token", endPos.Token),
		)
	default:
		if err := b.captureEndPosition(ctx, manifest); err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: capture end position: %w", err))
		}
	}
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
func (b *Backup) resolveResumeState(ctx context.Context) (prior *irbackup.Manifest, resumeAnchor ir.Position, err error) {
	prior, err = lineage.ReadManifestIfPresent(ctx, b.Store)
	if err != nil {
		return nil, ir.Position{}, fmt.Errorf("backup: inspect existing manifest: %w", err)
	}
	if prior == nil {
		return nil, ir.Position{}, nil
	}
	switch prior.PartialState {
	case irbackup.BackupStateInProgress:
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
		// the optional [irbackup.AnchorSweeper] surface: a sweep
		// failure must not fail the resume itself.
		if sweeper, ok := b.Source.(irbackup.AnchorSweeper); ok {
			if err := sweeper.SweepOrphanedBackupAnchors(ctx, b.SourceDSN); err != nil {
				slog.WarnContext(
					ctx, "backup: orphaned-anchor sweep failed; stale anchor slots may still be retaining WAL on the source",
					slog.String("engine", b.Source.Name()),
					slog.String("err", err.Error()),
				)
			}
		}
		resumeAnchor = prior.EndPosition
	case irbackup.BackupStateComplete, "":
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
		if err := migcore.PreflightChainResume(ctx, b.Source, b.SourceDSN, resumeAnchor); err != nil {
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
// v0.18.0: when the engine implements [irbackup.SnapshotOpener], we
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
// [irbackup.PositionCapturer] capture (later, in
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
// cold-start dispatcher) — same selection logic, [irbackup.Snapshot]
// shape.
//
//   - source implements [irbackup.TableScopedBackupSnapshotOpener] AND there
//     are filtered tables (len(tables) > 0) → OpenBackupSnapshotForTables,
//     so a PlanetScale backup scopes its VStream COPY to the included
//     tables (avoids the ADR-0071 over-stream/buffer-overflow on a large
//     unrelated keyspace table).
//   - else source implements [irbackup.SnapshotOpener] → OpenBackupSnapshot
//     (unchanged whole-snapshot path — PG, vanilla MySQL via base).
//   - else → not implemented (ok=false); the caller takes the v0.17.x
//     non-snapshot fallback.
//
// The schema passed here is already filtered to the included tables by
// [migcore.ApplyTableFilter] (called earlier in Run), so its table names are the
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
func (b *Backup) openBackupSnapshotScoped(ctx context.Context, schema *ir.Schema, persistChainSlot bool, requestedReaders int) (snap *irbackup.Snapshot, implemented bool, err error) {
	opts := irbackup.SnapshotOptions{
		SlotName:          b.SlotName,
		PersistChainSlot:  persistChainSlot,
		ReaderParallelism: requestedReaders,
	}
	tables := migcore.TableNamesForPublication(schema)
	if len(tables) > 0 {
		if scoped, ok := b.Source.(irbackup.TableScopedBackupSnapshotOpener); ok {
			snap, err = scoped.OpenBackupSnapshotForTables(ctx, b.SourceDSN, opts, tables)
			return snap, true, err
		}
	}
	if opener, ok := b.Source.(irbackup.SnapshotOpener); ok {
		snap, err = opener.OpenBackupSnapshot(ctx, b.SourceDSN, opts)
		return snap, true, err
	}
	return nil, false, nil
}

// applyVStreamFloatPolicy handles the VStream-COPY FLOAT display-rounding
// class for `backup full` (roadmap open-bug 2026-07-09). When the just-
// opened snapshot's reader display-rounds single-precision FLOAT columns
// (VStream) AND the schema has any:
//
//   - --no-float-exact-reread → WARN per column, archive ROUNDED (the
//     rounded-but-perfectly-consistent snapshot). No wrap, no re-read.
//   - default → wrap snap.Rows in a [floatExactPatchReader] so every
//     REPAIRABLE table (has a PK + a non-PK single-precision FLOAT column)
//     under the bounded-memory row cap is patched with EXACT source values;
//     an over-cap table falls back to WARN + rounded; a keyless /
//     float-PK-only FLOAT table (un-repairable) WARNs + rounded.
//   - --strict-float → the same wrap, but the un-repairable columns refuse
//     UPFRONT here (SLUICE-E-VSTREAM-FLOAT-LOSSY, exit 3) and an over-cap
//     table refuses inside the reader — "exact, or fail; never rounded".
//
// The exact re-read is BOUNDED-memory: the wrapper buffers at most
// --float-reread-max-rows PK→float entries per table (serial VStream
// sweep), falling back loudly past that rather than OOM-ing on a huge FLOAT
// table (the ADR-0071 tenet). Position correctness is untouched (see
// [floatExactPatchReader]). No-op on every non-VStream source and when the
// schema has no single-precision FLOAT column. Returns an error only for
// the --strict-float upfront refusal.
func (b *Backup) applyVStreamFloatPolicy(ctx context.Context, snap *irbackup.Snapshot, schema *ir.Schema) error {
	lossy, ok := snap.Rows.(ir.LossyFloatCopyReader)
	if !ok || !lossy.CopyDisplayRoundsFloats() {
		return nil
	}
	plan := planBackupFloatRepair(schema)
	// Split the single-precision FLOAT columns into repairable (a plan
	// table) and un-repairable (keyless / float-PK-only).
	var repairableCols, unrepairableCols []string
	for _, t := range schema.Tables {
		_, repairable := plan[t.Name]
		for _, c := range migcore.SinglePrecisionFloatColumns(t) {
			q := t.Name + "." + c.Name
			if repairable {
				repairableCols = append(repairableCols, q)
			} else {
				unrepairableCols = append(unrepairableCols, q)
			}
		}
	}
	if len(repairableCols)+len(unrepairableCols) == 0 {
		return nil
	}

	if b.NoFloatExactReread {
		for _, c := range append(append([]string{}, repairableCols...), unrepairableCols...) {
			slog.WarnContext(ctx,
				"backup: VStream COPY archives this single-precision FLOAT column display-rounded to 6 significant digits; "+
					"the exact re-read is DISABLED (--no-float-exact-reread), so the rounding is retained (a "+
					"perfectly-consistent snapshot). Drop the flag to archive exact float32 instead",
				slog.String("column", c))
		}
		return nil
	}

	// --strict-float: an un-repairable FLOAT column can NEVER be exact, so
	// refuse UPFRONT (before any table is archived) rather than fail
	// mid-sweep. Over-cap repairable tables refuse inside the reader.
	if b.StrictFloat && len(unrepairableCols) > 0 {
		return sluicecode.Wrap(sluicecode.CodeVStreamFloatLossy,
			"add a primary key to the table, or drop --strict-float (default archives exact where it can, rounded elsewhere)",
			fmt.Errorf("backup: --strict-float: %d single-precision FLOAT column(s) cannot be re-read exactly — the table "+
				"has no primary key to target the re-read, or the FLOAT is part of the PK (%s). Add a PK, exclude the table, "+
				"or drop --strict-float", len(unrepairableCols), strings.Join(unrepairableCols, ", ")))
	}

	// Default / strict repairable: WARN per column, then wrap for the exact
	// re-read.
	for _, c := range repairableCols {
		slog.WarnContext(ctx,
			"backup: VStream COPY display-rounds this single-precision FLOAT column; sluice re-reads it EXACTLY from the "+
				"source before archiving (tables over --float-reread-max-rows fall back loudly). Cost: a bounded within-row "+
				"temporal skew — the FLOAT value reflects a read instant slightly after the snapshot VGTID (ZERO on a "+
				"quiescent source; SELF-HEALS on a chain restore since incrementals replay from the full's position forward; "+
				"persists only for a standalone-full restore of a source with concurrent FLOAT writes). Pass "+
				"--no-float-exact-reread for a rounded-but-consistent archive, or --strict-float to refuse",
			slog.String("column", c))
	}
	// Default only: the un-repairable columns retain rounding with a WARN
	// (under --strict-float they already refused above).
	for _, c := range unrepairableCols {
		slog.WarnContext(ctx,
			"backup: VStream COPY display-rounds this single-precision FLOAT column and it CANNOT be re-read exactly — the "+
				"table has no primary key to target the re-read (or the FLOAT is part of the PK). The rounding is retained "+
				"for this column. Pass --strict-float to refuse instead",
			slog.String("column", c))
	}
	if len(plan) > 0 {
		snap.Rows = newFloatExactPatchReader(snap.Rows, b.Source, b.SourceDSN, plan, b.FloatRereadMaxRows, b.StrictFloat)
	}
	return nil
}

func (b *Backup) openSnapshotOrFallback(ctx context.Context, schema *ir.Schema, persistChainSlot bool) (ir.RowReader, *ir.Position, *irbackup.Snapshot, func(), error) {
	// ADR-0088: resolve the REQUESTED cross-table parallelism BEFORE the
	// snapshot opens, so an engine that opens coincident readers eagerly
	// (MySQL vanilla, under a brief FTWRL window) knows how many to open.
	// PG ignores it (its readers are minted lazily from the exported
	// snapshot name). The number is re-resolved against the actual task
	// count after staging (resolveBackupTableParallelism) — this is only
	// the snapshot-open hint.
	requestedReaders, err := b.resolveRequestedReaderParallelism(ctx, len(schema.Tables))
	if err != nil {
		return nil, nil, nil, func() {}, err
	}
	if snap, ok, err := b.openBackupSnapshotScoped(ctx, schema, persistChainSlot, requestedReaders); ok {
		if err == nil {
			// VStream-COPY FLOAT display-rounding (roadmap open-bug
			// 2026-07-09): a VStream snapshot renders single-precision FLOAT
			// at mysqld's 6-significant-digit display precision. By default
			// sluice re-reads FLOAT columns EXACTLY and patches the archived
			// rows (wraps snap.Rows here); --no-float-exact-reread archives
			// rounded-but-consistent; --strict-float refuses loudly.
			if err := b.applyVStreamFloatPolicy(ctx, snap, schema); err != nil {
				if cerr := snap.Close(); cerr != nil {
					slog.WarnContext(ctx, "backup: snapshot close after --strict-float refusal failed",
						slog.String("err", cerr.Error()))
				}
				return nil, nil, nil, func() {}, err
			}
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
		return nil, nil, nil, func() {}, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: open source row reader: %w", err))
	}
	cleanup := func() { migcore.CloseIf(rr) }
	return rr, nil, nil, cleanup, nil
}

// captureEndPosition queries the source for its current CDC position
// and stores it on manifest.EndPosition. v0.18.0: this path is the
// FALLBACK shape used only when the engine doesn't implement
// [irbackup.SnapshotOpener] — engines that do (PG, MySQL in v0.18.0+)
// route through [openSnapshotOrFallback] instead and capture the
// position at snapshot START rather than post-sweep.
//
// Engines that don't support CDC (Capabilities.CDC == ir.CDCNone) skip
// the capture; engines that do but don't implement
// [irbackup.PositionCapturer] also skip with a debug log line so the
// gap is visible to operators running with --log-level=debug.
//
// In the fallback shape the capture happens AFTER the per-table row
// sweep, so the recorded position reflects "the source has produced
// everything up to here at the moment the backup completes." Writes
// during the backup window are NOT covered by this path's row sweep
// (no shared snapshot) and NOT covered by the chain's next link's
// `--since=<full>.EndPosition` window (those LSNs are before this
// captured EndPosition) — the documented v0.17.2 caveat.
func (b *Backup) captureEndPosition(ctx context.Context, manifest *irbackup.Manifest) error {
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
	defer migcore.CloseIf(sr)

	capturer, ok := sr.(irbackup.PositionCapturer)
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
	case b.Sign && b.Ed25519Signer != nil:
		// The two schemes are mutually exclusive; the CLI refuses --sign +
		// --sign-key, but guard defensively so a mixed-scheme chain can
		// never be produced.
		return errors.New("backup: --sign (HMAC-off-KEK) and --sign-key (Ed25519) are mutually exclusive — choose one signing scheme")
	case b.Sign && b.Encryption == nil:
		// HMAC-off-KEK (--sign) keys the HMAC off the chain KEK, so it needs
		// an encrypted chain. Plaintext signing uses --sign-key (Ed25519).
		return errors.New("backup: --sign (HMAC-off-KEK) needs an encrypted chain — add --encrypt (with a passphrase), or use --sign-key to sign a plaintext backup with an Ed25519 keypair")
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

	// The chunk-roll / upload-skip / per-chunk-checkpoint plumbing lives
	// in [backupChunkStreamer], shared verbatim with the ADR-0149 range
	// workers. This single-stream path is its own (sole) index allocator
	// — the counters are trivially sequential here.
	var chunkIdx, rowsTotal atomic.Int64
	s := b.newBackupChunkStreamer(table, entry, chunkRows, committer, chainCEK, &chunkIdx, &rowsTotal)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, ok := <-rows:
			if !ok {
				if err := s.flush(ctx); err != nil {
					return err
				}
				// Surface any sticky error captured by the reader's
				// streaming goroutine (Bug 68 loud-failure gate; now a
				// first-class [ir.RowReader.Err] surface, no longer an
				// optional type assertion).
				if err := migcore.ReaderStreamErr(rr, table); err != nil {
					return err
				}
				// Per-table checkpoint: flip the entry to its terminal
				// state (natural EOF) and commit. Empty tables and tables
				// whose row count is an exact chunk multiple rely on this
				// commit — their last per-chunk checkpoint (if any)
				// doesn't carry the Partial=false flip.
				if err := committer.finishTable(ctx, entry, rowsTotal.Load()); err != nil {
					return fmt.Errorf("checkpoint manifest after table: %w", err)
				}
				slog.InfoContext(
					ctx, "backup: table complete",
					slog.String("table", table.Name),
					slog.Int64("rows", rowsTotal.Load()),
					slog.Int("chunks", len(entry.Chunks)),
				)
				return nil
			}
			if err := s.writeRow(ctx, row); err != nil {
				return err
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
func priorResumableTables(prior *irbackup.Manifest) []*irbackup.TableManifest {
	if prior == nil || prior.PartialState != irbackup.BackupStateInProgress {
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

// manifestSchemaFingerprint hashes s for the resume drift guard,
// which compares a reader-fresh schema against prior.Schema — a
// schema that has been stored in (and re-read from) a manifest.
// [irbackup.ComputeSchemaHash] is stable across that manifest JSON
// round-trip (task #49: its canonical view normalizes nil
// Column.Default to the explicit DefaultNone the decode hooks
// materialize), so the plain hash serves both domains; this wrapper
// used to JSON-round-trip s itself to paper over the asymmetry.
// Pinned by [TestBackup_ResumeAdoptsPriorAnchor] (identical schema
// across attempts must not refuse).
func manifestSchemaFingerprint(s *ir.Schema) (string, error) {
	return irbackup.ComputeSchemaHash(s)
}

// refuseKeylessRestreamOnAnchoredResume refuses an anchored resume when
// any table that must be (re-)streamed THIS run is truly keyless — no
// PRIMARY KEY and no all-NOT-NULL plain-column UNIQUE index
// ([irbackup.TableReplayIdempotent]). Such a table's chunks are read at this
// run's later snapshot and therefore OVERLAP the chain's replay window
// (adopted anchor, stop]; the chain appliers' keyless fallback is plain
// INSERT (ADR-0010), so the overlap would duplicate rows silently.
// Kept tables may be keyless — they are exact at the adopted anchor,
// no overlap.
func refuseKeylessRestreamOnAnchoredResume(tasks []backupTableTask) error {
	var keyless []string
	for _, task := range tasks {
		if !irbackup.TableReplayIdempotent(task.table) {
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
// stripped. Other implementations of [irbackup.Store] fall back to
// `<unknown-store>` so log shape is stable.
func backupStoreDescriptor(s irbackup.Store) string {
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
func tableChunksAllPresent(ctx context.Context, store irbackup.Store, entry *irbackup.TableManifest) (bool, error) {
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
func tableManifestFullyComplete(ctx context.Context, store irbackup.Store, entry *irbackup.TableManifest) (bool, error) {
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
func chunkAlreadyMatches(ctx context.Context, store irbackup.Store, key, expectedSHA256 string) (bool, error) {
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
	got, err := blobcodec.HashChunkBytes(ctx, rc)
	if err != nil {
		return false, fmt.Errorf("hash %q: %w", key, err)
	}
	return got == expectedSHA256, nil
}

// setupChainEncryption configures the manifest's [irbackup.ChainEncryption]
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
func (b *Backup) setupChainEncryption(manifest, prior *irbackup.Manifest) ([]byte, error) {
	if b.Encryption == nil {
		return nil, nil
	}
	enc := b.Encryption
	if enc.Envelope == nil {
		return nil, errors.New("backup: encryption envelope is nil")
	}
	mode := enc.Mode
	// Bug 179 (full-resume edition): a resumed in-progress full must keep the
	// mode the chain root already committed. Inherit it when --encrypt-mode is
	// omitted; refuse an explicit conflicting mode loudly — otherwise the
	// resumed manifest records one mode while the prior chunks are the other,
	// producing an un-restorable chain (the same shape the chain-extend fix
	// closes in alignEncryption).
	if prior != nil && prior.ChainEncryption != nil && prior.ChainEncryption.Mode != "" {
		priorMode := prior.ChainEncryption.Mode
		if mode == "" {
			mode = priorMode
		} else if mode != priorMode {
			return nil, fmt.Errorf("backup: --encrypt-mode=%q conflicts with the in-progress chain's mode %q; "+
				"resume with the same mode or omit --encrypt-mode to inherit it", mode, priorMode)
		}
	}
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	// Bug 180: make the resolved mode authoritative for the sibling
	// resolveChunkCEK too (it reads b.Encryption.Mode). Without this, an
	// omitted --encrypt-mode resuming a per-chunk full would stamp the
	// manifest per-chunk here while resolveChunkCEK defaulted per-chain — the
	// same un-restorable mode-source split the chain-extend fix closes.
	enc.Mode = mode

	// ADR-0152: an encrypted manifest is stamped the chunk-binding
	// format version — its chunks carry the GCM AAD position binding
	// and its CEK wrap the identity binding; readers derive both from
	// this recorded stamp. One exception, mirroring the Bug-179
	// inherit-the-chain's-shape rule: RESUMING a pre-v5 encrypted
	// in-progress run keeps the prior version, because the kept chunks
	// on the store were written UNBOUND and a v5 stamp would send the
	// restore down the AAD path against them. The stamp must precede
	// the CEK wrap below so [irbackup.CEKBinding] gates consistently.
	resumingPreBinding := prior != nil && prior.ChainEncryption != nil &&
		prior.FormatVersion < irbackup.FormatVersionEncryptedChunkBinding
	// SEC-F1 / SEC-1: resuming an encrypted run written before the ROW-CHUNK
	// TABLE-binding format (v5/v6) keeps that prior version for the whole run —
	// its already-written row chunks carry the identity+path AAD WITHOUT the
	// parent-table field, so stamping v7 would send restore down the
	// table-bound AAD path against chunks written without it. Same
	// inherit-the-chain's-shape rule as resumingPreBinding, one tier up.
	resumingPreTableBinding := prior != nil && prior.ChainEncryption != nil &&
		prior.FormatVersion < irbackup.FormatVersionChunkTableBinding

	switch {
	case resumingPreBinding:
		slog.Info("backup: resuming an encrypted run written before the chunk-binding format; its format version and unbound chunk shape are kept for the whole run",
			slog.Int("format_version", prior.FormatVersion))
	case resumingPreTableBinding:
		// Resumed v5/v6 encrypted chain: keep its (no-table-AAD) shape so the
		// unbound chunks already on the store still decrypt.
		manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
	default:
		// SEC-1: a FRESH encrypted backup binds its row-chunk GCM AAD to the
		// parent (schema, table) — FormatVersion 7 — WHETHER OR NOT it is
		// signed. GCM enforces the AAD regardless of any signature, so a
		// store-write adversary who swaps the chunk lists of two same-column-
		// set tables fails to decrypt (green-tag cross-table restore) on an
		// UNSIGNED encrypted backup too, not just a signed one. (SEC-F1
		// originally table-bound only signed-encrypted fulls; signedness is
		// determined from the .sig artifact, not this version, so binding the
		// table here does not imply a signature is required.)
		manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionChunkTableBinding)
	}

	// ADR-0154: a signed backup fails fast on a non-signable envelope (KMS:
	// Phase 3) or a resumed pre-binding chain (unbound chunks can't be brought
	// under a signature) — refusing here, before the sweep, beats writing
	// chunks we then can't sign. The FormatVersion is already table-bound (v7)
	// for a fresh encrypted run from the stamp above; a resumed pre-v7 signed
	// run keeps the signed-manifest version (v6) so its kept chunks — written
	// without the table field — still decrypt.
	if b.signingRequested() {
		if resumingPreBinding {
			return nil, errors.New("backup: signing cannot extend a resumed pre-chunk-binding encrypted run; start a fresh signed chain with --force-overwrite")
		}
		// HMAC-off-KEK (--sign) needs a signable local KEK; Ed25519
		// (--sign-key) is independent of the envelope, so it is allowed on
		// ANY encrypted chain including KMS (Ed25519 signs the manifest,
		// KMS wraps the CEK — orthogonal).
		if b.Sign {
			if _, ok, err := lineage.NewSigner(enc.Envelope); err != nil {
				return nil, fmt.Errorf("backup: --sign: %w", err)
			} else if !ok {
				return nil, errors.New("backup: --sign on this encrypted chain is unsupported in ADR-0154 Phase 1: HMAC signing needs a local passphrase-derived key, but a KMS-encrypted chain's KEK never leaves the HSM (KMS Sign is Phase 3). Use --sign-key (Ed25519) to sign a KMS-encrypted chain")
			}
		}
		signedVersion := irbackup.FormatVersionChunkTableBinding
		if resumingPreTableBinding {
			signedVersion = irbackup.FormatVersionSignedManifest
		}
		manifest.FormatVersion = max(manifest.FormatVersion, signedVersion)
	}

	chainEnc := &irbackup.ChainEncryption{
		Algorithm: crypto.AlgorithmAESGCM,
		Mode:      mode,
		KEKMode:   enc.Envelope.Mode(),
		KEKRef:    enc.KEKRef,
	}
	// Passphrase mode: record the Argon2id params so a restore-side
	// envelope can re-derive the same KEK.
	if pe, ok := enc.Envelope.(*crypto.PassphraseEnvelope); ok {
		p := pe.Params()
		chainEnc.Argon2id = &irbackup.Argon2idParams{
			Salt:        p.Salt,
			Memory:      p.Memory,
			Iterations:  p.Iterations,
			Parallelism: p.Parallelism,
			KeyLen:      p.KeyLen,
		}
	}

	// Per-chunk mode: chain-level WrappedCEK stays empty; each chunk
	// generates its own CEK. The KEKRef still records the envelope's
	// resolved (Azure: version-pinned) reference so per-chunk unwraps
	// can be retargeted after key rotation (audit N-9).
	if mode == crypto.EncryptModePerChunk {
		chainEnc.KEKRef = lineage.ResolvedKEKRef(enc.Envelope, chainEnc.KEKRef)
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
		if err := enc.RebindForChain(prior.ChainEncryption.Argon2id); err != nil {
			return nil, fmt.Errorf("rebuild envelope for prior chain: %w", err)
		}
		// Unwrap to recover the in-flight CEK. Routed through the
		// ADR-0152 chokepoint: the PRIOR manifest owns the wrap, so its
		// recorded FormatVersion decides bound-vs-unbound and its
		// identity is the binding (identical to this run's — resume
		// adopts the prior CreatedAt).
		cek, err := lineage.UnwrapChainCEK(enc.Envelope, prior.ChainEncryption.WrappedCEK, prior)
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
		// ADR-0152 chokepoint: bound to THIS manifest's identity when
		// it is stamped v5 and the envelope supports bindings.
		wrapped, err := lineage.WrapChainCEK(enc.Envelope, cek, manifest)
		if err != nil {
			return nil, fmt.Errorf("wrap chain cek: %w", err)
		}
		chainCEK = cek
		chainEnc.WrappedCEK = wrapped
		// Audit N-9: prefer the envelope's resolved (Azure: versioned)
		// key reference over the operator-typed one, so restore-side
		// unwraps can be retargeted at the wrap-time key version after
		// rotation. Identity fallback for every other envelope.
		chainEnc.KEKRef = lineage.ResolvedKEKRef(enc.Envelope, chainEnc.KEKRef)
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
