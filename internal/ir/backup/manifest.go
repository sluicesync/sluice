// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package backup defines the logical-backup contract shared between
// the writer (`sluice backup`) and reader (`sluice restore`) sides of
// the backup feature: the manifest types, the storage abstraction,
// the chain identity/fingerprint helpers, and the optional engine
// surfaces the backup orchestrator type-asserts on.
//
// Everything here is pure types and pure functions over internal/ir —
// no I/O, no engine knowledge. The package depends on core ir; core ir
// never imports it. The sealed-interface JSON codec the manifest's
// schema field rides on ([ir.Column.MarshalJSON] / [ir.MarshalType])
// stays in core ir (`schema_wire.go`) because it is method-bound to
// core types and shared with the CDC schema-history store.
//
// What lives in this file:
//
//   - The [Store] interface — the storage abstraction every backend
//     (local-FS, S3 / GCS / Azure) plugs into.
//   - The [Manifest] / [TableManifest] / [ChunkInfo] types — the
//     serialised public contract of a backup directory. Operators
//     interact with this via `sluice backup verify`; tooling depends
//     on it; restore reads it first. The format-version field is the
//     load-bearing forward-compat anchor — older sluice refuses
//     newer manifests, newer sluice always reads older.
//
// See `docs/dev/design/logical-backups.md` for the full design.
package backup

import (
	"context"
	"io"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// BackupFormatVersion is the highest manifest-schema version this
// build understands. Older sluice refuses newer manifests with a
// clear error (preflight in `internal/pipeline.readManifest`); newer
// sluice always reads older.
//
// v0.16.x manifests carry FormatVersion=1 (full backups only). v0.17.0
// kept FormatVersion=1: every Phase 3 addition was forward-compatible
// (older sluice ignored [Manifest.Kind]/[Manifest.ParentBackupID]/etc.
// — those manifests appear as orphan fulls when read by an older
// binary, which is the right degraded behaviour for incrementals
// nobody can chain anyway).
//
// v0.94.1 introduces FormatVersion=2 to close Bug 116: schema-
// metadata fields ([ir.Table.RLSEnabled], [ir.Table.RLSForced],
// [ir.Table.Policies], [ir.Table.ExcludeConstraints]) added under the
// FormatVersion=1 "field-additions are forward-compatible" policy were
// silently dropped by older binaries reading manifests written by
// newer binaries — a CRITICAL silent-loss class for security policies
// (RLS / policies) and correctness constraints (EXCLUDE). The fix is
// proportional: a v0.94.1+ manifest is stamped FormatVersion=2 ONLY
// when its schema actually uses one of those features; otherwise it
// stays FormatVersion=1 and continues to restore on older binaries.
// Older binaries reading a v2 manifest refuse loudly at the preflight
// rather than silently dropping the security/correctness metadata.
//
// Going forward: any schema-metadata addition with security or
// correctness implications bumps FormatVersion. Purely informational
// or behaviorally-idempotent additions (e.g. annotations, statistics
// hints) can stay under the field-additions-are-forward-compatible
// rule.
//
// v0.99.39+ introduces FormatVersion=3 for IN-PROGRESS full-backup
// manifests written in the sidecar-checkpoint layout (ADR-0086, the
// task-#54 O(N²) manifest-rewrite fix): the base manifest is written
// once and per-chunk / per-table progress accrues as appended JSONL
// deltas in [ProgressSidecarRef.File]. The truth about a crashed run
// is base + sidecar replay; an older binary reading only the base
// would silently treat completed tables as not-started and — worse —
// finish a resume while leaving the new layout's sidecar behind. The
// version bump makes the older binary refuse LOUDLY at the manifest
// preflight instead (the ADR-0082 one-way-sentinel posture). The bump
// is proportional, mirroring Bug 116: FINALIZED manifests are
// re-stamped with [FormatVersionFor] (1 or 2) and carry no sidecar
// reference, so successful backups remain readable by older binaries.
//
// v0.99.175+ introduces FormatVersion=4 for schemas carrying
// STANDALONE sequences ([ir.Schema.Sequences], item-51 TRIAGE finding
// #1). Same Bug-116 class as FormatVersion=2: an older binary reading
// the manifest ignores the unknown Sequences field and restores a
// target without the sequence object — silently for a sequence no
// column references (app-driven nextval), or as a confusing mid-
// restore DDL failure for one that is (the column's carried
// `DEFAULT nextval(...)` names a relation that was never created).
// The bump makes the older binary refuse loudly at the manifest
// preflight instead, before anything lands on the target.
// Proportional as always: only manifests whose schema actually
// carries standalone sequences are stamped 4.
//
// v0.99.202+ introduces FormatVersion=5 for ENCRYPTED manifests
// (ADR-0152, audit N-8): chunks written under a FormatVersion-5
// manifest carry a GCM AAD binding to (manifest identity + chunk
// path), and the chain CEK's wrap is bound to the manifest identity
// (KMS EncryptionContext / GCP AAD / passphrase-wrap AAD; the
// versioned Azure kek_ref). Readers derive the decrypt shape from the
// RECORDED version — v5+ requires the binding, pre-v5 decrypts with
// the legacy nil-AAD path — never by trying both. Proportional per
// the Bug-116 discipline: plaintext manifests stay on 1/2/4 and keep
// restoring on older binaries; only encrypted backups (whose chunks
// an older binary would fail to decrypt with a MISLEADING bare
// auth-tag error anyway) are stamped 5, turning that into the loud
// version-refusal at the manifest preflight.
//
// v0.99.20x+ introduces FormatVersion=6 for SIGNED encrypted manifests
// (ADR-0154 Phase 1, audit N-8 residual): the manifest carries a
// DETACHED HMAC-SHA-256 signature (`<manifest>.sig`) keyed off a key
// HKDF-derived from the chain KEK, and the chain's lineage catalog
// carries a sibling `lineage.json.sig`. A v6 manifest ASSERTS it was
// signed: restore of an encrypted v6 chain (which always has the KEK, so
// it can always verify) refuses loudly on a missing/invalid signature
// ([SLUICE-E-BACKUP-SIGNATURE-MISSING] / [-INVALID]). Proportional per
// the Bug-116 discipline: signing is opt-in (`--sign`), so an ordinary
// encrypted backup stays on 5 and an unsigned one on 1/2/4; only a
// backup the operator asked to sign is stamped 6. Pre-v6 manifests
// carry no signature and restore normally (the version gate means
// "predates signing", not "untrusted").
const BackupFormatVersion = 6

// FormatVersionLegacy / FormatVersionSecurityMetadata name the
// historically-recorded values so callers don't sprinkle bare ints
// around. New code should use FormatVersionFor / chooseFormatVersion.
const (
	// FormatVersionLegacy is the pre-Bug-116 manifest version
	// (v0.16.x..v0.94.0). Carries schema metadata under the
	// "field-additions are forward-compatible" rule.
	FormatVersionLegacy = 1

	// FormatVersionSecurityMetadata is the v0.94.1+ version stamped
	// on manifests whose schema uses security or correctness fields
	// older binaries would silently drop. Bug 116 closure.
	FormatVersionSecurityMetadata = 2

	// FormatVersionProgressSidecar is the v0.99.39+ version stamped on
	// IN-PROGRESS full-backup manifests whose per-chunk / per-table
	// progress lives in the appended JSONL sidecar
	// ([Manifest.ProgressSidecar]) rather than in the base manifest
	// itself (ADR-0086). Older binaries refuse it loudly instead of
	// mis-resuming off a base that under-reports progress. Never
	// stamped on finalized manifests — those re-stamp with
	// [FormatVersionFor] and stay readable by older binaries.
	FormatVersionProgressSidecar = 3

	// FormatVersionStandaloneSequences is the v0.99.175+ version
	// stamped on manifests whose schema carries standalone sequences
	// ([ir.Schema.Sequences]). Older binaries would silently drop the
	// field on restore (the Bug 116 class — the restored target's
	// nextval() topology diverges from the source's); the bump makes
	// them refuse loudly at the manifest preflight instead.
	FormatVersionStandaloneSequences = 4

	// FormatVersionEncryptedChunkBinding is the v0.99.202+ version
	// stamped on ENCRYPTED manifests (ADR-0152, audit N-8/N-9). It is
	// the read-side gate for the integrity layer: chunks belonging to
	// a manifest at this version or above were written with the GCM
	// AAD binding ([ChunkAAD]) and their chain CEK wrap with the
	// identity binding ([CEKBinding]); readers MUST supply them.
	// Chunks belonging to older manifests decrypt via the legacy
	// nil-AAD path. The recorded version decides — never guess, never
	// try both. Stamped only when encryption is enabled (proportional
	// per Bug 116); the writers additionally never stamp it onto a
	// RESUMED pre-v5 encrypted run, whose kept chunks are unbound.
	FormatVersionEncryptedChunkBinding = 5

	// FormatVersionSignedManifest is the FormatVersion stamped on a
	// SIGNED encrypted manifest (ADR-0154 Phase 1). It is the read-side
	// gate for whole-manifest authentication: a manifest at this version
	// asserts a detached HMAC signature exists and must verify, so a
	// verifier holding the chain KEK refuses loudly on a
	// missing/invalid/rolled-back signature. Stamped only when signing
	// was requested AND the chain is encrypted (proportional per Bug
	// 116); an unsigned encrypted backup stays on
	// [FormatVersionEncryptedChunkBinding].
	FormatVersionSignedManifest = 6
)

// chooseFormatVersion returns the smallest manifest format version
// safe for the supplied schema: [FormatVersionStandaloneSequences]
// when the schema carries standalone sequences,
// [FormatVersionSecurityMetadata] when it uses any of the Bug-116
// security/correctness fields, [FormatVersionLegacy] otherwise. nil
// schema → legacy (a manifest with no schema can't carry security
// drift; degenerate cases stay maximally compatible).
//
// Closes Bug 116 by proportionally bumping only the manifests that
// actually carry the bumped-feature surface — innocent backups
// continue to restore on older binaries.
func chooseFormatVersion(s *ir.Schema) int {
	if s == nil {
		return FormatVersionLegacy
	}
	// Standalone sequences gate the highest tier, so they win over
	// the table-level security fields below.
	if len(s.Sequences) > 0 {
		return FormatVersionStandaloneSequences
	}
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		if t.RLSEnabled || t.RLSForced {
			return FormatVersionSecurityMetadata
		}
		if len(t.Policies) > 0 {
			return FormatVersionSecurityMetadata
		}
		if len(t.ExcludeConstraints) > 0 {
			return FormatVersionSecurityMetadata
		}
	}
	return FormatVersionLegacy
}

// FormatVersionFor is the exported wrapper around [chooseFormatVersion]
// so the pipeline package (which builds [Manifest] values directly)
// stamps the correct version without duplicating the detection logic.
func FormatVersionFor(s *ir.Schema) int { return chooseFormatVersion(s) }

// Manifest kind constants. String literals are part of the on-disk
// format; renaming requires a [BackupFormatVersion] bump.
const (
	// BackupKindFull marks a manifest produced by `sluice backup full`
	// — a self-contained snapshot of the source schema + every row.
	// Empty Kind is treated as full for backward compatibility with
	// v0.16.x manifests written before the field existed.
	BackupKindFull = "full"

	// BackupKindIncremental marks a manifest produced by `sluice
	// backup incremental` — a window of [ir.Change] events sourced from
	// the engine's CDC pump, replayed at restore time on top of a
	// parent (full or earlier incremental). Phase 3 of the logical-
	// backup feature; see `docs/dev/design/logical-backups-phase-3.md`.
	BackupKindIncremental = "incremental"
)

// SchemaDeltaKind enumerates the kinds of schema changes recorded in
// [Manifest.SchemaDelta]. String literals are part of the on-disk
// format.
const (
	// SchemaDeltaAddTable records a CREATE TABLE event observed
	// during an incremental's window. Before is nil; After holds the
	// post-CREATE table shape.
	SchemaDeltaAddTable = "add_table"

	// SchemaDeltaDropTable records a DROP TABLE event. Before holds
	// the pre-DROP table shape; After is nil.
	SchemaDeltaDropTable = "drop_table"

	// SchemaDeltaAlterTable records a structural change to an existing
	// table (column added, removed, retyped). Before and After both
	// non-nil; restore replays whichever ALTER fragments the engine's
	// schema-delta applier supports.
	SchemaDeltaAlterTable = "alter_table"
)

// SchemaDeltaEntry is one structural change observed during an
// incremental backup's window. Restore-side replay applies these
// against the target schema before streaming the incremental's row
// events; the order in [Manifest.SchemaDelta] is the order of
// observation.
//
// Before and After are full table shapes (not column-level diffs) so
// the restore-side applier can decide its own ALTER strategy without
// re-deriving the source intent from a column-level patch. For
// SchemaDeltaAddTable, Before is nil; for SchemaDeltaDropTable, After
// is nil; for SchemaDeltaAlterTable, both are non-nil.
type SchemaDeltaEntry struct {
	// Kind is one of SchemaDeltaAddTable / SchemaDeltaDropTable /
	// SchemaDeltaAlterTable.
	Kind string `json:"kind"`

	// Schema is the namespace the table belongs to (Postgres). Empty
	// for flat-scope engines (MySQL).
	Schema string `json:"schema,omitempty"`

	// Table is the unqualified table name.
	Table string `json:"table"`

	// Before is the table's shape at the start of the incremental's
	// window. Nil for SchemaDeltaAddTable.
	Before *ir.Table `json:"before,omitempty"`

	// After is the table's shape at the end of the incremental's
	// window. Nil for SchemaDeltaDropTable.
	After *ir.Table `json:"after,omitempty"`
}

// SchemaHistoryEntry is one ADR-0049 (Chunk D) per-table schema-history
// version observed during an incremental backup's window. Restore-side
// replay seeds the target's sluice_cdc_schema_history with these so a
// stream resumed at the backup's EndPosition can resolve the schema in
// effect there via [ir.ResolveSchemaVersion]. Append-only — older readers
// ignore unknown fields (the documented pre-Chunk-D state: a restore +
// resume falls to the loud ADR-0022 cold-start floor, never silent).
type SchemaHistoryEntry struct {
	// StreamID is the CDC stream this version belongs to (mirrors
	// sluice_cdc_schema_history.stream_id).
	StreamID string `json:"stream_id"`

	// Schema/Table identify the affected table; AnchorPosition is the
	// boundary event's own position captured at detection (HP-3).
	Schema         string      `json:"schema"`
	Table          string      `json:"table"`
	AnchorPosition ir.Position `json:"anchor_position"`

	// TableJSON is the post-DDL ir.Table serialised via [ir.MarshalTable]
	// — byte-identical to what the engine stores in
	// sluice_cdc_schema_history.ir_schema_json (locked decision #1).
	TableJSON []byte `json:"table_json"`
}

// Store is the storage abstraction for logical backups. Phase 1
// ships a single implementation ([blobcodec.LocalStore]) backed by the
// local filesystem; Phase 2 will add S3, GCS, and Azure Blob backends
// behind the same interface so the writer / restore paths don't change.
//
// The interface is small by design: four methods covers backup writes
// (Put), restore reads (Get + List), and retention pruning (Delete,
// Phase 2+). Streaming I/O on Put / Get keeps memory bounded for
// arbitrarily-large chunk files.
//
// Path conventions: paths are forward-slash separated and relative to
// the store's root (operators name the root via `--output-dir` /
// `--from-dir` / `s3://bucket/prefix/`). The store is responsible for
// translating to backend-native conventions (Windows backslashes for
// LocalStore, object keys for S3, etc.).
type Store interface {
	// Put writes the contents of r to the named path within the store.
	// Implementations buffer / stream as appropriate; callers SHOULD
	// pass a reader that doesn't require seeking. Existing content at
	// path is overwritten.
	Put(ctx context.Context, path string, r io.Reader) error

	// Get returns a reader for the contents of path. The caller is
	// responsible for closing the returned ReadCloser.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// List returns every path within the store whose key starts with
	// prefix. Paths are returned in unspecified order; callers sort if
	// they care. Empty prefix returns every path.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes path from the store. Idempotent — deleting a
	// non-existent path returns nil. Used by Phase 2+ retention
	// pruning; Phase 1 backups don't auto-prune.
	Delete(ctx context.Context, path string) error

	// Exists reports whether a blob is present at path. Phase 2's
	// resumable backup writer uses this to decide whether to skip
	// re-uploading a chunk on restart. A "not present" result is
	// (false, nil) — callers reserve the error return for transport
	// or auth failures, not for a missing key.
	Exists(ctx context.Context, path string) (bool, error)
}

// Manifest is the serialised public contract of a backup. Lives at
// `manifest.json` at the root of the backup output directory; restore
// reads it first to discover the schema and chunk layout. Operators
// can `cat manifest.json | jq` to inspect a backup without sluice.
//
// Field-renames are permanent and require a [BackupFormatVersion]
// bump. Field-additions are forward-compatible (older sluice ignores
// unknown fields) and don't require a version bump.
type Manifest struct {
	// FormatVersion identifies the manifest schema. Older sluice
	// refuses newer values with a clear error message; newer sluice
	// always accepts older values.
	FormatVersion int `json:"format_version"`

	// SluiceVersion is the build identifier of the sluice binary that
	// produced the backup. Informational — restore doesn't gate on
	// it. Useful for "which sluice version produced this archive"
	// debugging.
	SluiceVersion string `json:"sluice_version"`

	// CreatedAt is the wall-clock timestamp the backup started. UTC,
	// RFC3339 with nanosecond precision.
	CreatedAt time.Time `json:"created_at"`

	// SourceEngine is the engine name (e.g. "mysql", "postgres") the
	// schema and rows were read from. Restore reads this so it can
	// route the schema through `translate.RetargetForEngine` when
	// the operator asks for cross-engine restore.
	SourceEngine string `json:"source_engine"`

	// Schema is the full source schema, serialised via the tagged-
	// union JSON envelope so the IR's sealed interfaces round-trip.
	// On restore the schema can be re-targeted for a different engine
	// before being applied.
	Schema *ir.Schema `json:"schema"`

	// Tables lists every table that was backed up, with its row count
	// and the chunk files that contain its data. Order matches the
	// schema's table order.
	Tables []*TableManifest `json:"tables"`

	// PartialState records whether the backup represented by this
	// manifest finished successfully. Set to "in_progress" after each
	// table completes (so a crash leaves a per-table-level resumable
	// checkpoint on disk) and to "complete" only when the full backup
	// finishes. The empty string is treated the same as "complete" for
	// forward-compat with Phase-1 manifests written before this field
	// existed.
	//
	// Phase 2 resume semantics (see internal/pipeline/backup.go):
	//
	//   - "complete" / "" → re-running into the same destination
	//     refuses unless --force-overwrite is set.
	//   - "in_progress" → re-running resumes from the next un-completed
	//     table; chunks already present on the store with matching
	//     SHA-256 are skipped, mismatched ones are overwritten.
	PartialState string `json:"partial_state,omitempty"`

	// ProgressSidecar, when non-nil on an "in_progress" manifest,
	// declares that this base manifest's per-chunk / per-table progress
	// lives in the named append-only JSONL sidecar (ADR-0086): the
	// truth about the crashed run is base + replay of the sidecar's
	// matching-attempt events ([ReplayProgress]). Readers that skip the
	// replay under-report progress, which is why manifests carrying
	// this field are stamped [FormatVersionProgressSidecar] — older
	// binaries refuse loudly. Always nil on finalized manifests (the
	// final write folds the progress back into Tables, clears this
	// field, and deletes the sidecar).
	ProgressSidecar *ProgressSidecarRef `json:"progress_sidecar,omitempty"`

	// BackupID is a deterministic identifier for this manifest,
	// derived from CreatedAt + SourceEngine + Kind + (for incrementals)
	// EndPosition. Used to link incrementals to their parent via
	// [ParentBackupID] without depending on the manifest's URL or
	// filename. Empty in pre-v0.17.0 manifests; restore treats those
	// as orphan fulls.
	BackupID string `json:"backup_id,omitempty"`

	// Kind is the manifest's flavour: [BackupKindFull] or
	// [BackupKindIncremental]. Empty is treated as full for
	// backward-compat with v0.16.x manifests.
	Kind string `json:"kind,omitempty"`

	// ParentBackupID is the [BackupID] of the manifest this
	// incremental was taken on top of. Empty for fulls; required for
	// incrementals (chain restore validates the link). Same chain may
	// reference a full or another incremental; the restore-side walk
	// follows the chain via this field.
	ParentBackupID string `json:"parent_backup_id,omitempty"`

	// StartPosition is the source-engine CDC position the incremental
	// began streaming from. For an incremental whose parent is a full,
	// this equals the full's [EndPosition] (= the snapshot point). For
	// an incremental whose parent is another incremental, this equals
	// the parent's [EndPosition]. The restore-side walk validates the
	// equality before applying the chain.
	//
	// Empty on full manifests (Phase 3 didn't grow snapshot-position
	// recording into the full path; that's a Phase 3.3 follow-up so
	// `--position-from-manifest` can be a uniform read).
	//
	// The position's engine-tagged opaque token carries the
	// engine-specific bookmark: Postgres LSN under [pgPos], MySQL
	// binlog file/pos or GTID set under [binlogPos]. Operators
	// inspecting the manifest see the JSON-encoded form; sluice
	// decodes it through the same engine-side helpers the CDC
	// reader uses.
	StartPosition ir.Position `json:"start_position,omitempty"`

	// EndPosition is the source-engine CDC position the manifest
	// resolves to as a chain-handoff resume point.
	//
	//   - For incremental manifests, this is the source position the
	//     incremental's CDC pump stopped at. The next incremental in
	//     the chain starts from here, and `sluice sync start
	//     --position-from-manifest` resumes CDC from here against a
	//     freshly-restored target.
	//   - For full manifests written by v0.17.0–v0.17.3, this is the
	//     source position captured AFTER the row sweep completed (an
	//     end-of-backup reading). Writes that landed on already-read
	//     tables during the backup window are NOT in the row chunks
	//     AND are NOT in the first incremental's `--since` window
	//     (their LSNs are before this captured EndPosition) — the
	//     v0.17.2 release notes documented this caveat.
	//   - For full manifests written by v0.18.0+, this is the source
	//     position captured AT snapshot START — the cross-table
	//     consistent read view's anchor LSN/GTID. The row sweep reads
	//     the source as it appeared at this position; CDC from this
	//     position forward covers every write after the snapshot.
	//     The during-backup window gap is closed.
	//   - For a RESUMED full (task #42, ADR-0085), this is the FIRST
	//     interrupted attempt's snapshot anchor, adopted verbatim — the
	//     minimum anchor across attempts. Kept tables are exact at that
	//     anchor; tables (re-)streamed by the resumed run are read at a
	//     later snapshot, so their contents overlap the chain's replay
	//     window — sound because the chain appliers are idempotent on
	//     a key (ADR-0010; truly keyless re-streams are refused at
	//     resume). In-progress full manifests carry the anchor from
	//     their first write so a crash can never lose it.
	//
	// Pre-v0.17.0 full manifests carry an empty value here; the
	// chain-walk treats them as orphan fulls.
	EndPosition ir.Position `json:"end_position,omitempty"`

	// SchemaHash is a deterministic fingerprint of [ir.Schema] at the
	// point the manifest was written. Computed via SHA-256 over the
	// canonical JSON serialisation ([ComputeSchemaHash]). Chain-restore
	// recomputes it from the carried Schema and refuses on mismatch
	// (ChainRestore.verifySchemaHashes) — a CORRUPTION check, not
	// tamper-proofing: the hash lives in the same unsigned manifest as
	// the schema, so an adversary who can rewrite one can rewrite both
	// (ADR-0152 documents the boundary). Empty on manifests written
	// before the field existed; the check skips those.
	SchemaHash string `json:"schema_hash,omitempty"`

	// SchemaDelta records structural changes observed during an
	// incremental's window. Empty when no DDL ran on the source
	// during the window. Restore-side applies entries in slice order
	// before streaming the incremental's row events.
	SchemaDelta []*SchemaDeltaEntry `json:"schema_delta,omitempty"`

	// SchemaHistory is the ADR-0049 (Chunk D) per-table schema-history
	// versions emitted during this backup's window. On restore + resume
	// these are reloaded into the target's sluice_cdc_schema_history so
	// a stream resuming at the backup's EndPosition can resolve the
	// schema in effect there (without it, every resumed event before
	// the first post-restore DDL hits the loud ErrPositionInvalid cold-
	// start floor — the documented pre-Chunk-D state). Append-only,
	// older readers ignore (zero BackupFormatVersion bump per locked
	// decision #1).
	SchemaHistory []*SchemaHistoryEntry `json:"schema_history,omitempty"`

	// ChangeChunks lists the chunk files containing serialised
	// [ir.Change] events for an incremental backup. Empty for full
	// manifests; populated for incrementals as the writer rolls
	// chunks during the window. Path conventions mirror full
	// backups' table chunks but live under `chunks/_changes/`.
	ChangeChunks []*ChunkInfo `json:"change_chunks,omitempty"`

	// ChainEncryption, when non-nil, identifies this manifest's chain
	// as encrypted under Phase 6 client-side envelope encryption. Empty
	// (the zero default) means plaintext chunks — the v0.16.x..v0.21.x
	// shape, preserved for backward compatibility.
	//
	// On encrypted chains the field is populated on the full manifest
	// (the chain root); incremental manifests inherit the chain-level
	// settings by reference. The chain walker reads the full's
	// [ChainEncryption] to set up the [crypto.EnvelopeEncryption] used
	// to decrypt every chunk in the chain.
	ChainEncryption *ChainEncryption `json:"chain_encryption,omitempty"`
}

// Manifest partial-state constants. String literals are part of the
// on-disk format; renaming requires a BackupFormatVersion bump.
const (
	BackupStateInProgress = "in_progress"
	BackupStateComplete   = "complete"
)

// TableManifest is one entry within [Manifest.Tables]. Carries the
// row count (load-bearing for restore-time row-count verification —
// layer 2 in the proto-ADR's "100% confidence" story) and the per-
// chunk metadata.
type TableManifest struct {
	// Name is the table's identifier, matching `Schema.Tables[i].Name`.
	// Schema-qualified for engines with namespaced schemas (Postgres);
	// bare name for flat-scope engines (MySQL).
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name"`

	// RowCount is the total number of rows the writer recorded across
	// every chunk for this table. Restore compares this against the
	// sum of [ChunkInfo.RowCount] when streaming chunks, and against
	// the actual delivered count after restore completes.
	RowCount int64 `json:"row_count"`

	// Partial reports whether this entry is a mid-table per-chunk
	// checkpoint (v0.16.1+) rather than a fully-completed table. True
	// means the row stream was killed before reaching EOF — the chunks
	// listed are the chunks completed so far. Default (false / omitted)
	// means the entry represents a fully-completed table; Phase-1 and
	// v0.16.0 manifests carry no partial information so the omitted
	// default preserves backward-compat (those entries were only ever
	// persisted at table boundaries).
	Partial bool `json:"partial,omitempty"`

	// Chunks are the chunk files for this table, in write order.
	// Empty when the table is empty (no chunk file is created in
	// that case to avoid clutter).
	Chunks []*ChunkInfo `json:"chunks,omitempty"`
}

// ChunkInfo describes a single chunk file within a table backup.
// The path is relative to the store's root; SHA-256 covers the
// uncompressed-on-disk byte stream so a corrupted chunk surfaces
// at restore time as a hash mismatch (loud-failure tenet).
type ChunkInfo struct {
	// File is the relative path of the chunk file within the backup
	// root. Forward-slash separated regardless of platform.
	File string `json:"file"`

	// RowCount is the number of rows this chunk contains.
	RowCount int64 `json:"row_count"`

	// SHA256 is the hex-encoded SHA-256 of the chunk file's bytes
	// AS WRITTEN TO STORAGE (i.e. after compression AND encryption,
	// when encryption is enabled — `backup verify` is sha256-only and
	// must match what's on disk). Restore computes the hash on the
	// bytes it reads back and compares; any mismatch is a hard failure.
	SHA256 string `json:"sha256"`

	// Encryption, when non-nil, marks this chunk as encrypted under
	// Phase 6 client-side envelope encryption. Empty (the zero default)
	// means plaintext — the v0.16.x..v0.21.x shape, preserved for
	// backward compatibility.
	//
	// In per-chain mode, [ChunkEncryption.WrappedCEK] is empty and the
	// chunk reader uses the chain-level CEK from
	// [Manifest.ChainEncryption.WrappedCEK]. In per-chunk mode,
	// WrappedCEK carries this chunk's own wrapped CEK.
	Encryption *ChunkEncryption `json:"encryption,omitempty"`
}

// ChunkEncryption is the per-chunk Phase 6 encryption metadata. Empty
// (nil pointer) means the chunk is plaintext; non-empty means the
// chunk's bytes are AES-256-GCM ciphertext shaped as
// `[nonce | ciphertext | authtag]`.
//
// Field-additions are forward-compatible (older sluice ignores unknown
// fields); the chunk-format version on the manifest gates renames /
// removals.
type ChunkEncryption struct {
	// Algorithm names the bulk cipher. Phase 6.1 ships only
	// "AES-256-GCM"; future revisions may add ChaCha20-Poly1305.
	Algorithm string `json:"algorithm,omitempty"`

	// NonceLen is the byte length of the per-chunk random nonce that
	// prefixes the ciphertext. 12 for AES-256-GCM (NIST recommended).
	// Recorded explicitly so a future revision could vary it without
	// breaking older readers.
	NonceLen int `json:"nonce_len,omitempty"`

	// AuthTagLen is the byte length of the AES-GCM auth tag that
	// follows the ciphertext. 16 for AES-256-GCM. Same forward-compat
	// rationale as NonceLen.
	AuthTagLen int `json:"auth_tag_len,omitempty"`

	// WrappedCEK is the per-chunk wrapped Content Encryption Key.
	// Empty means "use the chain-level CEK" (per-chain mode);
	// non-empty means this chunk has its own CEK (per-chunk mode).
	WrappedCEK []byte `json:"wrapped_cek,omitempty"`
}

// ChainEncryption is the chain-level Phase 6 encryption metadata. Empty
// (nil pointer) means the chain is plaintext; non-empty means every
// chunk in the chain is encrypted under the recorded shape.
//
// The chain root (full manifest) carries this field; incremental
// manifests inherit by reference. Mixed-mode chains (some chunks
// encrypted, some not) are not supported — the chain-walker refuses
// them at restore time.
type ChainEncryption struct {
	// Algorithm names the bulk cipher used by the chunk codec.
	// Phase 6.1: "AES-256-GCM".
	Algorithm string `json:"algorithm,omitempty"`

	// Mode is "per-chain" or "per-chunk" — see the Phase 6 design.
	// Per-chain (default) wraps a single CEK at the chain header and
	// reuses it for every chunk; per-chunk wraps a fresh CEK per
	// chunk for defense-in-depth.
	Mode string `json:"mode,omitempty"`

	// KEKMode identifies the Key Encryption Key derivation/access mode.
	// Phase 6.1: "passphrase-argon2id". Phase 6.2/6.3 will add
	// "aws-kms" / "gcp-kms" / "azure-keyvault".
	KEKMode string `json:"kek_mode,omitempty"`

	// KEKRef is the operator-visible reference to the KEK material.
	// For passphrase mode: empty (the salt + Argon2id params in
	// [ChainEncryption.Argon2id] is the reference). For KMS modes:
	// the key ARN / resource name.
	KEKRef string `json:"kek_ref,omitempty"`

	// WrappedCEK is the chain-level wrapped Content Encryption Key,
	// populated in per-chain mode. Empty in per-chunk mode (each
	// [ChunkEncryption.WrappedCEK] carries the wrap there).
	WrappedCEK []byte `json:"wrapped_cek,omitempty"`

	// Argon2id, populated when [KEKMode] == "passphrase-argon2id",
	// records the KEK-derivation params used so a restore-side
	// PassphraseEnvelope can re-derive the same KEK from the
	// operator's passphrase.
	Argon2id *Argon2idParams `json:"argon2id,omitempty"`
}

// Argon2idParams matches the wire shape used by the Phase 6.1
// passphrase-mode KEK derivation. See `internal/crypto/envelope.go`
// for the runtime equivalent. Fields are mirrored here so the
// manifest IR doesn't depend on the crypto package.
type Argon2idParams struct {
	// Salt is the per-chain Argon2id salt. 16 bytes; recorded so a
	// restore-side passphrase can re-derive the same KEK.
	Salt []byte `json:"salt,omitempty"`

	// Memory is the Argon2id memory cost, in KiB. Default 65536
	// (64 MiB).
	Memory uint32 `json:"memory_kib,omitempty"`

	// Iterations is the Argon2id time-cost parameter. Default 3.
	Iterations uint32 `json:"iterations,omitempty"`

	// Parallelism is the Argon2id parallelism parameter. Default 4.
	Parallelism uint8 `json:"parallelism,omitempty"`

	// KeyLen is the derived key length. Always 32 for AES-256-GCM in
	// Phase 6.1; explicit so a future cipher swap can be recorded.
	KeyLen uint32 `json:"key_len,omitempty"`
}
