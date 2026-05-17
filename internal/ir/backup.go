// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Logical-backup primitives shared between the writer (`sluice backup`)
// and reader (`sluice restore`) sides of the Phase 1 backup feature.
//
// Three things live here:
//
//   - The [BackupStore] interface — the storage abstraction every
//     backend (local-FS today, S3 / GCS / Azure in Phase 2) plugs into.
//     Designed for cloud backends from day one even though Phase 1
//     ships only the [pipeline.LocalStore] implementation.
//   - The [Manifest] / [TableManifest] / [ChunkInfo] types — the
//     serialised public contract of a backup directory. Operators
//     interact with this via `sluice backup verify`; tooling depends
//     on it; restore reads it first. The format-version field is the
//     load-bearing forward-compat anchor — older sluice refuses
//     newer manifests, newer sluice always reads older.
//   - JSON round-tripping for the IR's sealed [Type] / [DefaultValue]
//     interfaces. Without this, `Schema.Tables[i].Columns[j].Type`
//     can't survive a round-trip through `encoding/json` (it's a
//     sealed interface; the decoder has no way to recover the
//     concrete type). The tagged-union envelope keeps the manifest
//     human-readable while round-tripping unambiguously.
//
// What's deliberately out of scope here:
//
//   - Encryption — Phase 6. Phase 1 backups rest on disk unencrypted;
//     operators relying on filesystem-level encryption (LUKS /
//     BitLocker / FileVault) carry that responsibility today.
//   - Incremental backups — Phase 3. The format-version field will
//     bump when that lands.
//   - Cloud backends — Phase 2. `BackupStore` is here so the orchestrator
//     code in `internal/pipeline` doesn't need re-shaping when S3 /
//     GCS / Azure implementations land; only the implementations are
//     missing.
//   - Compression algorithm choice — Phase 1 uses gzip via stdlib.
//     Phase 2 may swap to zstd if benchmarks show it matters.
//
// See `docs/dev/design-logical-backups.md` for the full design.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// BackupFormatVersion is the integer version of the manifest schema.
// Bumped whenever a non-additive change is made (a field added or
// removed in a way that older readers couldn't safely ignore). Older
// sluice refuses newer manifests with a clear error; newer sluice
// always reads older.
//
// v0.16.x manifests carry FormatVersion=1 (full backups only). v0.17.0
// keeps FormatVersion=1: every Phase 3 addition is forward-compatible
// (older sluice ignores [Manifest.Kind]/[Manifest.ParentBackupID]/etc.
// — those manifests appear as orphan fulls when read by an older
// binary, which is the right degraded behaviour for incrementals
// nobody can chain anyway). The version bumps when a future change
// would break older readers.
const BackupFormatVersion = 1

// Manifest kind constants. String literals are part of the on-disk
// format; renaming requires a [BackupFormatVersion] bump.
const (
	// BackupKindFull marks a manifest produced by `sluice backup full`
	// — a self-contained snapshot of the source schema + every row.
	// Empty Kind is treated as full for backward compatibility with
	// v0.16.x manifests written before the field existed.
	BackupKindFull = "full"

	// BackupKindIncremental marks a manifest produced by `sluice
	// backup incremental` — a window of [Change] events sourced from
	// the engine's CDC pump, replayed at restore time on top of a
	// parent (full or earlier incremental). Phase 3 of the logical-
	// backup feature; see `docs/dev/design-logical-backups-phase-3.md`.
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
	Before *Table `json:"before,omitempty"`

	// After is the table's shape at the end of the incremental's
	// window. Nil for SchemaDeltaDropTable.
	After *Table `json:"after,omitempty"`
}

// BackupStore is the storage abstraction for logical backups. Phase 1
// ships a single implementation ([pipeline.LocalStore]) backed by the
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
type BackupStore interface {
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
	Schema *Schema `json:"schema"`

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
	StartPosition Position `json:"start_position,omitempty"`

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
	//
	// Pre-v0.17.0 full manifests carry an empty value here; the
	// chain-walk treats them as orphan fulls.
	EndPosition Position `json:"end_position,omitempty"`

	// SchemaHash is a deterministic fingerprint of [Schema] at the
	// point the manifest was written. Computed via SHA-256 over the
	// canonical JSON serialisation (encoding/json with the existing
	// IR marshalling). Used by chain-restore as a sanity check that
	// the chain's schema lineage hasn't been tampered with.
	SchemaHash string `json:"schema_hash,omitempty"`

	// SchemaDelta records structural changes observed during an
	// incremental's window. Empty when no DDL ran on the source
	// during the window. Restore-side applies entries in slice order
	// before streaming the incremental's row events.
	SchemaDelta []*SchemaDeltaEntry `json:"schema_delta,omitempty"`

	// ChangeChunks lists the chunk files containing serialised
	// [Change] events for an incremental backup. Empty for full
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

// MarshalJSON for [Schema] uses the tagged-union envelope so the
// sealed Type / DefaultValue interfaces round-trip through standard
// encoding/json. Same wire shape as the in-memory struct, but with
// every Column / DefaultValue / Type wrapped in a tagged envelope.
//
// We don't customise marshal at the Schema level; instead we marshal
// each component via its own MarshalJSON below. Schema's natural
// struct shape is sufficient because Tables / Views are concrete
// pointer slices, not interface slices.

// schemaTypeEnvelope is the tagged-union form a [Type] takes on the
// wire: a `kind` discriminator plus the type's natural fields. The
// decoder branches on Kind to recover the concrete type.
type schemaTypeEnvelope struct {
	Kind string `json:"kind"`

	// Numeric / bit-width fields (Integer, Float, etc.).
	Width         int8  `json:"width,omitempty"`
	Unsigned      bool  `json:"unsigned,omitempty"`
	AutoIncrement bool  `json:"auto_increment,omitempty"`
	Precision     int   `json:"precision,omitempty"`
	Scale         int   `json:"scale,omitempty"`
	FloatPrec     uint8 `json:"float_precision,omitempty"`

	// String / byte fields (Char, Varchar, Text, Binary, Varbinary, Blob).
	Length    int    `json:"length,omitempty"`
	Charset   string `json:"charset,omitempty"`
	Collation string `json:"collation,omitempty"`
	TextSize  uint8  `json:"text_size,omitempty"`
	BlobSize  uint8  `json:"blob_size,omitempty"`

	// Temporal fields (Time, DateTime, Timestamp). Precision is reused.
	WithTimeZone bool `json:"with_time_zone,omitempty"`

	// JSON.
	Binary bool `json:"binary,omitempty"`

	// Decimal arbitrary-precision (catalog Bug 69). True for bare
	// `numeric`/`decimal` with no declared precision/scale. Append-only;
	// older sluice ignores it and reads the column as numeric(0,0),
	// which is the pre-fix lossy behaviour — acceptable for forward
	// compat since manifests are produced and consumed by the same
	// build in practice.
	DecimalUnconstrained bool `json:"decimal_unconstrained,omitempty"`

	// Enum / Set values. Empty for other types.
	Values []string `json:"values,omitempty"`

	// Geometry.
	GeometrySubtype uint8 `json:"geometry_subtype,omitempty"`
	SRID            int   `json:"srid,omitempty"`
	IsGeography     bool  `json:"is_geography,omitempty"`
	HasZ            bool  `json:"has_z,omitempty"`
	HasM            bool  `json:"has_m,omitempty"`

	// Array recursive.
	Element json.RawMessage `json:"element,omitempty"`

	// ExtensionType (ADR-0032) and VerbatimType (ADR-0047). Extension /
	// Name / Modifiers carry the catalogued-extension shape;
	// VerbatimDefinition carries the uncatalogued verbatim PG type
	// spelling. New fields are append-only (older sluice ignores them);
	// no existing field was renamed/renumbered.
	Extension          string `json:"extension,omitempty"`
	Name               string `json:"name,omitempty"`
	Modifiers          []int  `json:"modifiers,omitempty"`
	VerbatimDefinition string `json:"verbatim_definition,omitempty"`
}

// MarshalType renders an IR [Type] as a tagged-union JSON envelope.
// Used by the manifest writer; exported so backup-format tooling can
// reuse the encoding without copying it.
func MarshalType(t Type) ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	env := schemaTypeEnvelope{}
	switch v := t.(type) {
	case Boolean:
		env.Kind = "Boolean"
	case Integer:
		env.Kind = "Integer"
		env.Width = v.Width
		env.Unsigned = v.Unsigned
		env.AutoIncrement = v.AutoIncrement
	case Decimal:
		env.Kind = "Decimal"
		env.Precision = v.Precision
		env.Scale = v.Scale
		env.DecimalUnconstrained = v.Unconstrained
	case Float:
		env.Kind = "Float"
		env.FloatPrec = uint8(v.Precision)
	case Char:
		env.Kind = "Char"
		env.Length = v.Length
		env.Charset = v.Charset
		env.Collation = v.Collation
	case Varchar:
		env.Kind = "Varchar"
		env.Length = v.Length
		env.Charset = v.Charset
		env.Collation = v.Collation
	case Text:
		env.Kind = "Text"
		env.TextSize = uint8(v.Size)
		env.Charset = v.Charset
		env.Collation = v.Collation
	case Binary:
		env.Kind = "Binary"
		env.Length = v.Length
	case Varbinary:
		env.Kind = "Varbinary"
		env.Length = v.Length
	case Blob:
		env.Kind = "Blob"
		env.BlobSize = uint8(v.Size)
	case Date:
		env.Kind = "Date"
	case Time:
		env.Kind = "Time"
		env.Precision = v.Precision
		env.WithTimeZone = v.WithTimeZone
	case DateTime:
		env.Kind = "DateTime"
		env.Precision = v.Precision
	case Timestamp:
		env.Kind = "Timestamp"
		env.Precision = v.Precision
		env.WithTimeZone = v.WithTimeZone
	case JSON:
		env.Kind = "JSON"
		env.Binary = v.Binary
	case Enum:
		env.Kind = "Enum"
		env.Values = v.Values
	case Set:
		env.Kind = "Set"
		env.Values = v.Values
	case UUID:
		env.Kind = "UUID"
	case Array:
		env.Kind = "Array"
		if v.Element != nil {
			elem, err := MarshalType(v.Element)
			if err != nil {
				return nil, fmt.Errorf("array element: %w", err)
			}
			env.Element = elem
		}
	case Geometry:
		env.Kind = "Geometry"
		env.GeometrySubtype = uint8(v.Subtype)
		env.SRID = v.SRID
		env.IsGeography = v.IsGeography
		env.HasZ = v.HasZ
		env.HasM = v.HasM
	case Inet:
		env.Kind = "Inet"
	case Cidr:
		env.Kind = "Cidr"
	case Macaddr:
		env.Kind = "Macaddr"
	case VerbatimType:
		// ADR-0047: uncatalogued PG extension type carried verbatim.
		// PG-restore-only; the lineage-segment marker + restore-time
		// engine gate enforce that. Round-trips the exact format_type
		// spelling so a PG restore re-creates the column identically.
		env.Kind = "VerbatimType"
		env.VerbatimDefinition = v.Definition
	default:
		return nil, fmt.Errorf("unsupported IR type for backup encoding: %T", t)
	}
	return json.Marshal(env)
}

// UnmarshalType decodes a tagged-union JSON envelope back to a
// concrete IR [Type]. Returns nil and a clear error for unrecognised
// kinds — adding a new IR type means adding a branch here AND in
// [MarshalType].
func UnmarshalType(b []byte) (Type, error) {
	if len(b) == 0 || string(b) == "null" {
		return nil, nil
	}
	var env schemaTypeEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode type envelope: %w", err)
	}
	switch env.Kind {
	case "Boolean":
		return Boolean{}, nil
	case "Integer":
		return Integer{Width: env.Width, Unsigned: env.Unsigned, AutoIncrement: env.AutoIncrement}, nil
	case "Decimal":
		return Decimal{Precision: env.Precision, Scale: env.Scale, Unconstrained: env.DecimalUnconstrained}, nil
	case "Float":
		return Float{Precision: FloatPrecision(env.FloatPrec)}, nil
	case "Char":
		return Char{Length: env.Length, Charset: env.Charset, Collation: env.Collation}, nil
	case "Varchar":
		return Varchar{Length: env.Length, Charset: env.Charset, Collation: env.Collation}, nil
	case "Text":
		return Text{Size: TextSize(env.TextSize), Charset: env.Charset, Collation: env.Collation}, nil
	case "Binary":
		return Binary{Length: env.Length}, nil
	case "Varbinary":
		return Varbinary{Length: env.Length}, nil
	case "Blob":
		return Blob{Size: BlobSize(env.BlobSize)}, nil
	case "Date":
		return Date{}, nil
	case "Time":
		return Time{Precision: env.Precision, WithTimeZone: env.WithTimeZone}, nil
	case "DateTime":
		return DateTime{Precision: env.Precision}, nil
	case "Timestamp":
		return Timestamp{Precision: env.Precision, WithTimeZone: env.WithTimeZone}, nil
	case "JSON":
		return JSON{Binary: env.Binary}, nil
	case "Enum":
		return Enum{Values: env.Values}, nil
	case "Set":
		return Set{Values: env.Values}, nil
	case "UUID":
		return UUID{}, nil
	case "Array":
		var elem Type
		if len(env.Element) > 0 && string(env.Element) != "null" {
			var err error
			elem, err = UnmarshalType(env.Element)
			if err != nil {
				return nil, fmt.Errorf("array element: %w", err)
			}
		}
		return Array{Element: elem}, nil
	case "Geometry":
		return Geometry{
			Subtype:     GeometrySubtype(env.GeometrySubtype),
			SRID:        env.SRID,
			IsGeography: env.IsGeography,
			HasZ:        env.HasZ,
			HasM:        env.HasM,
		}, nil
	case "Inet":
		return Inet{}, nil
	case "Cidr":
		return Cidr{}, nil
	case "Macaddr":
		return Macaddr{}, nil
	case "VerbatimType":
		// ADR-0047. Recover the exact PG type spelling. Decode is
		// engine-agnostic; the restore-time engine gate (checked before
		// any data moves) refuses a non-PG target loudly.
		return VerbatimType{Definition: env.VerbatimDefinition}, nil
	default:
		return nil, fmt.Errorf("unknown IR type kind %q in backup", env.Kind)
	}
}

// defaultValueEnvelope is the tagged-union form a [DefaultValue] takes
// on the wire.
type defaultValueEnvelope struct {
	Kind    string `json:"kind"`
	Value   string `json:"value,omitempty"`
	Expr    string `json:"expr,omitempty"`
	Dialect string `json:"dialect,omitempty"`
}

// MarshalDefault renders a [DefaultValue] as a tagged-union envelope.
func MarshalDefault(d DefaultValue) ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}
	switch v := d.(type) {
	case DefaultNone:
		return json.Marshal(defaultValueEnvelope{Kind: "None"})
	case DefaultLiteral:
		return json.Marshal(defaultValueEnvelope{Kind: "Literal", Value: v.Value})
	case DefaultExpression:
		return json.Marshal(defaultValueEnvelope{Kind: "Expression", Expr: v.Expr, Dialect: v.Dialect})
	default:
		return nil, fmt.Errorf("unsupported DefaultValue type for backup encoding: %T", d)
	}
}

// UnmarshalDefault decodes a tagged-union envelope back to a
// concrete [DefaultValue]. nil JSON or zero-length input returns
// DefaultNone — matches the IR convention that an absent default
// is the same as "no default".
func UnmarshalDefault(b []byte) (DefaultValue, error) {
	if len(b) == 0 || string(b) == "null" {
		return DefaultNone{}, nil
	}
	var env defaultValueEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode default envelope: %w", err)
	}
	switch env.Kind {
	case "", "None":
		return DefaultNone{}, nil
	case "Literal":
		return DefaultLiteral{Value: env.Value}, nil
	case "Expression":
		return DefaultExpression{Expr: env.Expr, Dialect: env.Dialect}, nil
	default:
		return nil, fmt.Errorf("unknown DefaultValue kind %q in backup", env.Kind)
	}
}

// columnWire is the on-wire JSON shape for [Column]. Type and Default
// are pre-marshalled raw envelopes so the surrounding struct can use
// the standard encoding/json machinery.
type columnWire struct {
	Name                 string          `json:"name"`
	Type                 json.RawMessage `json:"type"`
	Nullable             bool            `json:"nullable,omitempty"`
	Default              json.RawMessage `json:"default,omitempty"`
	Comment              string          `json:"comment,omitempty"`
	GeneratedExpr        string          `json:"generated_expr,omitempty"`
	GeneratedStored      bool            `json:"generated_stored,omitempty"`
	GeneratedExprDialect string          `json:"generated_expr_dialect,omitempty"`
}

// MarshalJSON for [Column] emits the tagged-union envelope for Type
// and Default and the natural shape for the rest. Required because
// the standard marshaller can't introspect a sealed interface to
// recover the concrete type at decode time.
func (c *Column) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	w := columnWire{
		Name:                 c.Name,
		Nullable:             c.Nullable,
		Comment:              c.Comment,
		GeneratedExpr:        c.GeneratedExpr,
		GeneratedStored:      c.GeneratedStored,
		GeneratedExprDialect: c.GeneratedExprDialect,
	}
	tb, err := MarshalType(c.Type)
	if err != nil {
		return nil, fmt.Errorf("column %q type: %w", c.Name, err)
	}
	w.Type = tb
	if c.Default != nil {
		db, err := MarshalDefault(c.Default)
		if err != nil {
			return nil, fmt.Errorf("column %q default: %w", c.Name, err)
		}
		// Suppress an emitted "null" so the omitempty on the wire
		// keeps the JSON tidy on columns without a default.
		if string(db) != "null" {
			w.Default = db
		}
	}
	return json.Marshal(w)
}

// UnmarshalJSON for [Column] is the inverse of [Column.MarshalJSON]:
// rebuilds the IR shape from the tagged-union envelopes.
func (c *Column) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	var w columnWire
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("decode column: %w", err)
	}
	c.Name = w.Name
	c.Nullable = w.Nullable
	c.Comment = w.Comment
	c.GeneratedExpr = w.GeneratedExpr
	c.GeneratedStored = w.GeneratedStored
	c.GeneratedExprDialect = w.GeneratedExprDialect
	t, err := UnmarshalType(w.Type)
	if err != nil {
		return fmt.Errorf("column %q type: %w", w.Name, err)
	}
	c.Type = t
	if len(w.Default) > 0 {
		d, err := UnmarshalDefault(w.Default)
		if err != nil {
			return fmt.Errorf("column %q default: %w", w.Name, err)
		}
		c.Default = d
	} else {
		c.Default = DefaultNone{}
	}
	return nil
}
