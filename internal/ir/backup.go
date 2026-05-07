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
const BackupFormatVersion = 1

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
	// AS WRITTEN TO STORAGE (i.e. after compression). Restore computes
	// the hash on the bytes it reads back and compares; any mismatch
	// is a hard failure.
	SHA256 string `json:"sha256"`
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

	// Enum / Set values. Empty for other types.
	Values []string `json:"values,omitempty"`

	// Geometry.
	GeometrySubtype uint8 `json:"geometry_subtype,omitempty"`
	SRID            int   `json:"srid,omitempty"`

	// Array recursive.
	Element json.RawMessage `json:"element,omitempty"`
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
	case Inet:
		env.Kind = "Inet"
	case Cidr:
		env.Kind = "Cidr"
	case Macaddr:
		env.Kind = "Macaddr"
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
		return Decimal{Precision: env.Precision, Scale: env.Scale}, nil
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
		return Time{Precision: env.Precision}, nil
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
		return Geometry{Subtype: GeometrySubtype(env.GeometrySubtype), SRID: env.SRID}, nil
	case "Inet":
		return Inet{}, nil
	case "Cidr":
		return Cidr{}, nil
	case "Macaddr":
		return Macaddr{}, nil
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
