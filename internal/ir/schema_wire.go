// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// JSON round-tripping for the IR's sealed [Type] / [DefaultValue]
// interfaces. Without this, `Schema.Tables[i].Columns[j].Type` can't
// survive a round-trip through `encoding/json` (it's a sealed
// interface; the decoder has no way to recover the concrete type).
// The tagged-union envelope keeps the serialised form human-readable
// while round-tripping unambiguously.
//
// This is core IR contract, not a backup detail: the CDC schema
// history machinery ([ResolveSchemaVersion] / `schema_history.go`) and
// the backup manifests (`internal/ir/backup`) both persist schemas through
// exactly this codec — byte-identical wire shapes (ADR-0049 locked
// decision #1). It lives in the core package because the
// [Column.MarshalJSON] / [Column.UnmarshalJSON] hooks must be methods
// on the core type.

import (
	"encoding/json"
	"fmt"
)

// MarshalTable serialises a single [Table] through the same tagged-
// union JSON codec the backup manifest uses. It is a thin wrapper over
// the standard json marshaller — [Column.MarshalJSON] does the sealed-
// interface (Type / DefaultValue) work and Table's other fields are
// concrete, so the natural struct shape round-trips. Exposed so the
// ADR-0049 schema-history store has a named, documented entrypoint
// rather than open-coding json.Marshal at the call site; this is NOT a
// second serialization scheme — it composes the existing Column codec
// verbatim (ADR-0049 locked decision #1).
func MarshalTable(t *Table) ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("marshal table %q: %w", t.Name, err)
	}
	return b, nil
}

// UnmarshalTable is the inverse of [MarshalTable]. Returns nil for a
// JSON null / empty input (mirrors the codec's other Unmarshal* helpers).
func UnmarshalTable(b []byte) (*Table, error) {
	if len(b) == 0 || string(b) == "null" {
		return nil, nil
	}
	var t Table
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("decode table: %w", err)
	}
	return &t, nil
}

// MarshalJSON for [Schema] uses the tagged-union envelope so the
// sealed Type / DefaultValue interfaces round-trip through standard
// encoding/json. Same wire shape as the in-memory struct, but with
// every Column / DefaultValue / Type wrapped in a tagged envelope.
//
// We don't customise marshal at the Schema level; instead we marshal
// each component via its own MarshalJSON below. Schema's natural
// struct shape is sufficient because Tables / Views / Sequences are
// concrete pointer slices, not interface slices ([Sequence] is all
// plain fields, so it needs no envelope of its own).

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

	// Temporal precision-unspecified (restore-parity TRIAGE #3, the
	// temporal counterpart of DecimalUnconstrained). True for the bare
	// PG `time`/`timestamp`/`timestamptz` form with no declared
	// precision (atttypmod -1). Append-only; older sluice ignores it
	// and reads the column as precision 0 — which its emitter renders
	// as the bare form, the pre-fix behaviour. Manifests written by
	// OLDER binaries carry the materialized Precision=6 with this flag
	// absent, and decode as an explicit (6) — restoring them keeps
	// behaving exactly as it did pre-fix. No format bump needed.
	TemporalPrecisionUnspecified bool `json:"temporal_precision_unspecified,omitempty"`

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

	// Bit (ADR-0049 Chunk B/C prerequisite). Bit.Length reuses the
	// string `Length` field above; BitVarying distinguishes
	// `bit varying`/`varbit` from fixed-width `bit`. Append-only.
	BitVarying bool `json:"bit_varying,omitempty"`

	// Domain (Bug 113 closure, v0.95.1). DomainName carries the
	// operator-declared identifier; DomainBaseType is the recursively-
	// encoded base [Type] envelope (same pattern as Array.Element);
	// DomainChecks carries the CHECK definitions. Append-only —
	// older sluice ignores these fields and reads a Domain envelope
	// as a no-base Kind=Domain it doesn't know how to construct,
	// which surfaces as a clear "unknown IR type kind" error
	// rather than silent drop.
	DomainName     string              `json:"domain_name,omitempty"`
	DomainBaseType json.RawMessage     `json:"domain_base_type,omitempty"`
	DomainChecks   []domainCheckOnDisk `json:"domain_checks,omitempty"`
}

// domainCheckOnDisk is the wire shape of one [DomainCheck] inside a
// schema-type envelope. Append-only.
type domainCheckOnDisk struct {
	Name string `json:"name,omitempty"`
	Body string `json:"body,omitempty"`
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
	case Interval:
		// PG duration type (the MySQL TIME → PG interval override); no
		// fields. Round-trips so a schema-history snapshot / backup of an
		// interval-overridden column survives.
		env.Kind = "Interval"
	case Time:
		env.Kind = "Time"
		env.Precision = v.Precision
		env.WithTimeZone = v.WithTimeZone
		env.TemporalPrecisionUnspecified = v.PrecisionUnspecified
	case DateTime:
		env.Kind = "DateTime"
		env.Precision = v.Precision
		env.TemporalPrecisionUnspecified = v.PrecisionUnspecified
	case Timestamp:
		env.Kind = "Timestamp"
		env.Precision = v.Precision
		env.WithTimeZone = v.WithTimeZone
		env.TemporalPrecisionUnspecified = v.PrecisionUnspecified
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
	case Bit:
		// ADR-0049 Chunk B/C prerequisite: a schema-history snapshot of
		// a table carrying a `bit`/`varbit` column (catalog Bug 62/77)
		// must round-trip, not loud-fail.
		env.Kind = "Bit"
		env.Length = v.Length
		env.BitVarying = v.Varying
	case ExtensionType:
		// ADR-0049 Chunk B/C prerequisite: the ADR-0032 catalogued
		// extension types (vector/pg_trgm/hstore/citext/postgis/
		// pgcrypto/uuid-ossp). Extension/Name/Modifiers were already
		// provisioned in the envelope; only the codec case was missing.
		env.Kind = "ExtensionType"
		env.Extension = v.Extension
		env.Name = v.Name
		env.Modifiers = v.Modifiers
	case Domain:
		// Bug 113 closure (v0.95.1). Round-trips Domain.Name +
		// Domain.BaseType (recursive) + Domain.Checks so the PG
		// writer can re-emit `CREATE DOMAIN ... AS ... CHECK (...)`
		// before tables that reference it.
		env.Kind = "Domain"
		env.DomainName = v.Name
		if v.BaseType != nil {
			base, err := MarshalType(v.BaseType)
			if err != nil {
				return nil, fmt.Errorf("domain base type: %w", err)
			}
			env.DomainBaseType = base
		}
		if len(v.Checks) > 0 {
			env.DomainChecks = make([]domainCheckOnDisk, len(v.Checks))
			for i, c := range v.Checks {
				env.DomainChecks[i] = domainCheckOnDisk(c)
			}
		}
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
	case "Interval":
		return Interval{}, nil
	case "Time":
		return Time{Precision: env.Precision, WithTimeZone: env.WithTimeZone, PrecisionUnspecified: env.TemporalPrecisionUnspecified}, nil
	case "DateTime":
		return DateTime{Precision: env.Precision, PrecisionUnspecified: env.TemporalPrecisionUnspecified}, nil
	case "Timestamp":
		return Timestamp{Precision: env.Precision, WithTimeZone: env.WithTimeZone, PrecisionUnspecified: env.TemporalPrecisionUnspecified}, nil
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
	case "Bit":
		return Bit{Length: env.Length, Varying: env.BitVarying}, nil
	case "ExtensionType":
		return ExtensionType{
			Extension: env.Extension,
			Name:      env.Name,
			Modifiers: env.Modifiers,
		}, nil
	case "Domain":
		// Bug 113 closure (v0.95.1).
		var base Type
		if len(env.DomainBaseType) > 0 && string(env.DomainBaseType) != "null" {
			var err error
			base, err = UnmarshalType(env.DomainBaseType)
			if err != nil {
				return nil, fmt.Errorf("domain base type: %w", err)
			}
		}
		var checks []DomainCheck
		if len(env.DomainChecks) > 0 {
			checks = make([]DomainCheck, len(env.DomainChecks))
			for i, c := range env.DomainChecks {
				checks[i] = DomainCheck(c)
			}
		}
		return Domain{Name: env.DomainName, BaseType: base, Checks: checks}, nil
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
