// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package ir defines the dialect-neutral intermediate representation
// at the heart of sluice. Every translation between supported database
// engines passes through types defined here.
//
// The IR is organised into two tiers (see [Type.Tier]):
//
//   - Core types are the universal SQL primitives every engine must
//     read and write (Boolean, Integer, Decimal, Char, Varchar, Text,
//     Date, Timestamp, JSON, etc.).
//   - Extension types are richer constructs that only some engines
//     support natively (Enum, Array, Geometry, Inet, etc.). Engines
//     declare which extension types they support via [Capabilities].
//
// The split keeps the IR genuinely engine-neutral: new engines can be
// added without amending the core, and engine-specific richness arrives
// through extension types rather than through dialect-specific shapes.
//
// The [Type] interface is sealed by an unexported method, so only types
// defined within this package can satisfy it. Translation code can
// therefore exhaustively match on IR types via type switches.
package ir

import "fmt"

// Tier identifies whether a [Type] is part of the universal core or an
// engine-specific extension.
type Tier uint8

const (
	// TierCore types are required to be readable and writable by every
	// supported engine.
	TierCore Tier = iota
	// TierExtension types are optional per-engine; the engine's
	// [Capabilities.SupportedTypes] lists which extension types it
	// handles natively.
	TierExtension
)

// String returns "core" or "extension".
func (t Tier) String() string {
	switch t {
	case TierCore:
		return "core"
	case TierExtension:
		return "extension"
	default:
		return "unknown"
	}
}

// Type is the sealed interface for all IR column types. Implementations
// live in this package only.
type Type interface {
	// isType seals the interface; only IR-defined types can satisfy it.
	isType()
	// Tier reports whether this type is core or an extension.
	Tier() Tier
	// String returns a stable, dialect-neutral textual representation
	// suitable for logging, error messages, and golden-file tests.
	String() string
}

// FloatPrecision distinguishes single-precision (32-bit) and
// double-precision (64-bit) floats.
type FloatPrecision uint8

// Recognised FloatPrecision values.
const (
	FloatSingle FloatPrecision = iota
	FloatDouble
)

func (p FloatPrecision) String() string {
	switch p {
	case FloatSingle:
		return "single"
	case FloatDouble:
		return "double"
	default:
		return "unknown"
	}
}

// TextSize buckets the size class of a [Text] column. Exact byte limits
// are dialect-specific; the IR preserves only the categorical bucket.
type TextSize uint8

// Recognised TextSize buckets, ordered from smallest to largest.
const (
	TextTiny TextSize = iota
	TextRegular
	TextMedium
	TextLong
)

func (s TextSize) String() string {
	switch s {
	case TextTiny:
		return "tiny"
	case TextRegular:
		return "regular"
	case TextMedium:
		return "medium"
	case TextLong:
		return "long"
	default:
		return "unknown"
	}
}

// BlobSize buckets the size class of a [Blob] column.
type BlobSize uint8

// Recognised BlobSize buckets, ordered from smallest to largest.
const (
	BlobTiny BlobSize = iota
	BlobRegular
	BlobMedium
	BlobLong
)

func (s BlobSize) String() string {
	switch s {
	case BlobTiny:
		return "tiny"
	case BlobRegular:
		return "regular"
	case BlobMedium:
		return "medium"
	case BlobLong:
		return "long"
	default:
		return "unknown"
	}
}

// =====================================================================
// Core IR types — every engine must be able to read and write these.
// =====================================================================

// Boolean represents a logical true/false value.
type Boolean struct{}

func (Boolean) isType()        {}
func (Boolean) Tier() Tier     { return TierCore }
func (Boolean) String() string { return "Boolean" }

// Integer represents an integral numeric value of a given bit width.
// Width may be 8, 16, 24 (MySQL MEDIUMINT), 32, or 64.
type Integer struct {
	Width         int8
	Unsigned      bool
	AutoIncrement bool
}

func (Integer) isType()    {}
func (Integer) Tier() Tier { return TierCore }

func (i Integer) String() string {
	sign := "Int"
	if i.Unsigned {
		sign = "UInt"
	}
	s := fmt.Sprintf("%s%d", sign, i.Width)
	if i.AutoIncrement {
		s += " AutoIncrement"
	}
	return s
}

// Decimal represents a fixed-point exact-precision number.
//
// Unconstrained models the arbitrary-precision case — a column declared
// as bare `numeric` / `decimal` with NO precision or scale (PostgreSQL
// supports this; the engine stores the value with whatever precision the
// data requires). When Unconstrained is true, Precision and Scale carry
// no meaning and MUST be zero; when false the column is the bounded
// `numeric(p,s)` form and Precision/Scale are authoritative. The two
// cases are genuinely distinct on the wire — PG renders bare `NUMERIC`
// vs `NUMERIC(p,s)` — so the IR must distinguish them rather than
// collapsing an absent precision to (0,0) (catalog Bug 69).
type Decimal struct {
	Precision     int  // total number of digits (meaningful only when !Unconstrained)
	Scale         int  // digits right of the point (meaningful only when !Unconstrained)
	Unconstrained bool // true for bare arbitrary-precision numeric/decimal
}

func (Decimal) isType()    {}
func (Decimal) Tier() Tier { return TierCore }

func (d Decimal) String() string {
	if d.Unconstrained {
		return "Decimal(unconstrained)"
	}
	return fmt.Sprintf("Decimal(%d,%d)", d.Precision, d.Scale)
}

// Float represents an IEEE-754 floating-point number.
type Float struct {
	Precision FloatPrecision
}

func (Float) isType()    {}
func (Float) Tier() Tier { return TierCore }

func (f Float) String() string { return fmt.Sprintf("Float[%s]", f.Precision) }

// Char is a fixed-length character string.
type Char struct {
	Length    int
	Charset   string
	Collation string
}

func (Char) isType()    {}
func (Char) Tier() Tier { return TierCore }

func (c Char) String() string { return fmt.Sprintf("Char(%d)", c.Length) }

// Varchar is a variable-length character string with an explicit max length.
type Varchar struct {
	Length    int
	Charset   string
	Collation string
}

func (Varchar) isType()    {}
func (Varchar) Tier() Tier { return TierCore }

func (v Varchar) String() string { return fmt.Sprintf("Varchar(%d)", v.Length) }

// Text is a variable-length character string sized by category. Postgres
// TEXT (unbounded) maps to TextLong.
type Text struct {
	Size      TextSize
	Charset   string
	Collation string
}

func (Text) isType()    {}
func (Text) Tier() Tier { return TierCore }

func (t Text) String() string { return fmt.Sprintf("Text[%s]", t.Size) }

// Binary is a fixed-length byte string.
type Binary struct {
	Length int
}

func (Binary) isType()    {}
func (Binary) Tier() Tier { return TierCore }

func (b Binary) String() string { return fmt.Sprintf("Binary(%d)", b.Length) }

// Varbinary is a variable-length byte string with an explicit max length.
type Varbinary struct {
	Length int
}

func (Varbinary) isType()    {}
func (Varbinary) Tier() Tier { return TierCore }

func (v Varbinary) String() string { return fmt.Sprintf("Varbinary(%d)", v.Length) }

// Blob is a variable-length byte string sized by category.
type Blob struct {
	Size BlobSize
}

func (Blob) isType()    {}
func (Blob) Tier() Tier { return TierCore }

func (b Blob) String() string { return fmt.Sprintf("Blob[%s]", b.Size) }

// Bit is a fixed-width bit string of Length bits — MySQL's BIT(N) and
// PostgreSQL's bit(N). Length is the declared bit count (1..64; MySQL
// caps BIT at 64). BIT(1) is *not* modelled here: the MySQL reader maps
// the conventional single-bit column to [Boolean] (and the boolean
// round-trip is the validated path). Bit covers BIT(N) for N > 1, which
// both engines round-trip losslessly as a native bit string with a
// `b'…'` / `B'…'` literal default. Modelling it as Varbinary (the
// pre-v0.65.1 behaviour) mis-typed the column on every target and
// decimal-stringified the bit-literal default (catalog Bug 62).
// Varying distinguishes a variable-length bit string (PostgreSQL `bit
// varying(N)` / `varbit`) from a fixed-width one (`bit(N)`, MySQL
// `BIT(N)`). For a varying column Length is the declared maximum and
// individual values may be shorter; for a fixed column every value is
// exactly Length bits. Collapsing the two (catalog Bug 75) made the
// PG DDL emitter render `bit varying(16)` as fixed `bit(16)`, so a
// shorter value was loud-rejected (SQLSTATE 22026) after the value
// path was finally faithful. MySQL has no varying-bit type; a
// varying source lands as MySQL `BIT(Length)` (fixed, the closest
// faithful shape — values are zero-extended, which BIN() round-trips).
type Bit struct {
	Length  int
	Varying bool
}

func (Bit) isType()    {}
func (Bit) Tier() Tier { return TierCore }

func (b Bit) String() string {
	if b.Varying {
		return fmt.Sprintf("Varbit(%d)", b.Length)
	}
	return fmt.Sprintf("Bit(%d)", b.Length)
}

// Date is a calendar date with no time-of-day component.
type Date struct{}

func (Date) isType()        {}
func (Date) Tier() Tier     { return TierCore }
func (Date) String() string { return "Date" }

// Time is a time-of-day with sub-second precision. WithTimeZone
// distinguishes Postgres `timetz` (`time with time zone`) from plain
// `time`, mirroring [Timestamp.WithTimeZone]. The two PG wire types
// have distinct OIDs (1266 vs 1083) and the tz-bearing form cannot be
// encoded into the tz-less one — collapsing them mis-mapped timetz to
// time and hard-failed the COPY writer (catalog Bug 71).
//
// PrecisionUnspecified models the bare declared form — a column
// declared as `time` / `time with time zone` with NO precision
// (pg_attribute.atttypmod = -1; behaves as the engine default, 6).
// When true, Precision carries no meaning and MUST be zero; when
// false, Precision is the explicitly declared value (0 is a real,
// distinct declaration — `time(0)` rounds to whole seconds). The two
// cases are genuinely distinct on the wire — PG renders bare `time`
// vs `time(p)` — so the IR must distinguish them rather than
// materializing information_schema's default-6 into the catalog (the
// temporal counterpart of [Decimal].Unconstrained, catalog Bug 69;
// restore-parity TRIAGE #3). Engines whose readers always know the
// precision (MySQL reports 0 for bare TIME) never set it.
type Time struct {
	Precision            int
	WithTimeZone         bool
	PrecisionUnspecified bool
}

func (Time) isType()    {}
func (Time) Tier() Tier { return TierCore }

func (t Time) String() string {
	if t.WithTimeZone {
		if t.PrecisionUnspecified {
			return "TimeTZ(unspecified)"
		}
		return fmt.Sprintf("TimeTZ(%d)", t.Precision)
	}
	if t.PrecisionUnspecified {
		return "Time(unspecified)"
	}
	return fmt.Sprintf("Time(%d)", t.Precision)
}

// Interval is a span of time (a duration), distinct from Time (a
// time-of-day). It is an EXTENSION type: PostgreSQL has a native
// `interval`, but MySQL has no equivalent — a MySQL `TIME` column is a
// duration in the range -838:59:59…838:59:59, which exceeds PG `time`'s
// 00:00–24:00 time-of-day range, so carrying such a column to PG needs
// `interval`, not `time`. There is no default reader mapping to Interval
// (MySQL `TIME` still reads as [Time] by default); it is reached only via
// an explicit `--type-override col=interval`, for the MySQL `TIME`
// duration → PG `interval` case. Values are carried as their textual
// form (e.g. "838:59:59", "-12:00:00"), which PG's interval input
// parser accepts. A MySQL/non-PG target has no native interval and is
// refused loudly (emitColumnType / cross-engine supportability check).
type Interval struct{}

func (Interval) isType()    {}
func (Interval) Tier() Tier { return TierExtension }

func (Interval) String() string { return "Interval" }

// DateTime is a calendar date plus time-of-day, without a timezone.
// PrecisionUnspecified mirrors [Time].PrecisionUnspecified — the bare
// PG `timestamp` (typmod -1) form, distinct from `timestamp(p)`.
type DateTime struct {
	Precision            int
	PrecisionUnspecified bool
}

func (DateTime) isType()    {}
func (DateTime) Tier() Tier { return TierCore }

func (d DateTime) String() string {
	if d.PrecisionUnspecified {
		return "DateTime(unspecified)"
	}
	return fmt.Sprintf("DateTime(%d)", d.Precision)
}

// Timestamp is a calendar date plus time-of-day with optional timezone.
// PrecisionUnspecified mirrors [Time].PrecisionUnspecified — the bare
// PG `timestamp with time zone` (typmod -1) form, distinct from
// `timestamptz(p)`; when true, Precision MUST be zero.
type Timestamp struct {
	Precision            int
	WithTimeZone         bool
	PrecisionUnspecified bool
}

func (Timestamp) isType()    {}
func (Timestamp) Tier() Tier { return TierCore }

func (t Timestamp) String() string {
	if t.WithTimeZone {
		if t.PrecisionUnspecified {
			return "TimestampTZ(unspecified)"
		}
		return fmt.Sprintf("TimestampTZ(%d)", t.Precision)
	}
	if t.PrecisionUnspecified {
		return "Timestamp(unspecified)"
	}
	return fmt.Sprintf("Timestamp(%d)", t.Precision)
}

// JSON represents a JSON-typed column. Binary indicates whether the
// value is stored in a parsed/normalised form (Postgres jsonb, MySQL
// JSON) versus a textual form (Postgres json).
type JSON struct {
	Binary bool
}

func (JSON) isType()    {}
func (JSON) Tier() Tier { return TierCore }

func (j JSON) String() string {
	if j.Binary {
		return "JSON[binary]"
	}
	return "JSON[text]"
}
