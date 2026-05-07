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
type Decimal struct {
	Precision int // total number of digits
	Scale     int // digits to the right of the decimal point
}

func (Decimal) isType()    {}
func (Decimal) Tier() Tier { return TierCore }

func (d Decimal) String() string {
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

// Date is a calendar date with no time-of-day component.
type Date struct{}

func (Date) isType()        {}
func (Date) Tier() Tier     { return TierCore }
func (Date) String() string { return "Date" }

// Time is a time-of-day with sub-second precision.
type Time struct {
	Precision int
}

func (Time) isType()    {}
func (Time) Tier() Tier { return TierCore }

func (t Time) String() string { return fmt.Sprintf("Time(%d)", t.Precision) }

// DateTime is a calendar date plus time-of-day, without a timezone.
type DateTime struct {
	Precision int
}

func (DateTime) isType()    {}
func (DateTime) Tier() Tier { return TierCore }

func (d DateTime) String() string { return fmt.Sprintf("DateTime(%d)", d.Precision) }

// Timestamp is a calendar date plus time-of-day with optional timezone.
type Timestamp struct {
	Precision    int
	WithTimeZone bool
}

func (Timestamp) isType()    {}
func (Timestamp) Tier() Tier { return TierCore }

func (t Timestamp) String() string {
	if t.WithTimeZone {
		return fmt.Sprintf("TimestampTZ(%d)", t.Precision)
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
