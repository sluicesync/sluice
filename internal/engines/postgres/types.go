package postgres

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// columnMeta is the subset of information_schema.columns the type
// translator needs, augmented with metadata sluice resolves separately
// (enum values, array element type) so the translator can stay pure.
type columnMeta struct {
	// DataType is information_schema.columns.data_type — e.g. "integer",
	// "character varying", "ARRAY", "USER-DEFINED". Lowercase.
	DataType string

	// UDTName is information_schema.columns.udt_name — the canonical
	// underlying type name. For arrays, prefixed with underscore
	// (e.g. "_int4"). For custom types like enums, the type name.
	UDTName string

	// CharMaxLen is character_maximum_length, or nil when not applicable.
	CharMaxLen *int64

	// NumPrec is numeric_precision, or nil when not applicable.
	NumPrec *int64

	// NumScale is numeric_scale, or nil when not applicable.
	NumScale *int64

	// DTPrec is datetime_precision, or nil when not applicable.
	DTPrec *int64

	// IsAutoIncrement is true for SERIAL/BIGSERIAL/IDENTITY columns.
	// The schema reader sets this based on either is_identity or a
	// nextval() default; the translator just respects it.
	IsAutoIncrement bool

	// ArrayElement is populated when DataType == "ARRAY". It carries
	// the resolved metadata of the array's element type so the
	// translator can recurse into it.
	ArrayElement *columnMeta

	// EnumValues is populated when the column's UDT is a Postgres
	// enum type. The schema reader resolves the values via pg_enum
	// before invoking the translator.
	EnumValues []string
}

// translateType maps a Postgres column's metadata to an IR type. The
// function is pure; all required external lookups (enum values, array
// element types) must be resolved by the caller and supplied via
// columnMeta before invoking translateType.
func translateType(c columnMeta) (ir.Type, error) {
	// Arrays first — the rest of the logic dispatches on the
	// "scalar" type name, and arrays carry that name on their element.
	if c.DataType == "array" || c.DataType == "ARRAY" {
		if c.ArrayElement == nil {
			return nil, fmt.Errorf("postgres: array column with unresolved element type (udt %q)", c.UDTName)
		}
		elem, err := translateType(*c.ArrayElement)
		if err != nil {
			return nil, fmt.Errorf("postgres: array element: %w", err)
		}
		return ir.Array{Element: elem}, nil
	}

	// USER-DEFINED covers enums, composites, and domain types. We
	// support enums; composites/domains are not modelled in the IR.
	if c.DataType == "user-defined" || c.DataType == "USER-DEFINED" {
		if c.EnumValues != nil {
			return ir.Enum{Values: c.EnumValues}, nil
		}
		return nil, fmt.Errorf("postgres: user-defined type %q is not a recognised enum", c.UDTName)
	}

	switch c.DataType {
	// ---- Boolean ----
	case "boolean":
		return ir.Boolean{}, nil

	// ---- Integer family ----
	case "smallint":
		return ir.Integer{Width: 16, AutoIncrement: c.IsAutoIncrement}, nil
	case "integer":
		return ir.Integer{Width: 32, AutoIncrement: c.IsAutoIncrement}, nil
	case "bigint":
		return ir.Integer{Width: 64, AutoIncrement: c.IsAutoIncrement}, nil

	// ---- Decimal / float ----
	case "numeric", "decimal":
		return ir.Decimal{Precision: int(int64Ptr(c.NumPrec)), Scale: int(int64Ptr(c.NumScale))}, nil
	case "real":
		return ir.Float{Precision: ir.FloatSingle}, nil
	case "double precision":
		return ir.Float{Precision: ir.FloatDouble}, nil

	// ---- Character ----
	case "character":
		return ir.Char{Length: int(int64Ptr(c.CharMaxLen))}, nil
	case "character varying":
		return ir.Varchar{Length: int(int64Ptr(c.CharMaxLen))}, nil
	case "text":
		// Postgres text is unbounded; the IR's TextLong is the
		// closest match that round-trips correctly to MySQL's LONGTEXT.
		return ir.Text{Size: ir.TextLong}, nil

	// ---- Binary ----
	case "bytea":
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Temporal ----
	case "date":
		return ir.Date{}, nil
	case "time without time zone", "time":
		return ir.Time{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "time with time zone":
		// The IR currently has a single Time type without a TZ flag.
		// Modelled as Time; the TZ-ness is implicit and lossy on
		// MySQL output. A future IR extension could add a TZ flag.
		return ir.Time{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "timestamp without time zone", "timestamp":
		return ir.DateTime{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "timestamp with time zone":
		return ir.Timestamp{Precision: int(int64Ptr(c.DTPrec)), WithTimeZone: true}, nil

	// ---- Structured ----
	case "json":
		return ir.JSON{Binary: false}, nil
	case "jsonb":
		return ir.JSON{Binary: true}, nil

	// ---- Identity / network ----
	case "uuid":
		return ir.UUID{}, nil
	case "inet":
		return ir.Inet{}, nil
	case "cidr":
		return ir.Cidr{}, nil
	case "macaddr", "macaddr8":
		return ir.Macaddr{}, nil
	}

	return nil, fmt.Errorf("postgres: unsupported data_type %q (udt %q)", c.DataType, c.UDTName)
}

// int64Ptr returns *p, or 0 if p is nil. Used to translate
// information_schema's nullable numeric columns into the IR's int
// fields without per-call nil checks.
func int64Ptr(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
