// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package parquetexport maps IR tables and row values onto Apache
// Parquet for `sluice backup export-as-parquet` (ADR-0163). It is the
// EXIT-ONLY half of the analytics surface: sluice writes Parquet, it
// never reads its own Parquet output back — restore keeps the
// JSON-Lines chunk path. That posture collapses the lossless-round-trip
// type-mapping problem to "faithful columnar, documented edges".
//
// The contract mirrors docs/value-types.md on the input side: every
// encoder accepts exactly the Go shapes the IR Row contract defines for
// its column family, and REFUSES anything else loudly (a deviation
// indicates a bug upstream, never something to coerce). On the output
// side, a value the Parquet type system cannot hold faithfully is a
// loud, coded refusal ([sluicecode.CodeExportUnrepresentable]) naming
// the column and the value — never a silent clamp/truncate/normalize.
// The two deliberate, documented downgrades (unbounded/oversized
// DECIMAL → UTF8 string, TIMETZ → UTF8 string) preserve the exact IR
// value bytes and surface as operator-visible notes, not silence.
//
// This package is pure translation over internal/ir + parquet-go; it
// knows nothing about backup stores, manifests, or orchestration (the
// exporter in internal/pipeline/backup drives it chunk by chunk).
package parquetexport

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/ir"
)

// Metadata keys stamped into each Parquet file's footer key-value
// metadata. The `sluice:` prefix namespaces them away from other
// producers; "geo" is the GeoParquet spec's well-known key.
const (
	// MetaEnumValues is a JSON object mapping enum column name → the
	// column's declared value list, so downstream tooling can recover
	// the discrete-value semantics a plain UTF8 column drops.
	MetaEnumValues = "sluice:enum_values"

	// MetaSetValues is the [MetaEnumValues] analogue for MySQL SET
	// columns (exported as LIST<STRING>).
	MetaSetValues = "sluice:set_values"

	// MetaGeo is the GeoParquet metadata key ("geo"). Stamped when the
	// table carries Geometry columns: encoding WKB, per the GeoParquet
	// spec, so GeoPandas / DuckDB-Spatial recover the geometry columns.
	MetaGeo = "geo"
)

// TableCodec translates one IR table's rows into Parquet: the derived
// parquet schema, the per-column value encoders, footer metadata, and
// the operator-visible notes for the documented lossy edges.
//
// Build one per table via [NewTableCodec]; it is stateless after
// construction and safe to reuse across the table's chunks.
type TableCodec struct {
	// Schema is the derived Parquet schema. Every column is OPTIONAL
	// (SQL NULL → Parquet null) regardless of the IR column's
	// Nullable flag — nullability is re-imposable downstream and an
	// exit-only export never validates it.
	Schema *parquet.Schema

	// Metadata is the extra footer key-value metadata this table's
	// type mapping produces (enum/set value lists, GeoParquet block).
	// The exporter merges it with its own provenance keys.
	Metadata map[string]string

	// Notes are the operator-visible type-mapping notes for this
	// table's documented lossy/downgraded edges (one per affected
	// column). The exporter WARNs each once and records them in
	// parquet_index.json.
	Notes []string

	columns []columnCodec
}

// columnCodec pairs one exported column with its value encoder.
type columnCodec struct {
	name   string
	encode encodeFunc
}

// encodeFunc converts one non-nil IR row value into the Go shape the
// column's parquet leaf expects. NULL never reaches an encoder —
// [TableCodec.EncodeRow] short-circuits nil to Parquet null.
type encodeFunc func(v any) (any, error)

// NewTableCodec derives the Parquet schema + encoders for table.
// Generated columns are skipped — the backup chunk writer never
// archives them, so the chunks this codec decodes carry exactly the
// non-generated column set.
//
// A column type with no faithful Parquet mapping fails construction
// loudly (before any file is written); the two documented string
// downgrades (unbounded DECIMAL, TIMETZ) construct fine and surface
// via [TableCodec.Notes].
func NewTableCodec(table *ir.Table) (*TableCodec, error) {
	if table == nil {
		return nil, errors.New("parquetexport: nil table")
	}
	c := &TableCodec{Metadata: map[string]string{}}
	group := parquet.Group{}
	enumValues := map[string][]string{}
	setValues := map[string][]string{}
	var geoColumns []string
	for _, col := range table.Columns {
		if col == nil || col.IsGenerated() {
			continue
		}
		node, enc, err := c.columnNode(col.Type, col.Name)
		if err != nil {
			return nil, fmt.Errorf("parquetexport: table %q column %q: %w", table.Name, col.Name, err)
		}
		group[col.Name] = parquet.Optional(node)
		c.columns = append(c.columns, columnCodec{name: col.Name, encode: enc})
		switch t := col.Type.(type) {
		case ir.Enum:
			enumValues[col.Name] = t.Values
		case ir.Set:
			setValues[col.Name] = t.Values
		case ir.Geometry:
			geoColumns = append(geoColumns, col.Name)
		}
	}
	if len(c.columns) == 0 {
		return nil, fmt.Errorf("parquetexport: table %q has no exportable columns", table.Name)
	}
	if err := c.stampJSONMeta(MetaEnumValues, enumValues); err != nil {
		return nil, err
	}
	if err := c.stampJSONMeta(MetaSetValues, setValues); err != nil {
		return nil, err
	}
	if len(geoColumns) > 0 {
		geo, err := geoParquetMetadata(geoColumns)
		if err != nil {
			return nil, err
		}
		c.Metadata[MetaGeo] = geo
	}
	c.Schema = parquet.NewSchema(table.Name, group)
	return c, nil
}

// stampJSONMeta records a non-empty map as a JSON metadata value.
func (c *TableCodec) stampJSONMeta(key string, m map[string][]string) error {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("parquetexport: marshal %s metadata: %w", key, err)
	}
	c.Metadata[key] = string(b)
	return nil
}

// geoParquetMetadata renders the minimal GeoParquet "geo" block for
// the table's WKB geometry columns. Metadata-only per the GeoParquet
// spec — no geometry library is involved; the value bytes are the IR's
// raw WKB unchanged. geometry_types is left empty (unconstrained): the
// IR subtype describes the DECLARED column type, but GeoParquet's field
// promises the set of types present in the DATA, which an exit-only
// streaming export does not scan ahead to prove.
func geoParquetMetadata(columns []string) (string, error) {
	type geoColumn struct {
		Encoding      string   `json:"encoding"`
		GeometryTypes []string `json:"geometry_types"`
	}
	cols := make(map[string]geoColumn, len(columns))
	for _, name := range columns {
		cols[name] = geoColumn{Encoding: "WKB", GeometryTypes: []string{}}
	}
	block := struct {
		Version       string               `json:"version"`
		PrimaryColumn string               `json:"primary_column"`
		Columns       map[string]geoColumn `json:"columns"`
	}{Version: "1.1.0", PrimaryColumn: columns[0], Columns: cols}
	b, err := json.Marshal(block)
	if err != nil {
		return "", fmt.Errorf("parquetexport: marshal geo metadata: %w", err)
	}
	return string(b), nil
}

// EncodeRow converts one decoded backup-chunk row into the
// map[string]any shape the parquet writer deconstructs against
// [TableCodec.Schema]. SQL NULL (nil, or an absent key — the chunk
// writer records both identically) becomes Parquet null. Any encoder
// refusal is wrapped with the column name; the exporter adds table /
// chunk / row coordinates and the coded refusal class.
func (c *TableCodec) EncodeRow(row ir.Row) (map[string]any, error) {
	out := make(map[string]any, len(c.columns))
	for i := range c.columns {
		col := &c.columns[i]
		v, ok := row[col.name]
		if !ok || v == nil {
			out[col.name] = nil
			continue
		}
		encoded, err := col.encode(v)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.name, err)
		}
		out[col.name] = boxLeafValue(encoded)
	}
	return out, nil
}

// boxLeafValue wraps a scalar leaf value in a pointer before it enters
// parquet-go's map deconstruction. THE WART, named: parquet-go's
// isNullValue treats every Go ZERO value in an optional column as
// parquet NULL when rows are map[string]any (reflect IsZero — false,
// 0, -0.0, "" would all silently export as NULL, a whole silent-loss
// class of its own). A non-nil pointer is never "zero", so wrapping
// restores the SQL-faithful null semantics: nil ⇒ NULL, everything
// else ⇒ the value. Slices ([]byte, []any lists) are only null when
// nil and pass through unwrapped. Pinned by the zero-value non-null
// assertions in roundtrip_pin_test.go.
func boxLeafValue(v any) any {
	switch x := v.(type) {
	case bool:
		return &x
	case int32:
		return &x
	case int64:
		return &x
	case uint64:
		return &x
	case float64:
		return &x
	case string:
		return &x
	}
	return v
}

// note records an operator-visible lossy-edge note for a column.
func (c *TableCodec) note(format string, args ...any) {
	c.Notes = append(c.Notes, fmt.Sprintf(format, args...))
}

// columnNode is the family dispatch: it maps one IR type onto a
// Parquet node + value encoder. The switch is exhaustive over the IR's
// sealed Type set — an unknown type is a loud construction error, so a
// future IR type addition cannot silently export as the wrong shape.
//
// The mapping table lives in ADR-0163 (and docs/type-mapping.md's
// cross-reference); the load-bearing invariants:
//
//   - decimals keep exactness (INT32/INT64/FLBA(16) DECIMAL for
//     precision ≤ 38; the exact string otherwise — never float64);
//   - uint64 keeps its full range (UINT_64 annotation, never a lossy
//     signed reinterpretation);
//   - temporals keep microsecond instants (TIMESTAMP micros, UTC
//     adjusted only for tz-bearing types) — never a string strftime.
func (c *TableCodec) columnNode(t ir.Type, colName string) (parquet.Node, encodeFunc, error) {
	switch v := t.(type) {
	case ir.Boolean:
		return parquet.Leaf(parquet.BooleanType), encodeBool, nil
	case ir.Integer:
		if v.Unsigned {
			return parquet.Uint(64), encodeUint64, nil
		}
		return parquet.Int(64), encodeInt64, nil
	case ir.Decimal:
		return c.decimalNode(v, colName)
	case ir.Float:
		// Single-precision values were widened to float64 by the
		// reader (lossless); DOUBLE holds NaN/±Inf natively, so the
		// backup's non-finite fidelity (Bug 138) carries through.
		return parquet.Leaf(parquet.DoubleType), encodeFloat64, nil
	case ir.Char, ir.Varchar, ir.Text:
		return parquet.String(), encodeString, nil
	case ir.Binary, ir.Varbinary, ir.Blob:
		return parquet.Leaf(parquet.ByteArrayType), encodeBytes, nil
	case ir.Bit:
		// The IR contract for Bit values is the canonical '0'/'1'
		// bit-string (internal/ir/bit.go); the string IS the value.
		return parquet.String(), encodeString, nil
	case ir.Date:
		return parquet.Date(), encodeDate, nil
	case ir.Time:
		if v.WithTimeZone {
			// Parquet TIME has no offset-bearing form. The IR value is
			// the source's exact text (e.g. "08:30:00+02"); export it
			// verbatim as UTF8 rather than strip the offset silently.
			c.note("column %q: TIME WITH TIME ZONE exported as UTF8 string (Parquet TIME has no time-zone form; the value text is carried exactly)", colName)
			return parquet.String(), encodeString, nil
		}
		return parquet.TimeAdjusted(parquet.Microsecond, false), encodeTimeOfDayMicros, nil
	case ir.DateTime:
		return parquet.TimestampAdjusted(parquet.Microsecond, false), encodeTimestampMicros, nil
	case ir.Timestamp:
		// A tz-bearing timestamp is an instant: the IR already carries
		// it UTC-normalized, which is exactly Parquet's
		// isAdjustedToUTC=true semantics (the operator-visible source
		// zone was never on the value — documented lossy edge of the
		// IR contract itself, not of this export).
		return parquet.TimestampAdjusted(parquet.Microsecond, v.WithTimeZone), encodeTimestampMicros, nil
	case ir.JSON:
		return parquet.JSON(), encodeJSONBytes, nil
	case ir.Enum:
		// Value list recorded in file metadata (MetaEnumValues).
		return parquet.String(), encodeString, nil
	case ir.Set:
		// The currently-selected members, in declaration order; the
		// declared universe rides in MetaSetValues.
		return parquet.List(parquet.Optional(parquet.String())), encodeStringList, nil
	case ir.UUID:
		// Canonical hyphenated lowercase string, exactly the IR value.
		// (The research sketch floated the UUID logical type, but the
		// spec restricts it to FIXED_LEN_BYTE_ARRAY(16); the string is
		// what DuckDB / PyArrow / Spark consume directly.)
		return parquet.String(), encodeString, nil
	case ir.Array:
		if v.Element == nil {
			return nil, nil, errors.New("array type carries no element type")
		}
		elemNode, elemEnc, err := c.columnNode(v.Element, colName)
		if err != nil {
			return nil, nil, err
		}
		return parquet.List(parquet.Optional(elemNode)), encodeArray(elemEnc, colName), nil
	case ir.Geometry:
		// Raw WKB bytes, GeoParquet-annotated via file metadata.
		return parquet.Leaf(parquet.ByteArrayType), encodeBytes, nil
	case ir.Inet, ir.Cidr, ir.Macaddr, ir.Interval:
		// Canonical textual forms per the IR value contract.
		return parquet.String(), encodeString, nil
	case ir.Domain:
		// A domain is a thin wrapper over its base type; values flow
		// in the base type's shape (Bug 122). The CHECK-constraint
		// semantics are not representable in Parquet — exit-only, so
		// downstream re-imposes them if it cares.
		if v.BaseType == nil {
			return nil, nil, fmt.Errorf("domain %q carries no base type", v.Name)
		}
		return c.columnNode(v.BaseType, colName)
	case ir.ExtensionType, ir.VerbatimType:
		// Opaque extension values ride the type's text I/O (pgvector's
		// "[1,2,3]", ltree paths, …): string or bytes-of-text per the
		// engine decoders. Export the text verbatim as UTF8.
		return parquet.String(), encodeOpaqueText, nil
	}
	return nil, nil, fmt.Errorf("IR type %s has no Parquet mapping", t.String())
}
