// Package translate carries pure schema-rewrite passes that sit
// between the source-side SchemaReader and the target-side
// SchemaWriter in the orchestrator. Today it holds a single pass —
// ApplyMappings — that rewrites column types according to per-column
// overrides specified in sluice.yaml's `mappings:` section.
//
// The package is deliberately I/O-free and engine-agnostic. It
// consumes ir.Schema + config.Mapping and produces a new ir.Schema;
// any failure surfaces as a clear error naming the offending entry.
package translate

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
)

// ApplyMappings rewrites column types in s according to the per-
// column rules in mappings. The returned schema is a copy that
// shares pointers with s for unaffected tables and columns; tables
// containing at least one mapped column are duplicated so callers
// can still rely on s being unchanged.
//
// Errors:
//   - Unknown TargetType values produce a clear error naming the
//     mapping's table+column.
//   - Mappings that reference a table or column not present in s
//     are errors. Silent passthrough would mask typos in the
//     operator's config.
//
// When mappings is empty, ApplyMappings returns s unchanged with a
// nil error — the no-op fast path the orchestrator hits when no
// overrides are configured.
func ApplyMappings(s *ir.Schema, mappings []config.Mapping) (*ir.Schema, error) {
	if s == nil {
		return nil, errors.New("translate: schema is nil")
	}
	if len(mappings) == 0 {
		return s, nil
	}

	// Group mappings by table for cheap lookup; resolve each
	// target_type once so a typo surfaces before we start rewriting.
	byTable, err := groupAndResolveMappings(mappings)
	if err != nil {
		return nil, err
	}

	// Validate that every mapped (table, column) exists in the
	// source schema before mutating. Strict-mode error.
	if err := validateMappingsAgainstSchema(s, byTable); err != nil {
		return nil, err
	}

	// Walk tables: copy-on-write only the ones with at least one
	// matching mapping. Unaffected tables share their pointer with
	// the source schema.
	out := &ir.Schema{Tables: make([]*ir.Table, len(s.Tables))}
	for i, tbl := range s.Tables {
		colMap, hit := byTable[tbl.Name]
		if !hit {
			out.Tables[i] = tbl
			continue
		}
		out.Tables[i] = rewriteTable(tbl, colMap)
	}
	return out, nil
}

// resolvedMapping is a config.Mapping plus its already-resolved
// ir.Type. We resolve once up front so the rewrite loop can't fail
// on a per-column basis with the same error twice.
type resolvedMapping struct {
	cfg  config.Mapping
	irTy ir.Type
}

// groupAndResolveMappings returns a `table -> column -> resolvedMapping`
// nested map. A duplicate (table, column) pair surfaces as an error —
// two competing overrides on the same column is an operator bug, not
// a "last one wins" feature.
func groupAndResolveMappings(mappings []config.Mapping) (map[string]map[string]resolvedMapping, error) {
	out := map[string]map[string]resolvedMapping{}
	for i, m := range mappings {
		if m.Table == "" {
			return nil, fmt.Errorf("translate: mappings[%d]: table is required", i)
		}
		if m.Column == "" {
			return nil, fmt.Errorf("translate: mappings[%d] (%s): column is required", i, m.Table)
		}
		if m.TargetType == "" {
			return nil, fmt.Errorf("translate: mappings[%d] (%s.%s): target_type is required", i, m.Table, m.Column)
		}
		ty, err := resolveTargetType(m.TargetType, m.TargetTypeOptions)
		if err != nil {
			return nil, fmt.Errorf("translate: mappings[%d] (%s.%s): %w", i, m.Table, m.Column, err)
		}
		cols, ok := out[m.Table]
		if !ok {
			cols = map[string]resolvedMapping{}
			out[m.Table] = cols
		}
		if _, dup := cols[m.Column]; dup {
			return nil, fmt.Errorf("translate: mappings[%d]: duplicate override for %s.%s", i, m.Table, m.Column)
		}
		cols[m.Column] = resolvedMapping{cfg: m, irTy: ty}
	}
	return out, nil
}

// validateMappingsAgainstSchema returns an error for any mapping
// that names a table or column the schema doesn't contain. Stricter
// than necessary, but the alternative (silent passthrough) would
// mask typos and downstream "why didn't my override take effect?"
// surprise.
func validateMappingsAgainstSchema(s *ir.Schema, byTable map[string]map[string]resolvedMapping) error {
	tables := map[string]*ir.Table{}
	for _, t := range s.Tables {
		tables[t.Name] = t
	}
	for tableName, cols := range byTable {
		tbl, ok := tables[tableName]
		if !ok {
			return fmt.Errorf("translate: mapping references unknown table %q", tableName)
		}
		colSet := map[string]struct{}{}
		for _, c := range tbl.Columns {
			colSet[c.Name] = struct{}{}
		}
		for colName := range cols {
			if _, ok := colSet[colName]; !ok {
				return fmt.Errorf("translate: mapping references unknown column %s.%s", tableName, colName)
			}
		}
	}
	return nil
}

// rewriteTable produces a copy of tbl with columns named in colMap
// rewritten to the resolved IR type. Columns not in colMap share
// pointers with the source — schemas are large and most tables
// won't have any overrides at all.
func rewriteTable(tbl *ir.Table, colMap map[string]resolvedMapping) *ir.Table {
	out := *tbl
	out.Columns = make([]*ir.Column, len(tbl.Columns))
	for i, c := range tbl.Columns {
		mapping, mapped := colMap[c.Name]
		if !mapped {
			out.Columns[i] = c
			continue
		}
		newCol := *c
		newCol.Type = mapping.irTy
		out.Columns[i] = &newCol
	}
	return &out
}

// targetTypeRegistry is the v1 set of target_type aliases that don't
// take options. Adding a new alias is a one-line edit here plus a
// test case in mappings_test.go. Aliases that take parameters
// (varchar length, postgis SRIDs) live in resolveTargetType rather
// than in this literal so the per-alias parameter handling stays
// explicit.
//
// The aliases are deliberately engine-neutral names — the goal is
// "what should the column be on the target" expressed in IR terms,
// not "MySQL TEXT" or "PG TEXT". Each engine's writer is responsible
// for emitting the right native type.
var targetTypeRegistry = map[string]ir.Type{
	"text":       ir.Text{Size: ir.TextLong},
	"text_array": ir.Array{Element: ir.Text{Size: ir.TextLong}},
	"jsonb":      ir.JSON{Binary: true},
	"json":       ir.JSON{Binary: false},
	"bytea":      ir.Blob{Size: ir.BlobLong},
}

// postgisAliasSubtypes maps the postgis_<subtype> aliases to their
// matching ir.GeometrySubtype. The SRID is read at resolve time from
// target_type_options.srid (default 0). Kept as a separate registry
// from targetTypeRegistry because every entry needs the same SRID
// option-handling — folding them into the literal would force a
// `switch on geometry-ness` at every read site.
var postgisAliasSubtypes = map[string]ir.GeometrySubtype{
	"postgis_point":              ir.GeometryPoint,
	"postgis_linestring":         ir.GeometryLineString,
	"postgis_polygon":            ir.GeometryPolygon,
	"postgis_multipoint":         ir.GeometryMultiPoint,
	"postgis_multilinestring":    ir.GeometryMultiLineString,
	"postgis_multipolygon":       ir.GeometryMultiPolygon,
	"postgis_geometrycollection": ir.GeometryCollection,
	"postgis_geometry":           ir.GeometryUnspecified,
}

// resolveTargetType maps a target_type alias plus any options to a
// concrete ir.Type. Unknown aliases return an error naming the
// alias and listing the recognised set so the operator can spot a
// typo at a glance.
//
// Options handling is alias-specific. Today only `varchar` consults
// options (`length`); the rest take options as a no-op so future
// additions don't require a signature change.
func resolveTargetType(name string, opts map[string]any) (ir.Type, error) {
	if name == "varchar" {
		length := 255
		if raw, ok := opts["length"]; ok {
			switch v := raw.(type) {
			case int:
				length = v
			case int64:
				length = int(v)
			case float64:
				// koanf decodes plain numbers as float64 from JSON-shaped sources.
				length = int(v)
			default:
				return nil, fmt.Errorf("target_type=varchar: option `length` must be an integer, got %T", raw)
			}
		}
		if length <= 0 {
			return nil, fmt.Errorf("target_type=varchar: option `length` must be positive, got %d", length)
		}
		return ir.Varchar{Length: length}, nil
	}
	if subtype, ok := postgisAliasSubtypes[name]; ok {
		srid, err := readSRIDOption(opts)
		if err != nil {
			return nil, fmt.Errorf("target_type=%s: %w", name, err)
		}
		return ir.Geometry{Subtype: subtype, SRID: srid}, nil
	}
	if ty, ok := targetTypeRegistry[name]; ok {
		return ty, nil
	}
	return nil, fmt.Errorf("unknown target_type %q (recognised: %s)", name, knownTargetTypes())
}

// readSRIDOption pulls the optional `srid` value out of a mapping's
// target_type_options. Defaults to 0 (PostGIS "unknown CRS") when
// absent. Same int/int64/float64 acceptance as varchar's `length`
// because koanf hands numbers in different shapes depending on the
// config source.
func readSRIDOption(opts map[string]any) (int, error) {
	raw, ok := opts["srid"]
	if !ok {
		return 0, nil
	}
	switch v := raw.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("option `srid` must be an integer, got %T", raw)
	}
}

// knownTargetTypes returns a comma-separated list of recognised
// target_type aliases, including parameterised ones. Used in error
// messages so operators get a hint when they typo an alias.
func knownTargetTypes() string {
	names := make([]string, 0, len(targetTypeRegistry)+len(postgisAliasSubtypes)+1)
	names = append(names, "varchar") // parameterised alias not in any registry literal
	for n := range targetTypeRegistry {
		names = append(names, n)
	}
	for n := range postgisAliasSubtypes {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
