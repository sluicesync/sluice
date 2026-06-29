package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/config"
)

// resolveMappings combines the --type-override CLI flags with the
// YAML config's `mappings:` list, applying the same wholesale-
// override precedence the table-filter resolution uses: CLI flags,
// when supplied, replace the YAML config entirely (rather than
// merging). The reasoning is the same — combining CLI and YAML
// produces ambiguous outcomes ("does CLI add to YAML or replace
// entries with the same table.column?"), and operators are
// consistently better served by a clear "CLI wins, ignore YAML"
// policy than by an undocumented merge dance.
//
// CLI flag values are parsed via parseTypeOverride; format errors
// surface up to the operator before any DSN is dialed.
//
// Operators who want target-type options (e.g. forcing JSONB with
// binary=true on Postgres) need to use the YAML mappings form —
// the CLI flag deliberately doesn't try to encode a key/value map
// in a single string, since that surface gets ugly fast and the
// YAML is exactly what it's there for.
func resolveMappings(cliOverrides []string, cfg *config.Config) ([]config.Mapping, error) {
	if len(cliOverrides) == 0 {
		return cfg.Mappings, nil
	}
	out := make([]config.Mapping, 0, len(cliOverrides))
	for i, raw := range cliOverrides {
		m, err := parseTypeOverride(raw)
		if err != nil {
			return nil, fmt.Errorf("--type-override[%d]: %w", i, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// parseTypeOverride decodes a single --type-override flag value of
// the form `TABLE.COLUMN=TYPE` into a [config.Mapping]. The split
// is lexical — first `.` separates table from column, first `=`
// after that separates column from type. SQL identifiers can in
// theory contain `.` and `=` (when quoted), but sluice's IR uses
// unqualified identifiers throughout and the YAML mappings form
// has the same restriction; the CLI surface mirrors that.
//
// Empty table, column, or type produces a clear error so a typo
// (e.g. `=longtext` or `t.=longtext`) doesn't silently produce a
// half-shaped mapping.
func parseTypeOverride(raw string) (config.Mapping, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return config.Mapping{}, errors.New("empty value")
	}

	eqIdx := strings.IndexByte(raw, '=')
	if eqIdx < 0 {
		return config.Mapping{}, fmt.Errorf("missing '=' in %q (expected TABLE.COLUMN=TYPE)", raw)
	}
	left := strings.TrimSpace(raw[:eqIdx])
	targetType := strings.TrimSpace(raw[eqIdx+1:])

	dotIdx := strings.IndexByte(left, '.')
	if dotIdx < 0 {
		return config.Mapping{}, fmt.Errorf("missing '.' between table and column in %q (expected TABLE.COLUMN=TYPE)", raw)
	}
	table := strings.TrimSpace(left[:dotIdx])
	column := strings.TrimSpace(left[dotIdx+1:])

	if table == "" {
		return config.Mapping{}, fmt.Errorf("empty table name in %q", raw)
	}
	if column == "" {
		return config.Mapping{}, fmt.Errorf("empty column name in %q", raw)
	}
	if targetType == "" {
		return config.Mapping{}, fmt.Errorf("empty target_type in %q", raw)
	}

	name, opts, err := parseTargetTypeSpec(targetType)
	if err != nil {
		return config.Mapping{}, fmt.Errorf("%w (in %q)", err, raw)
	}

	return config.Mapping{
		Table:             table,
		Column:            column,
		TargetType:        name,
		TargetTypeOptions: opts,
	}, nil
}

// parseTargetTypeSpec splits a CLI target-type spec into the bare type
// name and its options. It supports the concise parenthesised forms for
// the precision/length-bearing types — the common case operators need
// from the CLI without resorting to the YAML `mappings:` form:
//
//	decimal(20,0) / numeric(20,0)  → decimal, {precision:20, scale:0}
//	decimal(20)   / numeric(20)    → decimal, {precision:20}
//	varchar(255)                   → varchar, {length:255}
//	text, jsonb, smallint, …       → bare name, no options
//
// (`numeric` is normalised to `decimal` by resolveTargetType.) A bare
// name with no parentheses passes through unchanged, so every existing
// token keeps working. Anything malformed (unbalanced/empty parens,
// non-integer or wrong-arity arguments, parens on a type that takes
// none) is a clear error rather than a silently-ignored suffix.
func parseTargetTypeSpec(spec string) (name string, opts map[string]any, err error) {
	open := strings.IndexByte(spec, '(')
	if open < 0 {
		// SQL type names are case-insensitive; canonicalise to lower case so
		// `BIGINT` / `Text` resolve identically to `bigint` / `text` (Bug 171 —
		// the Bug-170 remedy suggests `VARCHAR(n)`, which must parse).
		return strings.ToLower(spec), nil, nil
	}
	if !strings.HasSuffix(spec, ")") {
		return "", nil, fmt.Errorf("type %q has an unbalanced '('", spec)
	}
	name = strings.ToLower(strings.TrimSpace(spec[:open]))
	inner := strings.TrimSpace(spec[open+1 : len(spec)-1])
	if name == "" {
		return "", nil, fmt.Errorf("missing type name before '(' in %q", spec)
	}
	if inner == "" {
		return "", nil, fmt.Errorf("type %q has empty parentheses", spec)
	}
	parts := strings.Split(inner, ",")
	args := make([]int, len(parts))
	for i, p := range parts {
		n, perr := strconv.Atoi(strings.TrimSpace(p))
		if perr != nil {
			return "", nil, fmt.Errorf("type %q: argument %q is not an integer", spec, strings.TrimSpace(p))
		}
		args[i] = n
	}

	switch name {
	case "decimal", "numeric":
		if len(args) < 1 || len(args) > 2 {
			return "", nil, fmt.Errorf("type %q: %s takes (precision) or (precision,scale)", spec, name)
		}
		opts = map[string]any{"precision": args[0]}
		if len(args) == 2 {
			opts["scale"] = args[1]
		}
		return name, opts, nil
	case "varchar":
		if len(args) != 1 {
			return "", nil, fmt.Errorf("type %q: varchar takes a single (length)", spec)
		}
		return name, map[string]any{"length": args[0]}, nil
	default:
		return "", nil, fmt.Errorf("type %q does not take parenthesised arguments", name)
	}
}
