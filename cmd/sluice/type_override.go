package main

import (
	"errors"
	"fmt"
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

	return config.Mapping{
		Table:      table,
		Column:     column,
		TargetType: targetType,
	}, nil
}
