package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/config"
)

// resolveExpressionMappings combines the --expr-override CLI flags
// with the YAML config's `expression_mappings:` list. Same wholesale-
// override precedence the type-mapping resolution uses (see
// resolveMappings): CLI flags, when supplied, replace the YAML config
// entirely rather than merging. Operators consistently get clearer
// behaviour from "CLI wins" than from an undocumented merge dance.
//
// CLI flag values are parsed via parseExprOverride; format errors
// surface up to the operator before any DSN is dialed.
func resolveExpressionMappings(cliOverrides []string, cfg *config.Config) ([]config.ExpressionMapping, error) {
	if len(cliOverrides) == 0 {
		return cfg.ExpressionMappings, nil
	}
	out := make([]config.ExpressionMapping, 0, len(cliOverrides))
	for i, raw := range cliOverrides {
		m, err := parseExprOverride(raw)
		if err != nil {
			return nil, fmt.Errorf("--expr-override[%d]: %w", i, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// parseExprOverride decodes a single --expr-override flag value of
// the form `TABLE.COLUMN=EXPRESSION` into a [config.ExpressionMapping].
// The split mirrors --type-override: first `.` separates table from
// column, first `=` after that separates column from expression.
//
// The expression part can contain arbitrary characters including
// `=`, `(`, `)`, `'`, `"`, etc. — operators frequently override with
// expressions like `coalesce((notes IS NULL)::int, 0)` that are full
// of punctuation. Only the FIRST `=` after the column name is the
// separator; everything else is expression text. Empty table,
// column, or expression produces a clear error.
func parseExprOverride(raw string) (config.ExpressionMapping, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return config.ExpressionMapping{}, errors.New("empty value")
	}

	eqIdx := strings.IndexByte(raw, '=')
	if eqIdx < 0 {
		return config.ExpressionMapping{}, fmt.Errorf("missing '=' in %q (expected TABLE.COLUMN=EXPRESSION)", raw)
	}
	left := strings.TrimSpace(raw[:eqIdx])
	// The expression preserves operator-supplied internal whitespace
	// (TrimSpace only on the outer ends). Multi-line expressions are
	// uncommon on a CLI flag but possible via shell quoting; the trim
	// strips leading/trailing whitespace so `--expr-override
	// 't.c= foo '` and `--expr-override 't.c=foo'` produce the same
	// stored override.
	expression := strings.TrimSpace(raw[eqIdx+1:])

	dotIdx := strings.IndexByte(left, '.')
	if dotIdx < 0 {
		return config.ExpressionMapping{}, fmt.Errorf("missing '.' between table and column in %q (expected TABLE.COLUMN=EXPRESSION)", raw)
	}
	table := strings.TrimSpace(left[:dotIdx])
	column := strings.TrimSpace(left[dotIdx+1:])

	if table == "" {
		return config.ExpressionMapping{}, fmt.Errorf("empty table name in %q", raw)
	}
	if column == "" {
		return config.ExpressionMapping{}, fmt.Errorf("empty column name in %q", raw)
	}
	if expression == "" {
		return config.ExpressionMapping{}, fmt.Errorf("empty expression in %q", raw)
	}

	return config.ExpressionMapping{
		Table:      table,
		Column:     column,
		Expression: expression,
	}, nil
}
