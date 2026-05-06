// Package config loads sluice's runtime configuration from a YAML
// file overlaid with environment variables. The shape of the Config
// struct will grow as the orchestrator and translator mature; for
// now it captures the per-column type-mapping overrides and the
// Postgres extension allowlist documented in
// docs/architecture.md.
//
// Precedence (lowest → highest):
//
//  1. Defaults baked into the Config struct's zero values.
//  2. Values from the YAML file at the given path.
//  3. Environment variables prefixed with SLUICE_, with each
//     underscore in the variable name interpreted as a key separator
//     (SLUICE_FOO_BAR → foo.bar).
//
// CLI flags are not part of this layering — they are kong's concern
// and override anything the orchestrator reads from Config.
package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config is the loaded runtime configuration. Nil-safe: every field
// has a useful zero value, and Load returns a non-nil *Config even
// when the input is absent.
type Config struct {
	// Mappings is a list of per-column type-mapping overrides.
	// Each entry forces the translator to emit a specific target type
	// for the named (table, column) pair, ignoring whatever default
	// the type-mapping policy would otherwise pick.
	Mappings []Mapping `koanf:"mappings"`

	// ExpressionMappings is a list of per-column expression overrides.
	// Each entry replaces the source's `GENERATED ALWAYS AS (<expr>)`
	// body with operator-supplied target-dialect expression text,
	// bypassing the cross-dialect translator (ADR-0016) entirely for
	// that column. Operator owns the syntax; sluice emits the override
	// verbatim. The escape hatch for cases the translator's hand-coded
	// rewrite table doesn't recognise — see ADR-0016 §"Added in v0.10.0".
	ExpressionMappings []ExpressionMapping `koanf:"expression_mappings"`

	// Extensions controls how engine-specific extensions (notably
	// Postgres extensions) are handled during a migration.
	Extensions Extensions `koanf:"extensions"`

	// IncludeTables is the table-filter allow-list. Entries are
	// matched against unqualified source table names with stdlib
	// path.Match glob semantics ("audit_*"). Mutually exclusive
	// with ExcludeTables; the orchestrator surfaces a clear error
	// when both are populated. CLI flags --include-table /
	// --exclude-table override these YAML fields when supplied.
	IncludeTables []string `koanf:"include_tables"`

	// ExcludeTables is the table-filter deny-list. Same matching
	// semantics as IncludeTables, opposite sense. Mutually
	// exclusive with IncludeTables.
	ExcludeTables []string `koanf:"exclude_tables"`
}

// Mapping is a single per-column override.
type Mapping struct {
	// Table is the unqualified table name the override applies to.
	Table string `koanf:"table"`
	// Column is the column within the table.
	Column string `koanf:"column"`
	// TargetType names the target-engine type to emit. The valid set
	// is engine-specific and validated by the writer at apply time.
	TargetType string `koanf:"target_type"`
	// TargetTypeOptions carries optional sub-knobs for the target
	// type (for example, {"binary": true} when forcing JSONB on
	// Postgres). Free-form so writers can add options without
	// schema migrations of the config file.
	TargetTypeOptions map[string]any `koanf:"target_type_options"`
}

// ExpressionMapping is a single per-column generated-expression
// override. The Expression field is target-dialect text that sluice
// emits verbatim — the translator's pattern-based rewrites
// (ADR-0016) do not run when an override is present, so the operator
// is fully responsible for the syntax.
//
// v0.10.0 scope: generated-column bodies only. CHECK constraints,
// index expressions, and DEFAULT expressions get their own override
// types if/when real-world testing surfaces the need.
type ExpressionMapping struct {
	// Table is the unqualified table name the override applies to.
	Table string `koanf:"table"`
	// Column is the generated column whose body is being overridden.
	Column string `koanf:"column"`
	// Expression is the target-dialect text to emit verbatim inside
	// the `GENERATED ALWAYS AS (...)` clause. Operator-owned syntax;
	// sluice does not parse or validate it.
	Expression string `koanf:"expression"`
}

// Extensions controls extension-related behaviour, currently scoped
// to Postgres but extensible to other engines if a similar concept
// emerges.
type Extensions struct {
	// Allow lists the extensions the user has explicitly opted into
	// during a migration. Anything outside the list triggers a clear
	// error rather than silent best-effort handling.
	Allow []string `koanf:"allow"`
}

// Load reads the YAML file at path (if non-empty), then overlays
// SLUICE_-prefixed environment variables, and returns the merged
// Config.
//
// An empty path is valid: the function returns an empty Config
// (still merged with any env vars) without error. A non-empty path
// that doesn't exist is an error.
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("config: load %q: %w", path, err)
		}
	}

	// Environment variables. SLUICE_FOO_BAR maps to foo.bar in the
	// koanf key namespace; SLUICE_EXTENSIONS_ALLOW would map to
	// extensions.allow (which is a slice — koanf will split a comma-
	// separated env value into a slice via the unmarshal step).
	envProvider := env.Provider("SLUICE_", ".", func(s string) string {
		return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s, "SLUICE_")), "_", ".")
	})
	if err := k.Load(envProvider, nil); err != nil {
		return nil, fmt.Errorf("config: load env: %w", err)
	}

	var c Config
	if err := k.Unmarshal("", &c); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	return &c, nil
}
