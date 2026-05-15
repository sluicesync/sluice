// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

	// Redactions is the YAML form of the `--redact` CLI flag (PII
	// Phase 1.5). Each entry declares a per-column redaction rule
	// that the orchestrator applies before the value reaches the
	// target. CLI `--redact` flags append to this list; duplicates
	// on the same column emit a WARN and last-write-wins.
	Redactions []Redaction `koanf:"redactions"`

	// KeysetSource mirrors `--keyset-source` (file:PATH | env:VAR |
	// db:DSN). Resolved ONCE at startup into an immutable keyset
	// snapshot (PII Phase 4, ADR-0041; startup-snapshot only — no
	// hot-reload). Consulted when at least one Redactions entry uses
	// `hash:hmac-sha256` or `tokenize:dict`; those strategies REQUIRE
	// a resolvable keyset (the Phase 1 --redact-key-source flag and
	// the built-in v0.61.0 tokenize key were removed — clean break).
	KeysetSource string `koanf:"keyset_source"`

	// Dictionaries is the YAML `dictionaries:` block (PII Phase 3,
	// v0.61.0+). Each entry declares a named dictionary that
	// `randomize:dict:<name>` and `tokenize:dict:<name>` rules
	// reference. Two forms per entry: inline `entries:` list, or
	// `file:` pointing at a one-entry-per-line file (`#`-prefixed
	// comment + blank-line tolerant). See
	// `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md`
	// Phase 3 section.
	Dictionaries map[string]Dictionary `koanf:"dictionaries"`
}

// Dictionary is one entry from the YAML `dictionaries:` block. PII
// Phase 3 (v0.61.0+). Operators declare it as either an inline list:
//
//	dictionaries:
//	  first_names:
//	    entries:
//	      - Alice
//	      - Bob
//	      - Carol
//
// or via a file pointer (one entry per line; #-prefixed and blank
// lines are tolerated):
//
//	dictionaries:
//	  city_names:
//	    file: ./fixtures/cities.txt
//
// Declaring both `file:` and `entries:` on the same dictionary is
// operator error and refused loudly at load time. Empty dictionaries
// (0 effective entries after trimming) are also refused — they would
// produce mod-by-zero in the strategies' RNG selection.
//
// Loaded by [redact.LoadDictionaries] (in the `internal/redact`
// package) before the per-rule parsers run; the resolved entries are
// embedded in each [redact.RandomizeDict] / [redact.TokenizeDict]
// instance so the strategy itself is self-contained at row-process
// time. See ADR-0040 for the determinism contract.
type Dictionary struct {
	// File is a path to a one-entry-per-line dictionary file.
	// `#`-prefixed lines are treated as comments; blank lines are
	// skipped. Trimming is applied to every line. Mutually exclusive
	// with Entries.
	File string `koanf:"file"`

	// Entries is an inline list of dictionary entries. Mutually
	// exclusive with File. Whitespace is trimmed from each entry;
	// empties are dropped.
	Entries []string `koanf:"entries"`
}

// Redaction is one entry from the YAML `redactions:` block. Mirrors
// the `--redact TABLE.COLUMN=STRATEGY[:options]` flag shape, broken
// into separate keys for YAML ergonomics.
//
//	redactions:
//	  - table: users.email          # [schema.]table.column
//	    strategy: hash              # null | static | hash | truncate
//	    algo: sha256                # hash:<algo>; "sha256" or "hmac-sha256"
//	  - table: users.phone
//	    strategy: truncate
//	    length: 4
//	  - table: billing.accounts.ssn
//	    strategy: static
//	    value: REDACTED
//	  - table: users.middle_name
//	    strategy: "null"             # MUST be quoted; bare `null` is YAML's null literal
//
// The CLI layer's parseRedactFlags converts these entries (plus any
// CLI flags) into a [redact.Registry]. The YAML form is the
// preferred mode for production deployments — version-controllable,
// reviewable, audit-friendly. The CLI form stays for ad-hoc use.
//
// Note on the `strategy: "null"` quoting: YAML treats the bare word
// `null` (also `~`, `Null`, `NULL`) as the YAML null literal which
// unmarshals to Go's empty string. Quoting forces it to stay a
// string. sluice's CLI form (`--redact users.middle=null`) has no
// such ambiguity. The quoting requirement is documented in
// operator-facing docs.
type Redaction struct {
	// Table is the full `[schema.]table.column` triple naming the
	// column to redact. Required.
	Table string `koanf:"table"`

	// Strategy is one of "null", "static", "hash", "truncate".
	// Required.
	Strategy string `koanf:"strategy"`

	// Algo is the hash algorithm when Strategy == "hash". Valid
	// values: "sha256", "hmac-sha256". Required for hash; ignored
	// for other strategies.
	Algo string `koanf:"algo"`

	// Value is the literal replacement when Strategy == "static".
	// Required for static; ignored for other strategies. Empty
	// string is a valid replacement (operator-explicit empty-out).
	Value string `koanf:"value"`

	// Length is the rune-count when Strategy == "truncate". Required
	// for truncate (must be non-negative); ignored for other
	// strategies.
	Length int `koanf:"length"`

	// Form is the mask form when Strategy == "mask". Valid values:
	// "inner" / "outer". Required for mask; ignored otherwise.
	// PII Phase 2.a (v0.56.0+).
	Form string `koanf:"form"`

	// M1 is the "first N chars" margin when Strategy == "mask".
	// Required for mask; non-negative.
	M1 int `koanf:"m1"`

	// M2 is the "last N chars" margin when Strategy == "mask".
	// Required for mask; non-negative.
	M2 int `koanf:"m2"`

	// Char is the mask character when Strategy == "mask". Defaults
	// to "X" when empty. Single rune only.
	Char string `koanf:"char"`

	// Min / Max are the integer bounds when Strategy == "randomize"
	// and Form == "int". PII Phase 2.c (v0.59.0). Inclusive; Min
	// must not exceed Max. Ignored for other forms / strategies.
	Min int64 `koanf:"min"`
	Max int64 `koanf:"max"`

	// Brand selects the issuer prefix when Strategy == "randomize"
	// and Form == "pan". PII Phase 2.c second wave (v0.60.0).
	// Valid values: "visa", "mastercard", "amex". Empty means
	// "pick a brand at random" (deterministic per-row seed).
	// Ignored for other forms / strategies.
	Brand string `koanf:"brand"`

	// CountryCode selects the country when Strategy == "randomize"
	// and Form == "iban". PII Phase 2.c second wave (v0.60.0).
	// Valid values: "DE", "GB", "FR". Empty means "pick a
	// country at random" (deterministic per-row seed). Ignored
	// for other forms / strategies.
	CountryCode string `koanf:"country_code"`

	// Dict names the dictionary the strategy resolves against when
	// Strategy == "tokenize" or Strategy == "randomize" + Form ==
	// "dict". PII Phase 3 (v0.61.0+). The named dictionary must
	// exist under the top-level `dictionaries:` block; absent /
	// typo'd names are refused at load time. Ignored for other
	// strategy / form combinations.
	Dict string `koanf:"dict"`

	// Key names which key in the operator keyset (resolved from
	// --keyset-source / config keyset_source) this rule uses. PII
	// Phase 4 (ADR-0041). Valid for Strategy == "hash" + Algo ==
	// "hmac-sha256" and Strategy == "tokenize". Empty uses the
	// keyset's declared `default` (or its sole entry when exactly
	// one key exists); with multiple keys and no default, omitting
	// Key is refused loudly. A named key pins to that key's active
	// generation regardless of rotation (see ADR-0041 determinism
	// contract). Ignored for other strategies.
	Key string `koanf:"key"`
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
