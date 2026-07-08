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
//  3. Environment variables prefixed with SLUICE_, resolved against
//     the Config struct's koanf keys: flat keys keep their
//     underscores (SLUICE_KEYSET_SOURCE → keyset_source), nested keys
//     map to dotted paths (SLUICE_EXTENSIONS_ALLOW →
//     extensions.allow), and map-valued keys take the variable's tail
//     as the map key (SLUICE_NAMESPACE_MAP_APP → namespace_map.app).
//
// CLI flags are not part of this layering — they are kong's concern
// and override anything the orchestrator reads from Config.
package config

import (
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"sluicesync.dev/sluice/internal/sluicecode"
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

	// NamespaceMap is the YAML form of the --map-database / --map-schema
	// per-namespace target rename (ADR-0142). Each key is a SOURCE namespace
	// (MySQL database / PG schema); each value is the TARGET namespace it
	// routes to in a multi-namespace fan-out. Identity by default (an absent
	// key keeps its source name). CLI --map-* flags override this YAML field
	// wholesale when supplied (same precedence as --include-table over
	// include_tables). Many-to-one (two sources → one target) is refused
	// loudly at construction.
	//
	//	namespace_map:
	//	  app: app_prod
	//	  billing: billing_prod
	NamespaceMap map[string]string `koanf:"namespace_map"`

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
// Load errors are wrapped in [sluicecode.ConfigError] so the CLI's
// exit boundary maps them to exit code 2 (config error) — this is the
// single construction chokepoint, so no command file needs to know.
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, &sluicecode.ConfigError{Err: fmt.Errorf("config: load %q: %w", path, err)}
		}
	}

	// Environment variables. Each SLUICE_* variable is resolved against
	// the Config struct's koanf keys (see [envKeyIndex]); a slice-typed
	// key accepts a comma-separated value (koanf's default unmarshal
	// splits it), e.g. SLUICE_EXTENSIONS_ALLOW="citext,pg_trgm".
	//
	// Pre-audit-N-10 the mapping replaced EVERY underscore with a dot,
	// which made any underscore-keyed field (keyset_source,
	// include_tables, exclude_tables, …) silently unreachable from the
	// environment. A variable that resolves to no known key is now
	// skipped from the overlay with a loud WARN naming it and the valid
	// keys — deliberately NOT a hard error, because SLUICE_-prefixed
	// names are also legitimate outside this file: kong env bindings
	// (SLUICE_SOURCE / SLUICE_TARGET / SLUICE_NOTIFY_*) and operator-
	// chosen secret holders (--encrypt-passphrase-source
	// env:SLUICE_BACKUP_PASS, --keyset-source env:SLUICE_KEYSET — both
	// documented shapes) would all break under a refusal here.
	idx := buildEnvKeyIndex()
	var unknown []string
	envProvider := env.Provider("SLUICE_", ".", func(s string) string {
		key, ok := idx.resolve(s)
		if !ok {
			if !isProcessLevelEnvVar(s) {
				unknown = append(unknown, s)
			}
			return "" // koanf skips empty keys — the var is not a config key
		}
		return key
	})
	if err := k.Load(envProvider, nil); err != nil {
		return nil, &sluicecode.ConfigError{Err: fmt.Errorf("config: load env: %w", err)}
	}
	for _, name := range unknown {
		slog.Warn(
			"config: ignoring SLUICE_ environment variable that matches no config key (typo?)",
			slog.String("var", name),
			slog.String("valid_keys", strings.Join(idx.validKeys(), ", ")),
		)
	}

	// The explicit StringToSliceHookFunc makes a comma-separated env
	// value land as a []string ("citext,pg_trgm" → [citext pg_trgm]) —
	// koanf's default unmarshal leaves it a one-element slice. Mirrors
	// the fleet loader's decoder config (cmd/sluice/sync_run.go).
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &mapstructure.DecoderConfig{
			DecodeHook:       mapstructure.StringToSliceHookFunc(","),
			Result:           &c,
			WeaklyTypedInput: true,
			TagName:          "koanf",
		},
	}); err != nil {
		return nil, &sluicecode.ConfigError{Err: fmt.Errorf("config: unmarshal: %w", err)}
	}
	return &c, nil
}

// envKeyIndex maps SLUICE_* environment variable names onto the Config
// struct's koanf key namespace. Built by reflection over the koanf tags
// so a new Config field is automatically addressable from the
// environment without touching a hand-maintained table.
type envKeyIndex struct {
	// exact maps the underscored form of every env-settable key to its
	// canonical dotted koanf path: "keyset_source" → "keyset_source",
	// "extensions_allow" → "extensions.allow".
	exact map[string]string

	// mapPrefix maps the underscored path of every map[string]string
	// field to its dotted path; the remainder of the variable name
	// becomes the (lowercased) map key: SLUICE_NAMESPACE_MAP_APP →
	// namespace_map.app.
	mapPrefix map[string]string
}

func buildEnvKeyIndex() *envKeyIndex {
	idx := &envKeyIndex{exact: map[string]string{}, mapPrefix: map[string]string{}}
	addEnvKeys(reflect.TypeOf(Config{}), "", idx)
	return idx
}

// addEnvKeys walks t's koanf-tagged fields, registering each
// env-settable key path on idx. Slice-of-struct and map-of-struct
// blocks (mappings, redactions, dictionaries, …) are YAML-only — an
// environment variable can't express them — so they are deliberately
// NOT registered; an attempt to set one from env warns as unknown
// rather than half-parsing.
func addEnvKeys(t reflect.Type, dotted string, idx *envKeyIndex) {
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("koanf")
		if tag == "" || !f.IsExported() {
			continue
		}
		path := tag
		if dotted != "" {
			path = dotted + "." + tag
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		switch ft.Kind() {
		case reflect.Struct:
			addEnvKeys(ft, path, idx)
		case reflect.Map:
			if ft.Elem().Kind() == reflect.String {
				idx.mapPrefix[underscored(path)] = path
			}
		case reflect.Slice:
			if ft.Elem().Kind() != reflect.Struct {
				idx.exact[underscored(path)] = path
			}
		default:
			idx.exact[underscored(path)] = path
		}
	}
}

func underscored(dotted string) string { return strings.ReplaceAll(dotted, ".", "_") }

// resolve maps one SLUICE_* environment variable name to its koanf
// key. ok=false means the variable names no known config key.
func (idx *envKeyIndex) resolve(envName string) (key string, ok bool) {
	rest := strings.ToLower(strings.TrimPrefix(envName, "SLUICE_"))
	if key, ok := idx.exact[rest]; ok {
		return key, true
	}
	for prefix, dotted := range idx.mapPrefix {
		if strings.HasPrefix(rest, prefix+"_") && len(rest) > len(prefix)+1 {
			return dotted + "." + rest[len(prefix)+1:], true
		}
	}
	return "", false
}

// validKeys renders the settable key set for the unknown-variable WARN,
// sorted for stable output. Map-valued keys are shown as `path.*`.
func (idx *envKeyIndex) validKeys() []string {
	keys := make([]string, 0, len(idx.exact)+len(idx.mapPrefix))
	for _, dotted := range idx.exact {
		keys = append(keys, dotted)
	}
	for _, dotted := range idx.mapPrefix {
		keys = append(keys, dotted+".*")
	}
	sort.Strings(keys)
	return keys
}

// isProcessLevelEnvVar reports whether name is a SLUICE_-prefixed
// variable that belongs to the CLI / hook surface rather than the
// config file — those are consumed elsewhere (kong `env:` tags,
// os.Getenv) and must neither overlay the config nor trip the
// unknown-variable WARN. The set mirrors the kong bindings in
// cmd/sluice; SLUICE_ROLLOVER_* is the env sluice itself exports to
// backup rollover hooks.
func isProcessLevelEnvVar(name string) bool {
	switch name {
	case "SLUICE_SOURCE", "SLUICE_TARGET",
		"SLUICE_NOTIFY_WEBHOOK", "SLUICE_NOTIFY_SLACK", "SLUICE_NOTIFY_SMTP_PASSWORD":
		return true
	}
	return strings.HasPrefix(name, "SLUICE_ROLLOVER_")
}
