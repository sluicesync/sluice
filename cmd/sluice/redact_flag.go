// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/redact"
)

// parseRedactFlags converts the operator's `--redact TABLE.COLUMN=STRATEGY[:options]`
// repeatable values into a [redact.Registry]. Returns (nil, nil) when
// the slice is empty (no redactions configured).
//
// Per-flag value format:
//
//	[schema.]table.column=strategy[:options]
//
// Schema is optional; empty schema applies to engines that resolve
// schema implicitly (MySQL's "database" defaulting to the DSN's
// configured database). Strategy is one of:
//
//   - `null`                 — replace with NULL (column must be NULLABLE)
//   - `static:<value>`       — replace with literal constant
//   - `hash:sha256`          — SHA-256 hex (stateless, deterministic)
//   - `hash:hmac-sha256`     — HMAC-SHA256 hex (requires --keyset-source)
//   - `truncate:<n>`         — keep first N runes (string columns only)
//
// keyset is the operator keyset resolved ONCE at startup from
// `--keyset-source` (PII Phase 4, ADR-0041; startup-snapshot only,
// decision D1). May be nil when no --keyset-source was supplied;
// rules that need a key (`hash:hmac-sha256`, `tokenize:dict`) then
// refuse loudly at preflight (decision D2 — clean break, no Phase 1
// shim). For CLI rules the optional `key:` name is the trailing
// `:<keyname>` segment of the spec (`hash:hmac-sha256:<keyname>`,
// `tokenize:dict:<dict>:<keyname>`); omitting it uses the keyset's
// default / sole entry.
//
// Returns an error on any malformed value (unknown strategy, bad
// option, missing keyset for HMAC, etc.) so misconfiguration fails
// loudly at startup before any data moves.
//
// streamID is mixed into `tokenize:dict`'s HMAC message; pass an
// empty string in contexts (like `sluice migrate`) where a
// stream-id isn't applicable.
//
// dictionaries is the pre-resolved dictionary map (name → entries)
// from [redact.LoadDictionaries]. Pass nil when no dictionaries are
// declared. Any rule referencing `tokenize:dict:<name>` or
// `randomize:dict:<name>` against a missing dictionary name is
// refused loudly. PII Phase 3 (v0.61.0+).
func parseRedactFlags(values []string, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (*redact.Registry, error) {
	if len(values) == 0 {
		return nil, nil
	}
	reg := redact.New()
	for _, raw := range values {
		schema, table, column, strategySpec, err := splitRedactValue(raw)
		if err != nil {
			return nil, fmt.Errorf("--redact %q: %w", raw, err)
		}
		strategy, err := strategyFromSpec(strategySpec, keyset, streamID, dictionaries)
		if err != nil {
			return nil, fmt.Errorf("--redact %q: %w", raw, err)
		}
		reg.Set(schema, table, column, strategy)
	}
	return reg, nil
}

// mergeYAMLRedactions extends an existing Registry (from CLI flag
// parsing) with the YAML `redactions:` block from the operator's
// config. CLI rules are processed FIRST, so YAML entries on the
// same column are silently overwritten — operators wanting YAML to
// be authoritative should not pass conflicting CLI flags.
//
// keyset is the startup-resolved operator keyset (ADR-0041); a
// YAML entry's optional `key:` field names which keyset key the
// hash/tokenize rule uses (omitting it uses the keyset default /
// sole entry). streamID is mixed into tokenize:dict's HMAC. Returns
// the (potentially-augmented) Registry or an error if any YAML
// entry is malformed.
//
// dictionaries is the pre-resolved dictionary map (name → entries)
// from [redact.LoadDictionaries]. Any YAML entry referencing a
// missing dictionary name is refused loudly. PII Phase 3 (v0.61.0+).
func mergeYAMLRedactions(reg *redact.Registry, entries []config.Redaction, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (*redact.Registry, error) {
	if len(entries) == 0 {
		return reg, nil
	}
	if reg == nil {
		reg = redact.New()
	}
	for i, entry := range entries {
		strategy, err := yamlStrategyToSluice(entry, keyset, streamID, dictionaries)
		if err != nil {
			return nil, fmt.Errorf("redactions[%d] (table=%q strategy=%q): %w", i, entry.Table, entry.Strategy, err)
		}
		schema, table, column, err := splitTriple(entry.Table)
		if err != nil {
			return nil, fmt.Errorf("redactions[%d]: %w", i, err)
		}
		reg.Set(schema, table, column, strategy)
	}
	return reg, nil
}

// yamlStrategyToSluice converts a config.Redaction to a redact.Strategy.
func yamlStrategyToSluice(entry config.Redaction, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (redact.Strategy, error) {
	// Field-cross-checks: `dict:` is only valid on `tokenize` and on
	// `randomize` + `form: dict`. Refusing spurious dict on other
	// strategies catches operator typos early. The randomize check
	// happens inside yamlRandomizeToSluice (it knows the form).
	if entry.Dict != "" && entry.Strategy != "tokenize" && entry.Strategy != "randomize" {
		return nil, fmt.Errorf("strategy %q takes no 'dict' field; remove it (dict is only valid on 'tokenize' and 'randomize' + form:dict)", entry.Strategy)
	}
	switch entry.Strategy {
	case "null":
		return redact.Null{}, nil
	case "static":
		// Empty Value is allowed (operator-explicit empty replacement).
		return redact.Static{Value: entry.Value}, nil
	case "hash":
		switch entry.Algo {
		case "sha256":
			return redact.Hash{Algo: "sha256"}, nil
		case "hmac-sha256":
			key, err := resolveKeysetKey(keyset, entry.Key, "hash:hmac-sha256")
			if err != nil {
				return nil, err
			}
			return redact.Hash{Algo: "hmac-sha256", Key: key}, nil
		case "":
			return nil, errors.New("strategy 'hash' requires 'algo' field: 'sha256' or 'hmac-sha256'")
		default:
			return nil, fmt.Errorf("strategy 'hash:%s' is not supported (use 'sha256' or 'hmac-sha256')", entry.Algo)
		}
	case "truncate":
		if entry.Length < 0 {
			return nil, fmt.Errorf("strategy 'truncate' requires non-negative 'length'; got %d", entry.Length)
		}
		return redact.Truncate{N: entry.Length}, nil
	case "mask":
		return yamlMaskToSluice(entry)
	case "randomize":
		return yamlRandomizeToSluice(entry, streamID, dictionaries)
	case "tokenize":
		return yamlTokenizeToSluice(entry, keyset, streamID, dictionaries)
	case "":
		return nil, errors.New("'strategy' field is required (null, static, hash, truncate, mask, randomize, tokenize)")
	default:
		return nil, fmt.Errorf("unknown strategy %q (supported: null, static, hash, truncate, mask, randomize, tokenize)", entry.Strategy)
	}
}

// yamlTokenizeToSluice converts a `strategy: tokenize` YAML entry
// into a concrete [redact.Strategy]. PII Phase 3 (v0.61.0+). Only
// shape supported today is `form: dict` + `dict: <name>`:
//
//   - table: users.first_name
//     strategy: tokenize
//     dict: first_names
//
// The `form:` field is optional and defaults to "dict" (the only
// supported shape) — operators can omit it.
//
// Refuses spurious min/max/brand/country_code (these belong to
// randomize, not tokenize), missing `dict:` field, or a `dict:`
// referencing a name not declared under top-level `dictionaries:`.
func yamlTokenizeToSluice(entry config.Redaction, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (redact.Strategy, error) {
	if entry.Min != 0 || entry.Max != 0 {
		return nil, errors.New("strategy 'tokenize' takes no min/max; remove the fields")
	}
	if entry.Brand != "" {
		return nil, errors.New("strategy 'tokenize' takes no brand; remove the field")
	}
	if entry.CountryCode != "" {
		return nil, errors.New("strategy 'tokenize' takes no country_code; remove the field")
	}
	// Form is optional and defaults to "dict". Anything else is
	// reserved for future shapes; refuse unknown forms loudly.
	switch entry.Form {
	case "", "dict":
		// ok
	default:
		return nil, fmt.Errorf("strategy 'tokenize' has unknown form %q (supported: dict)", entry.Form)
	}
	if entry.Dict == "" {
		return nil, errors.New("strategy 'tokenize' requires 'dict' field naming a dictionary declared under top-level 'dictionaries:'")
	}
	entries, err := redact.ResolveDictEntries(dictionaries, entry.Dict)
	if err != nil {
		return nil, err
	}
	key, err := resolveKeysetKey(keyset, entry.Key, "tokenize:dict")
	if err != nil {
		return nil, err
	}
	return redact.TokenizeDict{DictName: entry.Dict, Entries: entries, StreamID: streamID, Key: key}, nil
}

// yamlRandomizeToSluice converts a `strategy: randomize` YAML entry
// into a concrete [redact.Strategy]. The Form field selects between
// the nine randomize:* forms (PII Phase 2.c, v0.59.0 first wave +
// v0.60.0 second wave):
//
//   - form: int       — requires `min:` + `max:` integer fields
//   - form: email     — no other fields
//   - form: us-phone  — no other fields
//   - form: uuid      — no other fields
//   - form: ssn       — no other fields
//   - form: pan       — optional `brand:` (visa, mastercard, amex)
//   - form: ca-sin    — no other fields
//   - form: uk-nin    — no other fields
//   - form: iban      — optional `country_code:` (DE, GB, FR)
//
// Validation refuses missing form, unknown form, missing/invalid
// min/max on int form, spurious min/max/brand/country_code on
// forms that don't take them, and unsupported brand / country_code
// values (so operator misconfiguration is loud, not silent).
func yamlRandomizeToSluice(entry config.Redaction, _ string, dictionaries map[string][]string) (redact.Strategy, error) {
	// Guard helpers: each form rejects fields it doesn't use.
	noBounds := func(form string) error {
		if entry.Min != 0 || entry.Max != 0 {
			return fmt.Errorf("strategy 'randomize' form %q takes no min/max; remove the fields", form)
		}
		return nil
	}
	noBrand := func(form string) error {
		if entry.Brand != "" {
			return fmt.Errorf("strategy 'randomize' form %q takes no brand; remove the field", form)
		}
		return nil
	}
	noCountry := func(form string) error {
		if entry.CountryCode != "" {
			return fmt.Errorf("strategy 'randomize' form %q takes no country_code; remove the field", form)
		}
		return nil
	}
	noDict := func(form string) error {
		if entry.Dict != "" {
			return fmt.Errorf("strategy 'randomize' form %q takes no dict; remove the field (dict only applies to form: dict)", form)
		}
		return nil
	}
	rejectAllOptionals := func(form string) error {
		if err := noBounds(form); err != nil {
			return err
		}
		if err := noBrand(form); err != nil {
			return err
		}
		if err := noCountry(form); err != nil {
			return err
		}
		return noDict(form)
	}
	switch entry.Form {
	case "":
		return nil, errors.New("strategy 'randomize' requires 'form' field: one of 'int', 'email', 'us-phone', 'uuid', 'ssn', 'pan', 'ca-sin', 'uk-nin', 'iban', 'dict'")
	case "email":
		if err := rejectAllOptionals("email"); err != nil {
			return nil, err
		}
		return redact.RandomizeEmail{}, nil
	case "us-phone":
		if err := rejectAllOptionals("us-phone"); err != nil {
			return nil, err
		}
		return redact.RandomizeUSPhone{}, nil
	case "uuid":
		if err := rejectAllOptionals("uuid"); err != nil {
			return nil, err
		}
		return redact.RandomizeUUID{}, nil
	case "ssn":
		if err := rejectAllOptionals("ssn"); err != nil {
			return nil, err
		}
		return redact.RandomizeSSN{}, nil
	case "ca-sin":
		if err := rejectAllOptionals("ca-sin"); err != nil {
			return nil, err
		}
		return redact.RandomizeCASIN{}, nil
	case "uk-nin":
		if err := rejectAllOptionals("uk-nin"); err != nil {
			return nil, err
		}
		return redact.RandomizeUKNIN{}, nil
	case "pan":
		if err := noBounds("pan"); err != nil {
			return nil, err
		}
		if err := noCountry("pan"); err != nil {
			return nil, err
		}
		if err := noDict("pan"); err != nil {
			return nil, err
		}
		if err := redact.ValidatePANBrand(entry.Brand); err != nil {
			return nil, err
		}
		return redact.RandomizePAN{Brand: entry.Brand}, nil
	case "iban":
		if err := noBounds("iban"); err != nil {
			return nil, err
		}
		if err := noBrand("iban"); err != nil {
			return nil, err
		}
		if err := noDict("iban"); err != nil {
			return nil, err
		}
		if err := redact.ValidateIBANCountry(entry.CountryCode); err != nil {
			return nil, err
		}
		return redact.RandomizeIBAN{CountryCode: entry.CountryCode}, nil
	case "int":
		if err := noBrand("int"); err != nil {
			return nil, err
		}
		if err := noCountry("int"); err != nil {
			return nil, err
		}
		if err := noDict("int"); err != nil {
			return nil, err
		}
		if entry.Min > entry.Max {
			return nil, fmt.Errorf("strategy 'randomize' form 'int' requires min <= max; got min=%d, max=%d", entry.Min, entry.Max)
		}
		return redact.RandomizeInt{Min: entry.Min, Max: entry.Max}, nil
	case "dict":
		if err := noBounds("dict"); err != nil {
			return nil, err
		}
		if err := noBrand("dict"); err != nil {
			return nil, err
		}
		if err := noCountry("dict"); err != nil {
			return nil, err
		}
		if entry.Dict == "" {
			return nil, errors.New("strategy 'randomize' form 'dict' requires 'dict' field naming a dictionary declared under top-level 'dictionaries:'")
		}
		entries, err := redact.ResolveDictEntries(dictionaries, entry.Dict)
		if err != nil {
			return nil, err
		}
		return redact.RandomizeDict{DictName: entry.Dict, Entries: entries}, nil
	default:
		return nil, fmt.Errorf("strategy 'randomize' has unknown form %q (supported: int, email, us-phone, uuid, ssn, pan, ca-sin, uk-nin, iban, dict)", entry.Form)
	}
}

// yamlMaskToSluice converts a `strategy: mask` YAML entry into a
// concrete [redact.Strategy]. Two shapes are supported (mirrors
// the CLI's [parseMaskStrategy]):
//
// Generic format-preserving (PII Phase 2.a, v0.56.0+):
//
//   - table: users.pan
//     strategy: mask
//     form: inner      # inner | outer
//     m1: 4
//     m2: 4
//     char: X          # optional, single rune, defaults to "X"
//
// Country/format-specific preset (PII Phase 2.b, v0.57.0+):
//
//   - table: users.ssn
//     strategy: mask
//     form: ssn        # ssn | pan | pan-relaxed | email — no other fields needed
//
// Validation refuses negative margins, missing form, non-single-
// rune char, and spurious M1/M2/Char fields on presets (so
// operator misconfiguration is loud, not silent).
func yamlMaskToSluice(entry config.Redaction) (redact.Strategy, error) {
	switch entry.Form {
	case "":
		return nil, errors.New("strategy 'mask' requires 'form' field: 'inner', 'outer', or a preset (ssn, pan, pan-relaxed, email, ca-sin, uk-nin, iban, uuid)")
	case "ssn", "pan", "pan-relaxed", "email", "ca-sin", "uk-nin", "iban", "uuid":
		if entry.M1 != 0 || entry.M2 != 0 || entry.Char != "" {
			return nil, fmt.Errorf("strategy 'mask' preset 'form: %s' takes no other fields; remove m1/m2/char", entry.Form)
		}
		return parseMaskPreset(entry.Form)
	}
	var form redact.MaskForm
	switch entry.Form {
	case "inner":
		form = redact.MaskInner
	case "outer":
		form = redact.MaskOuter
	default:
		return nil, fmt.Errorf("strategy 'mask' has unknown form %q (supported: inner, outer, ssn, pan, pan-relaxed, email, ca-sin, uk-nin, iban, uuid)", entry.Form)
	}
	if entry.M1 < 0 {
		return nil, fmt.Errorf("strategy 'mask' requires non-negative 'm1'; got %d", entry.M1)
	}
	if entry.M2 < 0 {
		return nil, fmt.Errorf("strategy 'mask' requires non-negative 'm2'; got %d", entry.M2)
	}
	if entry.Char != "" {
		if n := utf8.RuneCountInString(entry.Char); n != 1 {
			return nil, fmt.Errorf("strategy 'mask' 'char' must be a single rune; got %d runes in %q", n, entry.Char)
		}
	}
	return redact.Mask{Form: form, M1: entry.M1, M2: entry.M2, Char: entry.Char}, nil
}

// splitTriple is YAML's variant of splitRedactValue's left half:
// "[schema.]table.column" → (schema, table, column).
func splitTriple(raw string) (schema, table, column string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", "", errors.New("'table' field is empty")
	}
	parts := strings.Split(raw, ".")
	switch len(parts) {
	case 2:
		return "", parts[0], parts[1], nil
	case 3:
		return parts[0], parts[1], parts[2], nil
	default:
		return "", "", "", fmt.Errorf("'table' field %q must be 'table.column' or 'schema.table.column'", raw)
	}
}

// splitRedactValue parses a `[schema.]table.column=strategy[:opts]`
// value into its parts. The split is conservative: the leftmost `=`
// separates the column-triple from the strategy spec, and within the
// triple the LAST two dots are the schema/table/column boundaries
// (so `customer_svc.users.email` parses as
// schema=customer_svc, table=users, column=email).
//
// Empty schema is allowed: `users.email=hash:sha256` parses as
// schema="", table=users, column=email.
func splitRedactValue(raw string) (schema, table, column, strategySpec string, err error) {
	eq := strings.Index(raw, "=")
	if eq < 0 {
		return "", "", "", "", errors.New("missing '=' between column triple and strategy")
	}
	triple := strings.TrimSpace(raw[:eq])
	strategySpec = strings.TrimSpace(raw[eq+1:])
	if triple == "" {
		return "", "", "", "", errors.New("column triple is empty")
	}
	if strategySpec == "" {
		return "", "", "", "", errors.New("strategy is empty")
	}
	parts := strings.Split(triple, ".")
	switch len(parts) {
	case 2:
		// table.column → schema empty
		return "", parts[0], parts[1], strategySpec, nil
	case 3:
		// schema.table.column
		return parts[0], parts[1], parts[2], strategySpec, nil
	default:
		return "", "", "", "", fmt.Errorf("column triple %q must be either 'table.column' or 'schema.table.column'", triple)
	}
}

// strategyFromSpec parses the strategy-spec portion of a --redact
// value into a [redact.Strategy]. The supported spec forms are
// listed in the parseRedactFlags doc-comment.
func strategyFromSpec(spec string, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (redact.Strategy, error) {
	name, opts, _ := strings.Cut(spec, ":")
	name = strings.TrimSpace(name)
	opts = strings.TrimSpace(opts)
	switch name {
	case "null":
		if opts != "" {
			return nil, fmt.Errorf("strategy 'null' takes no options; got ':%s'", opts)
		}
		return redact.Null{}, nil
	case "static":
		// `static:` with no value means empty replacement; `static:foo`
		// replaces with literal "foo". Either is acceptable.
		return redact.Static{Value: opts}, nil
	case "hash":
		// `hash:hmac-sha256[:<keyname>]` — the optional trailing
		// segment names a key in the operator keyset (ADR-0041);
		// omitting it uses the keyset default / sole entry.
		algo, keyName, _ := strings.Cut(opts, ":")
		switch algo {
		case "sha256":
			if keyName != "" {
				return nil, fmt.Errorf("strategy 'hash:sha256' takes no key name; got ':%s' (key names apply only to hmac-sha256)", keyName)
			}
			return redact.Hash{Algo: "sha256"}, nil
		case "hmac-sha256":
			key, err := resolveKeysetKey(keyset, keyName, "hash:hmac-sha256")
			if err != nil {
				return nil, err
			}
			return redact.Hash{Algo: "hmac-sha256", Key: key}, nil
		case "":
			return nil, errors.New("strategy 'hash' requires an algorithm: 'hash:sha256' or 'hash:hmac-sha256'")
		default:
			return nil, fmt.Errorf("strategy 'hash:%s' is not supported (use 'hash:sha256' or 'hash:hmac-sha256')", opts)
		}
	case "truncate":
		if opts == "" {
			return nil, errors.New("strategy 'truncate' requires a length: 'truncate:N'")
		}
		n, err := strconv.Atoi(opts)
		if err != nil {
			return nil, fmt.Errorf("strategy 'truncate:%s': length must be an integer", opts)
		}
		if n < 0 {
			return nil, fmt.Errorf("strategy 'truncate:%s': length must be non-negative", opts)
		}
		return redact.Truncate{N: n}, nil
	case "mask":
		return parseMaskStrategy(opts)
	case "randomize":
		return parseRandomizeStrategy(opts, dictionaries)
	case "tokenize":
		return parseTokenizeStrategy(opts, keyset, streamID, dictionaries)
	default:
		return nil, fmt.Errorf("unknown strategy %q (supported: null, static:<v>, hash:sha256, hash:hmac-sha256, truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:<preset>, randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid, randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>], randomize:dict:<name>, tokenize:dict:<name>)", name)
	}
}

// parseTokenizeStrategy parses the suffix of a `tokenize:` spec
// (PII Phase 3, v0.61.0). Only `dict:<name>` is supported today.
// streamID is captured into the strategy so the HMAC input depends
// on the active stream identifier (empty when migrate / no-stream
// contexts; see [redact.TokenizeDict] for the determinism contract).
//
// CLI form REQUIRES a YAML config because dictionaries live under
// the top-level `dictionaries:` block — there's no CLI-only form for
// declaring dictionary content. Operators using only CLI flags see
// a clear refusal naming the missing dictionary.
//
// The CLI form is `tokenize:dict:<dict>[:<keyname>]` — the optional
// trailing segment names a key in the operator keyset (ADR-0041);
// omitting it uses the keyset default / sole entry. tokenize:dict
// REQUIRES a resolvable keyset (the built-in v0.61.0 key was
// removed in Phase 4 — decision D2).
//
// Refuses on:
//
//   - Empty opts (no form supplied)
//   - Unknown form (today only "dict" is valid)
//   - Empty / missing dictionary name after `dict:`
//   - Dictionary name not declared in YAML
//   - No resolvable keyset key
func parseTokenizeStrategy(opts string, keyset *redact.Keyset, streamID string, dictionaries map[string][]string) (redact.Strategy, error) {
	if opts == "" {
		return nil, errors.New("strategy 'tokenize' requires a form: 'tokenize:dict:<name>' (the dictionary must be declared in YAML under 'dictionaries:')")
	}
	formName, rest, _ := strings.Cut(opts, ":")
	switch formName {
	case "dict":
		if rest == "" {
			return nil, errors.New("strategy 'tokenize:dict:': dictionary name is empty (use 'tokenize:dict:<name>' where <name> is declared in YAML under 'dictionaries:')")
		}
		dictName, keyName, _ := strings.Cut(rest, ":")
		if dictName == "" {
			return nil, errors.New("strategy 'tokenize:dict:': dictionary name is empty (use 'tokenize:dict:<name>' where <name> is declared in YAML under 'dictionaries:')")
		}
		entries, err := redact.ResolveDictEntries(dictionaries, dictName)
		if err != nil {
			return nil, err
		}
		key, err := resolveKeysetKey(keyset, keyName, "tokenize:dict")
		if err != nil {
			return nil, err
		}
		return redact.TokenizeDict{DictName: dictName, Entries: entries, StreamID: streamID, Key: key}, nil
	default:
		return nil, fmt.Errorf("strategy 'tokenize:%s': unknown form (supported: dict:<name>)", opts)
	}
}

// parseRandomizeStrategy parses the suffix of a `randomize:` spec
// (PII Phase 2.c, v0.59.0 first wave + v0.60.0 second wave).
// Supported forms:
//
//   - `int:<min>,<max>`       — integer in [min, max] inclusive
//   - `email`                 — `<rand-local>@<rand-domain>.test`
//   - `us-phone`              — `XXX-XXX-XXXX` (NANP-valid area/exchange)
//   - `uuid`                  — random UUIDv4 (canonical hyphenated form)
//   - `ssn`                   — US SSN `XXX-XX-XXXX`, reserved-range-avoiding
//   - `pan[:<brand>]`         — Luhn-valid PAN; brand: visa | mastercard | amex
//   - `ca-sin`                — Luhn-valid Canadian SIN `XXX-XXX-XXX`
//   - `uk-nin`                — UK NIN `AA999999A`
//   - `iban[:<country-code>]` — mod-97-valid IBAN; country: DE | GB | FR
//
// All randomize:* strategies derive their output from a per-row
// SHA-256 seed (streamID + table + column + primary-key values),
// so the same source row always produces the same target value.
// Tables without a primary key fail the preflight; randomize:*
// requires a PK for replay stability.
//
// Refuses on:
//
//   - Unknown form
//   - `int:` missing or malformed min/max (non-integer; min > max)
//   - Brand / country-code outside the supported set
//   - No-options form supplied with options (e.g., `randomize:uuid:foo`)
func parseRandomizeStrategy(opts string, dictionaries map[string][]string) (redact.Strategy, error) {
	if opts == "" {
		return nil, errors.New("strategy 'randomize' requires a form: 'int:<min>,<max>', 'email', 'us-phone', 'uuid', 'ssn', 'pan[:<brand>]', 'ca-sin', 'uk-nin', 'iban[:<country-code>]', or 'dict:<name>'")
	}
	formName, rest, ok := strings.Cut(opts, ":")
	if !ok {
		// No colon — a no-options form, or pan/iban with their
		// brand/country defaulted, or int (which requires bounds).
		switch formName {
		case "email":
			return redact.RandomizeEmail{}, nil
		case "us-phone":
			return redact.RandomizeUSPhone{}, nil
		case "uuid":
			return redact.RandomizeUUID{}, nil
		case "ssn":
			return redact.RandomizeSSN{}, nil
		case "ca-sin":
			return redact.RandomizeCASIN{}, nil
		case "uk-nin":
			return redact.RandomizeUKNIN{}, nil
		case "pan":
			return redact.RandomizePAN{}, nil
		case "iban":
			return redact.RandomizeIBAN{}, nil
		case "int":
			return nil, errors.New("strategy 'randomize:int' requires bounds: 'randomize:int:<min>,<max>'")
		case "dict":
			return nil, errors.New("strategy 'randomize:dict' requires a dictionary name: 'randomize:dict:<name>' (the dictionary must be declared in YAML under 'dictionaries:')")
		default:
			return nil, fmt.Errorf("strategy 'randomize:%s': unknown form (supported: int:<min>,<max>, email, us-phone, uuid, ssn, pan[:<brand>], ca-sin, uk-nin, iban[:<country-code>], dict:<name>)", formName)
		}
	}
	// Has a colon — int with min,max; pan with brand; iban with
	// country; dict with dictionary name; or a no-options form that
	// was given unexpected options.
	switch formName {
	case "email", "us-phone", "uuid", "ssn", "ca-sin", "uk-nin":
		return nil, fmt.Errorf("strategy 'randomize:%s': form '%s' takes no options (drop ':%s')", opts, formName, rest)
	case "dict":
		if rest == "" {
			return nil, errors.New("strategy 'randomize:dict:': dictionary name is empty (use 'randomize:dict:<name>' where <name> is declared in YAML under 'dictionaries:')")
		}
		entries, err := redact.ResolveDictEntries(dictionaries, rest)
		if err != nil {
			return nil, err
		}
		return redact.RandomizeDict{DictName: rest, Entries: entries}, nil
	case "pan":
		// `randomize:pan:` (trailing colon, empty brand) is operator
		// error — the no-brand form is `randomize:pan`. Refuse rather
		// than silently treating it as "random brand".
		if rest == "" {
			return nil, errors.New("strategy 'randomize:pan:': brand is empty after the colon (use 'randomize:pan' for random brand, or 'randomize:pan:visa' / ':mastercard' / ':amex')")
		}
		if err := redact.ValidatePANBrand(rest); err != nil {
			return nil, err
		}
		return redact.RandomizePAN{Brand: rest}, nil
	case "iban":
		// Same: refuse trailing-colon empty country.
		if rest == "" {
			return nil, errors.New("strategy 'randomize:iban:': country code is empty after the colon (use 'randomize:iban' for random country, or 'randomize:iban:DE' / ':GB' / ':FR')")
		}
		if err := redact.ValidateIBANCountry(rest); err != nil {
			return nil, err
		}
		return redact.RandomizeIBAN{CountryCode: rest}, nil
	case "int":
		parts := strings.Split(rest, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("strategy 'randomize:int:%s': expected '<min>,<max>'; got %d args", rest, len(parts))
		}
		minVal, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("strategy 'randomize:int:%s': min must be an integer", rest)
		}
		maxVal, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("strategy 'randomize:int:%s': max must be an integer", rest)
		}
		if minVal > maxVal {
			return nil, fmt.Errorf("strategy 'randomize:int:%s': min (%d) must not exceed max (%d)", rest, minVal, maxVal)
		}
		return redact.RandomizeInt{Min: minVal, Max: maxVal}, nil
	default:
		return nil, fmt.Errorf("strategy 'randomize:%s': unknown form %q (supported: int:<min>,<max>, email, us-phone, uuid, ssn, pan[:<brand>], ca-sin, uk-nin, iban[:<country-code>], dict:<name>)", opts, formName)
	}
}

// parseMaskStrategy parses the suffix of a `mask:` spec into a
// concrete [redact.Strategy]. Two shapes are supported:
//
//   - Generic format-preserving masks (PII Phase 2.a, v0.56.0):
//     `inner:<m1>,<m2>[,<char>]` and `outer:<m1>,<m2>[,<char>]`.
//     Examples: `mask:inner:4,4`, `mask:outer:2,2,*`.
//   - Country/format-specific presets (PII Phase 2.b, v0.57.0):
//     no-options names — `ssn`, `pan`, `pan-relaxed`, `email`.
//     Examples: `mask:ssn`, `mask:pan`, `mask:email`.
//
// Refuses on:
//
//   - Unknown form / preset
//   - Missing margins (for inner/outer)
//   - Non-integer or negative margins (for inner/outer)
//   - Multi-rune char (for inner/outer)
//   - Spurious options on a preset (operators tempted to write
//     `mask:ssn:something` get a clear "preset takes no options")
func parseMaskStrategy(opts string) (redact.Strategy, error) {
	if opts == "" {
		return nil, errors.New("strategy 'mask' requires a form: 'inner:<m1>,<m2>[,<char>]' / 'outer:<m1>,<m2>[,<char>]' or a preset (ssn, pan, pan-relaxed, email)")
	}
	formName, rest, ok := strings.Cut(opts, ":")
	if !ok {
		// No colon — must be a no-options preset.
		return parseMaskPreset(opts)
	}
	// Has a colon — must be inner/outer with margins, OR a preset
	// that was given unexpected options.
	switch formName {
	case "ssn", "pan", "pan-relaxed", "email", "ca-sin", "uk-nin", "iban", "uuid":
		return nil, fmt.Errorf("strategy 'mask:%s': preset 'mask:%s' takes no options (drop ':%s')", opts, formName, rest)
	}
	var form redact.MaskForm
	switch formName {
	case "inner":
		form = redact.MaskInner
	case "outer":
		form = redact.MaskOuter
	default:
		return nil, fmt.Errorf("strategy 'mask:%s': unknown form %q (supported: inner, outer, ssn, pan, pan-relaxed, email, ca-sin, uk-nin, iban, uuid)", opts, formName)
	}
	parts := strings.Split(rest, ",")
	if len(parts) < 2 || len(parts) > 3 {
		return nil, fmt.Errorf("strategy 'mask:%s': expected '<m1>,<m2>[,<char>]'; got %d args", opts, len(parts))
	}
	m1, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("strategy 'mask:%s': m1 must be an integer", opts)
	}
	if m1 < 0 {
		return nil, fmt.Errorf("strategy 'mask:%s': m1 must be non-negative", opts)
	}
	m2, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, fmt.Errorf("strategy 'mask:%s': m2 must be an integer", opts)
	}
	if m2 < 0 {
		return nil, fmt.Errorf("strategy 'mask:%s': m2 must be non-negative", opts)
	}
	char := ""
	if len(parts) == 3 {
		char = parts[2]
		if char == "" {
			return nil, fmt.Errorf("strategy 'mask:%s': char argument is empty (omit the trailing comma to default to 'X')", opts)
		}
		// Refuse multi-rune char defensively. Operators wanting
		// multi-character mask sequences should use a different
		// strategy.
		if n := utf8.RuneCountInString(char); n != 1 {
			return nil, fmt.Errorf("strategy 'mask:%s': char must be a single rune; got %d runes in %q", opts, n, char)
		}
	}
	return redact.Mask{Form: form, M1: m1, M2: m2, Char: char}, nil
}

// parseMaskPreset returns the concrete [redact.Strategy] for one of
// the PII Phase 2.b country/format-specific preset names (no
// options). Returns a clear error naming the unknown preset and
// listing the supported set so operators see what's available.
func parseMaskPreset(name string) (redact.Strategy, error) {
	switch name {
	case "ssn":
		return redact.MaskSSN{}, nil
	case "pan":
		return redact.MaskPAN{}, nil
	case "pan-relaxed":
		return redact.MaskPANRelaxed{}, nil
	case "email":
		return redact.MaskEmail{}, nil
	case "ca-sin":
		return redact.MaskCASIN{}, nil
	case "uk-nin":
		return redact.MaskUKNIN{}, nil
	case "iban":
		return redact.MaskIBAN{}, nil
	case "uuid":
		return redact.MaskUUID{}, nil
	case "inner", "outer":
		// Common mistake: dropped the colon + margins.
		return nil, fmt.Errorf("strategy 'mask:%s': '%s' is a generic form requiring margins (use 'mask:%s:<m1>,<m2>[,<char>]')", name, name, name)
	default:
		return nil, fmt.Errorf("strategy 'mask:%s': unknown form/preset (supported: inner:<m1>,<m2>[,<char>], outer:<m1>,<m2>[,<char>], ssn, pan, pan-relaxed, email, ca-sin, uk-nin, iban, uuid)", name)
	}
}

// resolveKeysetKey resolves the HMAC secret for a keyset-using
// strategy (`hash:hmac-sha256` / `tokenize:dict`) from the
// startup-resolved operator keyset (PII Phase 4, ADR-0041; clean
// break from Phase 1's --redact-key-source — decision D2).
//
// keyName is the rule's optional `key:` name (empty → keyset
// default / sole entry). strategyLabel names the rule for the loud
// preflight refusal when no keyset was supplied: any rule using
// hash:hmac-sha256 or tokenize:dict REQUIRES --keyset-source and
// the built-in v0.61.0 key was removed.
func resolveKeysetKey(keyset *redact.Keyset, keyName, strategyLabel string) ([]byte, error) {
	if keyset == nil {
		return nil, fmt.Errorf("strategy %q requires --keyset-source; the built-in v0.61.0 key was removed in PII Phase 4 (ADR-0041)", strategyLabel)
	}
	secret, _, _, err := keyset.ResolveKey(keyName)
	if err != nil {
		return nil, fmt.Errorf("strategy %q: %w", strategyLabel, err)
	}
	return secret, nil
}

// logRedactionConfig emits a single INFO line at command start
// summarising the operator's redaction configuration. Per the prep
// doc's audit-log decision: log the distinct strategy names + the
// column count, but NOT per-column rules (which could leak which
// columns hold PII — `--redact billing.credit_card=truncate:4` is
// itself sensitive information).
func logRedactionConfig(reg *redact.Registry, scope string) {
	if reg.Empty() {
		return
	}
	rules := reg.Rules()
	strategies := make([]string, 0, len(rules))
	seen := map[string]bool{}
	for _, r := range rules {
		name := r.Strategy.Name()
		if seen[name] {
			continue
		}
		seen[name] = true
		strategies = append(strategies, name)
	}
	slog.Info("sluice: redaction configured",
		slog.String("scope", scope),
		slog.Int("columns", len(rules)),
		slog.Any("strategies", strategies),
	)
}

// logKeysetLoaded emits the single ADR-0041 §"Audit log entry"
// INFO line at startup: source scheme + per-key generation list +
// active generation + hmac algo. Secret bytes are NEVER logged
// (that would defeat redaction); the DSN form is already
// credential-redacted by the loader. No-op when no keyset is
// configured (the no-keyset-needed common path stays silent).
//
// Per-row surrogate audit is intentionally NOT logged (ADR-0041) —
// the startup line is enough for the "which key was approved by
// ticket #1234" compliance case.
func logKeysetLoaded(keyset *redact.Keyset) {
	if keyset == nil {
		return
	}
	summary := keyset.AuditSummary()
	keys := make([]string, 0, len(summary))
	for _, e := range summary {
		keys = append(keys, fmt.Sprintf("%s{generations=%v active=%d}", e.Name, e.Generations, e.Active))
	}
	slog.Info("sluice: keyset loaded",
		slog.String("source", keyset.Source),
		slog.Any("keys", keys),
		slog.String("hmac-algo", "sha256"),
	)
}
