// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
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
//   - `hash:hmac-sha256`     — HMAC-SHA256 hex (requires --redact-key-source)
//   - `truncate:<n>`         — keep first N runes (string columns only)
//
// keySource controls the HMAC keyset for `hash:hmac-sha256`. Supported
// forms (Phase 1):
//
//   - `env:VAR`              — read key from environment variable VAR
//   - `file:PATH`            — read key from file at PATH (one line, trimmed)
//   - `derive:<salt>`        — derive key from streamID + salt (default)
//
// Returns an error on any malformed value (unknown strategy, bad
// option, missing key-source for HMAC, etc.) so misconfiguration
// fails loudly at startup before any data moves.
//
// streamID is required only when keySource starts with `derive:`;
// pass an empty string in contexts (like `sluice migrate`) where a
// stream-id isn't applicable, in which case the salt alone keys the
// HMAC.
func parseRedactFlags(values []string, keySource, streamID string) (*redact.Registry, error) {
	if len(values) == 0 {
		return nil, nil
	}
	reg := redact.New()
	for _, raw := range values {
		schema, table, column, strategySpec, err := splitRedactValue(raw)
		if err != nil {
			return nil, fmt.Errorf("--redact %q: %w", raw, err)
		}
		strategy, err := strategyFromSpec(strategySpec, keySource, streamID)
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
// keySource and streamID are the effective values (CLI flag
// override of YAML). Returns the (potentially-augmented) Registry
// or an error if any YAML entry is malformed.
func mergeYAMLRedactions(reg *redact.Registry, entries []config.Redaction, keySource, streamID string) (*redact.Registry, error) {
	if len(entries) == 0 {
		return reg, nil
	}
	if reg == nil {
		reg = redact.New()
	}
	for i, entry := range entries {
		strategy, err := yamlStrategyToSluice(entry, keySource, streamID)
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
func yamlStrategyToSluice(entry config.Redaction, keySource, streamID string) (redact.Strategy, error) {
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
			key, err := resolveHMACKey(keySource, streamID)
			if err != nil {
				return nil, fmt.Errorf("strategy 'hash:hmac-sha256': %w", err)
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
		return yamlRandomizeToSluice(entry)
	case "":
		return nil, errors.New("'strategy' field is required (null, static, hash, truncate, mask, randomize)")
	default:
		return nil, fmt.Errorf("unknown strategy %q (supported: null, static, hash, truncate, mask, randomize)", entry.Strategy)
	}
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
func yamlRandomizeToSluice(entry config.Redaction) (redact.Strategy, error) {
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
	rejectAllOptionals := func(form string) error {
		if err := noBounds(form); err != nil {
			return err
		}
		if err := noBrand(form); err != nil {
			return err
		}
		return noCountry(form)
	}
	switch entry.Form {
	case "":
		return nil, errors.New("strategy 'randomize' requires 'form' field: one of 'int', 'email', 'us-phone', 'uuid', 'ssn', 'pan', 'ca-sin', 'uk-nin', 'iban'")
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
		if entry.Min > entry.Max {
			return nil, fmt.Errorf("strategy 'randomize' form 'int' requires min <= max; got min=%d, max=%d", entry.Min, entry.Max)
		}
		return redact.RandomizeInt{Min: entry.Min, Max: entry.Max}, nil
	default:
		return nil, fmt.Errorf("strategy 'randomize' has unknown form %q (supported: int, email, us-phone, uuid, ssn, pan, ca-sin, uk-nin, iban)", entry.Form)
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
func strategyFromSpec(spec, keySource, streamID string) (redact.Strategy, error) {
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
		switch opts {
		case "sha256":
			return redact.Hash{Algo: "sha256"}, nil
		case "hmac-sha256":
			key, err := resolveHMACKey(keySource, streamID)
			if err != nil {
				return nil, fmt.Errorf("strategy 'hash:hmac-sha256': %w", err)
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
		return parseRandomizeStrategy(opts)
	default:
		return nil, fmt.Errorf("unknown strategy %q (supported: null, static:<v>, hash:sha256, hash:hmac-sha256, truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:<preset>, randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid, randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>])", name)
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
func parseRandomizeStrategy(opts string) (redact.Strategy, error) {
	if opts == "" {
		return nil, errors.New("strategy 'randomize' requires a form: 'int:<min>,<max>', 'email', 'us-phone', 'uuid', 'ssn', 'pan[:<brand>]', 'ca-sin', 'uk-nin', or 'iban[:<country-code>]'")
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
		default:
			return nil, fmt.Errorf("strategy 'randomize:%s': unknown form (supported: int:<min>,<max>, email, us-phone, uuid, ssn, pan[:<brand>], ca-sin, uk-nin, iban[:<country-code>])", formName)
		}
	}
	// Has a colon — int with min,max; pan with brand; iban with
	// country; or a no-options form that was given unexpected options.
	switch formName {
	case "email", "us-phone", "uuid", "ssn", "ca-sin", "uk-nin":
		return nil, fmt.Errorf("strategy 'randomize:%s': form '%s' takes no options (drop ':%s')", opts, formName, rest)
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
		return nil, fmt.Errorf("strategy 'randomize:%s': unknown form %q (supported: int:<min>,<max>, email, us-phone, uuid, ssn, pan[:<brand>], ca-sin, uk-nin, iban[:<country-code>])", opts, formName)
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

// resolveHMACKey reads the HMAC keyset for `hash:hmac-sha256`
// according to the operator's `--redact-key-source` value:
//
//   - env:VAR        — value of environment variable VAR (trimmed)
//   - file:PATH      — first line of PATH (trimmed)
//   - derive:<salt>  — SHA-256(streamID + ":" + salt) bytes (Phase 1)
//
// streamID may be empty for contexts that don't have one
// (`sluice migrate`); the derive form still works — the key derives
// from just the salt.
func resolveHMACKey(source, streamID string) ([]byte, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, errors.New("--redact-key-source must be set when any rule uses 'hash:hmac-sha256'")
	}
	prefix, value, ok := strings.Cut(source, ":")
	if !ok {
		return nil, fmt.Errorf("--redact-key-source %q: expected 'env:VAR', 'file:PATH', or 'derive:<salt>'", source)
	}
	switch prefix {
	case "env":
		v := strings.TrimSpace(os.Getenv(value))
		if v == "" {
			return nil, fmt.Errorf("--redact-key-source env:%s: environment variable is empty", value)
		}
		return []byte(v), nil
	case "file":
		data, err := os.ReadFile(value)
		if err != nil {
			return nil, fmt.Errorf("--redact-key-source file:%s: %w", value, err)
		}
		// First line, trimmed. Multi-line files are operator error
		// (key files should be a single secret).
		first, _, _ := strings.Cut(string(data), "\n")
		key := strings.TrimSpace(first)
		if key == "" {
			return nil, fmt.Errorf("--redact-key-source file:%s: file is empty", value)
		}
		return []byte(key), nil
	case "derive":
		// Phase 1 derive: simple concat-and-hash. Phase 4 will replace
		// this with a proper keyset (ADR pending).
		return deriveHMACKey(streamID, value), nil
	default:
		return nil, fmt.Errorf("--redact-key-source %q: unknown scheme %q (expected env, file, or derive)", source, prefix)
	}
}

// deriveHMACKey is Phase 1's straightforward streamID+salt key
// derivation. SHA-256 of "streamID:salt" gives 32 bytes which is
// the standard HMAC-SHA256 key length. Phase 4 lands a proper
// keyset story; until then, operators wanting stable surrogates
// across multiple streams must use --redact-key-source env:VAR or
// file:PATH and supply the same key everywhere.
func deriveHMACKey(streamID, salt string) []byte {
	mat := streamID + ":" + salt
	sum := sha256SumImpl([]byte(mat))
	return sum[:]
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
