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
	default:
		return nil, fmt.Errorf("unknown strategy %q (supported: null, static:<v>, hash:sha256, hash:hmac-sha256, truncate:<n>)", name)
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
