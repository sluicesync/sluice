// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package diagnose implements the `sluice diagnose` operator-bundle
// assembler (ADR-0056) — the cockroach-debug-zip-shape ZIP bundle the
// operator attaches when filing GitHub issues against sluice.
//
// **Diagnose redaction is config/DSN-level, NOT row-value-level.**
// The `internal/redact` package implements per-column row-value
// redaction (hash:sha256, mask:pan, tokenize:dict, etc.) — that is
// data-level PII protection inside the streamer's hot path. Diagnose
// redaction is a DIFFERENT contract: it strips credentials from
// CONFIG (DSN userinfo, blob-store query strings) before the bundle
// hits the operator's filesystem. The two operate at different layers
// of the pipeline; conflating them would couple unrelated invariants.
//
// The redact helpers here MIRROR (don't depend on) `internal/redact`'s
// `redactDSNForAudit` and `internal/pipeline`'s `redactBlobURL`. We
// intentionally keep them as sibling implementations rather than
// exporting either: the audit-log helper and the blob-store helper
// are wired into hot paths where stability matters; the diagnose
// helper is bundle-time code that should remain free to evolve as
// the operator-bundle shape does.
package diagnose

import "strings"

// RedactDSN returns a credential-safe locator for the given DSN —
// host[:port][/db] only, with userinfo + query string stripped.
//
// Two DSN shapes are recognised:
//
//   - URI form (postgres://user:pass@host:port/db?sslmode=disable,
//     mysql://, etc.): the scheme prefix is stripped along with the
//     userinfo before '@' and any query string.
//   - go-sql-driver form (user:pass@tcp(host:port)/db?param=value):
//     everything up to and including the last '@' is dropped, and any
//     query string after the database name is dropped.
//
// Best-effort: an unparseable DSN collapses to "<dsn>" rather than
// risking leaking the raw string into the bundle. The DATABASE name
// (the path segment after host:port) is preserved deliberately — it's
// metadata an operator filing a GH issue needs to identify which
// database the bug reproduced against, and it's not credential
// material.
//
// Mirrors `internal/redact.redactDSNForAudit`'s shape; see the
// package comment for why we keep a sibling rather than importing.
func RedactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	// URI form: strip the scheme prefix, the userinfo before '@',
	// and any query string, leaving just host[:port][/db].
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			rest = rest[at+1:]
		}
		if q := strings.IndexAny(rest, "?"); q >= 0 {
			rest = rest[:q]
		}
		return rest
	}
	// go-sql-driver DSN form: take everything after the last '@'
	// (drops user:pw), then drop any query string.
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		rest := dsn[at+1:]
		if q := strings.IndexAny(rest, "?"); q >= 0 {
			rest = rest[:q]
		}
		return rest
	}
	return "<dsn>"
}

// redactedValue replaces the whole value of a bare-secret flag in the
// bundled command line. Unlike [RedactDSN] there is no host/db locator
// worth preserving — the value IS the credential.
const redactedValue = "<redacted>"

// RedactCLIArgs returns a copy of args with credential-bearing flag
// values redacted. Two flag families are recognised:
//
//   - DSN-bearing flags — --source, --target (the canonical DSN
//     flags), --keyset-source, --backup-target,
//     --position-from-manifest (URL-shaped flags that may carry
//     embedded credentials) — are run through [RedactDSN], which
//     preserves the host[:port][/db] locator.
//   - Bare-secret flags ([isSecretFlag]) — tokens, webhook URLs whose
//     path is the secret, passphrases — have their whole value
//     replaced with [redactedValue].
//
// Both forms are handled: `--flag value` (two tokens) and
// `--flag=value` (single token). Flags that aren't in the recognised
// sets are passed through untouched.
//
// This is the surface the `standard` and `verbose` privacy levels use
// to embed the operator's command-line in the bundle without leaking
// credentials.
func RedactCLIArgs(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out); i++ {
		arg := out[i]
		// --flag=value form: redact in place.
		if strings.HasPrefix(arg, "--") {
			eq := strings.Index(arg, "=")
			if eq > 0 {
				name := arg[:eq]
				switch {
				case isDSNFlag(name):
					out[i] = name + "=" + RedactDSN(arg[eq+1:])
				case isSecretFlag(name):
					out[i] = name + "=" + redactedValue
				}
				continue
			}
			// --flag value form: redact the next argument.
			if i+1 < len(out) {
				switch {
				case isDSNFlag(arg):
					out[i+1] = RedactDSN(out[i+1])
					i++ // skip the value we just rewrote
				case isSecretFlag(arg):
					out[i+1] = redactedValue
					i++
				}
			}
		}
	}
	return out
}

// isDSNFlag reports whether the given kong-style flag name is one of
// the recognised DSN-bearing flag families. The list is curated — we
// don't pattern-match on "looks like a URL" because operators may
// legitimately want some URL-shaped flag values (--metrics-listen,
// --pprof-listen) preserved as-is in the bundle for debugging.
func isDSNFlag(name string) bool {
	switch name {
	case "--source", "--target",
		"--keyset-source",
		"--backup-target",
		"--position-from-manifest":
		return true
	}
	return false
}

// isSecretFlag reports whether the flag's value is itself a
// credential and must be masked WHOLE ([redactedValue]), not run
// through the DSN redactor: service tokens, webhook URLs (a Slack
// incoming-webhook URL's path IS the secret), and passphrases. The
// list is curated for the same reason as [isDSNFlag]'s — it mirrors
// the cmd/sluice flags whose help text marks them as credentials.
func isSecretFlag(name string) bool {
	switch name {
	case "--planetscale-metrics-token",
		"--planetscale-metrics-token-id",
		"--notify-webhook",
		"--notify-slack",
		"--notify-smtp-password",
		"--encryption-passphrase":
		return true
	}
	return false
}
