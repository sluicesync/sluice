// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// Pooler-endpoint source advisory (roadmap item 69a, live-validated
// against Neon + Supabase 2026-07-15).
//
// A connection pooler in front of Postgres (Neon's `-pooler` hosts,
// Supabase's Supavisor, pgbouncer) proxies plain SQL fine — a full
// snapshot-pinned parallel migrate through Neon's pooler passed at the
// 50k-row validation scale — but it carries two real hazards the
// operator can't see from a green run:
//
//   - CDC/logical replication usually cannot run through it: most
//     poolers in transaction/statement mode strip the
//     `replication=database` startup parameter (Supavisor does), so
//     `sync start` fails at slot creation (the coded refusal in
//     pooler_detect.go). This is provider-dependent, not universal —
//     some session-mode/modern-pgbouncer setups forward replication
//     connections 1:1 (pgbouncer >= 1.24; Vultr's managed pools carry
//     CDC end-to-end, live-verified 2026-07-16) — which is why this
//     stays a WARN and the refusal fires only on the observed strip.
//   - sluice's parallel bulk copy pins N server connections inside
//     long-lived snapshot transactions; at higher parallelism/scale
//     that risks exhausting the pool mid-copy with a confusing
//     failure, and transaction-mode poolers additionally break pgx's
//     statement cache (SQLSTATE 42P05 → sluice's single-reader
//     fallback, silently losing parallel copy).
//
// Neither hazard is certain enough to refuse a bulk migrate that
// works, so this is the WARN-level [ir.SourceHostAdvisor] surface:
// detect the host class up front and recommend the direct endpoint.

// poolerHostPattern is one named entry in the pooler-host table. The
// label names the matched class in the WARN so an operator (or a
// report) can tell which heuristic fired; match is a pure predicate
// over the lowercased hostname.
type poolerHostPattern struct {
	label string
	match func(host string) bool
}

// poolerHostPatterns is the named table of known pooler host shapes,
// first match wins. Kept as a package-level table (the
// planetScaleMySQLHostSuffixes disposition) so adding a provider is a
// one-line edit with the evidence in the comment.
var poolerHostPatterns = []poolerHostPattern{
	// Supabase Supavisor: aws-0-<region>.pooler.supabase.com — both the
	// session (:5432) and transaction (:6543) modes share the hostname.
	{
		label: "Supabase Supavisor pooler",
		match: func(host string) bool { return strings.HasSuffix(host, ".pooler.supabase.com") },
	},
	// Neon: the pooled endpoint is the direct hostname with a `-pooler`
	// suffix on the first label (ep-xxx-pooler.<region>.aws.neon.tech).
	// Matching `-pooler.` as a label suffix also covers other providers
	// that reuse the convention.
	{
		label: "pooler endpoint (`-pooler` host label, e.g. Neon)",
		match: func(host string) bool { return strings.Contains(host, "-pooler.") },
	},
	// Generic: a hostname that names pgbouncer is a self-describing
	// pooler front.
	{
		label: "pgbouncer",
		match: func(host string) bool { return strings.Contains(host, "pgbouncer") },
	},
}

// SourceHostAdvisories implements [ir.SourceHostAdvisor]: it WARNs —
// for both migrate and CDC-anchoring runs — when the source DSN's
// host matches a known connection-pooler pattern, recommending the
// direct endpoint. Unparseable DSNs, socket paths, and non-matching
// hosts return nil (the connect path will surface any real DSN
// problem itself).
func (e Engine) SourceHostAdvisories(dsn string, cdc bool) []ir.SourceHostAdvisory {
	// Strip sluice's custom `schema` DSN parameter first (pgx rejects
	// it as unknown — the same reason every connection path routes
	// through parseDSN).
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil
	}
	host, ok := pgDSNHost(cfg.dsn)
	if !ok {
		return nil
	}
	for _, p := range poolerHostPatterns {
		if !p.match(host) {
			continue
		}
		msg := fmt.Sprintf(
			"source host %q matches a known connection-pooler pattern (%s): "+
				"most poolers strip replication=database, so CDC/logical replication typically fails at "+
				"slot creation, and parallel bulk copy pins connections in "+
				"long-lived snapshot transactions that can exhaust the pool mid-copy at scale "+
				"(transaction-mode poolers also disable parallel copy via the statement-cache fallback) — "+
				"prefer the provider's direct database endpoint",
			host, p.label,
		)
		if cdc {
			msg = fmt.Sprintf(
				"source host %q matches a known connection-pooler pattern (%s): "+
					"this run needs CDC/logical replication, and most poolers strip the "+
					"replication=database startup parameter (Supavisor and transaction-mode pgbouncer do, "+
					"so slot creation will fail there; some session-mode/modern-pgbouncer setups forward it) — "+
					"prefer the provider's direct database endpoint",
				host, p.label,
			)
		}
		return []ir.SourceHostAdvisory{{
			Message: msg,
			Hint:    "point --source at the direct (non-pooler) endpoint; see docs/managed-services.md",
		}}
	}
	return nil
}

// pgDSNHost extracts the lowercased hostname from a Postgres DSN (URI
// or keyword form) without connecting. Returns ("", false) on parse
// failure or when the "host" is a Unix-socket directory (an absolute
// path) — those configurations never match a managed-pooler pattern
// by construction.
func pgDSNHost(dsn string) (string, bool) {
	if dsn == "" {
		return "", false
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil || cfg.Host == "" || strings.HasPrefix(cfg.Host, "/") {
		return "", false
	}
	return strings.ToLower(cfg.Host), true
}
