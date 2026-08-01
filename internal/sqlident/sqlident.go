// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package sqlident is the shape guard for the handful of DDL positions
// where an emitter interpolates a SCHEMA-SUPPLIED name into a statement
// BARE — no quoting, because the grammar at those positions does not take
// a quoted identifier (`USING <am>`, `FOR <command>`, `AS <type>`,
// `<opclass>`, `CHARACTER SET <name>`).
//
// Everywhere else in sluice a name goes through the engine's quoteIdent
// and a hostile value is inert. At these five positions it is not: every
// Postgres DDL statement runs through ExecContext with zero arguments,
// and pgx v5 forces QueryExecModeSimpleProtocol when there are none — and
// the simple protocol takes `;`-separated statements. So a schema whose
// `Index.Method` reads `btree; CREATE ROLE attacker SUPERUSER; --` is not
// a corrupt schema, it is arbitrary SQL (ADR-0183 Tier 1).
//
// The guard is deliberately a SHAPE test, not an allowlist of known
// access methods or collations: `ivfflat` / `hnsw` (pgvector),
// `gin_trgm_ops` (pg_trgm) and every future extension-introduced name are
// legitimate by design, and an allowlist would refuse tomorrow's
// extension. What is refused is anything that is not a bare identifier —
// which every real access method, opclass, charset and collation is, and
// which no injected statement can be.
//
// It lives here rather than in each engine because these positions are
// ONE class across two engines, and a guard that ships in one engine and
// not its sibling is this project's most expensive recurring shape.
package sqlident

import (
	"fmt"
	"regexp"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// BarePattern is the accepted shape for a bare-interpolated identifier: a
// leading letter or underscore, then letters, digits, underscores or `$`.
// It accepts every access method, operator class, charset and collation
// name the readers can produce (`btree`, `ivfflat`, `hnsw`,
// `gin_trgm_ops`, `text_pattern_ops`, `utf8mb4`, `utf8mb4_0900_ai_ci`)
// and nothing that carries a quote, a space, a semicolon, or a comment
// introducer.
const BarePattern = `^[A-Za-z_][A-Za-z0-9_$]*$`

var bareIdent = regexp.MustCompile(BarePattern)

// IsBare reports whether s can be interpolated into DDL unquoted.
func IsBare(s string) bool { return bareIdent.MatchString(s) }

// Refuse returns the coded refusal for a value that cannot be emitted at
// a bare position. kind names the position ("index access method"), where
// names the object the value came from ("index \"orders_idx\" on
// \"public\".\"orders\"") so the operator can find the row — a refusal
// that does not name what it refused is a support ticket.
func Refuse(kind, where, value string) error {
	return sluicecode.Wrap(sluicecode.CodeSchemaIdentifierInvalid,
		"correct the value at the named object in the SOURCE schema (or the backup manifest it was read from) and re-run; "+
			"there is deliberately no flag to emit it anyway. If this came from a backup, the manifest's schema may have been "+
			"tampered with — sign chains (--sign/--sign-key + --require-signature) so an edit is caught at verify time. "+
			"Background: docs/operator/error-codes.md, SLUICE-E-SCHEMA-IDENTIFIER-INVALID",
		fmt.Errorf("%s: %s %q is not a bare SQL identifier (%s) — refusing to emit it unquoted into DDL",
			where, kind, value, BarePattern))
}

// Check refuses value unless it is a bare identifier. An EMPTY value is
// accepted: every one of these positions is optional, and the emitters
// omit the whole clause when the IR carries nothing.
func Check(kind, where, value string) error {
	if value == "" || IsBare(value) {
		return nil
	}
	return Refuse(kind, where, value)
}

// CheckOneOf refuses value unless it is one of allowed — the closed-set
// form, for grammar positions that take a KEYWORD rather than a name
// (`CREATE POLICY ... FOR <command>`, `CREATE SEQUENCE ... AS <type>`).
// A closed set is available there and is strictly stronger than the shape
// test, so it is what those positions use. An empty value is accepted for
// the same reason as [Check]; the caller has already applied its default.
func CheckOneOf(kind, where, value string, allowed ...string) error {
	if value == "" {
		return nil
	}
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return sluicecode.Wrap(sluicecode.CodeSchemaIdentifierInvalid,
		"correct the value at the named object in the SOURCE schema (or the backup manifest it was read from) and re-run; "+
			"there is deliberately no flag to emit it anyway. If this came from a backup, the manifest's schema may have been "+
			"tampered with — sign chains (--sign/--sign-key + --require-signature) so an edit is caught at verify time. "+
			"Background: docs/operator/error-codes.md, SLUICE-E-SCHEMA-IDENTIFIER-INVALID",
		fmt.Errorf("%s: %s %q is not one of the accepted values (%s) — refusing to emit it into DDL",
			where, kind, value, strings.Join(allowed, ", ")))
}
