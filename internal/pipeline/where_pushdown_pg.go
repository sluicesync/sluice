// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
)

// ADR-0176 — Postgres publication row-filter push-down eligibility.
//
// Pushing a `--where` predicate into the publication makes the SERVER
// authoritative for what is delivered; the client-side evaluator degrades to
// a belt that can only see what already survived, so any cell where the
// server evaluates STRICTER than the client is silent loss — the exact A0
// shape ADR-0174 hit on VStream (NO-PAD server filter vs PAD-SPACE client).
// The defense is the same shape as A0's: a CONSERVATIVE classifier that
// enumerates the PROVEN-equivalent families only, with everything outside
// the envelope streaming UNFILTERED server-side and filtered client-side
// (the pre-ADR-0176 behavior, known-correct). The envelope below is exactly
// the family matrix the real-PG equivalence oracle exercises
// (TestPublicationScope_PushdownOracle); WIDENING IT WITHOUT EXTENDING THE
// ORACLE IS THE BUG-74 TRAP — the change-detector pin
// (TestPGPushdownEligible_EnvelopePin) fails on any widening so the two can
// only move together.

// pgPushdownEligible reports whether a compiled `--where` predicate on tbl
// falls entirely inside the proven push-down envelope, and — when it does
// not — a reason string for the operator-visible fallback log.
//
// The envelope, per referenced column (both dimensions of the oracle's
// matrix; the predicate-shape dimension needs no per-shape gating because
// every shape the rowpredicate grammar compiles is in the oracle):
//
//   - integer, numeric/decimal, boolean, date, timestamp WITHOUT time zone:
//     eligible. (tz-aware timestamps never compile — rowpredicate refuses.)
//   - text/varchar under the default collation ("") or COLLATE "C":
//     eligible. Any other named collation — POSIX included, byte-identical
//     or not — is OUTSIDE the proven envelope and falls back.
//   - everything else (char/bpchar PAD SPACE, enum, uuid/inet/cidr/macaddr,
//     time-of-day, float, binary/blob/JSON, …): fallback. Some of these are
//     probably equivalent, but "probably" is what the oracle exists to
//     replace; a family joins the envelope only with its oracle cells.
//
// One term-level exclusion rides on the predicate rather than the column:
// a BOOLEAN compared to a 0/1 numeric literal (`flag = 1`) is legal in the
// client grammar but not valid Postgres SQL (`boolean = integer` has no
// operator), so pushing it would fail the publication DDL loudly at sync
// start — classified out instead, it streams client-side like today.
func pgPushdownEligible(tbl *ir.Table, p *rowpredicate.Predicate) (eligible bool, reason string) {
	if tbl == nil || p == nil {
		return false, "no compiled predicate"
	}
	cols := make(map[string]*ir.Column, len(tbl.Columns))
	for _, c := range tbl.Columns {
		if c != nil {
			cols[strings.ToLower(c.Name)] = c
		}
	}
	for _, term := range p.PushdownTerms() {
		c, ok := cols[term.Column]
		if !ok {
			// Unreachable — Compile already refused unknown columns — but a
			// classifier must fail CLOSED, never push an unverifiable term.
			return false, fmt.Sprintf("column %q not found in the source schema", term.Column)
		}
		if term.BoolNumericLiteral {
			return false, fmt.Sprintf("boolean column %q is compared to a 0/1 numeric literal, which is not valid Postgres SQL in a publication row filter (use TRUE/FALSE)", c.Name)
		}
		if ok, reason := pgPushdownEligibleColumn(c); !ok {
			return false, reason
		}
	}
	return true, ""
}

// pgPushdownEligibleColumn is the per-column-type arm of the envelope.
// The switch is EXHAUSTIVELY pinned by TestPGPushdownEligible_EnvelopePin:
// adding a case here without extending both the pin and the real-PG oracle
// fails the build gate by design.
func pgPushdownEligibleColumn(c *ir.Column) (eligible bool, reason string) {
	switch t := c.Type.(type) {
	case ir.Integer, ir.Decimal, ir.Boolean, ir.Date, ir.DateTime:
		// ir.DateTime is the naive-timestamp family's PG spelling: the PG
		// catalog reads `timestamp [without time zone]` as ir.DateTime
		// (types.go), so it — not ir.Timestamp{} — is the type the oracle's
		// timestamp cells actually exercise on a PG source.
		return true, ""
	case ir.Timestamp:
		if t.WithTimeZone {
			// Unreachable (Compile refuses tz-aware comparisons); fail closed.
			return false, fmt.Sprintf("column %q is a tz-aware timestamp", c.Name)
		}
		// The cross-engine-shaped spelling of the same naive-timestamp
		// family (a PG catalog read never produces it — see ir.DateTime
		// above — but the family's semantics are identical).
		return true, ""
	case ir.Varchar:
		return pgPushdownEligibleTextCollation(c.Name, t.Collation)
	case ir.Text:
		return pgPushdownEligibleTextCollation(c.Name, t.Collation)
	default:
		return false, fmt.Sprintf("column %q has type %T, which is outside the proven publication push-down envelope", c.Name, c.Type)
	}
}

// pgPushdownEligibleTextCollation admits text/varchar only under the two
// collations the oracle ground-truths: the default ("" — the column
// inherits the database collation) and the explicit byte-order "C".
func pgPushdownEligibleTextCollation(name, collation string) (eligible bool, reason string) {
	if collation == "" || collation == "C" {
		return true, ""
	}
	return false, fmt.Sprintf("text column %q carries collation %q, outside the proven publication push-down envelope (default and \"C\" are proven)", name, collation)
}
