// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// ADR-0174 Piece 1 — faithful case/accent-insensitive string comparison.
//
// A filtered CDC stream evaluates the `--where` predicate CLIENT-SIDE to
// classify each change (move-in / move-out / in-scope). For a string column
// under a case- or accent-insensitive collation (MySQL's platform default),
// a byte-exact client compare would DIVERGE from the source's own `=` — and
// on the VStream path, from the server-side filter Vitess already applied —
// silently leaking or dropping a row. Rather than reimplement collation
// folding (wrong for accents, locale tailoring, ß, Turkish dotless-i, …),
// sluice reuses Vitess's OWN comparator: the same evalengine.NullsafeCompare
// over the same collations.Environment that the source uses. The client-side
// equality is then byte-identical to the source's by construction.
//
// This is the mechanism the pre-0174 blanket refusal is replaced with: a
// column whose collation RESOLVES here is compared faithfully; a collation
// that does not resolve (or an operator who passes --where-strict-collation)
// still refuses loudly at compile time. The evaluator never guesses.

// collationID aliases the Vitess collation identifier so the rest of the
// package refers to collations by this local name without importing the
// vitess collations package. The zero value (collations.Unknown) means
// "no faithful collation resolved" — a byte-exact or refused comparison.
type collationID = collations.ID

var (
	collationEnvOnce sync.Once
	collationEnvInst *collations.Environment
)

// collationEnv returns the shared Vitess collation environment. The 8.0
// collation set is a superset: it carries the utf8mb4_0900_* family (MySQL 8
// default) plus the legacy utf8mb4_general_ci / utf8_general_ci / latin1_* /
// *_bin collations, so an older server's column collation still resolves.
// Vitess caches one Environment per version globally, so sharing is safe.
func collationEnv() *collations.Environment {
	collationEnvOnce.Do(func() {
		collationEnvInst = collations.NewEnvironment("8.0.30")
	})
	return collationEnvInst
}

// resolveCollation maps a MySQL collation NAME (e.g. "utf8mb4_0900_ai_ci") to
// a Vitess collation ID and reports whether it is usable for a faithful
// client-side comparison. An empty or unrecognized name yields (0, false) so
// the caller refuses loudly rather than compare under a guessed collation.
func resolveCollation(name string) (collationID, bool) {
	if name == "" {
		return 0, false
	}
	id := collationEnv().LookupByName(name)
	if id == collations.Unknown {
		return 0, false
	}
	return id, true
}

// collationEqual reports whether a == b under collation id, using Vitess's
// own comparator — the identical code path MySQL/Vitess evaluate `=` with, so
// the result cannot diverge from the source. id must be a resolved
// (non-Unknown) collation, guaranteed by the compile-time gate.
func collationEqual(a, b string, id collationID) bool {
	r, err := evalengine.NullsafeCompare(sqltypes.NewVarChar(a), sqltypes.NewVarChar(b), collationEnv(), id, nil)
	if err != nil {
		// Two VARCHARs under a resolved collation do not error in practice;
		// treat an unexpected error as not-equal (the safe, non-widening
		// direction) rather than risk a false in-scope classification.
		return false
	}
	return r == 0
}
