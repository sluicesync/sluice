// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"strings"
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
// sluice reuses Vitess's OWN comparator ([evalengine.NullsafeCompare]) for
// the case/accent axis.
//
// But that reuse is faithful only within a fenced envelope, because Vitess's
// comparator does NOT reproduce two axes of MySQL's `=` (audit 2026-07-18
// F0-1/F0-2/F0-6, ground-truthed against real MySQL 8.0):
//   - PAD_ATTRIBUTE: NullsafeCompare is NO-PAD regardless of the collation's
//     real pad attribute, so on a PAD SPACE collation (every legacy collation:
//     `*_general_ci`, `*_bin`, `latin1_*`, …) a value differing only by
//     TRAILING SPACE (`'EU'` vs `'EU '`) would mis-classify. The caller strips
//     trailing spaces on a PAD SPACE column before comparing ([collationNoPad]).
//   - CHARSET: sluice's row values are UTF-8 bytes; under a non-UTF-8 charset
//     collation (latin1, …) NullsafeCompare mis-decodes them. Such columns are
//     REFUSED at compile time ([collationCharsetUTF8]), not silently compared.
//
// A collation that does not resolve, a non-UTF-8 charset, a Postgres named
// (possibly non-deterministic) collation, or --where-strict-collation on the
// ci/ai path still refuses loudly at compile time. The evaluator never guesses.

// collationNoPad reports whether a collation compares with NO-PAD semantics
// (trailing spaces are SIGNIFICANT), as opposed to the legacy PAD SPACE default
// (trailing spaces ignored in `=`). MySQL's UCA-9.0.0 collations (`*_0900_*`)
// and the `binary` collation are NO PAD; every other collation is PAD SPACE
// (information_schema.COLLATIONS.PAD_ATTRIBUTE). A stable MySQL rule, ground-
// truthed in the real-MySQL collation family matrix. When false, the caller
// right-trims ASCII spaces before comparison to reproduce PAD SPACE `=`.
func collationNoPad(name string) bool {
	lc := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lc, "_0900_") || lc == "binary"
}

// collationCharsetUTF8 reports whether the collation's charset is a UTF-8
// family (utf8mb4 / utf8mb3 / utf8 / ascii) — the only charsets for which
// sluice's UTF-8-encoded row-value bytes are decoded correctly by
// [evalengine.NullsafeCompare]. A non-UTF-8 charset (latin1, gbk, …) would
// have the comparator read the value under the wrong encoding, so such a
// column's string comparison is refused rather than transcoded (audit F0-6).
func collationCharsetUTF8(name string) bool {
	lc := strings.ToLower(strings.TrimSpace(name))
	return strings.HasPrefix(lc, "utf8mb4_") || strings.HasPrefix(lc, "utf8mb3_") ||
		strings.HasPrefix(lc, "utf8_") || strings.HasPrefix(lc, "ascii_")
}

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
