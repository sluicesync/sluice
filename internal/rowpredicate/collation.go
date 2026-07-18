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
// A collation that does not resolve, a non-UTF-8 charset, a Postgres
// non-deterministic named collation, or --where-strict-collation on the ci/ai
// path still refuses loudly at compile time. The evaluator never guesses.
// (A Postgres DETERMINISTIC named collation — pg_collation.collisdeterministic
// — has a byte-exact `=`, so it is classified byte-exact upstream in
// [stringColumnInfo] and never reaches this Vitess comparator; only the
// MySQL-family ci/ai fold path does.)
//
// TODO(audit-2026-07-18 M2.1 / M-A1): this file is the ONLY importer of
// vitess.io/vitess outside internal/engines/mysql, so the engine-neutral
// evaluator carries a hard compile edge to a MySQL collation library. The
// clean containment is an engine-supplied collation-resolver interface (MySQL
// provides the Vitess-backed comparator; Postgres/SQLite resolve byte-exact-
// or-refuse through their own lens), threaded from the source engine into
// Compile. That refactor is deferred: it is not needed for correctness here —
// the Postgres path is already fully byte-exact-or-refuse WITHOUT touching this
// comparator (the determinism gate above), and MySQL is this comparator's only
// legitimate consumer — so the leak is an architectural wart, not a live
// fidelity bug. Batch B can introduce the resolver seam.

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

// collationEnvVersion is the MySQL version whose collation set + weights the
// client-side ci/ai comparator is pinned to. See [collationEnv].
const collationEnvVersion = "8.0.30"

// collationEnv returns the shared Vitess collation environment. The 8.0
// collation set is a superset: it carries the utf8mb4_0900_* family (MySQL 8
// default) plus the legacy utf8mb4_general_ci / utf8_general_ci / latin1_* /
// *_bin collations, so an older server's column collation still resolves.
// Vitess caches one Environment per version globally, so sharing is safe.
//
// F0-7 (audit 2026-07-18) — the version pin's assumption, stated honestly.
// The comparator's case/accent WEIGHTS come from this pinned env, not from the
// source tablet. For the standard MySQL/PlanetScale/Vitess deployments this is
// faithful: the utf8mb4 collation weight tables are stable across the 8.0.x
// line, and 8.0.30's set is a superset of the 5.7 / MariaDB legacy names. The
// residual risk is an EXOTIC self-hosted-Vitess tablet running a MySQL/MariaDB
// build whose weights for a given collation DIFFER from 8.0.30 — a
// theoretically possible divergence for the ci/ai fold path. sluice cannot
// cheaply learn a VStream tablet's underlying mysqld version at predicate-
// compile time (there is no reachable per-tablet version signal on the source
// engine surface the evaluator sees), so it does NOT silently trust a
// mismatch it can't detect: the operator-facing contract (documented in
// docs/operator/filtered-subset-migration.md) is that the client-side ci/ai
// collation set is pinned to MySQL 8.0.30, and an operator on such an exotic
// build who needs a byte-exact guarantee uses --where-strict-collation (which
// refuses the fold path outright) or normalizes on the source. When a
// reachable version signal is added to the source engine surface, this env can
// be constructed per-source and a detected mismatch warned/refused; until then
// the pin is explicit and the escape hatch is documented rather than a silent
// best-effort.
func collationEnv() *collations.Environment {
	collationEnvOnce.Do(func() {
		collationEnvInst = collations.NewEnvironment(collationEnvVersion)
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
