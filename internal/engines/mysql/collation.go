// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"sluicesync.dev/sluice/internal/ir"
)

// MySQL-family collation resolution for continuous FILTERED sync (ADR-0174
// Piece 1; audit 2026-07-18 M2.1 / M-A1).
//
// A filtered CDC stream evaluates the `--where` predicate CLIENT-SIDE per
// change to classify each row (move-in / move-out / in-scope). For a string
// column under a case- or accent-insensitive collation (MySQL's platform
// default) a byte-exact client compare would DIVERGE from the source's own `=`
// — and on the VStream path, from the server-side filter Vitess already applied
// — silently leaking or dropping a row. Rather than reimplement collation
// folding (wrong for accents, locale tailoring, ß, Turkish dotless-i, …), sluice
// reuses Vitess's OWN comparator ([evalengine.NullsafeCompare]) for the
// case/accent axis.
//
// This logic used to live in internal/rowpredicate (the engine-neutral
// evaluator), which made that package the ONLY importer of vitess.io/vitess
// outside this engine — a compile-time edge to a MySQL collation library in the
// orchestrator layer (audit M-A1). It now lives HERE, behind the engine-neutral
// [ir.CollationResolver] seam: the evaluator consumes only that interface, and
// MySQL supplies this Vitess-backed implementation via
// [Engine.CollationResolver]. Postgres/SQLite supply [ir.ByteExactCollationResolver].
//
// Two axes of MySQL's `=` that Vitess's comparator does NOT reproduce are
// fenced here (ground-truthed against real MySQL 8.0 — the family matrix in
// internal/rowpredicate/collation_realmysql_integration_test.go):
//   - PAD_ATTRIBUTE: NullsafeCompare is NO-PAD regardless of the collation's
//     real pad attribute, so on a PAD SPACE collation (every legacy collation)
//     a value differing only by TRAILING SPACE would mis-classify. The resolved
//     policy sets [ir.StringEquality.PadSpace] so the evaluator right-trims.
//   - CHARSET: sluice's row values are UTF-8 bytes; a non-UTF-8 charset
//     collation (latin1, …) would be mis-decoded, so such a column is REFUSED
//     (Faithful=false), not silently compared.

// mysqlCollationResolver is MySQL's [ir.CollationResolver]: it maps a MySQL
// collation name to the client-side string-comparison policy that reproduces
// the source's `=` faithfully, or refuses when it cannot. It carries the
// engine's flavor for the TEMPORAL axis only — the string lens is
// flavor-independent, but MariaDB truncates a finer-than-µs temporal literal
// where MySQL rounds it (see ResolveTemporalLiteralSemantics).
type mysqlCollationResolver struct {
	flavor Flavor
}

// CollationResolver implements [ir.CollationResolverProvider]: MySQL (and its
// PlanetScale/Vitess/MariaDB flavors) share one collation lens; the flavor is
// threaded for the temporal-literal axis, where MariaDB diverges.
func (e Engine) CollationResolver() ir.CollationResolver {
	return mysqlCollationResolver{flavor: e.Flavor}
}

// ResolveStringEquality implements [ir.CollationResolver] for the MySQL family.
// determinism is ignored (MySQL fidelity is driven by collation name + charset,
// not the PG determinism signal); strict is the --where-strict-collation
// opt-out that refuses the case/accent-insensitive FOLD path (byte-exact `_bin`
// stays allowed — byte-exact IS the strict guarantee). fixedChar is ignored:
// MySQL CHAR shares the collation's PAD_ATTRIBUTE with VARCHAR (already carried
// by collationNoPad below), unlike Postgres bpchar's collation-independent PAD
// SPACE — so a MySQL CHAR needs no char-specific override.
func (mysqlCollationResolver) ResolveStringEquality(collation string, _ ir.CollationDeterminism, strict, _ bool) ir.StringEquality {
	// A faithful comparison needs a KNOWN, UTF-8-charset collation: sluice
	// delivers row values as UTF-8 bytes, so a non-UTF-8 charset (latin1, …)
	// would be mis-decoded by the comparator (audit F0-6), and an unknown/empty
	// collation can't have its pad/fold reproduced — both refuse loudly.
	if collation == "" || !collationCharsetUTF8(collation) {
		return ir.StringEquality{}
	}
	// PAD SPACE (every legacy collation) vs NO PAD (*_0900_*, binary): the
	// evaluator right-trims trailing spaces on a PAD SPACE column so it matches
	// the source's own `=` (audit F0-1/F0-2). Carried on both the byte-exact
	// and the ci/ai path.
	padSpace := !collationNoPad(collation)
	if collationByteExactMySQL(collation) {
		// Byte-exact (_bin / binary): a raw memcmp reproduces the source's `=`,
		// with PAD handling; strict is irrelevant here (byte-exact IS the strict
		// guarantee).
		return ir.StringEquality{Faithful: true, PadSpace: padSpace}
	}
	// Collation-aware — the ci/ai folds AND the case+accent-SENSITIVE UCA
	// collations (utf8mb4_0900_as_cs and the tailored `_as_cs`/`_cs` language
	// variants): MySQL's `=` folds canonical equivalence (NFC/NFD) and ignores
	// UCA-ignorable code points (e.g. a soft hyphen), none of which a byte
	// compare reproduces (audit 2026-07-19 A1). Faithful via Vitess's comparator
	// IF the collation resolves and strict mode is off; else refuse.
	if !strict {
		if id, ok := resolveCollation(collation); ok {
			cid := id
			return ir.StringEquality{
				Faithful: true,
				PadSpace: padSpace,
				Compare:  func(a, b string) bool { return collationEqual(a, b, cid) },
			}
		}
	}
	return ir.StringEquality{}
}

// ResolveTemporalLiteralSemantics implements [ir.TemporalLiteralResolver] for
// the MySQL family: a DATE column compared against a time-bearing literal is
// PROMOTED to datetime and compared as the full instant (observed on MySQL
// 8.0.46 AND MariaDB 11.8.8, 2026-07-23: `d = '2026-01-15 08:30:00'` is
// FALSE, `d < '2026-01-15 12:00:00'` is TRUE — the opposite of Postgres,
// which truncates the literal to the date). Fractional seconds beyond the
// 6-digit fsp ceiling diverge BY FLAVOR: MySQL 8.0.46 rounds HALF-UP
// ('.1234565' → .123457, '.9999995' carries +1s) while MariaDB 11.8.8
// TRUNCATES ('.1234565' → .123456, '.9999995' → .999999, no carry) — the
// same server-behavior split the SHOW BINLOG STATUS / PAD_ATTRIBUTE
// catalogs carry, resolved per flavor here. PlanetScale/Vitess keep the
// vanilla MySQL rule (mysqld backs the tablets); the VStream server-side
// filter's OWN coercion of a finer-than-µs literal (vtgate evalengine) is a
// separate, unverified surface — noted in the ADR-0174 residuals, not
// resolved here.
func (r mysqlCollationResolver) ResolveTemporalLiteralSemantics() ir.TemporalLiteralSemantics {
	if r.flavor == FlavorMariaDB {
		return ir.TemporalLiteralPromoteTruncate
	}
	return ir.TemporalLiteralPromoteRoundHalfUp
}

// collationByteExactMySQL reports whether a MySQL collation's `=` is a raw
// byte/codepoint comparison (memcmp), so a client-side byte compare is faithful.
// ONLY the `_bin` collations and the `binary` pseudo-collation are byte-exact.
// Every other collation — including the case+accent-SENSITIVE UCA collations
// (`utf8mb4_0900_as_cs` and the `_cs`/`_as_cs`/tailored language variants) — is
// UCA-based: MySQL's `=` folds canonical equivalence (NFC/NFD) and ignores
// UCA-ignorable code points, which a byte compare does NOT reproduce (audit
// 2026-07-19 A1 — a `_0900_as_cs` column silently mis-classified an NFC/NFD or
// soft-hyphen-bearing row-move). Those route through the Vitess comparator
// instead. The caller has already excluded the empty/unknown collation.
//
// The `binary` disjunct is DEFENSIVE, not a live path: [ResolveStringEquality]
// refuses any non-UTF-8-charset collation (which `binary` is) before it reaches
// here, so a `binary` column never actually flows through this predicate today
// (audit 2026-07-19 E2). It is kept so the function is correct in isolation for
// any future caller that doesn't front it with the charset gate.
func collationByteExactMySQL(collation string) bool {
	lc := strings.ToLower(strings.TrimSpace(collation))
	return lc == "binary" || strings.HasSuffix(lc, "_bin")
}

// collationNoPad reports whether a collation compares with NO-PAD semantics
// (trailing spaces are SIGNIFICANT), as opposed to the legacy PAD SPACE default
// (trailing spaces ignored in `=`). The NO-PAD collations are:
//   - MySQL's UCA-9.0.0 collations (`*_0900_*`) and the `binary` collation
//     (information_schema.COLLATIONS.PAD_ATTRIBUTE on MySQL);
//   - MariaDB's `*_nopad_*` collations (`utf8mb4_nopad_bin`,
//     `utf8mb4_general_nopad_ci`, `utf8mb4_unicode[_520]_nopad_ci`). MariaDB's
//     information_schema.COLLATIONS.PAD_ATTRIBUTE is version-dependent — ABSENT
//     through the 11.x LTS line (11.4, 11.8) and 12.0, then ADDED in 12.1 — so
//     the catalog is not a version-robust signal; the `nopad` NAME token IS its
//     version-independent NO-PAD marker across every release (ground-truthed
//     behaviorally: `WHERE v='EU'` matches only `'EU'`, not `'EU '`, on
//     `utf8mb4_nopad_bin` in all versions).
//
// Every other collation is PAD SPACE. When false, the caller right-trims ASCII
// spaces before comparison to reproduce PAD SPACE `=`. Missing the MariaDB
// `nopad` family here silently mis-classified trailing-space row-moves on a
// MariaDB `utf8mb4_nopad_bin` `--where` column (audit 2026-07-19 SL-COLL-1 —
// same PAD-SPACE class as A1/A2). Ground-truthed in the real-server matrices.
func collationNoPad(name string) bool {
	lc := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lc, "_0900_") || strings.Contains(lc, "nopad") || lc == "binary"
}

// collationCharsetUTF8 reports whether the collation's charset is a UTF-8
// family (utf8mb4 / utf8mb3 / utf8 / ascii) — the only charsets for which
// sluice's UTF-8-encoded row-value bytes are decoded correctly by
// [evalengine.NullsafeCompare]. A non-UTF-8 charset (latin1, gbk, …) would have
// the comparator read the value under the wrong encoding, so such a column's
// string comparison is refused rather than transcoded (audit F0-6).
func collationCharsetUTF8(name string) bool {
	lc := strings.ToLower(strings.TrimSpace(name))
	return strings.HasPrefix(lc, "utf8mb4_") || strings.HasPrefix(lc, "utf8mb3_") ||
		strings.HasPrefix(lc, "utf8_") || strings.HasPrefix(lc, "ascii_")
}

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
// F0-7 (audit 2026-07-18) — the version pin's assumption, stated honestly. The
// comparator's case/accent WEIGHTS come from this pinned env, not from the
// source tablet. For the standard MySQL/PlanetScale/Vitess deployments this is
// faithful: the utf8mb4 collation weight tables are stable across the 8.0.x
// line, and 8.0.30's set is a superset of the 5.7 / MariaDB legacy names. The
// residual risk is an EXOTIC self-hosted-Vitess tablet running a MySQL/MariaDB
// build whose weights for a given collation DIFFER from 8.0.30. An operator on
// such a build who needs a byte-exact guarantee uses --where-strict-collation
// (which refuses the fold path) or normalizes on the source; the contract is
// documented in docs/operator/filtered-subset-migration.md.
func collationEnv() *collations.Environment {
	collationEnvOnce.Do(func() {
		collationEnvInst = collations.NewEnvironment(collationEnvVersion)
	})
	return collationEnvInst
}

// resolveCollation maps a MySQL collation NAME (e.g. "utf8mb4_0900_ai_ci") to a
// Vitess collation ID and reports whether it is usable for a faithful
// client-side comparison. An empty or unrecognized name yields (0, false) so
// the caller refuses loudly rather than compare under a guessed collation.
func resolveCollation(name string) (collations.ID, bool) {
	if name == "" {
		return 0, false
	}
	id := collationEnv().LookupByName(name)
	if id == collations.Unknown {
		return 0, false
	}
	return id, true
}

// collationEqual reports whether a == b under collation id, using Vitess's own
// comparator — the identical code path MySQL/Vitess evaluate `=` with, so the
// result cannot diverge from the source. id must be a resolved (non-Unknown)
// collation, guaranteed by the resolver's compile-time gate.
func collationEqual(a, b string, id collations.ID) bool {
	r, err := evalengine.NullsafeCompare(sqltypes.NewVarChar(a), sqltypes.NewVarChar(b), collationEnv(), id, nil)
	if err != nil {
		// Two VARCHARs under a resolved collation do not error in practice;
		// treat an unexpected error as not-equal (the safe, non-widening
		// direction) rather than risk a false in-scope classification.
		return false
	}
	return r == 0
}
