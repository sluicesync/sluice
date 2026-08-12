// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// The SQLite engine answers the pre-create shape gate's "how do you compare
// table names?" question (audit 2026-08-11, PRF-2). The pin is here, next to
// the fold it returns, so a refactor that drops the method fails to compile
// rather than silently reverting the gate to a case-sensitive lookup.
var _ ir.TargetTableNameFolder = Engine{}

// SQLite compares object names case-insensitively, and this file is the one
// place that says what "case" means on this engine (roadmap item 150).
//
// # The rule, and it is narrower than Go's
//
// SQLite's identifier comparison folds ASCII ONLY. A stock build carries a
// 256-entry `sqlite3UpperToLower` table that maps `A`-`Z` to `a`-`z` and leaves
// every other byte — including every byte of every multi-byte UTF-8 sequence —
// alone. So `é` and `É` are two objects on a SQLite target, and `Café_Order`
// and `CAFÉ_ORDER` are two objects, because the `É`/`é` bytes never fold even
// though the ASCII letters around them do.
//
// [strings.ToLower] is Unicode-aware and therefore folds a strict SUPERSET.
// That superset is not a rounding error; three shapes are measured in
// [TestFoldSQLiteIdentifierMatchesRealSQLite]:
//
//	é      / É        -> ToLower folds; SQLite does not (the reported pair)
//	idx_k  / idx_<U+212A KELVIN SIGN>
//	                  -> ToLower folds the Kelvin sign to ASCII `k`; SQLite does not
//	i      / <U+0130 CAPITAL I WITH DOT ABOVE>
//	                  -> ToLower yields TWO runes, changing the identifier's LENGTH
//
// Using it as the collision key made sluice refuse pairs a real SQLite target
// keeps — a behaviour regression that shipped in v0.114.0 for the index walk
// and was caught by the v0.115.0 regression cycle against real SQLite. The
// direction was safe (a loud refusal naming a rename, never a silent loss), but
// the rename it demanded was advice the operator did not need.
//
// # Why bytes rather than runes
//
// The walk is over BYTES on purpose. SQLite does not validate that an
// identifier is well-formed UTF-8, so a name can carry bytes that are not; a
// rune-wise fold ([strings.Map] and friends) would replace each of those with
// U+FFFD and hand [namecollide] a key that two DIFFERENT invalid names share.
// Byte-wise, everything outside `A`-`Z` survives byte-exactly, which is exactly
// what SQLite's own comparison does.
//
// # Do NOT share this fold with another engine
//
// It is SQLite's rule, not a utility. MySQL's twin — [mysql.foldMySQLIdentifier]
// — imitates a SERVER-side fold that DOES fold non-ASCII case under
// `lower_case_table_names != 0` (`é`/`É` measured as one table on both `mysql:8`
// and `mariadb:11.4`, roadmap item 149), and Postgres compares sluice's quoted
// identifiers byte-exactly. Handing either of them this function would be an
// UNDER-refusal on MySQL — the silent direction. What is shared between the
// three is the [namecollide] seam; the rule is per engine, and
// TestNameCollideFoldIsDeclaredByItsOwnEngine is what keeps it that way.

// foldSQLiteIdentifier returns the key SQLite would compare name under: ASCII
// upper-case letters lowered, every other byte untouched.
//
// It allocates only when the name actually contains an ASCII upper-case letter,
// which is the uncommon case for a schema that never collides.
func foldSQLiteIdentifier(name string) string {
	var folded []byte
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c < 'A' || c > 'Z' {
			continue
		}
		if folded == nil {
			folded = []byte(name)
		}
		folded[i] = c + ('a' - 'A')
	}
	if folded == nil {
		return name
	}
	return string(folded)
}

// TargetTableNameFold implements [ir.TargetTableNameFolder]: SQLite folds
// identifier ASCII case UNCONDITIONALLY (it is an engine property, not a
// server setting), so the fold is always [foldSQLiteIdentifier] and dsn is
// irrelevant. The pre-create shape gate uses it to look a source table up in
// the target catalog under SQLite's own rule, so a source `Orders` meets a
// pre-existing `orders` instead of slipping past `CREATE TABLE IF NOT EXISTS`
// into it (audit 2026-08-11, PRF-2).
func (Engine) TargetTableNameFold(_ context.Context, _ string) (func(string) string, error) {
	return foldSQLiteIdentifier, nil
}
