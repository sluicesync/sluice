// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
	"sluicesync.dev/sluice/internal/ir"
)

// TestDomainTransparency_TranslateDispatchRoster extends the Bug 233 gate
// to `internal/translate` (roadmap item 155).
//
// The gate's package doc scoped it to "every engine that can be a TARGET
// for a Postgres-family source", and by that reading translate was
// outside — it is not an engine. But the class is about a `col.Type`
// dispatch missing an ir.Domain arm, and item 153 put ir.Domain dispatch
// in this package, so the scope was drawn around WHO OWNS THE CODE rather
// than around where the defect lives. Two real instances were sitting
// here when the gate arrived: a PG DOMAIN over `varchar(70000)` and one
// over bare `numeric` are both down-mapped by MySQL's emitter (its
// ir.Domain arm recurses into the base type) and neither scanner emitted
// its notice, so the operator got a silent TEXT/DECIMAL(65,30) rewrite.
//
// `TestRetargetRuleArmsAreAllProbed` does NOT cover this and is worth
// naming so it is not mistaken for coverage: it grades the RETARGET rule
// table's arms, which is one function in one file. It says nothing about
// the advisory scanners or the note/hint registries, which are where both
// instances were.
func TestDomainTransparency_TranslateDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:      ".",
		Engine:   "translate",
		MinSites: 8,
		Allowed:  translateDomainDispatchExemptions,
	})
}

// translateDomainDispatchExemptions is the fail-by-default roster. One
// reason, the SOURCE-SIDE one the MySQL and SQLite rosters also use:
//
//   - SOURCE-SIDE — the scanner returns nil unless the SOURCE engine is
//     MySQL-family, and MySQL has no CREATE DOMAIN, so no schema that
//     reaches the dispatch can carry an ir.Domain.
//
// That is a claim about MySQL rather than about this package, so
// TestMySQLFamilySourceCannotCarryADomain below is the premise pin
// (the premise-naming step) — it drives the reader-produced side rather
// than restating the argument.
//
// The note/hint registries in notes.go are NOT here: they unwrap
// instead. See the comment on noteEntries for why an exemption keyed on
// a whole `var` was the wrong instrument there.
var translateDomainDispatchExemptions = map[string]string{
	"mysql_time_range.go:ScanMySQLTimeRangeNotices:col.Type": "SOURCE-SIDE: returns nil unless " +
		"IsMySQLFamily(sourceEngine), and MySQL has no CREATE DOMAIN — the TIME columns scanned here " +
		"were read out of a MySQL catalog.",
	"unsigned_bigint.go:ScanUnsignedBigintNotices:col.Type": "SOURCE-SIDE: same guard — `bigint unsigned` " +
		"is a MySQL-source shape, so the schema walked here cannot carry a domain wrapper.",
	"target_emit_shape.go:postgresTargetEmitShape:c.Type": "MIRRORS-THE-EMITTER: internal/engines/postgres " +
		"dispatches the Bug-25 generated-enum rewrite on the DECLARED type at all three of its sites " +
		"(emitColumnDef's `c.Type.(ir.Enum)`, and emitTableDef's SET / generated-enum constraint loops), so " +
		"a DOMAIN-wrapped enum does NOT take that branch and lands as its domain type. Unwrapping here " +
		"would predict TEXT for a column the emitter creates as the domain — a prediction that disagrees " +
		"with the DDL is worse than none, because the comparison then reports drift on a target migrate " +
		"just created. If the emitter's declared-type dispatch is itself the Bug 233 class, that is a " +
		"defect to fix THERE and this mirror follows it; TestPostgresGeneratedEnumEmitDispatchIsDeclaredType " +
		"in internal/engines/postgres is the premise pin so the two cannot drift apart silently.",
}

// TestMySQLFamilySourceCannotCarryADomain is the premise the SOURCE-SIDE
// exemptions rest on. It is deliberately a check on the MySQL SCHEMA
// READER's own type vocabulary rather than a sentence about MySQL: the
// exemptions are false the moment some MySQL-family reader starts
// producing an ir.Domain (a future flavor synthesising one, a
// `--type-override` alias resolving to one), and this is what would
// fail.
//
// SCOPE, stated so it cannot be read as broader: there are TWO routes by
// which an ir.Domain could enter the schema these scanners walk, and this
// pin reaches ONE of them. The scanners run on the POST-override schema
// (their own docs say so), so `--type-override` is a live route and
// [targetTypeRegistry] is what this grades. The other route — a
// MySQL-family SCHEMA READER starting to synthesise an ir.Domain — cannot
// be pinned from here (engines/mysql imports this package, so the
// dependency only goes one way) and is asserted, not checked.
func TestMySQLFamilySourceCannotCarryADomain(t *testing.T) {
	if len(targetTypeRegistry) < 5 {
		t.Fatalf("targetTypeRegistry has %d entries; too few for this premise pin to be grading anything",
			len(targetTypeRegistry))
	}
	for alias, ty := range targetTypeRegistry {
		if _, isDomain := ty.(ir.Domain); isDomain {
			t.Errorf("--type-override alias %q resolves to an ir.Domain; the SOURCE-SIDE exemptions in "+
				"translateDomainDispatchExemptions assume no schema reaching those scanners can carry "+
				"one, and the scanners run POST-override", alias)
		}
	}
}
