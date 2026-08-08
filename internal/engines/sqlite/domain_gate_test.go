// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
	"sluicesync.dev/sluice/internal/ir"
)

// TestDomainTransparency_SQLiteDispatchRoster is this engine's
// instantiation of the Bug 233 gate. Every exemption here rests on ONE
// argument — SQLite refuses an ir.Domain column LOUDLY before any value
// moves — and that argument is not taken on trust: the premise pin
// below drives both of its halves.
//
// The gate is instantiated for this engine even though no site needed a
// fix, which is the point: the MySQL half of this class shipped for
// three releases precisely because nobody asked the same question of
// the other targets, and an engine with no gate file is a gap that
// reads like an absence of hazard.
func TestDomainTransparency_SQLiteDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:      ".",
		Engine:   "sqlite",
		MinSites: 5,
		Allowed:  sqliteDomainDispatchExemptions,
	})
}

// sqliteDomainDispatchExemptions is the fail-by-default roster. Two
// reasons, as in the MySQL sibling — SOURCE-SIDE, and here a second
// one this engine can make and MySQL cannot:
//
//   - REFUSED-UPSTREAM — SQLite's emitColumnType has no ir.Domain arm
//     and its default arm refuses, so a PG DOMAIN column never reaches
//     a value on this target at all. That is a claim about another
//     function, so TestDomainOnSQLiteTargetIsRefusedNotDispatched below
//     drives it rather than asserting it.
var sqliteDomainDispatchExemptions = map[string]string{
	"d1_rows.go:pkKeysetSafe:col.Type": "SOURCE-SIDE: the keyset-safety verdict is about the SQLite/D1 " +
		"source's own PK columns, and SQLite has no DOMAIN.",
	"row_reader.go:selectColumnExpr:c.Type": "SOURCE-SIDE: the SELECT list is built against the SQLite " +
		"source's own columns.",
	"row_reader_range.go:pkCursorDisqualified:c.Type": "SOURCE-SIDE: same SQLite-source PK columns.",
	"schema_reader.go:markRowidAutoIncrement:c.Type": "SOURCE-SIDE: this IS the SQLite catalog read that " +
		"produces the IR.",
	"ddl_emit.go:soleIntegerPKColumn:c.Type": "REFUSED-UPSTREAM: a DOMAIN-typed column fails emitColumnType " +
		"in the same CREATE TABLE emit, so a wrong answer here cannot survive the statement it feeds.",
	"value_encode.go:encodeValue:col.Type": "REFUSED-UPSTREAM: unreachable for a DOMAIN (the schema write " +
		"refused first), and its own default arm is loud rather than silent if it ever were.",
}

// TestDomainOnSQLiteTargetIsRefusedNotDispatched is the premise the
// REFUSED-UPSTREAM exemptions above rest on (the premise-naming step).
//
// It drives both halves: the DDL emitter refuses a DOMAIN-typed column,
// and — the half that matters if the CREATE is ever skipped, as it is
// for a pre-existing target table — the value encoder refuses too
// rather than encoding the wrapper as some default shape.
func TestDomainOnSQLiteTargetIsRefusedNotDispatched(t *testing.T) {
	dom := ir.Domain{Name: "email_address", BaseType: ir.Text{}}

	if got, err := emitColumnType(dom); err == nil {
		t.Errorf("emitColumnType(DOMAIN) = %q with no error; the SQLite exemptions in this file all rest "+
			"on this refusal, so a silent carry here makes every one of them false", got)
	}

	col := &ir.Column{Name: "addr", Type: dom}
	got, err := encodeValue(col, "someone@example.com")
	if err == nil {
		t.Errorf("encodeValue(DOMAIN column) = %#v with no error; a value reaching the encoder with the "+
			"wrapper intact must be refused loudly, not encoded as whatever the default arm decides", got)
		return
	}
	if !strings.Contains(err.Error(), "addr") {
		t.Errorf("the refusal does not name the column: %v", err)
	}
}
