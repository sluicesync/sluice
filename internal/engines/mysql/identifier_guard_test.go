// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCharsetCollationRefuseInjection is the MySQL half of the ADR-0183
// Tier-1 bare-identifier guard (the Postgres positions are pinned in that
// engine's identifier_guard_test.go).
//
// `CHARACTER SET x COLLATE y` interpolates both values bare, and sluice
// runs DDL with no bind parameters, so a `;` inside either is a second
// statement rather than a bad name. Both halves are pinned, on BOTH
// type families that reach appendCharsetCollation — CHAR/VARCHAR through
// emitCharType and the four TEXT tiers through emitTextType — because
// they are separate call sites, and the wide-VARCHAR down-map routes a
// VARCHAR through the TEXT one.
func TestCharsetCollationRefuseInjection(t *testing.T) {
	const hostileCharset = `utf8mb4; CREATE USER attacker; --`
	const hostileCollation = `utf8mb4_general_ci; CREATE USER attacker; --`

	m := mysqlEmitter{}
	cases := map[string]ir.Type{
		"char charset":       ir.Char{Length: 10, Charset: hostileCharset},
		"char collation":     ir.Char{Length: 10, Charset: "utf8mb4", Collation: hostileCollation},
		"varchar charset":    ir.Varchar{Length: 10, Charset: hostileCharset},
		"varchar collation":  ir.Varchar{Length: 10, Charset: "utf8mb4", Collation: hostileCollation},
		"text charset":       ir.Text{Size: ir.TextRegular, Charset: hostileCharset},
		"text collation":     ir.Text{Size: ir.TextRegular, Charset: "utf8mb4", Collation: hostileCollation},
		"tinytext charset":   ir.Text{Size: ir.TextTiny, Charset: hostileCharset},
		"mediumtext charset": ir.Text{Size: ir.TextMedium, Charset: hostileCharset},
		"longtext charset":   ir.Text{Size: ir.TextLong, Charset: hostileCharset},
		// The Bug 72 wide-VARCHAR down-map: a VARCHAR that reaches the
		// TEXT emitter instead. Same guard, different route to it.
		"wide varchar charset": ir.Varchar{Length: 20000, Charset: hostileCharset},
	}
	for name, typ := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := m.emitColumnType(typ)
			if err == nil {
				t.Fatalf("%s: hostile value emitted instead of refused: %q", name, out)
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaIdentifierInvalid {
				t.Errorf("%s: refused with %v, want code %q", name, err, sluicecode.CodeSchemaIdentifierInvalid)
			}
			if out != "" {
				t.Errorf("%s: refusal returned a non-empty type expression %q", name, out)
			}
		})
	}
}

// TestCharsetCollationAcceptRealValues is the regression half: every
// charset/collation a real MySQL catalog produces must emit unchanged.
func TestCharsetCollationAcceptRealValues(t *testing.T) {
	m := mysqlEmitter{}
	pairs := []struct{ charset, collation string }{
		{"utf8mb4", "utf8mb4_0900_ai_ci"},
		{"utf8mb4", "utf8mb4_general_ci"},
		{"utf8mb4", "utf8mb4_bin"},
		{"utf8mb4", "utf8mb4_zh_0900_as_cs"},
		{"utf8mb3", "utf8mb3_general_ci"},
		{"latin1", "latin1_swedish_ci"},
		{"binary", "binary"},
		{"armscii8", "armscii8_general_ci"},
		{"utf8mb4", ""},
		{"", ""},
	}
	for _, p := range pairs {
		got, err := m.emitColumnType(ir.Varchar{Length: 10, Charset: p.charset, Collation: p.collation})
		if err != nil {
			t.Errorf("charset %q collation %q refused: %v", p.charset, p.collation, err)
			continue
		}
		if p.charset != "" && !strings.Contains(got, "CHARACTER SET "+p.charset) {
			t.Errorf("charset %q not emitted: %q", p.charset, got)
		}
	}
}
