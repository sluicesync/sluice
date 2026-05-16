// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestRequoteMySQLReservedIdents pins the validation-rig catalog #5
// fix: the read boundary strips backtick identifier quotes for IR
// portability, which de-quotes reserved-word column names (`order`,
// `key`) and breaks the MySQL target parser (Error 1064). The writer
// re-quotes any bare token that is a MySQL reserved word.
func TestRequoteMySQLReservedIdents(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"catalog #5 STORED shape — order reserved word",
			"if((discarded_at is null),order,NULL)",
			"if((discarded_at is null),`order`,NULL)",
		},
		{
			"catalog #5 VIRTUAL shape — key reserved word",
			"unhex(md5(key))",
			"unhex(md5(`key`))",
		},
		{
			"non-reserved identifiers pass through",
			"qty * price - discount",
			"qty * price - discount",
		},
		{
			"reserved word in call position is left alone (function name)",
			"left(name, 3)",
			"left(name, 3)",
		},
		{
			"already-backticked reserved word untouched",
			"`order` + 1",
			"`order` + 1",
		},
		{
			"reserved word inside a string literal is not requoted",
			"status = 'order placed'",
			"status = 'order placed'",
		},
		{
			"mixed: reserved + non-reserved + literal",
			"concat(`first`, ' ', last) and `key` is not null",
			// `first` already quoted; last non-reserved; key reserved →
			// requoted; the string literal ' ' untouched.
			"concat(`first`, ' ', last) and `key` is not null",
		},
		{
			"case-insensitive reserved match",
			"Order + Key",
			"`Order` + `Key`",
		},
		{
			"numeric literal not mistaken for identifier",
			"col + 100",
			"col + 100",
		},
		{"empty", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := requoteMySQLReservedIdents(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitColumnDef_GeneratedReservedWord is the end-to-end pin for
// catalog #5: a generated column whose body references a reserved-word
// column emits valid MySQL DDL (the reserved word stays backtick-
// quoted) for both STORED and VIRTUAL.
func TestEmitColumnDef_GeneratedReservedWord(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "STORED generated referencing reserved word `order`",
			in: &ir.Column{
				Name:                 "col_1",
				Type:                 ir.Decimal{Precision: 30, Scale: 10},
				Nullable:             true,
				GeneratedExpr:        "if((discarded_at is null),order,NULL)",
				GeneratedExprDialect: "mysql",
				GeneratedStored:      true,
			},
			want: "`col_1` DECIMAL(30,10) GENERATED ALWAYS AS (if((discarded_at is null),`order`,NULL)) STORED",
		},
		{
			name: "VIRTUAL generated referencing reserved word `key`",
			in: &ir.Column{
				Name:                 "key_hash",
				Type:                 ir.Binary{Length: 16},
				GeneratedExpr:        "unhex(md5(key))",
				GeneratedExprDialect: "mysql",
				GeneratedStored:      false,
			},
			want: "`key_hash` BINARY(16) GENERATED ALWAYS AS (unhex(md5(`key`))) VIRTUAL NOT NULL",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}
