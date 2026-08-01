// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// mustRLS / mustPolicy are the RLS emit helpers with the ADR-0183
// identifier refusal asserted away — every fixture in rls_emit_test.go
// carries a legitimate policy command, so a refusal there is a bug in the
// guard, not the case under test. The refusal itself is pinned below.
func mustRLS(t *testing.T, schema string, table *ir.Table) []string {
	t.Helper()
	out, err := emitRLSStatements(schema, table)
	if err != nil {
		t.Fatalf("emitRLSStatements: %v", err)
	}
	return out
}

func mustPolicy(t *testing.T, qualifiedTable string, p *ir.Policy) string {
	t.Helper()
	out, err := emitCreatePolicy(qualifiedTable, p)
	if err != nil {
		t.Fatalf("emitCreatePolicy: %v", err)
	}
	return out
}

// hostile is the audit-2026-08-01 probe value: a plausible prefix, a
// statement separator, and a comment introducer to swallow the rest of
// the emitted DDL. Every Postgres statement sluice runs goes through
// ExecContext with zero arguments, which pgx v5 forces onto the simple
// protocol — where this is two statements, not one bad identifier.
const hostile = `btree; CREATE ROLE attacker SUPERUSER; --`

// TestBareIdentifierPositionsRefuseInjection walks EVERY Postgres Tier-1
// bare-identifier position (ADR-0183) and asserts each refuses a hostile
// value with the coded refusal rather than emitting it.
//
// Pin the class, not the representative: these five positions share no
// code path — a sequence type, a policy keyword, an access method and an
// operator class are validated by four different call sites in three
// files — so one green case says nothing about the others. The MySQL
// charset/collation pair is pinned in the mysql package's sibling test.
func TestBareIdentifierPositionsRefuseInjection(t *testing.T) {
	cases := map[string]func() (string, error){
		"sequence data type": func() (string, error) {
			return emitCreateSequence("public", &ir.Sequence{
				Name: "s", Increment: 1, DataType: `bigint; CREATE ROLE attacker SUPERUSER; --`,
			})
		},
		"policy command": func() (string, error) {
			return emitCreatePolicy(`"public"."t"`, &ir.Policy{
				Name: "p", Command: `ALL; CREATE ROLE attacker SUPERUSER; --`, Permissive: true, Using: "true",
			})
		},
		"index access method": func() (string, error) {
			return emitCreateIndex("public", "t", &ir.Index{
				Name: "i", Method: hostile, Columns: []ir.IndexColumn{{Column: "c"}},
			}, emitOpts{})
		},
		"index operator class": func() (string, error) {
			return emitCreateIndex("public", "t", &ir.Index{
				Name: "i", Columns: []ir.IndexColumn{{Column: "c", OperatorClass: `text_ops; CREATE ROLE attacker SUPERUSER; --`}},
			}, emitOpts{})
		},
		// The opclass also rides the CREATE TABLE path (inline PRIMARY KEY
		// and the Bug 125 inline unique key), which is a DIFFERENT call
		// site of emitIndexColumnList — the sibling a per-representative
		// pin would have missed.
		"index operator class (inline primary key)": func() (string, error) {
			return emitTableDef("public", &ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "c", Type: ir.Text{}}},
				PrimaryKey: &ir.Index{
					Name: "t_pkey", Unique: true,
					Columns: []ir.IndexColumn{{Column: "c", OperatorClass: `text_ops; CREATE ROLE attacker SUPERUSER; --`}},
				},
			}, emitOpts{})
		},
	}
	for name, emit := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := emit()
			if err == nil {
				t.Fatalf("%s: hostile value emitted instead of refused: %q", name, out)
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaIdentifierInvalid {
				t.Errorf("%s: refused with %v, want code %q", name, err, sluicecode.CodeSchemaIdentifierInvalid)
			}
			// The refusal must NAME the object; an operator holding a
			// 400-table schema cannot act on "an index".
			if !strings.Contains(err.Error(), "postgres:") {
				t.Errorf("%s: refusal does not name the object: %v", name, err)
			}
			if out != "" {
				t.Errorf("%s: refusal returned a non-empty statement %q", name, out)
			}
		})
	}
}

// TestBareIdentifierPositionsAcceptRealValues is the other half, and the
// one that keeps the guard from being a regression: every value the PG
// reader can actually produce at these positions must still emit
// UNCHANGED. An access-method allowlist would have failed here the day
// pgvector shipped a new index type.
func TestBareIdentifierPositionsAcceptRealValues(t *testing.T) {
	for _, method := range []string{"btree", "hash", "gin", "gist", "spgist", "brin", "ivfflat", "hnsw", "bm25"} {
		stmt, err := emitCreateIndex("public", "t", &ir.Index{
			Name: "i", Method: method, Columns: []ir.IndexColumn{{Column: "c"}},
		}, emitOpts{})
		if err != nil {
			t.Errorf("access method %q refused: %v", method, err)
			continue
		}
		if !strings.Contains(stmt, "USING "+method+" ") {
			t.Errorf("access method %q not emitted: %q", method, stmt)
		}
	}
	for _, opclass := range []string{
		"gin_trgm_ops", "gist_trgm_ops", "text_pattern_ops", "varchar_pattern_ops",
		"jsonb_path_ops", "vector_l2_ops", "gist__int_ops", "_int4_ops",
	} {
		stmt, err := emitCreateIndex("public", "t", &ir.Index{
			Name: "i", Columns: []ir.IndexColumn{{Column: "c", OperatorClass: opclass}},
		}, emitOpts{})
		if err != nil {
			t.Errorf("opclass %q refused: %v", opclass, err)
			continue
		}
		if !strings.Contains(stmt, `"c" `+opclass) {
			t.Errorf("opclass %q not emitted: %q", opclass, stmt)
		}
	}
	// The three sequence types PG accepts after AS, and the empty default.
	for _, dt := range []string{"", "smallint", "integer", "bigint"} {
		if _, err := emitCreateSequence("public", &ir.Sequence{Name: "s", Increment: 1, DataType: dt}); err != nil {
			t.Errorf("sequence data type %q refused: %v", dt, err)
		}
	}
	// Every policy command, in both the catalog's upper case and the
	// lower case a hand-built IR may carry (the emitter upper-cases first,
	// so both must pass — a guard placed before the fold would have broken
	// the lowercase half).
	for _, cmd := range []string{"", "ALL", "SELECT", "INSERT", "UPDATE", "DELETE", "select", "update"} {
		if _, err := emitCreatePolicy(`"public"."t"`, &ir.Policy{Name: "p", Command: cmd, Permissive: true, Using: "true"}); err != nil {
			t.Errorf("policy command %q refused: %v", cmd, err)
		}
	}
}

// TestSequenceDataTypeRefusesUnknownType pins that the sequence position
// is a CLOSED SET, not a shape test: `text` is a perfectly bare
// identifier and still has no business after `AS`.
func TestSequenceDataTypeRefusesUnknownType(t *testing.T) {
	_, err := emitCreateSequence("public", &ir.Sequence{Name: "s", Increment: 1, DataType: "text"})
	if err == nil {
		t.Fatal("sequence data type \"text\" accepted; the position is a closed set of {smallint, integer, bigint}")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeSchemaIdentifierInvalid {
		t.Errorf("refused with %v, want code %q", err, sluicecode.CodeSchemaIdentifierInvalid)
	}
}
