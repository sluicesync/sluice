// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// item-51: a schema carrying a standalone PG sequence must refuse
// loudly on a SQLite target, naming the sequence and the remedy —
// SQLite has no sequence objects, and silently dropping one changes
// what post-migration nextval-driven inserts would produce. The
// refusal fires before any DDL touches the target (same posture as
// the EXCLUDE/RLS refusals).
func TestCreateTables_StandaloneSequenceRefuses(t *testing.T) {
	s := &ir.Schema{
		Tables: []*ir.Table{{
			Name: "orders",
			Columns: []*ir.Column{
				{
					Name: "order_number", Type: ir.Integer{Width: 64},
					Default: ir.DefaultExpression{Expr: "nextval('order_number_seq'::regclass)", Dialect: "postgres"},
				},
			},
		}},
		Sequences: []*ir.Sequence{{Schema: "public", Name: "order_number_seq", Start: 1000, Increment: 5}},
	}
	// The refusal precedes any database work, so a zero writer is
	// sufficient — reaching the db would panic and fail the test.
	err := (&SchemaWriter{}).CreateTablesWithoutConstraints(context.Background(), s)
	if err == nil {
		t.Fatal("want standalone-sequence refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, `"order_number_seq"`) {
		t.Errorf("error %q does not name the sequence", msg)
	}
	if !strings.Contains(msg, "Recovery:") {
		t.Errorf("error %q missing the remedy", msg)
	}
}
