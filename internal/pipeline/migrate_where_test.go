// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fkOrphanWriter is a full ir.SchemaWriter (via noopSchemaWriter) that also
// implements ir.FKOrphanClassifier, so it can drive rowFilterFKOrphanRefusal.
type fkOrphanWriter struct {
	noopSchemaWriter
	violation ir.FKOrphanViolation
	ok        bool
}

func (w fkOrphanWriter) AsFKOrphanViolation(error) (ir.FKOrphanViolation, bool) {
	return w.violation, w.ok
}

// orphanSchema is a two-table schema whose child (orders) has an FK to the
// parent (users) — the shape a --where filter on users would orphan.
func orphanSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{Name: "users"},
		{Name: "orders", ForeignKeys: []*ir.ForeignKey{
			{Name: "orders_user_id_fkey", Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}},
		}},
	}}
}

func TestRowFilterFKOrphanRefusal(t *testing.T) {
	schema := orphanSchema()
	rawErr := errors.New("postgres: add foreign key: 23503")
	presented := errors.New("pipeline: create constraints: wrapped")

	t.Run("filtered parent + 23503 -> coded refusal naming parent", func(t *testing.T) {
		m := &Migrator{RowFilters: map[string]string{"users": "country = 'US'"}}
		w := fkOrphanWriter{violation: ir.FKOrphanViolation{ChildTable: "orders", ConstraintName: "orders_user_id_fkey"}, ok: true}
		got := m.rowFilterFKOrphanRefusal(w, schema, rawErr, presented)
		ce, ok := sluicecode.FromError(got)
		if !ok {
			t.Fatalf("want a coded error, got %v", got)
		}
		if ce.Code != sluicecode.CodeWhereFilterFKOrphan {
			t.Errorf("code = %q; want %q", ce.Code, sluicecode.CodeWhereFilterFKOrphan)
		}
		// The message must name the child, the constraint, and the parent.
		for _, want := range []string{"orders", "orders_user_id_fkey", "users"} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("message %q does not name %q", got.Error(), want)
			}
		}
		// The presented error stays in the chain.
		if !errors.Is(got, presented) {
			t.Error("presented error not preserved in chain")
		}
	})

	// assertPassthrough fails if got carries the coded refusal — a
	// passthrough must return the original presented error untouched.
	assertPassthrough := func(t *testing.T, got error) {
		t.Helper()
		if _, coded := sluicecode.FromError(got); coded {
			t.Errorf("want passthrough of presented, got a coded refusal: %v", got)
		}
		if !errors.Is(got, presented) {
			t.Errorf("presented error not preserved: %v", got)
		}
	}

	t.Run("no row filter -> passthrough", func(t *testing.T) {
		m := &Migrator{}
		w := fkOrphanWriter{violation: ir.FKOrphanViolation{ChildTable: "orders", ConstraintName: "orders_user_id_fkey"}, ok: true}
		assertPassthrough(t, m.rowFilterFKOrphanRefusal(w, schema, rawErr, presented))
	})

	t.Run("classifier reports no violation -> passthrough", func(t *testing.T) {
		m := &Migrator{RowFilters: map[string]string{"users": "x"}}
		w := fkOrphanWriter{ok: false}
		assertPassthrough(t, m.rowFilterFKOrphanRefusal(w, schema, rawErr, presented))
	})

	t.Run("writer without classifier (e.g. MySQL) -> passthrough", func(t *testing.T) {
		m := &Migrator{RowFilters: map[string]string{"users": "x"}}
		assertPassthrough(t, m.rowFilterFKOrphanRefusal(noopSchemaWriter{}, schema, rawErr, presented))
	})
}

func TestFKReferencedParent(t *testing.T) {
	schema := orphanSchema()
	if got := fkReferencedParent(schema, "orders", "orders_user_id_fkey"); got != "users" {
		t.Errorf("fkReferencedParent = %q; want users", got)
	}
	if got := fkReferencedParent(schema, "orders", "nope"); got != "" {
		t.Errorf("unknown constraint = %q; want empty", got)
	}
	if got := fkReferencedParent(nil, "orders", "x"); got != "" {
		t.Errorf("nil schema = %q; want empty", got)
	}
}

// filterRecorderReader implements ir.RowFilterSetter and records the map.
type filterRecorderReader struct {
	got map[string]string
}

func (r *filterRecorderReader) SetRowFilters(f map[string]string) { r.got = f }

// plainReader implements no optional surfaces (stands in for a source engine
// that can't push down --where, e.g. SQLite/D1).
type plainReader struct{}

func TestApplyRowFilters(t *testing.T) {
	filters := map[string]string{"users": "country = 'US'"}

	t.Run("no filters is a no-op even on an unsupported reader", func(t *testing.T) {
		if err := migcore.ApplyRowFilters(plainReader{}, nil, "sqlite"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("supported reader records the map", func(t *testing.T) {
		r := &filterRecorderReader{}
		if err := migcore.ApplyRowFilters(r, filters, "postgres"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.got["users"] != "country = 'US'" {
			t.Errorf("filter not recorded: %v", r.got)
		}
	})

	t.Run("unsupported reader with filters refuses loudly naming the engine", func(t *testing.T) {
		err := migcore.ApplyRowFilters(plainReader{}, filters, "sqlite")
		if err == nil {
			t.Fatal("want a loud refusal, got nil")
		}
		if !strings.Contains(err.Error(), "sqlite") {
			t.Errorf("refusal %q does not name the engine", err.Error())
		}
	})
}

func TestRawCopyGate_RowFilterForcesIRPath(t *testing.T) {
	// Same-engine, no other transform, but a --where filter present: the
	// raw byte-pipe would bypass the predicate, so the gate must refuse it.
	ok, reason := rawCopyGate(rawCopyConfig{
		sourceEngine: "postgres",
		targetEngine: "postgres",
		rowFilters:   map[string]string{"users": "country = 'US'"},
	})
	if ok {
		t.Fatalf("rawCopyGate ok = true with a row filter; want false")
	}
	if !strings.Contains(reason, "where") && !strings.Contains(reason, "filter") {
		t.Errorf("reason %q does not mention the row filter", reason)
	}
}
