// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// TestPreflightRedactTypes covers Bug 60's preflight refusal +
// every short-circuit path.
func TestPreflightRedactTypes(t *testing.T) {
	uuidCol := &ir.Column{Name: "id", Type: ir.UUID{}}
	textCol := &ir.Column{Name: "id", Type: ir.Text{}}
	emailCol := &ir.Column{Name: "email", Type: ir.Text{}}

	schemaWith := func(table string, cols ...*ir.Column) *ir.Schema {
		return &ir.Schema{
			Tables: []*ir.Table{{Name: table, Columns: cols}},
		}
	}

	t.Run("nil registry is no-op", func(t *testing.T) {
		if err := preflightRedactTypes(nil, schemaWith("users", uuidCol)); err != nil {
			t.Errorf("nil registry: got %v; want nil", err)
		}
	})
	t.Run("empty registry is no-op", func(t *testing.T) {
		if err := preflightRedactTypes(redact.New(), schemaWith("users", uuidCol)); err != nil {
			t.Errorf("empty registry: got %v; want nil", err)
		}
	})
	t.Run("nil schema is no-op", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		if err := preflightRedactTypes(r, nil); err != nil {
			t.Errorf("nil schema: got %v; want nil", err)
		}
	})

	t.Run("mask:uuid on UUID column refuses", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		err := preflightRedactTypes(r, schemaWith("users", uuidCol))
		if err == nil {
			t.Fatal("expected refusal; got nil")
		}
		if !errors.Is(err, errRedactTypeMismatch) {
			t.Errorf("error should wrap errRedactTypeMismatch; got %v", err)
		}
		msg := err.Error()
		for _, want := range []string{"users.id", "mask:uuid", "--type-override=users.id=text"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q should contain %q", msg, want)
			}
		}
	})

	t.Run("mask:uuid on text column passes (--type-override applied)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		if err := preflightRedactTypes(r, schemaWith("users", textCol)); err != nil {
			t.Errorf("got %v; want nil (type override should let mask:uuid pass)", err)
		}
	})

	t.Run("mask:uuid on missing column is silent", func(t *testing.T) {
		// Column filter pruned the table; rule registered but the
		// column isn't in scope. Don't refuse; the operator may have
		// intentionally narrowed the migration.
		r := redact.New()
		r.Set("", "other_table", "id", redact.MaskUUID{})
		if err := preflightRedactTypes(r, schemaWith("users", uuidCol)); err != nil {
			t.Errorf("got %v; want nil (column not in scope)", err)
		}
	})

	t.Run("non-mask:uuid strategy on UUID column passes", func(t *testing.T) {
		// hash:sha256 produces 64-char hex; lands as text in the operator's
		// target column choice. Out of scope for this preflight.
		r := redact.New()
		r.Set("", "users", "id", redact.Hash{Algo: "sha256"})
		if err := preflightRedactTypes(r, schemaWith("users", uuidCol)); err != nil {
			t.Errorf("got %v; want nil (hash:sha256 is not mask:uuid)", err)
		}
	})

	t.Run("mask:uuid on text + mask:email both pass", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		r.Set("", "users", "email", redact.MaskEmail{})
		schema := schemaWith("users", textCol, emailCol)
		if err := preflightRedactTypes(r, schema); err != nil {
			t.Errorf("got %v; want nil", err)
		}
	})

	t.Run("multiple offending rules are reported together", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		r.Set("", "orders", "uuid", redact.MaskUUID{})
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.UUID{}}}},
				{Name: "orders", Columns: []*ir.Column{{Name: "uuid", Type: ir.UUID{}}}},
			},
		}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("expected refusal; got nil")
		}
		msg := err.Error()
		for _, want := range []string{"users.id", "orders.uuid"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q should list %q (single-shot report)", msg, want)
			}
		}
	})
}
