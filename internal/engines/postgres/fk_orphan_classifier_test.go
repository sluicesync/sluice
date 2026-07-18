// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestAsFKOrphanViolation pins the ADR-0173 classifier: a wrapped 23503
// pgconn.PgError yields the child table + constraint; any other SQLSTATE
// (or non-pg error) reports no violation.
func TestAsFKOrphanViolation(t *testing.T) {
	w := &SchemaWriter{}

	t.Run("23503 with child + constraint", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: "23503", TableName: "orders", ConstraintName: "orders_user_id_fkey"}
		wrapped := fmt.Errorf("postgres: add foreign key %q on %q: %w", "orders_user_id_fkey", "orders", pgErr)
		v, ok := w.AsFKOrphanViolation(wrapped)
		if !ok {
			t.Fatal("want ok=true for a 23503 error")
		}
		if v.ChildTable != "orders" || v.ConstraintName != "orders_user_id_fkey" {
			t.Errorf("violation = %+v; want child=orders constraint=orders_user_id_fkey", v)
		}
	})

	t.Run("other SQLSTATE is not an orphan violation", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: "42P01"} // undefined_table
		if _, ok := w.AsFKOrphanViolation(fmt.Errorf("x: %w", pgErr)); ok {
			t.Error("want ok=false for a non-23503 error")
		}
	})

	t.Run("non-pg error is not an orphan violation", func(t *testing.T) {
		if _, ok := w.AsFKOrphanViolation(errors.New("plain error")); ok {
			t.Error("want ok=false for a non-pg error")
		}
	})
}
