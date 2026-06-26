// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsDuplicatePublication pins the benign "publication already exists"
// detector used to make publication creation idempotent for shared-source
// fleets (ADR-0122): several PG-source syncs that share one source race to
// CREATE the same publication at cold-start, and a check-then-create has a
// TOCTOU window where the loser hits a unique-violation on
// pg_publication's pubname index (SQLSTATE 23505) — benign, the
// publication now exists. 42710 duplicate_object is the single-session
// "already exists" shape. Everything else (and a non-pg error) is NOT a
// duplicate and must surface loudly.
func TestIsDuplicatePublication(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"23505", true},  // unique_violation — the concurrent-create race shape
		{"42710", true},  // duplicate_object — single-session already-exists
		{"42P01", false}, // undefined_table
		{"08P01", false}, // protocol_violation
		{"53100", false}, // disk_full
		{"", false},
	}
	for _, c := range cases {
		err := error(&pgconn.PgError{Code: c.code})
		if got := isDuplicatePublication(err); got != c.want {
			t.Errorf("isDuplicatePublication(code %q) = %v; want %v", c.code, got, c.want)
		}
	}

	// Wrapped pg errors are still detected (errors.As unwraps).
	wrapped := fmt.Errorf("postgres: create publication %q: %w", "sluice_pub", &pgconn.PgError{Code: "23505"})
	if !isDuplicatePublication(wrapped) {
		t.Error("wrapped 23505 should be detected as a duplicate")
	}

	// A plain (non-pg) error is never a duplicate.
	if isDuplicatePublication(errors.New("connection refused")) {
		t.Error("plain error should not be classified as a duplicate publication")
	}
	if isDuplicatePublication(nil) {
		t.Error("nil error should not be classified as a duplicate publication")
	}
}
