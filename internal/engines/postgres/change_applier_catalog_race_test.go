// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Task #29 / ADR-0054 Phase 2e — retryOnCatalogRace handles the narrow
// SQLSTATE 23505 race on pg_type_typname_nsp_index /
// pg_class_relname_nsp_index that fires when N concurrent shard streams
// call EnsureControlTable against a fresh target tightly.

func TestIsCatalogRaceError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "non-pg error",
			err:  errors.New("not a pg error"),
			want: false,
		},
		{
			name: "23505 on pg_type — race shape",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"},
			want: true,
		},
		{
			name: "23505 on pg_class — race shape",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "pg_class_relname_nsp_index"},
			want: true,
		},
		{
			name: "23505 on user table — explicitly non-retriable per ADR-0038",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "users_pkey"},
			want: false,
		},
		{
			name: "23505 with no constraint name (defensive)",
			err:  &pgconn.PgError{Code: "23505"},
			want: false,
		},
		{
			name: "23503 foreign-key violation",
			err:  &pgconn.PgError{Code: "23503", ConstraintName: "pg_type_typname_nsp_index"},
			want: false,
		},
		{
			name: "wrapped pg error",
			err:  errors.Join(errors.New("outer wrap"), &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isCatalogRaceError(tc.err); got != tc.want {
				t.Errorf("isCatalogRaceError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryOnCatalogRace_SucceedsImmediately(t *testing.T) {
	t.Parallel()
	calls := 0
	err := retryOnCatalogRace(context.Background(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("expected nil error; got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call; got %d", calls)
	}
}

func TestRetryOnCatalogRace_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	raceErr := &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}
	calls := 0
	err := retryOnCatalogRace(context.Background(), func() error {
		calls++
		if calls < 3 {
			return raceErr
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected nil error after retry; got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (2 retries); got %d", calls)
	}
}

func TestRetryOnCatalogRace_ExhaustedRetries(t *testing.T) {
	t.Parallel()
	raceErr := &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}
	calls := 0
	err := retryOnCatalogRace(context.Background(), func() error {
		calls++
		return raceErr
	})
	if !errors.Is(err, raceErr) {
		t.Errorf("expected raceErr after exhausted retries; got %v", err)
	}
	// 1 initial attempt + 3 retries (one per delay in the table) = 4 calls.
	if calls != 4 {
		t.Errorf("expected 4 calls (1 + 3 retries); got %d", calls)
	}
}

func TestRetryOnCatalogRace_NonRaceErrorReturnsImmediately(t *testing.T) {
	t.Parallel()
	otherErr := &pgconn.PgError{Code: "23505", ConstraintName: "users_pkey"}
	calls := 0
	err := retryOnCatalogRace(context.Background(), func() error {
		calls++
		return otherErr
	})
	if !errors.Is(err, otherErr) {
		t.Errorf("expected the other 23505 to surface immediately; got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry on non-race 23505); got %d", calls)
	}
}

func TestRetryOnCatalogRace_ContextCancelStops(t *testing.T) {
	t.Parallel()
	raceErr := &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel; first retry should observe the cancellation
	calls := 0
	err := retryOnCatalogRace(ctx, func() error {
		calls++
		return raceErr
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled after cancel; got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call before observing cancel; got %d", calls)
	}
}
