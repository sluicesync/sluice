// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyReaderError_DelegatesToApplierClassifier asserts the
// PG reader-side classifier matches the applier-side shapes 1:1. The
// v0.46.0 retry surfaces (source-pump errors → ADR-0038 retry loop)
// rely on the same [ir.RetriableError] gate the applier uses; the
// two classifiers must agree on every shape so neither surface gets
// stricter or laxer over time.
//
// GitHub issue #19.
func TestClassifyReaderError_DelegatesToApplierClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil passes through", nil},
		{"plain non-retriable error", errors.New("publication missing")},
		{"serialization_failure (40001)", &pgconn.PgError{Code: "40001", Message: "could not serialize access"}},
		{"deadlock_detected (40P01)", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
		{"admin_shutdown (57P01)", &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}},
		{"connection_failure (08006)", &pgconn.PgError{Code: "08006", Message: "connection failure"}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"io.EOF", io.EOF},
		{"db starting up", errors.New("the database system is starting up")},
		{"unique_violation (23505, non-retriable)", &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotReader := classifyReaderError(c.err)
			gotApplier := classifyApplierError(c.err)

			//nolint:errorlint // identity comparison is the assertion
			if (gotReader == c.err) != (gotApplier == c.err) {
				t.Errorf("reader/applier classifier disagree on identity-preservation for %q", c.name)
			}

			var reReader, reApplier ir.RetriableError
			retriableR := errors.As(gotReader, &reReader)
			retriableA := errors.As(gotApplier, &reApplier)
			if retriableR != retriableA {
				t.Errorf("reader/applier classifier disagree on retriable shape for %q: reader=%v applier=%v",
					c.name, retriableR, retriableA)
			}
		})
	}
}
