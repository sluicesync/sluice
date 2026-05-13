// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/orware/sluice/internal/ir"
)

// TestClassifyReaderError_DelegatesToApplierClassifier asserts the
// reader-side classifier matches the applier-side shapes 1:1. The
// v0.46.0 wiring relies on this identity — the streamer's
// retry loop (ADR-0038) classifies source errors and applier errors
// against the same [ir.RetriableError] interface, so the underlying
// transient table must agree. Divergence would be a silent regression
// where one surface retries on a shape the other treats as terminal.
//
// GitHub issue #19.
func TestClassifyReaderError_DelegatesToApplierClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil passes through", nil},
		{"plain non-retriable error", errors.New("schema mismatch")},
		{"InnoDB deadlock (1213)", &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}},
		{"Vitess tx-killer Aborted (1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Aborted desc = tx killer"}},
		{"Vitess Unavailable (1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = tablet not serving"}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"io.EOF", io.EOF},
		{"connection reset by peer", errors.New("read tcp: connection reset by peer")},
		{"duplicate key (explicit non-retriable)", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotReader := classifyReaderError(c.err)
			gotApplier := classifyApplierError(c.err)

			// Identity check: when the applier classifier returned the
			// input unchanged (non-retriable), reader must too.
			//
			//nolint:errorlint // identity comparison is the assertion
			if (gotReader == c.err) != (gotApplier == c.err) {
				t.Errorf("reader/applier classifier disagree on identity-preservation for %q", c.name)
			}

			// Both must agree on RetriableError satisfaction.
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
