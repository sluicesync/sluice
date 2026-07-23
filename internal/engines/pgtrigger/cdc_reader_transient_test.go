// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyPollError pins both classification legs of the change-log
// poll (the v0.99.286 tracked follow-up, now closed): PG SQLSTATE
// connection-availability transients and the shared transport shapes are
// retriable; a missing change-log table and unknown shapes stay terminal.
func TestClassifyPollError(t *testing.T) {
	retriable := func(err error) bool {
		var re ir.RetriableError
		return errors.As(err, &re) && re.Retriable()
	}
	pg := func(code string) error {
		return fmt.Errorf("query change_log: %w", &pgconn.PgError{Code: code, Message: "x"})
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"57P01 admin_shutdown (server restart)", pg("57P01"), true},
		{"57P03 cannot_connect_now (standby promoting)", pg("57P03"), true},
		{"08006 connection_failure", pg("08006"), true},
		{"transport: TLS handshake timeout", errors.New("net/http: TLS handshake timeout"), true},
		{"transport: connection reset", errors.New("read tcp 1.2.3.4:1->5.6.7.8:2: connection reset by peer"), true},
		{"42P01 missing change-log table = TERMINAL", pg("42P01"), false},
		{"28P01 bad password = TERMINAL", pg("28P01"), false},
		{"decode fault = TERMINAL", errors.New("pgtrigger: decode change payload: unexpected token"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifyPollError(c.err)
			if retriable(got) != c.want {
				t.Errorf("classifyPollError(%v) retriable = %v; want %v", c.err, !c.want, c.want)
			}
			// The wrapping must preserve the chain either way.
			if !errors.Is(got, c.err) && !errors.As(got, new(*pgconn.PgError)) && c.want {
				t.Errorf("classifyPollError lost the underlying error chain: %v", got)
			}
		})
	}
}
