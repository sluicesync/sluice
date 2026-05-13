// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/orware/sluice/internal/ir"
)

// TestClassifyApplierError_NilInNilOut is the boring boundary case
// the pipeline relies on: classifier must pass nil through unchanged
// so wrapping every applier return site doesn't accidentally turn a
// success into a typed-error.
func TestClassifyApplierError_NilInNilOut(t *testing.T) {
	if got := classifyApplierError(nil); got != nil {
		t.Errorf("classifyApplierError(nil) = %v; want nil", got)
	}
}

// TestClassifyApplierError_NonRetriableUnchanged covers the
// default-deny invariant from ADR-0038. Errors that don't match a
// known transient shape return verbatim — the pipeline's retry loop
// treats those as terminal (errors.As against ir.RetriableError will
// fail).
func TestClassifyApplierError_NonRetriableUnchanged(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("some random failure")},
		{"wrapped error", fmt.Errorf("wrapping: %w", errors.New("inner"))},
		{"duplicate key (explicit non-retriable per ADR-0038)", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '1179' for key 'events.PRIMARY'"}},
		{"foreign key violation", &gomysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row"}},
		{"syntax error", &gomysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}},
		{"unknown column", &gomysql.MySQLError{Number: 1054, Message: "Unknown column 'foo' in 'field list'"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			// Identity comparison is deliberate here: the
			// classifier MUST return the input value unchanged
			// (not a semantically-equivalent wrapper) so the
			// pipeline's errors.As(... &RetriableError{}) gate
			// fails for non-retriable inputs. errors.Is would
			// be wrong — it'd pass even on a hypothetical
			// future "non-retriable wrapper" that still chained
			// the original.
			//nolint:errorlint // see comment above — identity not equivalence
			if got != c.err {
				t.Errorf("classifyApplierError should return non-retriable errors verbatim; got wrapped %T", got)
			}
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("non-retriable error matched ir.RetriableError via errors.As — this is the bug ADR-0038's default-deny is meant to prevent")
			}
		})
	}
}

// TestClassifyApplierError_RetriableShapes covers each documented
// transient shape from the ADR-0038 classifier table. Each must
// produce a value that (a) satisfies ir.RetriableError, (b) reports
// Retriable()==true, (c) preserves the original error via Unwrap.
func TestClassifyApplierError_RetriableShapes(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"InnoDB deadlock (Error 1213)", &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}},
		{"Vitess tx-killer Aborted (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Aborted desc = transaction 1234: in use: for tx killer rollback"}},
		{"Vitess Unavailable (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = tablet not serving"}},
		{"Vitess ResourceExhausted (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = ResourceExhausted desc = throttler engaged"}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"io.EOF", io.EOF},
		{"wrapped driver.ErrBadConn", fmt.Errorf("query: %w", driver.ErrBadConn)},
		{"connection reset by peer", errors.New("write tcp: connection reset by peer")},
		{"connection refused", errors.New("dial tcp: connection refused")},
		{"broken pipe", errors.New("write tcp: broken pipe")},
		{"i/o timeout", errors.New("read tcp: i/o timeout")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if !errors.As(got, &re) {
				t.Fatalf("classifyApplierError did not produce ir.RetriableError; got %T (%v)", got, got)
			}
			if !re.Retriable() {
				t.Errorf("classified error's Retriable() = false; want true")
			}
			if !errors.Is(got, c.err) {
				t.Errorf("Unwrap chain broken: errors.Is(classified, original) = false")
			}
		})
	}
}

// TestClassifyApplierError_VitessNonTransientCodesNotRetriable covers
// the discriminator inside the Error-1105 branch: only Aborted /
// Unavailable / ResourceExhausted are transients. Other gRPC codes
// (InvalidArgument, FailedPrecondition, NotFound) represent terminal
// semantic errors and must NOT be retried — retrying would mask real
// bugs.
func TestClassifyApplierError_VitessNonTransientCodesNotRetriable(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"InvalidArgument", "vttablet: rpc error: code = InvalidArgument desc = column 'foo' not in schema"},
		{"FailedPrecondition", "vttablet: rpc error: code = FailedPrecondition desc = primary readonly"},
		{"NotFound", "vttablet: rpc error: code = NotFound desc = keyspace 'unknown' not found"},
		{"PermissionDenied", "vttablet: rpc error: code = PermissionDenied desc = user lacks INSERT"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := &gomysql.MySQLError{Number: 1105, Message: c.msg}
			got := classifyApplierError(err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("Vitess non-transient %s wrongly classified as retriable; would mask real bugs", c.name)
			}
		})
	}
}

// TestClassifyApplierError_Error1105WithoutVttablet covers the bare
// "Error 1105" shape that some non-Vitess MySQL builds emit for
// HY000-generic errors. Only Vitess-tagged messages should be
// retriable — a generic HY000 without "vttablet" stays terminal.
func TestClassifyApplierError_Error1105WithoutVttablet(t *testing.T) {
	err := &gomysql.MySQLError{Number: 1105, Message: "Unknown error condition during apply"}
	got := classifyApplierError(err)
	var re ir.RetriableError
	if errors.As(got, &re) {
		t.Errorf("Error 1105 without vttablet message wrongly classified as retriable")
	}
}

// TestClassifyVitessMessage covers the leaf helper directly so the
// gRPC-code matching is testable without constructing a full
// MySQLError shell.
func TestClassifyVitessMessage(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"vttablet: rpc error: code = Aborted desc = ...", true},
		{"vttablet: rpc error: code = Unavailable desc = ...", true},
		{"vttablet: rpc error: code = ResourceExhausted desc = ...", true},
		{"vttablet: rpc error: code = InvalidArgument desc = ...", false},
		{"vttablet: rpc error: code = NotFound desc = ...", false},
		{"some other error", false},
		{"", false},
		{"code = Aborted desc = ... without the discriminator tag", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.msg, func(t *testing.T) {
			if got := classifyVitessMessage(c.msg); got != c.want {
				t.Errorf("classifyVitessMessage(%q) = %v; want %v", c.msg, got, c.want)
			}
		})
	}
}
