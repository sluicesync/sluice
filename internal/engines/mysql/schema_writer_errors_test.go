// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// TestWrapDDLError_SafeMigrationsBlockedIsWrapped covers the GitHub
// issue #17 main fix path: when MySQL/Vitess returns
// `Error 1105 (HY000): direct DDL is disabled`, wrapDDLError must
// produce an [ErrSafeMigrationsBlocked]-wrapped error with the
// actionable operator hint, so the eventual "pipeline: create table
// X: ..." chain leads with the right diagnosis.
func TestWrapDDLError_SafeMigrationsBlockedIsWrapped(t *testing.T) {
	in := &gomysql.MySQLError{
		Number:  1105,
		Message: "direct DDL is disabled",
	}
	got := wrapDDLError(in)
	if !errors.Is(got, ErrSafeMigrationsBlocked) {
		t.Errorf("expected errors.Is(got, ErrSafeMigrationsBlocked) = true; got false. err=%v", got)
	}
	if !errors.Is(got, in) {
		t.Errorf("Unwrap chain broken; original MySQLError not reachable via errors.Is")
	}
	msg := got.Error()
	// Hint must mention both recovery paths (a) and (b).
	if !strings.Contains(msg, "--schema-already-applied") {
		t.Errorf("hint missing --schema-already-applied; got %q", msg)
	}
	if !strings.Contains(msg, "deploy request") {
		t.Errorf("hint missing 'deploy request' guidance; got %q", msg)
	}
	if !strings.Contains(msg, "Safe Migrations") {
		t.Errorf("hint missing 'Safe Migrations' name; got %q", msg)
	}
	if !strings.Contains(msg, "GitHub issue #17") {
		t.Errorf("hint missing GitHub #17 anchor; got %q", msg)
	}
}

// TestWrapDDLError_OtherErrorsUnchanged covers the default-pass
// invariant: errors that don't match a known operator-actionable
// shape return verbatim. The wrapper is cheap on the happy path
// (every non-1105 / non-recognised-1105 error returns unchanged).
func TestWrapDDLError_OtherErrorsUnchanged(t *testing.T) {
	cases := []struct {
		name string
		in   error
	}{
		{"nil in nil out", nil},
		{"plain error", errors.New("connection reset")},
		{"different MySQL code", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}},
		{"1105 without the magic text", &gomysql.MySQLError{Number: 1105, Message: "some other vitess error"}},
		{"1105 with retry-shape (not DDL refusal)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Aborted desc = transaction killed"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := wrapDDLError(c.in)
			if got == nil && c.in != nil {
				t.Errorf("wrapDDLError dropped a non-nil error")
			}
			if got != nil && c.in == nil {
				t.Errorf("wrapDDLError invented an error from nil")
			}
			// Non-matching errors must NOT carry the Safe Migrations
			// sentinel.
			if errors.Is(got, ErrSafeMigrationsBlocked) {
				t.Errorf("non-Safe-Migrations error wrongly flagged as such; got %v", got)
			}
		})
	}
}
