// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
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

// assertSyncFlagScoped fails when s names `--schema-already-applied`
// without the owning command in front of it — the item-71c bug shape:
// the shared create-table wrap site runs under BOTH migrate and sync,
// and an unscoped mention sent migrate operators chasing a flag
// migrate does not have.
func assertSyncFlagScoped(t *testing.T, s string) {
	t.Helper()
	const flag = "--schema-already-applied"
	for rest, i := s, 0; ; {
		i = strings.Index(rest, flag)
		if i < 0 {
			return
		}
		if !strings.HasSuffix(rest[:i], "sluice sync start ") {
			t.Errorf("%q names %s without scoping it to `sluice sync start`", s, flag)
			return
		}
		rest = rest[i+len(flag):]
	}
}

// TestWrapUserTableCreateError_1105CodedRefusal pins the item-71c fix
// shape at the user-table CREATE wrap site: the safe-migrations 1105
// becomes the coded SLUICE-E-PS-DIRECT-DDL-BLOCKED refusal (exit 3),
// names the refused table and both recovery paths, keeps the
// [ErrSafeMigrationsBlocked] sentinel and the driver error in the
// chain, and never attributes the sync-only flag to migrate.
func TestWrapUserTableCreateError_1105CodedRefusal(t *testing.T) {
	in := &gomysql.MySQLError{Number: 1105, Message: "direct DDL is disabled"}
	got := wrapUserTableCreateError(in, "orders")

	coded, ok := sluicecode.FromError(got)
	if !ok {
		t.Fatalf("want *sluicecode.CodedError; got %T: %v", got, got)
	}
	if coded.Code != sluicecode.CodePSDirectDDLBlocked {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodePSDirectDDLBlocked)
	}
	if !errors.Is(got, ErrSafeMigrationsBlocked) {
		t.Errorf("errors.Is(got, ErrSafeMigrationsBlocked) = false; err = %v", got)
	}
	if !errors.Is(got, in) {
		t.Error("Unwrap chain broken; original MySQLError not reachable via errors.Is")
	}
	msg := got.Error()
	for _, want := range []string{`"orders"`, "safe migrations on the branch", "sluice deploy-ddl", "sluice schema preview"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal %q should contain %q", msg, want)
		}
	}
	assertSyncFlagScoped(t, msg)
	assertSyncFlagScoped(t, coded.Hint)
}

// TestWrapUserTableCreateError_NonMatchingUnchanged pins the class
// boundary: anything that is not the safe-migrations 1105 falls
// through to [wrapDDLError]'s existing behavior — no code, no
// sentinel, error text untouched.
func TestWrapUserTableCreateError_NonMatchingUnchanged(t *testing.T) {
	cases := []struct {
		name string
		in   error
	}{
		{"plain error", errors.New("connection reset")},
		{"different MySQL code", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}},
		{"1105 without the magic text", &gomysql.MySQLError{Number: 1105, Message: "some other vitess error"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := wrapUserTableCreateError(c.in, "orders")
			if got != c.in { //nolint:errorlint // identity is the pin: pass-through, not rewrap
				t.Errorf("non-matching error rewrapped: got %v; want %v verbatim", got, c.in)
			}
			if _, ok := sluicecode.FromError(got); ok {
				t.Errorf("non-matching error wrongly coded: %v", got)
			}
		})
	}
}

// TestCreateTablesWithoutConstraints_1105CodedThroughWriter drives the
// wrap site end to end: a CREATE TABLE refused with the
// safe-migrations 1105 surfaces from CreateTablesWithoutConstraints as
// the coded refusal, under the site's own "mysql: create table" wrap.
func TestCreateTablesWithoutConstraints_1105CodedThroughWriter(t *testing.T) {
	script := &bootScript{execErr: directDDLDisabled()}
	w := &SchemaWriter{db: newBootDB(t, script), emitter: stdEmitter}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}

	err := w.CreateTablesWithoutConstraints(context.Background(), schema)
	if err == nil {
		t.Fatal("want the coded refusal; got nil")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("want *sluicecode.CodedError; got %T: %v", err, err)
	}
	if coded.Code != sluicecode.CodePSDirectDDLBlocked {
		t.Errorf("code = %s; want %s", coded.Code, sluicecode.CodePSDirectDDLBlocked)
	}
	if !strings.Contains(err.Error(), `mysql: create table "orders"`) {
		t.Errorf("outer wrap missing; err = %q", err)
	}
	assertSyncFlagScoped(t, err.Error())
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
