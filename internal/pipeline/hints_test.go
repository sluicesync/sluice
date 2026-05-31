// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"
)

// TestHintForRegistry covers each registry entry: a representative
// real-world error message (loosely based on the engines we ship)
// must surface the expected hint when paired with the matching
// phase. Adding a new hint to the registry should also add a row
// here so the entry is exercised.
func TestHintForRegistry(t *testing.T) {
	cases := []struct {
		name  string
		phase string
		err   error
		want  string
	}{
		{
			name:  "bulk-copy postgres relation does not exist",
			phase: PhaseBulkCopy,
			err: errors.New(
				`postgres: insert into "users" (3 rows): ERROR: relation "users" does not exist (SQLSTATE 42P01)`,
			),
			want: "target table not found",
		},
		{
			name:  "bulk-copy mysql table doesn't exist",
			phase: PhaseBulkCopy,
			err:   errors.New(`mysql: insert into "users" (3 rows): Error 1146: Table 'app.users' doesn't exist`),
			want:  "target table not found",
		},
		{
			name:  "bulk-copy generic copy-table failure surfaces resume hint (Bug 114)",
			phase: PhaseBulkCopy,
			err: errors.New(
				`pipeline: copy table "sentry_releases": postgres: insert into "sentry_releases": array of element type ir.JSON not supported (SQLSTATE 57014)`,
			),
			want: "use --resume to continue",
		},
		{
			name:  "connect: connection refused",
			phase: PhaseConnect,
			err:   errors.New(`postgres: open replication connection: dial tcp 127.0.0.1:5432: connect: connection refused`),
			want:  "verify the DSN host/port",
		},
		{
			name:  "connect: password authentication failed",
			phase: PhaseConnect,
			err:   errors.New(`pq: password authentication failed for user "sluice"`),
			want:  "verify the DSN username and password",
		},
		{
			name:  "connect: database does not exist",
			phase: PhaseConnect,
			err:   errors.New(`pq: database "wrongname" does not exist`),
			want:  "verify the --target DSN database name",
		},
		{
			name:  "schema-apply: permission denied for schema",
			phase: PhaseSchemaApply,
			err:   errors.New(`postgres: ddl: ERROR: permission denied for schema public (SQLSTATE 42501)`),
			want:  "the target role lacks CREATE on the schema",
		},
		{
			name:  "cdc: permission denied for replication",
			phase: PhaseCDC,
			err:   errors.New(`pq: permission denied for replication`),
			want:  "REPLICATION attribute",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := hintFor(c.phase, c.err)
			if got == "" {
				t.Fatalf("hintFor returned empty hint; expected one containing %q", c.want)
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("hintFor = %q; want substring %q", got, c.want)
			}
		})
	}
}

// TestHintForUnmatchedReturnsEmpty ensures that errors not matching
// any registry entry — or matching only in a different phase — get
// no hint.
func TestHintForUnmatchedReturnsEmpty(t *testing.T) {
	cases := []struct {
		name  string
		phase string
		err   error
	}{
		{
			name:  "unknown error in bulk-copy",
			phase: PhaseBulkCopy,
			err:   errors.New("postgres: copy: some unrelated driver explosion"),
		},
		{
			name:  "connection refused outside connect phase",
			phase: PhaseBulkCopy,
			err:   errors.New("connection refused"),
		},
		{
			name:  "permission denied for replication outside cdc phase",
			phase: PhaseSchemaApply,
			err:   errors.New("permission denied for replication"),
		},
		{
			name:  "nil error",
			phase: PhaseBulkCopy,
			err:   nil,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := hintFor(c.phase, c.err); got != "" {
				t.Errorf("hintFor(%q, %v) = %q; want empty", c.phase, c.err, got)
			}
		})
	}
}

// TestHintForCaseInsensitive verifies that casing in the underlying
// error message doesn't prevent a hint from matching. Drivers tend
// to mix cases (e.g. "ERROR: ..." vs "error: ...") and we don't
// want hints to flicker on/off based on which one fired.
func TestHintForCaseInsensitive(t *testing.T) {
	upper := errors.New(`POSTGRES: INSERT INTO "USERS": ERROR: RELATION "USERS" DOES NOT EXIST`)
	lower := errors.New(`postgres: insert into "users": error: relation "users" does not exist`)

	hUpper := hintFor(PhaseBulkCopy, upper)
	hLower := hintFor(PhaseBulkCopy, lower)
	if hUpper == "" || hLower == "" {
		t.Fatalf("expected matches in both cases; got upper=%q lower=%q", hUpper, hLower)
	}
	if hUpper != hLower {
		t.Errorf("case-insensitive match should produce identical hints: upper=%q lower=%q", hUpper, hLower)
	}
}

// TestWrapWithHintAppendsHintLine confirms that the wrapper format
// appends the hint after a newline with the "hint:" prefix, leaving
// the original error text intact above it.
func TestWrapWithHintAppendsHintLine(t *testing.T) {
	inner := errors.New(`postgres: insert into "users": ERROR: relation "users" does not exist`)
	wrapped := wrapWithHint(PhaseBulkCopy, inner)
	if wrapped == nil {
		t.Fatal("wrapWithHint returned nil for a hintable error")
	}
	got := wrapped.Error()
	if !strings.Contains(got, inner.Error()) {
		t.Errorf("wrapped error %q does not contain original %q", got, inner.Error())
	}
	if !strings.Contains(got, "\nhint: ") {
		t.Errorf("wrapped error %q does not contain a `\\nhint: ` line", got)
	}
}

// TestWrapWithHintNoMatchReturnsBareError checks that an error
// without a matching hint passes through unchanged — same Error()
// surface as the input. We assert on the message rather than the
// pointer to dodge errorlint's pointer-equality complaint, but the
// no-allocation pass-through is the property we care about.
func TestWrapWithHintNoMatchReturnsBareError(t *testing.T) {
	inner := errors.New("unrelated boring error")
	got := wrapWithHint(PhaseBulkCopy, inner)
	if got == nil || got.Error() != inner.Error() {
		t.Errorf("wrapWithHint should return the bare error when no hint matches; got %v want %v", got, inner)
	}
	if strings.Contains(got.Error(), "hint:") {
		t.Errorf("wrapWithHint added a hint when none should match: %q", got.Error())
	}
}

// TestWrapWithHintNil ensures wrapping nil returns nil so callers
// can use it inline at any error-return site without a guard.
func TestWrapWithHintNil(t *testing.T) {
	if got := wrapWithHint(PhaseBulkCopy, nil); got != nil {
		t.Errorf("wrapWithHint(_, nil) = %v; want nil", got)
	}
}

// TestWrapWithHintPreservesErrorsIs is the regression test for the
// %w usage: wrapping must keep [errors.Is] traversal working so
// that callers checking for sentinel errors aren't broken by the
// presentation-layer hint.
func TestWrapWithHintPreservesErrorsIs(t *testing.T) {
	sentinel := errors.New("sentinel")
	// Embed sentinel in an error that will trigger a hint match,
	// so we exercise the wrapping path (not the "no hint" pass-
	// through path) and still confirm errors.Is traversal.
	inner := errWithMsg{
		base: sentinel,
		msg:  `postgres: insert into "users": ERROR: relation "users" does not exist`,
	}
	wrapped := wrapWithHint(PhaseBulkCopy, inner)
	if !errors.Is(wrapped, sentinel) {
		t.Errorf("errors.Is should traverse through wrapWithHint; wrapped=%v sentinel=%v", wrapped, sentinel)
	}
}

// errWithMsg is a small helper that lets us craft an error whose
// Error() string triggers a registry match while still chaining to
// a sentinel via Unwrap. errors.New plus fmt.Errorf("%w", ...)
// can't easily produce both: we want full control over the surface
// message *and* the unwrap target.
type errWithMsg struct {
	base error
	msg  string
}

func (e errWithMsg) Error() string { return e.msg }
func (e errWithMsg) Unwrap() error { return e.base }
