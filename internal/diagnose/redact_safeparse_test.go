// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

// TestSafeParseError_StripsDSNFromParseError pins the credential-leak
// fix: net/url.Parse embeds the raw input (including a DSN's password)
// in its *url.Error, and sluice wraps that error with %w at several
// DSN-parse sites. SafeParseError must drop the embedded URL while
// preserving the useful reason.
func TestSafeParseError_StripsDSNFromParseError(t *testing.T) {
	// A DSN whose password is a real secret and whose host carries an
	// un-encoded control byte (\x7f) — the shape that makes url.Parse
	// fail *after* it has already captured the userinfo.
	const secret = "SUPERSECRET"
	// Build the string at runtime (a []byte→string conversion is not a
	// constant expr) so staticcheck's SA1007 doesn't flag the literal.
	ctrl := string([]byte{0x7f})
	_, perr := url.Parse("postgres://appuser:" + secret + "@host" + ctrl + "/db")
	if perr == nil {
		t.Fatal("expected url.Parse to fail on the control-char DSN")
	}
	// Ground-truth the leak we are guarding against: the raw error DOES
	// contain the password.
	if !strings.Contains(perr.Error(), secret) {
		t.Fatalf("precondition: expected raw url.Parse error to contain %q, got %q", secret, perr.Error())
	}

	got := SafeParseError(perr)
	if strings.Contains(got.Error(), secret) {
		t.Errorf("SafeParseError leaked the credential: %q", got.Error())
	}
	if !strings.Contains(got.Error(), "invalid control character") {
		t.Errorf("SafeParseError dropped the useful reason: got %q, want it to contain %q", got.Error(), "invalid control character")
	}
}

// TestSafeParseError_PassthroughNonURLError verifies a non-*url.Error
// is returned unchanged (identity), so the helper is safe to drop in
// front of any error at a DSN-parse site.
func TestSafeParseError_PassthroughNonURLError(t *testing.T) {
	plain := errors.New("some unrelated failure")
	if got := SafeParseError(plain); !errors.Is(got, plain) {
		t.Errorf("SafeParseError(non-url error) = %v; want the same error unchanged", got)
	}
}

// TestSafeParseError_Nil verifies nil in, nil out.
func TestSafeParseError_Nil(t *testing.T) {
	if got := SafeParseError(nil); got != nil {
		t.Errorf("SafeParseError(nil) = %v; want nil", got)
	}
}

// TestSafeParseError_WrappedURLError verifies errors.As reaches a
// *url.Error even when it is itself wrapped, matching how callers wrap
// it further with %w.
func TestSafeParseError_WrappedURLError(t *testing.T) {
	const secret = "hunter2"
	// A *url.Error carrying the DSN, itself wrapped by an outer error —
	// the shape produced once a caller wraps it with %w. errors.As must
	// still reach it and strip the URL.
	nested := &url.Error{Op: "parse", URL: "postgres://u:" + secret + "@h/d", Err: errors.New("boom")}
	outer := errWrap{err: nested}
	got := SafeParseError(outer)
	if strings.Contains(got.Error(), secret) {
		t.Errorf("SafeParseError leaked credential through a wrapped url.Error: %q", got.Error())
	}
	if got.Error() != "boom" {
		t.Errorf("SafeParseError(wrapped) = %q; want the inner reason %q", got.Error(), "boom")
	}
}

type errWrap struct{ err error }

func (e errWrap) Error() string { return "outer: " + e.err.Error() }
func (e errWrap) Unwrap() error { return e.err }
