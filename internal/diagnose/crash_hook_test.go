// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCrashHook_WritesBundleBeforeReturn pins the basic happy path:
// when the wrapped function returns an error, the hook writes a
// bundle to the configured directory.
func TestCrashHook_WritesBundleBeforeReturn(t *testing.T) {
	dir := t.TempDir()

	base, full := newFakeTarget("stream-x")
	target := &fakeEngineFull{fakeEngine: base, full: full}

	hook, ok, err := Install(CrashHookConfig{
		Dir:          dir,
		PrivacyLevel: PrivacyBasic,
		RequestTemplate: Request{
			StreamID:     "stream-x",
			TargetEngine: target,
			TargetDSN:    "postgres://u:p@h:5432/d",
		},
		Now: func() time.Time { return time.Date(2026, 5, 22, 12, 30, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !ok {
		t.Fatalf("Install returned ok=false; expected true")
	}

	original := errors.New("simulated crash: source unreachable")
	got := hook.Wrap(context.Background(), original)
	if !errors.Is(got, original) {
		t.Fatalf("Wrap returned %v, want errors.Is(got, original)", got)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one crash bundle; got %d entries", len(entries))
	}
	name := entries[0].Name()
	if !strings.HasPrefix(name, "crash-bundle-") || !strings.HasSuffix(name, ".zip") {
		t.Errorf("crash bundle filename = %q, want crash-bundle-*.zip", name)
	}
	if !strings.Contains(name, "stream-x") {
		t.Errorf("crash bundle filename = %q, want it to include the stream id", name)
	}
}

// TestCrashHook_BundleWriteFailureDoesNotMaskOriginalError pins the
// load-bearing loud-failure invariant: even when bundle assembly
// fails, the original error MUST propagate unchanged. The bundle is
// best-effort; refusing to surface the real crash to the operator
// would be self-defeating.
func TestCrashHook_BundleWriteFailureDoesNotMaskOriginalError(t *testing.T) {
	// Point the hook at a directory that exists at Install-time but
	// gets removed before Wrap fires — the bundle write will fail
	// (parent missing) but the wrapped error must still come through.
	dir := t.TempDir()

	hook, ok, err := Install(CrashHookConfig{
		Dir:          dir,
		PrivacyLevel: PrivacyBasic,
		RequestTemplate: Request{
			StreamID:     "stream-x",
			TargetEngine: nil, // intentional: no engine → state collection produces a reason file, but the zip still writes
			TargetDSN:    "",
		},
	})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !ok {
		t.Fatalf("Install returned ok=false")
	}

	// Remove the directory after Install passed its check — the
	// bundle-write should now fail.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	original := errors.New("simulated crash")
	got := hook.Wrap(context.Background(), original)
	if !errors.Is(got, original) {
		t.Errorf("Wrap returned %v, want errors.Is(got, original) (bundle-write failure must not mask)", got)
	}
}

// TestCrashHook_BasicLevelDefault pins ADR-0056's "crash bundles
// default to the safest level" decision: when PrivacyLevel is unset,
// Install defaults it to PrivacyBasic. Operators opting up must do so
// explicitly via the CLI flag.
func TestCrashHook_BasicLevelDefault(t *testing.T) {
	dir := t.TempDir()

	hook, ok, err := Install(CrashHookConfig{
		Dir: dir,
		// PrivacyLevel deliberately unset.
		RequestTemplate: Request{StreamID: "stream-x"},
	})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !ok {
		t.Fatalf("Install returned ok=false")
	}
	if hook.cfg.PrivacyLevel != PrivacyBasic {
		t.Errorf("Install with unset PrivacyLevel defaulted to %s, want basic", hook.cfg.PrivacyLevel)
	}
}

// TestCrashHook_DisabledWhenDirEmpty pins the opt-in default: a hook
// with an empty Dir is not installed at all (ok=false, no error).
func TestCrashHook_DisabledWhenDirEmpty(t *testing.T) {
	hook, ok, err := Install(CrashHookConfig{Dir: ""})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if ok {
		t.Errorf("Install with empty Dir returned ok=true; expected false (opt-in only)")
	}
	if hook != nil {
		t.Errorf("Install with empty Dir returned non-nil hook; expected nil")
	}
}

// TestCrashHook_RefusesInvalidDir pins that a nonexistent directory
// fails Install loudly (rather than later, mid-crash, when the
// operator most needs the bundle).
func TestCrashHook_RefusesInvalidDir(t *testing.T) {
	_, _, err := Install(CrashHookConfig{
		Dir:          filepath.Join(t.TempDir(), "does-not-exist"),
		PrivacyLevel: PrivacyBasic,
	})
	if err == nil {
		t.Errorf("Install with nonexistent dir returned nil err; expected refusal")
	}
}

// TestCrashHook_NilHook_PassesThroughError pins the safety property
// of the Wrap method when the receiver is nil — used by the CLI's
// "no-op fallback" path when the operator hasn't opted in. nil hook
// returns the original error untouched.
func TestCrashHook_NilHook_PassesThroughError(t *testing.T) {
	var h *CrashHook
	original := errors.New("simulated")
	got := h.Wrap(context.Background(), original)
	if !errors.Is(got, original) {
		t.Errorf("nil hook Wrap returned %v, want errors.Is(got, original)", got)
	}
}

// TestCrashHook_NilHook_NilError pins the nil-in-nil-out semantics.
func TestCrashHook_NilHook_NilError(t *testing.T) {
	var h *CrashHook
	if got := h.Wrap(context.Background(), nil); got != nil {
		t.Errorf("nil hook Wrap(nil) = %v, want nil", got)
	}
}
