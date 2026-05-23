// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CrashHookConfig parametrises the auto-on-crash bundle writer. The
// CLI builds one of these from --diagnose-on-crash-dir and the
// matching --diagnose-on-crash-privacy flag, then installs it on the
// runtime path via [CrashHook.Wrap].
//
// **Default-OFF**: the hook is inactive when Dir is empty. The
// operator must opt in explicitly — an unattended bundle landing on
// disk is a privacy risk if the operator never asked for it.
type CrashHookConfig struct {
	// Dir is the directory bundles will be written to. Empty disables
	// the hook entirely. The directory must exist; the hook does NOT
	// auto-create (operator owns the parent's permissions).
	Dir string

	// PrivacyLevel is the level the crash-bundle is assembled at.
	// Defaults to PrivacyBasic at the CLI layer — see ADR-0056 for
	// why crash bundles default to the safest level.
	PrivacyLevel PrivacyLevel

	// RequestTemplate carries the StreamID + engine handles + DSNs
	// the hook needs to populate a Request when it fires. The hook
	// fills in CrashContext + PrivacyLevel at write time.
	RequestTemplate Request

	// Now is a clock override for tests. Production callers leave
	// this nil and the hook uses time.Now().
	Now func() time.Time
}

// CrashHook installs the auto-on-crash bundle writer. Returned by
// [Install]; the caller wires its Wrap method around the
// runtime-error path (cmd/sluice/main.go's kong dispatch).
type CrashHook struct {
	cfg CrashHookConfig
}

// Install constructs a [CrashHook] from cfg. Returns ok=false (with
// no hook) when Dir is empty — the disabled case is the default and
// not an error condition.
//
// Install validates Dir at install-time so an unwritable directory
// surfaces NOW, not in the middle of a crash. The defensive check is
// a Stat call rather than a write probe: probing with a write file
// would create operator-visible noise on every sluice startup.
func Install(cfg CrashHookConfig) (*CrashHook, bool, error) {
	if cfg.Dir == "" {
		return nil, false, nil
	}
	if cfg.PrivacyLevel == PrivacyUnset {
		cfg.PrivacyLevel = PrivacyBasic
	}
	info, err := os.Stat(cfg.Dir)
	if err != nil {
		return nil, false, fmt.Errorf("diagnose: --diagnose-on-crash-dir %q: %w", cfg.Dir, err)
	}
	if !info.IsDir() {
		return nil, false, fmt.Errorf("diagnose: --diagnose-on-crash-dir %q is not a directory", cfg.Dir)
	}
	return &CrashHook{cfg: cfg}, true, nil
}

// Wrap intercepts runErr from the wrapped runtime function. When
// runErr is non-nil the hook writes a crash bundle to the configured
// directory and returns the ORIGINAL runErr unchanged — the bundle is
// best-effort instrumentation; the loud-failure tenet says the
// original error is authoritative. A bundle-write failure is logged
// (slog WARN) but never propagated; the operator's runbook is to
// react to the original failure shape, not to the secondary bundle-
// write shape.
//
// This is the load-bearing "don't mask the original error" invariant
// pinned by [TestCrashHook_BundleWriteFailureDoesNotMaskOriginalError].
func (h *CrashHook) Wrap(ctx context.Context, runErr error) error {
	if h == nil || runErr == nil {
		return runErr
	}
	// Build a request from the template, stamping the crash context.
	req := h.cfg.RequestTemplate
	req.PrivacyLevel = h.cfg.PrivacyLevel
	req.CrashContext = runErr.Error()
	if h.cfg.Now != nil {
		req.Now = h.cfg.Now()
	}

	path, werr := h.writeBundle(ctx, req)
	if werr != nil {
		slog.WarnContext(
			ctx, "diagnose: crash bundle write failed; original error preserved",
			slog.String("err", werr.Error()),
			slog.String("original_err", runErr.Error()),
		)
		return runErr
	}
	slog.InfoContext(
		ctx, "diagnose: crash bundle written",
		slog.String("path", path),
		slog.String("privacy_level", req.PrivacyLevel.String()),
	)
	return runErr
}

// writeBundle assembles the bundle on disk. Filename format:
// `crash-bundle-<RFC3339>-<stream-id>.zip`. The RFC3339 timestamp uses
// a filename-safe substitution (colons → dashes) because Windows
// filesystems reject colons in filenames.
func (h *CrashHook) writeBundle(ctx context.Context, req Request) (string, error) {
	if h.cfg.Dir == "" {
		return "", errors.New("crash-hook directory is empty")
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}
	stamp := strings.ReplaceAll(now.UTC().Format(time.RFC3339), ":", "-")
	name := fmt.Sprintf("crash-bundle-%s-%s.zip", stamp, sanitizeFilename(req.StreamID))
	path := filepath.Join(h.cfg.Dir, name)
	f, err := os.Create(path) //nolint:gosec // path is composed of operator-supplied directory + structured filename
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	if err := Write(ctx, f, req); err != nil {
		// Best-effort cleanup: a half-written zip is just noise on
		// the operator's disk.
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

// sanitizeFilename replaces filesystem-unsafe characters in the
// stream-id with a hyphen. Most stream-ids are alphanumeric +
// underscore, so the substitution rarely fires; the helper is
// defensive against an operator using slashes or other separators
// in the stream-id.
func sanitizeFilename(s string) string {
	if s == "" {
		return "no-stream-id"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_', c == '-', c == '.':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
