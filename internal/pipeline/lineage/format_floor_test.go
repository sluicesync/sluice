// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// captureWarnings swaps the default slog logger for one writing JSON to a
// buffer, runs fn, and returns the records at WARN or above. The helper
// under test logs through the package-level logger (as every operator-facing
// warning in this package does), so the seam is the default logger.
func captureWarnings(t *testing.T, fn func(ctx context.Context)) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	fn(context.Background())

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode captured log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// TestWarnChainFormatVersionRaise_FiresOnlyOnARaise pins the trigger. The
// silent cases are the load-bearing half: a warning that fires on every
// ordinary incremental would be tuned out long before the one that matters.
func TestWarnChainFormatVersionRaise_FiresOnlyOnARaise(t *testing.T) {
	cases := []struct {
		name           string
		parent, segRec int
		wantWarn       bool
	}{
		{
			name:   "the Bug 212 shape: a v9 binary extends a v7 chain",
			parent: irbackup.FormatVersionChunkTableBinding,
			segRec: irbackup.FormatVersionInjectiveChunkAAD,
			// The whole reason this helper exists.
			wantWarn: true,
		},
		{
			name:   "the v8 instance, which predates ADR-0181 entirely",
			parent: irbackup.FormatVersionLegacy,
			segRec: irbackup.FormatVersionCDCPositionBinding,
			// A VStream CDC segment stamps 8 against a schema-derived root.
			// Same class, shipped in v0.99.228 — the warning is generic
			// precisely so it covers this without a second code path.
			wantWarn: true,
		},
		{
			name:     "chain already at the new version: the common case after one raise",
			parent:   irbackup.FormatVersionInjectiveChunkAAD,
			segRec:   irbackup.FormatVersionInjectiveChunkAAD,
			wantWarn: false,
		},
		{
			name:     "a chain written entirely by one version never warns",
			parent:   irbackup.FormatVersionEncryptedChunkBinding,
			segRec:   irbackup.FormatVersionEncryptedChunkBinding,
			wantWarn: false,
		},
		{
			name:   "segment BELOW the parent does not warn",
			parent: irbackup.FormatVersionInjectiveChunkAAD,
			segRec: irbackup.FormatVersionChunkTableBinding,
			// The inherit-the-chain's-shape ladder can stamp lower than the
			// chain root. That lowers nothing — the floor is the max over
			// links, so an older segment cannot raise it.
			wantWarn: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recs := captureWarnings(t, func(ctx context.Context) {
				WarnChainFormatVersionRaise(ctx, "backup-abc123", c.parent, c.segRec)
			})
			if !c.wantWarn {
				if len(recs) != 0 {
					t.Fatalf("warned on a non-raise (%d -> %d): %v", c.parent, c.segRec, recs)
				}
				return
			}
			if len(recs) != 1 {
				t.Fatalf("got %d warnings for a raise (%d -> %d); want exactly 1: %v", len(recs), c.parent, c.segRec, recs)
			}
		})
	}
}

// TestWarnChainFormatVersionRaise_NamesTheConsequence pins the content. A
// warning that says only "format version changed" is worse than none: it
// looks like housekeeping. What an operator has to be able to act on is
// that OTHER binaries just lost the chain, and which release clears it.
func TestWarnChainFormatVersionRaise_NamesTheConsequence(t *testing.T) {
	recs := captureWarnings(t, func(ctx context.Context) {
		WarnChainFormatVersionRaise(ctx, "backup-abc123",
			irbackup.FormatVersionChunkTableBinding, irbackup.FormatVersionInjectiveChunkAAD)
	})
	if len(recs) != 1 {
		t.Fatalf("got %d warnings; want 1", len(recs))
	}
	rec := recs[0]

	msg, _ := rec["msg"].(string)
	for _, want := range []string{
		"can no longer restore",
		// The non-obvious half, and the half an operator gets wrong: it is
		// not just the new segment that is lost to them.
		"ANY part of this chain",
		"segments they wrote themselves",
		// The release named in the prose, not only in an attribute, since
		// a human-readable log line is where this will actually be read.
		"v0.104.0",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("warning message does not mention %q\nmessage: %s", want, msg)
		}
	}

	for key, want := range map[string]any{
		"chain":                   "backup-abc123",
		"previous_format_version": float64(irbackup.FormatVersionChunkTableBinding),
		"new_format_version":      float64(irbackup.FormatVersionInjectiveChunkAAD),
		"minimum_sluice_version":  "v0.104.0",
	} {
		if got := rec[key]; got != want {
			t.Errorf("attr %q = %v (%T); want %v", key, got, got, want)
		}
	}
}

// TestWarnChainFormatVersionRaise_UnknownVersionStillWarns covers the
// degraded path. A version with no entry in the release table must still
// produce the warning — losing the "upgrade to X" advice is acceptable,
// losing the warning is not.
func TestWarnChainFormatVersionRaise_UnknownVersionStillWarns(t *testing.T) {
	future := irbackup.BackupFormatVersion + 1
	if irbackup.MinimumReaderVersion(future) != "" {
		t.Fatalf("format version %d has a release entry; this test needs one that does not", future)
	}

	recs := captureWarnings(t, func(ctx context.Context) {
		WarnChainFormatVersionRaise(ctx, "backup-abc123", irbackup.BackupFormatVersion, future)
	})
	if len(recs) != 1 {
		t.Fatalf("got %d warnings for a raise to an unmapped version; want 1", len(recs))
	}
	if _, present := recs[0]["minimum_sluice_version"]; present {
		t.Error("minimum_sluice_version attr present for a version with no release mapping; " +
			"an invented version would send operators to a binary that still cannot read the chain")
	}
	if msg, _ := recs[0]["msg"].(string); !strings.Contains(msg, "can no longer restore") {
		t.Errorf("degraded warning lost the consequence: %s", msg)
	}
}
