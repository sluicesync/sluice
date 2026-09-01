// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// captureLogsAt swaps the default slog handler for one writing to a
// buffer at the given level, restoring it when the test ends.
func captureLogsAt(t *testing.T, level slog.Level) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestPositionModeAdvisoryApplies pins the flavor half of the gtid_mode
// advisory's decision.
//
// The advisory exists because sluice picks the weaker resume arm silently
// on MySQL 8's DEFAULT (gtid_mode=OFF), and file/pos is the arm carrying
// the instance-identity hazard. But two flavors have no choice to advise
// about, and telling them to "enable GTID mode" would be wrong, not just
// noisy: MariaDB is always in GTID mode, and the vtgate flavors resume on
// VStream positions and reach neither binlog arm.
func TestPositionModeAdvisoryApplies(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		flavor Flavor
		want   bool
		why    string
	}{
		{FlavorVanilla, true, "vanilla MySQL can be either mode — the whole point"},
		{FlavorMariaDB, false, "MariaDB is always GTID mode (gtidModeOnFor returns true unconditionally)"},
		{FlavorPlanetScale, false, "resumes on VStream positions; reaches neither binlog arm"},
		{FlavorVitess, false, "resumes on VStream positions; reaches neither binlog arm"},
	} {
		if got := positionModeAdvisoryApplies(c.flavor); got != c.want {
			t.Errorf("positionModeAdvisoryApplies(%v) = %v, want %v — %s", c.flavor, got, c.want, c.why)
		}
	}

	// Anti-vacuity: at least one flavor must apply and one must not, or a
	// predicate stuck at a constant would satisfy the table above only
	// because the table happened to agree with the constant.
	var applied, skipped int
	for _, f := range []Flavor{FlavorVanilla, FlavorMariaDB, FlavorPlanetScale, FlavorVitess} {
		if positionModeAdvisoryApplies(f) {
			applied++
		} else {
			skipped++
		}
	}
	if applied == 0 || skipped == 0 {
		t.Fatalf("the predicate is constant across every flavor (%d applied, %d skipped)", applied, skipped)
	}
}

// TestPositionModeAdvisoryText pins the two things the message must not
// lose: that file/pos is SUPPORTED (this is an INFO about a working
// configuration, and prose that reads as a defect trains operators to
// ignore it), and the reason it is nonetheless the weaker arm.
func TestPositionModeAdvisoryText(t *testing.T) {
	t.Parallel()
	for _, want := range []string{
		"supported and correct", // not a defect
		"instance-local",        // the actual mechanism
		"@@server_uuid",         // what protects the weaker arm today
		"gtid_mode=OFF",         // the condition the operator can check
	} {
		if !strings.Contains(positionModeAdvisoryText, want) {
			t.Errorf("the advisory no longer says %q:\n%s", want, positionModeAdvisoryText)
		}
	}
}

// TestVerifySourceInstanceIdentity_DegradedArmsAreLoud pins the 2026-09-01
// split. Both empty cases still return nil — the posture is deliberately
// unchanged — but neither is SILENT any more, because an operator could
// not previously tell an unverifiable resume from a verified one, and the
// residual on this path is silent data loss rather than a cosmetic gap.
//
// The two arms are asserted SEPARATELY, and their messages must differ:
// they are different questions (an old position vs a failed probe on a
// refusal-gating check) that shared one `||` until the split, and a single
// shared message would re-merge them in the operator's eyes.
func TestVerifySourceInstanceIdentity_DegradedArmsAreLoud(t *testing.T) {
	for _, c := range []struct {
		name           string
		persisted, cur string
		wantPhrase     string
	}{
		{"unverifiable position (pre-stamping)", "", "uuid-B", "carries no source instance identity"},
		{"probe failure on a gating check", "uuid-A", "", "could not be read"},
	} {
		t.Run(c.name, func(t *testing.T) {
			buf := captureLogsAt(t, slog.LevelWarn)
			if err := verifySourceInstanceIdentity(context.Background(), c.persisted, c.cur); err != nil {
				t.Fatalf("posture changed: this arm must still return nil, got %v", err)
			}
			got := buf.String()
			if !strings.Contains(got, unverifiedInstanceIdentityMarker) {
				t.Errorf("no %s WARN was emitted; the arm is still silent:\n%s",
					unverifiedInstanceIdentityMarker, got)
			}
			if !strings.Contains(got, c.wantPhrase) {
				t.Errorf("the WARN does not distinguish this arm (want %q):\n%s", c.wantPhrase, got)
			}
		})
	}

	// The healthy path must stay quiet, or the marker becomes noise that
	// an operator filters out — which would defeat both arms above.
	t.Run("matching identity is silent", func(t *testing.T) {
		buf := captureLogsAt(t, slog.LevelWarn)
		if err := verifySourceInstanceIdentity(context.Background(), "uuid-A", "uuid-A"); err != nil {
			t.Fatalf("a matching identity must resume: %v", err)
		}
		if strings.Contains(buf.String(), unverifiedInstanceIdentityMarker) {
			t.Errorf("a verified resume emitted the unverified marker:\n%s", buf.String())
		}
	})
}
