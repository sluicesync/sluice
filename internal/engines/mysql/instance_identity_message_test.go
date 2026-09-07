// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// A changed source identity REFUSES, and the refusal is terminal.
//
// # What this pins, and why the wrapping assertion is the important one
//
// [verifySourceInstanceIdentity]'s firing arm — the persisted position names
// one instance, the DSN answers as another — must return an error that does
// NOT wrap [ir.ErrPositionInvalid].
//
// That is not a stylistic preference, it is the entire mechanism. Wrapping
// ir.ErrPositionInvalid routes the streamer's ADR-0022 fall-through, and on a
// binlog source that fall-through AUTO-RE-SNAPSHOTS: it DROPS the target's
// tables and re-copies from whichever instance is now answering the DSN. So a
// later, entirely well-meant `%w` here would silently convert this refusal
// back into the most destructive action sluice can take, with no test failing
// and no message changing. Nothing else in the codebase would notice.
//
// # The history (audit SLM-6)
//
// This arm used to wrap ir.ErrPositionInvalid AND log that sluice was
// "refusing to resume to avoid a silent data gap" — so the one message an
// operator saw as the target was about to be dropped told them they were being
// protected from it. The audit observed `tables_dropped=2`.
//
// The first fix corrected only the claim. The posture was then decided in the
// same release, so no version ever shipped the interim "this is not a refusal"
// wording.
//
// # Why refuse here but not for a purge
//
// A purge means the SAME server advanced past the position: re-copying from it
// is right, and routine. An identity change means a DIFFERENT SERVER, which
// sluice cannot distinguish from a stale connection string or a load-balanced
// endpoint that landed elsewhere. The outcomes are asymmetric — refusing costs
// one --restart-from-scratch on an event that already involved a human
// rebuilding a server; not refusing destroys the target from the wrong source
// at exit 0. And no unattended event reaches this arm: only NON-GTID MySQL
// gets here (gtid_mode=ON takes the GTID arm, whose lineage check catches a
// replaced instance by construction; Vitess/PlanetScale never reach this file;
// MariaDB has its own lineage path).
func TestInstanceIdentityMismatch_RefusesTerminally(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	err := verifySourceInstanceIdentity(context.Background(), "uuid-old", "uuid-new")
	if err == nil {
		t.Fatal("a genuine identity mismatch was accepted; the resume would start at a byte offset " +
			"in an unrelated instance's binlog")
	}

	// THE load-bearing assertion. See the doc above: a `%w` added here turns
	// the refusal back into an automatic drop-and-re-copy, silently.
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("the identity-mismatch error wraps ir.ErrPositionInvalid again:\n  %v\n\n"+
			"That wrapping routes the streamer's ADR-0022 fall-through, which on a binlog source "+
			"AUTO-RE-SNAPSHOTS — it DROPS the target's tables and re-copies from whichever instance is "+
			"now answering the DSN. This arm exists precisely because that instance is a DIFFERENT "+
			"SERVER and may be a stale connection string rather than an intended replacement. If the "+
			"posture is genuinely being reverted to auto-recovery, that is an operator decision: change "+
			"this test's doc, not just the assertion.", err)
	}

	// The operator has to be able to act on it: what happened, and the one
	// command that makes the re-copy deliberate.
	for _, want := range []string{"REFUSING", "--restart-from-scratch", "uuid-old", "uuid-new"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal no longer names %q:\n  %v", want, err)
		}
	}

	// And the WARN carries the grep-stable marker plus both identities, so an
	// operator can find this in a log they are scrolling mid-incident.
	log := buf.String()
	for _, want := range []string{instanceIdentityChangedMarker, "uuid-old", "uuid-new"} {
		if !strings.Contains(log, want) {
			t.Errorf("the identity-mismatch WARN no longer names %q:\n  %s", want, strings.TrimSpace(log))
		}
	}
}

// TestInstanceIdentityProbeFailure_StaysPermissive pins the SIBLING arm, and
// the asymmetry with it, so neither gets "made consistent" by accident.
//
// `currentUUID == ""` means the probe could not run. That is a DIFFERENT
// question from a known mismatch, and it stays permissive on purpose (audit
// SLM-7): @@server_uuid is read ONCE at stream open, so an empty value is not
// a mid-stream transient — it means the source will not tell sluice who it is,
// whose realistic cause is a proxy or managed service that does not expose the
// variable. Failing closed there would hard-block those deployments on the
// file/pos path with no workaround, rather than catch a blip.
//
// It must stay LOUD, though: the whole residual is that an operator who cannot
// be protected knows it.
func TestInstanceIdentityProbeFailure_StaysPermissive(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	if err := verifySourceInstanceIdentity(context.Background(), "uuid-old", ""); err != nil {
		t.Fatalf("a probe failure now refuses: %v\n"+
			"That hard-blocks any source behind a proxy that does not expose @@server_uuid — the "+
			"realistic cause, since the value is read once at stream open. If this is a deliberate "+
			"tightening it needs an opt-out flag first; see the SLM-7 entry in "+
			"docs/dev/audit-backlog.md.", err)
	}
	if !strings.Contains(buf.String(), unverifiedInstanceIdentityMarker) {
		t.Errorf("the probe-failure arm went quiet. Permissive is only defensible while it is LOUD — "+
			"the residual is an operator who cannot be protected and does not know:\n  %s",
			strings.TrimSpace(buf.String()))
	}
}
