// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The identity-mismatch message must describe what sluice DOES.
//
// # The defect this pins (audit SLM-6)
//
// [verifySourceInstanceIdentity]'s firing arm used to log that sluice was
// "refusing to resume to avoid a silent data gap". It does not refuse. It
// returns an error wrapping [ir.ErrPositionInvalid] — deliberately, to route
// the streamer's ADR-0022 fall-through — and on a binlog/GTID source that
// fall-through AUTO-RE-SNAPSHOTS: it DROPS the target's tables and re-copies
// them from whichever instance is now answering the DSN. The audit observed
// `tables_dropped=2`.
//
// So at the exact moment sluice was about to perform its most destructive
// action, the only message the operator saw told them they were being
// protected from it. That is worse than silence: an operator reading "refusing
// to resume" has no reason to go and check which instance answered.
//
// # What is NOT being changed, and why that is deliberate
//
// The behaviour. For a genuinely replaced node the re-snapshot is the correct
// recovery and matches the slot-purge posture. Whether an identity change
// should instead refuse BY DEFAULT — it means a different SERVER, which sluice
// cannot tell apart from a misconfigured DSN — is a live operator question,
// and flipping it would turn a configuration that auto-recovers today into one
// that halts. This test therefore grades the claim, not the posture.
func TestInstanceIdentityMismatch_MessageDoesNotClaimARefusal(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	err := verifySourceInstanceIdentity(context.Background(), "uuid-old", "uuid-new")
	if err == nil {
		t.Fatal("a genuine identity mismatch was accepted; the resume would start at a byte offset " +
			"in an unrelated instance's binlog")
	}
	if !isPositionInvalid(err) {
		t.Fatalf("the mismatch error no longer wraps ir.ErrPositionInvalid (%v).\n"+
			"That wrapping is what routes the streamer's ADR-0022 fall-through. If you changed it to "+
			"make this a hard refusal, that is the operator question in docs/dev/audit-backlog.md — "+
			"update this test's doc rather than deleting the assertion.", err)
	}

	log := buf.String()

	// The claim that was false, pinned as the CLAIM rather than as the
	// word. A bare "refus" substring is the wrong assertion and was the
	// first cut here: it fires on the message's own correct negation
	// ("WHAT HAPPENS NEXT IS NOT A REFUSAL"), which is precisely the
	// sentence that fixes the defect. Grade what is asserted, not what is
	// mentioned.
	for _, falseClaim := range []string{
		"refusing to resume",
		"refuses to resume",
		"refusing to avoid",
	} {
		if strings.Contains(strings.ToLower(log), falseClaim) {
			t.Errorf("the identity-mismatch WARN claims %q again:\n  %s\n"+
				"sluice does not refuse here by default — it falls through to a cold start that DROPS "+
				"the target's tables and re-copies from the instance now answering the DSN. If the "+
				"posture genuinely changed to a refusal, the streamer fall-through has to change too, "+
				"and this test's doc with it.", falseClaim, strings.TrimSpace(log))
		}
	}
	// And the negation must actually be present, so the message cannot
	// simply go quiet about which of the two it is.
	if !strings.Contains(log, "NOT A REFUSAL") {
		t.Errorf("the WARN no longer says plainly that this is not a refusal:\n  %s",
			strings.TrimSpace(log))
	}

	// The facts an operator needs at that moment, each load-bearing:
	// that a destructive re-copy is what happens, that its SOURCE is
	// whatever now answers the DSN, and the flag that stops it.
	for _, want := range []string{
		instanceIdentityChangedMarker,
		"DROPS",
		"--no-auto-resnapshot",
		"uuid-old",
		"uuid-new",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("the identity-mismatch WARN no longer names %q. An operator reading it has to be "+
				"able to tell that a destructive re-copy is about to run, which instance it will read "+
				"from, and how to stop it.\n  %s", want, strings.TrimSpace(log))
		}
	}
}

// isPositionInvalid keeps the wrapping assertion readable above.
func isPositionInvalid(err error) bool {
	return err != nil && strings.Contains(err.Error(), ir.ErrPositionInvalid.Error())
}
