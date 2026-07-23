// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CLI-layer pins for `sluice sync decommission` (audit 2026-07-23
// DEVEX-3 / Q3). Per the Bug 180 lesson, the flag shape is pinned
// through the real kong grammar; the confirmation gate is pinned as a
// coded refusal that fires BEFORE any engine resolution or dial.

package main

import (
	"bytes"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestSyncDecommissionCmd_FlagShape pins the cross-DSN shape through
// kong: both driver/DSN pairs, the stream id, and the two safety
// flags defaulting OFF.
func TestSyncDecommissionCmd_FlagShape(t *testing.T) {
	cli := parseInto(
		t, "sync", "decommission",
		"--source-driver", "postgres", "--source", "postgres://u:p@src/db",
		"--target-driver", "postgres", "--target", "postgres://u:p@dst/db",
		"--stream-id", "wave-a",
	)
	d := cli.Sync.Decommission
	if d.SourceDriver != "postgres" || d.Source == "" || d.TargetDriver != "postgres" || d.Target == "" {
		t.Errorf("cross-DSN flags did not reach the command: %+v", d)
	}
	if d.StreamID != "wave-a" {
		t.Errorf("--stream-id = %q; want wave-a", d.StreamID)
	}
	if d.Yes || d.DryRun {
		t.Errorf("--yes/--dry-run must default off; got yes=%v dry-run=%v", d.Yes, d.DryRun)
	}
}

// TestSyncDecommissionCmd_RefusesWithoutYes pins the confirmation
// gate: without --yes (and without --dry-run) Run returns the coded
// CONFIRMATION-REQUIRED refusal (exit 3) before resolving any engine
// or dialing anything — the same non-interactive posture as
// `slot drop`.
func TestSyncDecommissionCmd_RefusesWithoutYes(t *testing.T) {
	cli := parseInto(
		t, "sync", "decommission",
		// A driver that doesn't exist: if the gate resolved engines
		// before refusing, the error would be a driver error instead.
		"--source-driver", "no-such-engine", "--source", "dsn",
		"--target-driver", "no-such-engine", "--target", "dsn",
		"--stream-id", "wave-a",
	)
	err := cli.Sync.Decommission.Run(&Globals{})
	if err == nil {
		t.Fatal("expected the confirmation refusal")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeConfirmationRequired {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeConfirmationRequired)
	}
	if got := exitCodeLikeKong(err); got != sluicecode.ExitRefusal {
		t.Errorf("exit code = %d; want %d (refusal)", got, sluicecode.ExitRefusal)
	}
	if !strings.Contains(coded.Hint, "--yes") || !strings.Contains(coded.Hint, "--dry-run") {
		t.Errorf("hint = %q; must name both --yes and the --dry-run preview", coded.Hint)
	}
	if !strings.Contains(err.Error(), "warm-resume") {
		t.Errorf("err = %v; the refusal must say why this needs confirming (no warm-resume after)", err)
	}
}

// TestSyncDecommissionCmd_DryRunSkipsConfirmation pins that --dry-run
// alone passes the gate (it touches nothing, so previewing must be
// frictionless): the returned error is then the driver-resolution
// error, NOT the confirmation refusal.
func TestSyncDecommissionCmd_DryRunSkipsConfirmation(t *testing.T) {
	cli := parseInto(
		t, "sync", "decommission",
		"--source-driver", "no-such-engine", "--source", "dsn",
		"--target-driver", "no-such-engine", "--target", "dsn",
		"--stream-id", "wave-a",
		"--dry-run",
	)
	err := cli.Sync.Decommission.Run(&Globals{})
	if err == nil {
		t.Fatal("expected a driver-resolution error")
	}
	if coded, ok := sluicecode.FromError(err); ok && coded.Code == sluicecode.CodeConfirmationRequired {
		t.Fatalf("--dry-run tripped the confirmation gate: %v", err)
	}
	if !strings.Contains(err.Error(), "--target-driver") {
		t.Errorf("err = %v; want the target-driver resolution error (the gate was passed)", err)
	}
}

// TestRenderDecommissionReport pins the per-object accounting the
// command's contract requires: one line per object in every state,
// dry-run reading as "would", and the kept-row line naming the re-run
// posture.
func TestRenderDecommissionReport(t *testing.T) {
	t.Run("full removal", func(t *testing.T) {
		var buf bytes.Buffer
		renderDecommissionReport(&buf, &pipeline.DecommissionReport{
			StreamID: "wave-a", SlotName: "sluice_wave_a", PublicationName: "sluice_wave_a",
			SlotDropped: true, PublicationDropped: true, ControlRowCleared: true,
		})
		out := buf.String()
		for _, want := range []string{
			`dropped replication slot "sluice_wave_a"`,
			`dropped per-stream publication "sluice_wave_a"`,
			`cleared control row for stream "wave-a"`,
			"can no longer warm-resume",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("output missing %q:\n%s", want, out)
			}
		}
	})
	t.Run("dry run", func(t *testing.T) {
		var buf bytes.Buffer
		renderDecommissionReport(&buf, &pipeline.DecommissionReport{
			StreamID: "wave-a", SlotName: "sluice_wave_a", PublicationName: "sluice_wave_a", DryRun: true,
			SlotDropped: true, PublicationDropped: true, ControlRowCleared: true,
		})
		out := buf.String()
		for _, want := range []string{
			"[dry-run] would drop replication slot",
			"[dry-run] would clear control row",
			"dry run: nothing was changed",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("output missing %q:\n%s", want, out)
			}
		}
		if strings.Contains(out, "decommissioned;") {
			t.Errorf("dry-run output must not claim completion:\n%s", out)
		}
	})
	t.Run("partial failure keeps row", func(t *testing.T) {
		var buf bytes.Buffer
		renderDecommissionReport(&buf, &pipeline.DecommissionReport{
			StreamID: "wave-a", SlotName: "sluice_wave_a", PublicationName: "sluice_wave_a",
			SlotDropped: true, PublicationSkipped: "the source engine does not expose publication management; drop the publication manually if the stream had its own",
			ControlRowCleared: false,
		})
		out := buf.String()
		if !strings.Contains(out, "KEPT (re-run to finish") {
			t.Errorf("kept-row line missing the re-run posture:\n%s", out)
		}
		if !strings.Contains(out, "no publication removed — the source engine does not expose") {
			t.Errorf("skip reason not surfaced:\n%s", out)
		}
	})
	t.Run("control row only", func(t *testing.T) {
		var buf bytes.Buffer
		renderDecommissionReport(&buf, &pipeline.DecommissionReport{
			StreamID:           "wave-a",
			SlotSkipped:        "the source engine has no replication slots (the binlog/change-log is the stream); nothing durable to remove on the source",
			PublicationSkipped: "the source engine has no publications",
			ControlRowCleared:  true,
		})
		out := buf.String()
		if !strings.Contains(out, "no replication slot removed — the source engine has no replication slots") {
			t.Errorf("slotless reason not surfaced:\n%s", out)
		}
		if !strings.Contains(out, `cleared control row for stream "wave-a"`) {
			t.Errorf("row clear missing:\n%s", out)
		}
	})
}
