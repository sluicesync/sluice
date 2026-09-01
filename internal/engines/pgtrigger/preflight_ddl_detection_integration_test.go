//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the two open-path DDL-capture WARNs against real
// PG, each in both directions:
//
//   - event-trigger tier (the default install): NO DDL-DETECTION-ABSENT
//     warn — the tier has real detection (op='X' refusal), so warning
//     would be a false alarm;
//   - polled-tier shape (no DDL capture function, no event trigger —
//     exactly what `setup --allow-polled-fingerprint` leaves on a role
//     that cannot CREATE EVENT TRIGGER): the WARN fires at CDC open with
//     the grep-stable marker and the drained-model steer;
//   - and the ARM-level sibling (audit 2026-08-31 D-1): an install with
//     the ddl_command_end arm but no `sql_drop` one — every install made
//     before v0.135 — WARNs DROP-CAPTURE-ABSENT and still opens, while an
//     install carrying both arms does not.
//
// The polled shape is produced by dropping BOTH halves of the
// event-trigger tier after a normal install — the same catalog state a
// genuine polled install has (fn absent ⇒ the F2 capture-shape door's
// event-trigger check is exempt, which is precisely the state the G1
// sweep found undefended). A real non-superuser polled install was
// reproduced end-to-end by the sweep (workspace/m2, verified); this pin
// uses the catalog-equivalent shape so it can run on the shared
// superuser container.
package pgtrigger

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDDLDetectionAbsentWarn_AtCDCOpen(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE g1_t (id BIGINT PRIMARY KEY, v TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"g1_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	t.Run("event-trigger tier: no DDL-DETECTION-ABSENT warn (no false alarm)", func(t *testing.T) {
		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if strings.Contains(logs, ddlDetectionAbsentMarker) {
			t.Fatalf("DDL-DETECTION-ABSENT warned on an event-trigger-tier install (false alarm):\n%s", logs)
		}
	})

	t.Run("event-trigger tier with the sql_drop arm: no DROP-CAPTURE-ABSENT warn", func(t *testing.T) {
		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if strings.Contains(logs, dropCaptureAbsentMarker) {
			t.Fatalf("%s warned on an install that HAS the sql_drop arm (false alarm):\n%s", dropCaptureAbsentMarker, logs)
		}
	})

	t.Run("pre-v0.135 shape: DROP-CAPTURE-ABSENT warns, and the open still succeeds", func(t *testing.T) {
		// Exactly what a v0.134.x install leaves behind: the ddl_command_end
		// arm, and nothing on sql_drop. It must WARN, not refuse — the D-1
		// gap is bounded and static, and refusing here would strand every
		// running sync the moment the operator upgrades the binary.
		applyPGSQL(t, dsn, `DROP EVENT TRIGGER `+CaptureTriggerDrop)
		applyPGSQL(t, dsn, `DROP FUNCTION public.`+CaptureFunctionDrop+`()`)

		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader refused a pre-sql_drop-arm install: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if !strings.Contains(logs, dropCaptureAbsentMarker) {
			t.Fatalf("an install without the sql_drop arm did not WARN with %s:\n%s", dropCaptureAbsentMarker, logs)
		}
		for _, want := range []string{"invisible to capture", "trigger setup", CaptureTriggerDrop} {
			if !strings.Contains(logs, want) {
				t.Errorf("WARN missing %q; logs:\n%s", want, logs)
			}
		}
	})

	t.Run("polled-tier shape: WARN fires with the marker and the drained-model steer", func(t *testing.T) {
		// Drop both halves of the event-trigger tier — the catalog shape a
		// genuine --allow-polled-fingerprint install has. (Dropping only
		// the event trigger is the F2 door's REFUSAL shape, distinct from
		// this tier.) The sql_drop arm went with the previous subtest; a
		// polled install has neither arm, which is what makes THIS the
		// tier-level warning rather than the arm-level one.
		applyPGSQL(t, dsn, `DROP EVENT TRIGGER `+CaptureTriggerDDL)
		applyPGSQL(t, dsn, `DROP FUNCTION public.`+CaptureFunctionDDL+`()`)

		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if !strings.Contains(logs, ddlDetectionAbsentMarker) {
			t.Fatalf("polled-tier CDC open did not WARN with %s:\n%s", ddlDetectionAbsentMarker, logs)
		}
		for _, want := range []string{"invisible to capture", "sync stop --wait", "trigger setup"} {
			if !strings.Contains(logs, want) {
				t.Errorf("WARN missing %q; logs:\n%s", want, logs)
			}
		}
	})
}
