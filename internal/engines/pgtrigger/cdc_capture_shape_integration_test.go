//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the capture-shape door (cdc_capture_shape.go, audit
// 2026-08-26 F2) against real PG: a dropped capture trigger, a DISABLEd
// trigger, and a dropped event trigger each refuse CDC open loudly with a
// re-setup remedy; the healthy install (and a `trigger setup` re-run after
// each defect) opens clean — the no-false-refuse floor. One stage also
// drives the refusal through Engine.OpenSnapshotStream, pinning that BOTH
// stream-open paths reach the door (the moved-door caller list).
//
// The stages share one container and run in order; each defect stage
// repairs the source (re-setup) before the next.

package pgtrigger

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCDCOpen_CaptureShapeDoor(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE shape_t (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	setup := func(t *testing.T) {
		t.Helper()
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"shape_t"}}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
	}
	openWantRefusal := func(t *testing.T, wantAll ...string) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatalf("CDC open succeeded; want a capture-shape refusal containing %q", wantAll)
		}
		for _, want := range wantAll {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
	}
	openWantClean := func(t *testing.T) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("CDC open refused a healthy install (false refuse): %v", err)
		}
		_ = r.(*CDCReader).Close()
	}

	setup(t)

	t.Run("healthy install opens clean", func(t *testing.T) {
		openWantClean(t)
	})

	t.Run("dropped capture trigger refuses on both open paths", func(t *testing.T) {
		applyPGSQL(t, dsn, `DROP TRIGGER sluice_capture ON shape_t`)
		defer setup(t)
		openWantRefusal(t, "shape_t", CaptureTriggerRow, "MISSING", "trigger setup")

		// The cold-start path reaches the same door: OpenSnapshotStream
		// builds the poller through openCDCReader.
		if stream, err := (Engine{}).OpenSnapshotStream(ctx, dsn); err == nil {
			_ = stream.Close()
			t.Fatal("OpenSnapshotStream succeeded on a source with a dropped capture trigger; want the capture-shape refusal")
		} else if !strings.Contains(err.Error(), "MISSING") {
			t.Errorf("OpenSnapshotStream refusal should carry the capture-shape message; got %v", err)
		}
	})

	t.Run("disabled capture trigger refuses", func(t *testing.T) {
		applyPGSQL(t, dsn, `ALTER TABLE shape_t DISABLE TRIGGER sluice_capture`)
		defer applyPGSQL(t, dsn, `ALTER TABLE shape_t ENABLE TRIGGER sluice_capture`)
		openWantRefusal(t, "shape_t", "DISABLED")
	})

	t.Run("dropped event trigger refuses", func(t *testing.T) {
		applyPGSQL(t, dsn, `DROP EVENT TRIGGER sluice_capture_ddl_trg`)
		defer setup(t)
		openWantRefusal(t, CaptureTriggerDDL, "MISSING")
	})

	t.Run("re-setup repairs every defect (remedy really runs)", func(t *testing.T) {
		openWantClean(t)
	})
}
