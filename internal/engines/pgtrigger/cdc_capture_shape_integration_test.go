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

	t.Run("a capture trigger keyed on the WRONG column refuses, on real catalog bytes", func(t *testing.T) {
		// THE CELL THAT WOULD HAVE CAUGHT THE INERT DOOR, and the reason
		// it has to be an integration cell rather than a unit one.
		//
		// The PK-argument grading shipped 100% inert: the query read
		// `encode(tgargs,'escape')`, which renders the NUL terminator as
		// the four literal characters \000, so the JSON parse failed and
		// the grading was skipped on every real install. Nothing caught
		// it, and mutation-running the fix afterwards showed why — the
		// unit cells feed the parser base64 DIRECTLY, so they grade the
		// parser and say nothing about the QUERY; and every other
		// integration cell asserts a refusal that some OTHER arm raises,
		// so all of them stayed green with the broken encoding restored.
		// The query→parser pairing was the untested seam, and it is
		// exactly where the defect lived.
		//
		// This cell drives that seam end to end: a real trigger, keyed on
		// a real column that is not the table's primary key, read back
		// through the real catalog query. If the argument encoding ever
		// stops round-tripping, the grade cannot fire and this fails —
		// which is the only signal that distinguishes "the door checked
		// and was satisfied" from "the door could not read its input".
		applyPGSQL(t, dsn, `DROP TRIGGER sluice_capture ON shape_t`)
		applyPGSQL(t, dsn, `CREATE TRIGGER sluice_capture
			AFTER INSERT OR UPDATE OR DELETE ON shape_t
			FOR EACH ROW EXECUTE FUNCTION `+CaptureFunctionRow+`('["note"]')`)
		defer setup(t)
		openWantRefusal(t, "shape_t", "note", "id")
	})

	t.Run("a NON-ASCII key name still grades — the family escape mangled", func(t *testing.T) {
		// `escape` octal-escapes every byte >= 0x80, so a column named
		// `café` came back as ["caf\303\251"]\000 and failed the parse
		// identically to the NUL case — a second family, same silent
		// skip. base64 is byte-faithful, and this proves it on the wire
		// rather than in a fixture.
		applyPGSQL(t, dsn, `CREATE TABLE shape_u ("café" BIGINT PRIMARY KEY, note TEXT)`)
		defer applyPGSQL(t, dsn, `DROP TABLE shape_u`)
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"shape_t", "shape_u"}}); err != nil {
			t.Fatalf("Setup with a non-ASCII key column: %v", err)
		}
		defer setup(t)

		// Healthy first: a correctly-keyed non-ASCII column must not be
		// refused. This is the floor — if the grade cannot read the name
		// it would either skip (silently) or false-refuse (loudly).
		openWantClean(t)

		// Then the same table keyed WRONG: the grade has to fire.
		applyPGSQL(t, dsn, `DROP TRIGGER sluice_capture ON shape_u`)
		applyPGSQL(t, dsn, `CREATE TRIGGER sluice_capture
			AFTER INSERT OR UPDATE OR DELETE ON shape_u
			FOR EACH ROW EXECUTE FUNCTION `+CaptureFunctionRow+`('["note"]')`)
		openWantRefusal(t, "shape_u", "note")
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
