// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the capture-shape door ([verifyCaptureTriggerShape]): the CDC
// reader refuses at stream start when an installed capture trigger's body is
// not what THIS binary's setup renders — the stale-install case (an older
// sluice's lossy format('%.17g') REAL capture, silently altered floats on
// SQLite ≥ 3.43) and the dropped-trigger case (an operation silently absent
// from the stream). Both lanes: the local file executor here, the D1
// transport in d1_cdc_test.go (TestD1CDC_StaleCaptureBodyRefused).

package sqlitetrigger

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// seedCaptureShapeSource writes a temp SQLite file with one table and a full
// trigger install, returning its path.
func seedCaptureShapeSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shape.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		`CREATE TABLE ev (id INTEGER PRIMARY KEY, r REAL, note TEXT)`,
		`INSERT INTO ev VALUES (1, 0.5, 'seed')`,
	} {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	if _, err := Setup(context.Background(), path, SetupOptions{Tables: []string{"ev"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return path
}

// rewriteTrigger replaces one installed trigger's body on the source file.
func rewriteTrigger(t *testing.T, path, name, createSQL string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for rewrite: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), `DROP TRIGGER "`+name+`"`); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if createSQL != "" {
		if _, err := db.ExecContext(context.Background(), createSQL); err != nil {
			t.Fatalf("recreate %s: %v", name, err)
		}
	}
}

// TestCDCOpen_FreshSetupPassesCaptureShapeDoor is the positive arm: a source
// that setup just installed opens cleanly (the door compares byte-identical
// bodies).
func TestCDCOpen_FreshSetupPassesCaptureShapeDoor(t *testing.T) {
	path := seedCaptureShapeSource(t)
	r, err := openCDCReader(context.Background(), path)
	if err != nil {
		t.Fatalf("openCDCReader after a fresh setup must pass the capture-shape door: %v", err)
	}
	_ = r.(interface{ Close() error }).Close()
}

// TestCDCOpen_RefusesStaleCaptureTriggerBody pins the stale-install refusal:
// a trigger body carrying the PRE-FIX lossy REAL rendering (format('%.17g'))
// — exactly what a v0.99.148..v0.131.x install left behind — must refuse at
// open with the re-setup remedy, never stream silently-altered floats.
func TestCDCOpen_RefusesStaleCaptureTriggerBody(t *testing.T) {
	path := seedCaptureShapeSource(t)

	// Reconstruct the OLD install's body from the current renderer: the only
	// difference between the pre-fix and current triggers is the REAL arm's
	// format spelling.
	expected := captureTriggerCreateSQL("ev", []string{"id", "r", "note"})
	name := triggerName("ev", "upd")
	stale := strings.ReplaceAll(expected[name], "%!.20g", "%.17g")
	if stale == expected[name] {
		t.Fatal("stale-body construction is vacuous: the rewrite changed nothing (the renderer no longer uses %!.20g?)")
	}
	rewriteTrigger(t, path, name, stale)

	_, err := openCDCReader(context.Background(), path)
	if err == nil {
		t.Fatal("openCDCReader accepted a stale (lossy 17g-rendered) capture trigger body — the capture-shape door is not firing")
	}
	for _, wantSub := range []string{name, "trigger setup", "older sluice"} {
		if !strings.Contains(err.Error(), wantSub) {
			t.Errorf("stale-body refusal does not name %q:\n%v", wantSub, err)
		}
	}
}

// TestCDCOpen_RefusesMissingCaptureTrigger pins the dropped-trigger refusal:
// a manually dropped capture trigger means that operation's changes silently
// vanish from the stream — the door must refuse at open instead. (Before the
// door existed, this case had NO guard at all: the fingerprint drift check
// only compares column sets.)
func TestCDCOpen_RefusesMissingCaptureTrigger(t *testing.T) {
	path := seedCaptureShapeSource(t)
	name := triggerName("ev", "del")
	rewriteTrigger(t, path, name, "") // drop, no recreate

	_, err := openCDCReader(context.Background(), path)
	if err == nil {
		t.Fatal("openCDCReader accepted a source whose DELETE capture trigger is missing — deletes would silently vanish from the stream")
	}
	for _, wantSub := range []string{name, "MISSING", "trigger setup"} {
		if !strings.Contains(err.Error(), wantSub) {
			t.Errorf("missing-trigger refusal does not name %q:\n%v", wantSub, err)
		}
	}
}
