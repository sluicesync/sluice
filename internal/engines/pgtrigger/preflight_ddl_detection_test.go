// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// captureLogsForUnitTest redirects the default slog logger into a buffer
// for the duration of fn and returns everything logged. Named distinctly
// from the integration file's captureWarnLogs because the -tags=integration
// build compiles both files into one package.
func captureLogsForUnitTest(fn func()) string {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// TestWarnDDLDetectionAbsent_ProbeErrorAlsoWarns pins the SL-1 probe-
// error discipline for the G1 door: a failed DDL-capture-state read must
// WARN ("cannot rule the blindness out"), never silently skip — a probe
// error falling through to silence is exactly the shape that turned a
// halt into a silent skip in SL-1.
func TestWarnDDLDetectionAbsent_ProbeErrorAlsoWarns(t *testing.T) {
	// A syntactically valid DSN at a closed port: the lazy pool opens,
	// the first query fails, the WARN path runs.
	db, err := sql.Open("pgx", "postgres://u:p@127.0.0.1:1/nope?connect_timeout=1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logs := captureLogsForUnitTest(func() {
		warnDDLDetectionAbsent(ctx, db, "public")
	})
	if !strings.Contains(logs, ddlDetectionAbsentMarker) {
		t.Fatalf("probe error did not WARN with the %s marker (silent skip — the SL-1 shape):\n%s",
			ddlDetectionAbsentMarker, logs)
	}
	if !strings.Contains(logs, "cannot read") {
		t.Errorf("probe-error WARN should say the state could not be read; got:\n%s", logs)
	}
}
