//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_SessionTZSwap_RefuseMode_MySQL is the audit 2026-09-06 H5
// fix, measured on a real MySQL 8 source at +09:00 before it was written.
//
// THE DEFECT, and it is the inverse of what the flag name promises.
// `--schema-changes=refuse` is documented on [Streamer.SchemaChanges] as
// "any source DDL refuses loudly (the conservative pre-ADR-0091
// behavior)". On a MySQL source that sentence was FALSE, and false in the
// direction that loses data: the session-time_zone cast refusal was armed
// on `schemaDeltaAppliesToTarget`, which is
// `singleStreamSchemaForwardActive() || shapeA` — and
// singleStreamSchemaForwardActive requires forwardSchemaEnabled(), which
// is precisely what refuse mode turns OFF. So the MOST conservative
// setting was the ONLY one with no session-zone door.
//
// It was armed by "does a forward path exist" rather than by the harm,
// and the harm set of a source zone swap has two members:
//
//   - FORWARDED: the target's pre-existing rows are re-cast against the
//     target session's zone (the shape SLM-1 pinned);
//   - NOT FORWARDED: the target column keeps the OLD type while every
//     post-boundary row arrives under the NEW one, so the target's own
//     rows disagree with each other by the session offset.
//
// Only the first needs a forward path. The second is what refuse mode
// does, and refuse mode had no door.
//
// MEASURED BEFORE FIXING (MySQL 8, --default-time-zone=+09:00, this
// harness): with SchemaChanges="refuse" the stream surfaced NO error in
// 90 s, stayed alive, and kept applying. The audit graded this
// UNVERIFIED PREMISE; it is now verified.
//
// PG IS NOT AFFECTED and the asymmetry is worth recording: the Postgres
// reader refuses relationChangeAlterColumnType under refuse mode at the
// reader (cdc_relations.go, Bug 112/119/120 preserved), independent of
// any arming flag. The MySQL reader carried exactly one coded refusal
// (CodeCDCXAUnsupported) and no schema refusal at all, so the doc's
// claim held for one engine of two.
func TestStreamer_SessionTZSwap_RefuseMode_MySQL(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	sourceDSN, targetDSN, cleanup := startMySQLBinlogAtTokyo(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE events (
			id BIGINT NOT NULL PRIMARY KEY,
			c  DATETIME NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO events (id, c) VALUES
			(1, '2020-01-01 21:00:00'),
			(2, '2020-01-01 21:00:00'),
			(3, '2020-01-01 21:00:00');
	`)

	streamer := &Streamer{
		Source:        myEng,
		Target:        myEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		StreamID:      "test-h5-refuse",
		SchemaChanges: "refuse",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "events", 3, 60*time.Second) {
		t.Fatalf("bulk-copy never landed the seed rows")
	}

	// The swap, from a session inheriting the server's +09:00 default,
	// followed by a row the stream would apply if it stayed alive.
	applyDDLMySQL(t, sourceDSN, "ALTER TABLE events MODIFY c TIMESTAMP NOT NULL;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO events (id, c) VALUES (4, '2020-01-01 21:00:00');")

	var streamErr error
	select {
	case streamErr = <-runErr:
	case <-time.After(90 * time.Second):
		t.Fatal("--schema-changes=refuse surfaced NO refusal in 90s on a zone-sibling swap. " +
			"This is the exact configuration whose documented contract is that ANY source DDL " +
			"refuses loudly, and it is the only mode with no session-zone door: the refusal is " +
			"armed on schemaDeltaAppliesToTarget, which refuse mode turns off. Every row after " +
			"this boundary lands in a target column of the OTHER zone family, so the target's own " +
			"rows disagree by the session offset at exit 0.")
	}
	if streamErr == nil {
		t.Fatal("streamer returned nil on a DATETIME→TIMESTAMP swap under --schema-changes=refuse; " +
			"want the session-time_zone cast refusal")
	}
	for _, want := range []string{"cannot be forwarded", `column "c"`, "time_zone", "drained model"} {
		if !strings.Contains(streamErr.Error(), want) {
			t.Errorf("stream error missing %q; got: %v", want, streamErr)
		}
	}

	// The refusal must fire BEFORE the post-boundary row is applied, or
	// it reports the divergence rather than preventing it. Read both
	// sides under an explicit +00:00 so neither number depends on the
	// reader's session.
	assertRefuseModeTargetUndiverged(t, sourceDSN, targetDSN)

	streamCancel()
}

// assertRefuseModeTargetUndiverged pins that the target still holds the
// three copied wall-clock rows and NOT the post-swap row, and that the
// source's swap really happened (without which the whole test is
// vacuous — a swap that did not occur cannot diverge anything).
func assertRefuseModeTargetUndiverged(t *testing.T, sourceDSN, targetDSN string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	open := func(dsn string) *sql.DB {
		t.Helper()
		db, err := sql.Open("mysql", dsn+"&time_zone=%27%2B00%3A00%27")
		if err != nil {
			t.Fatalf("open %s: %v", dsn, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	src, tgt := open(sourceDSN), open(targetDSN)

	// ANTI-VACUITY: the source ALTER must actually have converted the
	// stored values. Read under +00:00 the swapped source rows are
	// 12:00:00 — the ALTER resolved 21:00 Tokyo as 12:00 UTC. If this
	// reads 21:00 the swap did not happen and nothing below means
	// anything.
	var srcVal string
	if err := src.QueryRowContext(ctx, `SELECT DATE_FORMAT(c, '%H:%i:%s') FROM events WHERE id = 1`).Scan(&srcVal); err != nil {
		t.Fatalf("read source row 1: %v", err)
	}
	if srcVal != "12:00:00" {
		t.Fatalf("anti-vacuity: source row 1 reads %q under +00:00, want 12:00:00 — the ALTER did not "+
			"perform the zone conversion this test exists to catch, so the whole cell is vacuous", srcVal)
	}

	// The target keeps the copied wall clock, unconverted.
	var tgtVal string
	if err := tgt.QueryRowContext(ctx, `SELECT DATE_FORMAT(c, '%H:%i:%s') FROM events WHERE id = 1`).Scan(&tgtVal); err != nil {
		t.Fatalf("read target row 1: %v", err)
	}
	if tgtVal != "21:00:00" {
		t.Errorf("target row 1 reads %q, want the copied wall clock 21:00:00 — something re-cast the "+
			"pre-existing rows despite the refusal", tgtVal)
	}

	// And the post-boundary row never landed. If it had, the target
	// would hold 12:00 beside three 21:00 rows: internally inconsistent
	// by the session offset, which is the harm.
	var n int
	if err := tgt.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = 4`).Scan(&n); err != nil {
		t.Fatalf("count target row 4: %v", err)
	}
	if n != 0 {
		t.Errorf("the post-swap row landed on the target despite the refusal (%d row(s) with id=4) — "+
			"the refusal fired after the apply, so it reports the divergence rather than preventing it", n)
	}
}
