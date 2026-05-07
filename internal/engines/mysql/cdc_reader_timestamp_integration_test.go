//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 19 regression: silent TIMESTAMP corruption in MySQL CDC.
//
// MySQL's binlog wire format encodes TIMESTAMP as a UTC seconds-since-
// epoch integer. The go-mysql library's row decoder builds a time.Time
// via time.Unix(sec, ...) — the underlying instant is correct, but its
// Location defaults to time.Local. When the parser then formats the
// fracTime to a string (the ParseTime=false path sluice uses), it
// formats in the process's local TZ. The string flows into sluice's
// decodeTime, which parses naked datetime strings as UTC, silently
// shifting the value by the host's offset.
//
// The cold-start (database/sql) path has an analogous risk: if the
// MySQL session's `time_zone` inherits the host TZ (or the server's
// default_time_zone is set to something non-UTC), MySQL converts
// TIMESTAMP to that TZ for the wire format and the driver — running
// with cfg.Loc=UTC — reinterprets it as UTC, producing the same kind
// of silent corruption.
//
// The fix has two halves:
//
//   - cdc_reader.go sets BinlogSyncerConfig.TimestampStringLocation =
//     time.UTC, forcing the binlog parser to format TIMESTAMP strings
//     in UTC regardless of the host's local TZ.
//   - connect.go injects `time_zone='+00:00'` into cfg.Params so every
//     database/sql connection runs `SET time_zone='+00:00'` after the
//     handshake, forcing MySQL to emit TIMESTAMP wire values in UTC
//     regardless of the server's default_time_zone.
//
// This test guards both halves: it forces the test process's local
// TZ to America/Los_Angeles (a non-UTC zone), inserts a TIMESTAMP,
// reads it back via both the cold-start RowReader and the CDC reader,
// and asserts both paths return the same UTC instant the SQL literal
// describes.

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestCDCReader_TimestampNonUTCHost is the Bug 19 regression. It
// switches time.Local to a non-UTC zone for the duration of the test,
// inserts a known TIMESTAMP, and asserts the value comes back as the
// correct UTC instant from both the cold-start path and the CDC
// stream. Without the fix the CDC value drifts by the host's UTC
// offset (7h on PT in DST, 8h off DST).
func TestCDCReader_TimestampNonUTCHost(t *testing.T) {
	// Pin time.Local to PT for the duration of this test. time.Unix
	// in the go-mysql binlog decoder uses time.Local; without this
	// override the test would only catch the bug on machines whose
	// real local TZ happens to be non-UTC. Restore on cleanup so
	// later tests in the same process see the original value.
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("LoadLocation America/Los_Angeles: %v (tzdata unavailable)", err)
	}
	originalLocal := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = originalLocal })

	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE events (
			id          BIGINT NOT NULL AUTO_INCREMENT,
			occurred_at TIMESTAMP NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

	// The expected UTC instant for the SQL literal '2026-05-05 17:20:13'
	// when the connection's session time_zone is UTC. With sluice's
	// time_zone='+00:00' fix, every database/sql connection reads/writes
	// TIMESTAMP in UTC regardless of the host or server TZ, so this
	// literal gets stored as exactly this instant.
	want := time.Date(2026, 5, 5, 17, 20, 13, 0, time.UTC)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Insert the row using applyMySQL. Important: the bare sql.Open
	// in applyMySQL does NOT go through parseDSN, so its session has
	// the server's default time_zone (UTC in the testcontainer). To
	// guarantee the stored value matches `want` exactly we set
	// time_zone explicitly on the seed connection; the production
	// path's parseDSN does the same automatically.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		INSERT INTO events (id, occurred_at) VALUES (1, '2026-05-05 17:20:13');
	`)

	// ---- Cold-start (RowReader) ----

	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "events")
	if table == nil {
		t.Fatalf("events table not found in schema")
	}

	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(interface{ Close() error }).Close() }()

	rowCh, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var coldRows []ir.Row
	for row := range rowCh {
		coldRows = append(coldRows, row)
	}
	if rrCloser, ok := rr.(*RowReader); ok {
		if err := rrCloser.Err(); err != nil {
			t.Fatalf("RowReader.Err: %v", err)
		}
	}
	if len(coldRows) != 1 {
		t.Fatalf("cold-start: got %d rows; want 1", len(coldRows))
	}
	gotCold, ok := coldRows[0]["occurred_at"].(time.Time)
	if !ok {
		t.Fatalf("cold-start: occurred_at = %#v; want time.Time", coldRows[0]["occurred_at"])
	}
	if !gotCold.Equal(want) {
		t.Errorf("cold-start: occurred_at = %v (unix=%d); want %v (unix=%d) — Bug 19 cold-start regression",
			gotCold, gotCold.Unix(), want, want.Unix())
	}

	// ---- CDC stream ----

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(interface{ Close() error }).Close() }()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Settle: let the syncer register at "now" before triggering the
	// row event. 200ms matches other CDC integration tests.
	time.Sleep(200 * time.Millisecond)

	// Insert a fresh row carrying the same TIMESTAMP value. We use
	// INSERT rather than UPDATE-to-same-value because MySQL skips
	// binlog row events for updates whose column values don't
	// actually change. INSERT always produces a write_rows event.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		INSERT INTO events (id, occurred_at) VALUES (2, '2026-05-05 17:20:13');
	`)

	got := drainChanges(t, ctx, changes, 1, 30*time.Second)
	if len(got) != 1 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("CDC: got %d events; stream error: %v", len(got), streamErr)
			}
		}
		t.Fatalf("CDC: got %d events; want 1", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("CDC: change[0] = %T; want ir.Insert", got[0])
	}
	gotCDC, ok := ins.Row["occurred_at"].(time.Time)
	if !ok {
		t.Fatalf("CDC: Row[occurred_at] = %#v; want time.Time", ins.Row["occurred_at"])
	}
	if !gotCDC.Equal(want) {
		// Surface the offset for the bug report — silent CDC
		// corruption is the worst kind sluice can produce, so the
		// failure message is verbose on purpose.
		offset := want.Unix() - gotCDC.Unix()
		t.Errorf("CDC: Row[occurred_at] = %v (unix=%d); want %v (unix=%d); offset=%ds — Bug 19 regression",
			gotCDC, gotCDC.Unix(), want, want.Unix(), offset)
	}

	// And finally: cold-start and CDC must agree. The whole point
	// of the bug report was that they didn't.
	if !gotCold.Equal(gotCDC) {
		t.Errorf("cold-start instant %v != CDC instant %v — Bug 19 cross-path divergence",
			gotCold, gotCDC)
	}
}
