//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F3: a Postgres `timestamp(0)` → `timestamp(6)` ALTER on a running
// stream must forward. The Postgres comparison lens
// (engines/postgres/cdc_normalize.go) used to put bare, (0) and (6) in one
// class, so the boundary classified as no change, the forward never ran,
// and the target kept rounding every following value to whole seconds at
// exit 0. Every temporal family the lens dispatches on, CDC→CDC in one
// process — the first-boundary witness is not involved.
//
// The independent expected value is the target's catalog (format_type) and
// the row read back from it.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStreamer_PGTimestampZeroToSixForwards(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	cell := twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN}, streamID: "gc44-f3"}

	// One table per temporal family the lens collapses; each is (0) and
	// widened to (6).
	families := []struct {
		table, zeroType, sixType, wantType, value, readFmt, want string
	}{
		{
			"f3_ts", "timestamp(0)", "timestamp(6)", "timestamp(6) without time zone",
			"'2026-10-02 10:11:12.345678'", "to_char(v, 'HH24:MI:SS.US')", "10:11:12.345678",
		},
		{
			"f3_tstz", "timestamptz(0)", "timestamptz(6)", "timestamp(6) with time zone",
			"'2026-10-02 10:11:12.345678+00'", "to_char(v AT TIME ZONE 'UTC', 'HH24:MI:SS.US')", "10:11:12.345678",
		},
		{
			"f3_time", "time(0)", "time(6)", "time(6) without time zone",
			"'10:11:12.345678'", "to_char(v, 'HH24:MI:SS.US')", "10:11:12.345678",
		},
		{
			"f3_timetz", "timetz(0)", "timetz(6)", "time(6) with time zone",
			"'10:11:12.345678+00'", "v::text", "10:11:12.345678+00",
		},
	}
	for _, f := range families {
		cell.src.exec(t, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s); INSERT INTO %s VALUES (1, NULL);",
			f.table, f.zeroType, f.table))
	}
	run := startTWFBRun(cell.streamer())
	defer func() { _ = run.stop(t) }()
	for _, f := range families {
		if !cell.tgt.waitRow(t, f.table, 1, run, 180*time.Second) {
			t.Fatalf("cold start never delivered %s (stream: %v)", f.table, run.stop(t))
		}
	}
	cell.waitStreaming(t, run)
	// A row first, so the table's next boundary is CDC→CDC in this process.
	for _, f := range families {
		cell.src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (2, NULL)", f.table))
		if !cell.tgt.waitRow(t, f.table, 2, run, 60*time.Second) {
			t.Fatalf("%s row 2 never landed (stream: %v)", f.table, run.stop(t))
		}
	}
	for _, f := range families {
		cell.src.exec(t, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN v TYPE %s; INSERT INTO %s VALUES (3, %s);",
			f.table, f.sixType, f.table, f.value))
	}
	for _, f := range families {
		if !cell.tgt.waitRow(t, f.table, 3, run, 60*time.Second) {
			t.Fatalf("%s row 3 never landed (stream: %v)", f.table, run.stop(t))
		}
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("stream returned %v", err)
	}
	for _, f := range families {
		if got := cell.tgt.columnType(t, f.table, "v"); got != f.wantType {
			t.Errorf("GC-44 F3: %s.v on the target is %q, want %q — the (0) → (6) ALTER was not forwarded", f.table, got, f.wantType)
		}
		if got := cell.tgt.scalar(t, fmt.Sprintf("SELECT %s FROM %s WHERE id = 3", f.readFmt, f.table)); got != f.want {
			t.Errorf("GC-44 F3: %s row 3 read back %q, want %q", f.table, got, f.want)
		}
	}
}
