//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestControlTimestampAge_PreV01565RowOnATokyoDatabase pins GC-40 (c) on a
// real Postgres: a sluice_cdc_state row written the way every binary before
// v0.156.5 wrote it — updated_at from the session's CURRENT_TIMESTAMP — on a
// database whose zone is Asia/Tokyo lands nine hours in the FUTURE, and the
// age consumers must read it as unknown, not fresh. It also binds the
// premise ControlTimestampSkewTolerance rests on: the old write lands far
// past the 60 s tolerance, not inside it. Then the current binary's first
// position write heals the row, as the release notes promise.
//
// The independent expected value is the test process's own clock, against
// which the database's zone offset (+9h) is known.
func TestControlTimestampAge_PreV01565RowOnATokyoDatabase(t *testing.T) {
	_, dsn, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER DATABASE target_db SET timezone = 'Asia/Tokyo'"); err != nil {
		t.Fatalf("set database zone: %v", err)
	}
	_ = admin.Close()

	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("open applier: %v", err)
	}
	defer func() {
		if c, ok := applier.(io.Closer); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("ensure control table: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var zone string
	if err := db.QueryRowContext(ctx, "SHOW timezone").Scan(&zone); err != nil || zone != "Asia/Tokyo" {
		t.Fatalf("session zone = %q (err %v); the test is not measuring an east-of-UTC database", zone, err)
	}
	// The pre-v0.156.5 write, verbatim in its timestamp: CURRENT_TIMESTAMP
	// into the naive column keeps the session zone's wall-clock digits.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO public.sluice_cdc_state (stream_id, source_position, updated_at) VALUES ('tokyo', 'tok', CURRENT_TIMESTAMP)`); err != nil {
		t.Fatalf("write the old-style row: %v", err)
	}

	row := func() ir.StreamStatus {
		t.Helper()
		streams, err := applier.ListStreams(ctx)
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		for _, s := range streams {
			if s.StreamID == "tokyo" {
				return s
			}
		}
		t.Fatal("stream row not found")
		return ir.StreamStatus{}
	}

	old := row()
	age, readable := ControlTimestampAge(time.Now(), old.UpdatedAt)
	if readable {
		t.Fatalf("old-style row ages %v and reads as readable; want CONTROL-TIMESTAMP-IN-FUTURE", age.Round(time.Second))
	}
	// The premise: the old write is off by the zone's whole offset, far
	// past the tolerance — not a borderline skew.
	if age > -8*time.Hour || age < -10*time.Hour {
		t.Fatalf("old-style row ages %v; want about -9h on Asia/Tokyo", age.Round(time.Second))
	}
	var scrape bytes.Buffer
	emitMetrics(&scrape, []ir.StreamStatus{old}, time.Now())
	if !strings.Contains(scrape.String(), `sluice_seconds_since_last_apply{stream_id="tokyo"} +Inf`) ||
		!strings.Contains(scrape.String(), "# CONTROL-TIMESTAMP-IN-FUTURE") {
		t.Errorf("scrape of the old-style row does not fail closed:\n%s", scrape.String())
	}

	// Self-heal: the current binary's position write stores UTC.
	pw, ok := applier.(ir.PositionWriter)
	if !ok {
		t.Fatal("postgres applier does not implement ir.PositionWriter")
	}
	if err := pw.WritePosition(ctx, "tokyo", ir.Position{Engine: "postgres", Token: "tok2"}); err != nil {
		t.Fatalf("write position: %v", err)
	}
	age, readable = ControlTimestampAge(time.Now(), row().UpdatedAt)
	if !readable || age < -time.Minute || age > time.Minute {
		t.Fatalf("after a current position write the row ages %v (readable %v); want ≈0", age.Round(time.Second), readable)
	}
}
