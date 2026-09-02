//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL lane's session-`time_zone` cast refusal against a real server
// (audit 2026-08-31 SL-2).
//
// Two tests, and the first is the one that matters most: the refusal's
// whole justification is an engine-behaviour claim about the world outside
// sluice — "MySQL resolves a TIMESTAMP⇄DATETIME MODIFY using the executing
// session's time_zone" — and CLAUDE.md's premise-naming step says a safety
// argument citing an environmental fact owes that fact a check. So the
// premise is measured on the container rather than asserted in a comment,
// and the refusal is then pinned on a live binlog stream.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMySQLSessionTZCast_PremiseHoldsOnTheServer measures the mechanism
// the refusal exists for: the same stored TIMESTAMP, MODIFYed to DATETIME
// from two sessions whose `time_zone` differs, ends up as two different
// wall-clock values. If MySQL ever stopped doing this the refusal would be
// over-refusal and this test is where that shows up — the alternative is a
// comment nobody re-checks.
//
// The independent expected value is not sluice's: it is the UTC offset the
// two sessions declare (+09:00 vs +00:00 = 9h), asserted against the
// difference the server actually produced.
func TestMySQLSessionTZCast_PremiseHoldsOnTheServer(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	// Both rows start as the IDENTICAL stored instant, written at UTC.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		CREATE TABLE tz_at_tokyo (id INT PRIMARY KEY, c TIMESTAMP NOT NULL);
		CREATE TABLE tz_at_utc   (id INT PRIMARY KEY, c TIMESTAMP NOT NULL);
		INSERT INTO tz_at_tokyo VALUES (1, '2020-01-01 12:00:00');
		INSERT INTO tz_at_utc   VALUES (1, '2020-01-01 12:00:00');
	`)

	// The ONLY difference between the two ALTERs is the executing
	// session's zone. Neither statement mentions a zone.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+09:00';
		ALTER TABLE tz_at_tokyo MODIFY c DATETIME NOT NULL;
	`)
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		ALTER TABLE tz_at_utc MODIFY c DATETIME NOT NULL;
	`)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// DATETIME is wall-clock, so the reading session's zone is irrelevant
	// to what comes back — the divergence is frozen into storage.
	var diffSeconds int64
	if err := db.QueryRowContext(ctx, `
		SELECT TIMESTAMPDIFF(SECOND,
			(SELECT c FROM tz_at_utc   WHERE id = 1),
			(SELECT c FROM tz_at_tokyo WHERE id = 1))`).Scan(&diffSeconds); err != nil {
		t.Fatalf("measure divergence: %v", err)
	}
	const wantSeconds = 9 * 3600
	if diffSeconds != wantSeconds {
		var tokyo, utc string
		_ = db.QueryRowContext(ctx, `SELECT c FROM tz_at_tokyo WHERE id = 1`).Scan(&tokyo)
		_ = db.QueryRowContext(ctx, `SELECT c FROM tz_at_utc WHERE id = 1`).Scan(&utc)
		t.Fatalf("MODIFY-at-+09:00 produced %q and MODIFY-at-UTC produced %q — a %ds difference, want %ds. "+
			"The session-time_zone cast refusal's PREMISE no longer holds on this server; the refusal is now "+
			"over-refusal and both the code comment and ADR-0091's impl note must be revisited",
			tokyo, utc, diffSeconds, wantSeconds)
	}
}

// TestCDCReader_SessionTZCastRefusesOnALiveStream drives the binlog reader
// over a real MODIFY and asserts the stream dies loudly with the
// session-time_zone refusal rather than emitting the boundary a forward
// path would act on.
//
// The prime-then-swap shape pins the SECOND boundary a table sees — the
// reader's own last-emitted snapshot as prev. It used to be described
// here as "the production window"; it is not (SLM-1, audit 2026-09-01):
// the first boundary after a start is the one Shape A forwards, and
// [TestCDCReader_SessionTZCastRefusesAtTheFirstBoundary] below pins that
// one with no priming at all.
func TestCDCReader_SessionTZCastRefusesOnALiveStream(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		CREATE TABLE events (
			id          BIGINT NOT NULL AUTO_INCREMENT,
			occurred_at TIMESTAMP NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO events (id, occurred_at) VALUES (1, '2020-01-01 12:00:00');
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdr)
	}
	// Arm exactly as the streamer does when a forward path is live.
	cdc.SetSchemaForward(true)
	cdc.SetSchemaDeltaAppliesToTarget(true)
	defer func() { _ = cdc.Close() }()

	changes, err := cdc.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// PRIME: a harmless ADD COLUMN + a row, so the reader holds a real
	// pre-state signature for `events`.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		ALTER TABLE events ADD COLUMN note VARCHAR(16) NULL;
		INSERT INTO events (id, occurred_at, note) VALUES (2, '2020-01-01 12:00:00', 'p');
	`)

	// THE SWAP, run from a non-UTC session exactly as an operator's
	// 2038-remediation MODIFY would be.
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+09:00';
		ALTER TABLE events MODIFY occurred_at DATETIME NOT NULL;
		INSERT INTO events (id, occurred_at, note) VALUES (3, '2020-01-01 12:00:00', 'x');
	`)

	// The pump closes `changes` when it dies. Drain to completion, then
	// read the terminal error.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range changes {
		}
	}()
	select {
	case <-drained:
	case <-time.After(60 * time.Second):
		t.Fatal("stream did not terminate within 60s — the session-time_zone MODIFY was forwarded instead of refused")
	}

	streamErr := cdc.Err()
	if streamErr == nil {
		t.Fatal("stream ended with no error; the TIMESTAMP→DATETIME MODIFY must refuse loudly (a forwarded MODIFY re-casts every pre-existing target row against a different session's time_zone)")
	}
	for _, want := range []string{
		"cannot be forwarded", `column "occurred_at"`,
		"TIMESTAMP and DATETIME", "time_zone", "drained model",
	} {
		if !strings.Contains(streamErr.Error(), want) {
			t.Errorf("stream error missing %q; got: %v", want, streamErr)
		}
	}
}

// TestCDCReader_PrecisionOnlyModifyStillForwards is the no-over-refusal
// FLOOR on a real server: DATETIME(3) → DATETIME(6) carries no zone
// conversion, so the boundary must still be emitted and the stream must
// stay alive. A predicate broadened to "any temporal MODIFY" fails here.
func TestCDCReader_PrecisionOnlyModifyStillForwards(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		CREATE TABLE ticks (
			id BIGINT NOT NULL AUTO_INCREMENT,
			at DATETIME(3) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO ticks (id, at) VALUES (1, '2020-01-01 12:00:00.123');
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdr)
	}
	cdc.SetSchemaForward(true)
	cdc.SetSchemaDeltaAppliesToTarget(true)
	defer func() { _ = cdc.Close() }()

	changes, err := cdc.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+00:00';
		ALTER TABLE ticks ADD COLUMN note VARCHAR(16) NULL;
		INSERT INTO ticks (id, at, note) VALUES (2, '2020-01-01 12:00:00.123', 'p');
	`)
	applyMySQL(t, dsn, `
		SET SESSION time_zone = '+09:00';
		ALTER TABLE ticks MODIFY at DATETIME(6) NOT NULL;
		INSERT INTO ticks (id, at, note) VALUES (3, '2020-01-01 12:00:00.123456', 'x');
	`)

	// Two DML events are expected on the stream (id=2 primes, id=3 is the
	// post-ALTER row); id=1 predates StreamChanges. The id=3 arrival is the
	// assertion — the boundary forwarded and the stream stayed alive.
	got := drainChanges(t, ctx, changes, 2, 60*time.Second)
	if err := cdc.Err(); err != nil {
		t.Fatalf("precision-only DATETIME(3)→DATETIME(6) killed the stream: %v", err)
	}
	sawPostAlterRow := false
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		if id, ok := ins.Row["id"].(int64); ok && id == 3 {
			sawPostAlterRow = true
		}
	}
	if !sawPostAlterRow {
		t.Errorf("post-ALTER row id=3 never arrived (%d changes seen) — the precision-only MODIFY did not forward", len(got))
	}
}

// TestCDCReader_SessionTZCastRefusesAtTheFirstBoundary is the SLM-1 pin
// on the binlog lane: seeded exactly as the streamer seeds it (the
// SchemaReader's raw IR, handed through SetSchemaSeed before
// StreamChanges), the zone-sibling swap as the FIRST DDL the stream sees
// — no priming ADD COLUMN — refuses. Both directions, because the seed
// side and the boundary side come from two different projections
// (information_schema through the SchemaReader vs. through the CDC
// loader) and a mismatch in either would show up on one direction only.
//
// The server's default time_zone is +09:00 and the ALTER session sets
// nothing — the operator shape the audit observed, not a SET.
func TestCDCReader_SessionTZCastRefusesAtTheFirstBoundary(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t, "--default-time-zone=+09:00")
	defer cleanup()

	for _, tc := range []struct{ name, from, to string }{
		{"DATETIME→TIMESTAMP", "DATETIME", "TIMESTAMP"},
		{"TIMESTAMP→DATETIME", "TIMESTAMP", "DATETIME"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "first_" + strings.ToLower(tc.from)
			applyMySQL(t, dsn, `
				DROP TABLE IF EXISTS `+table+`;
				CREATE TABLE `+table+` (
					id BIGINT NOT NULL AUTO_INCREMENT,
					c  `+tc.from+` NOT NULL,
					PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
				INSERT INTO `+table+` (id, c) VALUES (1, '2020-01-01 21:00:00');
			`)

			eng := Engine{Flavor: FlavorVanilla}
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			// The seed: the raw source IR, as the streamer captures it.
			sr, err := eng.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			schema, err := sr.ReadSchema(ctx)
			if closer, ok := sr.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}

			rdr, err := eng.OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			cdc, ok := rdr.(*CDCReader)
			if !ok {
				t.Fatalf("OpenCDCReader returned %T; want *CDCReader", rdr)
			}
			cdc.SetSchemaForward(true)
			cdc.SetSchemaDeltaAppliesToTarget(true)
			cdc.SetSchemaSeed(schema.Tables)
			defer func() { _ = cdc.Close() }()

			changes, err := cdc.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			time.Sleep(200 * time.Millisecond)

			// THE SWAP, first and only DDL, from a session inheriting the
			// server's +09:00 default.
			applyMySQL(t, dsn, `
				ALTER TABLE `+table+` MODIFY c `+tc.to+` NOT NULL;
				INSERT INTO `+table+` (id, c) VALUES (2, '2020-01-01 21:00:00');
			`)

			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for range changes {
				}
			}()
			select {
			case <-drained:
			case <-time.After(60 * time.Second):
				t.Fatal("stream did not terminate within 60s — the first-boundary swap was forwarded instead of refused (SLM-1)")
			}
			streamErr := cdc.Err()
			if streamErr == nil {
				t.Fatal("stream ended with no error; the swap at the FIRST boundary must refuse loudly")
			}
			for _, want := range []string{"cannot be forwarded", `column "c"`, "TIMESTAMP and DATETIME", "time_zone", "drained model"} {
				if !strings.Contains(streamErr.Error(), want) {
					t.Errorf("stream error missing %q; got: %v", want, streamErr)
				}
			}
		})
	}
}
