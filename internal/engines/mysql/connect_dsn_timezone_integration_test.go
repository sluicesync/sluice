//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestSessionTimeZone_NonUTCShiftsTimestamp_AndTheDSNDoorRefusesIt binds
// GC-39 item 1's refusal to the premise it rests on, on a real server.
//
// The premise: with sluice's driver configuration (ParseTime, Loc = UTC)
// and a non-UTC SESSION zone, a TIMESTAMP is read and written shifted by
// the zone's offset while DATETIME is not. The door: parseDSN refuses a
// DSN that would produce that session, and a UTC spelling passes and reads
// exactly. If the premise ever stopped holding the first half fails and
// the refusal can be reconsidered; if the door is removed the second half
// fails. The independent expected value is the literal instant the seed
// statement wrote under an explicit `+00:00` session, read back through a
// plain driver connection that is not sluice's configuration.
func TestSessionTimeZone_NonUTCShiftsTimestamp_AndTheDSNDoorRefusesIt(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applyMySQLApplier(t, dsn, "SET time_zone='+00:00'; "+
		"CREATE TABLE tz_door (id INT PRIMARY KEY, ts TIMESTAMP(0) NULL, dt DATETIME(0) NULL); "+
		"INSERT INTO tz_door VALUES (1, '2026-01-01 12:00:00', '2026-01-01 12:00:00');")
	stored := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Premise: bypass the door and run sluice's config under +09:00.
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	cfg.Params["time_zone"] = "'+09:00'"
	shifted, err := openDB(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("openDB under +09:00: %v", err)
	}
	var ts, dt time.Time
	if err := shifted.QueryRowContext(ctx, "SELECT ts, dt FROM tz_door WHERE id = 1").Scan(&ts, &dt); err != nil {
		t.Fatalf("read under +09:00: %v", err)
	}
	if d := ts.Sub(stored); d != 9*time.Hour {
		t.Errorf("premise: TIMESTAMP read under a +09:00 session is off by %v; the refusal rests on it being off by the offset (9h)", d)
	}
	if !dt.Equal(stored) {
		t.Errorf("premise: DATETIME read under +09:00 = %v; want the naive %v unchanged", dt, stored)
	}
	if _, err := shifted.ExecContext(ctx, "INSERT INTO tz_door VALUES (2, ?, ?)", stored, stored); err != nil {
		t.Fatalf("write under +09:00: %v", err)
	}
	_ = shifted.Close()
	plain, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	defer func() { _ = plain.Close() }()
	plain.SetMaxOpenConns(1)
	if _, err := plain.ExecContext(ctx, "SET time_zone = '+00:00'"); err != nil {
		t.Fatalf("pin plain session to UTC: %v", err)
	}
	var back string
	if err := plain.QueryRowContext(ctx, "SELECT CAST(ts AS CHAR) FROM tz_door WHERE id = 2").Scan(&back); err != nil {
		t.Fatalf("read back write: %v", err)
	}
	if back != "2026-01-01 03:00:00" {
		t.Errorf("premise: a 12:00Z TIMESTAMP written under +09:00 stored as %q; want the 9h-shifted 03:00:00", back)
	}

	// The door.
	if _, err := parseDSN(dsn + "&time_zone=%27%2B09%3A00%27"); err == nil || !strings.Contains(err.Error(), "DSN-TIME-ZONE-NOT-UTC") {
		t.Fatalf("parseDSN with time_zone='+09:00' = %v; want the DSN-TIME-ZONE-NOT-UTC refusal", err)
	}
	for _, spelling := range []string{"", "&time_zone=%27%2B00%3A00%27", "&time_zone=%27-00%3A00%27"} {
		cfg, err := parseDSN(dsn + spelling)
		if err != nil {
			t.Fatalf("parseDSN(%q): %v", spelling, err)
		}
		db, err := openDB(ctx, cfg, nil)
		if err != nil {
			t.Fatalf("openDB(%q): %v", spelling, err)
		}
		if err := db.QueryRowContext(ctx, "SELECT ts FROM tz_door WHERE id = 1").Scan(&ts); err != nil {
			t.Fatalf("read (%q): %v", spelling, err)
		}
		if !ts.Equal(stored) {
			t.Errorf("TIMESTAMP read through an accepted DSN (%q) = %v; want %v exactly", spelling, ts.UTC(), stored)
		}
		_ = db.Close()
	}
}
