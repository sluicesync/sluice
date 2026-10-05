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

	"github.com/go-sql-driver/mysql"
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
	// The post-connect check (sessionInvariantsConnector) refuses this session —
	// the independent door, reached here because the parser was bypassed.
	if _, err := openDB(ctx, cfg, nil); err == nil || !strings.Contains(err.Error(), "DSN-TIME-ZONE-NOT-UTC") {
		t.Errorf("openDB with a +09:00 session that slipped the DSN parser = %v; want the post-connect DSN-TIME-ZONE-NOT-UTC refusal", err)
	}
	// The premise itself: sluice's driver config without that door.
	rawConnector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	shifted := sql.OpenDB(rawConnector)
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
	// The review's F1 race, end to end: a scoped key the parser did not
	// know used to reach the server beside the injected '+00:00' in random
	// map order, leaving a fraction of the pool on +09:00. Planted past the
	// parser, every fresh connection must now either be refused or be UTC.
	cfg, err = parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	cfg.Params["@@local.time_zone"] = "'+09:00'"
	connector, err := mysql.NewConnector(stripVStreamParams(cfg))
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	pool := sql.OpenDB(sessionInvariantsConnector{connector})
	pool.SetMaxIdleConns(0) // every Conn below is a fresh physical connection
	refused := 0
	for i := 0; i < 30; i++ {
		c, err := pool.Conn(ctx)
		if err != nil {
			if !strings.Contains(err.Error(), "DSN-TIME-ZONE-NOT-UTC") {
				t.Fatalf("fresh connection %d: %v", i, err)
			}
			refused++
			continue
		}
		var zone string
		if err := c.QueryRowContext(ctx, "SELECT @@session.time_zone").Scan(&zone); err != nil {
			t.Fatalf("read zone: %v", err)
		}
		if !sessionTimeZoneIsUTC(zone) {
			t.Errorf("fresh connection %d was handed out on session zone %q", i, zone)
		}
		_ = c.Close()
	}
	_ = pool.Close()
	if refused == 0 {
		// Each connection lands on +09:00 with roughly even odds (Go map
		// order), so zero in 30 means the planted key never reached a
		// session and this cell graded nothing.
		t.Error("no fresh connection was refused: the planted @@local.time_zone never reached a session — vacuous cell")
	}
	t.Logf("racy-key pool: %d of 30 fresh connections refused, the rest verified UTC", refused)

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
