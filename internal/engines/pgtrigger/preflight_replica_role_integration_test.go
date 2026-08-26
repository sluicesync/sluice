//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the replica-role capture-blindness preflight
// (audit 2026-08-26 F1, preflight_replica_role.go). Two halves:
//
//  1. The MECHANISM, pinned honestly on real PG: DML executed under
//     `session_replication_role = replica` does NOT fire the plain capture
//     triggers — the row lands in the user table and NO change-log row
//     exists for it. This is the silent loss the preflight exists to name;
//     if PG ever changes this behaviour (or setup moves to ENABLE ALWAYS
//     triggers), this pin fails and the preflight's premise is re-examined.
//  2. The PREFLIGHT: the WARN fires for the subscriber shape (a
//     pg_subscription row) at BOTH call sites (Setup and stream open), fires
//     for the relay shape (sluice's own sluice_cdc_state on the source), and
//     does NOT fire on a clean source (no false alarm).

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

// captureWarnLogs redirects the default slog logger into a buffer for the
// duration of fn and returns everything logged.
func captureWarnLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

func TestReplicaRoleWrites_InvisibleToCapture_AndPreflightWarns(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE relay_t (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"relay_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	changeLogCount := func() int64 {
		var n int64
		if err := db.QueryRowContext(ctx,
			"SELECT count(*) FROM public.sluice_change_log WHERE table_name = 'relay_t'").Scan(&n); err != nil {
			t.Fatalf("count change log: %v", err)
		}
		return n
	}

	t.Run("replica-role DML bypasses the capture trigger", func(t *testing.T) {
		// SET session_replication_role is session-scoped, so the INSERT must
		// ride the SAME connection. The container user is superuser, so the
		// SET succeeds — same as sluice's own privileged applier.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "SET session_replication_role = replica"); err != nil {
			t.Fatalf("SET replica role: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO relay_t VALUES (1, 'applied-under-replica-role')"); err != nil {
			t.Fatalf("replica-role INSERT: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "SET session_replication_role = DEFAULT"); err != nil {
			t.Fatalf("reset role: %v", err)
		}

		var rows int64
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM relay_t").Scan(&rows); err != nil {
			t.Fatalf("count relay_t: %v", err)
		}
		if rows != 1 {
			t.Fatalf("relay_t rows = %d; want 1 (the replica-role INSERT itself must succeed)", rows)
		}
		if got := changeLogCount(); got != 0 {
			t.Fatalf("change-log rows for relay_t = %d; want 0 — the capture trigger fired under replica role, "+
				"so the F1 blindness premise no longer holds and the preflight WARN should be re-examined", got)
		}

		// Differential: the same INSERT under the ORIGIN role IS captured —
		// proving the trigger works and the role is the only variable.
		if _, err := db.ExecContext(ctx, "INSERT INTO relay_t VALUES (2, 'origin-write')"); err != nil {
			t.Fatalf("origin INSERT: %v", err)
		}
		if got := changeLogCount(); got != 1 {
			t.Fatalf("change-log rows after origin INSERT = %d; want 1", got)
		}
	})

	t.Run("clean source: no WARN", func(t *testing.T) {
		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if strings.Contains(logs, captureGapRiskMarker) {
			t.Fatalf("capture-gap WARN fired on a clean source (false alarm):\n%s", logs)
		}
	})

	t.Run("subscriber shape WARNs at setup and at stream open", func(t *testing.T) {
		// connect=false creates the pg_subscription catalog row without
		// dialing the (nonexistent) publisher — the catalog shape is what the
		// preflight grades.
		applyPGSQL(t, dsn, `CREATE SUBSCRIPTION relay_sub CONNECTION 'host=nowhere.invalid dbname=nope' PUBLICATION relay_pub WITH (connect = false)`)
		defer func() {
			applyPGSQL(t, dsn, `ALTER SUBSCRIPTION relay_sub SET (slot_name = NONE)`)
			applyPGSQL(t, dsn, `DROP SUBSCRIPTION relay_sub`)
		}()

		setupLogs := captureWarnLogs(t, func() {
			if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"relay_t"}}); err != nil {
				t.Fatalf("Setup: %v", err)
			}
		})
		if !strings.Contains(setupLogs, captureGapRiskMarker) || !strings.Contains(setupLogs, "relay_sub") {
			t.Fatalf("Setup on a subscriber source should WARN naming the subscription; logs:\n%s", setupLogs)
		}

		openLogs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if !strings.Contains(openLogs, captureGapRiskMarker) || !strings.Contains(openLogs, "relay_sub") {
			t.Fatalf("stream open on a subscriber source should WARN naming the subscription; logs:\n%s", openLogs)
		}
	})

	t.Run("relay shape WARNs at stream open", func(t *testing.T) {
		// Simulate this source being the TARGET of another sluice sync: the
		// PG applier's per-target control table, one registered stream.
		applyPGSQL(t, dsn, `
			CREATE TABLE public.sluice_cdc_state (
				stream_id         VARCHAR(255) NOT NULL PRIMARY KEY,
				source_position   TEXT         NOT NULL,
				updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
				stop_requested_at TIMESTAMP    NULL
			);
			INSERT INTO public.sluice_cdc_state (stream_id, source_position) VALUES ('upstream-a-to-b', 'pos');
		`)
		defer applyPGSQL(t, dsn, `DROP TABLE public.sluice_cdc_state`)

		logs := captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		if !strings.Contains(logs, captureGapRiskMarker) || !strings.Contains(logs, "sluice_cdc_state") {
			t.Fatalf("stream open on a relay-shaped source should WARN naming the control table; logs:\n%s", logs)
		}
	})
}
