//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 G8 real-server pins for the MariaDB log_bin_compress refusal
// (cdc_binlog_compress_preflight.go): the preflight refuses ON at CDC
// open, the dispatch belt refuses a compressed row event delivered
// AFTER a passing preflight (the dynamic-flip shape the belt exists
// for), and vanilla MySQL — where the variable does not exist — passes
// via the real driver's error 1193. Pinned on mariadb:11.4 (the
// ground-truth line of the G8 filing); the refusal keys on the event
// TYPE go-mysql reports, which is version-independent wire vocabulary,
// so one line suffices for the refusal posture. Uses a DEDICATED
// container because the test flips the GLOBAL log_bin_compress state.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestMariaDB_CDCReader_LogBinCompress_RefusedLoudly pins both halves
// of the G8 refusal against a real MariaDB 11.4 booted with
// --log-bin-compress=ON (default log_bin_compress_min_len=256).
func TestMariaDB_CDCReader_LogBinCompress_RefusedLoudly(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--log-bin-compress=ON")
	defer cleanup()
	execSQLScript(t, dsn, `
		CREATE TABLE big (
			id BIGINT NOT NULL AUTO_INCREMENT,
			v  TEXT   NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;`)

	eng := Engine{Flavor: FlavorMariaDB}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Half 1 — the preflight: with the global ON, the CDC open refuses
	// up front, coded, before any event is read.
	t.Run("preflight refuses at open", func(t *testing.T) {
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() {
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()
		_, err = rdr.StreamChanges(ctx, ir.Position{})
		if err == nil {
			t.Fatal("StreamChanges with @@GLOBAL.log_bin_compress=ON = nil; want the coded refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCBinlogCompressed {
			t.Fatalf("want %s; got: %v", sluicecode.CodeCDCBinlogCompressed, err)
		}
		if !strings.Contains(err.Error(), "SET GLOBAL log_bin_compress=OFF") {
			t.Errorf("refusal message missing the remedy: %v", err)
		}
	})

	// Half 2 — the belt: flip the global OFF (dynamic), open a stream
	// (the preflight now passes), then flip it back ON mid-stream and
	// write a ≥256 B row. The compressed event that produces must stop
	// the stream with the same code — the exact scenario the preflight
	// cannot see and the reason the belt is the load-bearing half.
	t.Run("belt refuses after a mid-stream flip", func(t *testing.T) {
		setGlobalLogBinCompress(t, dsn, "OFF")
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() {
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}()
		changes, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges after SET GLOBAL log_bin_compress=OFF: %v (the preflight must pass with "+
				"the variable off)", err)
		}
		time.Sleep(300 * time.Millisecond)

		// A small row streams normally — compression is size-conditional
		// and OFF anyway; this proves the stream is live before the flip.
		applyMySQL(t, dsn, "INSERT INTO big (v) VALUES ('small')")
		got := drainChanges(t, ctx, changes, 1, 30*time.Second)
		if len(got) != 1 {
			t.Fatalf("pre-flip: got %d changes; want 1 (stream error: %v)", len(got), rdr.(*CDCReader).Err())
		}

		setGlobalLogBinCompress(t, dsn, "ON")
		applyMySQL(t, dsn, "INSERT INTO big (v) VALUES (REPEAT('X', 600))")

		drained := drainChanges(t, ctx, changes, 1, 30*time.Second)
		streamErr := rdr.(*CDCReader).Err()
		if streamErr == nil {
			t.Fatalf("G8 VERDICT: a ≥256 B row written after the mid-stream log_bin_compress flip produced "+
				"no error (drained %d changes) — the compressed row event was silently dropped", len(drained))
		}
		ce, ok := sluicecode.FromError(streamErr)
		if !ok || ce.Code != sluicecode.CodeCDCBinlogCompressed {
			t.Fatalf("want %s from the dispatch belt; got: %v", sluicecode.CodeCDCBinlogCompressed, streamErr)
		}
		if !strings.Contains(streamErr.Error(), "cdc_src.big") {
			t.Errorf("belt refusal does not name the table: %v", streamErr)
		}
	})
}

// TestPreflightBinlogCompress_VanillaMySQLPasses pins the
// absent-variable PASS against a real MySQL server through the real
// driver: SELECT @@GLOBAL.log_bin_compress errors 1193 there, which the
// preflight treats as proof the server cannot write compressed events.
func TestPreflightBinlogCompress_VanillaMySQLPasses(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "g8_compress_pass")
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := preflightBinlogCompress(context.Background(), db); err != nil {
		t.Fatalf("preflightBinlogCompress on vanilla MySQL = %v; want nil (variable absent → error 1193 → PASS)", err)
	}
}

// setGlobalLogBinCompress flips the dynamic global on the dedicated
// container.
func setGlobalLogBinCompress(t *testing.T, dsn, state string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "SET GLOBAL log_bin_compress="+state); err != nil {
		t.Fatalf("SET GLOBAL log_bin_compress=%s: %v", state, err)
	}
}
