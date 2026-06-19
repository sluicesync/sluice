//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 157 Q2 — the cross-engine schema-narrowing ADVISORY notices must
// surface on the `sync` cold-start path, not just `migrate`. The migrate
// path has always emitted them in phaseTranslateAndGateSchema; before this
// fix the sync cold-start path emitted none, so a `sync` cold-copy of a
// MySQL→PG schema with a `bigint unsigned` column got NO up-front warning.
// This is the end-to-end coverage referenced by the unit pins in
// cross_engine_notices_test.go: it proves emitCrossEngineTranslationNotices
// is actually wired into the live cold-start dispatch and the WARN lands
// before bulk-copy completes.

package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_ColdStart_UnsignedBigintNotice_Cross_MySQLToPG pins that a
// MySQL→PG `sync` cold-start of a schema carrying a `bigint unsigned`
// column emits the advisory unsigned-bigint WARN (Bug 11) — carrying the
// "sync cold-start" mode label — up front, before the bulk-copy completes.
// Common-case fidelity is unaffected: the notice is advisory and the rows
// still copy through.
func TestStreamer_ColdStart_UnsignedBigintNotice_Cross_MySQLToPG(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	// `accounts.id` and `accounts.owner_id` are BIGINT UNSIGNED — the Bug 11
	// narrowing class. `balance` is a plain signed bigint (not flagged).
	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE accounts (
			id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
			owner_id BIGINT UNSIGNED NOT NULL,
			balance  BIGINT NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO accounts (owner_id, balance) VALUES (10, 100), (20, 200);")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Capture WARN-level slog from the live cold-start path. The
	// goroutine-safe lockedBuffer is required because the streamer runs in
	// its own goroutine alongside the test goroutine.
	logBuf := &lockedBuffer{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prevDefault)

	streamer := &Streamer{
		Source:    myEng,
		Target:    pgEng,
		SourceDSN: mysqlDSN,
		TargetDSN: pgDSN,
		StreamID:  "test-coldstart-ubigint-notice",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// The bulk-copy landing the seed rows proves the cold-start dispatch ran
	// the copy phase; the notice is emitted BEFORE that point in coldStart.
	if !waitForPGRowCount(t, pgDSN, "accounts", 2, 60*time.Second) {
		t.Fatalf("bulk-copy never landed seed rows on PG target")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	out := string(logBuf.Bytes())
	if !strings.Contains(out, "bigint unsigned") {
		t.Errorf("sync cold-start did NOT emit the unsigned-bigint advisory WARN (Bug 157 Q2); logs:\n%s", out)
	}
	if !strings.Contains(out, "sync cold-start") {
		t.Errorf("the advisory WARN does not carry the \"sync cold-start\" mode label; logs:\n%s", out)
	}
	// The notice names the affected rows (loud-failure discipline).
	if !strings.Contains(out, "accounts.id") || !strings.Contains(out, "accounts.owner_id") {
		t.Errorf("the advisory WARN does not name the affected columns; logs:\n%s", out)
	}
}
