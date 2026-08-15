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

// TestOpenDB_RefusesMultiStatementEvenWhenDSNAsksForIt is the H-3
// server-differential pin (audit 2026-08-15): sluice's MySQL immunity to
// the C1-1 restore-injection class must be ENFORCED, not left resting on
// the operator's DSN omitting `multiStatements=true`. Even when the DSN
// explicitly sets the flag, a connection sluice opens must reject a
// second statement — the counterpart of the PostgreSQL single-statement
// door, enforced by the driver itself rather than a hand lexer.
//
// The mechanism proven end-to-end: a recorded/tampered CHECK body that
// appends `; DROP TABLE …` would, on a `multiStatements=true` DSN,
// otherwise execute as the restore role. With hardenAgainstMultiStatement
// clearing the flag in openDB, the second statement is refused (Error
// 1064) before it runs, and the canary survives.
func TestOpenDB_RefusesMultiStatementEvenWhenDSNAsksForIt(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A canary the injected second statement would drop.
	applyDDL(t, dsn, `DROP TABLE IF EXISTS h3_canary; CREATE TABLE h3_canary (id int)`)

	// Anti-vacuity: a RAW connection that keeps multiStatements=true DOES
	// run the injection — so the refusal below is the hardening, not the
	// server rejecting multi-statement Exec on its own.
	if raw, ok := probeMultiStatement(t, dsn+"&multiStatements=true"); !ok {
		t.Fatalf("anti-vacuity: a multiStatements=true connection did NOT execute the injection (%v) — the pin cannot prove the hardening blocks it", raw)
	}
	// Restore the canary the anti-vacuity probe dropped.
	applyDDL(t, dsn, `CREATE TABLE h3_canary (id int)`)

	// The real pin: sluice's own openDB, handed a DSN that ASKS for
	// multiStatements, must still refuse the second statement.
	db, err := openDB(ctx, mustParseDSN(t, dsn+"&multiStatements=true"), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, execErr := db.ExecContext(ctx, `SELECT 1; DROP TABLE h3_canary`)
	if execErr == nil {
		t.Fatal("openDB executed a multi-statement string despite the DSN — the H-3 injection surface is open (the second statement ran as the connection role)")
	}
	if !strings.Contains(execErr.Error(), "1064") {
		t.Logf("note: refusal was %q (expected the driver's Error 1064 syntax refusal); still refused, which is the security property", execErr)
	}

	// The canary must survive — proof the injected DROP never executed.
	var exists int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'h3_canary'`).
		Scan(&exists); err != nil {
		t.Fatalf("canary existence probe: %v", err)
	}
	if exists != 1 {
		t.Fatal("the canary was dropped — the injected second statement executed through sluice's own connection")
	}
}

// probeMultiStatement opens a RAW connection on the given DSN (NOT through
// openDB) and runs the injection, returning whether the canary was
// dropped. It is the anti-vacuity control: it proves the DSN + driver +
// server actually execute a multi-statement Exec, so the openDB refusal
// is attributable to the hardening and not to the environment.
func probeMultiStatement(t *testing.T, dsnWithFlag string) (execErr error, injected bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := sql.Open("mysql", dsnWithFlag)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer func() { _ = raw.Close() }()
	_, execErr = raw.ExecContext(ctx, `SELECT 1; DROP TABLE h3_canary`)
	var cnt int
	if qerr := raw.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'h3_canary'`).
		Scan(&cnt); qerr != nil {
		t.Fatalf("raw canary probe: %v", qerr)
	}
	return execErr, cnt == 0
}
