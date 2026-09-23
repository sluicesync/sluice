//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// unforwardedRestartCase is one source lane of the persisted-refusal pin.
// newStreamer builds a FRESH Streamer each call — the in-process stand-in
// for a new process (systemd Restart=on-failure, a pod restart): nothing
// but the target's control table carries over between them.
type unforwardedRestartCase struct {
	sourceDSN, targetDSN string
	apply                func(t *testing.T, dsn, sqlText string)
	newStreamer          func(ack string) *Streamer
	table                string
	// ddl is the source change the door refuses; targetFix is the same
	// change applied to the target — the remedy the acknowledgement is
	// meant to follow.
	ddl, targetFix string
}

// runUnforwardedRefusalRestart drives the whole contract on a real stream:
//
//  1. a stream that hits the door ends with the refusal, and the row
//     written after the DDL does NOT reach the target;
//  2. a restart WITHOUT --accept-unforwarded-schema-change refuses at
//     startup, before any change stream opens — before the fix, this
//     restart baselined the already-changed catalog and applied the
//     pending row silently, forever;
//  3. a restart WITH the acknowledgement (after the change is applied to
//     the target) clears the record, applies the pending row, and keeps
//     streaming.
func runUnforwardedRefusalRestart(t *testing.T, tc unforwardedRestartCase) {
	t.Helper()
	tgtDB, err := sql.Open("pgx", tc.targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	run := func(s *Streamer) (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- s.Run(ctx) }()
		return cancel, runErr
	}
	stopRun := func(cancel context.CancelFunc, runErr chan error) {
		cancel()
		select {
		case <-runErr:
		case <-time.After(30 * time.Second):
			t.Fatal("Streamer.Run did not return after ctx cancel")
		}
	}
	recorded := func() (string, bool) {
		var msg sql.NullString
		if err := tgtDB.QueryRow(`SELECT unforwarded_refusal FROM sluice_cdc_state`).Scan(&msg); err != nil {
			t.Fatalf("read the recorded refusal: %v", err)
		}
		return msg.String, msg.Valid
	}

	// ---- 1. The stream refuses, and records it. ----
	cancel, runErr := run(tc.newStreamer(""))
	defer cancel()
	if !waitForPGRowCount(t, tc.targetDSN, tc.table, 1, 90*time.Second) {
		t.Fatalf("cold copy never landed")
	}
	// A CDC row proves StreamChanges (and so the baseline) is live.
	tc.apply(t, tc.sourceDSN, "INSERT INTO "+tc.table+" (id, name) VALUES (2, 'b');")
	if !waitForPGRowCount(t, tc.targetDSN, tc.table, 2, 60*time.Second) {
		t.Fatalf("pre-DDL CDC row never landed")
	}
	tc.apply(t, tc.sourceDSN, tc.ddl+"; INSERT INTO "+tc.table+" (id, name) VALUES (3, 'c');")
	var first error
	select {
	case first = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("stream did not refuse within 60s")
	}
	if !errors.Is(first, ir.ErrUnforwardedSchemaChange) {
		t.Fatalf("stream ended with %v; want errors.Is(ir.ErrUnforwardedSchemaChange)", first)
	}
	if msg, ok := recorded(); !ok || !strings.Contains(msg, "UNFORWARDED-SCHEMA-CHANGE") {
		t.Fatalf("the refusal was not recorded on the target: %q (valid=%v)", msg, ok)
	}
	if n := pollRowCount(tc.targetDSN, tc.table); n != 2 {
		t.Fatalf("target has %d rows after the refusal; want 2 (the post-DDL row must not land)", n)
	}

	// ---- 2. A plain restart refuses at startup. ----
	cancel2, runErr2 := run(tc.newStreamer(""))
	var second error
	select {
	case second = <-runErr2:
	case <-time.After(45 * time.Second):
		n := pollRowCount(tc.targetDSN, tc.table)
		stopRun(cancel2, runErr2)
		t.Fatalf("a restart WITHOUT %s did not refuse (target rows now %d): it re-baselined past the refused change",
			unforwardedRefusalAckFlag, n)
	}
	cancel2()
	var replay *recordedUnforwardedRefusalError
	if !errors.As(second, &replay) || !errors.Is(second, ir.ErrUnforwardedSchemaChange) || !ir.IsTerminal(second) {
		t.Fatalf("restart without the acknowledgement ended with %v; want the startup door's terminal replay", second)
	}
	if !strings.Contains(second.Error(), unforwardedRefusalAckFlag) {
		t.Errorf("the replay does not name %s: %v", unforwardedRefusalAckFlag, second)
	}
	if n := pollRowCount(tc.targetDSN, tc.table); n != 2 {
		t.Fatalf("target has %d rows after the refusing restart; want 2", n)
	}
	if _, ok := recorded(); !ok {
		t.Fatal("the refusing restart cleared the record")
	}

	// ---- 2b. An acknowledgement naming a DIFFERENT refusal is refused. ----
	// The fingerprint binds the flag to the refusal the operator read, so a
	// value left in a service definition cannot pre-accept a later one.
	fp := regexp.MustCompile(`fingerprint ([0-9a-f]{12})`).FindStringSubmatch(second.Error())
	if fp == nil {
		t.Fatalf("the replay does not print the refusal's fingerprint: %v", second)
	}
	cancelW, runErrW := run(tc.newStreamer("000000000000"))
	var wrong error
	select {
	case wrong = <-runErrW:
	case <-time.After(45 * time.Second):
		stopRun(cancelW, runErrW)
		t.Fatal("a restart with a mismatched fingerprint did not refuse")
	}
	cancelW()
	if !errors.Is(wrong, ir.ErrUnforwardedSchemaChange) || !strings.Contains(wrong.Error(), "names a different refusal") {
		t.Fatalf("mismatched acknowledgement ended with %v; want the replay naming the mismatch", wrong)
	}
	if _, ok := recorded(); !ok {
		t.Fatal("a mismatched acknowledgement cleared the record")
	}

	// ---- 3. Apply the change to the target, then acknowledge. ----
	if _, err := tgtDB.Exec(tc.targetFix); err != nil {
		t.Fatalf("apply the change to the target: %v", err)
	}
	cancel3, runErr3 := run(tc.newStreamer(fp[1]))
	defer stopRun(cancel3, runErr3)
	if !waitForPGRowCount(t, tc.targetDSN, tc.table, 3, 60*time.Second) {
		select {
		case err := <-runErr3:
			t.Fatalf("acknowledged restart ended: %v", err)
		default:
		}
		t.Fatal("acknowledged restart never applied the pending row")
	}
	if msg, ok := recorded(); ok {
		t.Errorf("acknowledged restart left the record: %q", msg)
	}
	tc.apply(t, tc.sourceDSN, "INSERT INTO "+tc.table+" (id, name) VALUES (4, 'd');")
	if !waitForPGRowCount(t, tc.targetDSN, tc.table, 4, 60*time.Second) {
		t.Fatal("the acknowledged stream stopped streaming")
	}
}

// TestStreamer_PGSource_UnforwardedRefusal_SurvivesRestart pins the
// persisted refusal on a real PG → PG stream (see
// runUnforwardedRefusalRestart). Shard: TestStreamer_ prefix →
// pipeline-rest-streamer.
func TestStreamer_PGSource_UnforwardedRefusal_SurvivesRestart(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	applyPGDDL(t, sourceDSN, `CREATE TABLE gcr (id INT PRIMARY KEY, name TEXT); INSERT INTO gcr VALUES (1, 'a');`)
	runUnforwardedRefusalRestart(t, unforwardedRestartCase{
		sourceDSN: sourceDSN, targetDSN: targetDSN,
		apply: applyPGDDL,
		newStreamer: func(ack string) *Streamer {
			return &Streamer{
				Source: pgEng, Target: pgEng,
				SourceDSN: sourceDSN, TargetDSN: targetDSN,
				StreamID: "test-gc-restart",
				Filter:   migcore.TableFilter{Include: []string{"gcr"}},
				SlotName: "gcr", PublicationName: "gcr",

				AcceptUnforwardedSchemaChange: ack,
			}
		},
		table:     "gcr",
		ddl:       `ALTER TABLE gcr ADD CONSTRAINT gcr_name_uq UNIQUE (name)`,
		targetFix: `ALTER TABLE gcr ADD CONSTRAINT gcr_name_uq UNIQUE (name)`,
	})
}

// TestStreamer_MySQLSource_UnforwardedRefusal_SurvivesRestart is the binlog
// lane's sibling on a real MySQL → Postgres stream. Shard: TestStreamer_
// prefix → pipeline-rest-streamer.
func TestStreamer_MySQLSource_UnforwardedRefusal_SurvivesRestart(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, targetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	applyDDLMySQL(t, sourceDSN, `CREATE TABLE gcr (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB; INSERT INTO gcr VALUES (1, 'a');`)
	runUnforwardedRefusalRestart(t, unforwardedRestartCase{
		sourceDSN: sourceDSN, targetDSN: targetDSN,
		apply: applyDDLMySQL,
		newStreamer: func(ack string) *Streamer {
			return &Streamer{
				Source: myEng, Target: pgEng,
				SourceDSN: sourceDSN, TargetDSN: targetDSN,
				StreamID: "test-gc-restart-mysql",
				Filter:   migcore.TableFilter{Include: []string{"gcr"}},

				AcceptUnforwardedSchemaChange: ack,
			}
		},
		table:     "gcr",
		ddl:       `ALTER TABLE gcr ADD CONSTRAINT gcr_name_uq UNIQUE (name)`,
		targetFix: `ALTER TABLE gcr ADD CONSTRAINT gcr_name_uq UNIQUE (name)`,
	})
}
