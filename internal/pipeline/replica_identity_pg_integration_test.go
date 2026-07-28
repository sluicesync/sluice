//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 93 — the PATH pin for the source-side replica-identity
// refusal.
//
// The engine-level matrix (internal/engines/postgres/replica_identity_
// preflight_integration_test.go) oracles the classification against a
// real server. This file proves the thing that actually broke operators:
// a real PG → PG `sluice sync` whose scope contains a source table with
// no usable replica identity.
//
// Pre-fix behaviour, reproduced on v0.103.0/1/2: cold start succeeded,
// silently, logging NOTHING about replica identity — and from the moment
// it scoped the publication, Postgres refused the SOURCE APPLICATION's
// own UPDATE and DELETE on that table:
//
//	ERROR: cannot update table "dpk" because it does not have a replica
//	       identity and publishes updates
//
// INSERT kept working, so it looked healthy until the first update, and
// the failure surfaced in the operator's application where nothing
// pointed back at sluice.
//
// Two properties are load-bearing here and neither is provable by
// calling the preflight directly, which is why these tests drive
// Streamer.Run:
//
//  1. REACHABILITY — the refusal is on the real cold-start path.
//  2. TIMING — it fires BEFORE the publication is scoped. Every refusal
//     case asserts that no publication exists afterwards AND that the
//     source's own UPDATE still works. A preflight that fires after the
//     ALTER PUBLICATION would pass (1) and still leave the operator's
//     application broken, which IS the defect.
//
// The must-NOT-refuse direction carries equal weight: an over-refusal
// here would break every working PG sync, so the last test streams a
// stream made entirely of publishable-but-unusual shapes.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// assertSourceStillWritable is the timing assertion: after a refusal the
// source must be exactly as sluice found it — no publication, and the
// operator's own UPDATE still accepted.
func assertSourceStillWritable(t *testing.T, sourceDSN, table string) {
	t.Helper()
	if n := pgScalarInt(t, sourceDSN, "SELECT count(*) FROM pg_publication"); n != 0 {
		t.Errorf("sluice created/rescoped %d publication(s) before refusing — the refusal fired too late, "+
			"which is the defect rather than the fix", n)
	}
	applyDDL(t, sourceDSN, "UPDATE "+table+" SET v = v;")
}

// assertReplicaIdentityRefusal asserts err is the coded item-93 refusal
// and names the offending table.
func assertReplicaIdentityRefusal(t *testing.T, err error, table string) {
	t.Helper()
	if err == nil {
		t.Fatal("sync start succeeded against a source table with no usable replica identity — " +
			"the item-93 gate did not fire; the operator's own UPDATE/DELETE on it would now be refused by Postgres")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeSourceReplicaIdentity {
		t.Fatalf("sync failed, but not with %s.\n got: %v", sluicecode.CodeSourceReplicaIdentity, err)
	}
	if coded.Hint == "" {
		t.Error("refusal carries no remedy hint")
	}
	if !strings.Contains(err.Error(), table) {
		t.Errorf("refusal does not name the offending table %q: %v", table, err)
	}
}

// TestReplicaIdentity_SyncRefusesBeforeScopingThePublication walks the
// three shapes item 93 folds into one catalog read — the deferrable key,
// the keyless table, and an explicit REPLICA IDENTITY NOTHING — each
// through a real cold start, each asserting the source is left untouched.
func TestReplicaIdentity_SyncRefusesBeforeScopingThePublication(t *testing.T) {
	cases := []struct {
		name   string
		table  string
		ddl    string
		reason string
	}{
		{
			name:   "deferrable primary key",
			table:  "ri_dpk",
			ddl:    `CREATE TABLE ri_dpk (id int, v text, CONSTRAINT ri_dpk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);`,
			reason: "DEFERRABLE",
		},
		{
			// Today's behaviour for this shape was an INFO about copy
			// strategy and nothing else; the source broke identically.
			name:   "no key at all",
			table:  "ri_keyless",
			ddl:    `CREATE TABLE ri_keyless (id int, v text);`,
			reason: "no PRIMARY KEY",
		},
		{
			// A perfectly good primary key is not enough when the operator
			// has opted out of publishing an identity.
			name:  "REPLICA IDENTITY NOTHING",
			table: "ri_nothing",
			ddl: `CREATE TABLE ri_nothing (id int PRIMARY KEY, v text);
			      ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING;`,
			reason: "NOTHING",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()

			applyDDL(t, sourceDSN, tc.ddl+`
				CREATE TABLE ri_plain (id int PRIMARY KEY, v text);
				INSERT INTO `+tc.table+` (id, v) VALUES (1, 'a');
				INSERT INTO ri_plain (id, v) VALUES (1, 'a');
			`)

			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}
			s := &Streamer{
				Source:    pgEng,
				Target:    pgEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "replica-identity-" + tc.table,
				SlotName:  "replica_identity_" + tc.table,
			}

			// Generous but finite: pre-fix this run enters CDC and stays up
			// until the deadline, so a timeout is itself a failure signal.
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			err := s.Run(ctx)
			if ctx.Err() != nil {
				t.Fatalf("sync ran to the context deadline instead of refusing at start: %v", err)
			}
			assertReplicaIdentityRefusal(t, err, tc.table)
			if !strings.Contains(err.Error(), tc.reason) {
				t.Errorf("refusal does not name the cause (%q): %v", tc.reason, err)
			}
			// The refusal must not name the table it CAN serve.
			if strings.Contains(err.Error(), "ri_plain (") {
				t.Errorf("refusal names the ordinary sibling table: %v", err)
			}
			assertSourceStillWritable(t, sourceDSN, tc.table)
		})
	}
}

// TestReplicaIdentity_DeferrablePKPlusUniqueIndexIsStillRefused pins the
// deliberate asymmetry with the target-side Bug-211 gate. That gate
// clears a deferrable PK when an immediate NOT NULL UNIQUE index exists
// (ON CONFLICT can arbitrate on it). REPLICA IDENTITY DEFAULT cannot:
// it resolves to the primary key and nothing else, so on the SOURCE the
// same shape still has no identity — ground-truthed on PG 16 in the
// engine-level oracle. The refusal must therefore fire AND name the
// index the operator can nominate, which is the whole difference between
// an actionable refusal and a confusing one.
func TestReplicaIdentity_DeferrablePKPlusUniqueIndexIsStillRefused(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE ri_mixed (id int, k int NOT NULL, v text,
		  CONSTRAINT ri_mixed_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
		  CONSTRAINT ri_mixed_k  UNIQUE (k));
		INSERT INTO ri_mixed (id, k, v) VALUES (1, 10, 'a');
	`)

	pgEng, _ := engines.Get("postgres")
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "replica-identity-mixed",
		SlotName:  "replica_identity_mixed",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if ctx.Err() != nil {
		t.Fatalf("sync ran to the context deadline instead of refusing at start: %v", err)
	}
	assertReplicaIdentityRefusal(t, err, "ri_mixed")
	if !strings.Contains(err.Error(), "ri_mixed_k") {
		t.Errorf("refusal does not name the immediate UNIQUE index the operator could nominate: %v", err)
	}
	if !strings.Contains(err.Error(), "REPLICA IDENTITY USING INDEX") {
		t.Errorf("refusal does not offer the USING INDEX remedy: %v", err)
	}
	assertSourceStillWritable(t, sourceDSN, "ri_mixed")
}

// TestReplicaIdentity_PublishableShapesStillStream is the over-refusal
// guard, and it carries equal weight: every table in this stream is a
// shape the gate has to let through — an ordinary primary key, a
// DEFERRABLE primary key rescued by REPLICA IDENTITY USING INDEX, and a
// KEYLESS table rescued by REPLICA IDENTITY FULL. All three must
// cold-copy and then take live CDC exactly as before.
//
// (A deferrable PK under REPLICA IDENTITY FULL is publishable on the
// SOURCE too, but a PG → PG sync carries the deferrable key onto the
// target, where the Bug-211 gate refuses it — so that cell is pinned in
// deferrable_key_pg_integration_test.go instead of here.)
func TestReplicaIdentity_PublishableShapesStillStream(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE ri_ok_plain (id int PRIMARY KEY, v text);
		CREATE TABLE ri_ok_using (id int, k int NOT NULL, v text,
		  CONSTRAINT ri_ok_using_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
		  CONSTRAINT ri_ok_using_k  UNIQUE (k));
		ALTER TABLE ri_ok_using REPLICA IDENTITY USING INDEX ri_ok_using_k;
		CREATE TABLE ri_ok_keyless (id int, v text);
		ALTER TABLE ri_ok_keyless REPLICA IDENTITY FULL;
		INSERT INTO ri_ok_plain   (id, v)    VALUES (1, 'a'), (2, 'b');
		INSERT INTO ri_ok_using   (id, k, v) VALUES (1, 10, 'a'), (2, 20, 'b');
		INSERT INTO ri_ok_keyless (id, v)    VALUES (1, 'a'), (2, 'b');
	`)

	pgEng, _ := engines.Get("postgres")
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "replica-identity-ok",
		SlotName:  "replica_identity_ok",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	for _, table := range []string{"ri_ok_plain", "ri_ok_using", "ri_ok_keyless"} {
		if !waitForRowCount(t, targetDSN, table, 2, 90*time.Second) {
			t.Fatalf("cold start never delivered the seed rows for %q", table)
		}
	}
	applyDDL(t, sourceDSN, `
		INSERT INTO ri_ok_plain   (id, v)    VALUES (3, 'cdc');
		INSERT INTO ri_ok_using   (id, k, v) VALUES (3, 30, 'cdc');
		INSERT INTO ri_ok_keyless (id, v)    VALUES (3, 'cdc');
		UPDATE ri_ok_using   SET v = 'upd' WHERE id = 1;
		UPDATE ri_ok_keyless SET v = 'upd' WHERE id = 1;
	`)
	for _, table := range []string{"ri_ok_plain", "ri_ok_using", "ri_ok_keyless"} {
		if !waitForRowCount(t, targetDSN, table, 3, 90*time.Second) {
			t.Fatalf("CDC never delivered the live insert for %q", table)
		}
	}

	select {
	case err := <-runErr:
		t.Fatalf("stream exited early: %v", err)
	default:
	}
}

// TestReplicaIdentity_AddTableRefusesBeforeExtendingThePublication pins
// the OTHER door into the publication. `schema add-table` runs
// `ALTER PUBLICATION … ADD TABLE` against a live stream, after which
// Postgres refuses the source application's own UPDATE/DELETE on the
// added table exactly as cold start would have. Unlike the target-side
// sibling (Bug 211) there is no per-change floor to catch it later — the
// refusal happens in the operator's application and never reaches sluice
// — so the preflight is the only guard, and it has to run before the
// ALTER.
func TestReplicaIdentity_AddTableRefusesBeforeExtendingThePublication(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE ri_live (id int PRIMARY KEY, v text);
		INSERT INTO ri_live (id, v) VALUES (1, 'a');
	`)

	pgEng, _ := engines.Get("postgres")
	const streamID = "replica-identity-add-table"
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		SlotName:  "replica_identity_add_table",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(streamCtx) }()

	if !waitForRowCount(t, targetDSN, "ri_live", 1, 90*time.Second) {
		t.Fatal("cold start never delivered the seed row")
	}

	// A new source table whose only key is DEFERRABLE, added live.
	applyDDL(t, sourceDSN, `
		CREATE TABLE ri_late (id int, v text, CONSTRAINT ri_late_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
		INSERT INTO ri_late (id, v) VALUES (1, 'a');
	`)

	addCtx, addCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer addCancel()
	err := (&AddTable{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		TableName: "ri_late",
		LiveMode:  true,
	}).Run(addCtx)
	assertReplicaIdentityRefusal(t, err, "ri_late")

	// The publication exists (the stream owns it) but must NOT have
	// gained the new table — and the operator's own writes to it must
	// still work.
	if n := pgScalarInt(t, sourceDSN, `
		SELECT count(*) FROM pg_publication_rel pr
		JOIN pg_class c ON c.oid = pr.prrelid
		WHERE c.relname = 'ri_late'`); n != 0 {
		t.Errorf("add-table joined ri_late to the publication before refusing — the refusal fired too late")
	}
	applyDDL(t, sourceDSN, "UPDATE ri_late SET v = 'still writable';")

	// The running stream is untouched by the refused add.
	applyDDL(t, sourceDSN, "INSERT INTO ri_live (id, v) VALUES (2, 'b');")
	if !waitForRowCount(t, targetDSN, "ri_live", 2, 90*time.Second) {
		t.Fatal("the live stream wedged after a refused add-table")
	}
}

// TestReplicaIdentity_ExcludeTableClearsTheRefusal pins the escape the
// refusal advertises. Excluding the offending table must let the very
// same source stream — otherwise the remedy sluice prints is a lie.
func TestReplicaIdentity_ExcludeTableClearsTheRefusal(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE ri_dpk   (id int, v text, CONSTRAINT ri_dpk_pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED);
		CREATE TABLE ri_plain (id int PRIMARY KEY, v text);
		INSERT INTO ri_dpk   (id, v) VALUES (1, 'a'), (2, 'b');
		INSERT INTO ri_plain (id, v) VALUES (1, 'a'), (2, 'b');
	`)

	pgEng, _ := engines.Get("postgres")
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "replica-identity-excluded",
		SlotName:  "replica_identity_excluded",
		Filter:    migcore.TableFilter{Exclude: []string{"ri_dpk"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	if !waitForRowCount(t, targetDSN, "ri_plain", 2, 90*time.Second) {
		t.Fatal("cold start never delivered the seed rows for the in-scope table")
	}
	applyDDL(t, sourceDSN, "INSERT INTO ri_plain (id, v) VALUES (3, 'cdc');")
	if !waitForRowCount(t, targetDSN, "ri_plain", 3, 90*time.Second) {
		t.Fatal("CDC never delivered the live insert for the in-scope table")
	}
	// The excluded table stayed out of the publication, so the operator's
	// own writes to it keep working — the point of the escape.
	applyDDL(t, sourceDSN, "UPDATE ri_dpk SET v = 'still writable';")

	select {
	case err := <-runErr:
		t.Fatalf("stream exited early: %v", err)
	default:
	}
}
