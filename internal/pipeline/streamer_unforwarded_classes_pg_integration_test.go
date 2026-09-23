//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_PGSource_UnforwardedSchemaChange_Refuses is the end-to-end
// pin for GC-2 on a real PG → PG stream. pgoutput carries no constraint,
// RLS, policy or nullability information, so before the door each of
// these source DDLs was ignored at exit 0 and the target silently stayed
// weaker than the source. The door rests on one premise the unit tier
// cannot check — that every one of these DDLs makes pgoutput re-send the
// RelationMessage before the next change on the table — and this test is
// where that premise is asserted, per DDL.
//
// Each refusing case cold-starts its own table and stream, runs the DDL
// plus one INSERT (the INSERT is what pushes the RelationMessage), and
// requires the stream to END with UNFORWARDED-SCHEMA-CHANGE naming the
// change — graded against the target's catalog, which must NOT have it.
// The control forwards an ADD COLUMN NOT NULL DEFAULT and then a DROP
// COLUMN whose UNIQUE constraint cascades away, and requires the stream
// to keep applying: the exemptions are what keep the door from ending
// streams the ADR-0091 forward handles correctly.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.
func TestStreamer_PGSource_UnforwardedSchemaChange_Refuses(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	applyPGDDL(t, sourceDSN, `CREATE TABLE parent (id INT PRIMARY KEY); INSERT INTO parent VALUES (1);`)

	cases := []struct {
		table, create, ddl string
		wantDelta          string
		// targetHas counts the change on the target; it must be 0.
		targetHas string
	}{
		{
			table:     "gc2_fk",
			create:    `CREATE TABLE gc2_fk (id INT PRIMARY KEY, name TEXT)`,
			ddl:       `ALTER TABLE gc2_fk ADD COLUMN owner INT, ADD CONSTRAINT gc2_fk_owner FOREIGN KEY (owner) REFERENCES parent(id)`,
			wantDelta: `ADD CONSTRAINT "gc2_fk_owner" FOREIGN KEY (owner) REFERENCES parent(id)`,
			targetHas: `SELECT count(*) FROM pg_constraint WHERE conname = 'gc2_fk_owner'`,
		},
		{
			table:     "gc2_uq",
			create:    `CREATE TABLE gc2_uq (id INT PRIMARY KEY, name TEXT)`,
			ddl:       `ALTER TABLE gc2_uq ADD CONSTRAINT gc2_uq_name UNIQUE (name)`,
			wantDelta: `ADD CONSTRAINT "gc2_uq_name" UNIQUE (name)`,
			targetHas: `SELECT count(*) FROM pg_constraint WHERE conname = 'gc2_uq_name'`,
		},
		{
			table:     "gc2_rls",
			create:    `CREATE TABLE gc2_rls (id INT PRIMARY KEY, name TEXT)`,
			ddl:       `ALTER TABLE gc2_rls ENABLE ROW LEVEL SECURITY`,
			wantDelta: "row level security ENABLED",
			targetHas: `SELECT count(*) FROM pg_class WHERE relname = 'gc2_rls' AND relrowsecurity`,
		},
		{
			table:     "gc2_pol",
			create:    `CREATE TABLE gc2_pol (id INT PRIMARY KEY, name TEXT); ALTER TABLE gc2_pol ENABLE ROW LEVEL SECURITY`,
			ddl:       `CREATE POLICY gc2_pol_iso ON gc2_pol USING (name = current_user)`,
			wantDelta: `CREATE POLICY "gc2_pol_iso"`,
			targetHas: `SELECT count(*) FROM pg_policy WHERE polname = 'gc2_pol_iso'`,
		},
		{
			table:     "gc2_nn",
			create:    `CREATE TABLE gc2_nn (id INT PRIMARY KEY, name TEXT)`,
			ddl:       `ALTER TABLE gc2_nn ALTER COLUMN name SET NOT NULL`,
			wantDelta: `ALTER COLUMN "name" SET NOT NULL`,
			targetHas: `SELECT count(*) FROM information_schema.columns WHERE table_name = 'gc2_nn' AND column_name = 'name' AND is_nullable = 'NO'`,
		},
		{
			table:     "gc2_chk",
			create:    `CREATE TABLE gc2_chk (id INT PRIMARY KEY, name TEXT, CONSTRAINT gc2_chk_len CHECK (length(name) < 50))`,
			ddl:       `ALTER TABLE gc2_chk DROP CONSTRAINT gc2_chk_len`,
			wantDelta: `DROP CONSTRAINT "gc2_chk_len"`,
			// The target KEEPS the check the source dropped: count must stay 1.
			targetHas: `SELECT count(*) - 1 FROM pg_constraint WHERE conname = 'gc2_chk_len'`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			applyPGDDL(t, sourceDSN, tc.create+`; INSERT INTO `+tc.table+` (id, name) VALUES (1, 'a');`)
			streamer := &Streamer{
				Source: pgEng, Target: pgEng,
				SourceDSN: sourceDSN, TargetDSN: targetDSN,
				StreamID: "test-gc2-" + strings.ReplaceAll(tc.table, "_", "-"),
				Filter:   migcore.TableFilter{Include: []string{tc.table}},
				SlotName: tc.table, PublicationName: tc.table,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(ctx) }()
			if !waitForPGRowCount(t, targetDSN, tc.table, 1, 60*time.Second) {
				select {
				case err := <-runErr:
					t.Fatalf("cold copy never landed; stream ended: %v", err)
				default:
				}
				t.Fatalf("cold copy never landed")
			}
			// The cold copy landing does not mean StreamChanges has run: the
			// stream opens after the index and constraint phases, and a DDL in
			// that window lands IN the baseline and would pass for the wrong
			// reason. A row that arrives through CDC proves the stream (and
			// so the baseline) is live before the DDL.
			applyPGDDL(t, sourceDSN, `INSERT INTO `+tc.table+` (id, name) VALUES (3, 'c');`)
			if !waitForPGRowCount(t, targetDSN, tc.table, 2, 60*time.Second) {
				t.Fatalf("pre-DDL CDC row never landed")
			}

			applyPGDDL(t, sourceDSN, tc.ddl+`; INSERT INTO `+tc.table+` (id, name) VALUES (2, 'b');`)

			var got error
			select {
			case got = <-runErr:
			case <-time.After(60 * time.Second):
				t.Fatalf("stream did not refuse within 60s — the change was ignored (was the RelationMessage re-sent?)")
			}
			if got == nil || !strings.Contains(got.Error(), "UNFORWARDED-SCHEMA-CHANGE") || !strings.Contains(got.Error(), tc.wantDelta) {
				t.Fatalf("stream ended with %v; want UNFORWARDED-SCHEMA-CHANGE naming %q", got, tc.wantDelta)
			}
			var n int
			if err := tgtDB.QueryRow(tc.targetHas).Scan(&n); err != nil {
				t.Fatalf("read target catalog: %v", err)
			}
			if n != 0 {
				t.Errorf("target already reflects the change (%s = %d); the test is not measuring an uncarried change", tc.targetHas, n)
			}
			if pollRowCount(targetDSN, tc.table) != 2 {
				t.Errorf("the post-DDL row reached the target; the refusal must come before the change it guards")
			}
		})
	}

	t.Run("control_forwarded_column_changes_do_not_refuse", func(t *testing.T) {
		applyPGDDL(t, sourceDSN, `
			CREATE TABLE gc2_ctl (id INT PRIMARY KEY, name TEXT, doomed TEXT CONSTRAINT gc2_ctl_doomed_uq UNIQUE);
			ALTER TABLE gc2_ctl REPLICA IDENTITY FULL;
			INSERT INTO gc2_ctl (id, name, doomed) VALUES (1, 'a', 'x');`)
		streamer := &Streamer{
			Source: pgEng, Target: pgEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			StreamID: "test-gc2-ctl",
			Filter:   migcore.TableFilter{Include: []string{"gc2_ctl"}},
			SlotName: "gc2_ctl", PublicationName: "gc2_ctl",
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runErr := make(chan error, 1)
		go func() { runErr <- streamer.Run(ctx) }()
		if !waitForPGRowCount(t, targetDSN, "gc2_ctl", 1, 60*time.Second) {
			t.Fatalf("cold copy never landed")
		}
		// ADD COLUMN NOT NULL DEFAULT: the added column's own attributes ride
		// the forward. This is also the prime that makes the DROP below a
		// CDC→CDC boundary (the seed-guard skips a destructive shape against
		// the cold-start seed).
		applyPGDDL(t, sourceDSN, `
			ALTER TABLE gc2_ctl ADD COLUMN n INT NOT NULL DEFAULT 0;
			INSERT INTO gc2_ctl (id, name, doomed) VALUES (2, 'b', 'y');`)
		if !waitForPGColumn(t, tgtDB, "gc2_ctl", "n", true, 60*time.Second) {
			select {
			case err := <-runErr:
				t.Fatalf("stream ended on the ADD COLUMN forward: %v", err)
			default:
			}
			t.Fatalf("ADD COLUMN never forwarded")
		}
		// DROP COLUMN cascades its UNIQUE: exempt, the target's DROP does too.
		applyPGDDL(t, sourceDSN, `
			ALTER TABLE gc2_ctl DROP COLUMN doomed;
			INSERT INTO gc2_ctl (id, name) VALUES (3, 'c');`)
		if !waitForPGRowID(t, tgtDB, "gc2_ctl", 3, 60*time.Second) {
			select {
			case err := <-runErr:
				t.Fatalf("stream ended on the DROP COLUMN forward: %v", err)
			default:
			}
			t.Fatalf("post-DROP row never landed")
		}
		select {
		case err := <-runErr:
			t.Fatalf("stream ended: %v", err)
		default:
		}
		cancel()
		select {
		case <-runErr:
		case <-time.After(15 * time.Second):
			t.Fatal("Streamer.Run did not return after ctx cancel")
		}
	})
}

// TestStreamer_PGSource_UnforwardedSchemaChange_SurvivesTransientRetry is
// GC-32's end-to-end pin. The door's baseline lives in the CDC reader and
// an automatic retry opens a fresh one; before the carry, the fresh
// reader's catalog read already contained a change made before the
// transient, so a dropped replication connection between a source DDL and
// the table's next write accepted the change silently. Here the walsender
// is terminated right after the DDL, the ADR-0038 loop retries, and the
// stream must still refuse naming the change — graded on the refusal
// text, which the retried stream can only produce if it compared against
// the PRE-DDL baseline.
func TestStreamer_PGSource_UnforwardedSchemaChange_SurvivesTransientRetry(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	applyPGDDL(t, sourceDSN, `CREATE TABLE gc32 (id INT PRIMARY KEY, name TEXT); INSERT INTO gc32 VALUES (1, 'a');`)
	streamer := &Streamer{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID:              "test-gc32",
		ApplyRetryAttempts:    5,
		ApplyRetryBackoffBase: 50 * time.Millisecond,
		ApplyRetryBackoffCap:  500 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForPGRowCount(t, targetDSN, "gc32", 1, 60*time.Second) {
		t.Fatalf("cold copy never landed")
	}
	applyPGDDL(t, sourceDSN, `INSERT INTO gc32 VALUES (2, 'b');`)
	if !waitForPGRowCount(t, targetDSN, "gc32", 2, 60*time.Second) {
		t.Fatalf("pre-DDL CDC row never landed")
	}

	// The DDL alone emits nothing on the wire; the transient lands before
	// the write that would surface it.
	applyPGDDL(t, sourceDSN, `ALTER TABLE gc32 ADD CONSTRAINT gc32_name_uq UNIQUE (name);`)
	var killed int
	if err := srcDB.QueryRow(`SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE backend_type = 'walsender'`).Scan(&killed); err != nil {
		t.Fatalf("terminate walsender: %v", err)
	}
	if killed == 0 {
		t.Fatal("no walsender to terminate; the test cannot inject its transient")
	}
	// Give the retry loop time to reopen before the write, so the write is
	// decoded by the SECOND reader — the one that must carry the baseline.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := srcDB.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender'`).Scan(&n); err != nil {
			t.Fatalf("poll walsender: %v", err)
		}
		if n > 0 {
			break
		}
		select {
		case err := <-runErr:
			t.Fatalf("stream ended instead of retrying the terminated connection: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the stream never reconnected after the terminated walsender")
		}
		time.Sleep(200 * time.Millisecond)
	}
	applyPGDDL(t, sourceDSN, `INSERT INTO gc32 VALUES (3, 'c');`)

	var got error
	select {
	case got = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("stream did not refuse within 60s — the retry re-baselined and accepted the change (GC-32)")
	}
	if got == nil || !strings.Contains(got.Error(), "UNFORWARDED-SCHEMA-CHANGE") || !strings.Contains(got.Error(), `ADD CONSTRAINT "gc32_name_uq" UNIQUE (name)`) {
		t.Fatalf("stream ended with %v; want UNFORWARDED-SCHEMA-CHANGE naming the UNIQUE", got)
	}
}
