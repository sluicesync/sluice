// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// GC-41 MEDIUM-1 pins: a warm resume refuses when the Postgres source slot
// has been acknowledged past the position the target persisted
// (SLOT-ACKED-PAST-TARGET-POSITION), and never refuses a stream this binary
// wrote — DDL mid-stream included.

const slotAckedPastMarker = "SLOT-ACKED-PAST-TARGET-POSITION"

// TestStreamer_PostgresToMySQL_WarmResumeRefusesASlotAckedPastTheTarget is
// the upgrade case: a Postgres → MySQL stream stopped on v0.156.6 or earlier
// with changes still in an apply batch left the slot acknowledged past them.
// The old binary's ack is simulated with pg_replication_slot_advance to the
// current WAL after five transactions the target never received. Before this
// door the resume was silently fast-forwarded past them (the mutation run
// that removes the door: the resume returns nil and the gap is gone); now it
// refuses, names both LSNs, and accepts only an
// acknowledgement of that exact slot position — which then resumes and skips
// the gap, as documented.
func TestStreamer_PostgresToMySQL_WarmResumeRefusesASlotAckedPastTheTarget(t *testing.T) {
	pgSrc, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	dsn := dsNewDatabase(t, root, "gc41r")

	const (
		table    = "gc41r_rows"
		streamID = "gc41r-mysql"
		slot     = "sluice_gc41r_mysql"
	)
	applyPGDDL(t, pgSrc, fmt.Sprintf(`
		CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 10) g;`, table, table))
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	newStreamer := func(accept string) *Streamer {
		return &Streamer{
			Source: pgEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: dsn,
			StreamID: streamID, SlotName: slot, PublicationName: "pub_gc41r_mysql",
			Filter:                      migcore.TableFilter{Include: []string{table}},
			ApplyBatchSize:              1000,
			AcceptSlotAckedPastPosition: accept,
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer("").Run(ctx1) }()
	if !waitForRowCountMySQLQuoted(t, dsn, table, 10, 2*time.Minute) {
		t.Fatal("cold start never copied the seed rows")
	}
	applyPGDDL(t, pgSrc, fmt.Sprintf("INSERT INTO %s VALUES (11, 'cdc')", table))
	if !waitForRowCountMySQLQuoted(t, dsn, table, 11, time.Minute) {
		t.Fatal("the first CDC row never landed")
	}
	cancel1()
	<-run1

	persisted, ok := readPersistedLSNMySQL(dsn, streamID)
	if !ok {
		t.Fatal("no persisted position after the first run")
	}
	// The gap: committed on the source, never applied, and — the old
	// binary's damage — acknowledged on the slot.
	insertRowsOneTxEach(t, pgSrc, table, 12, 16)
	advanceSlotTo(t, pgSrc, slot, currentWALLSN(t, pgSrc))
	confirmed, _ := readConfirmedFlushLSN(t, pgSrc, slot)
	if confirmed <= persisted {
		t.Fatalf("precondition: confirmed_flush_lsn %s is not past the persisted %s", confirmed, persisted)
	}

	err := runBounded(t, newStreamer(""))
	if err == nil || !strings.Contains(err.Error(), slotAckedPastMarker) {
		t.Fatalf("warm resume with the slot acked past the target returned %v; want %s", err, slotAckedPastMarker)
	}
	// What Streamer.Run returns is what a fleet leg's supervisor sees: the
	// sentinel must survive every wrap between the reader and here, or the
	// leg would be restarted into the same refusal forever.
	if refusalARestartRepeats(err) != ir.ErrSlotAckedPastTargetPosition { //nolint:errorlint // identity of the returned sentinel is the property
		t.Errorf("the refusal Streamer.Run returned does not carry ir.ErrSlotAckedPastTargetPosition: %v", err)
	}
	for _, want := range []string{confirmed.String(), persisted.String(), slot, "--restart-from-scratch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q: %v", want, err)
		}
	}
	if got := pollRowCountMySQLQuoted(dsn, table); got != 11 {
		t.Errorf("the refused resume changed the target: %d rows; want 11", got)
	}

	if err := runBounded(t, newStreamer("0/1")); err == nil || !strings.Contains(err.Error(), slotAckedPastMarker) {
		t.Fatalf("an acknowledgement naming a different LSN returned %v; want the refusal", err)
	}

	// The exact acknowledgement resumes; the gap is skipped (that is what
	// the operator acknowledged), and changes after it flow.
	ctx4, cancel4 := context.WithCancel(context.Background())
	run4 := make(chan error, 1)
	go func() { run4 <- newStreamer(confirmed.String()).Run(ctx4) }()
	defer func() {
		cancel4()
		<-run4
	}()
	applyPGDDL(t, pgSrc, fmt.Sprintf("INSERT INTO %s VALUES (17, 'after')", table))
	if !waitForRowCountMySQLQuoted(t, dsn, table, 12, time.Minute) {
		t.Fatalf("the acknowledged resume never delivered the post-gap row (target %d rows)", pollRowCountMySQLQuoted(dsn, table))
	}
	time.Sleep(2 * time.Second)
	if got := pollRowCountMySQLQuoted(dsn, table); got != 12 {
		t.Errorf("after the acknowledged resume the target holds %d rows; want 12 (the 5-row gap skipped as acknowledged)", got)
	}
}

// TestStreamer_PostgresToPostgres_WarmResumeRefusesASlotAckedPastTheTarget
// pins the door's two other arms on a Postgres target.
//
//   - "slot recreated after the position": the --restart-from-scratch
//     stale-row shape. A restart re-copies onto a slot it creates at "now"
//     (the snapshot open refuses an existing slot, whose remedy is to drop
//     it) and keeps the old sluice_cdc_state row until its copy finishes; an
//     interruption before then leaves that old row next to the new slot, and
//     a plain rerun used to warm-resume the old LSN — fast-forwarded to the
//     new slot's position with the copy incomplete. Reproduced here as the
//     end state (drop and recreate the slot after a stopped stream), not by
//     racing a kill against a copy.
//   - "DDL mid-stream is not a false positive", on each apply path: a
//     SchemaSnapshot carries its RelationMessage's WAL start (pgoutput
//     stamps 0/0), below every end the stream has persisted and released.
//     The stream is stopped INSIDE the DDL transaction — its schema change
//     applied, its row held on the target by a trigger, the slot already
//     acknowledged into an earlier transaction — which is exactly where a
//     path that persisted a mid-transaction position would leave the target
//     behind its slot. The resume must not refuse and must deliver every row.
//     Paths: serial batch, per-change, concurrent lanes (the barrier).
func TestStreamer_PostgresToPostgres_WarmResumeRefusesASlotAckedPastTheTarget(t *testing.T) {
	srcDSN, dstDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	pgEng, _ := engines.Get("postgres")
	newStreamer := func(id string, batch, lanes int, tables ...string) *Streamer {
		return &Streamer{
			Source: pgEng, Target: pgEng, SourceDSN: srcDSN, TargetDSN: dstDSN,
			StreamID: id, SlotName: "sluice_" + id, PublicationName: "pub_" + id,
			Filter:           migcore.TableFilter{Include: tables},
			ApplyBatchSize:   batch,
			ApplyConcurrency: lanes,
		}
	}

	t.Run("slot recreated after the position", func(t *testing.T) {
		const id, table = "gc41_recreate", "gc41_recreate_rows"
		applyPGDDL(t, srcDSN, fmt.Sprintf(`
			CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
			INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 5) g;`, table, table))
		ctx, cancel := context.WithCancel(context.Background())
		run := make(chan error, 1)
		go func() { run <- newStreamer(id, 1000, 1, table).Run(ctx) }()
		if !waitForRowCount(t, dstDSN, table, 5, 2*time.Minute) {
			t.Fatal("cold start never copied the seed rows")
		}
		applyPGDDL(t, srcDSN, fmt.Sprintf("INSERT INTO %s VALUES (6, 'cdc')", table))
		if !waitForRowCount(t, dstDSN, table, 6, time.Minute) {
			t.Fatal("the first CDC row never landed")
		}
		cancel()
		<-run

		insertRowsOneTxEach(t, srcDSN, table, 7, 9)
		recreateSlot(t, srcDSN, "sluice_"+id)
		persisted, _ := readPersistedLSNPG(dstDSN, id)
		if c, _ := readConfirmedFlushLSN(t, srcDSN, "sluice_"+id); c <= persisted {
			t.Fatalf("precondition: the recreated slot's confirmed_flush_lsn %s is not past the persisted %s", c, persisted)
		}
		err := runBounded(t, newStreamer(id, 1000, 1, table))
		if err == nil || !strings.Contains(err.Error(), slotAckedPastMarker) {
			t.Fatalf("resume onto a slot recreated after the position returned %v; want %s", err, slotAckedPastMarker)
		}
		if refusalARestartRepeats(err) != ir.ErrSlotAckedPastTargetPosition { //nolint:errorlint // identity of the returned sentinel is the property
			t.Errorf("the refusal Streamer.Run returned does not carry ir.ErrSlotAckedPastTargetPosition: %v", err)
		}
	})

	for _, path := range []struct {
		name         string
		batch, lanes int
	}{
		{"serial batch", 1000, 1},
		{"per-change", 1, 1},
		{"concurrent lanes", 1000, 4},
	} {
		t.Run("DDL mid-stream is not a false positive/"+path.name, func(t *testing.T) {
			tag := strings.ReplaceAll(path.name, " ", "_")
			tag = strings.ReplaceAll(tag, "-", "_")
			id := "gc41_ddl_" + tag
			table, other := id+"_rows", id+"_other"
			applyPGDDL(t, srcDSN, fmt.Sprintf(`
				CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL);
				CREATE TABLE %s (id INT PRIMARY KEY);
				INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 5) g;`, table, other, table))
			slot := "sluice_" + id
			ctx, cancel := context.WithCancel(context.Background())
			run := make(chan error, 1)
			go func() { run <- newStreamer(id, path.batch, path.lanes, table, other).Run(ctx) }()
			if !waitForRowCount(t, dstDSN, table, 5, 2*time.Minute) {
				t.Fatal("cold start never copied the seed rows")
			}
			// Hold the DDL transaction's row on the TARGET (not its schema
			// change), so the stream can be stopped between the two.
			fn := id + "_block"
			applyPGDDL(t, dstDSN, fmt.Sprintf(`
				CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN IF NEW.id = 999 THEN PERFORM pg_sleep(300); END IF; RETURN NEW; END $$;
				CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s();
				-- ALWAYS: the applier runs with session_replication_role = replica.
				ALTER TABLE %s ENABLE ALWAYS TRIGGER %s;`, fn, fn, table, fn, table, fn))

			// T2 opens first: its schema change and row sit in WAL before T1.
			src, err := sql.Open("pgx", srcDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = src.Close() }()
			t2, err := src.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := t2.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN v TYPE VARCHAR(64)", table)); err != nil {
				t.Fatal(err)
			}
			if _, err := t2.Exec(fmt.Sprintf("INSERT INTO %s VALUES (999, 'ddl')", table)); err != nil {
				t.Fatal(err)
			}
			preT1 := currentWALLSN(t, srcDSN)
			applyPGDDL(t, srcDSN, fmt.Sprintf("INSERT INTO %s VALUES (1)", other)) // T1
			if !waitForRowCount(t, dstDSN, other, 1, time.Minute) {
				t.Fatal("T1 never landed")
			}
			// The slot is acknowledged into T1 (the reader acks T1's commit
			// LSN, one commit record short of the end the target persisted).
			if !waitSlotPasses(t, srcDSN, slot, preT1, 3*10*time.Second) {
				t.Fatalf("the slot never passed the WAL before T1 (%s)", preT1)
			}
			if err := t2.Commit(); err != nil {
				t.Fatal(err)
			}
			// The applier has passed T2's SchemaSnapshot (the first boundary
			// after a cold start is not forwarded, ADR-0091 §3, but it flows
			// through the applier all the same) and is held inside T2's row.
			if !waitForPGScalar(t, dstDSN,
				"SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'",
				1, time.Minute) {
				t.Fatalf("the DDL transaction's row never reached the blocking trigger on the target (activity: %s; target rows %d)",
					pgActivity(dstDSN), pgScalarCount(dstDSN, "SELECT COUNT(*) FROM "+table))
			}
			time.Sleep(2 * time.Second)
			cancel()
			<-run
			p, _ := readPersistedLSNPG(dstDSN, id)
			c, _ := readConfirmedFlushLSN(t, srcDSN, slot)
			t.Logf("stopped inside the DDL transaction: persisted %s, confirmed_flush_lsn %s, T1 began after %s", p, c, preT1)
			applyPGDDL(t, dstDSN, fmt.Sprintf("DROP TRIGGER %s ON %s; DROP FUNCTION %s();", fn, table, fn))

			ctx2, cancel2 := context.WithCancel(context.Background())
			run2 := make(chan error, 1)
			go func() { run2 <- newStreamer(id, path.batch, path.lanes, table, other).Run(ctx2) }()
			landed := waitForRowCount(t, dstDSN, table, 6, time.Minute)
			cancel2()
			if err := <-run2; err != nil && strings.Contains(err.Error(), slotAckedPastMarker) {
				t.Fatalf("a stream this binary wrote was refused after DDL mid-stream (a false positive): %v", err)
			}
			if !landed {
				t.Fatalf("the DDL transaction's row never landed after the resume (target %d rows; want 6)",
					pgScalarCount(dstDSN, "SELECT COUNT(*) FROM "+table))
			}
		})
	}
}

// TestStreamer_PostgresToMySQL_CleanStopWithForeignWALDoesNotRefuse pins the
// door's upgrade false-positive question: WAL the slot's publication does not
// carry — another database on the same server, an unpublished table in the
// source database, the source heartbeat table — must never put the slot past
// the persisted position, or every idle stream would refuse on resume.
//
// Ground truth behind it (2026-09-30, the released v0.156.6 binary, PG 16
// and PG 14 sources, MySQL 8 target, --source-heartbeat-interval 2s, 40 s of
// other-database and unpublished-table writes, then a draining `sync stop`):
// confirmed_flush_lsn stayed at or behind source_position every time (PG 16:
// 0/1DA8AF8 vs 0/1DA8B28; PG 14: 0/17E1F48 vs 0/17E1F78), and this binary's
// resume did not refuse. The old reader's "streamed" LSN advanced only on a
// decoded CommitMessage — never on a primary keepalive's ServerWALEnd — and it
// acked that commit's START while the applier persisted its END; foreign WAL
// is never decoded, and PG 14's empty BEGIN/COMMIT for unpublished writes
// reached the applier and were persisted like any other boundary. The old
// binary cannot run in CI, so this pins the same workload on this binary.
func TestStreamer_PostgresToMySQL_CleanStopWithForeignWALDoesNotRefuse(t *testing.T) {
	pgSrc, pgOther, pgCleanup := startPostgresLogical(t) // pgOther: a second database on the same server
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	dsn := dsNewDatabase(t, root, "gc41c")

	const table, streamID, slot = "gc41c_rows", "gc41c-mysql", "sluice_gc41c_mysql"
	applyPGDDL(t, pgSrc, fmt.Sprintf(`
		CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
		CREATE TABLE gc41c_unpublished (id SERIAL PRIMARY KEY, v TEXT);
		INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 5) g;`, table, table))
	applyPGDDL(t, pgOther, `CREATE TABLE gc41c_foreign (id SERIAL PRIMARY KEY, v TEXT);`)
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	newStreamer := func() *Streamer {
		return &Streamer{
			Source: pgEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: dsn,
			StreamID: streamID, SlotName: slot, PublicationName: "pub_gc41c_mysql",
			Filter:                  migcore.TableFilter{Include: []string{table}},
			ApplyBatchSize:          1000,
			SourceHeartbeatInterval: time.Second,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	run := make(chan error, 1)
	go func() { run <- newStreamer().Run(ctx) }()
	if !waitForRowCountMySQLQuoted(t, dsn, table, 5, 2*time.Minute) {
		t.Fatal("cold start never copied the seed rows")
	}
	insertRowsOneTxEach(t, pgSrc, table, 6, 8)
	if !waitForRowCountMySQLQuoted(t, dsn, table, 8, time.Minute) {
		t.Fatal("CDC rows never landed")
	}
	// ~2.5 keepalives of WAL the publication does not carry.
	for deadline := time.Now().Add(25 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		applyPGDDL(t, pgOther, "INSERT INTO gc41c_foreign (v) SELECT 'x' FROM generate_series(1, 50)")
		applyPGDDL(t, pgSrc, "INSERT INTO gc41c_unpublished (v) VALUES ('y')")
	}
	cancel() // idle and fully applied: nothing is buffered, so this is a clean stop
	<-run

	persisted, _ := readPersistedLSNMySQL(dsn, streamID)
	confirmed, _ := readConfirmedFlushLSN(t, pgSrc, slot)
	if confirmed > persisted {
		t.Fatalf("after a clean stop, foreign / unpublished / heartbeat WAL put confirmed_flush_lsn %s past the "+
			"persisted %s — every idle stream would refuse on resume", confirmed, persisted)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()
	applyPGDDL(t, pgSrc, fmt.Sprintf("INSERT INTO %s VALUES (9, 'after')", table))
	landed := waitForRowCountMySQLQuoted(t, dsn, table, 9, time.Minute)
	cancel2()
	if err := <-run2; err != nil && strings.Contains(err.Error(), slotAckedPastMarker) {
		t.Fatalf("a cleanly stopped stream was refused on resume (a false positive): %v", err)
	}
	if !landed {
		t.Fatal("the resumed stream never delivered the next row")
	}
}

// runBounded runs s to completion under a timeout and returns its error.
func runBounded(t *testing.T, s *Streamer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return s.Run(ctx)
}

// waitSlotPasses waits up to d for the slot's confirmed_flush_lsn to reach lsn.
func waitSlotPasses(t *testing.T, dsn, slot string, lsn pglogrepl.LSN, d time.Duration) bool {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(time.Second) {
		if c, ok := readConfirmedFlushLSN(t, dsn, slot); ok && c >= lsn {
			return true
		}
	}
	return false
}

// advanceSlotTo moves the slot's confirmed_flush_lsn to lsn, as an older
// binary's streamed-LSN ack would have, retrying while a stopped stream's
// walsender still holds the slot.
func advanceSlotTo(t *testing.T, dsn, slot string, lsn pglogrepl.LSN) {
	t.Helper()
	retryWhileSlotBusy(t, dsn, func(db *sql.DB) error {
		_, err := db.Exec("SELECT pg_replication_slot_advance($1, $2::pg_lsn)", slot, lsn.String())
		return err
	})
}

// recreateSlot drops the slot and creates it again at the current WAL.
func recreateSlot(t *testing.T, dsn, slot string) {
	t.Helper()
	retryWhileSlotBusy(t, dsn, func(db *sql.DB) error {
		_, err := db.Exec("SELECT pg_drop_replication_slot($1)", slot)
		return err
	})
	applyPGDDL(t, dsn, fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slot))
}

func retryWhileSlotBusy(t *testing.T, dsn string, op func(*sql.DB) error) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(90 * time.Second)
	for {
		err := op(db)
		if err == nil {
			return
		}
		if !strings.Contains(err.Error(), "active") || time.Now().After(deadline) {
			t.Fatalf("slot operation: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// pgActivity renders the non-idle backends, for a failure message.
func pgActivity(dsn string) string {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err.Error()
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT COALESCE(application_name,''), COALESCE(state,''), COALESCE(wait_event,''), LEFT(COALESCE(query,''), 80)
		FROM pg_stat_activity WHERE state IS DISTINCT FROM 'idle' AND pid <> pg_backend_pid()`)
	if err != nil {
		return err.Error()
	}
	defer func() { _ = rows.Close() }()
	var b strings.Builder
	for rows.Next() {
		var app, state, wait, q string
		if rows.Scan(&app, &state, &wait, &q) == nil {
			fmt.Fprintf(&b, "[%s|%s|%s|%s] ", app, state, wait, q)
		}
	}
	return b.String()
}
