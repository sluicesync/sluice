// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestStreamer_PostgresIdleSource_SlotFollowsKeepaliveBoundary is the GC-41
// (j) pin. A stream whose tables are idle while WAL flows elsewhere on the
// server must still move its slot: before the fix, Postgres 15+ skipped
// every transaction outside the publication (pgoutput drops empty
// transactions), the reader decoded nothing, no position was persisted, and
// confirmed_flush_lsn sat at the last user-table commit while the server
// retained WAL behind it without bound — measured 5.69 MB after 135 s on
// PG 15, PG 16 → PG and PG 16 → MySQL. Postgres 14 decodes the ADR-0061
// heartbeat's empty BEGIN/COMMIT, which is why the heartbeat worked there.
//
// The fix stands a primary keepalive's ServerWALEnd in for a commit while no
// transaction is open (postgres/cdc_keepalive_boundary.go). The pin is the
// apply-path roster for that boundary: a boundary-only transaction must be
// PERSISTED by every apply path on both target families, because only a
// persisted position can raise the slot's ack ceiling —
//
//   - per-change (ApplyBatchSize 1): persistSourceTxCommit;
//   - serial batch (W=1): the batch loop's writeBoundaryOnly on an empty
//     transaction (CheckpointOnlyAtTxBoundary, set on both engines);
//   - concurrent lanes (W=4): the frontier's RecordTxBoundary and the
//     idle checkpoint.
//
// The barrier path is not in the roster: it carries Truncate,
// SchemaSnapshot and keyless rows, never a Tx event. The broker and the
// backup capture lanes consume the same events but persist nothing to a
// target control row; their release is the backup chain's manifest
// EndPosition, which a boundary advances like any TxCommit.
//
// Independent evidence on both sides: the source server's own
// pg_current_wal_lsn() and pg_replication_slots, and the position the
// TARGET persisted. The slot must pass the WAL position read once the
// stream is idle, and must never pass the target's persisted position.
//
// PG 14 runs as the control, twice: with the heartbeat on (its empty
// transactions advance the slot with or without the fix) and off (only
// the keepalive boundary can).
func TestStreamer_PostgresIdleSource_SlotFollowsKeepaliveBoundary(t *testing.T) {
	pg16Src, pg16Dst, pg16Cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 16)
	defer pg16Cleanup()
	pg14Src, pg14Dst, pg14Cleanup := startPostgresLogicalImage(t, "postgres:14", 8)
	defer pg14Cleanup()
	myRoot, _, myCleanup := startMySQL(t)
	defer myCleanup()

	assertServerMajor(t, pg16Src, 16)
	assertServerMajor(t, pg14Src, 14)

	// The reader's premise check logs at ERROR; collect WARN and above.
	logs := &logcapture.Buffer{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prevLogger)

	// Foreign WAL: a database no stream reads from, written to throughout.
	stopNoise := make(chan struct{})
	var noise sync.WaitGroup
	for _, src := range []string{pg16Src, pg14Src} {
		noiseDSN := foreignWALDatabase(t, src)
		noise.Add(1)
		go func() {
			defer noise.Done()
			writeForeignWAL(noiseDSN, stopNoise)
		}()
	}
	defer func() {
		close(stopNoise)
		noise.Wait()
	}()

	type idleCase struct {
		name      string
		src, dst  string
		mysql     bool
		batch, w  int
		heartbeat bool
	}
	var cases []idleCase
	for _, p := range []struct {
		name     string
		batch, w int
	}{{"per_change", 1, 1}, {"serial", 1000, 1}, {"lanes", 1000, 4}} {
		cases = append(
			cases,
			idleCase{name: "pg16_to_pg_" + p.name, src: pg16Src, dst: pg16Dst, batch: p.batch, w: p.w},
			idleCase{name: "pg16_to_mysql_" + p.name, src: pg16Src, mysql: true, batch: p.batch, w: p.w},
		)
	}
	cases = append(
		cases,
		idleCase{name: "pg14_to_pg_lanes_heartbeat", src: pg14Src, dst: pg14Dst, batch: 1000, w: 4, heartbeat: true},
		idleCase{name: "pg14_to_pg_lanes", src: pg14Src, dst: pg14Dst, batch: 1000, w: 4},
	)

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	t.Run("roster", func(t *testing.T) {
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				id := "gc41j_" + c.name
				table := id
				slot := "sluice_" + id
				// Each stream gets its own source database. Not hygiene: on
				// PG 14 every transaction in the slot's database reaches it
				// as an empty BEGIN/COMMIT, so a neighbour's heartbeat or DDL
				// would advance a case meant to have nothing decoded at all.
				src := pgNewDatabase(t, c.src, id+"_src")
				applyPGDDL(t, src, fmt.Sprintf(`
					CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
					INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 5) g;`, table, table))

				// And its own target database: two appliers racing CREATE
				// TABLE IF NOT EXISTS for one control table is a Postgres
				// catalog race (42710), not what this test grades.
				dst := c.dst
				if !c.mysql {
					dst = pgNewDatabase(t, c.dst, id+"_dst")
				}
				s := &Streamer{
					Source: pgEng, Target: pgEng, SourceDSN: src, TargetDSN: dst,
					StreamID: id, SlotName: slot, PublicationName: "pub_" + id,
					Filter:           migcore.TableFilter{Include: []string{table}},
					ApplyBatchSize:   c.batch,
					ApplyConcurrency: c.w,
				}
				count := func() int { return pollRowCount(dst, table) }
				persisted := func() (pglogrepl.LSN, bool) { return readPersistedLSNPG(dst, id) }
				if c.mysql {
					s.Target = myEng
					s.TargetDSN = dsNewDatabase(t, myRoot, id)
					count = func() int { return pollRowCountMySQLQuoted(s.TargetDSN, table) }
					persisted = func() (pglogrepl.LSN, bool) { return readPersistedLSNMySQL(s.TargetDSN, id) }
				}
				if c.heartbeat {
					s.SourceHeartbeatInterval = time.Second
					s.SourceHeartbeatTableName = "hb_" + id
				}

				ctx, cancel := context.WithCancel(context.Background())
				runErr := make(chan error, 1)
				go func() { runErr <- s.Run(ctx) }()
				defer func() {
					cancel()
					<-runErr
				}()
				waitCount := func(n int) {
					t.Helper()
					for deadline := time.Now().Add(2 * time.Minute); count() < n; time.Sleep(200 * time.Millisecond) {
						select {
						case err := <-runErr:
							runErr <- err // the deferred stop still joins on it
							t.Fatalf("the stream exited before the target reached %d rows: %v", n, err)
						default:
						}
						if time.Now().After(deadline) {
							t.Fatalf("the target never reached %d rows", n)
						}
					}
				}
				waitCount(5)
				// One CDC row, so the stream has a persisted CDC position
				// and is then idle.
				applyPGDDL(t, src, fmt.Sprintf("INSERT INTO %s VALUES (6, 'cdc')", table))
				waitCount(6)

				idleFrom := currentWALLSN(t, src)
				idleAt := time.Now()
				// A boundary needs keepaliveBoundaryInterval (10 s) since the
				// CDC row's commit, then a persist, a ceiling read-back (5 s)
				// and a standby status (10 s): ~25 s, bounded here at 4.5
				// keepalives for a loaded -race runner.
				deadline := idleAt.Add(45 * time.Second)
				compared := 0
				for {
					conf, cok := readConfirmedFlushLSN(t, src, slot)
					p, pok := persisted()
					if cok && pok {
						compared++
						if conf > p {
							t.Fatalf("confirmed_flush_lsn %s passed the target's persisted position %s — the slot "+
								"was acked past what the target holds (GC-41 (a))", conf, p)
						}
					}
					if cok && conf >= idleFrom {
						t.Logf("slot passed the idle-time WAL %s after %s (confirmed %s, persisted %s)",
							idleFrom, time.Since(idleAt).Round(time.Second), conf, p)
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s: confirmed_flush_lsn is %s, still behind the WAL position %s read when the "+
							"stream went idle, %s later (target persisted %s) — an idle stream's slot retains the "+
							"server's WAL without bound (GC-41 (j))", c.name, conf, idleFrom, time.Since(idleAt).Round(time.Second), p)
					}
					time.Sleep(time.Second)
				}
				if compared == 0 {
					t.Fatal("anti-vacuity: no sample read both the slot and the target's persisted position")
				}
				if got := count(); got != 6 {
					t.Errorf("the idle target holds %d rows; want 6 — a boundary-only transaction must apply nothing", got)
				}

				// Commits after keepalive boundaries: each lands once a boundary
				// has been emitted (the slot just passed one, and the second
				// waits out the boundary interval), so the reader's premise
				// check runs against the real walsender, and the rows must
				// still all arrive.
				applyPGDDL(t, src, fmt.Sprintf("INSERT INTO %s VALUES (7, 'after-boundary')", table))
				time.Sleep(12 * time.Second)
				applyPGDDL(t, src, fmt.Sprintf("INSERT INTO %s VALUES (8, 'after-boundary')", table))
				waitCount(8)
			})
		}
	})
	if strings.Contains(logs.String(), postgres.KeepaliveBoundaryPassedCommitMarker) {
		t.Errorf("a stream logged %s — the walsender sent a keepalive past the commit of a transaction it "+
			"delivered afterwards, the premise the keepalive boundary rests on:\n%s",
			postgres.KeepaliveBoundaryPassedCommitMarker, logs.String())
	}
}

// assertServerMajor fails the test unless dsn's server is major version
// major: the cases here are graded by version, so an image tag that drifted
// would make them measure something else.
func assertServerMajor(t *testing.T, dsn string, major int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v int
	if err := db.QueryRow("SELECT current_setting('server_version_num')::int").Scan(&v); err != nil {
		t.Fatalf("server_version_num: %v", err)
	}
	if v/10000 != major {
		t.Fatalf("server_version_num %d; this case needs a Postgres %d server", v, major)
	}
}

// pgNewDatabase creates db on the server dsn points at and returns its DSN.
func pgNewDatabase(t *testing.T, dsn, db string) string {
	t.Helper()
	c, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Exec("CREATE DATABASE " + db); err != nil {
		t.Fatalf("create database %s: %v", db, err)
	}
	out, err := buildPGDSN(dsn, db)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// foreignWALDatabase creates a database on the server dsn points at that no
// stream reads, with one table to write to, and returns its DSN.
func foreignWALDatabase(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE DATABASE noise_db"); err != nil {
		t.Fatalf("create noise_db: %v", err)
	}
	noiseDSN, err := buildPGDSN(dsn, "noise_db")
	if err != nil {
		t.Fatal(err)
	}
	applyPGDDL(t, noiseDSN, "CREATE TABLE n (id BIGSERIAL PRIMARY KEY, v TEXT NOT NULL)")
	return noiseDSN
}

// writeForeignWAL writes ~40 KB of WAL every 200 ms to noiseDSN until stop
// closes — the measured shape of the defect's "WAL elsewhere on the server".
func writeForeignWAL(noiseDSN string, stop <-chan struct{}) {
	db, err := sql.Open("pgx", noiseDSN)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			_, _ = db.Exec("INSERT INTO n (v) SELECT repeat('z', 200) FROM generate_series(1, 100)")
		}
	}
}
