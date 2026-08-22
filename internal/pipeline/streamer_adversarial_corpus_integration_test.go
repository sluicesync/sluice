//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus — CDC rounds. The binlog and
// pgoutput decoders are DIFFERENT codecs from the bulk-copy readers
// (Bug-74 territory: identical sluice orchestration, different wire
// decode per family), so the same corpus rides both lanes:
//
//	row 1 — snapshot leg (bulk copy at stream cold-start)
//	row 2 — CDC INSERT leg (decoded from the replication wire)
//	UPDATE sentinels — CDC UPDATE leg (after-image decode)
//
// Every landed cell is ground-truthed against the real target by
// direct SQL, exactly as the cold-copy round does.

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// advStreamerExit reports (non-blockingly) whether Streamer.Run has
// already returned, and with what — a wait timeout with a dead
// streamer should say WHY the streamer died, not just "never landed".
func advStreamerExit(runErr <-chan error) string {
	select {
	case err := <-runErr:
		return fmt.Sprintf(" — Streamer.Run has EXITED: %v", err)
	default:
		return " (streamer still running)"
	}
}

// advCDCSplit partitions a corpus into the CDC-riding cells and the
// skipped ones. Every skip must carry a named reason; more than three
// skips fails — a quietly shrinking CDC corpus is exactly the
// gate-narrower-than-its-name failure shape.
func advCDCSplit(t *testing.T, cells []advCell) []advCell {
	t.Helper()
	ride := make([]advCell, 0, len(cells))
	skipped := 0
	for _, c := range cells {
		if c.cdcSkip != "" {
			skipped++
			t.Logf("CDC round skips %s/%s: %s", c.family, c.col, c.cdcSkip)
			continue
		}
		ride = append(ride, c)
	}
	if skipped > 3 {
		t.Fatalf("CDC round skips %d cells; floor allows at most 3 named skips — the CDC corpus is shrinking", skipped)
	}
	return ride
}

// TestStreamer_AdversarialCorpusCDC_MySQLToPostgres drives the corpus
// through the binlog decoder: snapshot row, CDC-INSERT row, and CDC
// UPDATE sentinels, each ground-truthed on the PG target.
func TestStreamer_AdversarialCorpusCDC_MySQLToPostgres(t *testing.T) {
	cells := advCorpusMySQLToPG()
	advAssertCorpusFloor(t, "mysql→pg cdc", cells, advM2PMinFamilies, advM2PMinCells)
	cdcCells := advCDCSplit(t, cells)

	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	advSeedMySQLCorpus(t, mysqlSourceDSN, "adv_corpus", cells, 1)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSourceDSN, TargetDSN: pgTargetDSN,
		StreamID: "adv-corpus-mysql-pg",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Snapshot leg: row 1 lands via bulk copy.
	if !advWaitRowVisible(t, "pgx", pgTargetDSN, "adv_corpus", 1, 120*time.Second) {
		t.Fatalf("snapshot leg: corpus row 1 never landed on the PG target%s", advStreamerExit(runErr))
	}
	t.Run("snapshot_leg", func(t *testing.T) {
		conn := advOpenConn(t, "pgx", pgTargetDSN)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 1, true)
	})

	// CDC INSERT leg: row 2 decoded from the binlog (the seedVal cells
	// arrive as a follow-up CDC UPDATE; wait for them to settle).
	advInsertMySQLCorpusRow(t, mysqlSourceDSN, "adv_corpus", cells, 2)
	if !advWaitRowVisible(t, "pgx", pgTargetDSN, "adv_corpus", 2, 60*time.Second) {
		t.Fatalf("CDC INSERT leg: corpus row 2 never landed on the PG target%s", advStreamerExit(runErr))
	}
	for _, c := range cdcCells {
		if c.seedVal == nil {
			continue
		}
		if !advWaitCellEquals(t, "pgx", pgTargetDSN, "adv_corpus", c, 2, nil, 60*time.Second) {
			t.Fatalf("CDC UPDATE (seed param) for %s never settled on row 2%s", c.col, advStreamerExit(runErr))
		}
	}
	t.Run("cdc_insert_leg", func(t *testing.T) {
		conn := advOpenConn(t, "pgx", pgTargetDSN)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 2, true)
	})

	// CDC UPDATE leg: rewrite sentinel columns with fresh adversarial
	// values and confirm the after-image decode lands each byte-exact.
	updates := []advCell{
		{family: "text", col: "t_emoji", probe: "%s", want: "moved 🦞 Ωmega"},
		{
			family: "decimal", col: "dec_max", probe: "%s::text",
			want: "0.000000000000000000000000000001",
		},
		{
			family: "float", col: "f8_17sig", probe: "encode(float8send(%s),'hex')",
			want: advFloat64Bits("0.9876543210987654321"),
		},
		{family: "json", col: "j_bigint", probe: "%s->>'n'", want: "9007199254740995"},
	}
	applyMySQLDDL(t, mysqlSourceDSN, `SET SESSION time_zone = '+00:00';
		UPDATE adv_corpus SET
			t_emoji = 'moved 🦞 Ωmega',
			dec_max = '0.000000000000000000000000000001',
			f8_17sig = 0.9876543210987654321,
			j_bigint = '{"n":9007199254740995}'
		WHERE id = 2;`)
	for _, u := range updates {
		u := u
		t.Run("cdc_update_leg/"+u.family+"/"+u.col, func(t *testing.T) {
			if !advWaitCellEquals(t, "pgx", pgTargetDSN, "adv_corpus", u, 2, nil, 60*time.Second) {
				t.Errorf("CDC UPDATE of %s never landed the expected value %q on the PG target", u.col, u.want)
			}
		})
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AdversarialCorpusCDC_PostgresToMySQL drives the corpus
// through the pgoutput decoder (REPLICA IDENTITY FULL) into a MySQL
// target: snapshot row, CDC-INSERT row, and CDC UPDATE sentinels.
func TestStreamer_AdversarialCorpusCDC_PostgresToMySQL(t *testing.T) {
	cells := advCorpusPGToMySQL()
	advAssertCorpusFloor(t, "pg→mysql cdc", cells, advP2MMinFamilies, advP2MMinCells)
	cdcCells := advCDCSplit(t, cells)

	pgSourceDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlTargetDSN, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	advSeedPGCorpus(t, pgSourceDSN, "adv_corpus", cells, 1)
	applyPGDDL(t, pgSourceDSN, "ALTER TABLE adv_corpus REPLICA IDENTITY FULL;")

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	streamer := &Streamer{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSourceDSN, TargetDSN: mysqlTargetDSN,
		StreamID: "adv-corpus-pg-mysql",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !advWaitRowVisible(t, "mysql", mysqlTargetDSN, "adv_corpus", 1, 120*time.Second) {
		t.Fatalf("snapshot leg: corpus row 1 never landed on the MySQL target%s", advStreamerExit(runErr))
	}
	t.Run("snapshot_leg", func(t *testing.T) {
		conn := advOpenConn(t, "mysql", mysqlTargetDSN, advMySQLUTCSession...)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 1, false)
	})

	advInsertPGCorpusRow(t, pgSourceDSN, "adv_corpus", cells, 2)
	if !advWaitRowVisible(t, "mysql", mysqlTargetDSN, "adv_corpus", 2, 60*time.Second) {
		t.Fatalf("CDC INSERT leg: corpus row 2 never landed on the MySQL target%s", advStreamerExit(runErr))
	}
	for _, c := range cdcCells {
		if c.seedVal == nil {
			continue
		}
		if !advWaitCellEquals(t, "mysql", mysqlTargetDSN, "adv_corpus", c, 2, advMySQLUTCSession, 60*time.Second) {
			t.Fatalf("CDC UPDATE (seed param) for %s never settled on row 2%s", c.col, advStreamerExit(runErr))
		}
	}
	t.Run("cdc_insert_leg", func(t *testing.T) {
		conn := advOpenConn(t, "mysql", mysqlTargetDSN, advMySQLUTCSession...)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 2, false)
	})

	// CDC UPDATE sentinels — including a numeric[][] rewrite, the
	// exact Bug-74 family on the CDC lane.
	updates := []advCell{
		{family: "text", col: "t_emoji", probe: "%s", want: "moved 🦞 Ωmega"},
		{family: "json", col: "jb_bigint", probe: "JSON_EXTRACT(%s,'$.n')", want: "9007199254740995"},
		{
			family: "array", col: "arr_num_2d",
			probe: "CONCAT(JSON_LENGTH(%s),'#',JSON_LENGTH(%s,'$[0]'),'#',JSON_UNQUOTE(JSON_EXTRACT(%s,'$[1][0]')))",
			want:  "2#2#7.125",
		},
	}
	applyPGDDL(t, pgSourceDSN, `UPDATE adv_corpus SET
			t_emoji = 'moved 🦞 Ωmega',
			jb_bigint = '{"n":9007199254740995}',
			arr_num_2d = '{{9.5,8.25},{7.125,6.0625}}'
		WHERE id = 2;`)
	for _, u := range updates {
		u := u
		t.Run("cdc_update_leg/"+u.family+"/"+u.col, func(t *testing.T) {
			if !advWaitCellEquals(t, "mysql", mysqlTargetDSN, "adv_corpus", u, 2, advMySQLUTCSession, 60*time.Second) {
				t.Errorf("CDC UPDATE of %s never landed the expected value %q on the MySQL target", u.col, u.want)
			}
		})
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
