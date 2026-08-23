//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adversarial value-fidelity corpus — the SQLite trigger-CDC lane
// (ADR-0135). The capture trigger's json_object((typeof, text/hex))
// encoding and the poll-side reconstruction are a DIFFERENT codec from
// the cold-start file reader (Bug-74 territory: the change-log is a
// store the values round-trip THROUGH — a codec per the new-surface
// checklist), so the same corpus rides:
//
//	row 1 — snapshot leg (cold-start bulk copy at stream start)
//	row 2 — CDC INSERT leg (capture trigger → change-log → poll →
//	        reconstruct → apply)
//	UPDATE sentinels — CDC UPDATE after-image decode
//	DELETE leg — the before-image key path
//
// Every landed cell is ground-truthed against the real PG target by
// direct SQL, exactly as the cold-copy round does.

package pipeline

import (
	"context"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding + mutating the temp source file

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// TestStreamer_AdversarialCorpusCDC_SQLiteTriggerToPostgres drives the
// SQLite corpus through the trigger-CDC codec into PG.
func TestStreamer_AdversarialCorpusCDC_SQLiteTriggerToPostgres(t *testing.T) {
	cells := advSQLiteCorpusView(t, "pg")
	advAssertCorpusFloor(t, "sqlite-trigger→pg cdc", cells, advSQLiteMinFamilies, advSQLiteMinCellsPG)
	cdcCells := advCDCSplit(t, cells)

	// Seed row 1 and enable WAL (ADR-0135 §5) BEFORE trigger setup.
	src := advSeedSQLiteCorpus(t, cells, 1)
	func() {
		db := advOpenSQLite(t, src)
		defer func() { _ = db.Close() }()
		advExecSQLite(t, db, `PRAGMA journal_mode=WAL`)
	}()
	if _, err := sqlitetrigger.Setup(context.Background(), src, sqlitetrigger.SetupOptions{
		Tables: []string{"adv_corpus"},
	}); err != nil {
		t.Fatalf("sqlitetrigger.Setup: %v", err)
	}

	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source: srcEng, Target: pgEng,
		SourceDSN: src, TargetDSN: pgTargetDSN,
		StreamID: "adv-corpus-sqlite-trigger-pg",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Snapshot leg: row 1 lands via the cold-start bulk copy.
	if !advWaitRowVisible(t, "pgx", pgTargetDSN, "adv_corpus", 1, 120*time.Second) {
		t.Fatalf("snapshot leg: corpus row 1 never landed on the PG target%s", advStreamerExit(runErr))
	}
	t.Run("snapshot_leg", func(t *testing.T) {
		conn := advOpenConn(t, "pgx", pgTargetDSN)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 1, true)
	})

	// CDC INSERT leg: row 2 rides capture → poll → reconstruct → apply.
	// The seedVal cells arrive as follow-up captured UPDATEs.
	func() {
		db := advOpenSQLite(t, src)
		defer func() { _ = db.Close() }()
		// Insert with the FULL cell list (the table's NOT NULL columns),
		// probe with the CDC view.
		advExecSQLite(t, db, advBuildInsert("adv_corpus", cells, 2))
		advSeedParams(t, db, "adv_corpus", cells, 2, false)
	}()
	if !advWaitRowVisible(t, "pgx", pgTargetDSN, "adv_corpus", 2, 90*time.Second) {
		t.Fatalf("CDC INSERT leg: corpus row 2 never landed on the PG target%s", advStreamerExit(runErr))
	}
	for _, c := range cdcCells {
		if c.seedVal == nil {
			continue
		}
		if !advWaitCellEquals(t, "pgx", pgTargetDSN, "adv_corpus", c, 2, nil, 90*time.Second) {
			t.Fatalf("CDC UPDATE (seed param) for %s never settled on row 2%s", c.col, advStreamerExit(runErr))
		}
	}
	t.Run("cdc_insert_leg", func(t *testing.T) {
		conn := advOpenConn(t, "pgx", pgTargetDSN)
		advProbeCells(t, conn, "adv_corpus", cdcCells, 2, true)
	})

	// CDC UPDATE leg: rewrite sentinel columns with fresh adversarial
	// values; the after-image decode must land each exactly. The big-int
	// sentinel is 2^53 + 3 — a json_object number capture would land …994.
	updates := []advCell{
		{family: "integer", col: "i53_plus1", probe: "%s::text", want: "9007199254740995"},
		{family: "text", col: "t_emoji", probe: "%s", want: "moved 🦞 Ωmega"},
		{
			family: "float", col: "f8_17sig", probe: "encode(float8send(%s),'hex')",
			want: advFloat64Bits("0.9876543210987654321"),
		},
		{
			family: "binary", col: "b_nulrun",
			probe: "encode(%s,'hex') || '#' || octet_length(%s)::text", want: "beef00ad#4",
		},
	}
	func() {
		db := advOpenSQLite(t, src)
		defer func() { _ = db.Close() }()
		advExecSQLite(t, db, `UPDATE adv_corpus SET
			i53_plus1 = 9007199254740995,
			t_emoji = 'moved 🦞 Ωmega',
			f8_17sig = 0.9876543210987654321,
			b_nulrun = x'BEEF00AD'
			WHERE id = 2`)
	}()
	for _, u := range updates {
		u := u
		t.Run("cdc_update_leg/"+u.family+"/"+u.col, func(t *testing.T) {
			if !advWaitCellEquals(t, "pgx", pgTargetDSN, "adv_corpus", u, 2, nil, 90*time.Second) {
				t.Errorf("CDC UPDATE of %s never landed the expected value %q on the PG target%s",
					u.col, u.want, advStreamerExit(runErr))
			}
		})
	}

	// CDC DELETE leg: the before-image key path removes row 2.
	func() {
		db := advOpenSQLite(t, src)
		defer func() { _ = db.Close() }()
		advExecSQLite(t, db, `DELETE FROM adv_corpus WHERE id = 2`)
	}()
	deleted := func() bool {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if !advWaitRowVisible(t, "pgx", pgTargetDSN, "adv_corpus", 2, time.Second) {
				return true
			}
			time.Sleep(250 * time.Millisecond)
		}
		return false
	}()
	if !deleted {
		t.Errorf("CDC DELETE leg: row 2 still present on the PG target%s", advStreamerExit(runErr))
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
