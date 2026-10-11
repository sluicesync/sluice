//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MARIADB-NOLOG-UNSIGNED end to end: a MariaDB source at its default
// binlog_row_metadata=NO_LOG records no signedness in its TABLE_MAPs, and the
// binlog reader decoded every UNSIGNED integer above its signed maximum as a
// negative number. These pins drive the lanes that read that binlog — a sync
// (cold copy, live CDC, a warm resume that replays a lagging window), a
// `backup incremental` and a `backup stream` (each chain restored) — and
// grade what the target holds.
//
// The independent expected value is the SOURCE row, read back from MariaDB
// as text: what the server holds, not anything sluice decoded. The key is an
// UNSIGNED integer above its signed maximum, so a misdecoded key is graded as
// well as a misdecoded value — an UPDATE or DELETE whose key decoded negative
// matched no target row and was silently tolerated as a replay.
//
// The reader-level family × metadata × server matrix is
// TestCDCSignedness_EveryNumericFamily (internal/engines/mysql).

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// unsignedDDL keys the table by an INT UNSIGNED, which every target holds
// (Postgres: bigint), so a key above 2^31 discriminates; bu is the BIGINT
// UNSIGNED a Postgres target holds only through a numeric override.
const unsignedDDL = `CREATE TABLE u (
	id INT UNSIGNED NOT NULL PRIMARY KEY,
	tu TINYINT UNSIGNED, su SMALLINT UNSIGNED, mu MEDIUMINT UNSIGNED,
	iu INT UNSIGNED, bu BIGINT UNSIGNED, note VARCHAR(16)
) ENGINE=InnoDB;`

// unsignedRow is an INSERT of one row: at=max writes every type's maximum,
// at=half its signed maximum plus one — the two values a signed decode turns
// negative.
func unsignedRow(id, at, note string) string {
	vals := map[string]string{
		"max":  "255, 65535, 16777215, 4294967295, 18446744073709551615",
		"half": "128, 32768, 8388608, 2147483648, 9223372036854775808",
	}[at]
	return "INSERT INTO u VALUES (" + id + ", " + vals + ", '" + note + "');\n"
}

const (
	idMax     = "4294967295"
	idMaxLess = "4294967294"
	idHalf    = "2147483648"
	idHalfUp  = "2147483649"
)

const unsignedSourceQuery = "SELECT CAST(id AS CHAR), CAST(tu AS CHAR), CAST(su AS CHAR), CAST(mu AS CHAR), " +
	"CAST(iu AS CHAR), CAST(bu AS CHAR), note FROM u ORDER BY id"

const unsignedPGQuery = "SELECT id::text, tu::text, su::text, mu::text, iu::text, bu::text, note FROM u ORDER BY id"

// assertUnsignedConverges polls until the target's u equals the MariaDB
// source's u, row for row as text, then asserts it. driver is the target's
// database/sql driver; targetQuery renders the same columns as text.
func assertUnsignedConverges(t *testing.T, what, sourceDSN, driver, targetDSN, targetQuery string, timeout time.Duration) {
	t.Helper()
	want := strings.Join(mysqlRowsAsText(t, sourceDSN, unsignedSourceQuery), "\n")
	db, err := sql.Open(driver, targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var got string
	for deadline := time.Now().Add(timeout); ; {
		got = strings.Join(rowsAsText(t, db, targetQuery), "\n")
		if got == want || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if got != want {
		t.Errorf("%s: the target does not hold what MariaDB holds.\nsource:\n%s\ntarget:\n%s", what, want, got)
	}
}

// TestSync_MariaDBNoLogUnsigned_ToPostgres: the cold copy (the SQL path,
// never affected — the control), live CDC at the head, and a warm resume
// whose window — an insert, an update and a delete written while the stream
// was down — the reader replays from behind the head. bu carries the
// documented numeric(20,0) override, so every width reaches the target.
func TestSync_MariaDBNoLogUnsigned_ToPostgres(t *testing.T) {
	sourceDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	applyMariaDBSQL(t, sourceDSN, unsignedDDL+unsignedRow(idMax, "max", "seed-max")+unsignedRow(idHalf, "half", "seed-half"))

	mariaEng, _ := engines.Get("mariadb")
	pgEng, _ := engines.Get("postgres")
	run := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		s := &Streamer{
			Source: mariaEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: pgDSN, StreamID: "nolog-unsigned",
			Mappings: []config.Mapping{{Table: "u", Column: "bu", TargetType: "numeric", TargetTypeOptions: map[string]any{"precision": 20, "scale": 0}}},
		}
		go func() { errc <- s.Run(ctx) }()
		return cancel, errc
	}
	stop := func(cancel context.CancelFunc, errc chan error) {
		cancel()
		select {
		case <-errc:
		case <-time.After(15 * time.Second):
			t.Fatal("Streamer.Run did not return after cancel")
		}
	}

	cancel, errc := run()
	if !waitForRowCount(t, pgDSN, "u", 2, 90*time.Second) {
		select {
		case err := <-errc:
			t.Fatalf("the cold copy never delivered the seed rows; Streamer.Run returned %v", err)
		default:
			t.Fatal("the cold copy never delivered the seed rows")
		}
	}
	assertUnsignedConverges(t, "cold copy", sourceDSN, "pgx", pgDSN, unsignedPGQuery, 5*time.Second)

	applyMariaDBSQL(t, sourceDSN, unsignedRow(idHalfUp, "max", "live")+
		"UPDATE u SET note = 'upd-live', iu = 4294967294 WHERE id = "+idMax+";\n"+
		"DELETE FROM u WHERE id = "+idHalf+";")
	assertUnsignedConverges(t, "live CDC", sourceDSN, "pgx", pgDSN, unsignedPGQuery, 30*time.Second)
	stop(cancel, errc)

	applyMariaDBSQL(t, sourceDSN, unsignedRow(idMaxLess, "half", "down")+
		"UPDATE u SET note = 'upd-down', bu = 18446744073709551613 WHERE id = "+idHalfUp+";\n"+
		"DELETE FROM u WHERE id = "+idMax+";")
	cancel, errc = run()
	defer stop(cancel, errc)
	assertUnsignedConverges(t, "warm resume", sourceDSN, "pgx", pgDSN, unsignedPGQuery, 60*time.Second)
}

// TestSync_MariaDBNoLogUnsigned_BigintAboveInt64_RefusesOnPostgres: without
// the override a Postgres target's BIGINT UNSIGNED column is a bigint, which
// cannot hold a value above 2^63-1. The cold copy refuses such a value
// loudly; before MARIADB-NOLOG-UNSIGNED was fixed the CDC lane decoded it as
// a negative number that fit, and stored it at exit 0. It must refuse too.
func TestSync_MariaDBNoLogUnsigned_BigintAboveInt64_RefusesOnPostgres(t *testing.T) {
	sourceDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	applyMariaDBSQL(t, sourceDSN, "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, bu BIGINT UNSIGNED) ENGINE=InnoDB;"+
		"INSERT INTO b VALUES (1, 9223372036854775807);")
	mariaEng, _ := engines.Get("mariadb")
	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- (&Streamer{Source: mariaEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: pgDSN, StreamID: "nolog-bigint"}).Run(ctx)
	}()
	if !waitForRowCount(t, pgDSN, "b", 1, 90*time.Second) {
		t.Fatal("the cold copy never delivered the seed row")
	}
	applyMariaDBSQL(t, sourceDSN, "INSERT INTO b VALUES (2, 9223372036854775808);")
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "9223372036854775808") {
			t.Fatalf("Streamer.Run = %v; want a refusal naming 9223372036854775808", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the stream did not refuse a BIGINT UNSIGNED above 2^63-1 bound for a bigint column")
	}
	if n := countRows(t, pgDSN, "b"); n != 1 {
		t.Errorf("the target holds %d rows; want only the seed row (the refused one must not land, negative or otherwise)", n)
	}
}

// TestBackup_MariaDBNoLogUnsigned_ChainRestore: a full, then the extremes
// written and changed, captured by `backup incremental` (one chain) and by
// `backup stream` (another), each chain restored into a fresh MariaDB
// database, which holds every width. Both capture lanes start behind the
// head by the length of their window.
func TestBackup_MariaDBNoLogUnsigned_ChainRestore(t *testing.T) {
	sourceDSN, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	eng, _ := engines.Get("mariadb")
	applyMariaDBSQL(t, sourceDSN, unsignedDDL+unsignedRow(idMax, "max", "seed-max")+unsignedRow(idHalf, "half", "seed-half"))
	ctx := context.Background()

	t.Run("incremental", func(t *testing.T) {
		store, full := unsignedFull(t, eng, sourceDSN)
		applyMariaDBSQL(t, sourceDSN, unsignedRow(idHalfUp, "max", "capt")+unsignedRow(idMaxLess, "half", "capt")+
			"UPDATE u SET note = 'upd' WHERE id = "+idMax+";\n"+
			"DELETE FROM u WHERE id = "+idHalf+";")
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		if err := (&IncrementalBackup{
			Source: eng, SourceDSN: sourceDSN, Store: store, ParentRef: full,
			Window: 10 * time.Second, MaxChanges: 12, SluiceVersion: "test", // 4 transactions × (begin, row, commit)
		}).Run(runCtx); err != nil {
			t.Fatalf("IncrementalBackup.Run: %v", err)
		}
		target := mariaDBSiblingDB(t, sourceDSN, "restore_inc")
		if err := (&backup.ChainRestore{Target: eng, TargetDSN: target, Store: store}).Run(ctx); err != nil {
			t.Fatalf("ChainRestore.Run: %v", err)
		}
		assertUnsignedConverges(t, "backup incremental → chain restore", sourceDSN, "mysql", target, unsignedSourceQuery, 0)
	})

	t.Run("stream", func(t *testing.T) {
		// The second chain's full captures the incremental subtest's rows;
		// its window writes the same shapes again on other keys.
		store, full := unsignedFull(t, eng, sourceDSN)
		applyMariaDBSQL(t, sourceDSN, "DELETE FROM u WHERE id = "+idHalfUp+";\n"+
			unsignedRow(idHalf, "max", "stream")+
			"UPDATE u SET note = 'upd-stream', mu = 16777214 WHERE id = "+idMaxLess+";")
		runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			errc <- (&BackupStream{
				Source: eng, SourceDSN: sourceDSN, Store: store, ParentRef: full,
				RolloverWindow: time.Second, RolloverMaxChanges: 3, RolloverMaxBytes: 1 << 30,
				ChunkChanges: 50, SluiceVersion: "test",
			}).Run(runCtx)
		}()
		for deadline := time.Now().Add(60 * time.Second); committedChangeRecords(t, store) < 9; { // 3 transactions × (begin, row, commit)
			if time.Now().After(deadline) {
				t.Fatalf("backup stream committed %d change records; want 9", committedChangeRecords(t, store))
			}
			time.Sleep(250 * time.Millisecond)
		}
		cancel()
		<-errc
		target := mariaDBSiblingDB(t, sourceDSN, "restore_stream")
		if err := (&backup.ChainRestore{Target: eng, TargetDSN: target, Store: store}).Run(ctx); err != nil {
			t.Fatalf("ChainRestore.Run: %v", err)
		}
		assertUnsignedConverges(t, "backup stream → chain restore", sourceDSN, "mysql", target, unsignedSourceQuery, 0)
	})
}

// mariaDBSiblingDB creates database name on sourceDSN's server and returns
// a DSN for it.
func mariaDBSiblingDB(t *testing.T, sourceDSN, name string) string {
	t.Helper()
	applyMariaDBSQL(t, sourceDSN, "CREATE DATABASE "+name+";")
	return strings.Replace(sourceDSN, "/source_db?", "/"+name+"?", 1)
}

// unsignedFull takes a full backup of the source into a fresh store and
// returns the store and the full's id.
func unsignedFull(t *testing.T, eng ir.Engine, sourceDSN string) (store *blobcodec.LocalStore, fullID string) {
	t.Helper()
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	return store, full.BackupID
}
