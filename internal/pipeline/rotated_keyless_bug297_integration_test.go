//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 297 end to end, on real Postgres and MySQL.
//
// A `backup stream` that ROTATES over a source holding a keyless table
// writes a chain whose later segment fulls chain restore must apply OVER the
// earlier segments' rows — which a keyless table cannot absorb. Through
// v0.156.12 the restore failed at the second segment full after part-writing
// the target (the regression cycle measured 18 of 36 keyed rows), with a
// message written for the VStream cold-start copy.
//
// Per engine, one rotated chain an older binary would have written (built
// through the legacyKeylessRotation seam, since this release refuses to
// build it), then:
//
//   - restore refuses, coded, before writing ANYTHING (no table is created);
//   - backup verify reports the same chain;
//   - a target pre-created with a SURROGATE-keyed table (the silent arm) is
//     refused before anything is written, even with the keyless table
//     excluded;
//   - the remedy the refusal names (--exclude-table) restores the keyed
//     table EXACTLY across every segment boundary — the healthy rotated
//     keyed path, through the same chain;
//   - `backup stream` with rotation refuses to START on the same source.
//
// Plus, on Postgres, the per-rotation door: a keyless table created after
// the stream started stops rotation (the stream stays on its open segment)
// and the chain restores exactly.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// r297Side is one engine's half of the matrix.
type r297Side struct {
	engine    string
	driver    string // database/sql driver name
	start     func(t *testing.T) (src, tgt string, cleanup func())
	exec      func(t *testing.T, dsn, sqlText string)
	schemaDDL string // kd (keyed) + kl (keyless)
	surrogate string // a target kd keyed only on a column the rows do not carry
	existsSQL string // one placeholder: the table name
	// seed takes the root full the stream chains off, into store.
	seed func(t *testing.T, store irbackup.Store, eng ir.Engine, src string) *irbackup.Manifest
}

func r297PG() r297Side {
	return r297Side{
		engine: "postgres", driver: "pgx",
		start: startPostgresLogical, exec: applyDDL,
		schemaDDL: `
			CREATE TABLE kd (id INT PRIMARY KEY, v TEXT);
			CREATE TABLE kl (a INT, b TEXT);
			ALTER TABLE kl REPLICA IDENTITY FULL;
			INSERT INTO kd VALUES (1,'seed'),(2,'seed');
			INSERT INTO kl VALUES (1,'seed'),(2,'seed');
			CREATE PUBLICATION sluice_pub FOR ALL TABLES;`,
		surrogate: `CREATE TABLE kd (sid BIGSERIAL PRIMARY KEY, id INT, v TEXT)`,
		existsSQL: `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
		seed: func(t *testing.T, store irbackup.Store, eng ir.Engine, src string) *irbackup.Manifest {
			t.Helper()
			lsn, err := createPGLogicalSlotReturningLSN(t, src, "sluice_slot")
			if err != nil {
				t.Fatalf("create slot: %v", err)
			}
			t.Cleanup(func() { dropPGLogicalSlot(t, src, "sluice_slot") })
			return rotationSeedFull(t, store, eng, src, lsn)
		},
	}
}

func r297MySQL() r297Side {
	return r297Side{
		engine: "mysql", driver: "mysql",
		start: startMySQLBinlog, exec: applyDDLMySQL,
		schemaDDL: `
			CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, v TEXT) ENGINE=InnoDB;
			CREATE TABLE kl (a INT, b TEXT) ENGINE=InnoDB;
			INSERT INTO kd VALUES (1,'seed'),(2,'seed');
			INSERT INTO kl VALUES (1,'seed'),(2,'seed');`,
		surrogate: `CREATE TABLE kd (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, id INT, v TEXT) ENGINE=InnoDB`,
		existsSQL: `SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
		seed: func(t *testing.T, store irbackup.Store, eng ir.Engine, src string) *irbackup.Manifest {
			t.Helper()
			ctx := context.Background()
			if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("seed Backup.Run: %v", err)
			}
			file, pos := readMySQLBinlogPos(t, src)
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatalf("read seed full: %v", err)
			}
			full.Kind = irbackup.BackupKindFull
			full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}
			full.BackupID = irbackup.ComputeBackupID(full)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
				t.Fatalf("rewrite seed full: %v", err)
			}
			_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.DefaultCodec)
			return full
		},
	}
}

func r297Query(t *testing.T, side r297Side, dsn, q string, args ...any) *sql.Rows {
	t.Helper()
	db, err := sql.Open(side.driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.QueryContext(context.Background(), q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return rows
}

func r297TableExists(t *testing.T, side r297Side, dsn, table string) bool {
	t.Helper()
	rows := r297Query(t, side, dsn, side.existsSQL, table)
	defer func() { _ = rows.Close() }()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	return n > 0
}

func r297Ints(t *testing.T, side r297Side, dsn, q string) []int64 {
	t.Helper()
	rows := r297Query(t, side, dsn, q)
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func r297AssertCode(t *testing.T, what string, err error, mentions ...string) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupRotatedKeylessTable {
		t.Fatalf("%s: err = %v; want %s", what, err, sluicecode.CodeBackupRotatedKeylessTable)
	}
	for _, m := range mentions {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("%s: refusal does not mention %q:\n%v", what, m, err)
		}
	}
}

// r297BuildRotatedChain streams kd and kl inserts across several rotations
// with the legacy seam on, and returns the store once the stream has
// stopped and the lineage holds at least three segments.
func r297BuildRotatedChain(t *testing.T, side r297Side, eng ir.Engine, src string) irbackup.Store {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := side.seed(t, store, eng, src)

	var wg sync.WaitGroup
	writeCtx, stopWriting := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 10; i < 40; i++ {
			select {
			case <-writeCtx.Done():
				return
			default:
			}
			side.exec(t, src, fmt.Sprintf(`INSERT INTO kd VALUES (%d,'w'); INSERT INTO kl VALUES (%d,'w');`, i, i))
			time.Sleep(150 * time.Millisecond)
		}
	}()

	stream := &BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		RolloverWindow: 900 * time.Millisecond, RolloverMaxChanges: 6, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 50, RetainRotateAtChainLength: 1, SluiceVersion: "test",
		legacyKeylessRotation: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	wg.Wait()
	stopWriting()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cat, ok, _ := lineage.LoadLineageCatalog(context.Background(), store)
		if ok && len(cat.Segments) >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(4 * time.Second) // let the tail rollover commit
	cancel()
	select {
	case err := <-streamErr:
		if err != nil {
			t.Fatalf("stream.Run = %v; want clean exit", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream did not exit after cancel")
	}
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok || len(cat.Segments) < 2 {
		t.Fatalf("fixture did not rotate: ok=%v err=%v", ok, err)
	}
	t.Logf("Bug 297 fixture: %d segments", len(cat.Segments))
	return store
}

func runBug297(t *testing.T, side r297Side) {
	src, tgt, cleanup := side.start(t)
	defer cleanup()
	side.exec(t, src, side.schemaDDL)
	eng, ok := engines.Get(side.engine)
	if !ok {
		t.Fatalf("%s engine not registered", side.engine)
	}
	ctx := context.Background()
	store := r297BuildRotatedChain(t, side, eng, src)

	// 1. The repro: refused, coded, and NOTHING on the target — not even a
	//    table (v0.156.12 left kd half-restored here).
	err := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx)
	r297AssertCode(t, "restore", err, `"kl"`, "segment full", "Nothing has been written")
	for _, tbl := range []string{"kd", "kl"} {
		if r297TableExists(t, side, tgt, tbl) {
			t.Errorf("restore refused but table %q exists on the target: the door fired after a write", tbl)
		}
	}

	// 2. backup verify reports the chain restore refuses.
	_, _, verr := backup.VerifyBackupCoded(ctx, store, backup.VerifyOptions{})
	r297AssertCode(t, "backup verify", verr, `"kl"`)

	exclude := migcore.TableFilter{Exclude: []string{"kl"}}

	// 3. The silent arm: kl excluded, kd pre-created keyed only on a
	//    surrogate. Every later full would land its kd rows again.
	side.exec(t, tgt, side.surrogate)
	err = (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store, Filter: exclude}).Run(ctx)
	r297AssertCode(t, "restore onto a surrogate-keyed target", err, `"kd"`, "target table")
	if got := r297Ints(t, side, tgt, "SELECT count(*) FROM kd"); len(got) != 1 || got[0] != 0 {
		t.Errorf("surrogate-keyed kd holds %v rows after the refusal; want 0", got)
	}
	side.exec(t, tgt, "DROP TABLE kd")

	// 4. The remedy the refusal names: everything else restores EXACTLY
	//    across every segment boundary.
	if err := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store, Filter: exclude}).Run(ctx); err != nil {
		t.Fatalf("restore --exclude-table=kl: %v", err)
	}
	want := r297Ints(t, side, src, "SELECT id FROM kd ORDER BY id")
	if len(want) != 32 { // 2 seeds + 30 streamed: anti-vacuity for the equality below
		t.Fatalf("source kd holds %d rows; the fixture writes 32", len(want))
	}
	got := r297Ints(t, side, tgt, "SELECT id FROM kd ORDER BY id")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("kd after the rotated restore = %v (%d rows); source = %v (%d rows)", got, len(got), want, len(want))
	}
	if r297TableExists(t, side, tgt, "kl") {
		t.Error("kl was restored although excluded")
	}

	// 5. The write side: the same source, rotation on, refuses to start.
	fresh, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := side.seed(t, fresh, eng, src)
	serr := (&BackupStream{
		Source: eng, SourceDSN: src, Store: fresh, ParentRef: full.BackupID,
		RolloverWindow: time.Second, RetainRotateAtChainLength: 1, SluiceVersion: "test",
	}).Run(ctx)
	r297AssertCode(t, "backup stream start", serr, `"kl"`, "Nothing has been written")
	recs, _ := lineage.ListAllManifestsViaWalk(ctx, fresh)
	if len(recs) != 1 {
		t.Errorf("refused stream left %d manifests; want 1 (the seed full)", len(recs))
	}
}

func TestBug297_RotatedKeylessChain_PG(t *testing.T)    { runBug297(t, r297PG()) }
func TestBug297_RotatedKeylessChain_MySQL(t *testing.T) { runBug297(t, r297MySQL()) }

// TestBug297_RotationRefusedForAKeylessTableAddedMidStream_PG is the
// per-rotation door: the stream starts on a keyed-only source (the start
// door passes), a keyless table appears mid-stream, and every rotation from
// then on is refused — logged with the code — while the stream keeps
// writing its open segment. The chain then restores EXACTLY, keyless table
// included, because no later full ever carried it.
func TestBug297_RotationRefusedForAKeylessTableAddedMidStream_PG(t *testing.T) {
	side := r297PG()
	src, tgt, cleanup := side.start(t)
	defer cleanup()
	side.exec(t, src, `
		CREATE TABLE kd (id INT PRIMARY KEY, v TEXT);
		INSERT INTO kd VALUES (1,'seed');
		CREATE PUBLICATION sluice_pub FOR ALL TABLES;`)
	eng, _ := engines.Get("postgres")
	ctx := context.Background()

	logBuf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	full := side.seed(t, store, eng, src)
	stream := &BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		RolloverWindow: 900 * time.Millisecond, RolloverMaxChanges: 6, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 50, RetainRotateAtChainLength: 1, SluiceVersion: "test",
	}
	sctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(sctx) }()

	insertKD := func(from, to int) {
		for i := from; i < to; i++ {
			side.exec(t, src, fmt.Sprintf(`INSERT INTO kd VALUES (%d,'w')`, i))
			time.Sleep(150 * time.Millisecond)
		}
	}
	// Rotate at least once on the keyed-only source.
	insertKD(10, 30)
	segs := func() int {
		cat, ok, _ := lineage.LoadLineageCatalog(ctx, store)
		if !ok {
			return 0
		}
		return len(cat.Segments)
	}
	deadline := time.Now().Add(45 * time.Second)
	for segs() < 2 && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
	}
	if segs() < 2 {
		t.Fatal("the keyed-only stream never rotated; the fixture cannot test the per-rotation door")
	}

	// A keyless table appears; from the next rotation on, rotation is refused.
	side.exec(t, src, `CREATE TABLE kl (a INT, b TEXT); ALTER TABLE kl REPLICA IDENTITY FULL;`)
	for i := 30; i < 60; i++ {
		side.exec(t, src, fmt.Sprintf(`INSERT INTO kd VALUES (%d,'w'); INSERT INTO kl VALUES (%d,'w');`, i, i))
		time.Sleep(150 * time.Millisecond)
	}
	deadline = time.Now().Add(45 * time.Second)
	for !strings.Contains(logBuf.String(), string(sluicecode.CodeBackupRotatedKeylessTable)) && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(4 * time.Second)
	cancel()
	if err := <-streamErr; err != nil {
		t.Fatalf("stream.Run = %v; want clean exit (a refused rotation stays on the open segment)", err)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "rotation aborted") || !strings.Contains(logs, string(sluicecode.CodeBackupRotatedKeylessTable)) {
		t.Fatalf("no rotation was refused with %s after the keyless table appeared", sluicecode.CodeBackupRotatedKeylessTable)
	}

	if _, _, err := backup.VerifyBackupCoded(ctx, store, backup.VerifyOptions{}); err != nil {
		t.Fatalf("verify of the chain the door kept restorable: %v", err)
	}
	if err := (&backup.Restore{Target: eng, TargetDSN: tgt, Store: store}).Run(ctx); err != nil {
		t.Fatalf("restore of the chain the door kept restorable: %v", err)
	}
	for _, tbl := range []string{"kd", "kl"} {
		col := map[string]string{"kd": "id", "kl": "a"}[tbl]
		q := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", col, tbl, col)
		want, got := r297Ints(t, side, src, q), r297Ints(t, side, tgt, q)
		if n := map[string]int{"kd": 51, "kl": 30}[tbl]; len(want) != n {
			t.Fatalf("source %s holds %d rows; the fixture writes %d", tbl, len(want), n)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s after restore = %v (%d rows); source = %v (%d rows)", tbl, got, len(got), want, len(want))
		}
	}
}
