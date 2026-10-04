//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit F-E1 against real databases: an interrupted `sync from-backup`
// incremental re-applies whole, and a keyless table used to gain a duplicate
// of every row the interrupted run had committed, at exit 0 (measured
// 2001 vs 1001 on Postgres). These are the reproduction harness of
// 2026-10-04 ported into pins against the interim door:
//
//   - the {error, ctx cancel} × {serial, lanes} matrix on a KEYLESS chain:
//     the broker now refuses (SLUICE-E-BROKER-KEYLESS-TABLE) before it
//     applies anything, whatever the interruption and apply mode;
//   - the same matrix on a KEYED chain: the interrupted run is re-applied and
//     converges, a mid-incremental cancel returns BROKER-INCREMENTAL-PARTIAL
//     (non-zero), and a cancel while idle is still a clean exit;
//   - the restore re-run door (SLUICE-E-RESTORE-KEYLESS-TABLE-NOT-EMPTY) on a
//     single full and on a chain, mixed and keyless-only, with the table
//     filter honoured;
//   - a MySQL target, where the keyless judgment is a different catalog read.

package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

const fe1Rows = 1000

// fe1Q runs a single-value query against a Postgres DSN and returns it as text.
func fe1Q(t *testing.T, dsn, q string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRowContext(context.Background(), q).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s
}

func fe1Count(t *testing.T, dsn, table string) string {
	t.Helper()
	return fe1Q(t, dsn, "SELECT count(*)::text FROM "+table)
}

// fe1Chain is a PG source + broker target + chain carrying ONE data-bearing
// incremental with fe1Rows inserts per table, in one source transaction.
type fe1Chain struct {
	src, dst string
	store    *blobcodec.LocalStore
	fullID   string
	incr     *lineage.SegmentRecord // the data-bearing incremental
	tailID   string                 // the chain's last link
}

// fe1Setup seeds the source with seedDDL, takes the full, restores it to the
// target (the operator's manual restore before --at-chain-id), then lets a
// backup stream capture loopBody fe1Rows times. beforeStream runs while the
// store holds only the full.
func fe1Setup(t *testing.T, seedDDL, loopBody string, beforeStream func(c *fe1Chain)) *fe1Chain {
	t.Helper()
	src, dst, store, fullID, td := brokerTestStreamSetup(t, seedDDL)
	t.Cleanup(td)
	c := &fe1Chain{src: src, dst: dst, store: store, fullID: fullID}
	pgEng, _ := engines.Get("postgres")
	if err := (&backup.Restore{Target: pgEng, TargetDSN: dst, Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	if beforeStream != nil {
		beforeStream(c)
	}
	stream := &BackupStream{
		Source: pgEng, SourceDSN: src, Store: store, ParentRef: fullID,
		RolloverWindow: 4 * time.Second, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 50, SluiceVersion: "test",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()
	applyDDL(t, src, fmt.Sprintf(`DO $$ BEGIN FOR i IN 1..%d LOOP %s END LOOP; END $$;`, fe1Rows, loopBody))
	waitForIncrementals(t, store, 1, 60*time.Second)
	time.Sleep(6 * time.Second) // let a trailing rollover settle
	cancel()
	<-done

	chain, err := (&SyncFromBackup{Store: store, ChainURL: "x", StreamID: "x"}).brokerChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range chain {
		if chain[i].Manifest.Kind == irbackup.BackupKindIncremental &&
			(c.incr == nil || len(chain[i].Manifest.ChangeChunks) > len(c.incr.Manifest.ChangeChunks)) {
			c.incr = &chain[i]
		}
	}
	if c.incr == nil || len(c.incr.Manifest.ChangeChunks) < 2 {
		t.Fatalf("the stream produced no multi-chunk incremental; the partial-apply cells need one")
	}
	c.tailID = lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
	return c
}

// corruptLastChunk flips a byte in the data incremental's last change chunk
// (a transient fetch failure, from the broker's point of view) and returns
// the function that puts the original back.
func (c *fe1Chain) corruptLastChunk(t *testing.T) (restore func()) {
	t.Helper()
	ctx := context.Background()
	seg := c.incr.Segment.Store(c.store)
	last := c.incr.Manifest.ChangeChunks[len(c.incr.Manifest.ChangeChunks)-1]
	rc, err := seg.Get(ctx, last.File)
	if err != nil {
		t.Fatal(err)
	}
	orig, _ := io.ReadAll(rc)
	_ = rc.Close()
	bad := append([]byte(nil), orig...)
	bad[len(bad)/2] ^= 0xFF
	if err := seg.Put(ctx, last.File, bytes.NewReader(bad)); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := seg.Put(ctx, last.File, bytes.NewReader(orig)); err != nil {
			t.Fatal(err)
		}
	}
}

func (c *fe1Chain) broker(streamID string, conc int, atChain string) *SyncFromBackup {
	pgEng, _ := engines.Get("postgres")
	return &SyncFromBackup{
		Target: pgEng, TargetDSN: c.dst, Store: c.store, ChainURL: "test://" + streamID,
		StreamID: streamID, PollInterval: 2 * time.Second, ApplyBatchSize: 100,
		ApplyConcurrency: conc, AtChainID: atChain, SluiceVersion: "test",
		brokerStatePath: "manifests/broker_state_" + streamID + ".json",
	}
}

var fe1Modes = []struct {
	name string
	conc int
}{{"serial", 1}, {"lanes", 0}}

// TestFE1_Broker_KeylessChain_RefusedBeforeAnything is the keyless half of
// the matrix, plus the restore re-run door on the same chain. Before the
// door, every broker cell ended with the keyless table at ~2001 rows (error)
// or ~1006 (cancel, exit 0) for 1001 on the source.
func TestFE1_Broker_KeylessChain_RefusedBeforeAnything(t *testing.T) {
	c := fe1Setup(t, `
		CREATE TABLE kl (v INT NOT NULL, note TEXT);
		ALTER TABLE kl REPLICA IDENTITY FULL;
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		INSERT INTO kl VALUES (-1, 'seed');
		INSERT INTO k VALUES (-1, 'seed');
	`, `INSERT INTO kl VALUES (i, 'r'||i); INSERT INTO k VALUES (i, 'r'||i);`,
		func(c *fe1Chain) {
			// The SINGLE-FULL restore re-run: the store holds only the full,
			// and the target already holds the first restore's rows.
			pgEng, _ := engines.Get("postgres")
			err := (&backup.Restore{Target: pgEng, TargetDSN: c.dst, Store: c.store}).Run(context.Background())
			assertFE1Refusal(t, err, sluicecode.CodeRestoreKeylessTableNotEmpty, []string{"kl"}, []string{"k"})
			if got := fe1Count(t, c.dst, "kl"); got != "1" {
				t.Errorf("single-full restore re-run: kl holds %s rows after the refusal; want the 1 the first restore wrote", got)
			}
		})

	for _, interrupt := range []string{"error", "cancel"} {
		for _, mode := range fe1Modes {
			t.Run(interrupt+"/"+mode.name, func(t *testing.T) {
				if interrupt == "error" {
					defer c.corruptLastChunk(t)()
				}
				streamID := "fe1-kl-" + interrupt + "-" + mode.name
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				if interrupt == "cancel" {
					// The SIGINT that, before the door, landed mid-incremental
					// and exited 0. The refusal must come first.
					time.AfterFunc(3*time.Second, cancel)
				}
				err := c.broker(streamID, mode.conc, c.fullID).Run(ctx)
				assertFE1Refusal(t, err, sluicecode.CodeBrokerKeylessTable, []string{"kl"}, []string{"k"})
				if kl, k := fe1Count(t, c.dst, "kl"), fe1Count(t, c.dst, "k"); kl != "1" || k != "1" {
					t.Errorf("target holds kl=%s k=%s after the refusal; want 1/1 — something was applied", kl, k)
				}
				if n := fe1Q(t, c.dst, fmt.Sprintf(`SELECT count(*)::text FROM sluice_cdc_state WHERE stream_id = '%s'`, streamID)); n != "0" {
					t.Errorf("the refused broker wrote %s position row(s)", n)
				}
			})
		}
	}

	// The CHAIN restore re-run. A failed first attempt leaves rows behind;
	// the documented recovery used to be "re-run the restore", which doubled
	// every keyless row (2001 vs 1001) at exit nil.
	t.Run("chain restore re-run", func(t *testing.T) {
		applyDDL(t, c.dst, `DROP TABLE kl; DROP TABLE k;`)
		putBack := c.corruptLastChunk(t)
		pgEng, _ := engines.Get("postgres")
		cr := func(filter migcore.TableFilter) error {
			return (&backup.ChainRestore{Target: pgEng, TargetDSN: c.dst, Store: c.store, ApplyConcurrency: 1, Filter: filter}).Run(context.Background())
		}
		if err := cr(migcore.TableFilter{}); err == nil {
			t.Fatal("the first chain restore succeeded over a corrupted chunk; the partial-attempt setup did not happen")
		}
		putBack()
		klAfterFirst, kAfterFirst := fe1Count(t, c.dst, "kl"), fe1Count(t, c.dst, "k")
		if klAfterFirst == "0" {
			t.Fatal("the failed first attempt left kl empty; the re-run cell would be vacuous")
		}

		t.Run("mixed schema", func(t *testing.T) {
			assertFE1Refusal(t, cr(migcore.TableFilter{}), sluicecode.CodeRestoreKeylessTableNotEmpty, []string{"kl"}, []string{"k"})
		})
		t.Run("keyless only (--include-table kl)", func(t *testing.T) {
			assertFE1Refusal(t, cr(migcore.TableFilter{Include: []string{"kl"}}), sluicecode.CodeRestoreKeylessTableNotEmpty, []string{"kl"}, nil)
		})
		t.Run("keyless table excluded: the keyed table fails loudly, as it always did", func(t *testing.T) {
			err := cr(migcore.TableFilter{Exclude: []string{"kl"}})
			if err == nil {
				t.Fatal("re-running onto a populated keyed table succeeded; expected its key to collide loudly")
			}
			if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeRestoreKeylessTableNotEmpty {
				t.Fatalf("the door refused although the only keyless table was excluded: %v", err)
			}
		})
		if kl, k := fe1Count(t, c.dst, "kl"), fe1Count(t, c.dst, "k"); kl != klAfterFirst || k != kAfterFirst {
			t.Errorf("the refused re-runs changed the target: kl %s→%s, k %s→%s", klAfterFirst, kl, kAfterFirst, k)
		}
	})
}

// TestFE1_Broker_KeyedChain_ConvergesAndCancelIsLoud is the keyed half: the
// interrupted incremental is re-applied whole and converges, in both apply
// modes; a cancel mid-incremental is BROKER-INCREMENTAL-PARTIAL; a cancel
// while idle is a clean exit.
func TestFE1_Broker_KeyedChain_ConvergesAndCancelIsLoud(t *testing.T) {
	c := fe1Setup(t, `
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		INSERT INTO k VALUES (-1, 'seed');
	`, `INSERT INTO k VALUES (i, 'r'||i);`, nil)
	want := fe1Count(t, c.src, "k")
	incrID := lineage.ManifestBackupID(c.incr.Manifest)

	for _, interrupt := range []string{"error", "cancel"} {
		for _, mode := range fe1Modes {
			t.Run(interrupt+"/"+mode.name, func(t *testing.T) {
				applyDDL(t, c.dst, `TRUNCATE k; INSERT INTO k VALUES (-1, 'seed');`)
				streamID := "fe1-k-" + interrupt + "-" + mode.name

				var err1 error
				if interrupt == "error" {
					putBack := c.corruptLastChunk(t)
					err1 = c.broker(streamID, mode.conc, c.fullID).Run(context.Background())
					putBack()
					if err1 == nil {
						t.Fatal("run 1 over a corrupted chunk returned nil")
					}
				} else {
					err1 = fe1CancelMidIncremental(t, c, c.broker(streamID, mode.conc, c.fullID))
					if err1 == nil || !strings.Contains(err1.Error(), BrokerIncrementalPartialMarker) || !strings.Contains(err1.Error(), incrID) {
						t.Fatalf("a cancel mid-incremental returned %v; want the %s error naming %s", err1, BrokerIncrementalPartialMarker, incrID)
					}
					if errors.Is(err1, context.Canceled) {
						t.Errorf("the partial error unwraps to context.Canceled; the live panel would report a clean stop")
					}
				}
				t.Logf("run 1: k=%s, err=%v", fe1Count(t, c.dst, "k"), err1)

				// Run 2: warm resume, re-applies the interrupted incremental,
				// then a cancel while IDLE must be the clean exit it always was.
				if err := fe1RunToTailThenCancel(t, c, c.broker(streamID, mode.conc, "")); err != nil {
					t.Errorf("run 2: a cancel while idle returned %v; want nil (exit 0)", err)
				}
				got := fe1Count(t, c.dst, "k")
				distinct := fe1Q(t, c.dst, `SELECT count(DISTINCT id)::text FROM k`)
				if got != want || distinct != want {
					t.Errorf("keyed table did not converge: target %s rows (%s distinct), source %s", got, distinct, want)
				}
			})
		}
	}
}

// fe1CancelMidIncremental runs b and cancels it as soon as the target shows
// the incremental partly applied. Fails the test (rather than passing
// vacuously) if the incremental finished before the cancel could land.
func fe1CancelMidIncremental(t *testing.T, c *fe1Chain, b *SyncFromBackup) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("broker exited before any apply: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		if n := fe1Count(t, c.dst, "k"); n != "1" && n != "0" {
			cancel()
			err := <-done
			if n == fe1Count(t, c.src, "k") {
				t.Fatalf("the incremental had fully applied (%s rows) before the cancel landed; the cell is vacuous", n)
			}
			return err
		}
	}
	t.Fatal("the broker never started applying")
	return nil
}

// fe1RunToTailThenCancel runs b until its persisted position names the
// chain's tail, then cancels it — while idle — and returns Run's error.
func fe1RunToTailThenCancel(t *testing.T, c *fe1Chain, b *SyncFromBackup) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("broker exited before reaching the tail: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
		pos := fe1Q(t, c.dst, fmt.Sprintf(`SELECT coalesce(string_agg(source_position, ','), '') FROM sluice_cdc_state WHERE stream_id = '%s'`, b.StreamID))
		if strings.Contains(pos, c.tailID) {
			time.Sleep(500 * time.Millisecond) // into the tick wait
			cancel()
			return <-done
		}
	}
	t.Fatal("the broker never reached the chain's tail")
	return nil
}

// assertFE1Refusal asserts err carries code and names every table in
// named (quoted, as the refusal renders them) and none in unnamed.
func assertFE1Refusal(t *testing.T, err error, code sluicecode.Code, named, unnamed []string) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != code {
		t.Fatalf("got %v; want %s", err, code)
	}
	for _, n := range named {
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", n)) {
			t.Errorf("refusal does not name %q: %v", n, err)
		}
	}
	for _, n := range unnamed {
		if strings.Contains(err.Error(), fmt.Sprintf("%q", n)) {
			t.Errorf("refusal names the keyed table %q: %v", n, err)
		}
	}
}

// TestFE1_Broker_MySQLTarget_KeylessDoor pins the door on a MySQL target,
// where the target judgment is a different catalog read
// (information_schema, not pg_index) and a different upsert (ON DUPLICATE
// KEY UPDATE, which does not collide on a NULL). Four shapes, one run: a
// source-keyless table and a nullable-UNIQUE-only table are refused on the
// recorded schema; a PK table whose TARGET copy lost its key is refused on
// the target; a NOT NULL UNIQUE table is accepted.
func TestFE1_Broker_MySQLTarget_KeylessDoor(t *testing.T) {
	src, dst, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, src, `
		CREATE TABLE kl  (v INT NOT NULL, note TEXT) ENGINE=InnoDB;
		CREATE TABLE nu  (u INT NULL UNIQUE, note TEXT) ENGINE=InnoDB;
		CREATE TABLE nnu (u INT NOT NULL UNIQUE, note TEXT) ENGINE=InnoDB;
		CREATE TABLE pkd (id INT NOT NULL PRIMARY KEY, note TEXT) ENGINE=InnoDB;
		INSERT INTO kl VALUES (1, 'a'); INSERT INTO nu VALUES (NULL, 'a');
		INSERT INTO nnu VALUES (1, 'a'); INSERT INTO pkd VALUES (1, 'a');
	`)
	mysqlEng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := (&backup.Backup{Source: mysqlEng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	file, pos := readMySQLBinlogPos(t, src)
	full, _ := lineage.ReadManifest(ctx, store)
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Restore{Target: mysqlEng, TargetDSN: dst, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	applyDDLMySQL(t, dst, `ALTER TABLE pkd DROP PRIMARY KEY;`)

	b := &SyncFromBackup{
		Target: mysqlEng, TargetDSN: dst, Store: store, ChainURL: "test://fe1-mysql",
		StreamID: "fe1-mysql", PollInterval: 2 * time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}
	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = b.Run(runCtx)
	assertFE1Refusal(t, err, sluicecode.CodeBrokerKeylessTable, []string{"kl", "nu", "pkd"}, []string{"nnu"})
	msg := err.Error()
	if i := strings.Index(msg, `"pkd"`); i < 0 || !strings.Contains(msg[i:], string(migcore.ReplayKeylessTarget)) {
		t.Errorf("pkd is keyed in the backup and keyless only on the target; the refusal must say the TARGET judgment failed: %v", err)
	}
	if i := strings.Index(msg, `"nu"`); i < 0 || !strings.HasPrefix(msg[i+len(`"nu" (`):], string(migcore.ReplayKeylessRecorded)) {
		t.Errorf("nu's only key is a nullable UNIQUE; the refusal must say the RECORDED judgment failed: %v", err)
	}
}
