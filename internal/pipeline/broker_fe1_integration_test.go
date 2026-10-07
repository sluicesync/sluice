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
//     with change identities (ADR-0191) the interrupted run and its re-run
//     converge to the source; stripped of them, the broker refuses
//     (SLUICE-E-BROKER-KEYLESS-TABLE) before it applies anything, whatever
//     the interruption and apply mode;
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
	return fe1SetupWith(t, seedDDL, func(t *testing.T, src string) {
		applyDDL(t, src, fmt.Sprintf(`DO $$ BEGIN FOR i IN 1..%d LOOP %s END LOOP; END $$;`, fe1Rows, loopBody))
	}, beforeStream)
}

// fe1SetupWith is fe1Setup with the captured source traffic given as a
// function (fe1Setup's is one source transaction of fe1Rows loop bodies).
func fe1SetupWith(t *testing.T, seedDDL string, traffic func(t *testing.T, src string), beforeStream func(c *fe1Chain)) *fe1Chain {
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
	traffic(t, src)
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
	if c.incr == nil || len(c.incr.Manifest.ChangeChunks) < 3 {
		t.Fatalf("the stream produced no incremental of 3+ chunks; the partial-apply cells need one (see corruptMiddleChunk)")
	}
	c.tailID = lineage.ManifestBackupID(chain[len(chain)-1].Manifest)
	return c
}

// corruptMiddleChunk flips a byte in a MIDDLE change chunk of the data
// incremental (a transient fetch failure, from the applier's point of view)
// and returns the function that puts the original back. A middle one, not
// the last: the severed-transaction door (F-E1-SEVERED-TAIL-REPLAY) decodes
// each incremental's FIRST and LAST chunks before anything is applied and
// refuses a link whose chunk it cannot read, so a corrupt last chunk now
// fails the run before the partial apply these cells exist to pin. A middle
// chunk is read only by the apply, after the chunks before it committed.
func (c *fe1Chain) corruptMiddleChunk(t *testing.T) (restore func()) {
	t.Helper()
	ctx := context.Background()
	seg := c.incr.Segment.Store(c.store)
	chunks := c.incr.Manifest.ChangeChunks
	if len(chunks) < 3 {
		t.Fatalf("the data incremental has %d chunks; a middle-chunk failure needs at least 3", len(chunks))
	}
	mid := chunks[len(chunks)/2]
	rc, err := seg.Get(ctx, mid.File)
	if err != nil {
		t.Fatal(err)
	}
	orig, _ := io.ReadAll(rc)
	_ = rc.Close()
	bad := append([]byte(nil), orig...)
	bad[len(bad)/2] ^= 0xFF
	if err := seg.Put(ctx, mid.File, bytes.NewReader(bad)); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := seg.Put(ctx, mid.File, bytes.NewReader(orig)); err != nil {
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

// failMiddleChunk arms the broker's replay failpoint to kill a run just
// before the data incremental's middle chunk is read (the crash suite's kill,
// §13 R12), and returns the disarm. A store-side corruption cannot interrupt
// a keyless chain's replay any more: the keyless door reads every chunk of an
// incremental that touches a keyless table before anything is applied.
func (c *fe1Chain) failMiddleChunk(t *testing.T) (disarm func()) {
	t.Helper()
	id := lineage.ManifestBackupID(c.incr.Manifest)
	mid := len(c.incr.Manifest.ChangeChunks) / 2
	brokerReplayChunkFailpoint = func(backupID string, chunkIdx int) error {
		if backupID == id && chunkIdx == mid {
			return errBrokerCrashKill
		}
		return nil
	}
	return func() { brokerReplayChunkFailpoint = nil }
}

// TestFE1_Broker_KeylessChain_ExactlyOnceOrRefused is the keyless half of
// the matrix, plus the restore re-run door on the same chain. Before the
// door, every broker cell ended with the keyless table at ~2001 rows (error)
// or ~1006 (cancel, exit 0) for 1001 on the source.
//
// Since ADR-0191 the door is per incremental: an incremental that records
// change identities into a target whose apply marks cover the keyless table
// is replayed exactly-once, so the interrupted run and its re-run converge to
// the SOURCE (the identity arm); the same incremental stripped of its
// identities — what an older sluice or smart compaction leaves — is refused
// before anything is applied, exactly as the interim door refused every
// keyless chain (the no-identity arm).
func TestFE1_Broker_KeylessChain_ExactlyOnceOrRefused(t *testing.T) {
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

	if !c.incr.Manifest.ApplyIdentity {
		t.Fatal("the captured incremental records no change identities; the identity arm would be vacuous")
	}
	reseed := func(t *testing.T) {
		applyDDL(t, c.dst, `TRUNCATE kl, k; INSERT INTO kl VALUES (-1, 'seed'); INSERT INTO k VALUES (-1, 'seed');`)
	}
	// The multiset of the keyless table, as a row count and a value sum, and
	// the keyed table's row and distinct-key counts: the SOURCE's are the
	// independent expected value.
	state := func(dsn string) string {
		return fe1Q(t, dsn, `SELECT (SELECT count(*)::text || '/' || coalesce(sum(v), 0)::text FROM kl) || ' ' ||
			(SELECT count(*)::text || '/' || count(DISTINCT id)::text FROM k)`)
	}
	want := state(c.src)
	for _, interrupt := range []string{"error", "cancel"} {
		for _, mode := range fe1Modes {
			t.Run("identities/"+interrupt+"/"+mode.name, func(t *testing.T) {
				reseed(t)
				streamID := "fe1-kli-" + interrupt + "-" + mode.name
				var err1 error
				if interrupt == "error" {
					disarm := c.failMiddleChunk(t)
					err1 = c.broker(streamID, mode.conc, c.fullID).Run(context.Background())
					disarm()
					if !errors.Is(err1, errBrokerCrashKill) {
						t.Fatalf("run 1 did not die at the kill: %v", err1)
					}
				} else {
					err1 = fe1CancelMidIncremental(t, c, c.broker(streamID, mode.conc, c.fullID))
					if err1 == nil || !strings.Contains(err1.Error(), BrokerIncrementalPartialMarker) {
						t.Fatalf("a cancel mid-incremental returned %v; want %s", err1, BrokerIncrementalPartialMarker)
					}
				}
				if kl := fe1Count(t, c.dst, "kl"); kl == "1" {
					t.Fatalf("run 1 committed no keyless row (err %v); the re-run cell would be vacuous", err1)
				}
				t.Logf("run 1: kl=%s k=%s, err=%v", fe1Count(t, c.dst, "kl"), fe1Count(t, c.dst, "k"), err1)
				if err := fe1RunToTailThenCancel(t, c, c.broker(streamID, mode.conc, "")); err != nil {
					t.Errorf("run 2: a cancel while idle returned %v; want nil (exit 0)", err)
				}
				if got := state(c.dst); got != want {
					t.Errorf("the re-run did not converge to the source: target kl rows/sum k rows/distinct %s, source %s", got, want)
				}
			})
		}
	}

	stripChainIdentities(t, c)
	for _, interrupt := range []string{"error", "cancel"} {
		for _, mode := range fe1Modes {
			t.Run("no identities/"+interrupt+"/"+mode.name, func(t *testing.T) {
				reseed(t)
				if interrupt == "error" {
					defer c.failMiddleChunk(t)()
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
				if !strings.Contains(err.Error(), "records no change identities") {
					t.Errorf("the refusal must say the incremental records no identities: %v", err)
				}
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
		putBack := c.corruptMiddleChunk(t)
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
					putBack := c.corruptMiddleChunk(t)
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
// the target; a NOT NULL UNIQUE table is accepted. That is the no-identity
// arm; with identities (ADR-0191) the MySQL target's apply marks cover all
// but the surrogate-keyed table, the only one refused.
func TestFE1_Broker_MySQLTarget_KeylessDoor(t *testing.T) {
	src, dst, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, src, `
		CREATE TABLE kl  (v INT NOT NULL, note TEXT) ENGINE=InnoDB;
		CREATE TABLE nu  (u INT NULL UNIQUE, note TEXT) ENGINE=InnoDB;
		CREATE TABLE nnu (u INT NOT NULL UNIQUE, note TEXT) ENGINE=InnoDB;
		CREATE TABLE pkd (id INT NOT NULL PRIMARY KEY, note TEXT) ENGINE=InnoDB;
		CREATE TABLE sur (id INT NOT NULL PRIMARY KEY, note TEXT) ENGINE=InnoDB;
		CREATE TABLE two (id INT NOT NULL PRIMARY KEY, note TEXT) ENGINE=InnoDB;
		INSERT INTO kl VALUES (1, 'a'); INSERT INTO nu VALUES (NULL, 'a');
		INSERT INTO nnu VALUES (1, 'a'); INSERT INTO pkd VALUES (1, 'a');
		INSERT INTO sur VALUES (1, 'a'); INSERT INTO two VALUES (1, 'a');
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
	applyDDLMySQL(t, dst, `
		ALTER TABLE pkd DROP PRIMARY KEY;
		ALTER TABLE sur DROP PRIMARY KEY, ADD COLUMN sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY;
		ALTER TABLE two DROP PRIMARY KEY, ADD COLUMN sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, ADD UNIQUE KEY two_id (id);
	`)

	// The F-E1 review's HIGH 1 on MySQL: "sur" is keyed in the backup and
	// keyed on the target — on an AUTO_INCREMENT surrogate the backup's rows
	// never carry, so ODKU collides with nothing. "two" carries the same
	// surrogate PLUS a NOT NULL UNIQUE (id) the rows do supply; ODKU collides
	// on any unique key, so it converges and must not be refused.
	t.Run("restore re-run", func(t *testing.T) {
		err := (&backup.Restore{Target: mysqlEng, TargetDSN: dst, Store: store}).Run(ctx)
		assertFE1Refusal(t, err, sluicecode.CodeRestoreKeylessTableNotEmpty, []string{"kl", "nu", "pkd", "sur"}, []string{"nnu", "two"})
	})

	// The door's documented premise for the tables it does NOT refuse: a
	// keyed table re-loaded onto rows it already holds fails LOUDLY on both
	// MySQL bulk paths. The batched INSERT fails on 1062; LOAD DATA LOCAL
	// downgrades the 1062 to a warning and skips the row, and the writer's
	// post-load check must then refuse it as LOAD-DATA-ROWS-SKIPPED. The
	// target row is changed first, so a silent skip would also be visible
	// as the stale value surviving with a nil error.
	t.Run("keyed re-load is loud on both MySQL bulk paths", func(t *testing.T) {
		applyDDLMySQL(t, dst, `UPDATE nnu SET note = 'stale-target' WHERE u = 1;`)
		defer applyDDLMySQL(t, dst, `SET GLOBAL local_infile = 0;`)
		for _, infile := range []string{"0", "1"} {
			applyDDLMySQL(t, dst, `SET GLOBAL local_infile = `+infile+`;`)
			err := (&backup.Restore{
				Target: mysqlEng, TargetDSN: dst, Store: store,
				Filter: migcore.TableFilter{Include: []string{"nnu"}},
			}).Run(ctx)
			if err == nil {
				t.Fatalf("local_infile=%s: re-loading a keyed table onto a different row under the same key exited nil", infile)
			}
			if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeRestoreKeylessTableNotEmpty {
				t.Fatalf("local_infile=%s: the keyless door refused a keyed table: %v", infile, err)
			}
			if infile == "1" && !strings.Contains(err.Error(), "LOAD-DATA-ROWS-SKIPPED") {
				t.Errorf("local_infile=1: want the LOAD DATA path's LOAD-DATA-ROWS-SKIPPED refusal, got: %v", err)
			}
			if infile == "0" && !strings.Contains(err.Error(), "1062") {
				t.Errorf("local_infile=0: want the batched path's 1062, got: %v", err)
			}
		}
	})

	// The broker's door is per incremental (ADR-0191 §3.5), so it needs an
	// incremental that touches every table.
	applyDDLMySQL(t, src, `
		INSERT INTO kl VALUES (2, 'b'); INSERT INTO nu VALUES (2, 'b'); INSERT INTO nnu VALUES (2, 'b');
		INSERT INTO pkd VALUES (2, 'b'); INSERT INTO sur VALUES (2, 'b'); INSERT INTO two VALUES (2, 'b');
	`)
	// The window, not a change count, closes the incremental: the target is a
	// database on the same server, so the binlog also carries this test's
	// writes to it, as empty transactions in the source's filter.
	incrCtx, incrCancel := context.WithTimeout(ctx, 90*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source: mysqlEng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		Window: 10 * time.Second, MaxChanges: 1 << 20, ChunkChanges: 50, SluiceVersion: "test",
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	chain, err := (&SyncFromBackup{Store: store, ChainURL: "x", StreamID: "x"}).brokerChain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	incr := &chain[len(chain)-1]
	if lineage.CanonicalKind(incr.Manifest.Kind) != irbackup.BackupKindIncremental || !incr.Manifest.ApplyIdentity {
		t.Fatalf("the chain's tail is not an identity-bearing incremental: kind %q, apply_identity %v", incr.Manifest.Kind, incr.Manifest.ApplyIdentity)
	}
	touched := map[string]bool{}
	for _, ev := range decodeIncrementalEvents(t, store, incr) {
		if name, ok := rowChangeTable(ev); ok {
			touched[name] = true
		}
	}
	for _, name := range []string{"kl", "nu", "nnu", "pkd", "sur", "two"} {
		if !touched[name] {
			t.Fatalf("the incremental does not touch %s (touched %v); the per-incremental door would not judge it", name, touched)
		}
	}
	runBroker := func(streamID string) error {
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		return (&SyncFromBackup{
			Target: mysqlEng, TargetDSN: dst, Store: store, ChainURL: "test://" + streamID,
			StreamID: streamID, PollInterval: 2 * time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
			brokerStatePath: "manifests/broker_state_" + streamID + ".json",
		}).Run(runCtx)
	}

	// With identities, every table the target's apply marks cover is lifted —
	// kl, nu and pkd are keyless on the target, so their marks are table-wide —
	// and only sur, keyed on a surrogate no replayed row carries, has no mark
	// key and is refused.
	t.Run("identities", func(t *testing.T) {
		err := runBroker("fe1-mysql-ids")
		assertFE1Refusal(t, err, sluicecode.CodeBrokerKeylessTable, []string{"sur"}, []string{"kl", "nu", "pkd", "nnu", "two"})
		if !strings.Contains(err.Error(), `apply marks cannot make its replay exactly-once`) || !strings.Contains(err.Error(), `"sid"`) {
			t.Errorf("sur's refusal must say the marks cannot key it on the unsupplied sid: %v", err)
		}
	})

	// Without them, the four tables are refused on the judgments the interim
	// door made.
	stripChainIdentities(t, &fe1Chain{store: store, incr: incr})
	err = runBroker("fe1-mysql")
	assertFE1Refusal(t, err, sluicecode.CodeBrokerKeylessTable, []string{"kl", "nu", "pkd", "sur"}, []string{"nnu", "two"})
	msg := err.Error()
	if i := strings.Index(msg, `"sur"`); i < 0 || !strings.HasPrefix(msg[i+len(`"sur" (`):], string(migcore.ReplayKeylessTarget)) {
		t.Errorf("sur is keyed in the backup and on the target, but only on a surrogate the rows do not carry; "+
			"the refusal must say the TARGET judgment failed: %v", err)
	}
	if i := strings.Index(msg, `"pkd"`); i < 0 || !strings.Contains(msg[i:], string(migcore.ReplayKeylessTarget)) {
		t.Errorf("pkd is keyed in the backup and keyless only on the target; the refusal must say the TARGET judgment failed: %v", err)
	}
	if i := strings.Index(msg, `"nu"`); i < 0 || !strings.HasPrefix(msg[i+len(`"nu" (`):], string(migcore.ReplayKeylessRecorded)) {
		t.Errorf("nu's only key is a nullable UNIQUE; the refusal must say the RECORDED judgment failed: %v", err)
	}
}

// TestFE1_Broker_SurrogateKeyedTarget_RefusedBeforeAnything is the F-E1
// review's HIGH 1 on Postgres, ported from its reproduction. "k" is keyed
// (PRIMARY KEY id) in the backup; the target dropped that key and was
// re-keyed on a bigserial surrogate. Both judgments used to say "keyed" —
// the target HAS a primary key and the applier names it in its ON CONFLICT —
// but no replayed row carries sid, so every re-applied INSERT drew a fresh
// one: measured 2,000 target rows for 1,001 after an interrupted broker run
// and a re-run, and a duplicated seed row on a restore re-run, both at exit
// nil. "kk" is the control: keyed the same way on both sides, never named.
//
// The independent expected value is the target's own row count and the
// position table, read straight from Postgres.
func TestFE1_Broker_SurrogateKeyedTarget_RefusedBeforeAnything(t *testing.T) {
	c := fe1Setup(t, `
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		CREATE TABLE kk (id INT PRIMARY KEY, note TEXT);
		INSERT INTO k VALUES (-1, 'seed');
		INSERT INTO kk VALUES (-1, 'seed');
	`, `INSERT INTO k VALUES (i, 'r'||i); INSERT INTO kk VALUES (i, 'r'||i);`, func(c *fe1Chain) {
		applyDDL(t, c.dst, `ALTER TABLE k DROP CONSTRAINT k_pkey; ALTER TABLE k ADD COLUMN sid bigserial PRIMARY KEY;`)
	})

	for _, mode := range fe1Modes {
		t.Run("broker/"+mode.name, func(t *testing.T) {
			streamID := "fe1-sur-" + mode.name
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			err := c.broker(streamID, mode.conc, c.fullID).Run(ctx)
			assertFE1Refusal(t, err, sluicecode.CodeBrokerKeylessTable, []string{"k"}, []string{"kk"})
			if !strings.Contains(err.Error(), string(migcore.ReplayKeylessTarget)) {
				t.Errorf("the refusal must say the TARGET judgment failed: %v", err)
			}
			if k, kk := fe1Count(t, c.dst, "k"), fe1Count(t, c.dst, "kk"); k != "1" || kk != "1" {
				t.Errorf("target holds k=%s kk=%s after the refusal; want 1/1 — something was applied", k, kk)
			}
			if n := fe1Q(t, c.dst, fmt.Sprintf(`SELECT count(*)::text FROM sluice_cdc_state WHERE stream_id = '%s'`, streamID)); n != "0" {
				t.Errorf("the refused broker wrote %s position row(s)", n)
			}
		})
	}

	t.Run("restore re-run", func(t *testing.T) {
		pgEng, _ := engines.Get("postgres")
		err := (&backup.Restore{Target: pgEng, TargetDSN: c.dst, Store: c.store}).Run(context.Background())
		assertFE1Refusal(t, err, sluicecode.CodeRestoreKeylessTableNotEmpty, []string{"k"}, []string{"kk"})
		if k := fe1Count(t, c.dst, "k"); k != "1" {
			t.Errorf("k holds %s rows after the refused re-run; want the 1 seed row", k)
		}
	})
}

// TestFE1_Broker_KeyChangingIncremental_RerunRefusesLoudly binds the
// BROKER-INCREMENTAL-PARTIAL recovery text (F-E1 second review) to the
// broker end to end. Every source row is inserted and then moved to a new
// key inside one incremental — ONE source transaction. A run interrupted
// after part of it committed leaves moved rows on the target.
//
// Since ADR-0191 the outcome depends on the chain, and both arms are pinned.
// On a chain that records the reader's identities (what this binary writes)
// the re-run resumes at that transaction's start and ADR-0190's marks skip
// its committed changes — the moves included — so it CONVERGES to the source
// (the identity arm; TestBroker_KeyReuseIncremental_Converges carries the
// measured key-reuse shapes). On a chain WITHOUT identities (written before
// ADR-0191, rewritten by smart compaction; simulated here by re-encoding the
// chunks without them) the re-run re-applies the transaction unmarked,
// re-inserts a row the first run already moved, and its move then collides
// with the moved copy — loud on every re-run, never a silent convergence
// claim. A failed re-run is not guaranteed to leave the target untouched: in
// the lane apply mode the re-inserted row commits in one lane before its move
// collides in another (measured: 1,002 rows for the source's 1,001). So that
// arm's pin is loud-every-time and stable across re-runs, and the documented
// recovery is --reset-target-data.
//
// The independent expected value is the SOURCE's own row count and key
// checksum for the identity arm, and the target's before and after each
// re-run for the no-identity arm.
func TestFE1_Broker_KeyChangingIncremental_RerunRefusesLoudly(t *testing.T) {
	c := fe1Setup(t, `
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		INSERT INTO k VALUES (-1, 'seed');
	`, `INSERT INTO k VALUES (i, 'r'||i); UPDATE k SET id = id + 100000 WHERE id = i;`, nil)
	state := func(dsn string) string {
		return fe1Q(t, dsn, `SELECT count(*)::text || '/' || coalesce(sum(id), 0)::text FROM k`)
	}

	for _, mode := range fe1Modes {
		t.Run("identities/"+mode.name, func(t *testing.T) {
			applyDDL(t, c.dst, `TRUNCATE k; INSERT INTO k VALUES (-1, 'seed');`)
			streamID := "fe1-kci-" + mode.name
			putBack := c.corruptMiddleChunk(t)
			err1 := c.broker(streamID, mode.conc, c.fullID).Run(context.Background())
			putBack()
			if err1 == nil {
				t.Fatal("run 1 over a corrupted chunk returned nil; the interrupted-incremental setup did not happen")
			}
			if moved := fe1Q(t, c.dst, `SELECT count(*)::text FROM k WHERE id >= 100000`); moved == "0" {
				t.Fatalf("run 1 committed no key change (err %v); the re-run cell would be vacuous", err1)
			}
			if err := fe1RunToTailThenCancel(t, c, c.broker(streamID, mode.conc, "")); err != nil {
				t.Fatalf("the re-run of a key-changing incremental that records identities failed: %v", err)
			}
			if got, want := state(c.dst), state(c.src); got != want {
				t.Errorf("the re-run did not converge to the source: target rows/key-sum %s, source %s", got, want)
			}
		})
	}

	stripChainIdentities(t, c)
	for _, mode := range fe1Modes {
		t.Run("no identities/"+mode.name, func(t *testing.T) {
			applyDDL(t, c.dst, `TRUNCATE k; INSERT INTO k VALUES (-1, 'seed');`)
			streamID := "fe1-kc-" + mode.name
			putBack := c.corruptMiddleChunk(t)
			err1 := c.broker(streamID, mode.conc, c.fullID).Run(context.Background())
			putBack()
			if err1 == nil {
				t.Fatal("run 1 over a corrupted chunk returned nil; the interrupted-incremental setup did not happen")
			}
			moved := fe1Q(t, c.dst, `SELECT count(*)::text FROM k WHERE id >= 100000`)
			if moved == "0" {
				t.Fatalf("run 1 committed no key change (err %v); the re-run cell would be vacuous", err1)
			}
			targetState := func() string {
				return fe1Q(t, c.dst, `SELECT count(*)::text || '/' || coalesce(sum(id), 0)::text FROM k`)
			}
			afterRun1 := targetState()
			afterRerun := make([]string, 3)

			for rerun := 1; rerun <= 2; rerun++ {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				err := c.broker(streamID, mode.conc, "").Run(ctx)
				cancel()
				if err == nil {
					t.Fatalf("re-run %d of a key-changing incremental returned nil (exit 0); want a loud duplicate-key failure", rerun)
				}
				if !strings.Contains(err.Error(), "23505") {
					t.Errorf("re-run %d failed, but not on the key the moved rows collide on: %v", rerun, err)
				}
				afterRerun[rerun] = targetState()
			}
			t.Logf("target rows/key-sum: after the interrupted run %s, after re-run 1 %s, after re-run 2 %s",
				afterRun1, afterRerun[1], afterRerun[2])
			if mode.conc == 1 && afterRerun[1] != afterRun1 {
				t.Errorf("serial: the failed re-run changed the target (%s → %s); a serial batch rolls back whole", afterRun1, afterRerun[1])
			}
			if afterRerun[2] != afterRerun[1] {
				t.Errorf("the second failed re-run changed the target again (%s → %s): re-runs are not stable", afterRerun[1], afterRerun[2])
			}
			pos := fe1Q(t, c.dst, fmt.Sprintf(`SELECT coalesce(string_agg(source_position, ','), '') FROM sluice_cdc_state WHERE stream_id = '%s'`, streamID))
			if tok, err := decodeBrokerPosition(ir.Position{Token: pos}); err != nil || tok.LastAppliedBackupID == lineage.ManifestBackupID(c.incr.Manifest) {
				t.Errorf("a failed re-run advanced the position past the key-changing incremental: %q (%v)", pos, err)
			}
		})
	}
}

// stripChainIdentities rewrites the data incremental as an older sluice (or
// smart compaction) leaves it: the same changes, with no `aid` on any of them
// and no ApplyIdentity on the manifest. The chunks keep their paths; their
// SHAs move, and the manifest is rewritten in place with the new ones.
func stripChainIdentities(t *testing.T, c *fe1Chain) {
	t.Helper()
	ctx := context.Background()
	seg := c.incr.Segment.Store(c.store)
	m := c.incr.Manifest
	for idx, chunk := range m.ChangeChunks {
		src, err := blobcodec.FetchChunkVerified(ctx, seg, chunk.File, chunk.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, nil, c.incr.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(m, chunk, idx))
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, c.incr.Segment.CodecOrDefault(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for {
			ch, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := cw.WriteChange(ir.WithoutApplyID(ch)); err != nil {
				t.Fatal(err)
			}
		}
		_ = cr.Close()
		if err := cw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := seg.Put(ctx, chunk.File, bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		chunk.SHA256 = cw.Hash()
	}
	m.ApplyIdentity = false
	if err := lineage.WriteManifestAt(ctx, seg, c.incr.Path, m); err != nil {
		t.Fatal(err)
	}
}
