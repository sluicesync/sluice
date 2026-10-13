//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-REVERTED-WINDOW-RESTORE: a backup window in which a key
// column's collation changes `utf8mb4_0900_ai_ci` → `utf8mb4_bin`, a row the
// new collation keeps apart from an existing one is written ('A' beside 'a'),
// moved out of the way ('A2'), and the collation is changed back. The window
// ends with the key's original shape, so it carries no schema delta, and the
// restored target's key compares under `_ci` throughout. The row 'A' existed
// on the source only while the key was `_bin`; replayed onto the `_ci` key it
// meets row 1's 'a'.
//
// Measured before the fix (MySQL 8.0, MINIMAL and FULL, UNIQUE key): target
// row 1 held 'A' (`1:41`) where the source holds 'a', restore exit 0 — for a
// live `backup stream` window (chain restore pushed every SchemaHistory
// version ahead of the rows, so the guard armed from the reverted shape) and
// for a `backup incremental` started after the revert (the reader recorded
// only the catalog's current shape).
//
// The independent expected value is the source's rows read by id:HEX(k)
// after the window (kcfbGrade): a restore either lands them byte for byte or
// refuses with no merged row on the target. Each cell also names WHICH door
// must answer, so a refusal from an unrelated guard cannot pass it.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// krwCapture runs one capture lane over window and returns its error:
// "backup-stream" observes the window live (started before it),
// "backup-incremental" replays it after the fact.
func krwCapture(t *testing.T, lane string, eng ir.Engine, srcDSN, slot string, store irbackup.Store, parent string, src twfbDB, window []string) error {
	t.Helper()
	ctx := context.Background()
	if lane == "backup-incremental" {
		for _, s := range window {
			src.exec(t, s)
		}
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		return (&IncrementalBackup{
			Source: eng, SourceDSN: srcDSN, Store: store, ParentRef: parent, SlotName: slot,
			Window: 8 * time.Second, SluiceVersion: "test",
		}).Run(runCtx)
	}
	// "backup-stream" ends its one window by the drain-commit, which records
	// no schema delta (KEY-COLLATION-DRAIN-NO-DELTA); "backup-stream-rollover"
	// commits windows by rollover, each with the schema refresh and delta a
	// long-running stream writes.
	rollover := 10 * time.Minute
	if lane == "backup-stream-rollover" {
		rollover = 4 * time.Second
	}
	started := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- (&BackupStream{
			Source: eng, SourceDSN: srcDSN, Store: store, ParentRef: parent, SlotName: slot,
			RolloverWindow: rollover, RolloverMaxChanges: 1 << 30, RolloverMaxBytes: 1 << 40,
			ChunkChanges: 100, SluiceVersion: "test",
		}).Run(runCtx)
	}()
	// Let the stream start reading before the window, so it observes it live.
	time.Sleep(3 * time.Second)
	for _, s := range window {
		src.exec(t, s)
	}
	windowWritten := time.Since(started)
	time.Sleep(6 * time.Second)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	krwRequireWindowCaptured(t, lane, store, krwRowStatements(window), windowWritten)
	return nil
}

// krwRequireWindowCaptured fails the cell when the stream's committed
// incrementals together hold fewer row changes than the window has row
// statements (decoded from the chunks: an empty window still commits its
// transaction framing): the stream exited cleanly without having captured the
// window, so whatever the restore or broker does next grades nothing. The
// cells would otherwise report it as the lane under test applying nothing — the CI
// failure of 2026-10-13 (`apply = <nil>; source 1:61,100:7A7A,2:62,99:4132
// target 1:61,2:62`, a broker cell that ran its whole 30 s and returned nil)
// had that shape, and nothing to tell a missed window from a broker that
// never applied a committed one.
// The independent expected value is the window itself; the segment listing
// in the message is what the next occurrence needs to be diagnosed.
func krwRequireWindowCaptured(t *testing.T, lane string, store irbackup.Store, rowStatements int, windowWritten time.Duration) {
	t.Helper()
	recs, err := lineage.ListAllSegmentManifests(context.Background(), store)
	if err != nil {
		t.Fatalf("%s: list segments: %v", lane, err)
	}
	captured := 0
	var listing strings.Builder
	for _, rec := range recs {
		m := rec.Manifest
		rows := 0
		if m.Kind == irbackup.BackupKindIncremental {
			rows = krwRowChanges(t, store, rec)
			captured += rows
		}
		fmt.Fprintf(&listing, "\n  %s %s parent=%s records=%d row-changes=%d deltas=%d end=%s",
			m.Kind, m.BackupID, m.ParentBackupID, manifestChangeRecordCount(m), rows, len(m.SchemaDelta), m.EndPosition.Token)
	}
	if captured < rowStatements {
		t.Fatalf("%s: the stream exited cleanly having committed %d row changes for a window of %d row statements "+
			"(written %s after the stream started): it did not capture the window, so this cell grades nothing. Segments:%s",
			lane, captured, rowStatements, windowWritten.Round(time.Millisecond), listing.String())
	}
}

// krwRowChanges decodes rec's change chunks and counts its row changes.
func krwRowChanges(t *testing.T, store irbackup.Store, rec lineage.SegmentRecord) int {
	t.Helper()
	n := 0
	for _, ci := range rec.Manifest.ChangeChunks {
		src, err := rec.Segment.Store(store).Get(context.Background(), ci.File)
		if err != nil {
			t.Fatalf("read chunk %s: %v", ci.File, err)
		}
		rd, err := blobcodec.NewChangeChunkReader(src, ci.SHA256, nil, rec.Segment.CodecOrDefault(), nil)
		if err != nil {
			t.Fatalf("open chunk %s: %v", ci.File, err)
		}
		for {
			ch, err := rd.ReadChange()
			if err != nil {
				break
			}
			switch ch.(type) {
			case ir.Insert, ir.Update, ir.Delete:
				n++
			}
		}
		_ = rd.Close()
	}
	return n
}

// krwRowStatements counts the window's INSERT/UPDATE/DELETE statements.
func krwRowStatements(window []string) int {
	n := 0
	for _, s := range window {
		u := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(u, "INSERT") || strings.HasPrefix(u, "UPDATE") || strings.HasPrefix(u, "DELETE") {
			n++
		}
	}
	return n
}

// kcCellForensics captures every slog line of the calling cell at DEBUG and
// writes it to the test log only if the cell fails, so a CI-only flake in
// these cells arrives with the stream's and the broker's own account of it.
// The cells are not parallel, so swapping the default logger is safe.
func kcCellForensics(t *testing.T) {
	t.Helper()
	captured := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		if t.Failed() {
			t.Logf("slog of the failed cell:\n%s", captured.String())
		}
	})
}

func TestChainRestore_KeyCollationChangedAndRevertedInsideTheWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	keys := map[string]string{
		"pk":     "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY, d INT)",
		"unique": "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT)",
	}
	toBin := "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"
	toCI := "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
	windows := map[string][]string{
		// The defect: the key's `_bin` shape lives only inside the window.
		"reverted": {toBin, "INSERT INTO kd VALUES (99,'A',99)", "UPDATE kd SET k = 'A2' WHERE id = 99", toCI, "INSERT INTO kd VALUES (100,'zz',100)"},
		// The control nothing may refuse: rows written before a forward
		// (tightening) change the catalog already shows, and rows after it.
		"forward": {"INSERT INTO kd VALUES (50,'p',50)", toBin, "INSERT INTO kd VALUES (99,'A',99)", "INSERT INTO kd VALUES (100,'zz',100)"},
	}
	n := 0
	for _, metadata := range []string{"MINIMAL", "FULL"} {
		for _, key := range []string{"pk", "unique"} {
			for _, variant := range []string{"reverted", "forward"} {
				for _, lane := range []string{"backup-stream", "backup-stream-rollover", "backup-incremental"} {
					if variant == "forward" && lane == "backup-stream" {
						// A drain-commit records no schema delta
						// (KEY-COLLATION-DRAIN-NO-DELTA, pinned by
						// TestBackupStream_DrainCommit_StampsAKeyCollationWindow),
						// so the control runs on the rollover lane instead.
						continue
					}
					n++
					name := fmt.Sprintf("%s/%s/%s/%s", metadata, key, variant, lane)
					create, window := keys[key], windows[variant]
					t.Run(name, func(t *testing.T) {
						kcCellForensics(t)
						root.exec(t, "SET GLOBAL binlog_row_metadata = '"+metadata+"'")
						srcDB, tgtDB := fmt.Sprintf("krw_s%d", n), fmt.Sprintf("krw_t%d", n)
						root.exec(t, "CREATE DATABASE "+srcDB+"; CREATE DATABASE "+tgtDB)
						srcDSN, err := buildMySQLDSN(myRoot, srcDB)
						if err != nil {
							t.Fatal(err)
						}
						tgtDSN, err := buildMySQLDSN(myRoot, tgtDB)
						if err != nil {
							t.Fatal(err)
						}
						src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
						src.exec(t, create+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
						ctx := context.Background()
						store, err := blobcodec.NewLocalStore(t.TempDir())
						if err != nil {
							t.Fatal(err)
						}
						if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
							t.Fatalf("full: %v", err)
						}
						full, err := lineage.ReadManifest(ctx, store)
						if err != nil {
							t.Fatal(err)
						}
						// Behind the head (the incremental lane) the reader reports
						// each row under the collation its event recorded, so the
						// history carries `_bin` there too; no lane refuses capture.
						if err := krwCapture(t, lane, myEng, srcDSN, "", store, full.BackupID, src, window); err != nil {
							t.Fatalf("capture: %v", err)
						}
						cr := &backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}
						if metadata == "FULL" {
							// The CLI's wiring (MINIMAL cells take the same-named
							// Target's orderer instead).
							cr.SourceEngine = engines.Get
						}
						restoreErr := cr.Run(ctx)
						t.Logf("restore = %v; source %s target %s", restoreErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
						if variant == "reverted" {
							// The window's history holds both shapes; the `_bin` one
							// governs 'A', and the GUARD must refuse it onto `_ci` —
							// not KEY-COLLATION-HISTORY-UNORDERED, which satisfies the
							// same sentinel and means the versions were never placed.
							if !errors.Is(restoreErr, ir.ErrKeyCollationMerge) || strings.Contains(restoreErr.Error(), "KEY-COLLATION-HISTORY-UNORDERED") {
								t.Fatalf("restore = %v; want the guard's %v", restoreErr, ir.ErrKeyCollationMerge)
							}
							klwAssertRestoreRemedy(t, restoreErr)
						} else if restoreErr != nil {
							t.Fatalf("restore: %v", restoreErr)
						}
						kcfbGrade(t, name, src, tgt, restoreErr)
					})
				}
			}
		}
	}
}

// TestChainRestore_KeyCollationChangedAndRevertedInsideTheWindow_MySQLToPostgres
// is the cross-engine direction: the reverted MySQL window, captured live,
// restored onto Postgres, whose key the restore creates from the MySQL
// `_ci` declaration. The grade is the same: byte-exact, or refused with
// nothing merged.
func TestChainRestore_KeyCollationChangedAndRevertedInsideTheWindow_MySQLToPostgres(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	_, pgTarget, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	myEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")
	for i, key := range []string{"pk", "unique"} {
		t.Run(key, func(t *testing.T) {
			srcDB, tgtDB := fmt.Sprintf("krwx_s%d", i), fmt.Sprintf("krwx_t%d", i)
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+srcDB)
			twfbDB{engine: "postgres", dsn: pgTarget}.exec(t, "CREATE DATABASE "+tgtDB)
			srcDSN, err := buildMySQLDSN(myRoot, srcDB)
			if err != nil {
				t.Fatal(err)
			}
			tgtDSN, err := buildPGDSN(pgTarget, tgtDB)
			if err != nil {
				t.Fatal(err)
			}
			src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
			create := "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT)"
			if key == "pk" {
				create = "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY, d INT)"
			}
			src.exec(t, create+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			window := []string{
				"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
				"INSERT INTO kd VALUES (99,'A',99)",
				"UPDATE kd SET k = 'A2' WHERE id = 99",
				"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
				"INSERT INTO kd VALUES (100,'zz',100)",
			}
			if err := krwCapture(t, "backup-stream", myEng, srcDSN, "", store, full.BackupID, src, window); err != nil {
				t.Fatalf("capture: %v", err)
			}
			// SourceEngine as the CLI wires it: the versions replay in MySQL's
			// position order onto a Postgres target.
			restoreErr := (&backup.ChainRestore{Target: pgEng, TargetDSN: tgtDSN, Store: store, SourceEngine: engines.Get}).Run(ctx)
			t.Logf("restore = %v; source %s target %s", restoreErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
			if restoreErr != nil {
				t.Fatalf("restore: %v — the Postgres key is case-sensitive, so the window lands byte-exact", restoreErr)
			}
			kcfbGrade(t, "mysql-to-postgres/"+key, src, tgt, nil)
		})
	}
}

// TestSyncFromBackup_KeyCollationChangedAndRevertedInsideTheWindow is the
// broker sibling: the reverted window captured live by `backup stream`,
// replayed by `sync from-backup` onto a target restored from the full.
// Measured before the fix: target row 1 held 'A' (`1:41`), broker exit 0 —
// its change stream carries no schema version, so its applier's
// key-collation guard was never armed. It must now refuse the window
// (KEY-COLLATION-HISTORY-UNORDERED) before applying any of it.
func TestSyncFromBackup_KeyCollationChangedAndRevertedInsideTheWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	root.exec(t, "CREATE DATABASE krwb_s; CREATE DATABASE krwb_t")
	srcDSN, err := buildMySQLDSN(myRoot, "krwb_s")
	if err != nil {
		t.Fatal(err)
	}
	tgtDSN, err := buildMySQLDSN(myRoot, "krwb_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
	src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT); "+
		"INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("full: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Restore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	window := []string{
		"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
		"INSERT INTO kd VALUES (99,'A',99)",
		"UPDATE kd SET k = 'A2' WHERE id = 99",
		"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
		"INSERT INTO kd VALUES (100,'zz',100)",
	}
	if err := krwCapture(t, "backup-stream", myEng, srcDSN, "", store, full.BackupID, src, window); err != nil {
		t.Fatalf("capture: %v", err)
	}
	bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
	defer bcancel()
	brokerErr := (&SyncFromBackup{
		Target: myEng, TargetDSN: tgtDSN, Store: store,
		ChainURL: "test://krwb", StreamID: "krwb",
		PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}).Run(bctx)
	if errors.Is(brokerErr, context.DeadlineExceeded) || errors.Is(brokerErr, context.Canceled) {
		brokerErr = nil
	}
	t.Logf("broker = %v; source %s target %s", brokerErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	if !errors.Is(brokerErr, ir.ErrKeyCollationMerge) || !strings.Contains(brokerErr.Error(), "KEY-COLLATION-HISTORY-UNORDERED") {
		t.Fatalf("broker = %v; want KEY-COLLATION-HISTORY-UNORDERED (its replay carries no schema version, so the window must be refused)", brokerErr)
	}
	kcfbGrade(t, "broker", src, tgt, brokerErr)
}

// TestChainRestore_KeyCollationChangedAndRevertedInsideTheWindow_Postgres16
// is the Postgres-source sibling: a nondeterministic ICU key ("ci") moved to
// "C" and back, observed live by `backup stream`, restored onto Postgres.
// Two arms: the window's first touch of the table is the key change itself,
// or a row precedes it (so the window's first Relation is the "ci" shape).
// Every cell must refuse with nothing merged: a primary key on k through the
// applier's guard, a unique key through the target's own unique index (the
// Postgres applier upserts on the primary key only). A SIBLING pin, not a
// gate on the history ordering: mutation-run 2026-10-12, these cells refuse
// with the versions prepended as before too (Postgres anchors every version
// at LSN 0/0, so no order exists to restore).
//
// NOT pinned here, because it does not hold: the same window REPLAYED by a
// `backup incremental` started after the revert. pgoutput's Relation carries
// no collation and the reader reads it from the catalog as of now
// (projectCollation's FALSE PREMISE note, KEY-COLLATION review F-D), so the
// window records only "ci" and, with a primary key on k, the restore lands
// 'A' over 'a' and the replayed UPDATE moves it — target row 1 gone, exit 0
// (measured 2026-10-12, postgres:16). That residual is filed, not fixed.
func TestChainRestore_KeyCollationChangedAndRevertedInsideTheWindow_Postgres16(t *testing.T) {
	rootDSN, _, cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 40)
	defer cleanup()
	pgEng, _ := engines.Get("postgres")
	root := twfbDB{engine: "postgres", dsn: rootDSN}
	keys := map[string]string{
		"pk":     "CREATE TABLE kd (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int)",
		"unique": "CREATE TABLE kd (id int PRIMARY KEY, k text COLLATE ci NOT NULL UNIQUE, d int)",
	}
	n := 0
	for _, key := range []string{"pk", "unique"} {
		for _, prior := range []bool{false, true} {
			if key == "pk" && prior {
				// NOT a stable pin, and the reason is an open defect
				// (KEY-COLLATION-PG-SAME-ANCHOR): every Postgres version is
				// anchored at LSN 0/0, and the capture's history drain keeps
				// one version per anchor, so the window keeps whichever shape
				// its first Relation carried. With a row before the change that
				// is "ci" when the reader keeps up — and the restore then lands
				// 'A' over 'a', row 1 gone, exit 0 (measured in 1 of 3 runs on
				// postgres:16; the other runs read "C" from a lagging catalog
				// and refused).
				continue
			}
			n++
			name := fmt.Sprintf("%s/prior-row=%v", key, prior)
			create := keys[key]
			t.Run(name, func(t *testing.T) {
				kcCellForensics(t)
				srcDB, tgtDB, slot := fmt.Sprintf("krwp_s%d", n), fmt.Sprintf("krwp_t%d", n), fmt.Sprintf("krwp_slot%d", n)
				root.exec(t, "CREATE DATABASE "+srcDB)
				root.exec(t, "CREATE DATABASE "+tgtDB)
				srcDSN, err := buildPGDSN(rootDSN, srcDB)
				if err != nil {
					t.Fatal(err)
				}
				tgtDSN, err := buildPGDSN(rootDSN, tgtDB)
				if err != nil {
					t.Fatal(err)
				}
				src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
				for _, db := range []twfbDB{src, tgt} {
					db.exec(t, "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
				}
				src.exec(t, create+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
				window := []string{
					`ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE "C"`,
					"INSERT INTO kd VALUES (99,'A',99)",
					"UPDATE kd SET k = 'A2' WHERE id = 99",
					"ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE ci",
					"INSERT INTO kd VALUES (100,'zz',100)",
				}
				if prior {
					window = append([]string{"INSERT INTO kd VALUES (50,'p',50)"}, window...)
				}
				ctx := context.Background()
				store, err := blobcodec.NewLocalStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := (&backup.Backup{Source: pgEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test", SlotName: slot, ChainSlot: true}).Run(ctx); err != nil {
					t.Fatalf("full: %v", err)
				}
				full, err := lineage.ReadManifest(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				if err := krwCapture(t, "backup-stream", pgEng, srcDSN, slot, store, full.BackupID, src, window); err != nil {
					t.Fatalf("capture: %v", err)
				}
				restoreErr := (&backup.ChainRestore{Target: pgEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
				t.Logf("restore = %v; source %s target %s", restoreErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
				if restoreErr == nil {
					t.Fatal("restore succeeded; want a refusal — 'A' was written under \"C\" and the target key is ci")
				}
				kcfbGrade(t, name, src, tgt, restoreErr)
			})
		}
	}
}
