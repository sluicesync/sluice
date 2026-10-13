//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-LOOSENED-WINDOW: the MySQL twin of
// KEY-COLLATION-PG-LOOSENED-REPLAY. The key is `utf8mb4_bin` at the full,
// holding (1,'a'); inside one window 'A' is written beside it (distinct under
// `_bin`), moved out of the way, and the key is LOOSENED to
// `utf8mb4_0900_ai_ci`. The window's delta (`_bin` → `_ci`) is applied to the
// target before any of its rows, so 'A' meets 'a' on a `_ci` key.
//
// The independent expected value is the source's rows read by id:HEX(k)
// after the window (kcfbGrade): byte-exact, or refused with nothing merged.
//
// Bug 306: the window runs with and without a row of kd after the loosening.
// Without one, the window records no version of kd, and the window-start
// version — built from a MySQL full manifest, which names no namespace — was
// filed under `kd` while the rows looked up `<db>.kd`: the guard never armed
// and both capture lanes restored row 1 merged at exit 0. The target's own
// schema history is read as the second, independent witness: every version
// of kd must name the database the rows name.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

const (
	klwToCI = "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
)

func klwKeys() map[string]string {
	return map[string]string{
		"pk":     "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY, d INT)",
		"unique": "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL UNIQUE, d INT)",
	}
}

// klwWindow is the window: 'A' written beside 'a' under `_bin`, moved out
// of the way, and the key loosened. trailing adds a later row of kd, which
// gives the window a recorded version of the table naming its database —
// without it, the loosening is the window's last event for kd and the
// window records no version of kd at all (Bug 306).
func klwWindow(trailing bool) []string {
	w := []string{"INSERT INTO kd VALUES (99,'A',99)", "UPDATE kd SET k = 'A2' WHERE id = 99", klwToCI}
	if trailing {
		w = append(w, "INSERT INTO kd VALUES (100,'zz',100)")
	}
	return w
}

// klwLanes is every lane for the trailing-row window; the no-trailing-row
// window (Bug 306) runs the lanes that replay a delta (the drain-commit lane
// records none, so it never reaches the shape).
func klwLanes(trailing bool) []string {
	if trailing {
		return []string{"backup-stream", "backup-stream-rollover", "backup-incremental", "broker", "broker-rollover"}
	}
	return []string{"backup-stream-rollover", "backup-incremental", "broker-rollover"}
}

// klwAssertRestoreRemedy pins Bug 308 on the restore path, end to end: the
// refusal's remedy LEADS with the fresh full backup, and names nothing
// `restore` cannot run — no --restart-from-scratch (a `sync start` flag), no
// "ALTER the key and restart" (a re-run starts over from the full).
func klwAssertRestoreRemedy(t *testing.T, err error) {
	t.Helper()
	_, remedy, ok := strings.Cut(err.Error(), "(the whole statement rolled back). ")
	if !ok || !strings.HasPrefix(remedy, "Take a fresh full backup after the key change") {
		t.Errorf("restore refusal does not lead with the fresh full backup: %v", err)
	}
	for _, f := range []string{"--restart-from-scratch", "and restart", "sync start"} {
		if strings.Contains(err.Error(), f) {
			t.Errorf("restore refusal names %q, which `restore` cannot run: %v", f, err)
		}
	}
}

// klwHistoryNamespaces renders the target's schema-history rows for kd as
// "[schema_name]" each — the namespace the seed and the window's rows must
// agree on (Bug 306: the seed was written under "").
func klwHistoryNamespaces(t *testing.T, tgt twfbDB) string {
	t.Helper()
	return tgt.scalar(t, "SELECT IFNULL(GROUP_CONCAT(CONCAT('[', schema_name, ']') ORDER BY anchor_position SEPARATOR ','), '') "+
		"FROM sluice_cdc_schema_history WHERE table_name = 'kd'")
}

func TestChainRestore_KeyCollationLoosenedInsideTheWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	n, graded := 0, 0
	for _, trailing := range []bool{true, false} {
		for _, metadata := range []string{"MINIMAL", "FULL"} {
			for _, key := range []string{"pk", "unique"} {
				for _, lane := range klwLanes(trailing) {
					n++
					name := fmt.Sprintf("%s/%s/%s", metadata, key, lane)
					if !trailing {
						name = "no row after the loosening/" + name
					}
					create := klwKeys()[key]
					t.Run(name, func(t *testing.T) {
						kcCellForensics(t)
						root.exec(t, "SET GLOBAL binlog_row_metadata = '"+metadata+"'")
						srcDB, tgtDB := fmt.Sprintf("klw_s%d", n), fmt.Sprintf("klw_t%d", n)
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
						captureLane, broker := lane, strings.HasPrefix(lane, "broker")
						if broker {
							captureLane = strings.Replace(lane, "broker", "backup-stream", 1)
							if err := (&backup.Restore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
								t.Fatalf("seed restore: %v", err)
							}
						}
						captureErr := krwCapture(t, captureLane, myEng, srcDSN, "", store, full.BackupID, src, klwWindow(trailing))
						if captureErr != nil {
							// Only the key-collation guard may refuse a capture here;
							// any other capture error would otherwise pass the cell
							// having graded nothing.
							if !errors.Is(captureErr, ir.ErrKeyCollationMerge) {
								t.Fatalf("capture = %v; want success or the guard's KEY-COLLATION-MERGE", captureErr)
							}
							t.Logf("capture refused: %v", captureErr)
							return
						}
						var applyErr error
						if broker {
							bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
							applyErr = (&SyncFromBackup{
								Target: myEng, TargetDSN: tgtDSN, Store: store,
								ChainURL: "test://klwb", StreamID: "klwb",
								PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
							}).Run(bctx)
							bcancel()
							if errors.Is(applyErr, context.DeadlineExceeded) || errors.Is(applyErr, context.Canceled) {
								applyErr = nil
							}
						} else {
							applyErr = (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
						}
						t.Logf("apply = %v; source %s target %s", applyErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
						if !broker {
							// Bug 306: every version of kd the restore wrote names the
							// database the window's rows name, where their lookup looks.
							history := klwHistoryNamespaces(t, tgt)
							t.Logf("target schema history for kd: %s (source database %s)", history, srcDB)
							if strings.Contains(history, "[]") {
								t.Errorf("a version of kd was written naming no namespace (%s); the rows name %s", history, srcDB)
							}
							if lane != "backup-stream" && !strings.Contains(history, "["+srcDB+"]") {
								t.Errorf("no version of kd names %s (%s)", srcDB, history)
							}
						}
						unordered := applyErr != nil && strings.Contains(applyErr.Error(), "KEY-COLLATION-HISTORY-UNORDERED")
						switch {
						case lane == "backup-stream":
							// The drain-commit records no delta (KEY-COLLATION-DRAIN-NO-DELTA),
							// so the target key stays `_bin` and the window lands exactly.
							if applyErr != nil {
								t.Fatalf("apply = %v; want the window exact on the unchanged `_bin` key", applyErr)
							}
						case broker:
							// The broker places no version between a window's changes, so
							// it refuses a window that changes a key's collation.
							if !unordered {
								t.Fatalf("apply = %v; want KEY-COLLATION-HISTORY-UNORDERED", applyErr)
							}
						default:
							// The delta loosens the target first; the window-start version
							// arms the guard for 'A', written under `_bin`.
							if !errors.Is(applyErr, ir.ErrKeyCollationMerge) || unordered {
								t.Fatalf("apply = %v; want the guard's KEY-COLLATION-MERGE (from the window-start version)", applyErr)
							}
							// A restart re-applies the loosening ahead of the same rows,
							// so the remedy that converges is a fresh full backup.
							klwAssertRestoreRemedy(t, applyErr)
						}
						kcfbGrade(t, name, src, tgt, applyErr)
						graded++
					})
				}
			}
		}
	}
	// Anti-vacuity floor: every cell reaches the grade — the 20 trailing-row
	// cells (measured on CI and locally, #353) and the 12 no-trailing-row
	// cells Bug 306 added (measured locally: none refuses at capture). A
	// capture that starts refusing must be looked at, not absorbed by the
	// early return above.
	if n != 32 || graded < n {
		t.Errorf("%d of %d cells reached the grade; want all 32", graded, n)
	}
}

// TestChainRestore_KeyCollationLoosenedWindowEndsOnItsRecordedShape pins the
// window-end version (backup.windowEndVersions). Window 1 loosens kd's key
// with nothing colliding and no row of kd after the ALTER — but a row of
// another table after it, so the window's end position lies past the ALTER
// and window 2's reader, seeded from window 1's recorded `_ci` shape, owes
// kd no boundary: neither window records a version of the `_ci` shape.
// Window 2 changes row 1's key 'a' → 'A' — on the now `_ci` source a
// case-only change of one row, which merges nothing. Without a version of
// the shape window 1 ended on, the window-start `_bin` version (which Bug
// 306's fix made reachable: it used to be filed where nothing looked) would
// stay the applier's active shape for window 2. The chain must restore
// byte-exact (graded against the source by id:HEX(k)), and the versions the
// restore handed the applier must include kd's `_ci` shape under the
// database. The target's sluice_cdc_schema_history is read as the witness
// of that sequence (each version is written there in the transaction that
// caches it), NOT as resume state: it is filed under ChainRestoreStreamID,
// which nothing primes from (review of 7ff76a4c, L1). What this does not
// show: that the stale `_bin` shape would have refused anything in window 2
// — with the window-end version removed, this chain still restored exactly.
func TestChainRestore_KeyCollationLoosenedWindowEndsOnItsRecordedShape(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	for i, key := range []string{"pk", "unique"} {
		t.Run(key, func(t *testing.T) {
			// No floor here: this cell has no early return — every capture
			// and restore error is fatal — so it grades or fails.
			kcCellForensics(t)
			srcDB, tgtDB := fmt.Sprintf("klwe_s%d", i), fmt.Sprintf("klwe_t%d", i)
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
			src.exec(t, klwKeys()[key]+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2); CREATE TABLE other (id INT NOT NULL PRIMARY KEY)")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			incr := func() {
				t.Helper()
				runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				if err := (&IncrementalBackup{Source: myEng, SourceDSN: srcDSN, Store: store, Window: 8 * time.Second, SluiceVersion: "test"}).Run(runCtx); err != nil {
					t.Fatalf("incremental: %v", err)
				}
			}
			src.exec(t, "INSERT INTO kd VALUES (50,'q',50)")
			src.exec(t, klwToCI)
			src.exec(t, "INSERT INTO other VALUES (1)")
			incr()
			if key == "pk" {
				src.exec(t, "UPDATE kd SET k = 'A' WHERE k = 'a'")
			} else {
				src.exec(t, "UPDATE kd SET k = 'A' WHERE id = 1")
			}
			incr()
			cr := &backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store, SourceEngine: engines.Get}
			restoreErr := cr.Run(ctx)
			history := tgt.scalar(t, "SELECT IFNULL(GROUP_CONCAT(CONCAT('[', schema_name, ']', "+
				"IF(ir_schema_json LIKE '%utf8mb4_bin%', '_bin', '_ci')) SEPARATOR ','), '') "+
				"FROM sluice_cdc_schema_history WHERE table_name = 'kd'")
			t.Logf("restore = %v; source %s target %s; kd history %s", restoreErr,
				kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)), history)
			if restoreErr != nil {
				t.Fatalf("restore = %v; want the chain exact (window 2 merges nothing)", restoreErr)
			}
			kcfbGrade(t, key, src, tgt, nil)
			if !strings.Contains(history, "["+srcDB+"]_ci") {
				t.Errorf("kd's schema history (%s) holds no `_ci` shape under %s: the window-start `_bin` "+
					"version stayed the applier's last version of kd", history, srcDB)
			}
		})
	}
}

// TestChainRestore_KeyCollationLoosenedPlusAddColumnWindow pins the review
// of 7ff76a4c (HIGH, loud): one window writes a row of kd, loosens its key
// and adds a column to it, with nothing colliding. Both capture lanes append
// the ADD COLUMN fill — one UPDATE per row, by primary key — at the window's
// end, naming the namespace the flat MySQL manifest records: none. The
// first cut of Bug 306's fix bound the window's namespace from the row
// before and refused the fill's "" as a contradiction
// (KEY-COLLATION-VERSION-NAMESPACE), where v0.159.2 restored exactly. A
// change naming no namespace now neither binds nor contradicts it. The
// independent expected value is the source's rows read by id:HEX(k):e.
func TestChainRestore_KeyCollationLoosenedPlusAddColumnWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	rows := func(t *testing.T, db twfbDB) string {
		t.Helper()
		return db.scalar(t, "SELECT IFNULL(GROUP_CONCAT(CONCAT(id, ':', HEX(k), ':', IFNULL(e, 'NULL')) ORDER BY id), '') FROM kd")
	}
	// collide is Bug 306's own shape with the ADD COLUMN in the same window
	// (review of 5f1f19a6, LOW 5): 'A' written beside 'a' under `_bin`, moved
	// away, the key loosened, the column added. It must refuse with nothing
	// merged; the quiet window must restore exactly.
	windows := map[bool][]string{
		false: {"INSERT INTO kd VALUES (3,'c',3)", klwToCI, "ALTER TABLE kd ADD COLUMN e INT NOT NULL DEFAULT 7"},
		true: {
			"INSERT INTO kd VALUES (99,'A',99)", "UPDATE kd SET k = 'A2' WHERE id = 99", klwToCI,
			"ALTER TABLE kd ADD COLUMN e INT NOT NULL DEFAULT 7",
		},
	}
	n, graded, refusedAtCapture := 0, 0, 0
	for _, collide := range []bool{false, true} {
		for _, metadata := range []string{"MINIMAL", "FULL"} {
			for _, key := range []string{"pk", "unique"} {
				for _, lane := range []string{"backup-incremental", "backup-stream-rollover"} {
					n++
					name := fmt.Sprintf("%s/%s/%s", metadata, key, lane)
					if collide {
						name = "A beside a/" + name
					}
					t.Run(name, func(t *testing.T) {
						kcCellForensics(t)
						root.exec(t, "SET GLOBAL binlog_row_metadata = '"+metadata+"'")
						srcDB, tgtDB := fmt.Sprintf("klwa_s%d", n), fmt.Sprintf("klwa_t%d", n)
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
						src.exec(t, klwKeys()[key]+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
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
						captureErr := krwCapture(t, lane, myEng, srcDSN, "", store, full.BackupID, src, windows[collide])
						if metadata == "MINIMAL" && lane == "backup-incremental" {
							// Read behind the head under MINIMAL, the row written before the
							// ADD COLUMN cannot be decoded by name, and the reader refuses the
							// capture before anything is recorded — measured; the restore half
							// of this cell is unreachable. Only that refusal is accepted:
							// its code and its own text.
							ce, coded := sluicecode.FromError(captureErr)
							if !coded || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch ||
								!strings.Contains(captureErr.Error(), "binlog_row_metadata is not FULL") {
								t.Fatalf("capture = %v; want the reader's MINIMAL-behind-the-head refusal (%s)",
									captureErr, sluicecode.CodeCDCSchemaReplayMismatch)
							}
							refusedAtCapture++
							return
						}
						if captureErr != nil {
							t.Fatalf("capture: %v", captureErr)
						}
						restoreErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store, SourceEngine: engines.Get}).Run(ctx)
						s, g := rows(t, src), rows(t, tgt)
						t.Logf("restore = %v; source %s target %s", restoreErr, s, g)
						if !collide {
							if restoreErr != nil {
								t.Fatalf("restore = %v; want the window exact (nothing in it collides)", restoreErr)
							}
							if s != g {
								t.Errorf("target differs from the source by id:HEX(k):e\nsource: %s\ntarget: %s", s, g)
							}
							graded++
							return
						}
						// Refused, with nothing merged: every target row is the source's
						// row of the same id, key bytes and e, and the colliding row 99
						// never landed.
						if !errors.Is(restoreErr, ir.ErrKeyCollationMerge) {
							t.Fatalf("restore = %v; want KEY-COLLATION-MERGE", restoreErr)
						}
						klwAssertRestoreRemedy(t, restoreErr)
						srcRows := map[string]bool{}
						for _, r := range strings.Split(s, ",") {
							srcRows[r] = true
						}
						for _, r := range strings.Split(g, ",") {
							if r != "" && !srcRows[r] {
								t.Errorf("target row %s is not the source's (a merge)\nsource: %s\ntarget: %s", r, s, g)
							}
							if strings.HasPrefix(r, "99:") {
								t.Errorf("the colliding row 99 landed before the refusal: %s", g)
							}
						}
						graded++
					})
				}
			}
		}
	}
	// Anti-vacuity floor (#353's discipline): of the 16 cells, the 4 MINIMAL ×
	// backup-incremental cells (both windows, both keys) refuse at capture by
	// design and are asserted as that refusal; the other 12 must reach the
	// grade (measured locally: all 12 do).
	if n != 16 || graded < 12 || graded+refusedAtCapture < n {
		t.Errorf("%d of %d cells reached the grade and %d refused at capture as asserted; want 12 graded and the other 4 refused",
			graded, n, refusedAtCapture)
	}
}

// TestBackupStream_UnloggedKeyCollationChangeRefuses pins review of
// d6ecb939 finding 2 on a real server: a key collation changed with
// sql_log_bin=0 reaches the catalog and the TABLE_MAP but writes no QUERY
// event, so no schema boundary would carry it; the binlog reader refuses
// COLLATION-CHANGE-UNANNOUNCED (DEFERRED-SCHEMA-CHANGE) before decoding the
// row written under it, instead of reporting it under a shape no boundary
// announces.
func TestBackupStream_UnloggedKeyCollationChangeRefuses(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	for i, other := range []string{"", "ALTER TABLE other ADD COLUMN x INT"} {
		name := "alone"
		if other != "" {
			// A logged ALTER of an unrelated table first: routine on a busy
			// source, and it must not silence the check for kd.
			name = "after another table's logged ALTER"
		}
		t.Run(name, func(t *testing.T) {
			db := fmt.Sprintf("kul_s%d", i)
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+db)
			srcDSN, err := buildMySQLDSN(myRoot, db)
			if err != nil {
				t.Fatal(err)
			}
			src := twfbDB{engine: "mysql", dsn: srcDSN}
			src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT); "+
				"CREATE TABLE other (id INT NOT NULL PRIMARY KEY); INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
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
			// kd's first row of the window comes first, so the hidden change
			// is not its first touch.
			window := []string{"INSERT INTO kd VALUES (50,'p',50)"}
			if other != "" {
				window = append(window, other)
			}
			window = append(window,
				"SET SESSION sql_log_bin = 0; ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL; SET SESSION sql_log_bin = 1",
				"INSERT INTO kd VALUES (99,'A',99)")
			captureErr := krwCapture(t, "backup-stream", myEng, srcDSN, "", store, full.BackupID, src, window)
			if !errors.Is(captureErr, ir.ErrDeferredSchemaChange) || !strings.Contains(captureErr.Error(), "COLLATION-CHANGE-UNANNOUNCED") {
				t.Fatalf("capture = %v; want COLLATION-CHANGE-UNANNOUNCED", captureErr)
			}
		})
	}
}

// TestBackupIncremental_CollationAlteredAfterTheWindowEnds is the false
// refusal the third review of a0878c3c found. A manifest whose window carries
// a schema delta records the catalog read at window CLOSE, not the shape at
// its EndPosition. A LOGGED `ALTER … COLLATE` landing after the window's last
// event but before that read makes the parent record the new collation, and
// the next window replays the table's earlier rows, recorded under the old
// collation, against a basis seeded from that parent. Nothing was unlogged;
// the ALTER is still ahead in the stream. Measured before the fix: window 2
// refused COLLATION-CHANGE-UNANNOUNCED in both cells, on every restart (the
// parent is immutable). Now window 2 must capture, and the chain must
// restore byte-exact (graded against the source by id:HEX(k)).
//
// MaxChanges=1 closes window 1 after row 10. Row 11 and the ALTER (both
// already committed) lie past its EndPosition, while its end-of-window read
// sees the ALTER. Two shapes reach a parent recording the new collation:
//   - a key column, whose collation change is a delta of its own;
//   - a non-key column, whose collation change is no delta, carried into the
//     recorded schema because another table's ADD COLUMN in the same span is
//     (a non-key change alone leaves the parent's schema as it was —
//     measured, which is why a first draft of this cell passed vacuously).
func TestBackupIncremental_CollationAlteredAfterTheWindowEnds(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	cells := []struct{ name, create, alter, after string }{
		{
			"key",
			"CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT)",
			"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
			"",
		},
		{
			"non-key, beside another table's delta",
			"CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci, d INT); CREATE TABLE y (id INT NOT NULL PRIMARY KEY)",
			"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			"ALTER TABLE y ADD COLUMN z INT",
		},
	}
	for i, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			srcDB, tgtDB := fmt.Sprintf("kca_s%d", i), fmt.Sprintf("kca_t%d", i)
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
			src.exec(t, cell.create+"; INSERT INTO kd VALUES (1,'a',1)")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			src.exec(t, "INSERT INTO kd VALUES (10,'p',10)")
			src.exec(t, "INSERT INTO kd VALUES (11,'q',11)")
			src.exec(t, cell.alter)
			if cell.after != "" {
				src.exec(t, cell.after)
			}
			incr := func(maxChanges int) error {
				runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				return (&IncrementalBackup{
					Source: myEng, SourceDSN: srcDSN, Store: store,
					Window: 8 * time.Second, MaxChanges: maxChanges, SluiceVersion: "test",
				}).Run(runCtx)
			}
			if err := incr(1); err != nil {
				t.Fatalf("window 1: %v", err)
			}
			// The premise this cell needs: window 1 recorded the post-ALTER
			// collation from its end-of-window read.
			recs, err := lineage.ListAllSegmentManifests(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			if got := klwColumnCollation(recs[len(recs)-1].Manifest.Schema, "kd", "k"); got != "utf8mb4_bin" {
				t.Fatalf("window 1 recorded kd.k under %q; want utf8mb4_bin — the cell no longer reaches the shape it pins", got)
			}
			if err := incr(0); err != nil {
				t.Fatalf("window 2 refused rows recorded before a logged ALTER still ahead in the stream: %v", err)
			}
			applyErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
			if applyErr != nil {
				t.Fatalf("restore = %v; want the chain exact", applyErr)
			}
			kcfbGrade(t, cell.name, src, tgt, nil)
		})
	}
}

// klwColumnCollation is the collation s records for table.column, or "".
func klwColumnCollation(s *ir.Schema, table, column string) string {
	if s == nil {
		return ""
	}
	for _, tb := range s.Tables {
		if tb.Name != table {
			continue
		}
		for _, c := range tb.Columns {
			if c.Name == column {
				if v, ok := c.Type.(ir.Varchar); ok {
					return v.Collation
				}
			}
		}
	}
	return ""
}

// TestChainRestore_UnloggedRevertedKeyCollationBeforeTheWindowsFirstRow is
// the case the seeded basis caught and a skip of it would have opened: kd's
// key is changed to `_bin` with sql_log_bin=0, 'A' is written beside 'a',
// moved away, and the key is changed back, again unlogged — all before kd's
// first row of the window, so the reader's only basis is the seeded parent
// schema, no DDL is logged, the end-of-window read matches the parent, and
// the backup lanes arm no first touch. Measured with the seeded basis simply
// skipped: with no kd row after the revert, both lanes restored row 1 as 'A'
// — merged, at exit 0. Now kd's first row owes a boundary at the stream's
// start, so the window's history records the `_bin` shape the rows were
// written under. Graded through chain restore against the source by
// id:HEX(k): exact, or refused with nothing merged.
func TestChainRestore_UnloggedRevertedKeyCollationBeforeTheWindowsFirstRow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	unlogged := func(stmt string) string {
		return "SET SESSION sql_log_bin = 0; " + stmt + "; SET SESSION sql_log_bin = 1"
	}
	n, graded := 0, 0
	for _, trailing := range []bool{true, false} {
		for _, lane := range []string{"backup-incremental", "backup-stream-rollover"} {
			n++
			name := lane + "/no row after the revert"
			window := []string{
				unlogged("ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"),
				"INSERT INTO kd VALUES (99,'A',99)",
				"UPDATE kd SET k = 'A2' WHERE id = 99",
				unlogged("ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"),
			}
			if trailing {
				name = lane + "/a row after the revert"
				window = append(window, "INSERT INTO kd VALUES (100,'zz',100)")
			}
			t.Run(name, func(t *testing.T) {
				kcCellForensics(t)
				srcDB, tgtDB := fmt.Sprintf("kub_s%d", n), fmt.Sprintf("kub_t%d", n)
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
				src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT); INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
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
				captureErr := krwCapture(t, lane, myEng, srcDSN, "", store, full.BackupID, src, window)
				if captureErr != nil {
					// The only capture refusal this window may draw: the reader
					// meeting a row whose recorded collation no logged DDL
					// announced. Anything else would pass the cell ungraded.
					if !errors.Is(captureErr, ir.ErrDeferredSchemaChange) || !strings.Contains(captureErr.Error(), "COLLATION-CHANGE-UNANNOUNCED") {
						t.Fatalf("capture = %v; want success or DEFERRED-SCHEMA-CHANGE COLLATION-CHANGE-UNANNOUNCED", captureErr)
					}
					t.Logf("capture refused: %v", captureErr)
					return
				}
				applyErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
				t.Logf("apply = %v; source %s target %s", applyErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
				kcfbGrade(t, name, src, tgt, applyErr)
				graded++
			})
		}
	}
	// Anti-vacuity floor: the two "no row after the revert" cells have no
	// row for the reader to refuse at, so they always reach the grade (the
	// cells with a trailing row refuse at capture, measured on CI).
	if graded < 2 {
		t.Errorf("%d of %d cells reached the grade; want at least 2", graded, n)
	}
}
