//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 309: a chain restore's KEY-COLLATION-MERGE refusal, applied serially
// (--apply-concurrency=1) and by lanes, under both commands a chain restore
// runs inside (`restore`, and the `sync from-backup run --reset-target-data`
// cold start). The returned error must carry the sentinel and the command's
// own remedy (Bug 308), and NOTHING the run logs may carry the applier's
// stream remedy — `sync start --restart-from-scratch`, or a `sync from-backup
// run --reset-target-data` "which converges either way" — that the command
// cannot run, or that refuses again on the same chain.
//
// The independent expected value for the rows is the source read by
// id:HEX(k) (kcfbGrade); for the remedy it is the command's own wording
// (klwAssertRestoreRemedy, and the broker's fresh-full sentence below).

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
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// ksaStreamRemedyMarks are fragments only the applier's stream remedy
// (ir.KeyCollationMergeError.Remedy as the MySQL applier writes it) carries.
var ksaStreamRemedyMarks = []string{"--restart-from-scratch", "which converges either way", "and restart"}

func TestChainRestore_KeyCollationRefusal_SerialAndLaneApply(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	root.exec(t, "SET GLOBAL binlog_row_metadata = 'FULL'")
	n, graded := 0, 0
	for _, command := range []string{"restore", "broker-reset"} {
		for _, ac := range []int{1, 4} {
			n++
			t.Run(fmt.Sprintf("%s/apply-concurrency=%d", command, ac), func(t *testing.T) {
				// Every slog line of the run, at DEBUG: the refusal must not
				// reach the operator worded for a command that is not running.
				captured := &logcapture.Buffer{}
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelDebug})))
				t.Cleanup(func() {
					slog.SetDefault(prev)
					if t.Failed() {
						t.Logf("slog of the failed cell:\n%s", captured.String())
					}
				})

				srcDB, tgtDB := fmt.Sprintf("ksa_s%d", n), fmt.Sprintf("ksa_t%d", n)
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
				src.exec(t, klwKeys()["pk"]+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
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
				if err := krwCapture(t, "backup-incremental", myEng, srcDSN, "", store, full.BackupID, src, klwWindow(true)); err != nil {
					t.Fatalf("capture: %v", err)
				}
				captured.Reset()

				var applyErr error
				if command == "broker-reset" {
					bctx, bcancel := context.WithTimeout(ctx, 60*time.Second)
					applyErr = (&SyncFromBackup{
						Target: myEng, TargetDSN: tgtDSN, Store: store,
						ChainURL: "test://ksab", StreamID: fmt.Sprintf("ksab%d", n),
						PollInterval: time.Second, ResetTargetData: true, ApplyConcurrency: ac, SluiceVersion: "test",
					}).Run(bctx)
					bcancel()
				} else {
					applyErr = (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store, ApplyConcurrency: ac}).Run(ctx)
				}
				t.Logf("apply = %v; source %s target %s", applyErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))

				// The sentinel survives every apply path: the fleet supervisor's
				// do-not-restart roster and the remedy rewrite both key on it.
				if !errors.Is(applyErr, ir.ErrKeyCollationMerge) {
					t.Fatalf("apply = %v; want KEY-COLLATION-MERGE through errors.Is", applyErr)
				}
				if !ir.IsTerminal(applyErr) {
					t.Errorf("apply = %v; want it terminal (a retry replays the same row onto the same key)", applyErr)
				}
				if command == "broker-reset" {
					_, remedy, _ := strings.Cut(applyErr.Error(), "(the whole statement rolled back). ")
					if !strings.HasPrefix(remedy, "Take a fresh full backup after the key change, then replay the new chain from its full") {
						t.Errorf("broker refusal does not lead with the fresh full backup: %v", applyErr)
					}
					for _, f := range ksaStreamRemedyMarks {
						if strings.Contains(applyErr.Error(), f) {
							t.Errorf("broker refusal names %q: %v", f, applyErr)
						}
					}
				} else {
					klwAssertRestoreRemedy(t, applyErr)
				}
				// Bug 309: the serial batch loop logged the refusal at WARN, worded
				// for `sync start`, before the chain restore could replace its
				// remedy; the cycle read that line and ran a remedy that refuses
				// again.
				for _, line := range strings.Split(captured.String(), "\n") {
					if !strings.Contains(line, "KEY-COLLATION-MERGE") {
						continue
					}
					for _, f := range ksaStreamRemedyMarks {
						if strings.Contains(line, f) {
							t.Errorf("a log line carries the applier's stream remedy (%q), which this command cannot follow:\n%s", f, line)
						}
					}
				}
				kcfbGrade(t, command, src, tgt, applyErr)
				graded++
			})
		}
	}
	if graded != n || n != 4 {
		t.Errorf("%d of %d cells reached the grade; want all 4", graded, n)
	}
}
