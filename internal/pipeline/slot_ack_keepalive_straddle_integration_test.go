// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// TestStreamer_PostgresKeepaliveBoundaryStraddle_OpenTxnSurvivesResume pins
// the loss window of the GC-41 (j) keepalive boundary
// (postgres/cdc_keepalive_boundary.go). The pump stands a keepalive's
// ServerWALEnd in for a commit while no transaction is open ON THE STREAM —
// and a source transaction that has written but not committed is not open on
// the stream at all: pgoutput sends nothing for it until its commit is
// decoded. So a boundary E can be persisted on the target, and acked on the
// slot, while a source transaction T began below E and commits above it.
//
// Nothing in sluice protects T there. Its safety rests on the server: a
// resume from E must decode T whole, because the slot's restart_lsn is held
// back to T's first record independently of confirmed_flush_lsn, and T's
// commit lies at or after E so SnapBuildXactNeedsSkip does not skip it. The
// idle-source pin (slot_ack_idle_keepalive_integration_test.go) commits only
// after boundaries on a live connection; this one holds T open across a
// persisted, acked boundary and then ends the stream before T commits:
//
//   - stop: the stream is cancelled in process, the shape of `sync stop` /
//     SIGINT;
//   - crash: the built binary is killed (TerminateProcess / SIGKILL), so
//     nothing on the way out runs.
//
// Each runs on the serial batch path (W=1) and the concurrent lanes (W=4),
// with T small (not forced out of memory) and with T past
// logical_decoding_work_mem (its own spill files seen in pg_replslot before
// the stream ends, re-read on resume). T inserts N rows and also updates and
// deletes a seed row, so a partially decoded T shows.
//
// Independent expected value: the SOURCE table after T commits, read back
// from the source and compared row for row with the target — not the rows
// the test meant to write. Warm-resume witness: one seed row is deleted on
// the TARGET only, before the restart; a re-copy (which would deliver T by
// snapshot and grade nothing) brings it back, and the test refuses to grade.
// Window witness: before the stream ends, the target's persisted position and
// the slot's confirmed_flush_lsn must both be past pg_current_wal_insert_lsn()
// read inside T after its writes — so T's first record is below the
// boundary — and the case fails as vacuous if they never get there.
//
// Reach, mutation-checked: a reader that resumes at the server's current WAL
// instead of the persisted position turns all eight cases red on the SILENT
// LOSS assertion (T and the row after it missing). Ignoring the boundary's
// open flag (postgres keepaliveBoundary.due) stays green, and must: T is
// never open on the stream, so that flag does not guard this window — the
// server does.
// What the test grades is the server's premise and sluice's resume from the
// persisted position, not the pump's open-transaction gate.
func TestStreamer_PostgresKeepaliveBoundaryStraddle_OpenTxnSurvivesResume(t *testing.T) {
	src, dst, cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 16)
	defer cleanup()
	assertServerMajor(t, src, 16)

	// The spill cases need T past logical_decoding_work_mem; 64kB is the
	// floor. The small cases stay well under it; the spill cases prove their
	// shape by T's own spill files.
	applyPGDDL(t, src, "ALTER SYSTEM SET logical_decoding_work_mem = '64kB'")
	applyPGDDL(t, src, "SELECT pg_reload_conf()")

	bin := straddleBuildBinary(t)

	// The reader's premise check logs at ERROR; collect WARN and above for
	// the in-process runs (the binary's own log is checked per case).
	logs := &logcapture.Buffer{}
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prevLogger)

	// Foreign WAL keeps the walsender's read position moving while the
	// stream's own database is quiet, which is what makes keepalive
	// boundaries — and so the window — exist at all.
	stopNoise := make(chan struct{})
	var noise sync.WaitGroup
	noiseDSN := foreignWALDatabase(t, src)
	noise.Add(1)
	go func() {
		defer noise.Done()
		writeForeignWAL(noiseDSN, stopNoise)
	}()
	defer func() {
		close(stopNoise)
		noise.Wait()
	}()

	type straddleCase struct {
		name  string
		w     int
		crash bool
		rows  int
		spill bool
	}
	cases := make([]straddleCase, 0, 8)
	for _, path := range []struct {
		name string
		w    int
	}{{"serial", 1}, {"lanes", 4}} {
		for _, end := range []struct {
			name  string
			crash bool
		}{{"stop", false}, {"crash", true}} {
			cases = append(
				cases,
				straddleCase{name: path.name + "_" + end.name, w: path.w, crash: end.crash, rows: 50},
				straddleCase{name: path.name + "_" + end.name + "_spill", w: path.w, crash: end.crash, rows: 4000, spill: true},
			)
		}
	}

	pgEng, _ := engines.Get("postgres")
	t.Run("matrix", func(t *testing.T) {
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				id := "gc41j_straddle_" + c.name
				table := id
				slot := "sluice_" + id
				srcDB := pgNewDatabase(t, src, id+"_src")
				dstDB := pgNewDatabase(t, dst, id+"_dst")
				applyPGDDL(t, srcDB, fmt.Sprintf(`
					CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
					INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, 5) g;`, table, table))

				var run straddleRunner
				if c.crash {
					run = &straddleBinaryRun{
						t: t, bin: bin, logPath: filepath.Join(t.TempDir(), "sluice.log"),
						args: []string{
							"sync", "start",
							"--source-driver", "postgres", "--source", srcDB,
							"--target-driver", "postgres", "--target", dstDB,
							"--stream-id", id, "--slot-name", slot, "--publication-name", "pub_" + id,
							"--include-table", table,
							"--apply-batch-size", "1000", "--no-auto-tune",
							"--apply-concurrency", fmt.Sprint(c.w),
						},
					}
				} else {
					run = &straddleInProcessRun{newStreamer: func() *Streamer {
						return &Streamer{
							Source: pgEng, Target: pgEng, SourceDSN: srcDB, TargetDSN: dstDB,
							StreamID: id, SlotName: slot, PublicationName: "pub_" + id,
							Filter:           migcore.TableFilter{Include: []string{table}},
							ApplyBatchSize:   1000,
							ApplyConcurrency: c.w,
						}
					}}
				}
				defer run.kill()

				waitCount := func(n int) {
					t.Helper()
					for deadline := time.Now().Add(2 * time.Minute); pollRowCount(dstDB, table) < n; time.Sleep(200 * time.Millisecond) {
						if err := run.exited(); err != nil {
							t.Fatalf("the stream exited before the target reached %d rows: %v%s", n, err, run.logTail())
						}
						if time.Now().After(deadline) {
							t.Fatalf("the target never reached %d rows%s", n, run.logTail())
						}
					}
				}

				run.start()
				waitCount(5)
				// One CDC commit, so the stream holds a persisted CDC position
				// before T opens.
				applyPGDDL(t, srcDB, fmt.Sprintf("INSERT INTO %s VALUES (6, 'cdc')", table))
				waitCount(6)

				// T: begun, written, left open.
				srcConn, err := sql.Open("pgx", srcDB)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = srcConn.Close() }()
				ctx := context.Background()
				tx, err := srcConn.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				// Read before T writes anything: no xid is assigned yet, so
				// T's first record lies at or above this.
				tFirst := straddleInsertLSN(t, tx)
				pad := ""
				if c.spill {
					pad = strings.Repeat("x", 200)
				}
				if _, err := tx.ExecContext(ctx, fmt.Sprintf(
					"INSERT INTO %s SELECT g, 'straddle-' || g || $1 FROM generate_series(1000, %d) g",
					table, 1000+c.rows-1,
				), pad); err != nil {
					t.Fatalf("T insert: %v", err)
				}
				if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET v = 'straddled' WHERE id = 1", table)); err != nil {
					t.Fatalf("T update: %v", err)
				}
				if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 2", table)); err != nil {
					t.Fatalf("T delete: %v", err)
				}
				// After T's writes: a boundary past this is past T's first
				// record with T's whole body below it.
				tWritten := straddleInsertLSN(t, tx)
				var tXid int64
				if err := tx.QueryRowContext(ctx, "SELECT txid_current_if_assigned() % 4294967296").Scan(&tXid); err != nil {
					t.Fatalf("T's xid: %v", err)
				}

				// Wait for the window: a boundary above T's writes, persisted
				// on the target AND acked on the slot, while T is open.
				openAt := time.Now()
				deadline := openAt.Add(90 * time.Second)
				var conf, persisted pglogrepl.LSN
				for {
					if err := run.exited(); err != nil {
						t.Fatalf("the stream exited while T was open: %v%s", err, run.logTail())
					}
					var cok, pok bool
					conf, cok = readConfirmedFlushLSN(t, srcDB, slot)
					persisted, pok = readPersistedLSNPG(dstDB, id)
					if cok && pok && conf > persisted {
						t.Fatalf("confirmed_flush_lsn %s passed the target's persisted position %s (GC-41 (a))", conf, persisted)
					}
					if cok && pok && conf > tWritten && persisted > tWritten {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("anti-vacuity: %s after T opened, confirmed_flush_lsn %s and persisted %s are not both "+
							"past T's writes (%s) — no keepalive boundary straddled T, so the window was never "+
							"exercised%s", time.Since(openAt).Round(time.Second), conf, persisted, tWritten, run.logTail())
					}
					time.Sleep(time.Second)
				}
				restart := straddleRestartLSN(t, srcDB, slot)
				t.Logf("window open after %s: T first record >= %s, T written to %s; persisted %s, confirmed %s, restart_lsn %s",
					time.Since(openAt).Round(time.Second), tFirst, tWritten, persisted, conf, restart)
				// The premise the window rests on, observed: the server holds
				// the slot's restart point at or below T's first record.
				if restart > tFirst {
					t.Fatalf("restart_lsn %s is past T's first record (>= %s) while T is open — the server no longer "+
						"holds T's beginning for a resume", restart, tFirst)
				}
				// The spill shape, observed on T itself: its spill files in the
				// slot's directory while it is open and the boundary is past it.
				// Not asserted absent for the small cases — the 64kB limit is
				// the walsender's total, and catalog changes in the other cases'
				// databases can push the largest transaction (possibly T) out.
				spillFiles := straddleSpillFiles(t, srcDB, slot, tXid)
				t.Logf("T (xid %d) spill files: %d", tXid, spillFiles)
				if c.spill && spillFiles == 0 {
					t.Fatalf("anti-vacuity: no spill file for T (xid %d) in pg_replslot/%s — T never left memory", tXid, slot)
				}
				if got := pollRowCount(dstDB, table); got != 6 {
					t.Fatalf("the target holds %d rows while T is uncommitted; want 6", got)
				}

				// End the stream inside the window, then commit T.
				run.end()
				if p, ok := readPersistedLSNPG(dstDB, id); !ok || p <= tWritten {
					t.Fatalf("after the stream ended the persisted position is %s (ok=%v), not past T's writes %s", p, ok, tWritten)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit T: %v", err)
				}
				// A source-only commit after T: once it lands, the resumed
				// stream has decoded past T's commit.
				applyPGDDL(t, srcDB, fmt.Sprintf("INSERT INTO %s VALUES (7, 'after')", table))
				// Warm-resume witness, on the target only.
				applyPGDDL(t, dstDB, fmt.Sprintf("DELETE FROM %s WHERE id = 3", table))

				run.start()
				want := straddleRows(t, srcDB, table)
				delete(want, 3)
				var got map[int]string
				for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
					if err := run.exited(); err != nil {
						t.Fatalf("the resumed stream exited: %v%s", err, run.logTail())
					}
					got = straddleRows(t, dstDB, table)
					if _, ok := got[7]; ok && straddleEqual(got, want) {
						break
					}
					if time.Now().After(deadline) {
						break
					}
				}
				if _, ok := got[3]; ok {
					t.Fatalf("row 3, deleted on the target only, is back: the restart re-copied instead of resuming, " +
						"so T arrived by snapshot and the window was not graded")
				}
				if !straddleEqual(got, want) {
					t.Fatalf("SILENT LOSS (GC-41 (j)): after resuming from a keepalive boundary persisted while T was "+
						"open, the target differs from the source: %s%s", straddleDiff(got, want), run.logTail())
				}
				run.end()

				if strings.Contains(run.logTail(), postgres.KeepaliveBoundaryPassedCommitMarker) {
					t.Errorf("the binary logged %s%s", postgres.KeepaliveBoundaryPassedCommitMarker, run.logTail())
				}
			})
		}
	})
	if strings.Contains(logs.String(), postgres.KeepaliveBoundaryPassedCommitMarker) {
		t.Errorf("a stream logged %s:\n%s", postgres.KeepaliveBoundaryPassedCommitMarker, logs.String())
	}
}

// straddleRunner is one way of running a stream that can be started again
// after it ends.
type straddleRunner interface {
	start()
	// exited reports a stream that stopped by itself, nil while it runs.
	exited() error
	// end stops the stream: gracefully in process, by kill for the binary.
	end()
	// kill is the deferred teardown; safe after end.
	kill()
	// logTail is the end of the run's own log, for a failure message.
	logTail() string
}

// straddleInProcessRun runs a Streamer and stops it by cancelling its
// context — the shape of `sync stop` and SIGINT.
type straddleInProcessRun struct {
	newStreamer func() *Streamer
	cancel      context.CancelFunc
	done        chan error
	err         error
}

func (r *straddleInProcessRun) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done, r.err = cancel, make(chan error, 1), nil
	s := r.newStreamer()
	go func() { r.done <- s.Run(ctx) }()
}

func (r *straddleInProcessRun) exited() error {
	if r.err != nil {
		return r.err
	}
	select {
	case err := <-r.done:
		if err == nil {
			err = errors.New("returned nil")
		}
		r.err = fmt.Errorf("Run: %w", err)
		return r.err
	default:
		return nil
	}
}

func (r *straddleInProcessRun) end() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	if r.err == nil {
		<-r.done
	}
	r.cancel = nil
}

func (r *straddleInProcessRun) kill()           { r.end() }
func (r *straddleInProcessRun) logTail() string { return "" }

// straddleBinaryRun runs the built binary and ends it with Process.Kill, so
// no deferred flush, close or final status update runs: a crash.
type straddleBinaryRun struct {
	t       *testing.T
	bin     string
	args    []string
	logPath string
	cmd     *exec.Cmd
	done    chan error
	err     error
}

func (r *straddleBinaryRun) start() {
	r.t.Helper()
	f, err := os.OpenFile(r.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		r.t.Fatal(err)
	}
	cmd := exec.CommandContext(context.Background(), r.bin, r.args...)
	cmd.Env = os.Environ()
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		r.t.Fatalf("start sluice: %v", err)
	}
	r.cmd, r.done, r.err = cmd, make(chan error, 1), nil
	go func() {
		r.done <- cmd.Wait()
		_ = f.Close()
	}()
}

func (r *straddleBinaryRun) exited() error {
	if r.err != nil {
		return r.err
	}
	select {
	case err := <-r.done:
		if err == nil {
			err = errors.New("exit status 0")
		}
		r.err = fmt.Errorf("sluice exited: %w", err)
		return r.err
	default:
		return nil
	}
}

func (r *straddleBinaryRun) end() {
	if r.cmd == nil {
		return
	}
	_ = r.cmd.Process.Kill()
	if r.err == nil {
		<-r.done
	}
	r.cmd = nil
}

func (r *straddleBinaryRun) kill() { r.end() }

func (r *straddleBinaryRun) logTail() string {
	b, err := os.ReadFile(r.logPath)
	if err != nil {
		return ""
	}
	s := string(b)
	if len(s) > 6000 {
		s = s[len(s)-6000:]
	}
	return "\n--- sluice log (tail) ---\n" + s
}

// straddleBuildBinary is [buildSluiceBinary] in a directory whose removal is
// best effort. On Windows a killed process's image can stay locked for a
// moment after Wait returns (an AV scan holds a handle), and t.TempDir's
// cleanup turns that into a test failure that grades nothing.
func straddleBuildBinary(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sluice-straddle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for i := 0; i < 10 && os.RemoveAll(dir) != nil; i++ {
			time.Sleep(500 * time.Millisecond)
		}
	})
	bin := filepath.Join(dir, "sluice")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.CommandContext(context.Background(), "go", "build", "-o", bin, "sluicesync.dev/sluice/cmd/sluice").CombinedOutput(); err != nil {
		t.Fatalf("go build cmd/sluice: %v\n%s", err, out)
	}
	return bin
}

// straddleInsertLSN reads pg_current_wal_insert_lsn() inside tx.
func straddleInsertLSN(t *testing.T, tx *sql.Tx) pglogrepl.LSN {
	t.Helper()
	var s string
	if err := tx.QueryRowContext(context.Background(), "SELECT pg_current_wal_insert_lsn()::text").Scan(&s); err != nil {
		t.Fatalf("pg_current_wal_insert_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return lsn
}

// straddleRestartLSN reads the slot's restart_lsn.
func straddleRestartLSN(t *testing.T, dsn, slot string) pglogrepl.LSN {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRow("SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&s); err != nil {
		t.Fatalf("read restart_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse restart_lsn %q: %v", s, err)
	}
	return lsn
}

// straddleSpillFiles counts the files the slot's decoding has spilled for
// transaction xid: the reorder buffer writes them to
// pg_replslot/<slot>/xid-<xid>-lsn-<hi>-<lo>.spill. The slot's
// pg_stat_replication_slots counters are no evidence about one transaction —
// they count every transaction the walsender spilled, including other
// databases' catalog changes.
func straddleSpillFiles(t *testing.T, dsn, slot string, xid int64) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(
		"SELECT count(*) FROM pg_ls_dir('pg_replslot/' || $1) f WHERE f LIKE $2",
		slot, fmt.Sprintf("xid-%d-lsn-%%.spill", xid),
	).Scan(&n); err != nil {
		t.Fatalf("list pg_replslot/%s: %v", slot, err)
	}
	return n
}

// straddleRows reads table's rows as id → v.
func straddleRows(t *testing.T, dsn, table string) map[int]string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query("SELECT id, v FROM " + table)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int]string{}
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		out[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func straddleEqual(got, want map[int]string) bool {
	if len(got) != len(want) {
		return false
	}
	for id, v := range want {
		if g, ok := got[id]; !ok || g != v {
			return false
		}
	}
	return true
}

// straddleDiff summarizes how got differs from want.
func straddleDiff(got, want map[int]string) string {
	var missing, extra, changed []int
	for id, v := range want {
		g, ok := got[id]
		switch {
		case !ok:
			missing = append(missing, id)
		case g != v:
			changed = append(changed, id)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	clip := func(ids []int) string {
		if len(ids) > 10 {
			return fmt.Sprintf("%v… (%d)", ids[:10], len(ids))
		}
		return fmt.Sprint(ids)
	}
	return fmt.Sprintf("target %d rows, source %d; missing %s, extra %s, changed %s",
		len(got), len(want), clip(missing), clip(extra), clip(changed))
}
