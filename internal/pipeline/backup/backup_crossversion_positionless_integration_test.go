//go:build integration && crossversion

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The cross-version gate for FormatVersionPositionlessFull (audit
// 2026-09-15 F-2), a sibling of internal/pipeline's
// TestBackup_CrossVersionChainCompat and run by the same
// extended-suites.yml `crossversion` leg against the same pair of
// binaries (scripts/crossversion-build.sh).
//
// WHY A SECOND FILE. The chain-compat suite reaches the top format tier
// through `backup full` FLAGS on a Postgres source (xvTopFormatFlags), and
// version 11 is reachable by no flag: it is stamped by the SHAPE of the
// source — a full that finalizes without a CDC position on a source whose
// reader resumes from one. A Postgres primary always records an LSN. So
// the tier's own cross-version evidence needs a source that produces the
// shape for real, and the cheapest one is a MySQL server booted with its
// binary log OFF: its position capturer returns the EMPTY position with no
// error (pinned in the mysql engine's TestCaptureBackupPosition_NoBinlog),
// which is exactly the artifact the audit measured a v0.148.0 binary
// extending "from now".
//
// THE FIXTURE IS BEHAVIOURAL, NOT A HASH. A golden built from this tree's
// values would prove this binary agrees with itself (the item-104 shape).
// What the stamp exists to change is what an OLDER binary does with the
// artifact, so the OLD binary is a real release built from its tag, and
// the assertion is what it DOES.
//
//	cell A  positionless refusal   NEW writes a positionless full (MySQL, binlog OFF)
//	                                 → OLD `backup incremental`   REFUSES at the VERSION ceiling
//	                                 → OLD `backup verify`        REFUSES at the VERSION ceiling
//	                                 → NEW `backup incremental`   REFUSES at POSITIONLESS-FULL-ROOT
//	                                 → NEW `backup verify`        PASSES (restorable on its own)
//	cell B  control                NEW writes an ordinary full (MySQL, binlog ON)
//	                                 → root stays at the feature-minimum version
//	                                 → OLD `backup incremental`   PASSES and the chain gains a segment
//
// Cell B is not padding: it is what proves cell A's refusal comes from the
// stamp and from nothing else in the harness (the OLD binary, the MySQL
// image, the DSN). And the "not POSITIONLESS-FULL-ROOT" clause in cell A is
// the mutation-protocol step 5 made permanent: the derived OLD binary is
// whichever release last stamped 10, which since v0.153.1 carries the
// READER door too. A refusal that came from that door would grade the
// wrong mechanism; requiring the version-ceiling text and forbidding the
// reader marker is what makes the cell evidence for the ARTIFACT.

package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

const (
	xpCmdTimeout = 5 * time.Minute
	// xpIncrementalWindow is how long cell B's OLD incremental streams. The
	// delta is committed BEFORE the window opens, so the window's only job
	// is to elapse.
	xpIncrementalWindow = "6s"
	// xpVersionRefusal is the operator-facing substring every pre-11 reader
	// prints at its ceiling (lineage.ReadManifest, unchanged since v0.16).
	xpVersionRefusal = "newer than this build supports"
	// xpReaderDoorMarker is the v0.153.1 READER refusal. Cell A must NOT
	// see it from the OLD binary — see the file comment.
	xpReaderDoorMarker = "POSITIONLESS-FULL-ROOT"
	xpMySQLImage       = "mysql:8.0"
)

type xpBinaries struct {
	oldBin, newBin       string
	oldTag               string
	oldFormat, newFormat int
}

// xpLoadBinaries reads the subset of scripts/crossversion-build.sh's
// contract this suite needs. Every failure path is a t.Fatalf, never a
// t.Skip — a skipped cross-version gate is the vacuous green it exists to
// prevent.
func xpLoadBinaries(t *testing.T) xpBinaries {
	t.Helper()
	get := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("%s is unset — run `bash scripts/crossversion-build.sh` and export the KEY=VALUE lines it prints, "+
				"then re-run with -tags='integration crossversion'. (A FAILURE, not a skip, on purpose.)", k)
		}
		return v
	}
	getInt := func(k string) int {
		n, err := strconv.Atoi(get(k))
		if err != nil {
			t.Fatalf("%s=%q is not an integer: %v", k, os.Getenv(k), err)
		}
		return n
	}
	b := xpBinaries{
		// OLD is this tier's OWN below-tier binary, not the top-tier OLD:
		// since the FormatVersion-12 bump the newest release below the TOP
		// tier stamps 11 and reads a positionless full (see THE FOURTH AXIS
		// in scripts/crossversion-build.sh).
		oldBin:    get("CROSSVER_BELOW_POSITIONLESS_BIN"),
		newBin:    get("CROSSVER_NEW_BIN"),
		oldTag:    get("CROSSVER_BELOW_POSITIONLESS_TAG"),
		oldFormat: getInt("CROSSVER_BELOW_POSITIONLESS_FORMAT"),
		newFormat: getInt("CROSSVER_NEW_FORMAT"),
	}
	// The non-vacuity guards. NEW must be THIS tree (the tier under test is
	// this tree's constant), and OLD must sit below the tier — otherwise
	// cell A asserts a refusal the OLD binary cannot make.
	if b.newFormat != irbackup.BackupFormatVersion {
		t.Fatalf("CROSSVER_NEW_FORMAT=%d but this tree's BackupFormatVersion is %d — NEW is not the working tree",
			b.newFormat, irbackup.BackupFormatVersion)
	}
	if b.oldFormat >= irbackup.FormatVersionPositionlessFull {
		t.Fatalf("OLD %s stamps BackupFormatVersion=%d, at or above FormatVersionPositionlessFull=%d — it would READ the "+
			"positionless full and cell A would be vacuous. Re-run scripts/crossversion-build.sh.",
			b.oldTag, b.oldFormat, irbackup.FormatVersionPositionlessFull)
	}
	for _, p := range []string{b.oldBin, b.newBin} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("binary %q: %v", p, err)
		}
	}
	t.Logf("positionless-full gate: OLD=%s (%s, BackupFormatVersion=%d)  NEW=%s (BackupFormatVersion=%d)",
		b.oldBin, b.oldTag, b.oldFormat, b.newBin, b.newFormat)
	return b
}

// xpLedger fails the parent by name for any cell that never reached its
// assertions — the in-process half of the workflow's PASS-count guard.
type xpLedger struct {
	expected []string
	done     map[string]bool
}

func newXPLedger(t *testing.T, cells ...string) *xpLedger {
	l := &xpLedger{expected: cells, done: map[string]bool{}}
	t.Cleanup(func() {
		for _, c := range l.expected {
			if !l.done[c] {
				t.Errorf("cell %q never reached its assertions — the gate is vacuous for that cell", c)
			}
		}
	})
	return l
}

func (l *xpLedger) report(cell string) { l.done[cell] = true }

func xpExec(t *testing.T, bin string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), xpCmdTimeout)
	defer cancel()
	env := make([]string, 0, len(os.Environ()))
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "SLUICE_") {
			continue
		}
		env = append(env, kv)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func xpMustExec(t *testing.T, what, bin string, args ...string) string {
	t.Helper()
	out, err := xpExec(t, bin, args...)
	if err != nil {
		t.Fatalf("%s: %v\n--- %s %s ---\n%s", what, err, filepath.Base(bin), strings.Join(args, " "), out)
	}
	return out
}

// xpMySQL boots one MySQL 8.0 container with an explicit mysqld command
// and returns a root DSN. Not the shared prebaked image: cell A turns
// binary logging OFF, which is exactly what that image exists to have ON.
func xpMySQL(t *testing.T, ctx context.Context, cmd ...string) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	req := testcontainers.ContainerRequest{
		Image:        xpMySQLImage,
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "rootpw", "MYSQL_DATABASE": "app"},
		Cmd:          append([]string{"mysqld", "--server-id=1"}, cmd...),
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:rootpw@tcp(%s:%s)/app", host, port.Port())
		}).WithStartupTimeout(5 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("boot %s (%v): %v", xpMySQLImage, cmd, err)
	}
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(shutdown)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return fmt.Sprintf("root:rootpw@tcp(%s:%s)/app?parseTime=true", host, port.Port())
}

func xpDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// xpAssertLogBin is the ground-truth step: the server really is in the
// state the cell's premise names, read from the server rather than
// trusted from the boot flag.
func xpAssertLogBin(t *testing.T, ctx context.Context, dsn, want string) {
	t.Helper()
	var got string
	if err := xpDB(t, dsn).QueryRowContext(ctx, "SELECT @@GLOBAL.log_bin").Scan(&got); err != nil {
		t.Fatalf("read @@GLOBAL.log_bin: %v", err)
	}
	if got != want {
		t.Fatalf("@@GLOBAL.log_bin = %q; want %q — the premise this cell measures does not hold", got, want)
	}
}

func xpSeed(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	db := xpDB(t, dsn)
	for _, stmt := range []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL)",
		"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
}

// xpManifest decodes the fields cell assertions key on straight from the
// JSON, independent of any sluice reader — the numbers an OLD binary's
// ceiling check sees.
type xpManifest struct {
	FormatVersion int         `json:"format_version"`
	Kind          string      `json:"kind"`
	EndPosition   ir.Position `json:"end_position"`
}

func xpReadManifest(t *testing.T, dir string) xpManifest {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m xpManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return m
}

// xpStoreFiles lists every file under dir, so a cell can assert a refused
// command left the store byte-for-byte alone (nothing written, nothing
// removed).
func xpStoreFiles(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	files := map[string]int64{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files[path] = info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return files
}

func xpAssertStoreUntouched(t *testing.T, what string, before, after map[string]int64) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("%s: the store has %d files, had %d — a refused command must write nothing", what, len(after), len(before))
	}
	for p, size := range before {
		if got, ok := after[p]; !ok || got != size {
			t.Fatalf("%s: %s changed (size %d → %d, present=%v) — a refused command must leave the store alone", what, p, size, got, ok)
		}
	}
}

func xpBackupFull(t *testing.T, bin, dsn, dir string) {
	t.Helper()
	xpMustExec(t, "backup full", bin, "backup", "full", "--source-driver", "mysql", "--source", dsn, "--output-dir", dir)
}

func xpBackupIncremental(t *testing.T, bin, dsn, dir string) (string, error) {
	t.Helper()
	return xpExec(t, bin, "backup", "incremental", "--source-driver", "mysql", "--source", dsn, "--output-dir", dir,
		"--window", xpIncrementalWindow, "--max-changes", "0")
}

func xpVerify(t *testing.T, bin, dir string) (string, error) {
	t.Helper()
	return xpExec(t, bin, "backup", "verify", "--from-dir", dir)
}

// xpAssertVersionRefusal pins the refusal SHAPE: it failed, it names the
// version gap (so an operator can tell it from corruption), it names the
// version the artifact carries, and it did NOT come from the reader door.
func xpAssertVersionRefusal(t *testing.T, what, out string, err error, fv int) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: the older binary ACCEPTED a manifest stamped %d, above its ceiling — the artifact-side half of "+
			"POSITIONLESS-FULL-ROOT is gone, and a mixed fleet is back to extending positionless fulls from now.\n--- output ---\n%s",
			what, fv, out)
	}
	if !strings.Contains(out, xpVersionRefusal) {
		t.Fatalf("%s: refused (%v) but never said %q — the operator cannot tell a version gap from corruption.\n--- output ---\n%s",
			what, err, xpVersionRefusal, out)
	}
	if want := fmt.Sprintf("format version %d", fv); !strings.Contains(out, want) {
		t.Fatalf("%s: the refusal does not name %q, the version the artifact records.\n--- output ---\n%s", what, want, out)
	}
	if strings.Contains(out, xpReaderDoorMarker) {
		t.Fatalf("%s: the refusal carries %s — that is the READER door, so this cell graded the wrong mechanism and "+
			"says nothing about the artifact stamp.\n--- output ---\n%s", what, xpReaderDoorMarker, out)
	}
}

func TestBackup_CrossVersionPositionlessFull(t *testing.T) {
	bins := xpLoadBinaries(t)
	ledger := newXPLedger(t, "A-positionless-refusal", "B-control")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	t.Run("cellA_positionless_full_refused_by_OLD_at_the_version_ceiling", func(t *testing.T) {
		dsn := xpMySQL(t, ctx, "--skip-log-bin")
		xpAssertLogBin(t, ctx, dsn, "0")
		xpSeed(t, ctx, dsn)
		dir := t.TempDir()

		xpBackupFull(t, bins.newBin, dsn, dir)

		// The artifact under test, read independently of any sluice code:
		// a positionless full at the tier this tree stamps for it.
		m := xpReadManifest(t, dir)
		if m.Kind != irbackup.BackupKindFull || m.EndPosition != (ir.Position{}) {
			t.Fatalf("NEW wrote kind=%q end_position=%+v on a binlog-OFF source; the cell needs a positionless FULL", m.Kind, m.EndPosition)
		}
		if m.FormatVersion != irbackup.FormatVersionPositionlessFull {
			t.Fatalf("NEW stamped the positionless full at format_version %d, want %d — the stamp did not reach the store",
				m.FormatVersion, irbackup.FormatVersionPositionlessFull)
		}
		if m.FormatVersion <= bins.oldFormat {
			t.Fatalf("root stamped %d, which OLD (%d) reads — cell A cannot assert a refusal", m.FormatVersion, bins.oldFormat)
		}
		before := xpStoreFiles(t, dir)

		// OLD: both reader doors that a chain extension and a read pass
		// through refuse at the ceiling, and the store is untouched.
		out, err := xpBackupIncremental(t, bins.oldBin, dsn, dir)
		xpAssertVersionRefusal(t, "cell A (OLD backup incremental)", out, err, m.FormatVersion)
		xpAssertStoreUntouched(t, "cell A (OLD backup incremental)", before, xpStoreFiles(t, dir))
		out, err = xpVerify(t, bins.oldBin, dir)
		xpAssertVersionRefusal(t, "cell A (OLD backup verify)", out, err, m.FormatVersion)

		// NEW: the reader door still stands on the current binary — the
		// stamp is the artifact's copy of it, not a replacement — and the
		// full is a complete backup on its own.
		out, err = xpBackupIncremental(t, bins.newBin, dsn, dir)
		if err == nil || !strings.Contains(out, xpReaderDoorMarker) {
			t.Fatalf("cell A (NEW backup incremental): want the %s refusal; err=%v\n--- output ---\n%s", xpReaderDoorMarker, err, out)
		}
		xpAssertStoreUntouched(t, "cell A (NEW backup incremental)", before, xpStoreFiles(t, dir))
		if out, err := xpVerify(t, bins.newBin, dir); err != nil {
			t.Fatalf("cell A (NEW backup verify): a positionless full must verify on the binary that wrote it: %v\n--- output ---\n%s", err, out)
		}
		t.Logf("cell A: NEW stamped the positionless full %d; OLD (%s, reads up to %d) refused incremental and verify at its ceiling",
			m.FormatVersion, bins.oldTag, bins.oldFormat)
		ledger.report("A-positionless-refusal")
	})

	t.Run("cellB_control_ordinary_full_extended_by_OLD", func(t *testing.T) {
		dsn := xpMySQL(t, ctx, "--log-bin=mysql-bin", "--binlog-format=ROW")
		xpAssertLogBin(t, ctx, dsn, "1")
		xpSeed(t, ctx, dsn)
		dir := t.TempDir()

		xpBackupFull(t, bins.newBin, dsn, dir)
		m := xpReadManifest(t, dir)
		if m.EndPosition == (ir.Position{}) {
			t.Fatalf("NEW wrote an EMPTY end_position on a binlog-ON source — the control is not a control")
		}
		if m.FormatVersion > bins.oldFormat {
			t.Fatalf("NEW stamped an ORDINARY full at format_version %d, above OLD's ceiling %d — the bump is not "+
				"proportional: a full that recorded a position must keep its feature-minimum version", m.FormatVersion, bins.oldFormat)
		}

		// A committed delta for the window to carry, then the OLD binary
		// extends the chain — the ordinary thing the stamp must not break.
		if _, err := xpDB(t, dsn).ExecContext(ctx, "INSERT INTO users VALUES (3, 'carol')"); err != nil {
			t.Fatalf("delta: %v", err)
		}
		if out, err := xpBackupIncremental(t, bins.oldBin, dsn, dir); err != nil {
			t.Fatalf("cell B (OLD backup incremental) on an ordinary NEW-written full: %v\n--- output ---\n%s", err, out)
		}
		segs, err := filepath.Glob(filepath.Join(dir, "manifests", "incr-*.json"))
		if err != nil {
			t.Fatalf("glob segments: %v", err)
		}
		if len(segs) != 1 {
			t.Fatalf("cell B: chain carries %d incremental segment(s), want 1 — OLD reported success without extending the chain", len(segs))
		}
		t.Logf("cell B: NEW wrote an ordinary full at %d (OLD reads up to %d); OLD extended it with one segment", m.FormatVersion, bins.oldFormat)
		ledger.report("B-control")
	})
}
