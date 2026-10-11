//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// RENAME-SWAP-INVISIBLE — the pin (engines/mysql/cdc_column_identity.go).
//
// A MySQL column has no stable id, so a change that leaves a column name
// the table already had holding ANOTHER column is invisible to every
// by-name comparison whenever the names, order and types come out the same
// (or look like a different, harmless change). Measured before the fix on
// mysql:8.4, at exit 0 with the target's existing rows wrong, on every
// shape below, live at the head and on a warm resume under both
// binlog_row_metadata settings, and in a backup chain's restore; behind
// the head only MINIMAL's look-ahead refused (Bug 301), never FULL.
//
// The matrix is every shape × every way a stream meets it × both metadata
// settings. The grade is the target against the source, by column name —
// never the stream's own logs: each cell must either converge exactly or
// refuse with the target untouched. Where nothing but the column-identity
// ledger can refuse (at the head, and a warm resume with no row before the
// change) the refusal must be COLUMN-IDENTITY-CHANGED, so a mutant that
// disables the ledger fails here and not on some other door.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

type rsiShape struct {
	name   string
	change string
	insert string
}

// rsiCreate is every cell's source: kd, and pad, a table whose bulk insert
// puts a live stream behind the head.
const rsiCreate = "CREATE TABLE kd (id INT PRIMARY KEY, b INT, c INT); INSERT INTO kd VALUES (1,10,20),(2,11,21),(3,12,22); " +
	"CREATE TABLE pad (id INT PRIMARY KEY, v INT);"

var rsiShapes = []rsiShape{
	{"a-swap-then-modify-after", "ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b; ALTER TABLE kd MODIFY COLUMN b INT AFTER id", "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)"},
	{"a-change-swap-one-alter", "ALTER TABLE kd CHANGE COLUMN b c INT, CHANGE COLUMN c b INT AFTER id", "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)"},
	{"b-drop-add-same-position", "ALTER TABLE kd DROP COLUMN c, ADD COLUMN c INT", "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)"},
	{"b-drop-then-add-two-alters", "ALTER TABLE kd DROP COLUMN c; ALTER TABLE kd ADD COLUMN c INT", "INSERT INTO kd (id,b,c) VALUES (100,1000,2000)"},
	{"c-rename-to-tmp-add", "ALTER TABLE kd RENAME COLUMN c TO tmp, ADD COLUMN c INT", "INSERT INTO kd (id,b,tmp,c) VALUES (100,1000,1500,2000)"},
	{"d-drop-rename-onto", "ALTER TABLE kd DROP COLUMN b, RENAME COLUMN c TO b", "INSERT INTO kd (id,b) VALUES (100,1000)"},
	{"d-drop-then-rename-two-alters", "ALTER TABLE kd DROP COLUMN b; ALTER TABLE kd RENAME COLUMN c TO b", "INSERT INTO kd (id,b) VALUES (100,1000)"},
}

type rsiEntry string

const (
	rsiLiveHead   rsiEntry = "live-head"   // running, primed, at the head
	rsiLiveBehind rsiEntry = "live-behind" // running, a pre-change row decoded after the change committed
	rsiWarm       rsiEntry = "warm"        // stopped across the change
	rsiWarmPre    rsiEntry = "warm-pre"    // stopped across a pre-change row and the change
	rsiBackup     rsiEntry = "backup"      // full, change in an incremental's window, restore
	rsiBackupPre  rsiEntry = "backup-pre"  // the same with a pre-change row in the window
)

var rsiEntries = []rsiEntry{rsiLiveHead, rsiLiveBehind, rsiWarm, rsiWarmPre, rsiBackup, rsiBackupPre}

// rsiLedgerOnly: entries where no door but the column-identity ledger sees
// the change, so the refusal must be its own.
func rsiLedgerOnly(e rsiEntry) bool { return e == rsiLiveHead || e == rsiWarm }

// rsiReadOn renders db's kd rows over cols exactly as rsfRead does, or an
// error when the target lacks one of them.
func rsiReadOn(db twfbDB, cols []string) (string, error) {
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	byName := append([]string(nil), cols...)
	sort.Strings(byName)
	data, err := conn.Query("SELECT " + strings.Join(byName, ", ") + " FROM kd ORDER BY id")
	if err != nil {
		return "", err
	}
	defer func() { _ = data.Close() }()
	var sb strings.Builder
	for data.Next() {
		vals := make([]sql.NullString, len(byName))
		ptrs := make([]any, len(byName))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := data.Scan(ptrs...); err != nil {
			return "", err
		}
		for i, v := range vals {
			fmt.Fprintf(&sb, "%s=%s;", byName[i], v.String)
		}
		sb.WriteString("\n")
	}
	return sb.String(), data.Err()
}

// rsiVerdict is one cell's outcome.
type rsiVerdict struct {
	kind   string // EXACT, WRONG, REFUSED, REFUSED-DIRTY, STALLED
	err    error
	detail string
}

// rsiWithoutRow60 drops the pre-change row from rendered rows.
func rsiWithoutRow60(rows string) string {
	var keep []string
	for _, line := range strings.Split(rows, "\n") {
		if !strings.HasSuffix(line, "id=60;") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, "\n")
}

// rsiGrade grades the target against the source on the source's columns.
func rsiGrade(t *testing.T, src, tgt twfbDB) rsiVerdict {
	t.Helper()
	s := rsfRead(t, src)
	g, err := rsiReadOn(tgt, s.columns)
	if err != nil {
		return rsiVerdict{kind: "WRONG", detail: fmt.Sprintf("the target cannot be read on the source's columns %v: %v", s.columns, err)}
	}
	if g != s.rows {
		return rsiVerdict{kind: "WRONG", detail: fmt.Sprintf("source %v:\n%s\ntarget:\n%s", s.columns, s.rows, g)}
	}
	return rsiVerdict{kind: "EXACT"}
}

// rsiOutcome waits for the run to deliver row 100 or stop, then grades.
func rsiOutcome(t *testing.T, run *twfbRun, src, tgt twfbDB, before rsfTable) rsiVerdict {
	t.Helper()
	if tgt.waitRow(t, "kd", 100, run, 90*time.Second) {
		time.Sleep(2 * time.Second)
	}
	if run.exited() && run.err != nil && !errors.Is(run.err, context.Canceled) {
		// Row 60 was written under the old shape, before the change, and may
		// rightly land before the refusal; nothing else may have moved.
		after := rsfRead(t, tgt)
		if strings.Join(after.columns, ",") == strings.Join(before.columns, ",") &&
			rsiWithoutRow60(after.rows) == rsiWithoutRow60(before.rows) && !tgt.hasRow(t, "kd", 100) {
			return rsiVerdict{kind: "REFUSED", err: run.err}
		}
		return rsiVerdict{kind: "REFUSED-DIRTY", err: run.err, detail: fmt.Sprintf("before %v:\n%s\nafter %v:\n%s", before.columns, before.rows, after.columns, after.rows)}
	}
	if !tgt.hasRow(t, "kd", 100) {
		return rsiVerdict{kind: "STALLED", err: run.stop(t), detail: fmt.Sprintf("target pad rows: %s, kd rows: %s",
			tgt.scalar(t, "SELECT count(*) FROM pad"), tgt.scalar(t, "SELECT count(*) FROM kd"))}
	}
	v := rsiGrade(t, src, tgt)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		v.detail += fmt.Sprintf("\n(stream stopped with %v)", err)
	}
	return v
}

func rsiRunStreamCell(t *testing.T, srv rsfServer, shape rsiShape, entry rsiEntry, tag string) rsiVerdict {
	t.Helper()
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "rsi-" + tag}
	src.exec(t, rsiCreate)
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("cold start: %v", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO kd (id) VALUES (50)")
	if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
		t.Fatalf("prime: %v", run.stop(t))
	}
	before := rsfRead(t, tgt)
	switch entry {
	case rsiLiveHead:
		src.exec(t, shape.change)
		src.exec(t, shape.insert)
	case rsiLiveBehind:
		// 1,000 pad rows committed first (measured: this harness's applier
		// takes them over tens of seconds): the reader is still decoding
		// them when the rest of the script has committed (milliseconds
		// later), so it reaches the neutral ALTER (which clears its cache)
		// and row 60 with the change already in the catalog — a live
		// stream behind the head.
		src.exec(t, "SET SESSION cte_max_recursion_depth = 5000; "+
			"INSERT INTO pad (id, v) WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 1000) SELECT i, i FROM n; "+
			"ALTER TABLE kd COMMENT = 'rsi'; INSERT INTO kd (id,b,c) VALUES (60,600,601); "+shape.change+"; "+shape.insert)
		if tgt.hasRow(t, "pad", 1000) {
			t.Fatalf("the stream kept up with the pad batch: the cell is not behind the head")
		}
		_ = tgt.waitRow(t, "pad", 1000, run, 240*time.Second)
	case rsiWarm, rsiWarmPre:
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stop: %v", err)
		}
		if entry == rsiWarmPre {
			src.exec(t, "INSERT INTO kd (id,b,c) VALUES (60,600,601)")
		}
		src.exec(t, shape.change)
		src.exec(t, shape.insert)
		run = startTWFBRun(cell.streamer())
	}
	return rsiOutcome(t, run, src, tgt, before)
}

func rsiRunBackupCell(t *testing.T, srv rsfServer, shape rsiShape, entry rsiEntry, tag string) rsiVerdict {
	t.Helper()
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	src.exec(t, rsiCreate)
	eng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file, pos := readMySQLBinlogPos(t, src.dsn)
	ctx := context.Background()
	if err := (&backup.Backup{Source: eng, SourceDSN: src.dsn, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{Engine: "mysql", Token: fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, file, pos)}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatal(err)
	}
	if entry == rsiBackupPre {
		src.exec(t, "INSERT INTO kd (id,b,c) VALUES (60,600,601)")
	}
	src.exec(t, shape.change)
	src.exec(t, shape.insert)
	incrCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	err = (&IncrementalBackup{
		Source: eng, SourceDSN: src.dsn, Store: store, ParentRef: full.BackupID,
		Window: 8 * time.Second, ChunkChanges: 3, SluiceVersion: "test",
	}).Run(incrCtx)
	cancel()
	if err != nil {
		return rsiVerdict{kind: "REFUSED", err: fmt.Errorf("incremental: %w", err)}
	}
	if err := (&backup.Restore{Target: eng, TargetDSN: tgt.dsn, Store: store}).Run(ctx); err != nil {
		if tgt.hasRow(t, "kd", 100) || tgt.hasRow(t, "kd", 60) {
			return rsiVerdict{kind: "REFUSED-DIRTY", err: err, detail: "a window row reached the target before the refusal"}
		}
		return rsiVerdict{kind: "REFUSED", err: fmt.Errorf("restore: %w", err)}
	}
	return rsiGrade(t, src, tgt)
}

func rsiRunServer(t *testing.T, srv rsfServer) {
	idx := 0
	for _, shape := range rsiShapes {
		for _, entry := range rsiEntries {
			idx++
			tag := fmt.Sprintf("i%d", idx)
			t.Run(shape.name+"/"+string(entry), func(t *testing.T) {
				t.Parallel()
				var v rsiVerdict
				if entry == rsiBackup || entry == rsiBackupPre {
					v = rsiRunBackupCell(t, srv, shape, entry, tag)
				} else {
					v = rsiRunStreamCell(t, srv, shape, entry, tag)
				}
				t.Logf("RSI-MATRIX %s | %s | %s | %s | %v", srv.label, shape.name, entry, v.kind, v.err)
				switch {
				case v.kind == "EXACT":
				case v.kind == "REFUSED" && rsiLedgerOnly(entry) && !errors.Is(v.err, ir.ErrColumnIdentityChanged):
					t.Errorf("refused, but not COLUMN-IDENTITY-CHANGED — only the column-identity ledger can see this cell: %v", v.err)
				case v.kind == "REFUSED":
				default:
					t.Errorf("%s: the target is not the source by column name and the stream did not refuse cleanly (err=%v)\n%s", v.kind, v.err, v.detail)
				}
			})
		}
	}
}

func TestRenameSwapInvisible_MySQL84(t *testing.T) {
	t.Parallel()
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	t.Run("minimal", func(t *testing.T) {
		rsiRunServer(t, rsfMySQLFamily(t, "mysql-8.4/minimal", "mysql", dsn, buildMySQLDSN))
	})
}

func TestRenameSwapInvisible_MySQL84_FullMetadata(t *testing.T) {
	t.Parallel()
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	rsfFullMetadata(t, dsn)
	t.Run("full", func(t *testing.T) {
		rsiRunServer(t, rsfFull(rsfMySQLFamily(t, "mysql-8.4/full", "mysql", dsn, buildMySQLDSN)))
	})
}
