//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F5 review pins, on real servers.
//
//   - The override class: a --type-override column under refuse mode. The
//     witness lens does not compare an overridden column's type, so a live
//     source widen past the override was rounded at exit 0; it now refuses
//     (schema_change_refuse.go, overriddenColumnRefused), refuses again on a
//     restart (the history still holds the shape before it), and resumes
//     exact once the target holds the widened type.
//   - The pre-ALTER replay: a Postgres transaction that writes, alters a
//     column and writes again is replayed from its start on every restart,
//     pre-ALTER relation first. Under refuse mode and on a multi-schema
//     stream the reader's own gate refused that forever, whatever the
//     operator did to the target; the drained-model restart must resume.
//
// Verdicts are read off the target: the declared type and the rows read
// back.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
)

// refuseOverrideCell is one source dialect of the override pin.
type refuseOverrideCell struct {
	create, widen, insert string
	// readAmount reads row id's amount off the PG target as text.
	readAmount func(id int) string
}

func runRefuseOverridePin(t *testing.T, cell twfbCell, d refuseOverrideCell) {
	t.Helper()
	cell.schemaChanges = "refuse"
	cell.mappings = []config.Mapping{{
		Table: "t_ovr", Column: "amount", TargetType: "numeric",
		TargetTypeOptions: map[string]any{"precision": 12, "scale": 2},
	}}
	cell.src.exec(t, d.create)
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_ovr", 1, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered t_ovr (stream: %v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	if got := cell.tgt.columnType(t, "t_ovr", "amount"); got != "numeric(12,2)" {
		t.Fatalf("the override did not land: target amount is %q", got)
	}
	// A row before the change: the table's first boundary is accepted (the
	// override is wider than the source) and becomes the prior.
	cell.src.exec(t, fmt.Sprintf(d.insert, 2, "1.25"))
	if !cell.tgt.waitRow(t, "t_ovr", 2, run, 90*time.Second) {
		t.Fatalf("[before] row 2 never landed (stream: %v)", run.stop(t))
	}

	// The live widen past the override.
	cell.src.exec(t, d.widen)
	cell.src.exec(t, fmt.Sprintf(d.insert, 3, "1.2345"))
	for _, kind := range []string{"live", "restart"} {
		err := waitRefused(t, run, 60*time.Second)
		if err == nil || !strings.Contains(err.Error(), schemaChangeRefusedMarker) || !strings.Contains(err.Error(), "--type-override") {
			t.Errorf("[%s] the widen past the override was not refused with %s naming the override (err %v)", kind, schemaChangeRefusedMarker, run.stop(t))
		} else {
			t.Logf("[%s] refused: %v", kind, err)
		}
		_ = run.stop(t)
		if got := d.readAmount(3); !strings.HasPrefix(got, "<") {
			t.Errorf("[%s] row 3 LANDED past the refusal as %q (rounded into the override)", kind, got)
		}
		if kind == "live" {
			run = startTWFBRun(cell.streamer())
		}
	}

	// The drained-model recovery: the target takes the widened type.
	cell.tgt.exec(t, "ALTER TABLE t_ovr ALTER COLUMN amount TYPE numeric(14,4)")
	run = startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, "t_ovr", 3, run, 90*time.Second) {
		t.Fatalf("[recovered] row 3 never landed (stream: %v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[recovered] the stream returned %v", err)
	}
	if got := d.readAmount(3); got != "1.2345" {
		t.Errorf("[recovered] row 3 read back %q, want 1.2345", got)
	}
}

func TestStreamer_RefuseOverride_MySQLToPostgres(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	tgt := twfbDB{"postgres", tgtDSN}
	runRefuseOverridePin(t, twfbCell{src: twfbDB{"mysql", srcDSN}, tgt: tgt, streamID: "refuse-ovr-mysql"}, refuseOverrideCell{
		create: "CREATE TABLE t_ovr (id BIGINT NOT NULL PRIMARY KEY, amount DECIMAL(10,2) NOT NULL) ENGINE=InnoDB; INSERT INTO t_ovr VALUES (1, 1.00);",
		widen:  "ALTER TABLE t_ovr MODIFY amount DECIMAL(14,4) NOT NULL",
		insert: "INSERT INTO t_ovr VALUES (%d, %s)",
		readAmount: func(id int) string {
			return tgt.scalar(t, fmt.Sprintf("SELECT amount::text FROM t_ovr WHERE id = %d", id))
		},
	})
}

func TestStreamer_RefuseOverride_PostgresToPostgres(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	tgt := twfbDB{"postgres", tgtDSN}
	runRefuseOverridePin(t, twfbCell{src: twfbDB{"postgres", srcDSN}, tgt: tgt, streamID: "refuse-ovr-pg"}, refuseOverrideCell{
		create: "CREATE TABLE t_ovr (id bigint PRIMARY KEY, amount numeric(10,2) NOT NULL); INSERT INTO t_ovr VALUES (1, 1.00);",
		widen:  "ALTER TABLE t_ovr ALTER COLUMN amount TYPE numeric(14,4)",
		insert: "INSERT INTO t_ovr VALUES (%d, %s)",
		readAmount: func(id int) string {
			return tgt.scalar(t, fmt.Sprintf("SELECT amount::text FROM t_ovr WHERE id = %d", id))
		},
	})
}

// midTransactionAlterCell is one shape of the pre-ALTER replay pin.
type midTransactionAlterCell struct {
	name string
	// from / to are the column's types before and after the ALTER, in
	// Postgres spelling; to is also what the drained model gives the target.
	from, to string
	// long is the value the post-ALTER row carries; only the new type holds it.
	long string
}

// runMidTransactionAlterPin drives one transaction — write, ALTER, write —
// through a Postgres stream that forwards no DDL, then the drained model.
// ns is the source schema ("public" single-database, else one namespace of
// a multi-schema stream).
func runMidTransactionAlterPin(t *testing.T, cell twfbCell, ns string, c midTransactionAlterCell) {
	t.Helper()
	tbl := ns + ".mt"
	cell.src.exec(t, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v %s NOT NULL); INSERT INTO %s VALUES (1, 'a');", tbl, c.from, tbl))
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, tbl, 1, run, 180*time.Second) {
		t.Fatalf("[%s] the cold start never delivered %s (stream: %v)", c.name, tbl, run.stop(t))
	}
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf(`BEGIN; INSERT INTO %s VALUES (2, 'b'); ALTER TABLE %s ALTER COLUMN v TYPE %s;
		INSERT INTO %s VALUES (3, '%s'); COMMIT;`, tbl, tbl, c.to, tbl, c.long))
	if err := waitRefused(t, run, 60*time.Second); err == nil || errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the ALTER the target cannot hold was not refused (err %v)", c.name, run.stop(t))
	} else {
		t.Logf("[%s] refused: %v", c.name, err)
	}
	_ = run.stop(t)
	if cell.tgt.hasRow(t, tbl, 3) {
		t.Errorf("[%s] row 3 LANDED past the refusal", c.name)
	}

	// The drained model: the target takes the change, the stream restarts,
	// and the replay — pre-ALTER relation first — must go through.
	cell.tgt.exec(t, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN v TYPE %s", tbl, c.to))
	logs := twfbCaptureLogs(t)
	run = startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, tbl, 3, run, 90*time.Second) {
		t.Fatalf("[%s] the drained-model restart did not resume: row 3 never landed (stream: %v)\n%s",
			c.name, run.stop(t), divergenceLines(logs.String()))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the resumed stream returned %v", c.name, err)
	}
	if got := cell.tgt.scalar(t, fmt.Sprintf("SELECT v FROM %s WHERE id = 3", tbl)); got != c.long {
		t.Errorf("[%s] row 3 read back %q, want %q", c.name, got, c.long)
	}
	if !cell.tgt.hasRow(t, tbl, 2) {
		t.Errorf("[%s] row 2 (before the ALTER, same transaction) is missing", c.name)
	}

	// GC-44 F5 third review: one more restart after the recovery. The
	// retained history now holds the POST-ALTER shape; if the source
	// re-delivers the transaction, its pre-ALTER relation lies before that
	// version's anchor and must not read as a source narrowing.
	run = startTWFBRun(cell.streamer())
	cell.src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (4, 'd')", tbl))
	if !cell.tgt.waitRow(t, tbl, 4, run, 90*time.Second) {
		t.Fatalf("[%s] the restart after recovery did not resume: row 4 never landed (stream: %v)", c.name, run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the restart after recovery returned %v", c.name, err)
	}
}

// midTransactionAlterCells: a widen within one family, which the direction
// rule already holds, and VARCHAR → TEXT, which needs witnessWidthOrder's
// cross-family "the target is wider" relation.
func midTransactionAlterCells() []midTransactionAlterCell {
	long := strings.Repeat("x", 40)
	return []midTransactionAlterCell{
		{name: "varchar widen", from: "varchar(16)", to: "varchar(64)", long: long},
		{name: "varchar to text", from: "varchar(16)", to: "text", long: long},
	}
}

func TestStreamer_RefuseMidTransactionAlter_PostgresToPostgres(t *testing.T) {
	for _, c := range midTransactionAlterCells() {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			runMidTransactionAlterPin(t, twfbCell{
				src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "refuse-midtx", schemaChanges: "refuse",
			}, "public", c)
		})
	}
}

func TestStreamer_MultiSchemaMidTransactionAlter_PostgresToPostgres(t *testing.T) {
	for _, c := range midTransactionAlterCells() {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			src := twfbDB{"postgres", srcDSN}
			src.exec(t, "CREATE SCHEMA sales; CREATE SCHEMA billing; CREATE TABLE billing.other (id bigint PRIMARY KEY); INSERT INTO billing.other VALUES (1);")
			runMidTransactionAlterPin(t, twfbCell{
				src: src, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "multischema-midtx", databases: []string{"sales", "billing"},
			}, "sales", c)
		})
	}
}
