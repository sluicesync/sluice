//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 F5 fifth review: the pre-ALTER replay wedge (GC-44 F24), every
// shape it has and every stream shape it reaches, run to its documented
// recovery on real Postgres.
//
// The fourth review pinned the wedge for a column type WIDENING, and the
// docs claimed the same recoveries for DROP COLUMN and RENAME COLUMN and
// for Shape A. The reviewer measured otherwise: on a single-database
// stream `--schema-changes=forward` does not pass a DROP the drained model
// already applied (the forward path sees the replayed pre-DROP relation as
// an ADD COLUMN and probes a source DEFAULT that no longer exists) nor a
// RENAME (RESUME-SCHEMA-DIVERGENCE); and under --inject-shard-column the
// slot-drop + --restart-from-scratch re-copy is refused while the target
// holds this shard's rows. Each cell here runs the recovery the docs now
// state for that pair and reads the rows back:
//
//	change × stream         recovery
//	DROP   × single         slot drop + --restart-from-scratch (the universal one)
//	RENAME × single         slot drop + --restart-from-scratch
//	DROP / RENAME × multi-schema   slot drop + --restart-from-scratch
//	widen / DROP / RENAME × Shape A   delete this shard's rows, slot drop + --restart-from-scratch
//
// (widen × single and widen × multi-schema are TestStreamer_{Refuse,
// MultiSchema}MidTransactionAlter_PostgresToPostgres.) Measured and NOT a
// recovery: a DROP under one --schema-changes=forward start even after the
// column is added back on the target — the forward intercept still reads
// the replayed pre-DROP relation as an ADD COLUMN and refuses on the
// missing source DEFAULT (loud).
//
// Every first delivery must carry PRE-ALTER-REPLAY-WEDGE (the reader names
// the wedge on the refusal itself), and so must the drained-model restart
// (the reader's refusal, or the unforwarded check's "a column the target
// lacks" pointing at the wedge). The independent expected value is the
// rows the source holds, read back off the target.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// wedgeShape is one DDL inside the wedge transaction. The table starts as
// (id bigint PRIMARY KEY, v text NOT NULL, w varchar(16)) with row 1.
type wedgeShape struct {
	name string
	// alter is the source's DDL (%[1]s: the table); drained is the same
	// change on the target.
	alter, drained string
	// insertAfter writes row %[2]d of table %[1]s in the post-ALTER shape.
	insertAfter string
	// read is the readback expression; want the value for rows 1-4.
	read string
	want func(id int) string
}

func wedgeShapes() map[string]wedgeShape {
	return map[string]wedgeShape{
		"widen": {
			name:        "widen",
			alter:       "ALTER TABLE %[1]s ALTER COLUMN w TYPE varchar(64)",
			drained:     "ALTER TABLE %[1]s ALTER COLUMN w TYPE varchar(64)",
			insertAfter: "INSERT INTO %[1]s VALUES (%[2]d, 'r%[2]d', repeat('x', 40))",
			read:        "v || '/' || w",
			want: func(id int) string {
				if id <= 2 {
					return fmt.Sprintf("r%d/w%d", id, id)
				}
				return fmt.Sprintf("r%d/%s", id, strings.Repeat("x", 40))
			},
		},
		"drop": {
			name:        "drop",
			alter:       "ALTER TABLE %[1]s DROP COLUMN w",
			drained:     "ALTER TABLE %[1]s DROP COLUMN w",
			insertAfter: "INSERT INTO %[1]s VALUES (%[2]d, 'r%[2]d')",
			read:        "v",
			want:        func(id int) string { return fmt.Sprintf("r%d", id) },
		},
		"rename": {
			name:        "rename",
			alter:       "ALTER TABLE %[1]s RENAME COLUMN w TO w2",
			drained:     "ALTER TABLE %[1]s RENAME COLUMN w TO w2",
			insertAfter: "INSERT INTO %[1]s VALUES (%[2]d, 'r%[2]d', 'w%[2]d')",
			read:        "v || '/' || w2",
			want:        func(id int) string { return fmt.Sprintf("r%d/w%d", id, id) },
		},
	}
}

// siblingShardRow is a row another shard's stream wrote into a Shape A
// target; a recovery must leave it alone.
const siblingShardRow = 1001

// runWedgePin drives one wedge transaction — write row 2, the DDL, write
// row 3 — through a Postgres stream that forwards no DDL, then: the first
// delivery refuses naming the wedge; the drained model on the target does
// not pass it (the restart refuses again, naming the wedge); recover runs
// the documented recovery; rows 1-3 read back exact; and a restart in the
// stream's own mode resumes (row 4).
func runWedgePin(t *testing.T, cell twfbCell, ns string, sh wedgeShape, recover func(t *testing.T, cell twfbCell, tbl string) *twfbRun) {
	t.Helper()
	tbl := ns + ".mt"
	cell.src.exec(t, fmt.Sprintf("CREATE TABLE %[1]s (id bigint PRIMARY KEY, v text NOT NULL, w varchar(16)); "+
		"INSERT INTO %[1]s VALUES (1, 'r1', 'w1');", tbl))
	run := startTWFBRun(cell.streamer())
	if !cell.tgt.waitRow(t, tbl, 1, run, 180*time.Second) {
		t.Fatalf("[%s] the cold start never delivered %s (stream: %v)", sh.name, tbl, run.stop(t))
	}
	cell.waitStreaming(t, run)
	if cell.shard.Engaged() {
		cell.tgt.exec(t, fmt.Sprintf("INSERT INTO %s (%s, id, v, w) VALUES ('shard_b', %d, 'sibling', 'sw')",
			tbl, cell.shard.Name, siblingShardRow))
	}
	cell.src.exec(t, fmt.Sprintf("BEGIN; INSERT INTO %[1]s VALUES (2, 'r2', 'w2'); %[2]s; %[3]s; COMMIT;",
		tbl, fmt.Sprintf(sh.alter, tbl), fmt.Sprintf(sh.insertAfter, tbl, 3)))

	assertWedge := func(run *twfbRun, label string, wantReader bool) {
		t.Helper()
		err := waitRefused(t, run, 90*time.Second)
		_ = run.stop(t)
		switch {
		case err == nil || errors.Is(err, context.Canceled):
			t.Errorf("[%s/%s] the stream did not refuse (err %v)", sh.name, label, err)
		case !strings.Contains(err.Error(), ir.PreAlterReplayWedgeMarker):
			t.Errorf("[%s/%s] refused without naming %s: %v", sh.name, label, ir.PreAlterReplayWedgeMarker, err)
		case wantReader && !strings.Contains(err.Error(), readerWedgeRefusal):
			t.Errorf("[%s/%s] refused, but not at the reader: %v", sh.name, label, err)
		default:
			t.Logf("[%s/%s] refused: %v", sh.name, label, err)
		}
		if cell.tgt.hasRow(t, tbl, 3) {
			t.Errorf("[%s/%s] row 3, written after the refused DDL, LANDED", sh.name, label)
		}
	}
	assertWedge(run, "first delivery", true)
	cell.tgt.exec(t, fmt.Sprintf(sh.drained, tbl))
	assertWedge(startTWFBRun(cell.streamer()), "drained-model restart (the wedge)", false)

	run = recover(t, cell, tbl)
	for _, id := range []int{2, 3} {
		if !cell.tgt.waitRow(t, tbl, id, run, 180*time.Second) {
			t.Fatalf("[%s] the recovery never landed row %d (stream: %v)", sh.name, id, run.stop(t))
		}
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the recovery run returned %v", sh.name, err)
	}
	for _, id := range []int{1, 2, 3} {
		if got := cell.tgt.scalar(t, fmt.Sprintf("SELECT %s FROM %s WHERE id = %d", sh.read, tbl, id)); got != sh.want(id) {
			t.Errorf("[%s] row %d read back %q, want %q", sh.name, id, got, sh.want(id))
		}
	}
	if cell.shard.Engaged() {
		if got := cell.tgt.scalar(t, fmt.Sprintf("SELECT v FROM %s WHERE id = %d AND %s = 'shard_b'",
			tbl, siblingShardRow, cell.shard.Name)); got != "sibling" {
			t.Errorf("[%s] the sibling shard's row read back %q after the recovery; want it untouched", sh.name, got)
		}
	}

	run = startTWFBRun(cell.streamer())
	cell.waitStreaming(t, run)
	cell.src.exec(t, fmt.Sprintf(sh.insertAfter, tbl, 4))
	if !cell.tgt.waitRow(t, tbl, 4, run, 90*time.Second) {
		t.Fatalf("[%s] the restart after recovery did not resume: row 4 never landed (stream: %v)", sh.name, run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("[%s] the restart after recovery returned %v", sh.name, err)
	}
	if got := cell.tgt.scalar(t, fmt.Sprintf("SELECT %s FROM %s WHERE id = 4", sh.read, tbl)); got != sh.want(4) {
		t.Errorf("[%s] row 4 read back %q, want %q", sh.name, got, sh.want(4))
	}
}

// dropSlotIfPresent drops the stream's replication slot (the literal name
// `sluice slot list` prints), tolerating its absence.
func dropSlotIfPresent(t *testing.T, cell twfbCell) {
	t.Helper()
	cell.src.exec(t, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'sluice_slot'")
}

// recoverByReCopy is the universal recovery: drop the slot, then
// --restart-from-scratch, which clears the in-scope target tables and
// re-copies them from a fresh snapshot past the wedged transaction.
func recoverByReCopy(t *testing.T, cell twfbCell, _ string) *twfbRun {
	dropSlotIfPresent(t, cell)
	s := cell.streamer()
	s.RestartFromScratch = true
	return startTWFBRun(s)
}

// recoverShapeA is the Shape A recovery: the re-copy refuses while the
// target holds this shard's rows (the shard preflight) — pinned here, with
// its sync wording — so delete THIS shard's rows first; the sibling's stay.
func recoverShapeA(t *testing.T, cell twfbCell, tbl string) *twfbRun {
	dropSlotIfPresent(t, cell)
	s := cell.streamer()
	s.RestartFromScratch = true
	run := startTWFBRun(s)
	err := waitRefused(t, run, 120*time.Second)
	_ = run.stop(t)
	switch {
	case !errors.Is(err, errShardConsolidationRefused):
		t.Errorf("[shape A] the re-copy over this shard's rows was not refused by the shard preflight: %v", err)
	case strings.Contains(err.Error(), "--resume") || !strings.Contains(err.Error(), "DELETE FROM"):
		t.Errorf("[shape A] the shard preflight's sync recovery is not the per-shard delete: %v", err)
	}
	cell.tgt.exec(t, fmt.Sprintf("DELETE FROM %s WHERE %s = '%v'", tbl, cell.shard.Name, cell.shard.Value))
	return recoverByReCopy(t, cell, tbl)
}

func TestStreamer_WedgeRecovery_SingleDatabase_PostgresToPostgres(t *testing.T) {
	shapes := wedgeShapes()
	for _, tc := range []struct {
		name    string
		shape   wedgeShape
		recover func(t *testing.T, cell twfbCell, tbl string) *twfbRun
	}{
		{"drop/re-copy", shapes["drop"], recoverByReCopy},
		{"rename/re-copy", shapes["rename"], recoverByReCopy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			runWedgePin(t, twfbCell{
				src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "wedge-single", schemaChanges: "refuse",
			}, "public", tc.shape, tc.recover)
		})
	}
}

func TestStreamer_WedgeRecovery_MultiSchema_PostgresToPostgres(t *testing.T) {
	shapes := wedgeShapes()
	for _, name := range []string{"drop", "rename"} {
		t.Run(name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			src := twfbDB{"postgres", srcDSN}
			src.exec(t, "CREATE SCHEMA sales; CREATE SCHEMA billing; CREATE TABLE billing.other (id bigint PRIMARY KEY); INSERT INTO billing.other VALUES (1);")
			runWedgePin(t, twfbCell{
				src: src, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "wedge-multischema", databases: []string{"sales", "billing"},
			}, "sales", shapes[name], recoverByReCopy)
		})
	}
}

func TestStreamer_WedgeRecovery_ShapeA_PostgresToPostgres(t *testing.T) {
	shapes := wedgeShapes()
	for _, name := range []string{"widen", "drop", "rename"} {
		t.Run(name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			runWedgePin(t, twfbCell{
				src: twfbDB{"postgres", srcDSN}, tgt: twfbDB{"postgres", tgtDSN},
				streamID: "wedge-shapea", shard: ShardColumnSpec{Name: "source_shard_id", Value: "shard_a"},
			}, "public", shapes[name], recoverShapeA)
		})
	}
}
