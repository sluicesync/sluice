//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Dropping a MIDDLE column renumbers every later column's ordinal and moves
// nothing relative to anything else. The RENAME-SWAP-FORWARD checks compare
// relative order (and Postgres stable ids), never ordinals — and the F3
// rename-chain check positioned the inferred rename pair, so a DROP COLUMN
// gone + ADD COLUMN fresh read as "renamed more than one column", a
// column-order refusal with a re-copy remedy (CI on fa15f092,
// TestStreamer_SchemaForward_DropAddSameType_PG_Refuses).
//
// Both cells run live, CDC→CDC (an ADD COLUMN boundary forwarded first,
// as a destructive DROP is forwarded only there), on MySQL 8.4 and
// Postgres 16, graded against the target itself:
//
//   - drop a middle column alone: forwarded — the post-drop row lands,
//     the target no longer has the column, no refusal;
//   - drop a middle column and add one of the same type: the ambiguous-
//     rename refusal with its own reading — on Postgres the attnums prove
//     a drop+add; on MySQL the added column lands at the end, so it is a
//     drop+add or a rename chain ending there — never the rename-chain
//     refusal, and the target untouched.
func runDropMiddleCells(t *testing.T, srv rsfServer) {
	t.Helper()
	for i, addBack := range []bool{false, true} {
		name := "drop-middle"
		if addBack {
			name = "drop-middle-add-same-type"
		}
		t.Run(name, func(t *testing.T) {
			tag := fmt.Sprintf("dm%d", i)
			src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
			cell := twfbCell{src: src, tgt: tgt, streamID: "rsf-" + tag, slotName: "rsf_" + tag}
			src.exec(t, "CREATE TABLE kd (id INT PRIMARY KEY, b INT, gone INT, c INT); INSERT INTO kd VALUES (1,10,20,30),(2,11,21,31);")
			run := startTWFBRun(cell.streamer())
			defer func() { _ = run.stop(t) }()
			if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
				t.Fatalf("cold start: %v", run.stop(t))
			}
			cell.waitStreaming(t, run)
			// A forwarded ADD makes the next boundary CDC→CDC.
			src.exec(t, "ALTER TABLE kd ADD COLUMN p INT")
			src.exec(t, "INSERT INTO kd (id,b,gone,c,p) VALUES (50,15,25,35,45)")
			if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
				t.Fatalf("the priming ADD COLUMN was not forwarded: %v", run.stop(t))
			}
			before := rsfRead(t, tgt)
			if addBack {
				src.exec(t, "ALTER TABLE kd DROP COLUMN gone, ADD COLUMN fresh INT")
				src.exec(t, "INSERT INTO kd (id,b,c,p,fresh) VALUES (100,1000,3000,4000,5000)")
			} else {
				src.exec(t, "ALTER TABLE kd DROP COLUMN gone")
				src.exec(t, "INSERT INTO kd (id,b,c,p) VALUES (100,1000,3000,4000)")
			}

			if !addBack {
				if !tgt.waitRow(t, "kd", 100, run, 60*time.Second) {
					t.Fatalf("the drop was not forwarded: the post-drop row never landed (stream: %v)", run.stop(t))
				}
				if got := strings.Join(rsfRead(t, tgt).columns, ","); got != "id,b,c,p" {
					t.Errorf("target declares %s after the drop; want id,b,c,p", got)
				}
				if got := tgt.scalar(t, "SELECT c FROM kd WHERE id = 100"); got != "3000" {
					t.Errorf("row 100's c = %s, want 3000", got)
				}
				return
			}
			deadline := time.Now().Add(60 * time.Second)
			for !run.exited() && time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
			}
			if !run.exited() {
				t.Fatalf("the drop+add was forwarded; want the ambiguous-rename refusal (target %v)", rsfRead(t, tgt).columns)
			}
			msg := fmt.Sprint(run.err)
			t.Logf("refused: %s", msg)
			want := []string{`RENAME COLUMN "gone"→"fresh"`, "cannot be auto-forwarded", `DROP COLUMN "gone"`, `ADD COLUMN "fresh"`}
			if srv.dialect == "postgres" {
				want = append(want, "different stable ids")
			} else {
				want = append(want, "at the end of the table", "--reset-target-data")
			}
			for _, w := range want {
				if !strings.Contains(msg, w) {
					t.Errorf("the refusal lacks %q", w)
				}
			}
			if strings.Contains(msg, "renamed more than one column") || errors.Is(run.err, ir.ErrColumnOrderChanged) {
				t.Errorf("a drop+add was refused as a rename chain / column-order change")
			}
			rsfAssertUntouched(t, tgt, before, srv.label+"/drop-middle-add-same-type")
		})
	}
}

func TestSchemaForward_DropMiddleColumn_MySQL84(t *testing.T) {
	t.Parallel()
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	runDropMiddleCells(t, rsfMySQLFamily(t, "mysql-8.4", "mysql", dsn, buildMySQLDSN))
}

func TestSchemaForward_DropMiddleColumn_Postgres16(t *testing.T) {
	t.Parallel()
	srcDSN, _, cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 40)
	defer cleanup()
	root := twfbDB{engine: "postgres", dsn: srcDSN}
	runDropMiddleCells(t, rsfServer{
		label: "postgres-16", engine: "postgres", dialect: "postgres",
		newDB: func(t *testing.T, name string) twfbDB {
			t.Helper()
			root.exec(t, "CREATE DATABASE "+name)
			dsn, err := buildPGDSN(srcDSN, name)
			if err != nil {
				t.Fatalf("dsn for %s: %v", name, err)
			}
			return twfbDB{engine: "postgres", dsn: dsn}
		},
	})
}
