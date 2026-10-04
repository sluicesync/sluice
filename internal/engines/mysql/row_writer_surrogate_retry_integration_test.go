//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses is the F-E1 in-run
// retry pin against a real MySQL. A statement really commits and its
// acknowledgement is then "lost" (a classified transient injected AFTER the
// exec) — the committed-but-unacked branch flushWithReparentRetry's re-send
// gate exists for.
//
// The recorded table is keyed on `id`. The targets:
//
//   - "sur": keyed only on an AUTO_INCREMENT `sid` the rows never carry.
//     Before the fix the gate judged the recorded table alone, re-sent, the
//     server drew fresh `sid` values, and the copy returned nil with the
//     segment doubled. It must now refuse, naming the TARGET judgment.
//   - "two": the same surrogate PLUS a NOT NULL UNIQUE (id) the rows supply.
//     LOAD DATA collides on any unique key, so the replay converges (the
//     duplicates are skipped and the replay's warning accounting proves
//     them harmless) — the gate must license it.
//   - "kpk": keyed on `id` on both sides — the plain control.
//
// Reached cores: LOAD DATA end to end through WriteRows (its own after-exec
// failpoint), and the batched-INSERT core's flush driven directly through
// flushWithReparentRetry with the real INSERT statement, since that core has
// no failpoint of its own. The idempotent core shares the same gate and is
// pinned on the scripted driver (row_writer_surrogate_retry_test.go).
//
// The independent expected value is the target's own COUNT(*).
func TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	enableLocalInfile(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE sur (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, id BIGINT NOT NULL, v VARCHAR(16) NULL) ENGINE=InnoDB;
		CREATE TABLE two (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, id BIGINT NOT NULL, v VARCHAR(16) NULL,
			UNIQUE KEY two_id (id)) ENGINE=InnoDB;
		CREATE TABLE kpk (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(16) NULL) ENGINE=InnoDB;
		CREATE TABLE sur_ins (sid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, id BIGINT NOT NULL, v VARCHAR(16) NULL) ENGINE=InnoDB;`)
	recorded := func(name string) *ir.Table {
		return &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "v", Type: ir.Varchar{Length: 16}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}
	db := openTestDB(t, dsn)
	defer db.Close()
	count := func(table string) int64 { return scalarInt(ctx, t, db, "SELECT COUNT(*) FROM "+table) }

	const total = 200
	for _, c := range []struct {
		table   string
		refused bool
	}{{"sur", true}, {"two", false}, {"kpk", false}} {
		t.Run("LOAD DATA/"+c.table, func(t *testing.T) {
			withSmallLoadDataSegments(t, 700)
			withFastReparentBackoff(t, 12)
			fired := 0
			loadDataSegmentFailHookForTest = func(segment int, replay bool, phase loadDataSegmentPhase) error {
				if segment == 2 && !replay && phase == loadDataAfterExec {
					fired++
					return vttabletUnavailable()
				}
				return nil
			}
			defer func() { loadDataSegmentFailHookForTest = nil }()

			rw := openRowWriter(t, ctx, dsn)
			defer closeIf(rw)
			err := rw.WriteRows(ctx, recorded(c.table), numberedRows(total))
			if fired != 1 {
				t.Fatalf("the ack-loss injection fired %d times; want 1 (LOAD DATA path not taken?)", fired)
			}
			got := count(c.table)
			if !c.refused {
				if err != nil {
					t.Fatalf("a target keyed on a supplied column must ride the ack loss; got: %v", err)
				}
				if got != total {
					t.Errorf("target holds %d rows; want %d", got, total)
				}
				return
			}
			if err == nil {
				t.Fatalf("WriteRows returned nil after a committed segment lost its ack; target holds %d rows for %d", got, total)
			}
			assertTargetJudgmentRefusal(t, err)
			if dupes := scalarInt(ctx, t, db, "SELECT COUNT(*) FROM (SELECT id FROM "+c.table+" GROUP BY id HAVING COUNT(*) > 1) d"); dupes != 0 {
				t.Errorf("%d ids landed twice — the refusal exists to prevent exactly this", dupes)
			}
		})
	}

	t.Run("batched INSERT/sur_ins", func(t *testing.T) {
		withFastReparentBackoff(t, 12)
		rwIface := openRowWriter(t, ctx, dsn)
		defer closeIf(rwIface)
		w := rwIface.(*RowWriter)
		table := recorded("sur_ins")
		batch := []ir.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "b"}, {"id": int64(3), "v": "c"}}
		query := buildBatchInsert(table, len(batch))
		args, err := flattenArgs(batch, table)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := w.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		sends := 0
		err = w.flushWithReparentRetry(ctx, table, len(batch), func(c *sql.Conn, _ bool) error {
			sends++
			if _, err := c.ExecContext(ctx, query, args...); err != nil {
				return err
			}
			if sends == 1 {
				return vttabletUnavailable() // committed; the ack is lost
			}
			return nil
		}, conn)
		if err == nil {
			t.Fatalf("the re-send was licensed: target holds %d rows for %d", count("sur_ins"), len(batch))
		}
		assertTargetJudgmentRefusal(t, err)
		if sends != 1 {
			t.Errorf("the batch was sent %d times; want 1", sends)
		}
		if got := count("sur_ins"); got != int64(len(batch)) {
			t.Errorf("target holds %d rows; want exactly the one committed batch (%d)", got, len(batch))
		}
	})
}

func assertTargetJudgmentRefusal(t *testing.T, err error) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Fatalf("got %v; want %s", err, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	if !strings.Contains(err.Error(), irbackup.ReplayKeylessTarget.Describe()) {
		t.Errorf("the refusal must say the TARGET judgment failed; got: %v", err)
	}
}
