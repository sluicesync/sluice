//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-ROWWRITER: the key-collation guard on the idempotent copy
// writer (row_writer_batch.go), on real servers — MySQL (the shared
// container; SLUICE_TEST_MYSQL_IMAGE=mysql:8.4 runs it on 8.4) and MariaDB
// 11.4, whose upsert is spelled with VALUES() rather than the row alias.
// Both write cores run every cell: the serial WriteRowsIdempotent (resumed
// copy, chunked resume, add-table, the serial VStream COPY, restore) and
// the fan-out WriteRowsIdempotentParallel (the VStream COPY's write-side
// fan-out).
//
// The independent expected value is the target's own rows, read back by
// HEX(key) after the write, never the writer's error alone: a refusal must
// leave the target as it was before the refused statement, and a copy that
// passes must land the source's exact key bytes.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// krwCore is one idempotent write core of the copy writer.
type krwCore struct {
	name  string
	write func(ctx context.Context, w *RowWriter, table *ir.Table, rows []ir.Row) error
}

func krwCores() []krwCore {
	feed := func(rows []ir.Row) chan ir.Row {
		ch := make(chan ir.Row, len(rows))
		for _, r := range rows {
			ch <- r
		}
		close(ch)
		return ch
	}
	return []krwCore{
		{"serial", func(ctx context.Context, w *RowWriter, table *ir.Table, rows []ir.Row) error {
			return w.WriteRowsIdempotent(ctx, table, feed(rows))
		}},
		{"fan-out", func(ctx context.Context, w *RowWriter, table *ir.Table, rows []ir.Row) error {
			// One row per worker: the merging rows meet in DIFFERENT
			// statements on different connections, so the second to land
			// meets the first one's committed row.
			workers := make([]<-chan ir.Row, len(rows))
			for i, r := range rows {
				workers[i] = feed([]ir.Row{r})
			}
			return w.WriteRowsIdempotentParallel(ctx, table, workers)
		}},
	}
}

// TestRowWriterKeyCollationGuard_MySQLFamily is the copy writer's matrix:
// every key shape the upsert matches through (PRIMARY KEY, a UNIQUE key
// beside a PRIMARY KEY, a table whose every column is its key, a UNIQUE key
// with no PRIMARY KEY), two merging rows in one copy and a merge against a
// row an earlier batch landed, × both write cores × both servers × both
// upsert spellings. Each merge must refuse KEY-COLLATION-MERGE; a copy with
// no merge must land byte-exact, and a re-sent row (a resumed chunk) must
// pass.
func TestRowWriterKeyCollationGuard_MySQLFamily(t *testing.T) {
	for _, srv := range kcgServers(t) {
		for _, core := range krwCores() {
			srv, core := srv, core
			t.Run(srv.name+"/"+core.name, func(t *testing.T) {
				krwRunServer(t, srv, core)
			})
		}
	}
}

// krwScalar reads one value as text.
func krwScalar(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v sql.NullString
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return v.String
}

func krwRunServer(t *testing.T, srv kcgServer, core krwCore) {
	n := 0
	cell := func() string {
		n++
		db := fmt.Sprintf("krw_%s_%s_%d", strings.NewReplacer("-", "", ".", "").Replace(srv.name),
			strings.ReplaceAll(core.name, "-", ""), n)
		dsn, cleanup := srv.newDB(t, db)
		t.Cleanup(cleanup)
		return dsn
	}
	write := func(t *testing.T, dsn string, table *ir.Table, rows ...ir.Row) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rw, err := srv.eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		return core.write(ctx, rw.(*RowWriter), table, rows)
	}
	wantMerge := func(t *testing.T, err error, dsn, table, idCol string, before ...string) {
		t.Helper()
		got := kcgRows(t, dsn, table, idCol)
		if !errors.Is(err, ir.ErrKeyCollationMerge) {
			t.Fatalf("err = %v; want %s (the target holds %q)", err, ir.ErrKeyCollationMerge, got)
		}
		if !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
			t.Fatalf("the refusal text carries no marker: %v", err)
		}
		for _, b := range before {
			if got == b {
				return
			}
		}
		t.Fatalf("after the refusal the target is %q; want one of %q (no merged row)", got, before)
	}
	const tgtCI = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
	binKey := kcgVarchar("utf8mb4_bin")
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	threeCols := func(key ir.Type) []*ir.Column {
		return []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: key},
			{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
		}
	}
	pkOn := func(cols ...string) *ir.Index {
		ic := make([]ir.IndexColumn, len(cols))
		for i, c := range cols {
			ic[i] = ir.IndexColumn{Column: c}
		}
		return &ir.Index{Columns: ic}
	}

	for _, src := range []struct {
		name string
		key  ir.Type
	}{
		{"source _bin", binKey},
		// A Postgres source key on the database default collation. The
		// copy arms it inside the primary key too (unlike the applier): a
		// copy re-sends only rows of a snapshot, never a re-delivered
		// change of a key, so there is no wedge to avoid.
		{"source postgres default", pgDefault},
	} {
		src := src
		t.Run(src.name, func(t *testing.T) {
			t.Run("pk merge inside one copy", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
				tbl := &ir.Table{Name: "kd", Columns: threeCols(src.key), PrimaryKey: pkOn("k")}
				err := write(t, dsn, tbl,
					ir.Row{"id": int64(1), "k": "a", "d": int64(1)},
					ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				// Serial: both rows in one statement, rolled back together.
				// Fan-out: the first worker's row may have committed.
				wantMerge(t, err, dsn, "kd", "id", "", "1:61", "2:41")
			})
			t.Run("pk merge against an earlier batch", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
				applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a',1)")
				tbl := &ir.Table{Name: "kd", Columns: threeCols(src.key), PrimaryKey: pkOn("k")}
				err := write(t, dsn, tbl, ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				wantMerge(t, err, dsn, "kd", "id", "1:61")
			})
			t.Run("unique key beside a primary key", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE ku (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, UNIQUE KEY uk (k))")
				applyDDL(t, dsn, "INSERT INTO ku VALUES (1,'a',1)")
				tbl := &ir.Table{Name: "ku", Columns: threeCols(src.key), PrimaryKey: pkOn("id")}
				err := write(t, dsn, tbl, ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				wantMerge(t, err, dsn, "ku", "id", "1:61")
			})
			t.Run("every column is the key", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE ak (k VARCHAR(20) "+tgtCI+" NOT NULL PRIMARY KEY)")
				applyDDL(t, dsn, "INSERT INTO ak VALUES ('a')")
				tbl := &ir.Table{Name: "ak", Columns: []*ir.Column{{Name: "k", Type: src.key}}, PrimaryKey: pkOn("k")}
				// The no-op `k = new.k` absorbs any conflict: without the
				// guard the incoming 'A' silently vanishes.
				err := write(t, dsn, tbl, ir.Row{"k": "A"})
				wantMerge(t, err, dsn, "ak", "k", "a:61")
			})
			t.Run("unique key and no primary key", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE nk (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, UNIQUE KEY uk (k))")
				applyDDL(t, dsn, "INSERT INTO nk VALUES (1,'a',1)")
				tbl := &ir.Table{
					Name: "nk", Columns: threeCols(src.key),
					Indexes: []*ir.Index{{Name: "uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}},
				}
				tbl.Columns[1].Nullable = false
				err := write(t, dsn, tbl, ir.Row{"id": int64(2), "k": "A", "d": int64(2)})
				wantMerge(t, err, dsn, "nk", "id", "1:61")
			})
			t.Run("a copy with no merge lands byte-exact, and a re-sent row passes", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE ku (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, UNIQUE KEY uk (k))")
				tbl := &ir.Table{Name: "ku", Columns: threeCols(src.key), PrimaryKey: pkOn("id")}
				rows := []ir.Row{
					{"id": int64(1), "k": "A", "d": int64(1)},
					{"id": int64(2), "k": "b", "d": int64(2)},
					{"id": int64(3), "k": "ß", "d": int64(3)},
					{"id": int64(4), "k": "é", "d": int64(4)},
					{"id": int64(5), "k": "Z z", "d": nil},
				}
				if err := write(t, dsn, tbl, rows...); err != nil {
					t.Fatalf("a copy with no merge refused: %v", err)
				}
				const want = "1:41,2:62,3:C39F,4:C3A9,5:5A207A"
				if got := kcgRows(t, dsn, "ku", "id"); got != want {
					t.Fatalf("target %s; want %s", got, want)
				}
				// A resumed chunk re-sends rows already on the target: the
				// same source rows, absorbed.
				rows[0]["d"] = int64(10)
				if err := write(t, dsn, tbl, rows[:2]...); err != nil {
					t.Fatalf("re-sending rows already copied refused: %v", err)
				}
				if got := kcgRows(t, dsn, "ku", "id"); got != want {
					t.Fatalf("after the re-send the target is %s; want %s", got, want)
				}
				if d := krwScalar(t, dsn, "SELECT d FROM ku WHERE id = 1"); d != "10" {
					t.Fatalf("the re-sent row's value is %s; want 10 (the upsert did not refresh it)", d)
				}
			})
		})
	}

	// The source and target agree on the key's collation: nothing is armed,
	// and a case-only re-send of a `_ci` source's key converges exactly as
	// before (the source's own collation equates the two).
	t.Run("agreeing collations carry no guard", func(t *testing.T) {
		dsn := cell()
		applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
		applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'a',1)")
		tbl := &ir.Table{Name: "kd", Columns: threeCols(kcgVarchar("utf8mb4_general_ci")), PrimaryKey: pkOn("k")}
		if err := write(t, dsn, tbl, ir.Row{"id": int64(1), "k": "A", "d": int64(2)}); err != nil {
			t.Fatalf("an unarmed table refused: %v", err)
		}
		if got, want := kcgRows(t, dsn, "kd", "id"), "1:61"; got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	})
}

// TestRowWriterKeyCollationGuard_PlainCoresWithholdTheReplayTolerance pins
// the two PLAIN write cores' retry arms (KEY-COLLATION-ROWWRITER sibling
// sweep). A fresh copy's first attempt meets two source keys the target
// equates as a loud duplicate key. But after a transient, each core treats a
// duplicate on the RETRY as its own already-landed rows — the batched core
// tolerates the retry's 1062 (ADR-0108), LOAD DATA counts the replay's
// duplicate-key skips as re-sent rows — and both arguments assume the
// collision is with this batch's own rows. On an armed table it can be a
// merge: measured 2026-10-12, a transient before the first attempt then
// 'A' and 'a' in the retried batch dropped the batch (batched) or 'a'
// (LOAD DATA) at exit 0. Armed, the retry arm now refuses
// KEY-COLLATION-REPLAY-AMBIGUOUS.
//
// The independent expected value is the target's rows against the source's.
func TestRowWriterKeyCollationGuard_PlainCoresWithholdTheReplayTolerance(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "krw_plain_retry")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	enableLocalInfileOrSkip(t, ctx, dsn)
	withFastReparentBackoff(t, 12)
	withSmallLoadDataSegments(t, 1<<20)

	// inject fails the FIRST attempt with a classified transient, before its
	// statement ran (rolled back) or after it committed (the lost ack).
	cores := []struct {
		name   string
		bulk   ir.BulkLoadMethod
		inject func(afterExec bool) func()
	}{
		{"batched-insert", ir.BulkLoadBatchedInsert, func(afterExec bool) func() {
			batchedInsertFailHookForTest = func(isRetry, executed bool) error {
				if isRetry || executed != afterExec {
					return nil
				}
				return vttabletUnavailable()
			}
			return func() { batchedInsertFailHookForTest = nil }
		}},
		{"load-data", ir.BulkLoadLoadDataInfile, func(afterExec bool) func() {
			want := loadDataBeforeExec
			if afterExec {
				want = loadDataAfterExec
			}
			loadDataSegmentFailHookForTest = func(_ int, replay bool, phase loadDataSegmentPhase) error {
				if replay || phase != want {
					return nil
				}
				return vttabletUnavailable()
			}
			return func() { loadDataSegmentFailHookForTest = nil }
		}},
	}
	source := []ir.Row{{"id": int64(1), "k": "A", "d": int64(1)}, {"id": int64(2), "k": "a", "d": int64(2)}, {"id": int64(3), "k": "b", "d": int64(3)}}
	for _, core := range cores {
		for _, armed := range []bool{true, false} {
			core, armed := core, armed
			name := core.name + "/armed"
			if !armed {
				name = core.name + "/unarmed (the tolerance still converges)"
			}
			t.Run(name, func(t *testing.T) {
				srcColl, rows := "utf8mb4_bin", source
				if !armed {
					// An unarmed table cannot hold the pair at all; it is a
					// genuine lost-ack replay of distinct keys.
					srcColl, rows = "utf8mb4_general_ci", []ir.Row{source[0], source[2]}
				}
				applyDDL(t, dsn, "DROP TABLE IF EXISTS pr; CREATE TABLE pr (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 "+
					"COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY, d INT) ENGINE=InnoDB")
				tbl := &ir.Table{Name: "pr", Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 32}},
					{Name: "k", Type: kcgVarchar(srcColl)},
					{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
				}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}}
				// Armed: the first attempt rolled back, so the retry's
				// collision can only be the merge. Unarmed: it committed and
				// lost its ack, so the retry collides with its own rows.
				defer core.inject(!armed)()

				rw, err := Engine{}.OpenRowWriter(ctx, dsn)
				if err != nil {
					t.Fatalf("OpenRowWriter: %v", err)
				}
				w := rw.(*RowWriter)
				defer func() { _ = w.Close() }()
				w.bulkLoad = core.bulk
				ch := make(chan ir.Row, len(rows))
				for _, r := range rows {
					ch <- r
				}
				close(ch)
				err = w.WriteRows(ctx, tbl, ch)
				got := kcgRows(t, dsn, "pr", "id")
				if !armed {
					if err != nil || got != "1:41,3:62" {
						t.Fatalf("an unarmed retry no longer converges: err=%v target=%s", err, got)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), keyCollationReplayAmbiguousMarker) {
					t.Fatalf("err = %v; want %s (the target holds %q, the source three rows)", err, keyCollationReplayAmbiguousMarker, got)
				}
			})
		}
	}
}
