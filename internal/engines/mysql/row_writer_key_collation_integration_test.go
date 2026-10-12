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
		// Review 2026-10-12 finding 1: a NONDETERMINISTIC Postgres key,
		// armed exactly as the default is.
		{"source postgres nondeterministic", ir.Varchar{Length: 20, Collation: "ci", Determinism: ir.CollationNonDeterministic}},
		// Finding 5: a named Postgres collation whose determinism no reader
		// stamped — what a manifest written before v0.159.0 decodes to.
		{"source postgres named, determinism unknown (pre-v0.159.0 manifest)", ir.Varchar{Length: 20, Collation: "C"}},
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
			t.Run("accent-only pk merge against an earlier batch", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, PRIMARY KEY (k))")
				applyDDL(t, dsn, "INSERT INTO kd VALUES (1,'e',1)")
				tbl := &ir.Table{Name: "kd", Columns: threeCols(src.key), PrimaryKey: pkOn("k")}
				err := write(t, dsn, tbl, ir.Row{"id": int64(2), "k": "é", "d": int64(2)})
				wantMerge(t, err, dsn, "kd", "id", "1:65")
			})
			t.Run("composite primary key with one armed column", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE kc (id INT NOT NULL, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, PRIMARY KEY (k, id))")
				applyDDL(t, dsn, "INSERT INTO kc VALUES (1,'a',1)")
				tbl := &ir.Table{Name: "kc", Columns: threeCols(src.key), PrimaryKey: pkOn("k", "id")}
				err := write(t, dsn, tbl, ir.Row{"id": int64(1), "k": "A", "d": int64(2)})
				wantMerge(t, err, dsn, "kc", "id", "1:61")
			})
			// Review 2026-10-12 #2 finding 1: a PREFIX unique key. The server
			// matches 'Apple-1' against 'apple-2' on the five-character prefix
			// alone; a guard comparing whole values called them different and
			// let the upsert overwrite row 1 at exit 0.
			t.Run("unique prefix key beside a primary key", func(t *testing.T) {
				dsn := cell()
				applyDDL(t, dsn, "CREATE TABLE kx (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) "+tgtCI+" NOT NULL, d INT, UNIQUE KEY uk (k(5)))")
				applyDDL(t, dsn, "INSERT INTO kx VALUES (1,'apple-2',1)")
				tbl := &ir.Table{Name: "kx", Columns: threeCols(src.key), PrimaryKey: pkOn("id")}
				err := write(t, dsn, tbl, ir.Row{"id": int64(2), "k": "Apple-1", "d": int64(2)})
				wantMerge(t, err, dsn, "kx", "id", "1:6170706C652D32")
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
// (LOAD DATA) at exit 0.
//
// Since review 2026-10-12 finding 3 the two cores answer differently:
//   - batched: the retried batch is re-sent as the GUARDED upsert, so a
//     genuine lost-ack replay on an armed table CONVERGES (own rows) and
//     only a real merge refuses, as KEY-COLLATION-MERGE;
//   - LOAD DATA (no upsert form): a replay that skipped rows on an armed
//     table refuses KEY-COLLATION-REPLAY-AMBIGUOUS, the lost-ack replay
//     included (the stated residual); a replay whose only warnings are not
//     duplicate-key skips (a relaxed-mode clamp, every row inserted) does
//     NOT refuse.
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
	type core struct {
		name   string
		bulk   ir.BulkLoadMethod
		inject func(afterExec bool) func()
	}
	batched := core{"batched-insert", ir.BulkLoadBatchedInsert, func(afterExec bool) func() {
		batchedInsertFailHookForTest = func(isRetry, executed bool) error {
			if isRetry || executed != afterExec {
				return nil
			}
			return vttabletUnavailable()
		}
		return func() { batchedInsertFailHookForTest = nil }
	}}
	loadData := core{"load-data", ir.BulkLoadLoadDataInfile, func(afterExec bool) func() {
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
	}}
	pair := []ir.Row{{"id": int64(1), "k": "A", "d": int64(1)}, {"id": int64(2), "k": "a", "d": int64(2)}, {"id": int64(3), "k": "b", "d": int64(3)}}
	distinct := []ir.Row{pair[0], pair[2]}
	const (
		converges  = ""
		bin, ci    = "utf8mb4_bin", "utf8mb4_general_ci"
		relaxedRun = "relaxed"
	)
	cells := []struct {
		name      string
		core      core
		srcColl   string // ci: unarmed
		rows      []ir.Row
		afterExec bool   // the lost ack (true) or a rolled-back first attempt
		want      string // the refusal marker, or converges
		mode      string // relaxedRun: --mysql-sql-mode='' and a clamping value
	}{
		{"batched/armed merge (rolled back, 'A' and 'a' in the retry)", batched, bin, pair, false, keyCollationMergeMarker, ""},
		{"batched/armed own rows (lost ack) converges", batched, bin, distinct, true, converges, ""},
		{"batched/unarmed own rows (lost ack) converges", batched, ci, distinct, true, converges, ""},
		{"load-data/armed merge (rolled back, 'A' and 'a' in the replay)", loadData, bin, pair, false, keyCollationReplayAmbiguousMarker, ""},
		{"load-data/armed own rows (lost ack): ambiguous, refuses", loadData, bin, distinct, true, keyCollationReplayAmbiguousMarker, ""},
		{"load-data/unarmed own rows (lost ack) converges", loadData, ci, distinct, true, converges, ""},
		{"load-data/armed replay whose only warning is a clamp converges", loadData, bin, distinct, false, converges, relaxedRun},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			applyDDL(t, dsn, "DROP TABLE IF EXISTS pr; CREATE TABLE pr (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 "+
				"COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY, d INT, c TINYINT) ENGINE=InnoDB")
			tbl := &ir.Table{Name: "pr", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "k", Type: kcgVarchar(cell.srcColl)},
				{Name: "d", Type: ir.Integer{Width: 32}, Nullable: true},
				{Name: "c", Type: ir.Integer{Width: 8}, Nullable: true},
			}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}}
			rows := make([]ir.Row, len(cell.rows))
			for i, r := range cell.rows {
				rows[i] = ir.Row{"id": r["id"], "k": r["k"], "d": r["d"], "c": int64(1)}
				if cell.mode == relaxedRun && i == 0 {
					rows[i]["c"] = int64(300) // clamps to 127 under a relaxed sql_mode: a non-duplicate warning
				}
			}
			defer cell.core.inject(cell.afterExec)()

			var w *RowWriter
			if cell.mode == relaxedRun {
				relaxed := ""
				db, err := openDB(ctx, mustParseDSN(t, dsn), &relaxed)
				if err != nil {
					t.Fatalf("openDB: %v", err)
				}
				defer func() { _ = db.Close() }()
				w = &RowWriter{db: db, sqlMode: &relaxed}
			} else {
				rw, err := Engine{}.OpenRowWriter(ctx, dsn)
				if err != nil {
					t.Fatalf("OpenRowWriter: %v", err)
				}
				w = rw.(*RowWriter)
				defer func() { _ = w.Close() }()
			}
			w.bulkLoad = cell.core.bulk
			ch := make(chan ir.Row, len(rows))
			for _, r := range rows {
				ch <- r
			}
			close(ch)
			err := w.WriteRows(ctx, tbl, ch)
			got := kcgRows(t, dsn, "pr", "id")
			if cell.want == converges {
				if err != nil || got != "1:41,3:62" {
					t.Fatalf("the retry did not converge: err=%v target=%s (want 1:41,3:62)", err, got)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), cell.want) {
				t.Fatalf("err = %v; want %s (the target holds %q, the source %d rows)", err, cell.want, got, len(rows))
			}
			// A refusal leaves no merged row: nothing on the target holds a
			// source row's key with another row's id.
			for _, r := range strings.Split(got, ",") {
				if r != "" && r != "1:41" && r != "3:62" && r != "2:61" {
					t.Fatalf("the target holds %s, which is no source row (target %s)", r, got)
				}
			}
		})
	}
}
