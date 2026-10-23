//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The pin for VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED (Bug 331,
// docs/dev/audit-backlog.md): on a keyspace with more than one shard, the
// changes a shard makes after it has finished copying a table, while another
// shard is still copying it, must reach the target with full semantics.
//
// Each per-table VStream spans every shard. When one shard finishes its copy
// vttablet starts streaming that shard's binlog from its copy snapshot, and
// vtgate forwards those events before the stream-wide COPY_COMPLETED. Until
// the fix they went to the copy's row buffer, which keeps only after images:
// a DELETE vanished, and the table's resume position, taken at the
// stream-wide COPY_COMPLETED, was already past it.
//
// The shards are made uneven on purpose: the table is seeded through vtgate
// and then trimmed through a shard-targeted session, so the first shard holds
// a small fraction of the rows. The vtgate gRPC connection goes through the
// VSTREAM-COPY-RECONNECT-STALE-ROWS proxy in pass-through mode, only to slow
// the copy. The timing is proved, not hoped for: the changes are made after
// the log line recording the first shard's per-scope COPY_COMPLETED, and the
// cell fails as a RIG error unless the last shard's line is still absent
// when the changes have committed.
//
// Changes on each finished shard: deletes, payload updates, primary-key
// changes that keep the row on its shard (the vindex is on k, not on the
// key), inserts routed to that shard, and insert-then-delete pairs. On the
// shard still copying: deletes and updates. The verdict compares the SOURCE's
// (id, k, payload) rows read through vtgate with the TARGET's, after the CDC
// hand-off and a marker written behind the changes. It never consults
// sluice's own state.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags='integration vstream' -count=1 -timeout=60m -v \
//	  -run 'TestStreamer_VStreamShardSkewCopy' ./internal/pipeline/

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// vskRows is how many rows each skewed table is seeded with, before the
// shard-targeted trim.
const vskRows = 160_000

// vskArmPicks is how many existing ids one finished-shard arm changes (5 key
// changes, 15 deletes, 10 updates), and vskArmChanges the row changes its
// transaction carries: those 30, 13 inserts, and the deletes of 3 of them.
const (
	vskArmPicks   = 30
	vskArmChanges = 30 + 13 + 3
)

// vskHeldLine is the snapshot's INFO record of the changes a finished shard
// sent inside the copy, left to the CDC tail (shardCopyBoundary.logHeld).
const vskHeldLine = `msg="mysql/vstream: snapshot: shard finished its copy before the stream did; its later changes are replayed from its copy position"`

// vskSmallRows sizes the unskewed second table of the two-table cell.
const vskSmallRows = 1_000

// vskShardFinishedLine is the snapshot's DEBUG record of one shard's
// per-scope COPY_COMPLETED (logShardCopyFinished).
const vskShardFinishedLine = `msg="mysql/vstream: snapshot: shard finished its copy"`

// vskLayout is one keyspace's shard layout and how the cell skews it.
type vskLayout struct {
	shards []string
	// keep is, per shard, the highest seeded id that shard keeps; 0 keeps
	// every row the shard received.
	keep map[string]int64
	// finished are the shards that must have finished copying before the
	// changes are made; last must still be copying when they have committed.
	finished []string
	last     string
}

// vskShape is one cell.
type vskShape struct {
	prefix string
	// tables: 1 = one skewed table (the single-stream path); 2 = a skewed
	// table then an unskewed one (the per-table path), or two skewed tables
	// when bothSkewed (the concurrent copy).
	tables     int
	bothSkewed bool
	extraDSN   string
	backup     bool // drive backup full + incremental + chain restore instead of sync
	remedy     bool // establish the stream, leave stale rows, then re-copy with --restart-from-scratch
}

// vskArm is a set of changes and what the source must read afterwards.
type vskArm struct {
	name    string
	table   string
	deleted []int64 // must be absent on the target
	updated []int64 // must read upd-<id>
}

func vskShardDSN(mysqlDSN, keyspace, shard string) string {
	return strings.Replace(mysqlDSN, "/"+keyspace+"?", "/"+keyspace+":"+shard+"?", 1)
}

func vskCreate(t *testing.T, src twfbDB, sharded bool, keyspace string, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		src.exec(t, "CREATE TABLE "+tbl+" (id BIGINT NOT NULL, k BIGINT NOT NULL, payload VARCHAR(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
		if sharded {
			src.exec(t, "ALTER VSCHEMA ON "+keyspace+"."+tbl+" ADD VINDEX hash(k) USING hash")
		}
	}
}

// vskSeed inserts (id, k, payload) rows through vtgate.
func vskSeed(t *testing.T, dsn, table string, ids, ks []int64) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = db.Close() }()
	const per = 1000
	for start := 0; start < len(ids); start += per {
		end := min(start+per, len(ids))
		var b strings.Builder
		b.WriteString("INSERT INTO " + table + " (id, k, payload) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,%d,'v1-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy-%d')", ids[i], ks[i], ids[i])
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, err := db.ExecContext(ctx, b.String())
		cancel()
		if err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
}

// vskTrim deletes every row above keep through dsn, in batches under
// vttablet's 10,000-row DML cap.
func vskTrim(t *testing.T, dsn, table string, keep int64) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("trim open: %v", err)
	}
	defer func() { _ = db.Close() }()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		res, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id > %d LIMIT 5000", table, keep))
		cancel()
		if err != nil {
			t.Fatalf("trim %s: %v", table, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return
		}
	}
}

// vskRowsOf reads every (id, "k|payload") of table through db's driver.
func vskRowsOf(t *testing.T, db twfbDB, table string) map[int64]string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", db.engine, err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	q := "SELECT id, k, payload FROM " + table + " WHERE id > ? ORDER BY id LIMIT 5000"
	if db.engine == "postgres" {
		q = "SELECT id, k, payload FROM " + table + " WHERE id > $1 ORDER BY id LIMIT 5000"
	}
	out := map[int64]string{}
	after := int64(-1 << 62)
	for {
		rows, err := conn.QueryContext(ctx, q, after)
		if err != nil {
			t.Fatalf("read %s.%s: %v", db.engine, table, err)
		}
		n := 0
		for rows.Next() {
			var id, k int64
			var payload string
			if err := rows.Scan(&id, &k, &payload); err != nil {
				t.Fatalf("scan %s.%s: %v", db.engine, table, err)
			}
			out[id] = fmt.Sprintf("%d|%s", k, payload)
			after = id
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %s.%s: %v", db.engine, table, err)
		}
		_ = rows.Close()
		if n < 5000 {
			return out
		}
	}
}

// vskSorted returns the keys of set in ascending order.
func vskSorted(set map[int64]bool) []int64 {
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// vskPick returns n ids spread evenly over ids, skipping the first skip.
func vskPick(ids []int64, skip, n int) []int64 {
	ids = ids[skip:]
	out := make([]int64, 0, n)
	step := max(1, len(ids)/(n+1))
	for i := 1; i <= n && i*step < len(ids); i++ {
		out = append(out, ids[i*step])
	}
	return out
}

// vskShardLine is the shard-finished line for one shard of one table.
func vskShardLine(keyspace, shard, table string) string {
	return fmt.Sprintf("%s keyspace=%s shard=%s tables=%s", vskShardFinishedLine, keyspace, shard, table)
}

// vskCellResult is one run's outcome.
type vskCellResult struct {
	staleDeletes map[string]int // arm -> deleted ids still on the target
	staleUpdates map[string]int // arm -> updated ids with an old value
	extra        map[string][]int64
	missing      map[string][]int64
	valueDiff    map[string][]int64
	err          error
	note         string
	// held is, per "table shard" of a finished shard, the changes the
	// snapshot reported holding for the CDC tail; heldWant is what each
	// arm's transaction carried.
	held     map[string]int
	heldWant int
}

// vskHeldChanges sums the changes=N of every held-changes line for one
// shard of one table.
func vskHeldChanges(logs, keyspace, shard, table string) int {
	total := 0
	tag := fmt.Sprintf("%s keyspace=%s shard=%s tables=%s changes=", vskHeldLine, keyspace, shard, table)
	for _, line := range strings.Split(logs, "\n") {
		i := strings.Index(line, tag)
		if i < 0 {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(line[i+len(tag):], "%d", &n); err == nil {
			total += n
		}
	}
	return total
}

func (r vskCellResult) exact() bool {
	for _, m := range []map[string][]int64{r.extra, r.missing, r.valueDiff} {
		for _, ids := range m {
			if len(ids) > 0 {
				return false
			}
		}
	}
	return r.err == nil && r.note == ""
}

func (r vskCellResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "err=%v", r.err)
	arms := make([]string, 0, len(r.staleDeletes))
	for a := range r.staleDeletes {
		arms = append(arms, a)
	}
	sort.Strings(arms)
	for _, a := range arms {
		fmt.Fprintf(&b, "\n    %-34s stale-deletes=%d stale-updates=%d", a, r.staleDeletes[a], r.staleUpdates[a])
	}
	tables := make([]string, 0, len(r.extra))
	for tbl := range r.extra {
		tables = append(tables, tbl)
	}
	sort.Strings(tables)
	for _, tbl := range tables {
		fmt.Fprintf(&b, "\n    %s: extra-on-target=%d %v missing=%d %v value-diff=%d %v", tbl,
			len(r.extra[tbl]), r.extra[tbl][:min(8, len(r.extra[tbl]))],
			len(r.missing[tbl]), r.missing[tbl][:min(8, len(r.missing[tbl]))],
			len(r.valueDiff[tbl]), r.valueDiff[tbl][:min(8, len(r.valueDiff[tbl]))])
	}
	if r.note != "" {
		fmt.Fprintf(&b, "\n    note: %s", r.note)
	}
	fmt.Fprintf(&b, "\n    held for the CDC tail (want %d each): %v", r.heldWant, r.held)
	return b.String()
}

// vskRig is one vttestserver keyspace and one Postgres target.
type vskRig struct {
	vcrRig
	keyspace string
	layout   vskLayout
}

func (r vskRig) sharded() bool { return len(r.layout.shards) > 1 }

// vskRunCell builds the cell's tables, runs the copy, makes the changes
// while the layout's first shards have finished and its last is copying,
// and grades the target against the source.
func vskRunCell(t *testing.T, r vskRig, sh vskShape, run int) vskCellResult {
	t.Helper()
	a := fmt.Sprintf("%s%d_a", sh.prefix, run)
	b := fmt.Sprintf("%s%d_b", sh.prefix, run)
	tables := []string{a}
	skewed := []string{a}
	if sh.tables == 2 {
		tables = append(tables, b)
		if sh.bothSkewed {
			skewed = append(skewed, b)
		}
	}
	vskCreate(t, r.src(), r.sharded(), r.keyspace, tables...)
	seed := vcrRange(1, 1, vskRows)
	for _, tbl := range skewed {
		vskSeed(t, r.mysqlDSN, tbl, seed, seed)
	}
	if sh.tables == 2 && !sh.bothSkewed {
		small := vcrRange(1, 1, vskSmallRows)
		vskSeed(t, r.mysqlDSN, b, small, small)
	}
	// The skew: trim every shard but the last through a shard-targeted
	// session, then read each shard's ids back the same way.
	shardIDs := map[string]map[string][]int64{} // table -> shard -> ids
	for _, tbl := range skewed {
		shardIDs[tbl] = map[string][]int64{}
		for _, shard := range r.layout.shards {
			dsn := r.mysqlDSN
			if r.sharded() {
				dsn = vskShardDSN(r.mysqlDSN, r.keyspace, shard)
			}
			if keep := r.layout.keep[shard]; keep > 0 {
				vskTrim(t, dsn, tbl, keep)
			}
			shardIDs[tbl][shard] = vskSorted(vcrIDs(t, twfbDB{engine: "mysql", dsn: dsn}, tbl))
			t.Logf("  %s shard %s holds %d rows", tbl, shard, len(shardIDs[tbl][shard]))
		}
	}
	time.Sleep(3 * time.Second) // the schema tracker

	logStart := r.logs.Len()
	var (
		res       vskCellResult
		running   *twfbRun
		backed    chan error
		store     *blobcodec.LocalStore
		prevPos   string
		remedyArm *vskArm
	)
	res.staleDeletes, res.staleUpdates = map[string]int{}, map[string]int{}
	srcEng, _ := engines.Get("planetscale")
	pgEng, _ := engines.Get("postgres")
	streamID := fmt.Sprintf("%s%d", sh.prefix, run)
	cell := twfbCell{tgt: r.tgt(), streamID: streamID}
	if sh.backup {
		var err error
		store, err = blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		filter, err := migcore.NewTableFilter(tables, nil)
		if err != nil {
			t.Fatal(err)
		}
		backed = make(chan error, 1)
		go func() {
			backed <- (&backup.Backup{Source: srcEng, SourceDSN: r.streamDSN + sh.extraDSN, Store: store, Filter: filter, SluiceVersion: "test"}).Run(context.Background())
		}()
	} else {
		mk := func() *Streamer {
			s := r.streamer(streamID, tables, sh.extraDSN)
			s.AllowCrossShardMerge = r.sharded()
			return s
		}
		s := mk()
		if sh.remedy {
			// The state an affected release leaves behind: an established
			// stream whose target keeps rows the source deleted. Establish
			// the stream, stop it, delete rows on the source while it is
			// stopped (the target keeps them), then run the remedy, a
			// --restart-from-scratch re-copy, under the same in-copy
			// changes as every other cell.
			first := startTWFBRun(mk())
			if !vrsAwaitHandoff(t, cell, first, "", 6*time.Minute) {
				t.Fatalf("RIG: the establishing cold start never handed off (exited=%v err=%v)", first.exited(), first.err)
			}
			_ = first.stop(t)
			prevPos = cell.persistedPosition(t)
			kept := shardIDs[a][r.layout.finished[0]][1:21]
			for _, id := range kept {
				r.src().exec(t, fmt.Sprintf("DELETE FROM %s WHERE id = %d", a, id))
			}
			remedyArm = &vskArm{name: a + " kept by the stopped stream", table: a, deleted: kept}
			s.RestartFromScratch = true
			logStart = r.logs.Len()
		}
		running = startTWFBRun(s)
		defer func() { _ = running.stop(t) }()
	}
	exited := func() bool {
		if running != nil {
			return running.exited()
		}
		select {
		case err := <-backed:
			backed <- err
			return true
		default:
			return false
		}
	}

	// Wait for every finished shard of every skewed table, then require the
	// last shard of every skewed table to be still copying.
	deadline := time.Now().Add(6 * time.Minute)
	for {
		logs := r.logs.String()[logStart:]
		all := true
		for _, tbl := range skewed {
			for _, shard := range r.layout.finished {
				all = all && strings.Contains(logs, vskShardLine(r.keyspace, shard, tbl))
			}
		}
		if all {
			break
		}
		if exited() || time.Now().After(deadline) {
			vskLogTail(t, r, logStart)
			t.Fatalf("RIG: the copy exited or timed out before the first shards finished")
		}
		time.Sleep(20 * time.Millisecond)
	}

	db, err := sql.Open("mysql", r.mysqlDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	// One transaction per shard arm: vtgate's skew alignment releases a
	// shard's transactions as the slower shard's clock advances, about one
	// per heartbeat (measured), so many small transactions straddle the
	// stream-wide COPY_COMPLETED. One transaction arrives whole, inside the
	// copy or after it; vskHeldChanges proves which.
	inTx := func(stmts []string) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("source begin: %v", err)
		}
		for _, q := range stmts {
			if _, err := tx.Exec(q); err != nil {
				_ = tx.Rollback()
				t.Fatalf("source change %q: %v", q, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("source commit: %v", err)
		}
	}
	var arms []vskArm
	for _, tbl := range skewed {
		for si, shard := range r.layout.finished {
			ids := shardIDs[tbl][shard]
			picks := vskPick(ids, 0, vskArmPicks)
			pkc, del, upd := picks[:5], picks[5:20], picks[20:30]
			arm := vskArm{name: tbl + " finished " + shard, table: tbl, deleted: append([]int64(nil), del...), updated: upd}
			var stmts []string
			// A primary-key change that keeps the row on its shard (k, the
			// vindex column, is untouched): the old key must go.
			for _, id := range pkc {
				stmts = append(stmts, fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", tbl, id+50_000_000, id))
				arm.deleted = append(arm.deleted, id)
			}
			// Inserts routed to this shard (k = one of its ids, so the hash
			// vindex places the row here), and insert-then-delete pairs that
			// must leave nothing.
			for n := range 13 {
				id := int64(60_000_000 + si*1000 + n)
				stmts = append(stmts, fmt.Sprintf("INSERT INTO %s (id, k, payload) VALUES (%d, %d, 'ins-%d')", tbl, id, ids[n], id))
				if n >= 10 {
					stmts = append(stmts, fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, id))
					arm.deleted = append(arm.deleted, id)
				}
			}
			for _, id := range del {
				stmts = append(stmts, fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, id))
			}
			for _, id := range upd {
				stmts = append(stmts, fmt.Sprintf("UPDATE %s SET payload = 'upd-%d' WHERE id = %d", tbl, id, id))
			}
			inTx(stmts)
			arms = append(arms, arm)
		}
		lastIDs := shardIDs[tbl][r.layout.last]
		picks := vskPick(lastIDs, 0, 20)
		ctl := vskArm{name: tbl + " copying " + r.layout.last, table: tbl, deleted: picks[:10], updated: picks[10:]}
		var stmts []string
		for _, id := range ctl.deleted {
			stmts = append(stmts, fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, id))
		}
		for _, id := range ctl.updated {
			stmts = append(stmts, fmt.Sprintf("UPDATE %s SET payload = 'upd-%d' WHERE id = %d", tbl, id, id))
		}
		inTx(stmts)
		arms = append(arms, ctl)
	}
	if sh.tables == 2 && !sh.bothSkewed {
		nb := vskArm{name: b + " not started", table: b, deleted: vcrRange(1, 1, 10), updated: vcrRange(101, 1, 10)}
		var stmts []string
		for _, id := range nb.deleted {
			stmts = append(stmts, fmt.Sprintf("DELETE FROM %s WHERE id = %d", b, id))
		}
		for _, id := range nb.updated {
			stmts = append(stmts, fmt.Sprintf("UPDATE %s SET payload = 'upd-%d' WHERE id = %d", b, id, id))
		}
		inTx(stmts)
		arms = append(arms, nb)
	}
	_ = db.Close()
	if remedyArm != nil {
		arms = append(arms, *remedyArm)
	}
	// The timing proof: every change has committed and the last shard of
	// every skewed table has not finished its copy.
	logs := r.logs.String()[logStart:]
	for _, tbl := range skewed {
		if strings.Contains(logs, vskShardLine(r.keyspace, r.layout.last, tbl)) {
			t.Fatalf("RIG: shard %s finished copying %s before the changes had committed; the cell measured nothing", r.layout.last, tbl)
		}
	}
	t.Logf("  changes committed while %s was still copying (timing proved from the shard-finished log lines)", r.layout.last)

	if sh.backup {
		select {
		case err := <-backed:
			if err != nil {
				res.err = fmt.Errorf("backup full: %w", err)
				return res
			}
		case <-time.After(6 * time.Minute):
			t.Fatal("RIG: backup full did not finish in 6 minutes")
		}
		full, err := lineage.ReadManifest(context.Background(), store)
		if err != nil {
			t.Fatal(err)
		}
		for _, tbl := range tables {
			vskSeed(t, r.mysqlDSN, tbl, []int64{9_000_001}, []int64{9_000_001})
		}
		ictx, icancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err = (&IncrementalBackup{
			Source: srcEng, SourceDSN: r.streamDSN, Store: store, ParentRef: full.BackupID,
			Window: 20 * time.Second, SluiceVersion: "test",
		}).Run(ictx)
		icancel()
		if err != nil {
			res.err = fmt.Errorf("backup incremental: %w", err)
			return res
		}
		if err := (&backup.ChainRestore{Target: pgEng, TargetDSN: r.tgtDSN, Store: store}).Run(context.Background()); err != nil {
			res.err = fmt.Errorf("chain restore: %w", err)
			return res
		}
	} else {
		if !vrsAwaitHandoff(t, cell, running, prevPos, 6*time.Minute) {
			res.err = fmt.Errorf("no CDC hand-off (exited=%v err=%v)", running.exited(), running.err)
			vskLogTail(t, r, logStart)
			return res
		}
		for _, tbl := range tables {
			vskSeed(t, r.mysqlDSN, tbl, []int64{9_000_001}, []int64{9_000_001})
		}
		for _, tbl := range tables {
			if !r.tgt().waitRow(t, tbl, 9_000_001, running, 120*time.Second) {
				res.note += fmt.Sprintf("%s's marker never reached the target (exited=%v err=%v); ", tbl, running.exited(), running.err)
			}
		}
		time.Sleep(2 * time.Second)
	}
	cellLogs := r.logs.String()[logStart:]
	if dir := os.Getenv("SLUICE_VSK_LOGDIR"); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, streamID+".log"), []byte(cellLogs), 0o600)
	}
	res.held, res.heldWant = map[string]int{}, vskArmChanges
	for _, tbl := range skewed {
		for _, shard := range r.layout.finished {
			res.held[tbl+" "+shard] = vskHeldChanges(cellLogs, r.keyspace, shard, tbl)
		}
	}

	// The verdict, from the source and the target only.
	res.extra, res.missing, res.valueDiff = map[string][]int64{}, map[string][]int64{}, map[string][]int64{}
	src, tgt := map[string]map[int64]string{}, map[string]map[int64]string{}
	for _, tbl := range tables {
		src[tbl], tgt[tbl] = vskRowsOf(t, r.src(), tbl), vskRowsOf(t, r.tgt(), tbl)
		for id, sv := range src[tbl] {
			tv, ok := tgt[tbl][id]
			switch {
			case !ok:
				res.missing[tbl] = append(res.missing[tbl], id)
			case tv != sv:
				res.valueDiff[tbl] = append(res.valueDiff[tbl], id)
			}
		}
		for id := range tgt[tbl] {
			if _, ok := src[tbl][id]; !ok {
				res.extra[tbl] = append(res.extra[tbl], id)
			}
		}
		for _, m := range []map[string][]int64{res.extra, res.missing, res.valueDiff} {
			sort.Slice(m[tbl], func(i, j int) bool { return m[tbl][i] < m[tbl][j] })
		}
	}
	for _, arm := range arms {
		for _, id := range arm.deleted {
			if _, onSrc := src[arm.table][id]; onSrc {
				t.Fatalf("RIG: %s id %d still on the source after its delete", arm.table, id)
			}
			if _, onTgt := tgt[arm.table][id]; onTgt {
				res.staleDeletes[arm.name]++
			}
		}
		for _, id := range arm.updated {
			want := fmt.Sprintf("|upd-%d", id)
			if !strings.HasSuffix(src[arm.table][id], want) {
				t.Fatalf("RIG: %s id %d reads %q on the source, want suffix %q", arm.table, id, src[arm.table][id], want)
			}
			if !strings.HasSuffix(tgt[arm.table][id], want) {
				res.staleUpdates[arm.name]++
			}
		}
	}
	if !res.exact() {
		vskLogTail(t, r, logStart)
	}
	return res
}

// vskAddTableDrainedDefersReplays pins the drained `schema add-table`'s
// handling of changes Vitess replays inside its copy (ir.CopyReplayDeferrer).
// An established stream copies kz_est and is stopped; ka_new is added with
// `schema add-table`, and while its copy runs 40 of its rows are deleted and
// 5 of its keys changed. vttablet's keyspace-wide copy sorts ka_new before
// kz_est, so its catch-up before kz_est replays all 45 changes inside the
// copy. The copy cannot apply a DELETE or key change in order, and the
// stopped stream will replay them from its pre-add position, so the add must
// SUCCEED, report exactly 45 changes left to the stream (the proof the
// catch-up path ran), and the restarted stream must leave the target equal
// to the source by key and value. Before the deferral this add refused with
// VSTREAM-COPY-ROW-CHANGE-REFUSED, and before that refusal the copy dropped
// the deletes and wrote the key changes' new rows next to the old ones.
func vskAddTableDrainedDefersReplays(t *testing.T, r vskRig) {
	t.Helper()
	const est, added, streamID = "kz_est", "ka_new", "vskadd"
	vskCreate(t, r.src(), false, r.keyspace, est)
	small := vcrRange(1, 1, vskSmallRows)
	vskSeed(t, r.mysqlDSN, est, small, small)
	// ka_new exists, out of the stream's scope, before the stream is
	// established, as in the real flow: the drained restart then replays only
	// the add-time changes, not ka_new's whole seed.
	vskCreate(t, r.src(), false, r.keyspace, added)
	seed := vcrRange(1, 1, vskRows)
	vskSeed(t, r.mysqlDSN, added, seed, seed)
	time.Sleep(3 * time.Second)
	cell := twfbCell{tgt: r.tgt(), streamID: streamID}
	first := startTWFBRun(r.streamer(streamID, []string{est}, ""))
	if !vrsAwaitHandoff(t, cell, first, "", 6*time.Minute) {
		t.Fatalf("RIG: the establishing cold start never handed off (exited=%v err=%v)", first.exited(), first.err)
	}
	_ = first.stop(t)

	srcEng, _ := engines.Get("planetscale")
	pgEng, _ := engines.Get("postgres")
	addDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		addDone <- (&AddTable{
			Source: srcEng, Target: pgEng, SourceDSN: r.streamDSN, TargetDSN: r.tgtDSN,
			StreamID: streamID, TableName: added,
		}).Run(ctx)
	}()
	// Delete while ka_new is still copying: after its snapshot, before the
	// catch-up that precedes kz_est.
	deadline := time.Now().Add(6 * time.Minute)
	for {
		n, _ := strconv.Atoi(r.tgt().scalar(t, "SELECT count(*) FROM "+added))
		if n >= 20_000 {
			if n >= vskRows-1000 {
				t.Fatalf("RIG: %s held %d rows before the deletes; the copy was not mid-way", added, n)
			}
			break
		}
		select {
		case err := <-addDone:
			t.Fatalf("RIG: the add-table ended before the deletes: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("RIG: the add-table copy never reached 20,000 rows")
		}
		time.Sleep(50 * time.Millisecond)
	}
	logStart := r.logs.Len()
	deleted := append(vcrRange(1, 1, 20), vcrRange(150_001, 1, 20)...)
	for _, id := range deleted {
		r.src().exec(t, fmt.Sprintf("DELETE FROM %s WHERE id = %d", added, id))
	}
	// Key changes on rows the copy has already sent and on rows it has not.
	keyChanged := []int64{101, 102, 103, 140_001, 140_002}
	for _, id := range keyChanged {
		r.src().exec(t, fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", added, id+50_000_000, id))
	}
	var addErr error
	select {
	case addErr = <-addDone:
	case <-time.After(8 * time.Minute):
		t.Fatal("RIG: the add-table did not end")
	}
	if addErr != nil {
		t.Fatalf("the drained add-table failed: %v; a replayed change the copy cannot apply must be left to the stopped "+
			"stream, which replays it from before the copy", addErr)
	}
	tag := fmt.Sprintf("%s keyspace=%s table=%s changes=", vskDeferredLine, r.keyspace, added)
	deferred := 0
	for _, line := range strings.Split(r.logs.String()[logStart:], "\n") {
		if i := strings.Index(line, tag); i >= 0 {
			var n int
			if _, err := fmt.Sscanf(line[i+len(tag):], "%d", &n); err == nil {
				deferred += n
			}
		}
	}
	if want := len(deleted) + len(keyChanged); deferred != want {
		t.Errorf("TIMING: the add's copy left %d change(s) to the stream, want %d (every delete and key change, "+
			"replayed by vttablet's catch-up before the next table)", deferred, want)
	}
	t.Logf("the drained add succeeded and left %d replayed change(s) to the stream; target %s holds %s rows (source %s)",
		deferred, added, r.tgt().scalar(t, "SELECT count(*) FROM "+added), r.src().scalar(t, "SELECT count(*) FROM "+added))

	// The drained restart, which replays from the stream's pre-add position.
	run := startTWFBRun(r.streamer(streamID, []string{est}, ""))
	defer func() { _ = run.stop(t) }()
	for _, tbl := range []string{est, added} {
		vskSeed(t, r.mysqlDSN, tbl, []int64{9_000_001}, []int64{9_000_001})
	}
	for _, tbl := range []string{est, added} {
		if !r.tgt().waitRow(t, tbl, 9_000_001, run, 120*time.Second) {
			t.Fatalf("%s's marker never reached the target (exited=%v err=%v)", tbl, run.exited(), run.err)
		}
	}
	for _, tbl := range []string{est, added} {
		src, tgt := vskRowsOf(t, r.src(), tbl), vskRowsOf(t, r.tgt(), tbl)
		diff := 0
		for id, v := range src {
			if tgt[id] != v {
				diff++
			}
		}
		for id := range tgt {
			if _, ok := src[id]; !ok {
				diff++
			}
		}
		if diff != 0 {
			t.Errorf("SILENT DIVERGENCE after the drained restart: %s differs from the source in %d row(s) (source %d, target %d)", tbl, diff, len(src), len(tgt))
		} else {
			t.Logf("EXACT after the drained restart: %s source=%d target=%d", tbl, len(src), len(tgt))
		}
	}
}

// vskDeferredLine is the snapshot's INFO record of the replayed changes a
// copy left to the later change stream (logDeferredReplaysLocked).
const vskDeferredLine = `msg="mysql/vstream: snapshot: the copy received changes Vitess replayed inside it and left them to the change stream, which resumes from before the copy and replays them in order"`

func vskLogTail(t *testing.T, r vskRig, from int) {
	t.Helper()
	shown := 0
	for _, line := range strings.Split(r.logs.String()[from:], "\n") {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") || strings.Contains(line, "shard finished") ||
			strings.Contains(line, "bulk copy complete") || strings.Contains(line, "VSTREAM-") {
			if shown++; shown > 40 {
				t.Log("  ... (truncated)")
				return
			}
			t.Logf("  log: %s", line[:min(len(line), 400)])
		}
	}
}

// vskCell runs one shape and requires the target to match the source.
func vskCell(t *testing.T, r vskRig, sh vskShape) {
	t.Helper()
	res := vskRunCell(t, r, sh, 1)
	t.Logf("RESULT %s: %s", sh.prefix, res)
	if !res.exact() {
		t.Errorf("SILENT DIVERGENCE: the target does not match the source: %s", res)
	}
	// The cell measured the class only if every finished shard's
	// transaction arrived inside the copy, which the snapshot reports as
	// held changes. Zero means the transaction reached the CDC tail after
	// the stream-wide COPY_COMPLETED, where it was always applied.
	for scope, n := range res.held {
		if n != res.heldWant {
			t.Errorf("TIMING: the snapshot held %d change(s) of %s for the CDC tail, want %d (one arm's transaction, received inside the copy)",
				n, scope, res.heldWant)
		}
	}
}

func TestStreamer_VStreamShardSkewCopy(t *testing.T) {
	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	const keyspace = "commerce"

	boot := func(t *testing.T, layout vskLayout) (vskRig, func()) {
		t.Helper()
		mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, len(layout.shards))
		tgtDSN, cleanupTgt := startPGTarget(t)
		proxy := startVRSProxy(t, grpcEndpoint)
		shardParam := strings.Join(layout.shards, ",")
		r := vskRig{
			vcrRig: vcrRig{
				mysqlDSN: mysqlDSN,
				streamDSN: fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=%s",
					mysqlDSN, proxy.addr(), shardParam),
				tgtDSN: tgtDSN,
				logs:   &logs,
			},
			keyspace: keyspace,
			layout:   layout,
		}
		return r, func() { cleanupTgt(); cleanupSrc() }
	}

	// The control: one shard has no early finisher, so nothing is held and
	// the position is the copy's own. The changes land at the start of the
	// copy, in its snapshot or after it; either way the target must match.
	t.Run("OneShard", func(t *testing.T) {
		r, done := boot(t, vskLayout{shards: []string{"0"}, last: "0"})
		defer done()
		t.Run("OneTable", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "s1", tables: 1}) })
	})

	t.Run("TwoShards", func(t *testing.T) {
		r, done := boot(t, vskLayout{
			shards:   []string{"-80", "80-"},
			keep:     map[string]int64{"-80": 15_000},
			finished: []string{"-80"},
			last:     "80-",
		})
		defer done()
		t.Run("OneTable", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "so", tables: 1}) })
		t.Run("TwoTables", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "st", tables: 2}) })
		t.Run("ConcurrentCopy", func(t *testing.T) {
			vskCell(t, r, vskShape{prefix: "sk", tables: 2, bothSkewed: true, extraDSN: "&vstream_copy_table_parallelism=2"})
		})
		t.Run("BackupFull", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "sb", tables: 1, backup: true}) })
		// The operator remedy the notes name for a target an affected
		// release left stale: --restart-from-scratch, measured.
		t.Run("RemedyRestartFromScratch", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "sr", tables: 1, remedy: true}) })
	})

	// `schema add-table` copies the whole keyspace in one VStream, so a
	// delete or key change on the added table during its own copy is replayed
	// by vttablet's catch-up before the next table: the drained add must leave
	// it to the stopped stream, and the restart must land exact.
	t.Run("AddTable", func(t *testing.T) {
		r, done := boot(t, vskLayout{shards: []string{"0"}, last: "0"})
		defer done()
		vskAddTableDrainedDefersReplays(t, r)
	})

	t.Run("FourShards", func(t *testing.T) {
		r, done := boot(t, vskLayout{
			shards:   []string{"-40", "40-80", "80-c0", "c0-"},
			keep:     map[string]int64{"-40": 8_000, "40-80": 30_000, "80-c0": 60_000},
			finished: []string{"-40", "40-80"},
			last:     "c0-",
		})
		defer done()
		t.Run("OneTable", func(t *testing.T) { vskCell(t, r, vskShape{prefix: "fo", tables: 1}) })
	})
}
