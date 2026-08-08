//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0101 §3's central claim — "gaplessness is by construction, not by
// argument" — ground-truthed under concurrent churn.
//
// The construction is one sentence long: all N reader transactions and the
// ONE recorded binlog position are taken while a single FLUSH TABLES WITH
// READ LOCK is held, so the readers' cut and the recorded position name the
// same instant and nothing can commit between them. Every other property of
// the concurrent cold-copy rests on it.
//
// Until 2026-08-07 nothing checked it. The AST roster next door
// (backup_snapshot_capture_order_test.go) DISCOVERED
// [acquireConsistentSnapshot] and then exempted it, with the claim itself as
// the exemption's reason — so moving the position read below the UNLOCK left
// the whole tree green (mutation-verified: the entire `internal/engines/mysql`
// unit suite passed with that mutant applied). The roster is now widened to
// grade the bracket rather than take it on trust, and this file is the half a
// source-order walker cannot supply:
//
//   - the SERVER's own record that the three statements really execute in
//     that order on ONE connection (source order is not execution order once
//     the statements are spread across a loop and a helper), and
//   - the behaviour the order exists to produce: with writers committing
//     throughout the open, every committed row is in the snapshot or in the
//     CDC tail, and never in neither.
//
// The independent expected value for the second leg is the set of ids the
// test's own writer goroutines confirmed committed — a number that comes from
// neither the snapshot nor the recorded position, which is what the 2026-08-01
// rule asks for. The first leg's is `mysql.general_log`.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// covers the ADR-0101 native-binlog CONCURRENT opener
// ([acquireConsistentSnapshot], reached through
// [Engine.openBinlogSnapshotStreamConcurrent]). The serial cold-start floor
// and the backup lane have their own general-log pins
// (TestLockFreeSnapshotCapturesPositionBeforeSnapshot,
// TestBackupSnapshotCapturesPositionBeforeSnapshot); the VStream and Postgres
// lanes have no FTWRL window to bracket.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// concurrentGaplessWriters / concurrentGaplessRows shape the churn: enough
// concurrent committers that the FTWRL window is genuinely contended, and
// enough rows per writer that the window cannot fall in a quiet gap.
const (
	concurrentGaplessWriters = 4
	concurrentGaplessRows    = 400
	concurrentGaplessReaders = 4
)

// TestConcurrentSnapshotCapturesPositionUnderTheLock is the general-log leg:
// on the ONE connection that runs them, FLUSH TABLES WITH READ LOCK must
// precede the binlog-position read, which must precede UNLOCK TABLES.
//
// Keying on thread_id rather than user_host is deliberate — the opener pins
// N connections for the SAME user and only conn 0 holds the lock and reads
// the position, so a user-scoped ordering assertion would be satisfied by
// three statements spread across three connections, which is not the claim.
func TestConcurrentSnapshotCapturesPositionUnderTheLock(t *testing.T) {
	host, port, rootUser, rootPass := ensureSharedMySQL(t)
	resetSharedDB(t, "concgapless_order_db")
	dsn := sharedDSN(host, port, rootUser, rootPass, "concgapless_order_db")

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	root, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer func() { _ = root.Close() }()

	lockfreeExec(t, ctx, root, `CREATE TABLE t_a (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB`)
	lockfreeExec(t, ctx, root, `CREATE TABLE t_b (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB`)

	lockfreeExec(t, ctx, root, `SET GLOBAL log_output = 'TABLE'`)
	lockfreeExec(t, ctx, root, `TRUNCATE TABLE mysql.general_log`)
	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = ON`)
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = root.ExecContext(cctx, `SET GLOBAL general_log = OFF`)
	}()

	stream, err := (Engine{Flavor: FlavorVanilla}).openBinlogSnapshotStreamConcurrent(
		ctx, dsn, concurrentGaplessReaders, []string{"t_a", "t_b"},
	)
	if err != nil {
		t.Fatalf("openBinlogSnapshotStreamConcurrent: %v", err)
	}
	if stream.Position.Token == "" {
		t.Error("concurrent snapshot recorded an empty CDC anchor")
	}
	if err := stream.CloseFn(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	lockfreeExec(t, ctx, root, `SET GLOBAL general_log = OFF`)

	rows, err := root.QueryContext(ctx,
		`SELECT thread_id, CONVERT(argument USING utf8mb4)
		   FROM mysql.general_log
		  WHERE command_type = 'Query'
		  ORDER BY event_time, thread_id`)
	if err != nil {
		t.Fatalf("read general_log: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type marks struct{ lock, pos, unlock int }
	perThread := map[int64]*marks{}
	var (
		stmts = 0
		seq   = 0
		upper = func(s, sub string) bool { return strings.Contains(strings.ToUpper(s), sub) }
	)
	for rows.Next() {
		var (
			tid int64
			arg string
		)
		if err := rows.Scan(&tid, &arg); err != nil {
			t.Fatalf("scan general_log: %v", err)
		}
		stmts++
		seq++
		m, ok := perThread[tid]
		if !ok {
			m = &marks{lock: -1, pos: -1, unlock: -1}
			perThread[tid] = m
		}
		switch {
		case upper(arg, "FLUSH TABLES WITH READ LOCK"):
			if m.lock < 0 {
				m.lock = seq
			}
		case upper(arg, "UNLOCK TABLES"):
			if m.unlock < 0 {
				m.unlock = seq
			}
		case upper(arg, "BINARY LOG STATUS"), upper(arg, "MASTER STATUS"), upper(arg, "BINLOG STATUS"):
			if m.pos < 0 {
				m.pos = seq
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate general_log: %v", err)
	}
	if stmts == 0 {
		t.Fatal("general_log captured no statements — the log is not recording, so this gate is vacuous")
	}

	// Exactly one connection should carry all three marks: the coordinator.
	coordinators := 0
	for tid, m := range perThread {
		if m.lock < 0 && m.pos < 0 && m.unlock < 0 {
			continue
		}
		if m.lock < 0 || m.pos < 0 || m.unlock < 0 {
			t.Errorf("thread %d carries only part of the freeze window (lock=%d, position=%d, unlock=%d); the "+
				"lock, the anchor read and the unlock must all be on the coordinator connection — a position "+
				"read on a DIFFERENT connection is not covered by this connection's lock",
				tid, m.lock, m.pos, m.unlock)
			continue
		}
		coordinators++
		if !(m.lock < m.pos && m.pos < m.unlock) {
			t.Errorf("thread %d executed the freeze window out of order (FLUSH TABLES WITH READ LOCK at %d, "+
				"position read at %d, UNLOCK TABLES at %d; want lock < position < unlock). Outside the freeze, a "+
				"commit landing between the readers' views and the recorded position is above the views and "+
				"below the position, so it is in NEITHER the cold copy nor the CDC tail — the silent-loss case "+
				"ADR-0101 §3 says cannot happen by construction",
				tid, m.lock, m.pos, m.unlock)
		}
	}
	if coordinators != 1 {
		t.Fatalf("expected exactly 1 coordinator connection holding the whole freeze window, saw %d — this "+
			"assertion is vacuous unless the window was actually observed", coordinators)
	}
}

// TestConcurrentSnapshotIsGaplessUnderChurn is the behavioural leg: writers
// commit throughout the open, and afterwards every committed id must be in
// the readers' snapshot or in the CDC tail replayed from the ONE recorded
// position. It also asserts all N readers pinned the SAME cut, which is the
// property the FTWRL buys over N independent consistent snapshots
// (ADR-0101 §4's rejected design).
func TestConcurrentSnapshotIsGaplessUnderChurn(t *testing.T) {
	host, port, rootUser, rootPass := ensureSharedMySQL(t)
	resetSharedDB(t, "concgapless_db")
	dsn := sharedDSN(host, port, rootUser, rootPass, "concgapless_db")

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	root, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer func() { _ = root.Close() }()

	lockfreeExec(t, ctx, root, `CREATE TABLE churn (id BIGINT NOT NULL PRIMARY KEY, w INT NOT NULL) ENGINE=InnoDB`)
	lockfreeExec(t, ctx, root, `CREATE TABLE quiet (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(16)) ENGINE=InnoDB`)
	lockfreeExec(t, ctx, root, `INSERT INTO quiet (id, v) VALUES (1, 'a')`)

	// The churn. Each writer owns a disjoint id block and records only the
	// ids whose INSERT actually returned success — that recorded set is the
	// independent expected value, derived from neither the snapshot nor the
	// position.
	var (
		mu        sync.Mutex
		committed = map[int64]bool{}
		wg        sync.WaitGroup
	)
	churnCtx, stopChurn := context.WithCancel(ctx)
	defer stopChurn()
	for w := 0; w < concurrentGaplessWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			db, oerr := sql.Open("mysql", dsn)
			if oerr != nil {
				return
			}
			defer func() { _ = db.Close() }()
			for i := 0; i < concurrentGaplessRows; i++ {
				select {
				case <-churnCtx.Done():
					return
				default:
				}
				id := int64(w)*1_000_000 + int64(i) + 1
				if _, eerr := db.ExecContext(churnCtx,
					`INSERT INTO churn (id, w) VALUES (?, ?)`, id, w); eerr != nil {
					return
				}
				mu.Lock()
				committed[id] = true
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Let the churn reach steady state, then open the snapshot INSIDE it.
	time.Sleep(500 * time.Millisecond)
	e := Engine{Flavor: FlavorVanilla}
	stream, err := e.openBinlogSnapshotStreamConcurrent(
		ctx, dsn, concurrentGaplessReaders, []string{"churn", "quiet"},
	)
	if err != nil {
		t.Fatalf("openBinlogSnapshotStreamConcurrent: %v", err)
	}
	defer func() { _ = stream.CloseFn() }()

	// Keep churning past the window, then stop and freeze the expected set.
	time.Sleep(500 * time.Millisecond)
	stopChurn()
	wg.Wait()
	mu.Lock()
	want := make(map[int64]bool, len(committed))
	for id := range committed {
		want[id] = true
	}
	mu.Unlock()
	if len(want) < concurrentGaplessWriters {
		t.Fatalf("the churn committed only %d rows; this test proves nothing without contention", len(want))
	}

	rows, ok := stream.Rows.(*concurrentBinlogRows)
	if !ok {
		t.Fatalf("stream.Rows is %T, not *concurrentBinlogRows — the concurrent opener did not engage", stream.Rows)
	}
	churnTable := &ir.Table{
		Name: "churn",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "w", Type: ir.Integer{Width: 32}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	// Every pinned reader must see the SAME cut. Reading on each index in
	// turn uses the work-stealing door, which is correct precisely because
	// all N connections share the one FTWRL cut.
	var (
		snapshot map[int64]bool
		base     = -1
	)
	for i := 0; i < concurrentGaplessReaders; i++ {
		got, rerr := drainChurnIDs(ctx, rows, churnTable, i)
		if rerr != nil {
			t.Fatalf("reader %d: read churn: %v", i, rerr)
		}
		if base < 0 {
			base = len(got)
			snapshot = got
			continue
		}
		if len(got) != base || !sameIDSet(got, snapshot) {
			t.Fatalf("reader %d pinned a DIFFERENT cut than reader 0 (%d rows vs %d). All N consistent-snapshot "+
				"transactions are opened under ONE held FLUSH TABLES WITH READ LOCK precisely so they cannot "+
				"diverge; a divergence means some commit landed in some readers' views and not others, and no "+
				"single recorded position names a cut consistent with all N (ADR-0101 §4's rejected design)",
				i, len(got), base)
		}
	}
	if len(snapshot) == 0 {
		t.Fatal("the snapshot saw zero churn rows — the window fell before any commit, so the gaplessness " +
			"assertion below is vacuous")
	}
	if len(snapshot) == len(want) {
		t.Fatal("the snapshot saw EVERY committed row — the window fell after the churn stopped, so nothing " +
			"had to be recovered from the CDC tail and this test is vacuous")
	}

	// The tail: replay from the ONE recorded position and collect what CDC
	// delivers. Anything committed and not in the snapshot must arrive here.
	tail, err := drainCDCChurnIDs(ctx, stream, want, snapshot)
	if err != nil {
		t.Fatalf("replay the CDC tail from the recorded anchor: %v", err)
	}

	var missing []int64
	for id := range want {
		if snapshot[id] || tail[id] {
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) > 0 {
		t.Fatalf("%d committed rows are in NEITHER the readers' snapshot (%d rows) nor the CDC tail replayed "+
			"from the recorded anchor (%d rows): %v. That is the ADR-0101 §3 gaplessness claim failing — a row "+
			"committed above the frozen read views and below the recorded position is silently lost by every "+
			"cold-start that takes this path (the DEFAULT for a multi-table native-MySQL sync since the "+
			"perf-parity gap-3 chunk)",
			len(missing), len(snapshot), len(tail), missing[:minInt(len(missing), 10)])
	}
	t.Logf("PROVEN gapless under churn: %d committed, %d in the shared snapshot cut, %d recovered from the "+
		"CDC tail, 0 in neither", len(want), len(snapshot), len(tail))
}

// drainChurnIDs reads the churn table on the pinned connection at index i and
// returns the id set that connection's frozen view holds.
//
// It goes through the CHUNKED work-stealing door with an unbounded range and
// chunkIndex = i rather than [concurrentBinlogRows.ReadRowsOn], for a reason
// worth writing down: both doors funnel into the same ADR-0111 resumable read,
// which records a per-cursor-key `complete` marker, and ReadRowsOn keys on the
// bare table name. So reading one table on N connections through that door
// returns the rows once and then N-1 empty channels — which reads exactly like
// "the readers pinned different cuts". Distinct chunk indices give each read
// its own cursor key (workItemCursorKey), which is what makes N reads of one
// table comparable at all.
func drainChurnIDs(ctx context.Context, rows *concurrentBinlogRows, tbl *ir.Table, i int) (map[int64]bool, error) {
	ch, err := rows.ReadRowsRangeOn(ctx, tbl, nil, nil, i, i)
	if err != nil {
		return nil, err
	}
	out := map[int64]bool{}
	for row := range ch {
		id, ok := toInt64(row["id"])
		if !ok {
			return nil, fmt.Errorf("churn row id %v (%T) is not an integer", row["id"], row["id"])
		}
		out[id] = true
	}
	return out, rows.Err()
}

// drainCDCChurnIDs streams the binlog from the snapshot's recorded anchor and
// collects churn-table insert ids until everything `want` holds outside
// `snapshot` has arrived, or the deadline passes. Returning early on
// completeness keeps the happy path fast; the timeout is what turns a genuine
// gap into a failure rather than a hang.
func drainCDCChurnIDs(
	ctx context.Context,
	stream *ir.SnapshotStream,
	want, snapshot map[int64]bool,
) (map[int64]bool, error) {
	outstanding := 0
	for id := range want {
		if !snapshot[id] {
			outstanding++
		}
	}
	streamCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	changes, err := stream.Changes.StreamChanges(streamCtx, stream.Position)
	if err != nil {
		return nil, err
	}
	tail := map[int64]bool{}
	for c := range changes {
		ins, isInsert := c.(ir.Insert)
		if !isInsert || ins.Table != "churn" {
			continue
		}
		id, ok := toInt64(ins.Row["id"])
		if !ok {
			continue
		}
		tail[id] = true
		if !snapshot[id] && want[id] {
			outstanding--
			if outstanding == 0 {
				cancel()
			}
		}
	}
	return tail, nil
}

func sameIDSet(a, b map[int64]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
