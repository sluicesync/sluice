//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// The default-on added-column backfill (ADR-0058 §1c), graded on real
// servers.
//
// # The defect these cells exist for
//
// Django emits `ADD COLUMN c … NOT NULL DEFAULT 'v'` and then
// `ALTER COLUMN c DROP DEFAULT` for every AddField with a default. The
// source fills its pre-existing rows with 'v'; the forward reads the
// column's DEFAULT when the boundary reaches it — after the DROP — so it
// carried none, and every pre-existing TARGET row held NULL (the NOT NULL
// column included, since pgoutput carries no nullability), at exit 0. The
// same holds for any DEFAULT changed later in the window (SET DEFAULT) and
// for a non-constant one (now(), gen_random_uuid(): the target evaluates
// its own). Only reading the source's rows can fix it; the carried DEFAULT
// cannot.
//
// # The independent expected value
//
// The SOURCE's own value for every row that existed at the ALTER, read
// back with SELECT — the pre-existing-row gate's grading (fdGrade: per
// family canonical text, both sessions in UTC). Never sluice's log, never
// the target catalog.
//
// Shard: every test carries the TestStreamer_ prefix (the
// pipeline-rest-streamer shard, `-run ^TestStreamer_`).

// abCell is one source schema change followed by one post-change INSERT
// (the liveness row that carries the boundary past the stream).
type abCell struct {
	col   string
	label string // what the cell is, for the verdict table
	fam   fdFamily
	ddl   []string // run in order, each its own statement / transaction

	// insertValue, when non-empty, is the literal every later INSERT
	// supplies for col — required once a NOT NULL column has no DEFAULT.
	insertValue string
}

// abRun forwards every cell on one stream and grades each cell's
// pre-existing rows against the source.
func abRun(t *testing.T, lane fdLane, cells []abCell) {
	t.Helper()
	s := fdStartStream(t, lane, lane.sourceDSN, lane.targetDSN, "test-ac-backfill")
	defer s.cancel()
	s.waitRows(t, "cold start", fdSeedRows)
	// A row carried by CDC proves the change stream is live before the
	// first ALTER. Without it a MySQL-family reader could take its
	// schema-fact baseline (GC-2) between a cell's ADD COLUMN and its
	// DROP DEFAULT, and refuse the DROP as an unforwarded change — a
	// start-up race of the harness, not the shape under test.
	fdExec(t, lane.src, lane.sourceDSN, abInsert(lane.src, fdSeedRows+1, nil))
	s.waitRows(t, "first CDC row", fdSeedRows+1)

	insertCols := map[string]string{}
	graded := make([]fdCell, 0, len(cells))
	for i, c := range cells {
		for _, stmt := range c.ddl {
			fdExec(t, lane.src, lane.sourceDSN, stmt)
		}
		if c.insertValue != "" {
			insertCols[c.col] = c.insertValue
		}
		time.Sleep(lane.settle)
		id := fdSeedRows + i + 2
		fdExec(t, lane.src, lane.sourceDSN, abInsert(lane.src, id, insertCols))
		s.waitRows(t, c.col+": "+c.label, id)
		graded = append(graded, fdCell{shape: fdShape{col: c.col, def: c.label, fam: c.fam}, maxID: id - 1})
	}
	fdGrade(t, lane, lane.sourceDSN, lane.targetDSN, graded)
	abStopWithoutFalseRefusal(t, s)
}

// abStopWithoutFalseRefusal stops a stream whose backfills all reached the
// target and asserts the stop is clean: the backfill ledger must not raise
// ADD-COLUMN-BACKFILL-INCOMPLETE for work that is durable (measured once on
// a PG → MySQL lane, where the target-tagged persisted position failed to
// order against the source's watermark and every stop refused).
func abStopWithoutFalseRefusal(t *testing.T, s *fdStream) {
	t.Helper()
	s.cancel()
	select {
	case err := <-s.runErr:
		if err != nil && strings.Contains(err.Error(), addColumnBackfillIncompleteMarker) {
			t.Errorf("%s: a stop after every backfill reached the target refused: %v", s.lane.name, err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: Streamer.Run did not return after ctx cancel", s.lane.name)
	}
}

// abInsert is the post-change liveness INSERT for row id, supplying every
// NOT-NULL-without-default column added so far.
func abInsert(d fdDialect, id int, cols map[string]string) string {
	names := []string{"id", "name"}
	vals := []string{fmt.Sprint(id), fmt.Sprintf("'after-%d'", id)}
	keys := make([]string, 0, len(cols))
	for k := range cols {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		names = append(names, fdQuote(d, k))
		vals = append(vals, cols[k])
	}
	return "INSERT INTO w (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
}

// abPGCells is the Postgres-source matrix. Each multi-statement DDL runs as
// one implicit transaction, as a Django migration does on Postgres.
func abPGCells() []abCell {
	return []abCell{
		{
			col: "dj", label: "Django AddField: NOT NULL DEFAULT 'v', then DROP DEFAULT", fam: fdText,
			ddl:         []string{`ALTER TABLE w ADD COLUMN dj TEXT NOT NULL DEFAULT 'v'; ALTER TABLE w ALTER COLUMN dj DROP DEFAULT`},
			insertValue: "'x'",
		},
		{
			col: "ts", label: "DEFAULT now(), then DROP DEFAULT (source's fill instant)", fam: fdDateTime,
			ddl:         []string{`ALTER TABLE w ADD COLUMN ts TIMESTAMPTZ NOT NULL DEFAULT now(); ALTER TABLE w ALTER COLUMN ts DROP DEFAULT`},
			insertValue: "'2001-02-03 04:05:06+00'",
		},
		{
			col: "chg", label: "DEFAULT 'old', then SET DEFAULT 'new'", fam: fdText,
			ddl: []string{`ALTER TABLE w ADD COLUMN chg TEXT DEFAULT 'old'`, `ALTER TABLE w ALTER COLUMN chg SET DEFAULT 'new'`},
		},
		{
			col: "pl", label: "plain constant DEFAULT (regression)", fam: fdText,
			ddl: []string{`ALTER TABLE w ADD COLUMN pl TEXT DEFAULT 'const'`},
		},
		{
			col: "rc", label: "Django shape, then UPDATE of a pre-existing row", fam: fdText,
			ddl: []string{
				`ALTER TABLE w ADD COLUMN rc TEXT NOT NULL DEFAULT 'v'; ALTER TABLE w ALTER COLUMN rc DROP DEFAULT`,
				`UPDATE w SET rc = 'updated' WHERE id = 1`,
			},
			insertValue: "'x'",
		},
	}
}

// abPGUUIDCell: a per-row generated default — every pre-existing row holds
// a DIFFERENT value on the source, which no carried DEFAULT can reproduce.
func abPGUUIDCell() abCell {
	return abCell{
		col: "u", label: "DEFAULT gen_random_uuid(), then DROP DEFAULT (per-row values)", fam: fdText,
		ddl:         []string{`ALTER TABLE w ADD COLUMN u UUID NOT NULL DEFAULT gen_random_uuid(); ALTER TABLE w ALTER COLUMN u DROP DEFAULT`},
		insertValue: "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
	}
}

// abMySQLCells is the MySQL-family source matrix. MySQL DDL autocommits, so
// each statement is its own transaction (as Django's MySQL backend runs it).
func abMySQLCells() []abCell {
	return []abCell{
		{
			col: "dj", label: "Django AddField: NOT NULL DEFAULT 'v', then DROP DEFAULT", fam: fdText,
			ddl:         []string{"ALTER TABLE w ADD COLUMN dj VARCHAR(20) NOT NULL DEFAULT 'v'", "ALTER TABLE w ALTER COLUMN dj DROP DEFAULT"},
			insertValue: "'x'",
		},
		{
			col: "djn", label: "nullable AddField: DEFAULT 'v', then DROP DEFAULT", fam: fdText,
			ddl: []string{"ALTER TABLE w ADD COLUMN djn VARCHAR(20) NULL DEFAULT 'v'", "ALTER TABLE w ALTER COLUMN djn DROP DEFAULT"},
			// MySQL (not MariaDB) leaves a nullable column with NO default after
			// DROP DEFAULT, so a strict-mode INSERT must name it.
			insertValue: "'x'",
		},
		{
			col: "ts", label: "DEFAULT CURRENT_TIMESTAMP(6), then DROP DEFAULT (source's fill instant)", fam: fdDateTime,
			ddl:         []string{"ALTER TABLE w ADD COLUMN ts DATETIME(6) NULL DEFAULT CURRENT_TIMESTAMP(6)", "ALTER TABLE w ALTER COLUMN ts DROP DEFAULT"},
			insertValue: "'2001-02-03 04:05:06'",
		},
		{
			col: "chg", label: "DEFAULT 'old', then SET DEFAULT 'new'", fam: fdText,
			ddl: []string{"ALTER TABLE w ADD COLUMN chg VARCHAR(20) DEFAULT 'old'", "ALTER TABLE w ALTER COLUMN chg SET DEFAULT 'new'"},
		},
		{
			col: "pl", label: "plain constant DEFAULT (regression)", fam: fdText,
			ddl: []string{"ALTER TABLE w ADD COLUMN pl VARCHAR(20) DEFAULT 'const'"},
		},
		{
			col: "rc", label: "nullable Django shape, then UPDATE of a pre-existing row", fam: fdText,
			ddl: []string{
				"ALTER TABLE w ADD COLUMN rc VARCHAR(20) NULL DEFAULT 'v'",
				"ALTER TABLE w ALTER COLUMN rc DROP DEFAULT",
				"UPDATE w SET rc = 'updated' WHERE id = 1",
			},
			insertValue: "'x'",
		},
	}
}

// TestStreamer_AddColumnBackfill_PostgresToPostgres grades the pgoutput →
// Postgres lane.
func TestStreamer_AddColumnBackfill_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	abRun(t, fdLane{
		name: "backfill postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
	}, append(abPGCells(), abPGUUIDCell()))
}

// TestStreamer_AddColumnBackfill_PostgresToMySQL grades the pgoutput →
// MySQL lane.
func TestStreamer_AddColumnBackfill_PostgresToMySQL(t *testing.T) {
	src, _, srcCleanup := startPostgresLogical(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	abRun(t, fdLane{
		name: "backfill postgres->mysql", sourceEngine: "postgres", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdMySQL,
	}, append(abPGCells(), abPGUUIDCell()))
}

// TestStreamer_AddColumnBackfill_MySQLToPostgres grades the MySQL 8 binlog
// → Postgres lane.
func TestStreamer_AddColumnBackfill_MySQLToPostgres(t *testing.T) {
	src, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	abRun(t, fdLane{
		name: "backfill mysql->postgres", sourceEngine: "mysql", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
	}, abMySQLCells())
}

// TestStreamer_AddColumnBackfill_MariaDBToPostgres grades the MariaDB 11.4
// binlog → Postgres lane.
func TestStreamer_AddColumnBackfill_MariaDBToPostgres(t *testing.T) {
	src, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	abRun(t, fdLane{
		name: "backfill mariadb->postgres", sourceEngine: "mariadb", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
	}, abMySQLCells())
}

// TestStreamer_AddColumnBackfill_OptOut_PostgresToPostgres pins
// --no-backfill-added-column: the Django shape's pre-existing target rows
// keep the forwarded ALTER's fill (NULL — the DEFAULT was dropped before
// the boundary was read), exactly the pre-default-on behaviour, and a WARN
// names the column.
func TestStreamer_AddColumnBackfill_OptOut_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	var logs logcapture.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	cells := abPGCells()[:1] // the Django shape
	abRun(t, fdLane{
		name: "opt-out postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
		suppressBackfill: true,
		knownWrong: map[string]fdKnownWrong{
			"dj": {target: fdNull, defect: "opted out: the ALTER's own fill, the DEFAULT having been dropped first"},
		},
	}, cells)
	if !strings.Contains(logs.String(), "backfill suppressed") || !strings.Contains(logs.String(), "dj") {
		t.Errorf("no opt-out WARN naming the forwarded column; WARN log = %q", logs.String())
	}
}

// TestStreamer_AddColumnBackfill_ConcurrentUpdates_PostgresToPostgres is
// the ordering pin (ADR-0058 §1c): source writes to the new column race the
// backfill, which pages a table several pages long while an updater keeps
// rewriting random rows' new column. Every value the backfill writes was
// read after the boundary and is replayed ahead of every change that
// follows it, so each row must end at the source's final value — never at a
// stale backfilled one. The comparison is every row, target against source.
func TestStreamer_AddColumnBackfill_ConcurrentUpdates_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	lane := fdLane{
		name: "race postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
	}
	s := fdStartStream(t, lane, src, tgt, "test-ac-backfill-race")
	defer s.cancel()
	s.waitRows(t, "cold start", fdSeedRows)

	// Several backfill pages (migcore.DefaultBulkBatchSize is 5000).
	const rows = 8000
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w (id, name) SELECT g, 'bulk' FROM generate_series(%d, %d) g`, fdSeedRows+1, rows))
	s.waitRows(t, "bulk rows", rows)

	fdExec(t, fdPG, src, `ALTER TABLE w ADD COLUMN rc TEXT NOT NULL DEFAULT 'v'; ALTER TABLE w ALTER COLUMN rc DROP DEFAULT`)
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w (id, name, rc) VALUES (%d, 'boundary', 'x')`, rows+1))

	// The updater races the backfill: it starts as soon as the boundary
	// row is committed on the source, which is before the stream reaches
	// the boundary.
	db, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	updates := 0
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); updates++ {
		id := 1 + rand.IntN(rows)
		if _, err := db.ExecContext(ctx, `UPDATE w SET rc = $1 WHERE id = $2`, fmt.Sprintf("u%d", updates), id); err != nil {
			t.Fatalf("update: %v", err)
		}
	}
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w (id, name, rc) VALUES (%d, 'sentinel', 'x')`, rows+2))
	s.waitRows(t, "sentinel", rows+2)
	t.Logf("%d concurrent source updates raced the backfill", updates)

	// Every row that existed at the ALTER, target against source.
	fdGrade(t, lane, src, tgt, []fdCell{{shape: fdShape{col: "rc", def: "raced", fam: fdText}, maxID: rows}})
	abStopWithoutFalseRefusal(t, s)
}

// TestStreamer_AddColumnBackfill_ShapeA_TwoShards_PostgresToPostgres is the
// fan-in lane: two shards' streams into one consolidated target, both
// sources holding the SAME primary keys. Each shard's stream must fill only
// its OWN shard's pre-existing rows, from its OWN source.
//
// Shard A's source runs the Django shape first; the target ALTER lands and
// A's backfill runs while shard B's source has no such column. B's rows —
// same ids — must still hold the ALTER's own fill afterwards: a backfill
// that matched on the primary key alone would have written A's values into
// them. Then B runs the same shape with a different fill, and each shard's
// rows must equal its own source's.
func TestStreamer_AddColumnBackfill_ShapeA_TwoShards_PostgresToPostgres(t *testing.T) {
	srcA, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	srcB := fdFreshDB(t, fdPG, srcA, "source_b")
	laneFor := func(src, shard string) fdLane {
		return fdLane{
			name: "shapeA " + shard, sourceEngine: "postgres", targetEngine: "postgres",
			sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
			shardColumn: ShardColumnSpec{Name: "source_shard_id", Value: shard},
		}
	}
	a := fdStartStream(t, laneFor(srcA, "shard_a"), srcA, tgt, "test-ac-backfill-shard-a")
	defer a.cancel()
	a.waitRows(t, "shard_a cold start", fdSeedRows)
	b := fdStartStream(t, laneFor(srcB, "shard_b"), srcB, tgt, "test-ac-backfill-shard-b")
	defer b.cancel()
	b.waitRows(t, "shard_b cold start", 2*fdSeedRows)

	const django = `ALTER TABLE w ADD COLUMN dj TEXT NOT NULL DEFAULT '%s'; ALTER TABLE w ALTER COLUMN dj DROP DEFAULT`
	fdExec(t, fdPG, srcA, fmt.Sprintf(django, "va"))
	fdExec(t, fdPG, srcA, fmt.Sprintf(`INSERT INTO w (id, name, dj) VALUES (%d, 'after', 'x')`, fdSeedRows+1))
	a.waitRows(t, "shard_a boundary", 2*fdSeedRows+1)

	abAssertShardRows(t, tgt, "shard_a", "va")
	abAssertShardRows(t, tgt, "shard_b", fdNull) // untouched by shard_a's backfill

	fdExec(t, fdPG, srcB, fmt.Sprintf(django, "vb"))
	fdExec(t, fdPG, srcB, fmt.Sprintf(`INSERT INTO w (id, name, dj) VALUES (%d, 'after', 'x')`, fdSeedRows+1))
	b.waitRows(t, "shard_b boundary", 2*fdSeedRows+2)

	abAssertShardRows(t, tgt, "shard_a", "va")
	abAssertShardRows(t, tgt, "shard_b", "vb")
	abStopWithoutFalseRefusal(t, a)
	abStopWithoutFalseRefusal(t, b)
}

// TestStreamer_AddColumnBackfill_InterruptedRefusesOnRestart_PostgresToPostgres
// pins the interruption half (schema_forward_backfill_ledger.go). A backfill
// does NOT resume on a restart — the boundary is never seen again — so a
// stream stopped mid-backfill must not come back as if nothing happened.
// MEASURED before the ledger existed: stopped 12 rows into this 60,000-row
// backfill, the restart ran cleanly and left 59,989 pre-existing rows NULL
// at exit 0. Now the interrupted run ends with ADD-COLUMN-BACKFILL-INCOMPLETE
// and the restart refuses with the recorded refusal.
//
// The stop lands inside the backfill: it fires as soon as the first
// backfilled row is visible on the target, and 60,000 backfill UPDATEs
// cannot all commit within one 20 ms poll (the test fails, rather than
// passes vacuously, if the stop ever lands after the backfill finished).
func TestStreamer_AddColumnBackfill_InterruptedRefusesOnRestart_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	const rows = 60000
	fdExec(t, fdPG, src, `CREATE TABLE w (id BIGINT PRIMARY KEY, name VARCHAR(80) NOT NULL)`)
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w SELECT g, 'r' FROM generate_series(1, %d) g`, rows))
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	start := func() (context.CancelFunc, chan error) {
		s := &Streamer{Source: pgEng, Target: pgEng, SourceDSN: src, TargetDSN: tgt, StreamID: "test-ac-backfill-interrupt", SlotName: "test_ac_backfill_interrupt"}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		return cancel, done
	}
	cancel, done := start()
	defer cancel()
	if !waitForPGRowCount(t, tgt, "w", rows, 2*time.Minute) {
		t.Fatal("cold copy never landed")
	}
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w VALUES (%d, 'warm')`, rows+1))
	if !waitForPGRowCount(t, tgt, "w", rows+1, time.Minute) {
		t.Fatal("first CDC row never landed")
	}

	fdExec(t, fdPG, src, `ALTER TABLE w ADD COLUMN dj TEXT NOT NULL DEFAULT 'v'; ALTER TABLE w ALTER COLUMN dj DROP DEFAULT`)
	fdExec(t, fdPG, src, fmt.Sprintf(`INSERT INTO w VALUES (%d, 'boundary', 'x')`, rows+2))
	db, err := sql.Open("pgx", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	// Stop as soon as the first backfilled rows are on the target: the
	// backfill has started applying and cannot have finished.
	deadline := time.Now().Add(time.Minute)
	for {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM w WHERE id <= $1 AND dj IS NOT NULL`, rows+1).Scan(&n); err == nil && n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no backfilled row ever reached the target")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel() // the stop lands inside the backfill
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(time.Minute):
		t.Fatal("Run did not return after the stop")
	}
	var nulls int
	if err := db.QueryRow(`SELECT count(*) FROM w WHERE id <= $1 AND dj IS NULL`, rows+1).Scan(&nulls); err != nil {
		t.Fatalf("count unfilled rows: %v", err)
	}
	t.Logf("stopped with %d of %d pre-existing rows unfilled; Run returned: %v", nulls, rows+1, runErr)
	if nulls == 0 {
		t.Fatal("the stop landed after the backfill completed; the pin did not exercise an interruption")
	}
	if runErr == nil || !strings.Contains(runErr.Error(), addColumnBackfillIncompleteMarker) {
		t.Fatalf("an interrupted backfill returned %v; want %s", runErr, addColumnBackfillIncompleteMarker)
	}

	// The restart refuses on the recorded refusal instead of resuming past
	// the boundary with the rows unfilled.
	_, done2 := start()
	select {
	case runErr = <-done2:
	case <-time.After(2 * time.Minute):
		t.Fatal("the restart did not refuse; it is running past the interrupted backfill")
	}
	var replay *recordedUnforwardedRefusalError
	if !errors.As(runErr, &replay) || !strings.Contains(runErr.Error(), addColumnBackfillIncompleteMarker) {
		t.Fatalf("restart returned %v; want the recorded %s refusal", runErr, addColumnBackfillIncompleteMarker)
	}
}

// abAssertShardRows asserts the consolidated target's pre-existing rows of
// one shard all hold want for dj.
func abAssertShardRows(t *testing.T, tgt, shard, want string) {
	t.Helper()
	db, err := sql.Open("pgx", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id, dj FROM w WHERE source_shard_id = $1 AND id <= $2 ORDER BY id`, shard, fdSeedRows)
	if err != nil {
		t.Fatalf("read %s rows: %v", shard, err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var (
			id int
			v  sql.NullString
		)
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got := fdNull
		if v.Valid {
			got = v.String
		}
		if got != want {
			t.Errorf("%s pre-existing row id=%d: target dj = %s, want %s", shard, id, got, want)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if n != fdSeedRows {
		t.Errorf("%s: %d pre-existing rows on the target, want %d", shard, n, fdSeedRows)
	}
}
