// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sqlitedrv "modernc.org/sqlite"     // pure-Go driver; keeps these in the unit gate
	sqlitelib "modernc.org/sqlite/lib" // the driver's SQLITE_* result codes

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The two premises the sqlite-trigger gap-freedom argument rests on, each
// with its own test, because they fail on different inputs and only one of
// them is a property of SQLite.
//
//  1. SQLite serialises writers, so the change-log id is allocated in COMMIT
//     order — which is what licenses the pump's plain `id > lastSeen` scan
//     with no safety-lag predicate (the load-bearing simplification over
//     pgtrigger). [TestChangeLogIDsAreCommitOrderedUnderConcurrentWriters].
//  2. The id, being INTEGER PRIMARY KEY AUTOINCREMENT, is never reused. That
//     is a property of two pieces of ordinary writable state, not of the
//     locking — [TestCDCReader_RefusesChangeLogThatCanReissueIDs] grades the
//     refusal and [TestChangeLog_SequenceTamperStrandsTheChange] is the
//     defect proof that the refusal is worth having.

// openWritable is a small writable-connection helper for these tests.
func openWritable(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// busyRetryAttempts bounds [insertRetryingBusy]. Each attempt already carries
// its own full `busy_timeout` deadline, so exhausting the budget means the
// connection was starved through N CONSECUTIVE deadlines -- which is not a
// contention shape, it is a wedged database, and the test says so out loud.
// 40 attempts, not 10: the v0.122.0 tag run starved ONE writer's first row
// through all 10 attempts on the loaded Windows runner (each attempt races a
// winner's whole remaining batch, so consecutive losses compound), and the
// only honest lever is more headroom — passing runs never feel it, and the
// failure message still names exhaustion loudly.
const busyRetryAttempts = 40

// isSQLiteBusy reports whether err is SQLite's transient lock-contention
// refusal -- the one class a writer is supposed to retry rather than surface.
// Anything else (a constraint violation, a broken trigger, an I/O error) is a
// real defect and must NOT be retried into silence.
func isSQLiteBusy(err error) bool {
	var se *sqlitedrv.Error
	if !errors.As(err, &se) {
		return false
	}
	return se.Code() == sqlitelib.SQLITE_BUSY || se.Code() == sqlitelib.SQLITE_LOCKED
}

// insertRetryingBusy performs one INSERT, retrying while SQLite reports
// SQLITE_BUSY/SQLITE_LOCKED, and returns how many retries it took.
//
// WHY THIS EXISTS, since `busy_timeout` looks like it should already cover it:
// the busy handler is a DEADLINE, not a queue, and it is not fair. It backs off
// in sleeps of up to 100ms, so the connection that holds the write lock drains
// its ENTIRE batch while the losers sleep -- measured on this shape, the insert
// order is exactly [40 40 40 40] on every run, and a single INSERT was observed
// blocking for 1.1s on this box. That is the mechanism: a loser does not wait a
// lock-acquisition's worth of time, it waits a whole batch's worth, so the
// deadline is a race between the timeout and the winner's remaining work. It
// only lowers the PROBABILITY of a loss; it cannot remove it, and the only way
// to keep the caller's assertion exact is to retry. A real application retries
// here too, which is the other reason this belongs in a test whose subject is
// what real writers observe.
//
// The 10s deadline therefore had roughly 10x headroom on the authoring box and
// none to spare on a CI Windows runner about an order of magnitude slower --
// which is precisely where it was first exercised, and where it lost exactly
// one row of 160. Sweeping the deadline on this box reproduces the whole curve:
// 5ms loses 80 rows of 160, 100ms loses 10, and 500ms loses exactly 1, the same
// 159/160 CI reported. The tail never reaches zero; it just moves.
func insertRetryingBusy(ctx context.Context, db *sql.DB, id int64, w int) (retries int, err error) {
	for attempt := 0; attempt < busyRetryAttempts; attempt++ {
		_, err = db.ExecContext(ctx, `INSERT INTO t (id, w) VALUES (?, ?)`, id, w)
		if err == nil {
			return attempt, nil
		}
		if !isSQLiteBusy(err) {
			return attempt, err
		}
		// A growing pause before re-entering a fresh deadline, so a starved
		// connection is not immediately re-queued behind the same winners —
		// capped at 250ms, roughly the scale of a winner draining a batch on
		// a slow runner (the 1–10ms first cut re-queued the loser while the
		// winner was still mid-batch, which is how the v0.122.0 tag run
		// starved one row through ten consecutive deadlines).
		pause := time.Duration(attempt+1) * 10 * time.Millisecond
		if pause > 250*time.Millisecond {
			pause = 250 * time.Millisecond
		}
		time.Sleep(pause)
	}
	return busyRetryAttempts, fmt.Errorf("still SQLITE_BUSY after %d attempts, each with its own 10s deadline: %w",
		busyRetryAttempts, err)
}

// TestChangeLogIDsAreCommitOrderedUnderConcurrentWriters is the serialization
// pin the audit entry asked for. It runs W concurrent writers against one
// SQLite file while the real CDC reader polls, and asserts the two things the
// pump's comment claims: the reader observes every committed change exactly
// once, and it observes them in strictly increasing id order with no id ever
// arriving at or below the watermark it had already passed.
//
// The independent expected value is the set of rows the writer goroutines
// confirmed committed — collected by the test, from neither the change log
// nor the reader.
//
// It is a CONCURRENCY test, and it CHECKS that rather than asserting it: the
// property under test is SQLite's write serialisation, which cannot be observed
// without contention, so the run is graded on whether the writers were actually
// mid-flight at the same time (see the anti-vacuity check below).
//
// The writers RETRY on SQLITE_BUSY ([insertRetryingBusy]) and report anything
// they could not land. `busy_timeout` alone is not enough to keep the exact
// count honest, and finding that out cost a red Windows CI run on the v0.117.0
// tag -- routine pushes are Linux-only and the Windows matrix joins on TAG
// pushes, so any Windows-sensitive test here is first exercised at the worst
// possible moment.
func TestChangeLogIDsAreCommitOrderedUnderConcurrentWriters(t *testing.T) {
	const (
		writers  = 4
		perW     = 40
		expected = writers * perW
	)
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, w INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	var (
		mu        sync.Mutex
		committed = map[int64]bool{}
		lost      []string
		retried   int
		wg        sync.WaitGroup
	)
	// Per-writer first/last write instants, so the run can be GRADED on whether
	// the writers actually ran at the same time. See the anti-vacuity check.
	var (
		starts  = make([]time.Time, writers)
		ends    = make([]time.Time, writers)
		longest time.Duration
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// busy_timeout is how a real application survives SQLite's
			// single-writer lock, and it is NECESSARY BUT NOT SUFFICIENT --
			// see [insertRetryingBusy] for why this loop also retries.
			db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
			if err != nil {
				mu.Lock()
				lost = append(lost, fmt.Sprintf("writer %d never opened its connection: %v", w, err))
				mu.Unlock()
				return
			}
			defer func() { _ = db.Close() }()
			for i := 0; i < perW; i++ {
				id := int64(w)*1_000_000 + int64(i) + 1
				t0 := time.Now()
				retries, eerr := insertRetryingBusy(bg(), db, id, w)
				held := time.Since(t0)
				mu.Lock()
				if i == 0 {
					starts[w] = t0
				}
				ends[w] = time.Now()
				if held > longest {
					longest = held
				}
				switch {
				case eerr != nil:
					lost = append(lost, fmt.Sprintf("row id=%d (writer %d): %v", id, w, eerr))
				default:
					committed[id] = true
					retried += retries
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	// A write this loop could not land is reported with the driver's own error
	// rather than skipped. That distinction is the whole point: before this,
	// the writers swallowed every failure with a bare `continue`, so a lost row
	// from lock contention and a lost row from a genuinely broken INSERT
	// produced the identical "committed N of M" message and neither named the
	// row or the reason.
	if len(lost) > 0 {
		t.Fatalf("%d of %d writes did not land even after the busy-retry budget; the property under test is "+
			"about CONTENDED writes, so a partial run makes this pin weaker than it claims:\n  %s",
			len(lost), expected, strings.Join(lost, "\n  "))
	}
	if len(committed) != expected {
		t.Fatalf("the writers reported no failures but committed %d of %d distinct rows -- the ids this test "+
			"generates are unique by construction, so this means the bookkeeping is wrong, not the database",
			len(committed), expected)
	}

	// ANTI-VACUITY: the doc above calls this a concurrency test, and that is
	// exactly the kind of claim this repo has learned not to leave unchecked.
	// If the writers had in fact run back-to-back, every assertion below would
	// still pass while proving nothing about SQLite's behaviour UNDER
	// CONTENTION. So grade the overlap directly: the latest first-write must
	// precede the earliest last-write, i.e. all W writers were mid-flight at
	// once.
	//
	// Note what canNOT serve as this check, because it is the intuitive choice
	// and it is wrong: the interleaving of writers in the change log. SQLite's
	// busy handler backs off in sleeps of up to 100ms, so the connection
	// holding the lock drains its whole batch while the losers sleep -- the
	// insert order is measured to be exactly [40 40 40 40], four contiguous
	// blocks, on every run of this shape. Blocky order is what MAXIMAL
	// contention looks like here, not an absence of it.
	latestStart, earliestEnd := starts[0], ends[0]
	for w := 0; w < writers; w++ {
		if starts[w].After(latestStart) {
			latestStart = starts[w]
		}
		if ends[w].Before(earliestEnd) {
			earliestEnd = ends[w]
		}
	}
	if !latestStart.Before(earliestEnd) {
		t.Fatalf("the %d writers did not overlap in time (last writer began at %v, first writer had already "+
			"finished at %v), so this run did not actually CONTEND and the serialization property it claims "+
			"to pin was never exercised", writers, latestStart, earliestEnd)
	}
	t.Logf("%d contended commits landed (%d needed a SQLITE_BUSY retry); all %d writers were concurrent for %v, "+
		"and the longest single INSERT blocked for %v against its 10s busy deadline",
		len(committed), retried, writers, earliestEnd.Sub(latestStart), longest)

	r, err := openCDCReader(bg(), path)
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	changes := collect(t, r, pos0(t), expected)
	if len(changes) != expected {
		t.Fatalf("the reader emitted %d changes; want %d. A change captured by the trigger and never emitted "+
			"is the silent-loss shape the `id > lastSeen` poll can produce if ids are not commit-ordered",
			len(changes), expected)
	}

	var (
		prev int64
		seen = map[int64]int{}
	)
	for i, ch := range changes {
		p, ok, derr := decodePos(ch.Pos())
		if derr != nil || !ok {
			t.Fatalf("change[%d] position decode: ok=%v err=%v", i, ok, derr)
		}
		if p.LastID <= prev {
			t.Fatalf("change[%d] arrived with change-log id %d, at or below the watermark %d the reader had "+
				"already passed. The pump scans `id > lastSeen` with NO safety-lag predicate because SQLite "+
				"serialises writers, so a lower id can never commit after a higher one — that is the premise, "+
				"and this is what its failure looks like: the change is captured and never emitted",
				i, p.LastID, prev)
		}
		prev = p.LastID
		ins, isIns := ch.(ir.Insert)
		if !isIns {
			t.Fatalf("change[%d] is %T; want ir.Insert", i, ch)
		}
		rowID, _ := ins.Row["id"].(int64)
		seen[rowID]++
	}
	for id := range committed {
		switch seen[id] {
		case 1:
		case 0:
			t.Errorf("row id=%d committed on the source but was never emitted by the reader", id)
		default:
			t.Errorf("row id=%d was emitted %d times", id, seen[id])
		}
	}
	t.Logf("PROVEN: %d contended commits, %d changes emitted, strictly increasing change-log ids, "+
		"each row exactly once", len(committed), len(changes))
}

// TestChangeLog_SequenceTamperStrandsTheChange is the DEFECT PROOF: it binds
// the two halves that are each plausible alone. It asserts from SQLite itself
// that a lowered `sqlite_sequence` really does re-issue a low id once the log
// is empty, AND from sluice that a poll above that watermark emits nothing —
// so the refusal the gate adds is protecting against a reachable state rather
// than a theoretical one.
//
// Without this, "sqlite_sequence is writable" and "the poll is `id >
// watermark`" are two true facts with nothing joining them.
func TestChangeLog_SequenceTamperStrandsTheChange(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	exec(t, path, `INSERT INTO t (id, n) VALUES (1, 10)`)
	exec(t, path, `INSERT INTO t (id, n) VALUES (2, 20)`)

	db := openWritable(t, path)
	var watermark int64
	if err := db.QueryRowContext(bg(),
		`SELECT COALESCE(MAX(id), 0) FROM "`+ChangeLogTable+`"`).Scan(&watermark); err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if watermark < 2 {
		t.Fatalf("setup produced watermark %d; want >= 2", watermark)
	}

	// The reachable state: the auto-prune drains the applied prefix (which it
	// does on a cadence, by design), and the counter is lowered.
	if _, err := db.ExecContext(bg(), `DELETE FROM "`+ChangeLogTable+`"`); err != nil {
		t.Fatalf("drain change log: %v", err)
	}
	if _, err := db.ExecContext(bg(),
		`UPDATE sqlite_sequence SET seq = 0 WHERE name = ?`, ChangeLogTable); err != nil {
		t.Fatalf("lower sqlite_sequence: %v", err)
	}

	// Half 1, from the SERVER: the next capture really is allocated an id at
	// or below the watermark.
	exec(t, path, `INSERT INTO t (id, n) VALUES (3, 30)`)
	var newID int64
	if err := db.QueryRowContext(bg(),
		`SELECT MAX(id) FROM "`+ChangeLogTable+`"`).Scan(&newID); err != nil {
		t.Fatalf("read re-issued id: %v", err)
	}
	if newID > watermark {
		t.Fatalf("SQLite allocated id %d, above the watermark %d — the tamper did not reproduce, so the "+
			"assertion below would prove nothing", newID, watermark)
	}
	t.Logf("PROVEN half 1: after DELETE + `UPDATE sqlite_sequence SET seq = 0`, SQLite allocated change-log "+
		"id %d, at or below the watermark %d", newID, watermark)

	// Half 2, from SLUICE: a poll above that watermark emits nothing, so the
	// captured change is stranded forever.
	e := &localExecutor{db: openWritable(t, path)}
	rows, err := e.pollChangeLog(bg(), watermark, 100)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("poll(%d) returned %d rows; the stranding did not reproduce", watermark, len(rows))
	}
	t.Logf("PROVEN half 2: poll(%d) emits 0 rows — the captured change is invisible to the stream forever, "+
		"which is why verifyChangeLogWatermark refuses this state at both CDC doors", watermark)
}

// changeLogDoor is one CDC entry point the gate must cover. resume names the
// watermark the door reads: the cold-start snapshot and a "from now" stream
// both re-read MAX(id), while a warm resume carries a persisted position.
type changeLogDoor struct {
	name string
	open func(t *testing.T, path string, resume int64) error
}

func changeLogDoors() []changeLogDoor {
	return []changeLogDoor{
		{
			name: "cold_start_snapshot",
			open: func(t *testing.T, path string, _ int64) error {
				t.Helper()
				s, err := (Engine{}).OpenSnapshotStream(bg(), path)
				if err != nil {
					return err
				}
				return s.CloseFn()
			},
		},
		{
			name: "stream_changes_from_now",
			open: func(t *testing.T, path string, _ int64) error {
				t.Helper()
				r, err := openCDCReader(bg(), path)
				if err != nil {
					return err
				}
				defer func() { _ = r.(interface{ Close() error }).Close() }()
				_, serr := r.StreamChanges(bg(), ir.Position{})
				return serr
			},
		},
		{
			name: "stream_changes_warm_resume",
			open: func(t *testing.T, path string, resume int64) error {
				t.Helper()
				r, err := openCDCReader(bg(), path)
				if err != nil {
					return err
				}
				defer func() { _ = r.(interface{ Close() error }).Close() }()
				pos, perr := encodePos(sqliteTriggerPos{LastID: resume})
				if perr != nil {
					t.Fatalf("encode resume position: %v", perr)
				}
				_, serr := r.StreamChanges(bg(), pos)
				return serr
			},
		},
	}
}

// setupWithChanges builds a change log carrying n captured changes and
// returns (path, watermark).
func setupWithChanges(t *testing.T, n int) (path string, watermark int64) {
	t.Helper()
	path = newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= n; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i*10)
	}
	if err := openWritable(t, path).QueryRowContext(bg(),
		`SELECT COALESCE(MAX(id), 0) FROM "`+ChangeLogTable+`"`).Scan(&watermark); err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	return path, watermark
}

// TestCDCReader_RefusesChangeLogThatCanReissueIDs grades the gate: each state
// that admits an id at or below the watermark, at every CDC door — with the
// door coverage stated per state rather than assumed uniform, and with three
// positive controls so the gate cannot pass by refusing everything.
func TestCDCReader_RefusesChangeLogThatCanReissueIDs(t *testing.T) {
	// The SHADOW CHANGE LOG: a table of the same name that already existed
	// when `sluice trigger setup` ran, declared without AUTOINCREMENT.
	// CREATE TABLE IF NOT EXISTS accepts it and creates nothing (the
	// roadmap item 149b shape). Caught by the DDL arm, which does not depend
	// on the watermark — so it fires at EVERY door, including a cold start
	// whose anchor is 0.
	for _, d := range changeLogDoors() {
		t.Run("no_autoincrement/"+d.name, func(t *testing.T) {
			path := newSourceFile(t,
				`CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`,
				`CREATE TABLE `+ChangeLogTable+` (
					id           INTEGER PRIMARY KEY,
					op           TEXT NOT NULL,
					tbl          TEXT NOT NULL,
					before       TEXT,
					after        TEXT,
					captured_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
				)`)
			if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
				t.Fatalf("Setup: %v", err)
			}
			exec(t, path, `INSERT INTO t (id, n) VALUES (1, 10)`)
			err := d.open(t, path, 1)
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeCDCChangeLogIDReuse {
				t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCChangeLogIDReuse, err, err)
			}
		})
	}

	// The LOWERED SEQUENCE, after the auto-prune drained the log. Its door
	// coverage is deliberately ONE door, and the reason is a property of the
	// hazard rather than of the gate: the other two doors re-read the
	// watermark from MAX(id), so on a drained log they resume at 0 and the
	// re-issued low id is emitted normally — there is nothing to lose and
	// nothing to refuse. The loss needs a PERSISTED watermark above the
	// counter, which is exactly the warm resume.
	t.Run("sequence_lowered_after_prune/stream_changes_warm_resume", func(t *testing.T) {
		path, wm := setupWithChanges(t, 2)
		db := openWritable(t, path)
		if _, err := db.ExecContext(bg(), `DELETE FROM "`+ChangeLogTable+`"`); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if _, err := db.ExecContext(bg(),
			`UPDATE sqlite_sequence SET seq = 0 WHERE name = ?`, ChangeLogTable); err != nil {
			t.Fatalf("lower sqlite_sequence: %v", err)
		}
		err := changeLogDoors()[2].open(t, path, wm)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCChangeLogIDReuse {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCChangeLogIDReuse, err, err)
		}
		t.Logf("refused: %v", err)
	})

	// Control 1: an untouched change log passes every door. Without it, the
	// assertions above are satisfied by a gate that refuses everything.
	for _, d := range changeLogDoors() {
		t.Run("untouched_control/"+d.name, func(t *testing.T) {
			path, wm := setupWithChanges(t, 1)
			if err := d.open(t, path, wm); err != nil {
				t.Fatalf("a correctly-configured change log was refused: %v", err)
			}
		})
	}

	// Control 2: the routine auto-prune. It empties the log on a cadence BY
	// DESIGN, and AUTOINCREMENT keeps sqlite_sequence at the high-water mark
	// — so a drained log must pass a warm resume above its last id. This is
	// the cell that fails if the gate keys on MAX(id) instead of
	// max(MAX(id), seq), which would refuse every healthy pruned stream.
	t.Run("pruned_but_healthy_control", func(t *testing.T) {
		path, wm := setupWithChanges(t, 3)
		if _, err := openWritable(t, path).ExecContext(bg(),
			`DELETE FROM "`+ChangeLogTable+`"`); err != nil {
			t.Fatalf("drain: %v", err)
		}
		e := &localExecutor{db: openWritable(t, path)}
		st, err := e.changeLogAllocation(bg())
		if err != nil {
			t.Fatalf("changeLogAllocation: %v", err)
		}
		if st.floor < wm {
			t.Fatalf("after a full prune the allocation floor is %d; want >= the pre-prune watermark %d "+
				"(sqlite_sequence retains the high-water mark under AUTOINCREMENT)", st.floor, wm)
		}
		if err := verifyChangeLogWatermark(bg(), e, EngineName, wm); err != nil {
			t.Fatalf("a drained-but-healthy change log was refused: %v", err)
		}
		if err := changeLogDoors()[2].open(t, path, wm); err != nil {
			t.Fatalf("a drained-but-healthy change log was refused at the warm-resume door: %v", err)
		}
	})
}

// TestCDCReaderClose_JoinsThePumpBeforeClosingTheExecutor pins the lifecycle
// bug this batch's door-matrix tripped over: [CDCReader.Close] cancelled the
// pump and then closed + NIL'd the executor without waiting for the goroutine
// to exit, so a Close arriving in the window between the pump's select and
// its `r.exec.pollChangeLog` call dereferenced nil INSIDE the goroutine — an
// unrecovered panic that takes the process down, not a failed sync.
//
// The loop is the point: one iteration lands in the window rarely, and a
// fifty-iteration open/close cycle hit it on the first run here. There is no
// assertion body because the failure mode is a panic in another goroutine —
// the test failing IS the panic. Mutation-verified by removing the join.
//
// This is a CONCURRENCY fix: it wants the CI `-race` integration job, which
// grades the unsynchronised r.exec read/write the panic is a symptom of.
func TestCDCReaderClose_JoinsThePumpBeforeClosingTheExecutor(t *testing.T) {
	path, _ := setupWithChanges(t, 2)
	for i := 0; i < 50; i++ {
		r, err := openCDCReader(bg(), path)
		if err != nil {
			t.Fatalf("iteration %d: openCDCReader: %v", i, err)
		}
		if _, err := r.StreamChanges(bg(), ir.Position{}); err != nil {
			t.Fatalf("iteration %d: StreamChanges: %v", i, err)
		}
		if err := r.(interface{ Close() error }).Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
	}
}

// TestChangeLogDeclaresAutoincrement pins the token scan against the shapes
// that would otherwise produce a false positive or a false negative.
func TestChangeLogDeclaresAutoincrement(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{"real_setup_ddl", `CREATE TABLE x (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT)`, true},
		{"lowercase", `create table x (id integer primary key autoincrement)`, true},
		{"plain_rowid_pk", `CREATE TABLE x (id INTEGER PRIMARY KEY, op TEXT)`, false},
		{"no_pk", `CREATE TABLE x (id INTEGER, op TEXT)`, false},
		// A column whose NAME contains the keyword must not read as a
		// declaration — the boundary check is the whole reason this is not a
		// bare strings.Contains.
		{"column_named_like_the_keyword", `CREATE TABLE x (autoincrement_mode TEXT, id INTEGER PRIMARY KEY)`, false},
		{"suffixed_identifier", `CREATE TABLE x (my_autoincrementer TEXT, id INTEGER PRIMARY KEY)`, false},
		{"empty", ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := changeLogDeclaresAutoincrement(tc.sql); got != tc.want {
				t.Errorf("changeLogDeclaresAutoincrement(%q) = %v; want %v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestExecutorImplementorsAllGradeAllocation is the sibling-sweep floor: the
// gate is only as wide as the transports that implement its probe, and the
// engine has two. A compile-time pin is not enough on its own (an
// implementation returning a zero value would satisfy the interface and
// silently disable the gate), so this asserts the LOCAL one reports a real
// floor — the D1 one is covered by the d1_cdc_test.go request-shape mock.
func TestExecutorImplementorsAllGradeAllocation(t *testing.T) {
	var _ executor = (*localExecutor)(nil)
	var _ executor = (*d1Executor)(nil)

	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	exec(t, path, `INSERT INTO t (id, n) VALUES (1, 10)`)

	e := &localExecutor{db: openWritable(t, path)}
	st, err := e.changeLogAllocation(bg())
	if err != nil {
		t.Fatalf("changeLogAllocation: %v", err)
	}
	if !st.autoincrement {
		t.Error("the change log `sluice trigger setup` just created does not read as AUTOINCREMENT — either " +
			"renderSetupDDL changed or the probe cannot see it, and either way the gate is inverted")
	}
	if st.floor < 1 {
		t.Errorf("allocation floor = %d after one captured change; want >= 1 (a zero floor would make the "+
			"watermark comparison vacuous)", st.floor)
	}
}
