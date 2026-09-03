//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLM-1c, end to end on real containers: the Postgres CDC lane's twin of
// the SLM-1/1b stopped-stream gap. Two pins × {PG 16 → PG 16, PG 16 →
// MySQL 8.0} × {timestamp→timestamptz, timestamptz→timestamp}:
//
//   - (a) a source-only swap performed while the stream was cleanly
//     stopped REFUSES on warm resume — for a table WITH a retained
//     history row (touched before the stop) and, after that table's
//     drained-model recovery, for a table WITHOUT one (never touched
//     before the stop; the target witness is its only prior). Before
//     SLM-1c `checkSchemaRace`'s prior was `relations[OID]`, a cache that
//     belongs to THIS process, so the first RelationMessage after a
//     resume primed silently and every post-swap row landed in the
//     target's other-zone column at exit 0 — measured on the parent
//     commit, recorded in the commit message and in
//     [TestStreamer_PGSource_StoppedStreamZoneSwap]'s failure text.
//   - (b) the drained-model recovery RESUMES: after the operator's target
//     ALTER the seeded prior equals the target's post-ALTER family, so
//     the resumed stream lands the post-swap rows and stays up; no loop.
//   - (c) the override fallback: a `--type-override` on the temporal
//     column means the target's family is the override's, so the witness
//     must not speak for that table — on Postgres this matters MORE than
//     on MySQL, because the seeded check runs at a table's first
//     RelationMessage after a resume, which the first DML triggers with
//     no DDL at all; a phantom swap here would refuse every warm resume
//     of an overridden stream.
//
// # The Postgres mechanism (measured, not copied from the MySQL wording)
//
// `ALTER TABLE … ALTER COLUMN c TYPE timestamptz` on a `timestamp` column
// runs the timestamp→timestamptz assignment cast on every stored value,
// and that cast interprets the stored wall-clock time IN THE EXECUTING
// SESSION's `TimeZone` (PostgreSQL docs §8.5.1.3 / §9.9.4: "conversions
// between timestamp without time zone and timestamp with time zone
// normally assume that the timestamp without time zone value should be
// taken or given as timezone local time"). The reverse converts each
// instant to the session's zone and discards the zone. Measured on
// postgres:16 with `SET TIME ZONE 'Asia/Tokyo'`: a stored `timestamp`
// `2020-01-01 12:00:00` becomes `2020-01-01 03:00:00+00`; a stored
// `timestamptz` `2020-01-01 12:00:00+00` becomes the naive
// `2020-01-01 21:00:00`. (Since PG 12 the pair skips the table rewrite
// when the session's TimeZone is UTC — the same session-defined
// semantics, applied as a no-op.) Nothing on the pgoutput wire carries
// the ALTER session's TimeZone, so the target cannot reproduce the cast;
// the pin asserts both offsets on the SOURCE before any resume so the
// mechanism is ground-truthed by the cell that relies on it.
//
// The independent expected value for every landed row is the SOURCE's
// own rendering of that row under a UTC session — the value sluice
// decodes and the value a correct target holds. For the recovery pins
// the operator's target ALTER runs under the SAME session zone as the
// source's (Asia/Tokyo / +09:00), which is what the refusal text asks of
// them, and the pre-existing rows are asserted to converge — the
// mechanism's converse, pinned on both targets.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pgZoneSwapTarget describes one sync target of the PG-source matrix.
type pgZoneSwapTarget struct {
	name   string
	engine string
	driver string
	// zonedType / naiveType are the information_schema spellings of the
	// zone-family pair on this target.
	zonedType, naiveType string
	// alterZoned / alterNaive render the operator's drained-model ALTER
	// of a table to each member, executed in a session pinned to the
	// same zone the source's ALTER ran under (setTokyo).
	alterZoned, alterNaive func(table string) string
	setTokyo, setUTC       string
	columnTypeQuery        func(table string) string
	rowTextQuery           func(table string) string
	rowExistsQuery         func(table string) string
}

var pgZoneSwapTargets = []pgZoneSwapTarget{
	{
		name: "pg", engine: "postgres", driver: "pgx",
		zonedType: "timestamp with time zone", naiveType: "timestamp without time zone",
		alterZoned: func(tb string) string { return "ALTER TABLE " + tb + " ALTER COLUMN c TYPE timestamptz" },
		alterNaive: func(tb string) string { return "ALTER TABLE " + tb + " ALTER COLUMN c TYPE timestamp" },
		setTokyo:   "SET TIME ZONE 'Asia/Tokyo'",
		setUTC:     "SET TIME ZONE 'UTC'",
		columnTypeQuery: func(tb string) string {
			return "SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='" + tb + "' AND column_name='c'"
		},
		rowTextQuery: func(tb string) string {
			return "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM " + tb + " WHERE id = $1"
		},
		rowExistsQuery: func(tb string) string { return "SELECT COUNT(*) FROM " + tb + " WHERE id = $1" },
	},
	{
		name: "mysql", engine: "mysql", driver: "mysql",
		zonedType: "timestamp", naiveType: "datetime",
		alterZoned: func(tb string) string { return "ALTER TABLE " + tb + " MODIFY c TIMESTAMP NOT NULL" },
		alterNaive: func(tb string) string { return "ALTER TABLE " + tb + " MODIFY c DATETIME NOT NULL" },
		setTokyo:   "SET SESSION time_zone = '+09:00'",
		setUTC:     "SET SESSION time_zone = '+00:00'",
		columnTypeQuery: func(tb string) string {
			return "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '" + tb + "' AND column_name = 'c'"
		},
		// DATE_FORMAT rather than CAST: an unspecified PG precision lands
		// as MySQL (6), and CAST renders the trailing `.000000`.
		rowTextQuery: func(tb string) string {
			return "SELECT DATE_FORMAT(c, '%Y-%m-%d %H:%i:%s') FROM " + tb + " WHERE id = ?"
		},
		rowExistsQuery: func(tb string) string { return "SELECT COUNT(*) FROM " + tb + " WHERE id = ?" },
	},
}

// pgZoneSwapDirection is one direction of the swap on the PG source.
type pgZoneSwapDirection struct {
	name       string
	sourceType string // the column's type at cold start
	swapTo     string // the source operator's ALTER COLUMN TYPE target
	swapToZone bool   // whether swapTo is the zoned member
	// literal is a post-swap row value with the SAME instant as the seed
	// rows under a UTC reading (12:00 UTC / wall clock 12:00).
	literal string
	// seedRowAfterTokyoAlter is the SOURCE's UTC-session rendering of a
	// pre-existing seed row after the ALTER ran under Asia/Tokyo — the
	// measured mechanism (file comment).
	seedRowAfterTokyoAlter string
	// override is the --type-override that puts the TARGET on the
	// opposite family from the source at cold start (pin (c)).
	override string
}

var pgZoneSwapDirections = []pgZoneSwapDirection{
	{
		name: "timestamp→timestamptz", sourceType: "timestamp", swapTo: "timestamptz", swapToZone: true,
		literal: "'2020-01-01 12:00:00+00'", seedRowAfterTokyoAlter: "2020-01-01 03:00:00", override: "timestamptz",
	},
	{
		name: "timestamptz→timestamp", sourceType: "timestamptz", swapTo: "timestamp", swapToZone: false,
		// "datetime" is the override alias for the naive member on every
		// target (translate.mappings; "timestamp" is not an alias).
		literal: "'2020-01-01 12:00:00'", seedRowAfterTokyoAlter: "2020-01-01 21:00:00", override: "datetime",
	},
}

// pgZoneSwapRig is one (target, direction) cell's containers and handles.
type pgZoneSwapRig struct {
	t         *testing.T
	target    pgZoneSwapTarget
	dir       pgZoneSwapDirection
	sourceDSN string
	targetDSN string
	srcEng    ir.Engine
	tgtEng    ir.Engine
	srcDB     *sql.DB
	tgtDB     *sql.DB
}

func newPGZoneSwapRig(t *testing.T, target pgZoneSwapTarget, dir pgZoneSwapDirection) *pgZoneSwapRig {
	t.Helper()
	sourceDSN, pgTargetDSN, cleanupPG := startPostgresLogical(t)
	t.Cleanup(cleanupPG)
	targetDSN := pgTargetDSN
	if target.engine == "mysql" {
		_, mysqlDSN, cleanupMySQL := startMySQLBinlog(t)
		t.Cleanup(cleanupMySQL)
		targetDSN = mysqlDSN
	}
	srcEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	tgtEng, ok := engines.Get(target.engine)
	if !ok {
		t.Fatalf("%s engine not registered", target.engine)
	}
	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	t.Cleanup(func() { _ = srcDB.Close() })
	tgtDB, err := sql.Open(target.driver, targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	t.Cleanup(func() { _ = tgtDB.Close() })

	// Two tables, both with three rows at the instant 12:00 UTC (a
	// `timestamptz` seed) or wall clock 12:00 (a `timestamp` seed).
	// `events` is touched by CDC before the stop, so it carries a retained
	// history row; `quiet` never is, so the target witness is its only
	// possible prior.
	lit := "'2020-01-01 12:00:00'"
	if dir.sourceType == "timestamptz" {
		lit = "'2020-01-01 12:00:00+00'"
	}
	var ddl strings.Builder
	for _, tb := range []string{"events", "quiet"} {
		fmt.Fprintf(&ddl, `
			CREATE TABLE %[1]s (id BIGINT PRIMARY KEY, c %[2]s NOT NULL);
			ALTER TABLE %[1]s REPLICA IDENTITY FULL;
			INSERT INTO %[1]s (id, c) VALUES (1, %[3]s), (2, %[3]s), (3, %[3]s);
		`, tb, dir.sourceType, lit)
	}
	applyDDL(t, sourceDSN, ddl.String())
	return &pgZoneSwapRig{t: t, target: target, dir: dir, sourceDSN: sourceDSN, targetDSN: targetDSN, srcEng: srcEng, tgtEng: tgtEng, srcDB: srcDB, tgtDB: tgtDB}
}

func (r *pgZoneSwapRig) streamer(streamID string) *Streamer {
	return &Streamer{
		Source:    r.srcEng,
		Target:    r.tgtEng,
		SourceDSN: r.sourceDSN,
		TargetDSN: r.targetDSN,
		StreamID:  streamID,
	}
}

func (r *pgZoneSwapRig) run(ctx context.Context, s *Streamer) <-chan error {
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	return errc
}

func (r *pgZoneSwapRig) waitRowCount(table string, n int, timeout time.Duration) bool {
	if r.target.engine == "postgres" {
		return waitForPGRowCount(r.t, r.targetDSN, table, n, timeout)
	}
	return waitForRowCountMySQL(r.t, r.targetDSN, table, n, timeout)
}

func (r *pgZoneSwapRig) waitRowID(table string, id int, timeout time.Duration) bool {
	if r.target.engine == "postgres" {
		return waitForPGRowID(r.t, r.tgtDB, table, id, timeout)
	}
	return waitForMySQLRowID(r.t, r.tgtDB, table, id, timeout)
}

// sessionConn returns a connection on db with `pin` executed on it, so
// every statement the caller runs sees that session setting.
func (r *pgZoneSwapRig) sessionConn(ctx context.Context, db *sql.DB, pin string) *sql.Conn {
	r.t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		r.t.Fatalf("conn: %v", err)
	}
	r.t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(ctx, pin); err != nil {
		r.t.Fatalf("session pin %q: %v", pin, err)
	}
	return conn
}

// sourceRowTextAtUTC is the independent expected value: the source's
// own rendering of the row under a UTC session.
func (r *pgZoneSwapRig) sourceRowTextAtUTC(table string, id int) string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s string
	q := "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM " + table + " WHERE id = $1"
	if err := r.sessionConn(ctx, r.srcDB, "SET TIME ZONE 'UTC'").QueryRowContext(ctx, q, id).Scan(&s); err != nil {
		r.t.Fatalf("source %s row %d: %v", table, id, err)
	}
	return s
}

func (r *pgZoneSwapRig) targetColumnType(table string) string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dt string
	if err := r.tgtDB.QueryRowContext(ctx, r.target.columnTypeQuery(table)).Scan(&dt); err != nil {
		r.t.Fatalf("target %s column type: %v", table, err)
	}
	return strings.ToLower(dt)
}

func (r *pgZoneSwapRig) targetRowTextAtUTC(table string, id int) string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s string
	if err := r.sessionConn(ctx, r.tgtDB, r.target.setUTC).QueryRowContext(ctx, r.target.rowTextQuery(table), id).Scan(&s); err != nil {
		r.t.Fatalf("target %s row %d: %v", table, id, err)
	}
	return s
}

func (r *pgZoneSwapRig) targetRowExists(table string, id int) bool {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := r.tgtDB.QueryRowContext(ctx, r.target.rowExistsQuery(table), id).Scan(&n); err != nil {
		r.t.Fatalf("target %s row %d exists: %v", table, id, err)
	}
	return n > 0
}

// sourceSwapAtTokyo is the operator's source-only ALTER, run in a
// session whose TimeZone is Asia/Tokyo, followed by the post-swap row.
// Returns after asserting the measured mechanism on the source's own
// pre-existing row 1 (file comment).
func (r *pgZoneSwapRig) sourceSwapAtTokyo(table string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := r.sessionConn(ctx, r.srcDB, "SET TIME ZONE 'Asia/Tokyo'")
	stmts := fmt.Sprintf(`
		ALTER TABLE %[1]s ALTER COLUMN c TYPE %[2]s;
		INSERT INTO %[1]s (id, c) VALUES (4, %[3]s);
	`, table, r.dir.swapTo, r.dir.literal)
	if _, err := conn.ExecContext(ctx, stmts); err != nil {
		r.t.Fatalf("source swap on %s: %v", table, err)
	}
	if got := r.sourceRowTextAtUTC(table, 1); got != r.dir.seedRowAfterTokyoAlter {
		r.t.Fatalf("mechanism premise: source %s row 1 reads %q under UTC after the ALTER ran under Asia/Tokyo; want %q — the session-TimeZone cast this refusal exists for did not happen", table, got, r.dir.seedRowAfterTokyoAlter)
	}
	if got, want := r.sourceRowTextAtUTC(table, 4), "2020-01-01 12:00:00"; got != want {
		r.t.Fatalf("source %s row 4 reads %q under UTC; want %q", table, got, want)
	}
}

// operatorTargetAlterAtTokyo is the drained-model step: the same ALTER
// on the target, in a session pinned to the zone the source's ran under.
func (r *pgZoneSwapRig) operatorTargetAlterAtTokyo(table string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := r.sessionConn(ctx, r.tgtDB, r.target.setTokyo)
	alter := r.target.alterNaive(table)
	if r.dir.swapToZone {
		alter = r.target.alterZoned(table)
	}
	if _, err := conn.ExecContext(ctx, alter); err != nil {
		r.t.Fatalf("target ALTER %q: %v", alter, err)
	}
}

func (r *pgZoneSwapRig) swappedTargetType() string {
	if r.dir.swapToZone {
		return r.target.zonedType
	}
	return r.target.naiveType
}

func (r *pgZoneSwapRig) originalTargetType() string {
	if r.dir.swapToZone {
		return r.target.naiveType
	}
	return r.target.zonedType
}

func (r *pgZoneSwapRig) openApplier(ctx context.Context) ir.ChangeApplier {
	r.t.Helper()
	app, err := r.tgtEng.OpenChangeApplier(ctx, r.targetDSN)
	if err != nil {
		r.t.Fatalf("open applier: %v", err)
	}
	r.t.Cleanup(func() { closeIfErrIgnoredForTest(app) })
	return app
}

func (r *pgZoneSwapRig) waitPersistedPosition(streamID string) ir.Position {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pos, ok, err := r.openApplier(ctx).ReadPosition(ctx, streamID)
		cancel()
		if err != nil {
			r.t.Fatalf("read position: %v", err)
		}
		if ok {
			return pos
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.t.Fatal("no persisted position within 30s")
	return ir.Position{}
}

// waitPersistedPositionPast blocks until the persisted position has
// moved off `from` — the last applied transaction's post-commit write
// has landed — so a clean stop resumes AFTER it rather than re-delivering
// it (the SLM-1b rig's lesson, same reason here).
func (r *pgZoneSwapRig) waitPersistedPositionPast(streamID string, from ir.Position) {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if pos := r.waitPersistedPosition(streamID); pos.Token != from.Token {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.t.Fatalf("persisted position did not move past %s within 30s", from.Token)
}

func (r *pgZoneSwapRig) historyRowsFor(streamID, table string) int {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reader, ok := r.openApplier(ctx).(ir.SchemaHistoryReader)
	if !ok {
		r.t.Fatalf("%s applier does not implement ir.SchemaHistoryReader", r.target.engine)
	}
	rows, err := reader.ListSchemaHistory(ctx, streamID, 100)
	if err != nil {
		r.t.Fatalf("list schema history: %v", err)
	}
	n := 0
	for _, row := range rows {
		if row.TableName == table {
			n++
		}
	}
	return n
}

func (r *pgZoneSwapRig) assertNoSkippedTables(streamID string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lister, ok := r.openApplier(ctx).(ir.SkippedTableLister)
	if !ok {
		r.t.Fatalf("%s applier does not implement ir.SkippedTableLister", r.target.engine)
	}
	records, err := lister.ListSkippedTables(ctx)
	if err != nil {
		r.t.Fatalf("list skipped tables: %v", err)
	}
	for _, rec := range records {
		if rec.StreamID == streamID && rec.SkipCount > 0 {
			r.t.Errorf("sluice_cdc_skipped_tables holds %s (%d events) after the recovery; want the ledger empty", rec.Table, rec.SkipCount)
		}
	}
}

// expectRefusal waits for the streamer to surface the session-TimeZone
// refusal naming `table`; on a stream that keeps running instead it
// reports what the gap looks like on the target — the measurement the
// parent commit produced.
func (r *pgZoneSwapRig) expectRefusal(errc <-chan error, table, what string) {
	r.t.Helper()
	var err error
	select {
	case err = <-errc:
	case <-time.After(90 * time.Second):
		landed := "absent"
		if r.targetRowExists(table, 4) {
			landed = fmt.Sprintf("LANDED (reads %q under UTC; the source reads %q)", r.targetRowTextAtUTC(table, 4), r.sourceRowTextAtUTC(table, 4))
		}
		r.t.Fatalf("%s: streamer did not surface the session-TimeZone refusal within 90s — the swap on %s was primed; target column is still %q (source is %s), post-swap row 4 %s, pre-existing row 1 reads %q on the target vs %q on the source",
			what, table, r.targetColumnType(table), r.dir.swapTo, landed, r.targetRowTextAtUTC(table, 1), r.sourceRowTextAtUTC(table, 1))
	}
	if err == nil {
		r.t.Fatalf("%s: streamer returned nil; want the session-TimeZone cast refusal on %s", what, table)
	}
	for _, want := range []string{"cannot be forwarded", "public." + table, `column "c"`, "TimeZone", "while the stream was stopped", "drained model"} {
		if !strings.Contains(err.Error(), want) {
			r.t.Errorf("%s: stream error missing %q; got: %v", what, want, err)
		}
	}
}

func (r *pgZoneSwapRig) expectStillRunning(errc <-chan error, what string) {
	r.t.Helper()
	select {
	case err := <-errc:
		r.t.Fatalf("%s: streamer exited with %v; want it still streaming", what, err)
	case <-time.After(3 * time.Second):
	}
}

func (r *pgZoneSwapRig) stop(cancel context.CancelFunc, errc <-chan error) {
	r.t.Helper()
	cancel()
	select {
	case <-errc:
	case <-time.After(30 * time.Second):
		r.t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_PGSource_StoppedStreamZoneSwap is pins (a) and (b) in one
// flow per cell: cold start → one CDC transaction on `events` → clean
// stop → the source-only swap on BOTH tables under Asia/Tokyo → resume
// REFUSES naming `events` (history row + witness) → the operator's
// target ALTER on `events` → resume lands `events` and REFUSES naming
// `quiet` (witness only) → the operator's target ALTER on `quiet` →
// resume lands everything and stays up, every row equal to the source's
// own UTC reading, the skip ledger empty.
func TestStreamer_PGSource_StoppedStreamZoneSwap(t *testing.T) {
	for _, target := range pgZoneSwapTargets {
		for _, dir := range pgZoneSwapDirections {
			t.Run(target.name+"/"+dir.name, func(t *testing.T) {
				rig := newPGZoneSwapRig(t, target, dir)
				streamID := "slm1c-" + target.name

				// ---- Run 1: cold start, one applied CDC transaction on
				// events, a clean stop after its post-commit position write.
				ctx1, cancel1 := context.WithCancel(context.Background())
				defer cancel1()
				errc1 := rig.run(ctx1, rig.streamer(streamID))
				if !rig.waitRowCount("events", 3, 90*time.Second) || !rig.waitRowCount("quiet", 3, 90*time.Second) {
					t.Fatal("bulk-copy never landed the seed rows")
				}
				anchor := rig.waitPersistedPosition(streamID)
				applyDDL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (100, "+dir.literalForSource()+");")
				if !rig.waitRowID("events", 100, 90*time.Second) {
					t.Fatal("the CDC row never landed")
				}
				rig.waitPersistedPositionPast(streamID, anchor)
				rig.stop(cancel1, errc1)
				// Anti-vacuity on the two priors: events has a retained
				// history row (pgoutput's first RelationMessage per process
				// is a true-delta boundary), quiet has none.
				if n := rig.historyRowsFor(streamID, "events"); n == 0 {
					t.Fatal("anti-vacuity: no retained history row for events; this cell must resume with BOTH priors for it")
				}
				if n := rig.historyRowsFor(streamID, "quiet"); n != 0 {
					t.Fatalf("anti-vacuity: %d retained history row(s) for quiet; this cell must resume with the target witness as quiet's ONLY prior", n)
				}

				// ---- The source-only swap while the stream is stopped,
				// on both tables, plus the row that would land in the
				// mismatched column.
				rig.sourceSwapAtTokyo("events")
				rig.sourceSwapAtTokyo("quiet")

				// ---- Run 2: refuses on events.
				ctx2, cancel2 := context.WithCancel(context.Background())
				defer cancel2()
				errc2 := rig.run(ctx2, rig.streamer(streamID))
				rig.expectRefusal(errc2, "events", "warm resume after the swap")
				for _, tb := range []string{"events", "quiet"} {
					if got := rig.targetColumnType(tb); got != rig.originalTargetType() {
						t.Errorf("target %s.c is %q after the refusal; want %q untouched", tb, got, rig.originalTargetType())
					}
					if rig.targetRowExists(tb, 4) {
						t.Errorf("the post-swap row landed on %s in the zone-mismatched column", tb)
					}
				}

				// ---- The operator follows the hint on events; the
				// resumed stream passes events and refuses on quiet — the
				// table with NO history row, whose only prior is the
				// target witness.
				rig.operatorTargetAlterAtTokyo("events")
				ctx3, cancel3 := context.WithCancel(context.Background())
				defer cancel3()
				errc3 := rig.run(ctx3, rig.streamer(streamID))
				rig.expectRefusal(errc3, "quiet", "warm resume after the operator's ALTER on events")
				if got := rig.targetColumnType("quiet"); got != rig.originalTargetType() {
					t.Errorf("target quiet.c is %q after the refusal; want %q untouched", got, rig.originalTargetType())
				}
				if rig.targetRowExists("quiet", 4) {
					t.Error("the post-swap row landed on quiet in the zone-mismatched column")
				}

				// ---- The operator follows the hint on quiet; the resumed
				// stream lands both post-swap rows and stays up.
				rig.operatorTargetAlterAtTokyo("quiet")
				ctx4, cancel4 := context.WithCancel(context.Background())
				defer cancel4()
				errc4 := rig.run(ctx4, rig.streamer(streamID))
				for _, tb := range []string{"events", "quiet"} {
					if !rig.waitRowID(tb, 4, 90*time.Second) {
						select {
						case err := <-errc4:
							t.Fatalf("warm resume refused after the operator's target ALTERs — the recovery loop: %v", err)
						default:
						}
						t.Fatalf("the post-swap row never landed on %s after the recovery", tb)
					}
				}
				rig.expectStillRunning(errc4, "recovered stream")
				for _, tb := range []string{"events", "quiet"} {
					if got := rig.targetColumnType(tb); got != rig.swappedTargetType() {
						t.Errorf("target %s.c is %q after the recovery; want %q", tb, got, rig.swappedTargetType())
					}
					for _, id := range []int{1, 4} {
						if got, want := rig.targetRowTextAtUTC(tb, id), rig.sourceRowTextAtUTC(tb, id); got != want {
							t.Errorf("target %s row %d reads %q under UTC; want the source's own reading %q", tb, id, got, want)
						}
					}
				}
				rig.assertNoSkippedTables(streamID)
				rig.stop(cancel4, errc4)
			})
		}
	}
}

// literalForSource is a row value at the seed instant in the column's
// ORIGINAL type.
func (d pgZoneSwapDirection) literalForSource() string {
	if d.sourceType == "timestamptz" {
		return "'2020-01-01 12:00:00+00'"
	}
	return "'2020-01-01 12:00:00'"
}

// TestStreamer_PGSource_OverrideFallsBackToHistory is pin (c): with a
// `--type-override` putting the target on the opposite zone family, a
// warm resume followed by plain DML — no DDL anywhere — must NOT refuse.
// On Postgres the seeded check runs at the first RelationMessage after a
// resume, which the first DML triggers, so a witness that spoke for an
// overridden table would refuse every warm resume of that stream.
func TestStreamer_PGSource_OverrideFallsBackToHistory(t *testing.T) {
	target := pgZoneSwapTargets[0] // Postgres target: where an override changes the family.
	for _, dir := range pgZoneSwapDirections {
		t.Run(dir.name, func(t *testing.T) {
			rig := newPGZoneSwapRig(t, target, dir)
			streamID := "slm1c-override"
			override := []config.Mapping{{Table: "events", Column: "c", TargetType: dir.override}}
			s1 := rig.streamer(streamID)
			s1.Mappings = override
			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()
			errc1 := rig.run(ctx1, s1)
			if !rig.waitRowCount("events", 3, 90*time.Second) {
				select {
				case err := <-errc1:
					t.Fatalf("cold start with the override failed: %v", err)
				default:
				}
				t.Fatal("bulk-copy never landed the seed rows")
			}
			// The override put the target on the OPPOSITE family: the
			// witness, if consulted, would read the next RelationMessage
			// as a swap the operator never made.
			if got := rig.targetColumnType("events"); got != rig.swappedTargetType() {
				t.Fatalf("anti-vacuity: target events.c is %q; want the override's %q", got, rig.swappedTargetType())
			}
			anchor := rig.waitPersistedPosition(streamID)
			applyDDL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (100, "+dir.literalForSource()+");")
			if !rig.waitRowID("events", 100, 90*time.Second) {
				t.Fatal("the CDC row never landed")
			}
			rig.waitPersistedPositionPast(streamID, anchor)
			rig.stop(cancel1, errc1)

			// Plain DML after the stop: the first RelationMessage of the
			// resumed process carries the UNCHANGED source shape.
			applyDDL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (4, "+dir.literalForSource()+");")
			s2 := rig.streamer(streamID)
			s2.Mappings = override
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			errc2 := rig.run(ctx2, s2)
			if !rig.waitRowID("events", 4, 90*time.Second) {
				select {
				case err := <-errc2:
					t.Fatalf("warm resume with a --type-override on the temporal column refused plain DML — the witness spoke for an overridden table: %v", err)
				default:
				}
				t.Fatal("the post-resume row never landed")
			}
			rig.expectStillRunning(errc2, "overridden stream")
			rig.stop(cancel2, errc2)
		})
	}
}
