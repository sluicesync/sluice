//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLM-1b, end to end on real containers: the warm-resume prior for the
// session-`time_zone` cast refusal is the TARGET's current schema, with the
// retained history as fallback. Three pins, each × {MySQL 8.0 → PG 16,
// MySQL → MySQL} × {DATETIME→TIMESTAMP, TIMESTAMP→DATETIME}:
//
//   - (a) the recovery loop EXITS: a table WITH a history row refuses the
//     swap, the operator follows the hint (stop, ALTER the target, `sync
//     start` on the same stream), and the replayed DDL passes — the
//     post-DDL row lands, the skip ledger stays empty. Before SLM-1b the
//     history prior refused again, forever.
//   - (b) the residual is CLOSED: a table WITHOUT a history row (a clean
//     stop, a source-only swap, `sync start`) refuses on warm resume.
//     Before SLM-1b the boundary primed and the post-DDL rows landed in the
//     zone-mismatched column at exit 0.
//   - (c) the override fallback: a `--type-override` on the temporal
//     column means the target's family is the override's, not the
//     source's; the witness must not speak for that table, so a plain DDL
//     after warm resume is not refused as a phantom swap. The DDL is an
//     index-only ALTER: it drives the binlog reader's boundary path (cache
//     clear → rebuild → prior check) without needing a forward, because
//     the first DDL after a MySQL warm resume is a cache PRIME in the
//     forward intercept (no seed on that path) — a separate, pre-existing,
//     loud gap (a post-ADD-COLUMN row refuses with the schema-drift hint),
//     not this chunk's.
//
// Plus the written invariant the recovery relies on ("a same-type ALTER is
// a fast no-op" — postgres/mysql SchemaWriter.AlterColumnType): pinned by
// running it against the recovered target and reading the row back.
//
// The independent expected value for every post-DDL row is the SOURCE's
// own reading of that row under +00:00 — the value sluice decodes and the
// value a correct target holds. The source runs at --default-time-zone=
// +09:00 (startMySQLBinlogAtTokyo), so the swap genuinely re-zones.

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

// zoneWitnessTarget describes one sync target of the matrix.
type zoneWitnessTarget struct {
	name   string
	engine string
	driver string
	// zonedType / naiveType are the information_schema spellings of the
	// zone-family pair on this target; alterZoned / alterNaive the
	// operator's drained-model ALTER to each.
	zonedType, naiveType   string
	alterZoned, alterNaive string
	// columnTypeQuery / rowTextQuery read the column's type and a row's
	// wall-clock rendering under a UTC session.
	columnTypeQuery, rowTextQuery, setUTC string
	// zonedIR / naiveIR are the seed types the SchemaWriter's
	// AlterColumnType is handed for the no-op pin.
	zonedIR, naiveIR ir.Type
}

var zoneWitnessTargets = []zoneWitnessTarget{
	{
		name: "pg", engine: "postgres", driver: "pgx",
		zonedType: "timestamp with time zone", naiveType: "timestamp without time zone",
		alterZoned:      "ALTER TABLE events ALTER COLUMN c TYPE timestamptz",
		alterNaive:      "ALTER TABLE events ALTER COLUMN c TYPE timestamp",
		columnTypeQuery: "SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='events' AND column_name='c'",
		rowTextQuery:    "SELECT to_char(c, 'YYYY-MM-DD HH24:MI:SS') FROM events WHERE id = $1",
		setUTC:          "SET TIME ZONE 'UTC'",
		zonedIR:         ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true},
		naiveIR:         ir.DateTime{PrecisionUnspecified: true},
	},
	{
		name: "mysql", engine: "mysql", driver: "mysql",
		zonedType: "timestamp", naiveType: "datetime",
		alterZoned:      "ALTER TABLE events MODIFY c TIMESTAMP NOT NULL",
		alterNaive:      "ALTER TABLE events MODIFY c DATETIME NOT NULL",
		columnTypeQuery: "SELECT DATA_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'events' AND column_name = 'c'",
		rowTextQuery:    "SELECT CAST(c AS CHAR) FROM events WHERE id = ?",
		setUTC:          "SET SESSION time_zone = '+00:00'",
		zonedIR:         ir.Timestamp{WithTimeZone: true},
		naiveIR:         ir.DateTime{},
	},
}

// zoneWitnessDirection is one direction of the swap.
type zoneWitnessDirection struct {
	name       string
	sourceType string // the column's type at cold start
	swapTo     string // the source operator's MODIFY target
	swapToZone bool   // whether swapTo is the zoned member
	// override is the --type-override that puts the TARGET on the
	// opposite family from the source at cold start (pin (c)).
	override string
}

var zoneWitnessDirections = []zoneWitnessDirection{
	{name: "DATETIME→TIMESTAMP", sourceType: "DATETIME", swapTo: "TIMESTAMP", swapToZone: true, override: "timestamptz"},
	{name: "TIMESTAMP→DATETIME", sourceType: "TIMESTAMP", swapTo: "DATETIME", swapToZone: false, override: "datetime"},
}

// zoneWitnessRig is one (target, direction) cell's containers and handles.
type zoneWitnessRig struct {
	t         *testing.T
	target    zoneWitnessTarget
	dir       zoneWitnessDirection
	sourceDSN string
	targetDSN string
	srcEng    ir.Engine
	tgtEng    ir.Engine
	tgtDB     *sql.DB
}

func newZoneWitnessRig(t *testing.T, target zoneWitnessTarget, dir zoneWitnessDirection) *zoneWitnessRig {
	t.Helper()
	sourceDSN, _, cleanupMySQL := startMySQLBinlogAtTokyo(t)
	t.Cleanup(cleanupMySQL)
	// The target is a SEPARATE server on purpose, for MySQL too. On one
	// shared server the target applier's own control-table commits are
	// out-of-scope transactions the source reader still folds positions
	// through, so the persisted position ran past the in-scope DDL before
	// its deferred boundary was reached and the DDL never replayed on
	// resume — the cells passed without ever consulting a prior (found by
	// the M1 mutation run; a cross-server rig is the production shape).
	var targetDSN string
	if target.engine == "postgres" {
		_, pgDSN, cleanupPG := startPostgresLogical(t)
		t.Cleanup(cleanupPG)
		targetDSN = pgDSN
	} else {
		_, mysqlDSN, cleanupTarget := startMySQLBinlog(t)
		t.Cleanup(cleanupTarget)
		targetDSN = mysqlDSN
	}
	srcEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	tgtEng, ok := engines.Get(target.engine)
	if !ok {
		t.Fatalf("%s engine not registered", target.engine)
	}
	tgtDB, err := sql.Open(target.driver, targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	t.Cleanup(func() { _ = tgtDB.Close() })

	// Rows at Tokyo wall clock 21:00. A TIMESTAMP source stores them as
	// 12:00 UTC; a DATETIME source stores the wall clock.
	applyDDLMySQL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE events (
			id BIGINT NOT NULL PRIMARY KEY,
			c  %s NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO events (id, c) VALUES
			(1, '2020-01-01 21:00:00'),
			(2, '2020-01-01 21:00:00'),
			(3, '2020-01-01 21:00:00');
	`, dir.sourceType))
	return &zoneWitnessRig{t: t, target: target, dir: dir, sourceDSN: sourceDSN, targetDSN: targetDSN, srcEng: srcEng, tgtEng: tgtEng, tgtDB: tgtDB}
}

func (r *zoneWitnessRig) streamer(streamID string) *Streamer {
	return &Streamer{
		Source:    r.srcEng,
		Target:    r.tgtEng,
		SourceDSN: r.sourceDSN,
		TargetDSN: r.targetDSN,
		StreamID:  streamID,
	}
}

// run starts a streamer and returns its error channel.
func (r *zoneWitnessRig) run(ctx context.Context, s *Streamer) <-chan error {
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	return errc
}

func (r *zoneWitnessRig) waitRowCount(n int, timeout time.Duration) bool {
	if r.target.engine == "postgres" {
		return waitForPGRowCount(r.t, r.targetDSN, "events", n, timeout)
	}
	return waitForRowCountMySQL(r.t, r.targetDSN, "events", n, timeout)
}

func (r *zoneWitnessRig) waitRowID(id int, timeout time.Duration) bool {
	if r.target.engine == "postgres" {
		return waitForPGRowID(r.t, r.tgtDB, "events", id, timeout)
	}
	return waitForMySQLRowID(r.t, r.tgtDB, "events", id, timeout)
}

// utcConn is a target connection pinned to UTC for renderings.
func (r *zoneWitnessRig) utcConn(ctx context.Context) *sql.Conn {
	r.t.Helper()
	conn, err := r.tgtDB.Conn(ctx)
	if err != nil {
		r.t.Fatalf("target conn: %v", err)
	}
	r.t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(ctx, r.target.setUTC); err != nil {
		r.t.Fatalf("pin target session to UTC: %v", err)
	}
	return conn
}

func (r *zoneWitnessRig) targetColumnType() string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dt string
	if err := r.utcConn(ctx).QueryRowContext(ctx, r.target.columnTypeQuery).Scan(&dt); err != nil {
		r.t.Fatalf("target column type: %v", err)
	}
	return strings.ToLower(dt)
}

func (r *zoneWitnessRig) targetRowText(id int) string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s string
	if err := r.utcConn(ctx).QueryRowContext(ctx, r.target.rowTextQuery, id).Scan(&s); err != nil {
		r.t.Fatalf("target row %d: %v", id, err)
	}
	return s
}

func (r *zoneWitnessRig) targetRowExists(id int) bool {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := "SELECT COUNT(*) FROM events WHERE id = ?"
	if r.target.engine == "postgres" {
		q = "SELECT COUNT(*) FROM events WHERE id = $1"
	}
	var n int
	if err := r.tgtDB.QueryRowContext(ctx, q, id).Scan(&n); err != nil {
		r.t.Fatalf("target row %d exists: %v", id, err)
	}
	return n > 0
}

// sourceRowTextAtUTC is the independent expected value: the source's own
// reading of the row under +00:00.
func (r *zoneWitnessRig) sourceRowTextAtUTC(id int) string {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", r.sourceDSN)
	if err != nil {
		r.t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		r.t.Fatalf("source conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
		r.t.Fatalf("source time_zone: %v", err)
	}
	var s string
	if err := conn.QueryRowContext(ctx, "SELECT CAST(c AS CHAR) FROM events WHERE id = ?", id).Scan(&s); err != nil {
		r.t.Fatalf("source row %d: %v", id, err)
	}
	return s
}

func (r *zoneWitnessRig) applyTargetDDL(stmt string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := r.tgtDB.ExecContext(ctx, stmt); err != nil {
		r.t.Fatalf("target DDL %q: %v", stmt, err)
	}
}

// openApplier opens a target applier for the control-table reads.
func (r *zoneWitnessRig) openApplier(ctx context.Context) ir.ChangeApplier {
	r.t.Helper()
	app, err := r.tgtEng.OpenChangeApplier(ctx, r.targetDSN)
	if err != nil {
		r.t.Fatalf("open applier: %v", err)
	}
	r.t.Cleanup(func() {
		if c, ok := app.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return app
}

// waitPersistedPosition polls the stream's persisted position until one
// exists (the cold-start anchor lands a moment after the bulk copy).
func (r *zoneWitnessRig) waitPersistedPosition(streamID string) ir.Position {
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

// waitPersistedPositionPast blocks until the persisted position has moved
// off `from` — i.e. the last applied transaction's POST-commit position
// write has landed. A cancel that lands between a row's apply and that
// write leaves the pre-commit position persisted, and the resumed stream
// then RE-DELIVERS the transaction under its recorded (pre-swap) shape,
// which trips the CDC-4 table-map guard before this chunk's refusal is
// reached — a different, loud door. A clean stop must not race the write,
// or the cell measures that guard instead of the witness.
func (r *zoneWitnessRig) waitPersistedPositionPast(streamID string, from ir.Position) {
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

func (r *zoneWitnessRig) historyRowsFor(streamID, table string) int {
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

func (r *zoneWitnessRig) assertNoSkippedTables(streamID string) {
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

// expectRefusal waits for the streamer to surface the session-time_zone
// refusal, and fails loudly if it keeps running instead.
func (r *zoneWitnessRig) expectRefusal(errc <-chan error, what string) {
	r.t.Helper()
	var err error
	select {
	case err = <-errc:
	case <-time.After(90 * time.Second):
		r.t.Fatalf("%s: streamer did not surface the session-time_zone refusal within 90s — the swap was primed or forwarded", what)
	}
	if err == nil {
		r.t.Fatalf("%s: streamer returned nil; want the session-time_zone cast refusal", what)
	}
	for _, want := range []string{"cannot be forwarded", `column "c"`, "time_zone", "drained model"} {
		if !strings.Contains(err.Error(), want) {
			r.t.Errorf("%s: stream error missing %q; got: %v", what, want, err)
		}
	}
}

// expectStillRunning asserts the streamer has NOT exited: a settled
// stream that survived the boundary.
func (r *zoneWitnessRig) expectStillRunning(errc <-chan error, what string) {
	r.t.Helper()
	select {
	case err := <-errc:
		r.t.Fatalf("%s: streamer exited with %v; want it still streaming", what, err)
	case <-time.After(3 * time.Second):
	}
}

func (r *zoneWitnessRig) stop(cancel context.CancelFunc, errc <-chan error) {
	r.t.Helper()
	cancel()
	select {
	case <-errc:
	case <-time.After(30 * time.Second):
		r.t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

func (r *zoneWitnessRig) swappedTargetType() string {
	if r.dir.swapToZone {
		return r.target.zonedType
	}
	return r.target.naiveType
}

func (r *zoneWitnessRig) originalTargetType() string {
	if r.dir.swapToZone {
		return r.target.naiveType
	}
	return r.target.zonedType
}

func (r *zoneWitnessRig) operatorAlter() string {
	if r.dir.swapToZone {
		return r.target.alterZoned
	}
	return r.target.alterNaive
}

func (r *zoneWitnessRig) swappedIR() ir.Type {
	if r.dir.swapToZone {
		return r.target.zonedIR
	}
	return r.target.naiveIR
}

func TestStreamer_SessionTZWitness_RecoveryLoopExits(t *testing.T) {
	for _, target := range zoneWitnessTargets {
		for _, dir := range zoneWitnessDirections {
			t.Run(target.name+"/"+dir.name, func(t *testing.T) {
				rig := newZoneWitnessRig(t, target, dir)
				streamID := "slm1b-loop-" + target.name
				ctx1, cancel1 := context.WithCancel(context.Background())
				defer cancel1()
				errc1 := rig.run(ctx1, rig.streamer(streamID))
				if !rig.waitRowCount(3, 90*time.Second) {
					t.Fatal("bulk-copy never landed the seed rows")
				}

				// A forwarded ADD COLUMN writes the table's history row —
				// the precondition of the loop.
				applyDDLMySQL(t, rig.sourceDSN, "ALTER TABLE events ADD COLUMN note INT;")
				applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c, note) VALUES (100, '2020-01-01 21:00:00', 1);")
				if !rig.waitRowID(100, 90*time.Second) {
					t.Fatal("the post-ADD-COLUMN row never landed; the forward path is not live")
				}
				if n := rig.historyRowsFor(streamID, "events"); n == 0 {
					t.Fatal("anti-vacuity: no retained history row for events after the forwarded ADD COLUMN — this cell would not exercise the loop")
				}

				// THE SWAP, then the row the loop would strand.
				applyDDLMySQL(t, rig.sourceDSN, fmt.Sprintf("ALTER TABLE events MODIFY c %s NOT NULL;", dir.swapTo))
				applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c, note) VALUES (4, '2020-01-01 21:00:00', 4);")
				rig.expectRefusal(errc1, "first run")
				if got := rig.targetColumnType(); got != rig.originalTargetType() {
					t.Fatalf("target events.c is %q after the refusal; want %q untouched", got, rig.originalTargetType())
				}
				if rig.targetRowExists(4) {
					t.Fatal("the post-swap row was applied before the refusal")
				}

				// The operator follows the hint: the same ALTER on the
				// target, then `sync start` on the SAME stream.
				rig.applyTargetDDL(rig.operatorAlter())
				ctx2, cancel2 := context.WithCancel(context.Background())
				defer cancel2()
				errc2 := rig.run(ctx2, rig.streamer(streamID))
				if !rig.waitRowID(4, 90*time.Second) {
					select {
					case err := <-errc2:
						t.Fatalf("warm resume refused again after the operator's target ALTER — the recovery loop (SLM-1b gap 1): %v", err)
					default:
					}
					t.Fatal("the post-swap row never landed after the recovery")
				}
				rig.expectStillRunning(errc2, "recovered stream")
				if got, want := rig.targetRowText(4), rig.sourceRowTextAtUTC(4); got != want {
					t.Errorf("target row 4 reads %q; want the source's own reading under +00:00, %q", got, want)
				}
				rig.assertNoSkippedTables(streamID)
				rig.stop(cancel2, errc2)

				// The invariant the recovery leans on, pinned against the
				// real target: a same-type AlterColumnType leaves the
				// stored value untouched.
				before := rig.targetRowText(1)
				sw, err := rig.tgtEng.OpenSchemaWriter(context.Background(), rig.targetDSN)
				if err != nil {
					t.Fatalf("open target schema writer: %v", err)
				}
				defer closeIfErrIgnoredForTest(sw)
				da, ok := sw.(ir.ShapeDeltaApplier)
				if !ok {
					t.Fatalf("%s SchemaWriter does not implement ir.ShapeDeltaApplier", target.engine)
				}
				want := &ir.Column{Name: "c", Type: rig.swappedIR()}
				if err := da.AlterColumnType(context.Background(), &ir.Table{Name: "events"}, want); err != nil {
					t.Fatalf("same-type AlterColumnType: %v", err)
				}
				if after := rig.targetRowText(1); after != before {
					t.Errorf("same-type AlterColumnType changed row 1 from %q to %q; the written no-op invariant does not hold on %s", before, after, target.engine)
				}
				if got := rig.targetColumnType(); got != rig.swappedTargetType() {
					t.Errorf("target events.c is %q after the same-type ALTER; want %q", got, rig.swappedTargetType())
				}
			})
		}
	}
}

func closeIfErrIgnoredForTest(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

func TestStreamer_SessionTZWitness_ResidualRefusesWithoutHistory(t *testing.T) {
	for _, target := range zoneWitnessTargets {
		for _, dir := range zoneWitnessDirections {
			t.Run(target.name+"/"+dir.name, func(t *testing.T) {
				rig := newZoneWitnessRig(t, target, dir)
				streamID := "slm1b-residual-" + target.name
				ctx1, cancel1 := context.WithCancel(context.Background())
				defer cancel1()
				errc1 := rig.run(ctx1, rig.streamer(streamID))
				if !rig.waitRowCount(3, 90*time.Second) {
					t.Fatal("bulk-copy never landed the seed rows")
				}
				// One applied change so the persisted position is a real
				// CDC position, its post-commit write landed, then a clean
				// stop.
				anchor := rig.waitPersistedPosition(streamID)
				applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (100, '2020-01-01 21:00:00');")
				if !rig.waitRowID(100, 90*time.Second) {
					t.Fatal("the CDC row never landed")
				}
				rig.waitPersistedPositionPast(streamID, anchor)
				rig.stop(cancel1, errc1)
				if n := rig.historyRowsFor(streamID, "events"); n != 0 {
					t.Fatalf("anti-vacuity: %d retained history row(s) for events; this cell must resume with NO history prior", n)
				}

				// The source-only swap while the stream is stopped, then
				// the row that would land in the mismatched column.
				applyDDLMySQL(t, rig.sourceDSN, fmt.Sprintf("ALTER TABLE events MODIFY c %s NOT NULL;", dir.swapTo))
				applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (4, '2020-01-01 21:00:00');")

				ctx2, cancel2 := context.WithCancel(context.Background())
				defer cancel2()
				errc2 := rig.run(ctx2, rig.streamer(streamID))
				rig.expectRefusal(errc2, "warm resume")
				if got := rig.targetColumnType(); got != rig.originalTargetType() {
					t.Errorf("target events.c is %q; want %q untouched", got, rig.originalTargetType())
				}
				if rig.targetRowExists(4) {
					t.Error("the post-swap row landed in the zone-mismatched column (SLM-1b gap 2)")
				}
			})
		}
	}
}

func TestStreamer_SessionTZWitness_OverrideFallsBackToHistory(t *testing.T) {
	target := zoneWitnessTargets[0] // Postgres: the only target where an override changes the family.
	for _, dir := range zoneWitnessDirections {
		t.Run(dir.name, func(t *testing.T) {
			rig := newZoneWitnessRig(t, target, dir)
			streamID := "slm1b-override"
			override := []config.Mapping{{Table: "events", Column: "c", TargetType: dir.override}}
			s1 := rig.streamer(streamID)
			s1.Mappings = override
			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()
			errc1 := rig.run(ctx1, s1)
			if !rig.waitRowCount(3, 90*time.Second) {
				t.Fatal("bulk-copy never landed the seed rows")
			}
			// The override put the target on the OPPOSITE family: the
			// witness, if consulted, would read every later boundary as a
			// swap the operator never made.
			if got := rig.targetColumnType(); got != rig.swappedTargetType() {
				t.Fatalf("anti-vacuity: target events.c is %q; want the override's %q", got, rig.swappedTargetType())
			}
			anchor := rig.waitPersistedPosition(streamID)
			applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (100, '2020-01-01 21:00:00');")
			if !rig.waitRowID(100, 90*time.Second) {
				t.Fatal("the CDC row never landed")
			}
			rig.waitPersistedPositionPast(streamID, anchor)
			rig.stop(cancel1, errc1)

			// A plain DDL after the stop, and the row behind it. The
			// witness, if consulted, would read the rebuilt table against
			// the override's family and refuse this boundary as a swap.
			applyDDLMySQL(t, rig.sourceDSN, "ALTER TABLE events ADD INDEX events_c_idx (c);")
			applyDDLMySQL(t, rig.sourceDSN, "INSERT INTO events (id, c) VALUES (4, '2020-01-01 21:00:00');")

			s2 := rig.streamer(streamID)
			s2.Mappings = override
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			errc2 := rig.run(ctx2, s2)
			if !rig.waitRowID(4, 90*time.Second) {
				select {
				case err := <-errc2:
					t.Fatalf("warm resume with a --type-override on the temporal column refused a plain index-only ALTER — the witness spoke for an overridden table: %v", err)
				default:
				}
				t.Fatal("the post-ALTER row never landed after warm resume")
			}
			rig.expectStillRunning(errc2, "overridden stream")
			rig.stop(cancel2, errc2)
		})
	}
}
