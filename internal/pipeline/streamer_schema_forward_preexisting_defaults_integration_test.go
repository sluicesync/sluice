//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// The pre-existing-row DEFAULT gate (Bug 287's class).
//
// # Why rows that already exist are the whole question
//
// When sync forwards a source `ALTER TABLE t ADD COLUMN c … DEFAULT d`
// (ADR-0058 / ADR-0091), the TARGET fills every row it already holds
// with the target column's DEFAULT. The source fills ITS existing rows
// with the declared default too — but that fill emits no binlog / WAL
// row event, so CDC never carries a value that could correct the target.
// Whatever DEFAULT the forwarded ALTER carries is therefore the value of
// every pre-existing target row, permanently, at exit 0. A wrong default
// is not "a later INSERT reads back wrong" (the shape the Bug 286 and
// GC-29 pins grade); it is silent corruption of rows that already exist.
//
// # The independent expected value
//
// The SOURCE's own value for the same row, read back with SELECT after
// the ALTER. Never the target catalog (that would grade the emitter
// against itself) and never sluice's log. A cell passes when every row
// that existed at its ALTER holds, on the target, the canonical text the
// source holds for that row.
//
// # Canonical text, per family (and why none of it can mask a real diff)
//
// Each side renders the column with its own engine's text form; the only
// rewriting afterwards is the boolean spelling:
//
//   - fdText: MySQL `CAST(c AS CHAR)`, PG `c::text`. Covers strings,
//     enums, integers, decimals, floats (the shapes use exactly
//     representable values, so no engine's shortest-round-trip printer
//     is being compared against another's), DATE, TIME, YEAR, JSON, UUID.
//     Nothing is trimmed or case-folded.
//   - fdArray: a PG array as the JSON document a MySQL target stores it
//     as — PG `to_jsonb(c)::text`, MySQL `CAST(c AS CHAR)`.
//   - fdBool: rendered as fdText; PG's `true`/`false` are then mapped to
//     MySQL's `1`/`0`. Only those two exact tokens are rewritten, so a
//     `2`, a NULL or any other spelling still compares as itself.
//   - fdDateTime: DATETIME / TIMESTAMP, rendered to the microsecond
//     (`DATE_FORMAT(c,'%Y-%m-%d %H:%i:%s.%f')` / `to_char(c,'YYYY-MM-DD
//     HH24:MI:SS.US')`) with BOTH grading sessions pinned to UTC, so a
//     zone-aware type is compared as the instant it holds. Full
//     precision on both sides; nothing is truncated.
//   - fdBinary: lowercase hex of the bytes (`LOWER(HEX(c))` /
//     `encode(c,'hex')`) — every byte, NULs included.
//   - fdBit: BIT as its unsigned integer value — MySQL `CAST(c+0 AS
//     CHAR)`, PG `c::bigint::text` (the PG target type is bit(n)).
//   - fdSet: MySQL SET is its own comma list; on PG it is `text[]` by
//     design (docs/type-mapping.md), rendered `array_to_string(c, ',',
//     '<NULL-ELEMENT>')` — element order kept, a NULL element visible.
//   - fdInet: MariaDB INET6 is a bare address; PG `inet::text` always
//     appends the mask, so PG renders `host(c)` ONLY when the mask is the
//     full width of the family (the one mask a bare address implies) and
//     `c::text` otherwise — a narrowed mask still shows.
//   - fdDecimal: fdText with trailing zeros RIGHT OF A POINT dropped on
//     both sides (PG `trim_scale`, MySQL the same trim in SQL), because
//     an unconstrained numeric lands on MySQL at DECIMAL(65,30)'s display
//     scale. A truncated value (1 for 1.10) still differs.
//
// SQL NULL renders as <NULL> on both sides and compares as itself.
//
// # Known-wrong cells
//
// A cell whose pre-existing target rows are measured WRONG today is NOT
// skipped: it lives in its lane's knownWrong map with the exact value
// every such row holds, and the gate asserts that value. A fix therefore
// turns the cell red ("now correct — remove it from the known-wrong
// list") and forces the list to be updated in the same change; a
// different wrong value turns it red too.
//
// Shard: every test here carries the TestStreamer_ prefix, which routes
// it to the pipeline-rest-streamer shard (ci.yml `-run ^TestStreamer_`).

// fdFamily picks the canonical-text rendering a column is graded in.
type fdFamily int

const (
	fdText fdFamily = iota
	fdBool
	fdDateTime
	fdBinary
	fdBit
	fdSet
	fdInet
	// fdTime renders a time of day as HH:MM:SS.ffffff on both engines, so a
	// PG `time` and the MySQL `TIME(6)` it maps to compare equal when their
	// values are.
	fdTime
	// fdDecimal is fdText with trailing FRACTIONAL zeros (and a then-bare
	// point) dropped on both sides: an unconstrained PG numeric lands on
	// MySQL as DECIMAL(65,30) by the migrate policy, so 1.10 reads back as
	// 1.100000000000000000000000000000 — the same value at a wider display
	// scale. Only zeros right of a point are dropped, so a truncated 1 still
	// differs from 1.1, and an integer's own zeros are never touched.
	fdDecimal
	// fdArray renders a Postgres array as the JSON document MySQL stores it
	// as (docs/type-mapping.md: array → JSON): PG `to_jsonb(c)::text`, MySQL
	// `CAST(c AS CHAR)`. Both print jsonb/JSON's normalised form (`[1, 2]`),
	// element order, NULL elements and nesting kept.
	fdArray
)

// fdDialect is which engine's SQL a grading query is written in.
type fdDialect int

const (
	fdMySQL fdDialect = iota
	fdPG
)

func (d fdDialect) driver() string {
	if d == fdPG {
		return "pgx"
	}
	return "mysql"
}

// fdShape is one forwarded ADD COLUMN: the column, the rest of its
// definition as the source spells it, and how it is graded.
type fdShape struct {
	col, def string
	fam      fdFamily
}

// fdKnownWrong is a measured defect: every pre-existing target row of
// the cell holds target, where the source row holds something else.
type fdKnownWrong struct {
	target string
	defect string
}

// fdHalt is a shape whose forward must END the stream: the designed
// ADR-0058 §2a refusal of a volatile DEFAULT, or a measured loud
// failure (defect non-empty). want are lowercase substrings the
// terminal error carries. Unless altered, the column must not reach the
// target at all; altered marks a halt that fires only AFTER the target
// ALTER landed, whose pre-existing rows are then graded like any cell.
type fdHalt struct {
	shape   fdShape
	want    []string
	defect  string
	altered bool

	// prelude is forwarded (and must apply) before shape, for a defect
	// that only fires when shape is not the stream's first forward.
	prelude []fdShape
}

// afterAlter marks the halt as firing after the target ALTER landed.
func (h fdHalt) afterAlter() fdHalt {
	h.altered = true
	return h
}

// after gives the halt a prelude of forwards that must land first.
func (h fdHalt) after(prelude ...fdShape) fdHalt {
	h.prelude = prelude
	return h
}

// fdLane is one source → target pair.
type fdLane struct {
	name         string
	sourceEngine string
	targetEngine string
	sourceDSN    string
	targetDSN    string
	src, tgt     fdDialect
	shapes       []fdShape
	knownWrong   map[string]fdKnownWrong
	halts        []fdHalt

	// freshPair returns a new, empty source/target database pair for
	// one halt cell (a halt ends its stream, so each needs its own).
	// nil means the lane cannot mint one; it may then carry at most one
	// halt, run last on the matrix stream.
	freshPair func(t *testing.T, tag string) (src, tgt string)

	// settle is slept after source DDL on a VStream source, so vttablet's
	// schema tracker has the new shape before the next row event.
	settle time.Duration

	// streamParams is appended to the Streamer's source DSN only (the
	// VStream endpoint parameters, which a plain SQL session rejects).
	streamParams string

	// shardColumn, when engaged, runs the lane as a Shape A fan-in stream
	// (--inject-shard-column): every forward then goes through the
	// ADR-0054 boundary router instead of the single-stream forwarder.
	shardColumn ShardColumnSpec

	// oneAlter forwards the whole matrix as ONE multi-column ADD COLUMN
	// (one boundary) instead of one ALTER per shape. A Shape A lane needs
	// it: its lease is single-use per table until the GC sweep retires the
	// applied row, so a second DDL on `w` refuses loudly (Bug 262b, open).
	// Every shape still reaches the router's per-column carry, retarget and
	// emit. A oneAlter lane cannot carry a halt with a prelude.
	oneAlter bool
}

// fdDesignedRefusal is the ADR-0058 §2a volatile-DEFAULT refusal.
func fdDesignedRefusal(sh fdShape) fdHalt {
	return fdHalt{shape: sh, want: []string{"computed default", "adr-0058 §2a"}}
}

// fdSeedRows is how many rows exist before the first forwarded ALTER.
const fdSeedRows = 3

const fdNull = "<NULL>"

func fdQuote(d fdDialect, ident string) string {
	if d == fdPG {
		return `"` + ident + `"`
	}
	return "`" + ident + "`"
}

// fdCanonExpr is the per-family, per-dialect rendering documented at
// the top of the file.
func fdCanonExpr(d fdDialect, fam fdFamily, col string) string {
	c := fdQuote(d, col)
	switch d {
	case fdPG:
		switch fam {
		case fdDateTime:
			return "to_char(" + c + ", 'YYYY-MM-DD HH24:MI:SS.US')"
		case fdTime:
			return "to_char('2000-01-01'::date + " + c + ", 'HH24:MI:SS.US')"
		case fdArray:
			return "to_jsonb(" + c + ")::text"
		case fdBinary:
			return "encode(" + c + ", 'hex')"
		case fdBit:
			return c + "::bigint::text"
		case fdSet:
			return "array_to_string(" + c + ", ',', '<NULL-ELEMENT>')"
		case fdInet:
			return "CASE WHEN masklen(" + c + ") = CASE family(" + c + ") WHEN 4 THEN 32 ELSE 128 END THEN host(" + c + ") ELSE " + c + "::text END"
		case fdDecimal:
			return "trim_scale(" + c + ")::text"
		default:
			return c + "::text"
		}
	default:
		switch fam {
		case fdDateTime:
			return "DATE_FORMAT(" + c + ", '%Y-%m-%d %H:%i:%s.%f')"
		case fdTime:
			return "TIME_FORMAT(" + c + ", '%H:%i:%s.%f')"
		case fdBinary:
			return "LOWER(HEX(" + c + "))"
		case fdBit:
			return "CAST(" + c + "+0 AS CHAR)"
		case fdDecimal:
			s := "CAST(" + c + " AS CHAR)"
			return "CASE WHEN LOCATE('.', " + s + ") > 0 THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM " + s + ")) ELSE " + s + " END"
		default:
			return "CAST(" + c + " AS CHAR)"
		}
	}
}

// fdNormalize is the ONLY post-read rewrite: PG's boolean spelling.
func fdNormalize(fam fdFamily, s string) string {
	if fam == fdBool {
		switch s {
		case "t", "true":
			return "1"
		case "f", "false":
			return "0"
		}
	}
	return s
}

// fdUTC pins a grading session to UTC so zone-aware temporal values
// render as the instant they hold.
func fdUTC(d fdDialect, dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	if d == fdPG {
		return dsn + sep + "timezone=UTC"
	}
	return dsn + sep + "time_zone=%27%2B00%3A00%27"
}

func fdExec(t *testing.T, d fdDialect, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open(d.driver(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func fdRowCount(d fdDialect, dsn string) int {
	if d == fdPG {
		return pollRowCount(dsn, "w")
	}
	return pollRowCountMySQL(dsn, "w")
}

// fdColumnValues reads id → canonical text for rows id <= maxID.
func fdColumnValues(ctx context.Context, db *sql.DB, d fdDialect, sh fdShape, maxID int) (map[int]string, error) {
	q := "SELECT id, " + fdCanonExpr(d, sh.fam, sh.col) + " FROM w WHERE id <= " + strconv.Itoa(maxID) + " ORDER BY id"
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", q, err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[int]string, maxID)
	for rows.Next() {
		var (
			id int
			v  sql.NullString
		)
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		s := fdNull
		if v.Valid {
			s = fdNormalize(sh.fam, v.String)
		}
		out[id] = s
	}
	return out, rows.Err()
}

// fdStream is one running Streamer over a seeded `w` table.
type fdStream struct {
	lane     fdLane
	src, tgt string
	runErr   chan error
	cancel   context.CancelFunc
}

// fdStartStream creates and seeds `w` on src and starts a default-config
// Streamer to tgt. The caller registers the stream for cancellation and
// then waits (waitRows) for the cold start to land the seed rows.
func fdStartStream(t *testing.T, lane fdLane, src, tgt, streamID string) *fdStream {
	t.Helper()
	if lane.src == fdPG {
		// The enum type the e_mood shape uses exists before the stream
		// starts; only the column that uses it arrives mid-stream.
		fdExec(t, lane.src, src, `CREATE TYPE fd_mood AS ENUM ('sad', 'ok')`)
		fdExec(t, lane.src, src, `CREATE TABLE w (id BIGINT PRIMARY KEY, name VARCHAR(80) NOT NULL)`)
	} else {
		fdExec(t, lane.src, src, `CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(80) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	}
	for id := 1; id <= fdSeedRows; id++ {
		fdExec(t, lane.src, src, fmt.Sprintf("INSERT INTO w (id, name) VALUES (%d, 'seed%d')", id, id))
	}
	time.Sleep(lane.settle)

	srcEng, ok := engines.Get(lane.sourceEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.sourceEngine)
	}
	tgtEng, ok := engines.Get(lane.targetEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.targetEngine)
	}
	// Default config: forwarding is ON unless --schema-changes=refuse
	// (ADR-0091), so no flag is set — this grades what an operator gets.
	// The slot name is per stream only because a Postgres slot is
	// cluster-wide and the halt cells share their lane's server.
	streamer := &Streamer{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: src + lane.streamParams,
		TargetDSN: tgt,
		StreamID:  streamID,
		SlotName:  strings.ReplaceAll(streamID, "-", "_"),

		InjectShardColumn: lane.shardColumn,
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &fdStream{lane: lane, src: src, tgt: tgt, runErr: make(chan error, 1), cancel: cancel}
	go func() { s.runErr <- streamer.Run(ctx) }()
	return s
}

func (s *fdStream) waitRows(t *testing.T, what string, n int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for fdRowCount(s.lane.tgt, s.tgt) < n {
		select {
		case err := <-s.runErr:
			t.Fatalf("%s: %s: streamer halted: %v", s.lane.name, what, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s: target never reached %d rows", s.lane.name, what, n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// forward runs one source ADD COLUMN, then one source INSERT with id —
// the post-ALTER row the stream must carry past the boundary.
func (s *fdStream) forward(t *testing.T, sh fdShape, id int) {
	t.Helper()
	fdExec(t, s.lane.src, s.src, "ALTER TABLE w ADD COLUMN "+fdQuote(s.lane.src, sh.col)+" "+sh.def)
	time.Sleep(s.lane.settle)
	fdExec(t, s.lane.src, s.src, fmt.Sprintf("INSERT INTO w (id, name) VALUES (%d, 'after-%s')", id, sh.col))
}

// forwardAll runs ONE source ALTER adding every shape, then one source
// INSERT with id — a single boundary carrying all the added columns.
func (s *fdStream) forwardAll(t *testing.T, shapes []fdShape, id int) {
	t.Helper()
	adds := make([]string, len(shapes))
	for i, sh := range shapes {
		adds[i] = "ADD COLUMN " + fdQuote(s.lane.src, sh.col) + " " + sh.def
	}
	fdExec(t, s.lane.src, s.src, "ALTER TABLE w "+strings.Join(adds, ", "))
	time.Sleep(s.lane.settle)
	fdExec(t, s.lane.src, s.src, fmt.Sprintf("INSERT INTO w (id, name) VALUES (%d, 'after-all')", id))
}

func (s *fdStream) stop(t *testing.T) {
	t.Helper()
	s.cancel()
	select {
	case <-s.runErr:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: Streamer.Run did not return after ctx cancel", s.lane.name)
	}
}

// runForwardedDefaultLane drives one lane: seed, cold start, forward
// every shape, grade every pre-existing row against the source, then
// run each halt cell.
func runForwardedDefaultLane(t *testing.T, lane fdLane) {
	t.Helper()
	if lane.freshPair == nil && len(lane.halts) > 1 {
		t.Fatalf("%s: a lane without freshPair can carry at most one halt cell", lane.name)
	}
	for _, h := range lane.halts {
		if lane.oneAlter && len(h.prelude) > 0 {
			t.Fatalf("%s: a oneAlter lane cannot run halt %s's prelude (a second DDL on w)", lane.name, h.shape.col)
		}
	}
	// A halt cell ends its stream, so it leaves the matrix.
	halting := make(map[string]bool, len(lane.halts))
	for _, h := range lane.halts {
		halting[h.shape.col] = true
	}
	matrix := make([]fdShape, 0, len(lane.shapes))
	for _, sh := range lane.shapes {
		if !halting[sh.col] {
			matrix = append(matrix, sh)
		}
	}
	lane.shapes = matrix

	// A known-wrong entry no grading reaches would outlive its defect
	// silently, so every one must name a graded cell: a matrix shape or
	// a halt whose ALTER lands.
	graded := make(map[string]bool, len(matrix))
	for _, sh := range matrix {
		graded[sh.col] = true
	}
	for _, h := range lane.halts {
		if h.altered {
			graded[h.shape.col] = true
		}
	}
	for col := range lane.knownWrong {
		if !graded[col] {
			t.Errorf("%s: knownWrong names %q, which no graded cell of this lane reaches", lane.name, col)
		}
	}

	// Every stream is cancelled before the caller's deferred container
	// teardown, however this function exits.
	var streams []*fdStream
	defer func() {
		for _, st := range streams {
			st.cancel()
		}
	}()

	s := fdStartStream(t, lane, lane.sourceDSN, lane.targetDSN, "test-fwd-default-rows")
	streams = append(streams, s)
	s.waitRows(t, "cold start", fdSeedRows)

	// One ALTER per shape, one post-ALTER row each as the liveness
	// signal. Shape i's pre-existing rows are therefore ids
	// 1 .. fdSeedRows+i. A oneAlter lane has one ALTER, so every shape's
	// pre-existing rows are the seed rows.
	cells := make([]fdCell, len(lane.shapes))
	if lane.oneAlter {
		s.forwardAll(t, lane.shapes, fdSeedRows+1)
		s.waitRows(t, "forward the whole matrix in one ALTER", fdSeedRows+1)
		for i, sh := range lane.shapes {
			cells[i] = fdCell{shape: sh, maxID: fdSeedRows}
		}
	} else {
		for i, sh := range lane.shapes {
			id := fdSeedRows + i + 1
			s.forward(t, sh, id)
			s.waitRows(t, "forward "+sh.col+" "+sh.def, id)
			cells[i] = fdCell{shape: sh, maxID: fdSeedRows + i}
		}
	}
	fdGrade(t, lane, lane.sourceDSN, lane.targetDSN, cells)

	for i, h := range lane.halts {
		hs := s
		if lane.freshPair != nil {
			src, tgt := lane.freshPair(t, fmt.Sprintf("halt%d", i))
			hs = fdStartStream(t, lane, src, tgt, fmt.Sprintf("test-fwd-default-halt%d", i))
			streams = append(streams, hs)
			hs.waitRows(t, "cold start", fdSeedRows)
		}
		fdAssertHalt(t, hs, h)
	}
	if lane.freshPair != nil || len(lane.halts) == 0 {
		s.stop(t)
	}
}

// fdCell is one graded column: its shape and the highest row id that
// existed when its ALTER ran (rows 1..maxID are its pre-existing rows).
type fdCell struct {
	shape fdShape
	maxID int
}

// fdGrade compares every pre-existing row of every cell, target
// against source, and logs a verdict table. Each cell is one verdict
// (a wrong DEFAULT fills every pre-existing row alike, so the first
// divergent row is reported with the count).
func fdGrade(t *testing.T, lane fdLane, srcDSN, tgtDSN string, cells []fdCell) {
	t.Helper()
	srcDB, err := sql.Open(lane.src.driver(), fdUTC(lane.src, srcDSN))
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	tgtDB, err := sql.Open(lane.tgt.driver(), fdUTC(lane.tgt, tgtDSN))
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var table strings.Builder
	for _, c := range cells {
		sh := c.shape
		srcVals, err := fdColumnValues(ctx, srcDB, lane.src, sh, c.maxID)
		if err != nil {
			t.Fatalf("%s: %s: read source: %v", lane.name, sh.col, err)
		}
		tgtVals, err := fdColumnValues(ctx, tgtDB, lane.tgt, sh, c.maxID)
		if err != nil {
			t.Errorf("%s: %s %s: read target: %v", lane.name, sh.col, sh.def, err)
			continue
		}
		if len(srcVals) != c.maxID || len(tgtVals) != c.maxID {
			t.Errorf("%s: %s: want %d pre-existing rows on each side; source has %d, target %d",
				lane.name, sh.col, c.maxID, len(srcVals), len(tgtVals))
			continue
		}
		kw, known := lane.knownWrong[sh.col]
		var bad []int // rows that break the cell's expectation
		for id := 1; id <= c.maxID; id++ {
			s, g := srcVals[id], tgtVals[id]
			if (!known && s != g) || (known && (s == g || g != kw.target)) {
				bad = append(bad, id)
			}
		}
		verdict := "OK"
		switch {
		case len(bad) == 0 && known:
			verdict = "KNOWN-WRONG (" + kw.defect + ")"
		case len(bad) == 0:
		case !known:
			verdict = "WRONG"
			id := bad[0]
			t.Errorf("%s: %s %s: %d/%d pre-existing target rows differ from the source, e.g. id=%d source [%s] target [%s] — the forwarded DEFAULT filled rows that already existed with a value the source rows do not hold (Bug 287 class)",
				lane.name, sh.col, sh.def, len(bad), c.maxID, id, srcVals[id], tgtVals[id])
		case srcVals[bad[0]] == tgtVals[bad[0]]:
			verdict = "NOW-CORRECT"
			t.Errorf("%s: known-wrong cell %s %s (%s) now reads back the source value [%s] on row id=%d — remove it from the lane's knownWrong list",
				lane.name, sh.col, sh.def, kw.defect, srcVals[bad[0]], bad[0])
		default:
			verdict = "WRONG-DIFFERENTLY"
			t.Errorf("%s: known-wrong cell %s %s: row id=%d target [%s], recorded wrong value [%s], source [%s] — the defect changed shape",
				lane.name, sh.col, sh.def, bad[0], tgtVals[bad[0]], kw.target, srcVals[bad[0]])
		}
		fmt.Fprintf(&table, "  %-14s %-56s src=[%s] tgt=[%s] %s\n", sh.col, sh.def, srcVals[1], tgtVals[1], verdict)
	}
	t.Logf("%s pre-existing-row verdicts (row id=1 shown):\n%s", lane.name, table.String())
}

// fdAssertHalt forwards a halt cell's shape and asserts the stream ENDS
// with an error carrying every want substring — and that the column
// never reached the target, so no pre-existing row was filled at all.
func fdAssertHalt(t *testing.T, s *fdStream, h fdHalt) {
	t.Helper()
	lane := s.lane
	for i, p := range h.prelude {
		id := fdSeedRows + i + 1
		s.forward(t, p, id)
		s.waitRows(t, "prelude "+p.col, id)
	}
	s.forward(t, h.shape, 1000)
	var err error
	select {
	case err = <-s.runErr:
	case <-time.After(90 * time.Second):
		t.Fatalf("%s: %s %s: stream did not halt within 90s", lane.name, h.shape.col, h.shape.def)
	}
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("%s: %s %s: Run returned %v; want a halt", lane.name, h.shape.col, h.shape.def, err)
	}
	msg := strings.ToLower(err.Error())
	for _, w := range h.want {
		if !strings.Contains(msg, w) {
			t.Errorf("%s: %s %s: halt error does not carry %q: %v", lane.name, h.shape.col, h.shape.def, w, err)
		}
	}
	kind := "designed refusal"
	if h.defect != "" {
		kind = "KNOWN LOUD DEFECT (" + h.defect + ")"
	}
	short := err.Error()
	if i := strings.Index(short, " recovery:"); i > 0 {
		short = short[:i]
	}
	t.Logf("%s: %-12s %-50s HALT, %s: %s", lane.name, h.shape.col, h.shape.def, kind, short)

	if h.altered {
		// The target ALTER landed before the stream died, so the rows that
		// already existed were filled: grade them like any matrix cell.
		fdGrade(t, lane, s.src, s.tgt, []fdCell{{shape: h.shape, maxID: fdSeedRows + len(h.prelude)}})
		return
	}

	tgtDB, oerr := sql.Open(lane.tgt.driver(), s.tgt)
	if oerr != nil {
		t.Fatalf("open target: %v", oerr)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q := `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'w' AND column_name = ?`
	if lane.tgt == fdPG {
		q = `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'w' AND column_name = $1`
	}
	var n int
	if err := tgtDB.QueryRowContext(ctx, q, h.shape.col).Scan(&n); err != nil {
		t.Fatalf("%s: check target column %s: %v", lane.name, h.shape.col, err)
	}
	if n != 0 {
		t.Errorf("%s: target column %s exists although the stream halted on it", lane.name, h.shape.col)
	}
}

// fdFreshDB creates database name next to dsn's and returns its DSN.
func fdFreshDB(t *testing.T, d fdDialect, dsn, name string) string {
	t.Helper()
	fdExec(t, d, dsn, "CREATE DATABASE "+name)
	if d == fdMySQL {
		out, err := buildMySQLDSN(dsn, name)
		if err != nil {
			t.Fatalf("fresh mysql DSN: %v", err)
		}
		return out
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("fresh pg DSN: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// fdFreshPairs mints halt-cell database pairs on the lane's own servers.
func fdFreshPairs(src, tgt fdDialect, srcDSN, tgtDSN string) func(*testing.T, string) (string, string) {
	return func(t *testing.T, tag string) (string, string) {
		t.Helper()
		return fdFreshDB(t, src, srcDSN, "s_"+tag), fdFreshDB(t, tgt, tgtDSN, "t_"+tag)
	}
}

// fdMySQLShapes is the MySQL 8 source matrix: every default family the
// server accepts × the literal shapes that have broken before (quotes,
// backslash, the string 'NULL', NUL bytes at each position) plus NOT
// NULL variants and the 8.0.13+ expression defaults.
func fdMySQLShapes() []fdShape {
	return []fdShape{
		{"s_plain", "VARCHAR(20) DEFAULT 'abc'", fdText},
		{"s_empty", "VARCHAR(20) DEFAULT ''", fdText},
		{"s_quote", "VARCHAR(20) DEFAULT 'it''s'", fdText},
		{"s_bslash", `VARCHAR(20) DEFAULT 'a\\b'`, fdText},
		{"s_unicode", "VARCHAR(20) DEFAULT 'héllo ✓ 日本'", fdText},
		{"s_nullword", "VARCHAR(20) DEFAULT 'NULL'", fdText},
		{"s_char", "CHAR(5) DEFAULT 'ab'", fdText},
		{"s_nn", "VARCHAR(20) NOT NULL DEFAULT 'nn'", fdText},
		{"s_textexpr", "TEXT DEFAULT ('txt')", fdText},
		{"n_null", "VARCHAR(20) DEFAULT NULL", fdText},
		{"n_intnull", "INT DEFAULT NULL", fdText},
		{"i_int", "INT DEFAULT 42", fdText},
		{"i_neg", "INT DEFAULT -7", fdText},
		{"i_nn", "INT NOT NULL DEFAULT 0", fdText},
		{"i_small", "SMALLINT DEFAULT -32768", fdText},
		{"i_bigmax", "BIGINT DEFAULT 9223372036854775807", fdText},
		{"i_bigmin", "BIGINT DEFAULT -9223372036854775808", fdText},
		{"i_ubigmax", "BIGINT UNSIGNED DEFAULT 18446744073709551615", fdText},
		{"d_dec", "DECIMAL(10,3) DEFAULT 12.345", fdText},
		{"d_decneg", "DECIMAL(10,2) DEFAULT -0.50", fdText},
		{"f_float", "FLOAT DEFAULT 1.5", fdText},
		{"f_double", "DOUBLE DEFAULT -0.25", fdText},
		{"b_bool", "TINYINT(1) DEFAULT 1", fdBool},
		{"b_boolf", "BOOLEAN NOT NULL DEFAULT FALSE", fdBool},
		{"t_date", "DATE DEFAULT '2024-02-29'", fdText},
		{"t_dt", "DATETIME DEFAULT '2024-01-02 03:04:05'", fdDateTime},
		{"t_dt6", "DATETIME(6) DEFAULT '2024-01-02 03:04:05.123456'", fdDateTime},
		{"t_dtnn", "DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00'", fdDateTime},
		{"t_ts", "TIMESTAMP NULL DEFAULT '2024-01-02 03:04:05'", fdDateTime},
		{"t_time", "TIME DEFAULT '12:34:56'", fdText},
		{"t_year", "YEAR DEFAULT 2024", fdText},
		{"x_bin", "BINARY(3) DEFAULT 0x00AB00", fdBinary},
		{"x_vbin", "VARBINARY(4) DEFAULT 0xAB00CD", fdBinary},
		{"x_vbinplain", "VARBINARY(4) DEFAULT 0xDEAD", fdBinary},
		{"x_binstr", "BINARY(4) DEFAULT 'ab'", fdBinary},
		{"x_vbinempty", "VARBINARY(4) DEFAULT ''", fdBinary},
		{"x_blobexpr", "BLOB DEFAULT (0x00FF)", fdBinary},
		{"x_bit", "BIT(8) DEFAULT b'1010'", fdBit},
		{"e_enum", "ENUM('a','b','c d') DEFAULT 'c d'", fdText},
		{"e_enumq", "ENUM('x''y','z') DEFAULT 'x''y'", fdText},
		{"e_set", "SET('x','y') DEFAULT 'x,y'", fdSet},
		{"j_obj", "JSON DEFAULT (JSON_OBJECT())", fdText},
		{"j_kv", "JSON DEFAULT (JSON_OBJECT('a', 1))", fdText},
		{"g_expr", "INT DEFAULT (1 + 2)", fdText},
	}
}

// fdLoud is a measured LOUD defect: forwarding the lane shape named col
// ends the stream with an error carrying want. The runner takes the
// shape out of the matrix and runs it on its own database pair.
func fdLoud(t *testing.T, shapes []fdShape, col, want, defect string) fdHalt {
	t.Helper()
	for _, sh := range shapes {
		if sh.col == col {
			return fdHalt{shape: sh, want: []string{want}, defect: defect}
		}
	}
	t.Fatalf("fdLoud: no shape %q", col)
	return fdHalt{}
}

// fdRefused is a lane shape the ADR-0058 §2a door refuses by design
// (a volatile or unprovable expression DEFAULT).
func fdRefused(t *testing.T, shapes []fdShape, col string) fdHalt {
	t.Helper()
	h := fdLoud(t, shapes, col, "", "")
	return fdDesignedRefusal(h.shape)
}

// fdMySQLNow is the volatile cell every MySQL-family lane must refuse.
var fdMySQLNow = fdShape{"t_now", "DATETIME DEFAULT CURRENT_TIMESTAMP", fdDateTime}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_MySQLToPostgres
// grades the MySQL 8 binlog → Postgres lane. See the file comment.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_MySQLToPostgres(t *testing.T) {
	src, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	all := fdMySQLShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->postgres", sourceEngine: "mysql", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
		shapes:     all,
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			fdLoud(t, all, "i_ubigmax", "out of range for type bigint",
				"BIGINT UNSIGNED forwards as PG bigint; its max DEFAULT overflows the target ALTER"),
			fdLoud(t, all, "x_blobexpr", "is of type bytea but default expression is of type integer",
				"MySQL's (0x00FF) expression DEFAULT is re-emitted verbatim on PG, where 0x00FF lexes as an integer"),
			fdLoud(t, all, "x_bit", "does not match type bit(8)",
				"BIT(8) DEFAULT b'1010' is emitted as a 4-bit literal PG will not widen"),
			fdRefused(t, all, "j_obj"),
			fdRefused(t, all, "j_kv"),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: fdFreshPairs(fdMySQL, fdPG, src, tgt),
	})
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_ShapeAMySQLToPostgres
// is the MySQL binlog → Postgres lane as a Shape A fan-in stream. The
// binlog carries each DEFAULT in-band, so the router's carry reads nothing
// here; what this lane grades is the rest of the router's DEFAULT path —
// in-band defaults reaching the target through its retarget and apply,
// and the ADR-0058 §2a door, which the router did not have until GC-36 (1)
// (a CURRENT_TIMESTAMP default was forwarded and filled pre-existing rows
// with the target's clock). One ALTER for the matrix — see oneAlter.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_ShapeAMySQLToPostgres(t *testing.T) {
	src, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	all := fdMySQLShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "shapeA mysql->postgres", sourceEngine: "mysql", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
		shapes:      all,
		shardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "shard_a"},
		oneAlter:    true,
		knownWrong:  map[string]fdKnownWrong{},
		halts: []fdHalt{
			fdLoud(t, all, "i_ubigmax", "out of range for type bigint",
				"BIGINT UNSIGNED forwards as PG bigint; its max DEFAULT overflows the target ALTER"),
			fdLoud(t, all, "x_blobexpr", "is of type bytea but default expression is of type integer",
				"MySQL's (0x00FF) expression DEFAULT is re-emitted verbatim on PG, where 0x00FF lexes as an integer"),
			fdLoud(t, all, "x_bit", "does not match type bit(8)",
				"BIT(8) DEFAULT b'1010' is emitted as a 4-bit literal PG will not widen"),
			fdRefused(t, all, "j_obj"),
			fdRefused(t, all, "j_kv"),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: fdFreshPairs(fdMySQL, fdPG, src, tgt),
	})
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_MySQLToMySQL
// grades the MySQL 8 binlog → MySQL lane. Source and target are separate
// servers: sharing one measured 218s against 55s, every forward ~4x
// slower.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_MySQLToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	all := fdMySQLShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->mysql", sourceEngine: "mysql", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes:     all,
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			fdRefused(t, all, "j_obj"),
			fdRefused(t, all, "j_kv"),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: fdFreshPairs(fdMySQL, fdMySQL, src, tgt),
	})
}

// fdMariaDBShapes is the MariaDB 11.4 source matrix. It differs from
// MySQL's where MariaDB's grammar does: TEXT/BLOB take literal
// defaults, JSON is a LONGTEXT alias with a literal default, and the
// native UUID / INET6 types exist.
func fdMariaDBShapes() []fdShape {
	return []fdShape{
		{"s_plain", "VARCHAR(20) DEFAULT 'abc'", fdText},
		{"s_empty", "VARCHAR(20) DEFAULT ''", fdText},
		{"s_quote", "VARCHAR(20) DEFAULT 'it''s'", fdText},
		{"s_bslash", `VARCHAR(20) DEFAULT 'a\\b'`, fdText},
		{"s_unicode", "VARCHAR(20) DEFAULT 'héllo ✓ 日本'", fdText},
		{"s_nullword", "VARCHAR(20) DEFAULT 'NULL'", fdText},
		{"s_char", "CHAR(5) DEFAULT 'ab'", fdText},
		{"s_nn", "VARCHAR(20) NOT NULL DEFAULT 'nn'", fdText},
		{"s_text", "TEXT DEFAULT 'txt'", fdText},
		{"n_null", "VARCHAR(20) DEFAULT NULL", fdText},
		{"n_intnull", "INT DEFAULT NULL", fdText},
		{"i_int", "INT DEFAULT 42", fdText},
		{"i_neg", "INT DEFAULT -7", fdText},
		{"i_nn", "INT NOT NULL DEFAULT 0", fdText},
		{"i_bigmax", "BIGINT DEFAULT 9223372036854775807", fdText},
		{"i_bigmin", "BIGINT DEFAULT -9223372036854775808", fdText},
		{"i_ubigmax", "BIGINT UNSIGNED DEFAULT 18446744073709551615", fdText},
		{"d_dec", "DECIMAL(10,3) DEFAULT 12.345", fdText},
		{"d_decneg", "DECIMAL(10,2) DEFAULT -0.50", fdText},
		{"f_float", "FLOAT DEFAULT 1.5", fdText},
		{"f_double", "DOUBLE DEFAULT -0.25", fdText},
		{"b_bool", "TINYINT(1) DEFAULT 1", fdBool},
		{"b_boolf", "BOOLEAN NOT NULL DEFAULT FALSE", fdBool},
		{"t_date", "DATE DEFAULT '2024-02-29'", fdText},
		{"t_dt", "DATETIME DEFAULT '2024-01-02 03:04:05'", fdDateTime},
		{"t_dt6", "DATETIME(6) DEFAULT '2024-01-02 03:04:05.123456'", fdDateTime},
		{"t_dtnn", "DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00'", fdDateTime},
		{"t_ts", "TIMESTAMP NULL DEFAULT '2024-01-02 03:04:05'", fdDateTime},
		{"t_time", "TIME DEFAULT '12:34:56'", fdText},
		{"t_year", "YEAR DEFAULT 2024", fdText},
		{"x_bin", "BINARY(3) DEFAULT 0x00AB00", fdBinary},
		{"x_vbin", "VARBINARY(4) DEFAULT 0xAB00CD", fdBinary},
		{"x_vbinplain", "VARBINARY(4) DEFAULT 0xDEAD", fdBinary},
		{"x_binstr", "BINARY(4) DEFAULT 'ab'", fdBinary},
		{"x_bit", "BIT(8) DEFAULT b'1010'", fdBit},
		{"e_enum", "ENUM('a','b','c d') DEFAULT 'c d'", fdText},
		{"e_enumq", "ENUM('x''y','z') DEFAULT 'x''y'", fdText},
		{"e_set", "SET('x','y') DEFAULT 'x,y'", fdSet},
		{"j_json", `JSON DEFAULT '{"a": 1}'`, fdText},
		{"u_uuid", "UUID DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", fdText},
		{"u_inet6", "INET6 DEFAULT '::1'", fdInet},
		{"g_expr", "INT DEFAULT (1 + 2)", fdText},
	}
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_MariaDBToPostgres
// grades the MariaDB 11.4 binlog → Postgres lane.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_MariaDBToPostgres(t *testing.T) {
	src, srcCleanup := startMariaDBBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	all := fdMariaDBShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "mariadb->postgres", sourceEngine: "mariadb", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
		shapes:     all,
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			fdLoud(t, all, "i_ubigmax", "out of range for type bigint",
				"BIGINT UNSIGNED forwards as PG bigint; its max DEFAULT overflows the target ALTER"),
			fdLoud(t, all, "x_bit", "does not match type bit(8)",
				"BIT(8) DEFAULT b'1010' is emitted as a 4-bit literal PG will not widen"),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: fdFreshPairs(fdMySQL, fdPG, src, tgt),
	})
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_MariaDBToMariaDB
// grades the MariaDB 11.4 binlog → MariaDB lane, on separate servers
// for the same reason as the MySQL → MySQL lane.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_MariaDBToMariaDB(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	tgtServer, tgtCleanup := startMariaDBBinlog(t)
	defer tgtCleanup()
	tgt := fdFreshDB(t, fdMySQL, tgtServer, "target_db")
	all := fdMariaDBShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "mariadb->mariadb", sourceEngine: "mariadb", targetEngine: "mariadb",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes: all,
		// No known-wrong cells: the high-byte binary defaults (GC-36 item 5) and
		// the TEXT/JSON literal defaults (GC-36 item 4) are both fixed.
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			// KNOWN LOUD DEFECT: the forwarded column lands, then the first
			// row carrying the source's unsigned max fails to apply.
			fdLoud(t, all, "i_ubigmax", "out of range value for column 'i_ubigmax'",
				"MariaDB BIGINT UNSIGNED: the post-ALTER row is out of range on the target").afterAlter(),
			fdDesignedRefusal(fdMySQLNow),
		},
		freshPair: fdFreshPairs(fdMySQL, fdMySQL, src, tgtServer),
	})
}

// fdPGShapes is the Postgres source matrix.
func fdPGShapes() []fdShape {
	return []fdShape{
		{"s_plain", "VARCHAR(20) DEFAULT 'abc'", fdText},
		{"s_text", "TEXT DEFAULT 'abc'", fdText},
		{"s_empty", "TEXT DEFAULT ''", fdText},
		{"s_quote", "TEXT DEFAULT 'it''s'", fdText},
		{"s_bslash", `TEXT DEFAULT 'a\b'`, fdText},
		{"s_estr", `TEXT DEFAULT E'tab\there'`, fdText},
		{"s_unicode", "TEXT DEFAULT 'héllo ✓ 日本'", fdText},
		{"s_nullword", "TEXT DEFAULT 'NULL'", fdText},
		{"s_char", "CHAR(5) DEFAULT 'ab'", fdText},
		{"s_nn", "TEXT NOT NULL DEFAULT 'nn'", fdText},
		{"n_null", "TEXT DEFAULT NULL", fdText},
		{"n_intnull", "INTEGER DEFAULT NULL", fdText},
		{"i_int", "INTEGER DEFAULT 42", fdText},
		{"i_neg", "INTEGER DEFAULT -7", fdText},
		{"i_nn", "INTEGER NOT NULL DEFAULT 0", fdText},
		{"i_small", "SMALLINT DEFAULT -32768", fdText},
		{"i_bigmax", "BIGINT DEFAULT 9223372036854775807", fdText},
		{"i_bigmin", "BIGINT DEFAULT -9223372036854775808", fdText},
		{"d_num", "NUMERIC(10,3) DEFAULT 12.345", fdText},
		{"d_numneg", "NUMERIC(10,2) DEFAULT -0.50", fdText},
		{"f_real", "REAL DEFAULT 1.5", fdText},
		{"f_double", "DOUBLE PRECISION DEFAULT -0.25", fdText},
		{"b_bool", "BOOLEAN DEFAULT true", fdBool},
		{"b_boolf", "BOOLEAN NOT NULL DEFAULT false", fdBool},
		{"t_date", "DATE DEFAULT '2024-02-29'", fdText},
		{"t_ts", "TIMESTAMP DEFAULT '2024-01-02 03:04:05'", fdDateTime},
		{"t_ts6", "TIMESTAMP(6) DEFAULT '2024-01-02 03:04:05.123456'", fdDateTime},
		{"t_tstz", "TIMESTAMPTZ DEFAULT '2024-01-02 03:04:05+02'", fdDateTime},
		{"t_time", "TIME DEFAULT '12:34:56'", fdTime},
		{"x_bytea", `BYTEA DEFAULT '\x00ab00'`, fdBinary},
		{"x_bytea2", `BYTEA DEFAULT '\xab00cd'`, fdBinary},
		{"x_bytea3", `BYTEA DEFAULT '\xdead'`, fdBinary},
		{"x_byteaempty", "BYTEA DEFAULT ''", fdBinary},
		{"j_jsonb", "JSONB DEFAULT '{}'::jsonb", fdText},
		{"j_jsonbkv", `JSONB DEFAULT '{"a": 1}'`, fdText},
		{"u_uuid", "UUID DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", fdText},
		{"g_expr", "INTEGER DEFAULT (1 + 2)", fdText},
		{"d_numfree", "NUMERIC DEFAULT 1.10", fdDecimal},
		{"t_interval", "INTERVAL DEFAULT '1 day 02:00:00'", fdText},
		{"n_inet", "INET DEFAULT '10.0.0.1'", fdInet},
		{"a_int", "INTEGER[] DEFAULT '{1,2}'", fdArray},
		{"a_text", `TEXT[] DEFAULT '{a,"b c"}'`, fdArray},
		{"e_mood", "fd_mood DEFAULT 'ok'", fdText},
	}
}

// fdPGNow is the volatile cell every Postgres-source lane must refuse.
var fdPGNow = fdShape{"t_now", "TIMESTAMPTZ DEFAULT now()", fdDateTime}

// fdPrelude is a defaultless forward that makes a halt cell's shape
// the stream's second forwarded column.
var fdPrelude = fdShape{"p_first", "TEXT", fdText}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_PostgresToPostgres
// grades the Postgres pgoutput → Postgres lane.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_PostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	all := fdPGShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
		shapes: all,
		// No known-wrong cells: every Postgres-source DEFAULT is carried into
		// the forwarded ADD COLUMN since the carrySourceDefaults fix (all were
		// dropped before it — every pre-existing target row held NULL).
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			// KNOWN LOUD DEFECT: a jsonb column added mid-stream lands on the
			// PG target, then the first row carrying it fails to apply — but
			// only when it is NOT the stream's first forwarded column
			// (measured: jsonb alone applies; after one forwarded column of
			// any kind, even a defaultless one, it fails).
			fdLoud(t, all, "j_jsonb", "invalid input syntax for type json",
				"a jsonb column forwarded after another forward fails to apply its first row").
				afterAlter().after(fdPrelude),
			fdLoud(t, all, "j_jsonbkv", "invalid input syntax for type json",
				"a jsonb column forwarded after another forward fails to apply its first row").
				afterAlter().after(fdPrelude),
			// KNOWN LOUD DEFECT: a column of an existing enum type is
			// forwarded as a synthesised w_<col>_enum type that rejects the
			// source's own label on the first carried row.
			fdLoud(t, all, "e_mood", `invalid input value for enum w_e_mood_enum: "ok"`,
				"PG enum column forwarded as a synthesised enum without the source labels"),
			fdDesignedRefusal(fdPGNow),
		},
		freshPair: fdFreshPairs(fdPG, fdPG, src, tgt),
	})
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_ShapeAPostgresToPostgres
// grades the Postgres pgoutput → Postgres lane as a Shape A fan-in stream
// (--inject-shard-column), whose ADD COLUMN is forwarded by the ADR-0054
// boundary router, not the single-stream forwarder the other lanes grade
// (GC-36 (1)). The router shares the carry and the §2a door with that
// forwarder but has its own retarget, apply and takeover arms, so it gets
// the whole family matrix rather than one representative.
//
// One shard: the pre-existing-row fill is the holder's ALTER either way,
// and the multi-shard question — do peers agree on the carried DEFAULT —
// is the lease checksum's, pinned by
// TestRouteBoundary_AddColumn_PeerWithADifferentDefaultRefuses and, on
// real servers across three sources, by
// TestPhase2e_PG_StreamerHarness_3SourcesToTarget_ExactlyOnceApply.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_ShapeAPostgresToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	all := fdPGShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "shapeA postgres->postgres", sourceEngine: "postgres", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdPG,
		shapes:      all,
		shardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "shard_a"},
		oneAlter:    true,
		knownWrong:  map[string]fdKnownWrong{},
		halts: []fdHalt{
			fdLoud(t, all, "e_mood", `invalid input value for enum w_e_mood_enum: "ok"`,
				"PG enum column forwarded as a synthesised enum without the source labels"),
			fdDesignedRefusal(fdPGNow),
		},
		freshPair: fdFreshPairs(fdPG, fdPG, src, tgt),
	})
}

// TestStreamer_AddColumnForward_PreexistingRowDefaults_PostgresToMySQL
// grades the Postgres pgoutput → MySQL lane.
func TestStreamer_AddColumnForward_PreexistingRowDefaults_PostgresToMySQL(t *testing.T) {
	src, _, srcCleanup := startPostgresLogical(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	all := fdPGShapes()
	runForwardedDefaultLane(t, fdLane{
		name: "postgres->mysql", sourceEngine: "postgres", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdPG, tgt: fdMySQL,
		shapes: all,
		// No known-wrong cells: the default is carried (f73bb946), the MySQL
		// emitter lands TEXT/BLOB/JSON defaults (GC-36 item 4), and an
		// unconstrained NUMERIC forwards as DECIMAL(65,30) (GC-36 item 3).
		knownWrong: map[string]fdKnownWrong{},
		halts: []fdHalt{
			// A designed type refusal, not a DEFAULT defect: MySQL has no
			// type that holds a PG interval's range.
			fdLoud(t, all, "t_interval", "no interval type", ""),
			// KNOWN LOUD DEFECT (the same root as the PG → PG e_mood cell:
			// pgoutput carries no enum labels): the target ALTER is emitted
			// as an empty ENUM() and MySQL rejects its syntax.
			fdLoud(t, all, "e_mood", "error 1064",
				"PG enum column forwarded to MySQL as an empty ENUM()"),
			// KNOWN LOUD DEFECT, surfaced by the carrySourceDefaults fix (before it
			// the default was dropped and every pre-existing row landed NULL):
			// a TIMESTAMPTZ literal default carrying a UTC offset reaches MySQL
			// untranslated and the ALTER is rejected.
			fdLoud(t, all, "t_tstz", "error 1067",
				"PG TIMESTAMPTZ literal default with an offset is not translated for MySQL"),
			fdDesignedRefusal(fdPGNow),
		},
		freshPair: fdFreshPairs(fdPG, fdMySQL, src, tgt),
	})
}
