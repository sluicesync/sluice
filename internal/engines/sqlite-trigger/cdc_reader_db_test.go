// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver; lets these run in the unit gate (no Docker)

	"sluicesync.dev/sluice/internal/ir"
)

// These DB-backed tests run against a real temp SQLite file via the pure-Go
// modernc driver — so they execute in the plain `go test` unit gate (no Docker)
// and exercise the REAL capture trigger SQL + the REAL poll/decode path. They
// are the value-fidelity pins for the §crux load-bearing decision: every storage
// class round-trips EXACT through capture → reader, with no json-number rounding.

// bg is a tiny ctx helper for the noctx linter on seed Execs.
func bg() context.Context { return context.Background() }

// newSourceFile creates a temp SQLite file, applies stmts on a writable
// connection, and returns the path.
func newSourceFile(t *testing.T, stmts ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.ExecContext(bg(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// exec runs one statement on a fresh writable connection to path (each its own
// committed transaction, so each fires the triggers and gets its own change-log
// id).
func exec(t *testing.T, path, stmt string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open for exec: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(bg(), stmt, args...); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// collect drains a StreamChanges channel until it has at least want events (or
// the reader errors / a safety cap is hit), then returns them. The reader is a
// poller, so the test cancels the ctx once enough events are seen.
func collect(t *testing.T, r ir.CDCReader, from ir.Position, want int) []ir.Change {
	t.Helper()
	ctx, cancel := context.WithCancel(bg())
	defer cancel()
	ch, err := r.StreamChanges(ctx, from)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	var got []ir.Change
	for ev := range ch {
		got = append(got, ev)
		if len(got) >= want {
			cancel()
			// Drain the channel so the pump goroutine exits cleanly.
			for range ch {
			}
			break
		}
	}
	if errer, ok := r.(interface{ Err() error }); ok {
		if err := errer.Err(); err != nil {
			t.Fatalf("reader Err after stream: %v", err)
		}
	}
	return got
}

// pos0 is the explicit "from the beginning" position (last_id=0), so collect
// reads every change rather than "from now".
func pos0(t *testing.T) ir.Position {
	t.Helper()
	p, err := encodePos(sqliteTriggerPos{LastID: 0})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	return p
}

const (
	bigBeyond2p53 = int64(9007199254740993)    // 2^53 + 1 — a bare JSON number rounds this to …992
	maxInt64      = int64(9223372036854775807) // off by ~1193 through a JSON double
)

// TestCapture_FidelityMatrix is the Bug-74-class pin: it exercises EVERY storage
// class (integer / real / text / blob / null) AND the IR families that consume
// them (Integer, Float, Text, Blob, Decimal) through the capture trigger → poll
// → reconstruct → decodeCell path, asserting each value comes back EXACT. A
// json_object capture would silently corrupt the big integers and blob here.
func TestCapture_FidelityMatrix(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (
		id    INTEGER PRIMARY KEY,
		big   INTEGER,
		flt   REAL,
		txt   TEXT,
		blb   BLOB,
		num   NUMERIC
	)`)

	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// id=1: every class populated, big integer beyond 2^53.
	exec(t, path, `INSERT INTO t (id, big, flt, txt, blb, num) VALUES (1, ?, ?, ?, ?, ?)`,
		bigBeyond2p53, 0.1, "héllo→世界", []byte{0xde, 0xad, 0xbe, 0xef}, bigBeyond2p53)
	// id=2: max int64 (the value the JSON-double path is off by ~1193 on) + a
	// non-integer numeric (REAL storage in a NUMERIC column).
	exec(t, path, `INSERT INTO t (id, big, flt, txt, blb, num) VALUES (2, ?, ?, ?, ?, ?)`,
		maxInt64, -2.5, "", []byte{0x00, 0x01, 0x02}, 123.456)
	// id=3: all-NULL nullable columns (NULL is faithful for every type).
	exec(t, path, `INSERT INTO t (id, big, flt, txt, blb, num) VALUES (3, NULL, NULL, NULL, NULL, NULL)`)

	r, err := openCDCReader(bg(), path)
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	changes := collect(t, r, pos0(t), 3)
	if len(changes) != 3 {
		t.Fatalf("got %d changes; want 3", len(changes))
	}

	row1 := mustInsert(t, changes[0]).Row
	assertEq(t, "id1.big", row1["big"], bigBeyond2p53)
	assertEq(t, "id1.flt", row1["flt"], 0.1)
	assertEq(t, "id1.txt", row1["txt"], "héllo→世界")
	assertBytes(t, "id1.blb", row1["blb"], []byte{0xde, 0xad, 0xbe, 0xef})
	// NUMERIC integer beyond 2^53 → exact decimal string (int64 path).
	assertEq(t, "id1.num", row1["num"], "9007199254740993")

	row2 := mustInsert(t, changes[1]).Row
	assertEq(t, "id2.big", row2["big"], maxInt64)
	assertEq(t, "id2.flt", row2["flt"], -2.5)
	assertEq(t, "id2.txt", row2["txt"], "")
	assertBytes(t, "id2.blb", row2["blb"], []byte{0x00, 0x01, 0x02})
	assertEq(t, "id2.num", row2["num"], "123.456") // REAL→decimal string

	row3 := mustInsert(t, changes[2]).Row
	for _, c := range []string{"big", "flt", "txt", "blb", "num"} {
		if v := row3[c]; v != nil {
			t.Errorf("id3.%s = %#v; want nil (NULL faithful)", c, v)
		}
	}
}

// TestCapture_OrderAndWatermark pins the poll loop's id-ordered I/U/D emission,
// the before/after images, and the monotonic watermark advance.
func TestCapture_OrderAndWatermark(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, note TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	exec(t, path, `INSERT INTO t (id, n, note) VALUES (1, 10, 'a')`)
	exec(t, path, `INSERT INTO t (id, n, note) VALUES (2, 20, 'b')`)
	exec(t, path, `UPDATE t SET n = 99, note = 'a2' WHERE id = 1`)
	exec(t, path, `DELETE FROM t WHERE id = 2`)

	r, err := openCDCReader(bg(), path)
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	changes := collect(t, r, pos0(t), 4)
	if len(changes) != 4 {
		t.Fatalf("got %d changes; want 4", len(changes))
	}

	// id=1 insert, id=2 insert, id=1 update, id=2 delete — strictly id-ordered.
	mustInsert(t, changes[0])
	mustInsert(t, changes[1])

	upd, ok := changes[2].(ir.Update)
	if !ok {
		t.Fatalf("change[2] is %T; want ir.Update", changes[2])
	}
	assertEq(t, "upd.Before.n", upd.Before["n"], int64(10))
	assertEq(t, "upd.Before.note", upd.Before["note"], "a")
	assertEq(t, "upd.After.n", upd.After["n"], int64(99))
	assertEq(t, "upd.After.note", upd.After["note"], "a2")

	del, ok := changes[3].(ir.Delete)
	if !ok {
		t.Fatalf("change[3] is %T; want ir.Delete", changes[3])
	}
	assertEq(t, "del.Before.id", del.Before["id"], int64(2))

	// Watermark: the last change's position decodes to last_id == 4 (4 logged
	// rows), and positions are strictly increasing.
	var prev int64
	for i, ch := range changes {
		p, ok, err := decodePos(ch.Pos())
		if err != nil || !ok {
			t.Fatalf("change[%d] position decode: ok=%v err=%v", i, ok, err)
		}
		if p.LastID <= prev {
			t.Errorf("change[%d] last_id=%d not > prev %d", i, p.LastID, prev)
		}
		prev = p.LastID
	}
	if prev != 4 {
		t.Errorf("final watermark last_id=%d; want 4", prev)
	}
}

// TestCapture_WarmResumeSkipsAppliedIDs pins exactly-once resume: streaming from
// a durable watermark (last_id=2) emits ONLY the later changes (ids 3,4), never
// re-reading the already-applied prefix.
func TestCapture_WarmResumeSkipsAppliedIDs(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= 4; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i*10)
	}
	r, err := openCDCReader(bg(), path)
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	defer func() { _ = r.(interface{ Close() error }).Close() }()

	resume, err := encodePos(sqliteTriggerPos{LastID: 2})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	changes := collect(t, r, resume, 2)
	if len(changes) != 2 {
		t.Fatalf("got %d changes; want 2 (ids 3,4 only)", len(changes))
	}
	for i, ch := range changes {
		ins := mustInsert(t, ch)
		wantID := int64(i + 3)
		assertEq(t, "resume.id", ins.Row["id"], wantID)
	}
}

// TestSnapshotHandoff_AnchorIsMaxID pins the snapshot→CDC handoff anchor: the
// returned Position decodes to last_id == MAX(id) captured at OpenSnapshotStream,
// so CDC replays only changes after the snapshot.
func TestSnapshotHandoff_AnchorIsMaxID(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	for i := 1; i <= 3; i++ {
		exec(t, path, `INSERT INTO t (id, n) VALUES (?, ?)`, i, i)
	}
	stream, err := (Engine{}).OpenSnapshotStream(bg(), path)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	p, ok, err := decodePos(stream.Position)
	if err != nil || !ok {
		t.Fatalf("decode snapshot position: ok=%v err=%v", ok, err)
	}
	if p.LastID != 3 {
		t.Errorf("snapshot anchor last_id=%d; want 3 (MAX(id))", p.LastID)
	}
}

// TestSchemaReader_SkipsChangeLogTables pins that the cold-start schema read
// (via the trigger engine's delegated reader) NEVER surfaces the engine's own
// change-log/meta tables — so they are never migrated or self-captured.
func TestSchemaReader_SkipsChangeLogTables(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"users"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	sr, err := (Engine{}).OpenSchemaReader(bg(), path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = closeReader(sr) }()
	schema, err := sr.ReadSchema(bg())
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == ChangeLogTable || tbl.Name == ChangeLogMetaTable {
			t.Errorf("schema includes engine-internal table %q; it must be skipped", tbl.Name)
		}
	}
	// The genuine user table IS present.
	found := false
	for _, tbl := range schema.Tables {
		if tbl.Name == "users" {
			found = true
		}
	}
	if !found {
		t.Error("user table \"users\" missing from schema")
	}
}

// TestSetup_RefusesNoPK pins the loud refusal for a PK-less table (the applier
// identifies CDC rows by PK).
func TestSetup_RefusesNoPK(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE nopk (a INTEGER, b TEXT)`)
	plan, err := Setup(bg(), path, SetupOptions{Tables: []string{"nopk"}})
	if err == nil {
		t.Fatal("Setup should refuse a PK-less table")
	}
	if plan == nil || len(plan.Refusals) != 1 || plan.Refusals[0].Reason != "no-primary-key" {
		t.Fatalf("want one no-primary-key refusal; got plan=%+v err=%v", plan, err)
	}
}

// TestCapture_TemporalAndBoolFidelity pins the declared-temporal + boolean
// families through the capture trigger surface (the Bug-74 corollary: the decode
// is the shared ADR-0129 path, but the capture-trigger surface is new). It
// exercises DATE/DATETIME under BOTH the `iso` and `unixepoch` encodings and
// BOOLEAN, asserting the captured→reconstructed value equals what decodeCell
// (the cold-start reader's same path) produces for the same stored value.
func TestCapture_TemporalAndBoolFidelity(t *testing.T) {
	// --- iso encoding (the default) ---
	isoPath := newSourceFile(t, `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		dt DATETIME,
		d  DATE,
		bo BOOLEAN
	)`)
	if _, err := Setup(bg(), isoPath, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup (iso): %v", err)
	}
	exec(t, isoPath, `INSERT INTO t (id, dt, d, bo) VALUES (1, '2023-01-15 12:30:45', '2023-01-15', 1)`)
	exec(t, isoPath, `INSERT INTO t (id, dt, d, bo) VALUES (2, '2023-06-30 00:00:00', '2023-06-30', 0)`)

	r1, err := openCDCReader(bg(), isoPath)
	if err != nil {
		t.Fatalf("openCDCReader (iso): %v", err)
	}
	defer func() { _ = r1.(interface{ Close() error }).Close() }()
	iso := collect(t, r1, pos0(t), 2)
	row1 := mustInsert(t, iso[0]).Row
	assertEq(t, "iso.dt", row1["dt"], time.Date(2023, 1, 15, 12, 30, 45, 0, time.UTC))
	assertEq(t, "iso.d", row1["d"], time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC))
	assertEq(t, "iso.bo(true)", row1["bo"], true)
	assertEq(t, "iso.bo(false)", mustInsert(t, iso[1]).Row["bo"], false)

	// --- unixepoch encoding (DSN-scoped) ---
	uxPath := newSourceFile(t, `CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		dt DATETIME
	)`)
	if _, err := Setup(bg(), uxPath, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup (unixepoch): %v", err)
	}
	const epoch = int64(1673785845) // 2023-01-15 12:30:45 UTC
	exec(t, uxPath, `INSERT INTO t (id, dt) VALUES (1, ?)`, epoch)

	r2, err := openCDCReader(bg(), uxPath+"?sqlite_date_encoding=unixepoch")
	if err != nil {
		t.Fatalf("openCDCReader (unixepoch): %v", err)
	}
	defer func() { _ = r2.(interface{ Close() error }).Close() }()
	ux := collect(t, r2, pos0(t), 1)
	assertEq(t, "unixepoch.dt", mustInsert(t, ux[0]).Row["dt"], time.Unix(epoch, 0).UTC())
}

// TestCapture_RefusesAddColumnDrift pins the MEDIUM silent-loss guard: an
// ADD COLUMN after `trigger setup` without a re-setup leaves the stale trigger
// capturing the OLD column set — every captured column is still present, so the
// per-decode check never fires, and the new column's values would SILENTLY drop.
// The startup fingerprint check must refuse loudly instead.
func TestCapture_RefusesAddColumnDrift(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	exec(t, path, `INSERT INTO t (id, v) VALUES (1, 'a')`)
	// Source schema change WITHOUT re-running setup.
	exec(t, path, `ALTER TABLE t ADD COLUMN extra TEXT`)

	_, err := openCDCReader(bg(), path)
	if err == nil {
		t.Fatal("openCDCReader should refuse: an un-re-setup ADD COLUMN would silently drop the new column")
	}
	if !strings.Contains(err.Error(), "drift") || !strings.Contains(err.Error(), "trigger setup") {
		t.Errorf("drift refusal should name the drift + recovery action; got: %v", err)
	}
}

// TestCapture_RefusesDroppedTableDrift pins the other direction: a replicated
// table dropped after setup (live fingerprint absent) is refused at stream start.
func TestCapture_RefusesDroppedTableDrift(t *testing.T) {
	path := newSourceFile(t, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
	if _, err := Setup(bg(), path, SetupOptions{Tables: []string{"t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	exec(t, path, `DROP TABLE t`)
	if _, err := openCDCReader(bg(), path); err == nil {
		t.Fatal("openCDCReader should refuse when a replicated table no longer exists")
	}
}

// --- assertion helpers ---

func mustInsert(t *testing.T, ch ir.Change) ir.Insert {
	t.Helper()
	ins, ok := ch.(ir.Insert)
	if !ok {
		t.Fatalf("change is %T; want ir.Insert", ch)
	}
	return ins
}

func assertEq(t *testing.T, what string, got, want any) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %#v (%T); want %#v (%T)", what, got, got, want, want)
	}
}

func assertBytes(t *testing.T, what string, got any, want []byte) {
	t.Helper()
	b, ok := got.([]byte)
	if !ok {
		t.Errorf("%s = %#v (%T); want []byte", what, got, got)
		return
	}
	if len(b) != len(want) {
		t.Errorf("%s len=%d; want %d (%x vs %x)", what, len(b), len(want), b, want)
		return
	}
	for i := range want {
		if b[i] != want[i] {
			t.Errorf("%s = %x; want %x", what, b, want)
			return
		}
	}
}
