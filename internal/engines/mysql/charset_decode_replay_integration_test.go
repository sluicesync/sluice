//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) follow-up: a change-stream value replayed across a charset DDL —
// the stored position predates an `ALTER … MODIFY … CHARACTER SET` — must be
// decoded by the charset it was WRITTEN in, or refused; never decoded by the
// post-DDL charset at exit 0.
//
// # What this pins, and the independent expected value
//
// MEASURED on MySQL 8 against the first cut (which decoded by the catalog):
// utf8mb4 'é' replayed after a change to latin1 emitted 'Ã©', latin1 '€'
// after a change to cp1251 emitted 'Ђ', both at exit 0.
//
//   - Where the source writes the per-column collation into the TABLE_MAP
//     (MySQL 8's default binlog_row_metadata=MINIMAL; MariaDB MINIMAL/FULL),
//     that is the server's own statement of the written charset, and the
//     replay decodes by it: the expected value is the value inserted,
//     spelled as its bytes in the charset the insert named (_utf8mb4
//     X'C3A9' is 'é', _latin1 X'80' is '€').
//   - Where it does not (MariaDB's default NO_LOG), the charset-DDL guard
//     must refuse the stream with SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH when
//     the replay reaches the DDL. The rows before the DDL are emitted first
//     — the documented window the refusal's remedy (re-snapshot) repairs.
//
// The big5 loop is the one the first cut's remedy invited: big5 refused,
// operator converts to utf8mb4, resumes. Decoding by the recorded charset,
// the replayed row is still big5 and still refuses as not decodable — the
// big5 C3A9 (矇) is never emitted as utf8mb4 'é'.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// charsetReplayCase records a value in charset from, converts the column to
// charset to, then replays from before the value. want is the inserted value.
type charsetReplayCase struct {
	name, from, to, valueHex, want string
	// alter, when set, is the ALTER to run (%s is the table) in place of the
	// plain MODIFY to charset to.
	alter string
}

// mariaDBAlterSyntaxReplayCases are utf8mb4 'é' replayed across a change to
// latin1 spelled in MariaDB-only syntax the Vitess parser refuses or
// truncates at (MEASURED), a quoted charset, and a MariaDB-only collation
// name. The last case is one the parser cannot classify even after the
// prefix is normalised, which the fail-safe refuses.
var mariaDBAlterSyntaxReplayCases = []charsetReplayCase{
	{name: "ALTER TABLE IF EXISTS", alter: "ALTER TABLE IF EXISTS %s MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL"},
	{name: "ALTER ONLINE TABLE", alter: "ALTER ONLINE TABLE %s MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL"},
	{name: "WAIT n", alter: "ALTER TABLE %s WAIT 5 MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL"},
	{name: "NOWAIT", alter: "ALTER TABLE %s NOWAIT MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL"},
	{name: "quoted charset", alter: "ALTER TABLE %s MODIFY v VARCHAR(16) CHARACTER SET 'latin1' NULL"},
	{name: "MariaDB-only collation name", alter: "ALTER TABLE %s MODIFY v VARCHAR(16) COLLATE latin1_swedish_nopad_ci NULL"},
	{name: "unclassifiable (column IF EXISTS) — the fail-safe", alter: "ALTER TABLE %s MODIFY COLUMN IF EXISTS v VARCHAR(16) CHARACTER SET latin1 NULL"},
}

var charsetReplayCases = []charsetReplayCase{
	{name: "utf8mb4 é → latin1", from: "utf8mb4", to: "latin1", valueHex: "C3A9", want: "é"},
	{name: "latin1 € → cp1251", from: "latin1", to: "cp1251", valueHex: "80", want: "€"},
	{name: "latin1 é → utf8mb4", from: "latin1", to: "utf8mb4", valueHex: "E9", want: "é"},
}

// replayAcrossCharsetDDL runs one case against dsn and returns what the
// replay emitted — up to want changes, or until the stream ends — and the
// stream's terminal error.
func replayAcrossCharsetDDL(t *testing.T, dsn string, flavor Flavor, c charsetReplayCase, table string, want int) ([]ir.Change, error) {
	t.Helper()
	applyMySQL(t, dsn, fmt.Sprintf(
		"CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET %s NULL) ENGINE=InnoDB", table, c.from,
	))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{Flavor: flavor}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	resumeFrom := stream.Position
	_ = stream.Close()

	applyMySQL(t, dsn, fmt.Sprintf("INSERT INTO %s VALUES (1, _%s X'%s')", table, c.from, c.valueHex))
	alter := fmt.Sprintf("ALTER TABLE %s MODIFY v VARCHAR(16) CHARACTER SET %s NULL", table, c.to)
	if c.alter != "" {
		alter = fmt.Sprintf(c.alter, table)
	}
	applyMySQL(t, dsn, alter)

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if cl, ok := rdr.(interface{ Close() error }); ok {
			_ = cl.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, resumeFrom)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	got := drainChanges(t, ctx, changes, want, 20*time.Second)
	return got, rdr.(*CDCReader).Err()
}

// assertReplayExact requires the replayed insert to carry the value as it
// was written, with no stream error.
func assertReplayExact(t *testing.T, c charsetReplayCase, got []ir.Change, streamErr error) {
	t.Helper()
	if streamErr != nil {
		t.Fatalf("%s: stream error = %v; want the replay decoded by the TABLE_MAP's recorded charset", c.name, streamErr)
	}
	if len(got) != 1 {
		t.Fatalf("%s: got %d changes; want the one replayed insert", c.name, len(got))
	}
	ins, _ := got[0].(ir.Insert)
	if ins.Row["v"] != c.want {
		t.Errorf("%s: replayed v = %q (% X); want %q, the value as written in %s", c.name, ins.Row["v"], ins.Row["v"], c.want, c.from)
	}
}

// assertCharsetDDLGuardRefusal requires the stream to end in the guard's
// replay-mismatch refusal. Rows before the DDL may have been emitted: that
// is the stated window, and the refusal must say they need re-copying.
func assertCharsetDDLGuardRefusal(t *testing.T, what string, streamErr error) {
	t.Helper()
	var ce *sluicecode.CodedError
	if !errors.As(streamErr, &ce) || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("%s: stream error = %v; want the charset-DDL guard's %s", what, streamErr, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	msg := streamErr.Error()
	guard := strings.Contains(msg, "ALREADY carries that charset") || strings.Contains(msg, "could not parse it or could not name its collation")
	if !guard || !strings.Contains(msg, "--restart-from-scratch") || !strings.Contains(msg, "take a fresh full backup") {
		t.Errorf("%s: the refusal is not the charset-DDL guard's, or does not name the re-copy for sync and for a backup chain: %v", what, streamErr)
	}
}

// TestCDCReplay_AcrossCharsetDDL_DecodesWritten_MySQL: MySQL 8 writes column
// charsets into the TABLE_MAP under its default MINIMAL metadata.
func TestCDCReplay_AcrossCharsetDDL_DecodesWritten_MySQL(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "gc37j_replay_db")
	defer cleanup()
	for i, c := range charsetReplayCases {
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorVanilla, c, fmt.Sprintf("r%d", i), 1)
		assertReplayExact(t, c, got, err)
	}
}

// TestCDCReplay_Big5RefuseConvertResume_MySQL is the loop the first cut's
// remedy invited: a big5 value refuses as CHARSET-NOT-DECODABLE; the
// operator converts the column to utf8mb4 and resumes from the same
// position. The resume must still refuse (the history is big5), not emit
// big5 C3A9 (矇) as utf8mb4 'é'.
func TestCDCReplay_Big5RefuseConvertResume_MySQL(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "gc37j_big5_loop_db")
	defer cleanup()
	applyMySQL(t, dsn, "CREATE TABLE b (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET big5 NULL) ENGINE=InnoDB")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{Flavor: FlavorVanilla}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	resumeFrom := stream.Position
	_ = stream.Close()

	stream1 := func() ([]ir.Change, error) {
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		defer func() { _ = rdr.(interface{ Close() error }).Close() }()
		changes, err := rdr.StreamChanges(ctx, resumeFrom)
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		got := drainChanges(t, ctx, changes, 1, 20*time.Second)
		return got, rdr.(*CDCReader).Err()
	}

	applyMySQL(t, dsn, "INSERT INTO b VALUES (1, _big5 X'C3A9')")
	got, err := stream1()
	if len(got) != 0 || !errors.Is(err, errCharsetNotDecodable) {
		t.Fatalf("first run: got %#v, err %v; want %s and no row", got, err, charsetNotDecodableMarker)
	}
	if !strings.Contains(err.Error(), "--restart-from-scratch") {
		t.Errorf("the refusal's remedy must say re-snapshot, not convert-and-resume: %v", err)
	}

	// The operator converts the column, then resumes from the same place.
	applyMySQL(t, dsn, "ALTER TABLE b MODIFY v VARCHAR(16) CHARACTER SET utf8mb4 NULL")
	got, err = stream1()
	if len(got) != 0 || !errors.Is(err, errCharsetNotDecodable) {
		t.Fatalf("big5 → utf8mb4 resume: got %#v, err %v; want the recorded big5 to refuse again, no row", got, err)
	}
}

// TestCDCReplay_AcrossCharsetDDL_DecodesWritten_MariaDBMinimal: MariaDB
// writes the charsets too once binlog_row_metadata is MINIMAL or FULL — and
// names MariaDB-only collation IDs (uca1400) the server's own tables list.
func TestCDCReplay_AcrossCharsetDDL_DecodesWritten_MariaDBMinimal(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--binlog-row-metadata=MINIMAL")
	defer cleanup()
	for i, c := range charsetReplayCases {
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, c, fmt.Sprintf("r%d", i), 1)
		assertReplayExact(t, c, got, err)
	}
}

// TestCDCReplay_AcrossCharsetDDL_GuardRefuses_MariaDBNoLog: under MariaDB's
// default binlog_row_metadata=NO_LOG the TABLE_MAP carries no charset, so
// the replayed rows are decoded by the current catalog; the charset-DDL
// guard must refuse when the replay reaches a DDL into a non-UTF-8 charset.
// A replay into utf8mb4 is NOT refused (third review, item 1a): its old
// bytes are carried by passthrough exactly as v0.156.2 carried them — here
// latin1 0xE9, invalid UTF-8, which every target and the backup codec
// refuse loudly. Drains up to 10 changes: the stream ends at the refusal.
func TestCDCReplay_AcrossCharsetDDL_GuardRefuses_MariaDBNoLog(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	for i, c := range charsetReplayCases {
		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		// Drain past the ALTER in every case — a refusal ends the stream
		// early; a non-refusing one runs to the drain timeout — so the UTF-8
		// case's "not refused" is read after the stream has reached the DDL.
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, c, fmt.Sprintf("r%d", i), 10)
		slog.SetDefault(prev)
		if c.to == "utf8mb4" {
			if err != nil {
				t.Errorf("%s: err = %v; a replay into a UTF-8 charset is carried as v0.156.2 carried it, not refused", c.name, err)
			} else if len(got) != 1 {
				t.Errorf("%s: got %d changes; want the replayed insert", c.name, len(got))
			} else if ins, _ := got[0].(ir.Insert); ins.Row["v"] != "\xe9" {
				t.Errorf("%s: replayed v = %q; want the stored byte 0xE9 carried as-is (the v0.156.2 behaviour)", c.name, ins.Row["v"])
			}
		} else {
			t.Logf("%s: emitted before the refusal: %#v", c.name, got)
			assertCharsetDDLGuardRefusal(t, c.name, err)
		}
		// The table's current charset is what the rows were decoded by; the
		// WARN names a table only when that is non-UTF-8.
		if warned := strings.Contains(buf.String(), charsetHistoryUnrecordedMarker); warned != (c.to != "utf8mb4") {
			t.Errorf("%s: %s logged = %v; want %v (current charset %s). log:\n%s",
				c.name, charsetHistoryUnrecordedMarker, warned, c.to != "utf8mb4", c.to, buf.String())
		}
	}
}

// TestCDCReplay_MariaDBAlterSyntax_GuardRefuses_MariaDBNoLog: a replay into
// latin1 whose ALTER is spelled in MariaDB-only syntax, a quoted charset or a
// MariaDB-only collation name must refuse exactly like the plain spelling —
// before the third review each of these was silently unclassified — and one
// the parser cannot classify at all refuses through the fail-safe.
func TestCDCReplay_MariaDBAlterSyntax_GuardRefuses_MariaDBNoLog(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	for i, c := range mariaDBAlterSyntaxReplayCases {
		c.from, c.to, c.valueHex, c.want = "utf8mb4", "latin1", "C3A9", "é"
		_, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, c, fmt.Sprintf("s%d", i), 10)
		assertCharsetDDLGuardRefusal(t, c.name, err)
		// Which guard refused matters: every spelling but the last must be
		// CLASSIFIED (the fail-safe would refuse them too, and would hide a
		// normaliser or namer that stopped working).
		failSafe := i == len(mariaDBAlterSyntaxReplayCases)-1
		if err != nil && strings.Contains(err.Error(), "could not parse it or could not name its collation") != failSafe {
			t.Errorf("%s: refused by the wrong door (want fail-safe = %v): %v", c.name, failSafe, err)
		}
	}
}

// TestCDCLive_CharsetDDL_NoGuardRefusal_MariaDBNoLog: the guard's other
// direction. A stream that is LIVE when a charset DDL runs — its shape
// loaded before the DDL — must not refuse, and must decode rows on both
// sides of it by the charset they were written in. The third review
// MEASURED the routine DDLs below refusing a live stream (charset names
// compared, collations not, UTF-8 targets guarded), where v0.156.2 refused
// none; each runs here with a row decoded just before it, so the guard
// compares against a live cached shape every time.
func TestCDCLive_CharsetDDL_NoGuardRefusal_MariaDBNoLog(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	charsetLiveDDLLane(t, dsn)
}

// charsetLiveDDLLane runs the live no-refusal sequence on a binlog source.
func charsetLiveDDLLane(t *testing.T, dsn string) {
	t.Helper()
	applyMySQL(t, dsn, "CREATE TABLE u (id INT NOT NULL PRIMARY KEY, v VARCHAR(64) CHARACTER SET utf8mb4 NULL) ENGINE=InnoDB")
	applyMySQL(t, dsn, "CREATE TABLE l1 (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET latin1 NULL) ENGINE=InnoDB")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	eng := Engine{Flavor: FlavorMariaDB}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	resumeFrom := stream.Position
	_ = stream.Close()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(interface{ Close() error }).Close() }()
	changes, err := rdr.StreamChanges(ctx, resumeFrom)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	id := 0
	for _, step := range []struct{ table, value, ddl string }{
		{"u", "_utf8mb4 X'C3A9'", "ALTER TABLE u CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"},
		{"u", "_utf8mb4 X'C3A9'", "ALTER TABLE u MODIFY v VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 MODIFY v VARCHAR(16) COLLATE latin1_bin"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 MODIFY v VARCHAR(16) CHARACTER SET latin1 COLLATE latin1_general_ci"},
		{"l1", "_latin1 X'E9'", "ALTER TABLE l1 CONVERT TO CHARACTER SET latin1 COLLATE latin1_swedish_ci"},
		{"u", "_utf8mb4 X'C3A9'", ""},
		{"l1", "_latin1 X'E9'", ""},
	} {
		id++
		applyMySQL(t, dsn, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s)", step.table, id, step.value))
		got := drainChanges(t, ctx, changes, 1, 20*time.Second)
		if err := rdr.(*CDCReader).Err(); err != nil {
			t.Fatalf("live stream: err = %v after the DDL before row %d; want no refusal", err, id)
		}
		if len(got) != 1 {
			t.Fatalf("row %d of %s did not stream", id, step.table)
		}
		if ins, _ := got[0].(ir.Insert); ins.Row["v"] != "é" {
			t.Errorf("live row %d of %s: v = %q; want %q", id, step.table, ins.Row["v"], "é")
		}
		if step.ddl != "" {
			applyMySQL(t, dsn, step.ddl)
		}
	}
}

// TestCDCLive_CharsetDDLIntoLatin1_NoGuardRefusal_MariaDBNoLog is the first
// review's live pin: a live change INTO latin1 decodes both sides exactly.
func TestCDCLive_CharsetDDLIntoLatin1_NoGuardRefusal_MariaDBNoLog(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	applyMySQL(t, dsn, "CREATE TABLE l (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL) ENGINE=InnoDB")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	eng := Engine{Flavor: FlavorMariaDB}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	resumeFrom := stream.Position
	_ = stream.Close()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() { _ = rdr.(interface{ Close() error }).Close() }()
	changes, err := rdr.StreamChanges(ctx, resumeFrom)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	applyMySQL(t, dsn, "INSERT INTO l VALUES (1, _utf8mb4 X'C3A9')")
	before := drainChanges(t, ctx, changes, 1, 20*time.Second)
	applyMySQL(t, dsn, "ALTER TABLE l MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL")
	applyMySQL(t, dsn, "INSERT INTO l VALUES (2, _latin1 X'E9')")
	after := drainChanges(t, ctx, changes, 1, 20*time.Second)
	if err := rdr.(*CDCReader).Err(); err != nil {
		t.Fatalf("live stream across a charset DDL: err = %v; want no refusal", err)
	}
	for _, g := range [][]ir.Change{before, after} {
		if len(g) != 1 {
			t.Fatalf("got %d changes around the DDL; want 1 each side", len(g))
		}
		if ins, _ := g[0].(ir.Insert); ins.Row["v"] != "é" {
			t.Errorf("live row v = %q; want %q", ins.Row["v"], "é")
		}
	}
}
