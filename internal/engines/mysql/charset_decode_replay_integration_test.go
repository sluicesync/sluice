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
}

var charsetReplayCases = []charsetReplayCase{
	{"utf8mb4 é → latin1", "utf8mb4", "latin1", "C3A9", "é"},
	{"latin1 € → cp1251", "latin1", "cp1251", "80", "€"},
	{"latin1 é → utf8mb4", "latin1", "utf8mb4", "E9", "é"},
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
	applyMySQL(t, dsn, fmt.Sprintf("ALTER TABLE %s MODIFY v VARCHAR(16) CHARACTER SET %s NULL", table, c.to))

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
	if msg := streamErr.Error(); !strings.Contains(msg, "ALREADY carries that charset") || !strings.Contains(msg, "--restart-from-scratch") {
		t.Errorf("%s: the refusal is not the charset-DDL guard's, or does not name the re-snapshot remedy: %v", what, streamErr)
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
// guard must refuse when the replay reaches the DDL, for every direction.
// Drains up to 10 changes: the stream ends at the refusal.
func TestCDCReplay_AcrossCharsetDDL_GuardRefuses_MariaDBNoLog(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	for i, c := range charsetReplayCases {
		var buf logcapture.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, c, fmt.Sprintf("r%d", i), 10)
		slog.SetDefault(prev)
		t.Logf("%s: emitted before the refusal: %#v", c.name, got)
		assertCharsetDDLGuardRefusal(t, c.name, err)
		// The table's current charset is what the rows were decoded by; the
		// WARN names a table only when that is non-UTF-8.
		if warned := strings.Contains(buf.String(), charsetHistoryUnrecordedMarker); warned != (c.to != "utf8mb4") {
			t.Errorf("%s: %s logged = %v; want %v (current charset %s). log:\n%s",
				c.name, charsetHistoryUnrecordedMarker, warned, c.to != "utf8mb4", c.to, buf.String())
		}
	}
}

// TestCDCLive_CharsetDDL_NoGuardRefusal_MariaDBNoLog: the guard's other
// direction. A stream that is LIVE when the charset DDL runs — its shape
// loaded before the DDL — must not refuse, and must decode rows on both
// sides of it by the charset they were written in.
func TestCDCLive_CharsetDDL_NoGuardRefusal_MariaDBNoLog(t *testing.T) {
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
