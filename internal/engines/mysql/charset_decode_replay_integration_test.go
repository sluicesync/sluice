//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) follow-up, review F1: a change-stream value is decoded by the
// charset it was WRITTEN in, not the one the catalog shows now.
//
// # What this pins, and the independent expected value
//
// A replay across a charset DDL — the stored position predates an
// `ALTER … MODIFY … CHARACTER SET` — used to decode old bytes by the new
// charset. MEASURED on MySQL 8 before this follow-up: utf8mb4 'é' emitted
// as latin1 'Ã©', latin1 '€' as cp1251 'Ђ', both at exit 0; and the loop the
// first cut's own remedy invited — big5 refused, operator converts the
// column to utf8mb4, resumes — emitted big5 C3A9 (truth 矇) as 'é', silently.
//
// The expected value is the SERVER's statement of what the bytes were
// written in: the TABLE_MAP's per-column collation, which MySQL 8 writes
// under its default binlog_row_metadata=MINIMAL. Where it disagrees with
// the catalog, the stream must refuse (SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH,
// re-snapshot) BEFORE emitting a row. On MariaDB's default NO_LOG there is
// no such statement; that lane pins the limitation as the documented,
// known-wrong behaviour so a change to it is noticed.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// charsetReplayCase records a value in charset from, converts the column to
// charset to, then replays from before the value.
type charsetReplayCase struct {
	name, from, to, valueHex string
}

var charsetReplayCases = []charsetReplayCase{
	{"utf8mb4 é → latin1", "utf8mb4", "latin1", "C3A9"},
	{"latin1 € → cp1251", "latin1", "cp1251", "80"},
	{"latin1 é → utf8mb4", "latin1", "utf8mb4", "E9"},
}

// replayAcrossCharsetDDL runs one case against dsn and returns what the
// replay emitted and the stream's terminal error.
func replayAcrossCharsetDDL(t *testing.T, dsn string, flavor Flavor, c charsetReplayCase, table string) ([]ir.Change, error) {
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
	got := drainChanges(t, ctx, changes, 1, 20*time.Second)
	return got, rdr.(*CDCReader).Err()
}

// assertReplayMismatchRefusal requires the replay to have emitted nothing
// and refused with the CDC-4 class naming the column.
func assertReplayMismatchRefusal(t *testing.T, what string, got []ir.Change, streamErr error) {
	t.Helper()
	if len(got) != 0 {
		t.Errorf("%s: the replay emitted %#v decoded by the post-DDL charset; want a refusal before any row", what, got)
	}
	var ce *sluicecode.CodedError
	if !errors.As(streamErr, &ce) || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("%s: stream error = %v; want %s", what, streamErr, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if !strings.Contains(streamErr.Error(), `column "v"`) || !strings.Contains(streamErr.Error(), "CHARACTER SET") {
		t.Errorf("%s: refusal does not name the column and the charsets: %v", what, streamErr)
	}
}

// TestCDCReplay_AcrossCharsetDDL_Refuses_MySQL: MySQL 8 writes column
// charsets into the TABLE_MAP under its default MINIMAL metadata.
func TestCDCReplay_AcrossCharsetDDL_Refuses_MySQL(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "gc37j_replay_db")
	defer cleanup()
	for i, c := range charsetReplayCases {
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorVanilla, c, fmt.Sprintf("r%d", i))
		assertReplayMismatchRefusal(t, c.name, got, err)
	}
}

// TestCDCReplay_Big5RefuseConvertResume_MySQL is the loop the first cut's
// remedy invited: a big5 value refuses as CHARSET-NOT-DECODABLE; the
// operator converts the column to utf8mb4 and resumes from the same
// position. The resume must refuse (the history is big5), not emit big5
// C3A9 (矇) as utf8mb4 'é'.
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
	assertReplayMismatchRefusal(t, "big5 → utf8mb4 resume", got, err)
}

// TestCDCReplay_AcrossCharsetDDL_Refuses_MariaDBMinimal: MariaDB writes the
// charsets too once binlog_row_metadata is MINIMAL or FULL.
func TestCDCReplay_AcrossCharsetDDL_Refuses_MariaDBMinimal(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image, "--binlog-row-metadata=MINIMAL")
	defer cleanup()
	for i, c := range charsetReplayCases {
		got, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, c, fmt.Sprintf("r%d", i))
		assertReplayMismatchRefusal(t, c.name, got, err)
	}
}

// TestCDCReplay_AcrossCharsetDDL_MariaDBNoLogLimitation pins the LIMITATION
// the docs state: under MariaDB's default binlog_row_metadata=NO_LOG the
// TABLE_MAP carries no charset, so a replay across a charset DDL is decoded
// by the current charset — utf8mb4 'é' (C3A9) emitted as latin1 'Ã©'. This
// asserts the known-wrong value on purpose: if it ever changes (MariaDB
// starts logging charsets, or sluice gains another source of truth), this
// test fails and the documented limitation must be revisited.
func TestCDCReplay_AcrossCharsetDDL_MariaDBNoLogLimitation(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	got, err := replayAcrossCharsetDDL(t, dsn, FlavorMariaDB, charsetReplayCases[0], "r0")
	if err != nil {
		t.Fatalf("stream error = %v; the NO_LOG limitation is a silent decode, not a refusal", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d changes; want the one replayed insert", len(got))
	}
	ins, _ := got[0].(ir.Insert)
	if ins.Row["v"] != "Ã©" {
		t.Fatalf("NO_LOG replay emitted v=%q; the documented limitation says %q (utf8mb4 é decoded as latin1) — "+
			"if this changed, revisit the limitation in charset_decode.go and migrating-legacy-mysql.md", ins.Row["v"], "Ã©")
	}
}
