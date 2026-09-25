//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// TestLaneKey_CopyReadAndBinlogRouteAlike is the provenance half of the
// laneapply same-row-same-lane premise (the change-kind half is the
// Postgres TestCDCDecodeTypeStableAcrossChangeKinds; the backup-chunk half
// is blobcodec's TestCanonicalKeyValue_SurvivesTheBackupRoundTrip).
//
// # Why provenance matters now
//
// A backup chain's ADD COLUMN fill (pipeline incremental_add_column_fill.go)
// reads key + added columns with the COPY reader and replays them in the
// same change chunk as the window's binlog changes, and chain restore
// fans that chunk across concurrent lanes by primary-key hash. So one row
// reaches the router from both readers. The value contract permits the two
// to disagree on Go kind (an unsigned column may be int64 or uint64,
// docs/value-types.md), and before 2026-09-24 the router encoded by kind:
// a MySQL INT UNSIGNED key read by the text-protocol copy reader (int64)
// and by the binlog (uint64) hashed apart, so the fill's UPDATE could
// commit before the window's INSERT of the same row and match nothing (the
// value-fidelity review, finding 2).
//
// # What it asserts
//
// For every key family × a real server: the canonical lane-key bytes of
// the value the copy reader returns equal those of the value the binlog
// reader returns for the same row, and the router agrees. The independent
// expected value is the other reader's own decode of the same stored row.
// Every family is asserted — the ones that passed before the fix too —
// because the decimal cell rests on a stated premise (both readers render
// a DECIMAL at the column's scale) that this binds.
func TestLaneKey_CopyReadAndBinlogRouteAlike(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	cells := []struct{ name, ddlType, literal string }{
		{"tinyint_unsigned", "TINYINT UNSIGNED", "200"},
		{"smallint_unsigned", "SMALLINT UNSIGNED", "60000"},
		{"mediumint_unsigned", "MEDIUMINT UNSIGNED", "16000000"},
		{"int_unsigned", "INT UNSIGNED", "4000000000"},
		{"bigint_unsigned", "BIGINT UNSIGNED", "18446744073709551615"},
		{"bigint_unsigned_small", "BIGINT UNSIGNED", "42"},
		{"int", "INT", "-42"},
		{"bigint", "BIGINT", "-9000000000"},
		{"varchar", "VARCHAR(32)", "'k-42'"},
		{"char", "CHAR(8)", "'k-42'"},
		{"varbinary", "VARBINARY(8)", "0x0102FF"},
		{"decimal", "DECIMAL(10,2)", "1.50"},
		{"date", "DATE", "'2026-09-24'"},
		// GC-37 (j) review: non-UTF-8 text keys. The copy reader gets them
		// converted by the server; the binlog now converts them by the
		// column's charset — both must hand the router the same UTF-8 key.
		{"latin1_e_acute", "VARCHAR(16) CHARACTER SET latin1", "_latin1 X'E9'"},
		{"latin1_C3A9", "VARCHAR(16) CHARACTER SET latin1", "_latin1 X'C3A9'"},
		{"latin1_char_trailing_spaces", "CHAR(8) CHARACTER SET latin1", "_latin1 X'E92020'"},
		{"utf16_char", "CHAR(4) CHARACTER SET utf16", "_utf16 X'00E9'"},
		{"utf16le_char", "CHAR(4) CHARACTER SET utf16le", "_utf16le X'E900'"},
		{"ucs2_char", "CHAR(4) CHARACTER SET ucs2", "_ucs2 X'00E9'"},
		{"utf32_char", "CHAR(4) CHARACTER SET utf32", "_utf32 X'000000E9'"},
		{"sjis", "VARCHAR(16) CHARACTER SET sjis", "_sjis X'82A0'"},
		{"swe7", "VARCHAR(16) CHARACTER SET swe7", "_swe7 X'7B'"},
		{"gbk", "VARCHAR(16) CHARACTER SET gbk", "_gbk X'D6D0'"},
		{"cp1251", "VARCHAR(16) CHARACTER SET cp1251", "_cp1251 X'C0'"},
	}
	ddl := ""
	for i, c := range cells {
		ddl += fmt.Sprintf("CREATE TABLE lk_%02d (pk %s NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB;\n", i, c.ddlType)
	}
	applyMySQL(t, dsn, ddl)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	dml := ""
	for i, c := range cells {
		dml += fmt.Sprintf("INSERT INTO lk_%02d (pk, v) VALUES (%s, 1);\n", i, c.literal)
	}
	applyMySQL(t, dsn, dml)
	got := drainChanges(t, ctx, changes, len(cells), time.Minute)
	if len(got) != len(cells) {
		t.Fatalf("binlog delivered %d of %d inserts", len(got), len(cells))
	}
	binlogKey := map[string]any{}
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("got %T; want ir.Insert", c)
		}
		binlogKey[ins.Table] = ins.Row["pk"]
	}

	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tables := map[string]*ir.Table{}
	for _, tbl := range schema.Tables {
		tables[tbl.Name] = tbl
	}
	router := laneapply.NewRouter(997)
	for i, c := range cells {
		name := fmt.Sprintf("lk_%02d", i)
		t.Run(c.name, func(t *testing.T) {
			tbl := tables[name]
			if tbl == nil {
				t.Fatalf("schema has no table %s", name)
			}
			rr, err := eng.OpenRowReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowReader: %v", err)
			}
			rows, err := rr.ReadRows(ctx, tbl)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			var copyKey any
			n := 0
			for row := range rows {
				copyKey = row["pk"]
				n++
			}
			if err := rr.Err(); err != nil || n != 1 {
				t.Fatalf("copy read %d rows (err %v); want 1", n, err)
			}
			if c, ok := rr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
			var copyBytes, cdcBytes bytes.Buffer
			laneapply.WriteCanonicalKeyValue(&copyBytes, copyKey)
			laneapply.WriteCanonicalKeyValue(&cdcBytes, binlogKey[name])
			if !bytes.Equal(copyBytes.Bytes(), cdcBytes.Bytes()) {
				t.Errorf("copy read %T %v encodes %q, binlog %T %v encodes %q — one row, two lanes",
					copyKey, copyKey, copyBytes.Bytes(), binlogKey[name], binlogKey[name], cdcBytes.Bytes())
			}
			if lc, lb := router.LaneFor("source_db."+name, []any{copyKey}), router.LaneFor("source_db."+name, []any{binlogKey[name]}); lc != lb {
				t.Errorf("copy-read key routes to lane %d, binlog key to lane %d", lc, lb)
			}
		})
	}
}
