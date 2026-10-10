//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 301 against a real server: a reader that starts behind the binlog
// head decodes every row by the shape it was RECORDED under, or refuses
// before decoding any row of the table.
//
// The reader is the one every MySQL-family capture path opens — a
// `backup incremental` window, a `backup stream` restart, a warm-resumed
// `sync start`, a live one that lags, and the CDC phase after a cold copy
// all call StreamChanges at a position the server has since moved past.
// The reader cannot tell those apart, so one matrix at this level covers
// the mechanism for all of them; internal/pipeline pins the paths end to
// end.
//
// The independent expected value is the literal row each statement wrote,
// named by the columns the table had WHEN it was written. It is derived
// from the test's own SQL, not from any shape sluice read.

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// shapeChange is one expected change: the verb and the row (the after-image
// of an update, the PK-narrowed before-image of a delete).
type shapeChange struct {
	op  string
	row ir.Row
}

// shapeCase is one DDL shape. Every case starts from
// kd(id INT PRIMARY KEY, b VARCHAR(40), c VARCHAR(40)) holding (1,'101','201').
type shapeCase struct {
	name string
	// history runs after the start position is captured: rows recorded
	// before the DDL, the DDL itself, and rows after it.
	history string
	// want is every change of kd the history writes, in order.
	want []shapeChange
	// minimalDecodes: a shape the reader can decode without column names
	// (an in-place change whose recorded types still decode the same).
	// Every other case refuses under MINIMAL before any row is decoded.
	minimalDecodes bool
	// fullRefuses: a shape not even FULL metadata can type (a column the
	// binlog cannot describe on its own).
	fullRefuses bool
	// retypesInPlace: the MINIMAL decode rests on the per-event recheck of an
	// in-place redefinition, which a source that records no charset or
	// signedness (MariaDB NO_LOG) cannot pass — it refuses there instead.
	retypesInPlace bool
	// fullRefusalCode is the code a FULL-metadata refusal carries when it is
	// not the replay mismatch.
	fullRefusalCode sluicecode.Code
}

// preDDL is the history every case records before its DDL.
const preDDL = "INSERT INTO kd VALUES (2,'102','202'); UPDATE kd SET b='111' WHERE id=1; DELETE FROM kd WHERE id=2;"

func preDDLWant() []shapeChange {
	return []shapeChange{
		{"insert", ir.Row{"id": int64(2), "b": "102", "c": "202"}},
		{"update", ir.Row{"id": int64(1), "b": "111", "c": "201"}},
		{"delete", ir.Row{"id": int64(2)}},
	}
}

func shapeCases() []shapeCase {
	with := func(tail ...shapeChange) []shapeChange { return append(preDDLWant(), tail...) }
	return []shapeCase{
		{
			name:    "add-column",
			history: preDDL + " ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd VALUES (3,'103','203',3);",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "d": int64(3)}}),
		},
		{
			// The filing's control shape: the DDL comes first, so every row
			// the window holds was recorded under the current shape.
			name:           "ddl-before-every-row",
			history:        "ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd VALUES (3,'103','203',3);",
			want:           []shapeChange{{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "d": int64(3)}}},
			minimalDecodes: true,
		},
		{
			name:    "drop-column",
			history: preDDL + " ALTER TABLE kd DROP COLUMN c; INSERT INTO kd VALUES (3,'103');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "b": "103"}}),
		},
		{
			name:    "reorder-same-type",
			history: preDDL + " ALTER TABLE kd MODIFY c VARCHAR(40) AFTER id; INSERT INTO kd(id,b,c) VALUES (3,'103','203');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203"}}),
		},
		{
			name:    "rename-swap",
			history: preDDL + " ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b; INSERT INTO kd(id,b,c) VALUES (3,'103','203');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203"}}),
		},
		{
			name:    "rename",
			history: preDDL + " ALTER TABLE kd RENAME COLUMN b TO x; INSERT INTO kd(id,x,c) VALUES (3,'103','203');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "x": "103", "c": "203"}}),
		},
		{
			name:    "drop-add-same-type",
			history: preDDL + " ALTER TABLE kd DROP COLUMN b, ADD COLUMN e VARCHAR(40); INSERT INTO kd(id,c,e) VALUES (3,'203','303');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "c": "203", "e": "303"}}),
		},
		{
			name:    "type-change",
			history: preDDL + " ALTER TABLE kd MODIFY b INT; INSERT INTO kd VALUES (3,103,'203');",
			want:    with(shapeChange{"insert", ir.Row{"id": int64(3), "b": int64(103), "c": "203"}}),
		},
		{
			name:           "widen-in-place",
			history:        preDDL + " ALTER TABLE kd MODIFY b VARCHAR(80); INSERT INTO kd VALUES (3,'103','203');",
			want:           with(shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203"}}),
			minimalDecodes: true,
			retypesInPlace: true,
		},
		{
			// The cache is cleared mid-window by a DDL that changes nothing,
			// and reloaded while the column add is already in the catalog.
			name: "cache-cleared-then-add",
			history: preDDL + " ALTER TABLE kd COMMENT='clears the cache'; INSERT INTO kd VALUES (4,'104','204');" +
				" ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd VALUES (3,'103','203',3);",
			want: with(
				shapeChange{"insert", ir.Row{"id": int64(4), "b": "104", "c": "204"}},
				shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "d": int64(3)}},
			),
		},
		{
			name: "two-ddls-close-together",
			history: preDDL + " ALTER TABLE kd ADD COLUMN d INT; INSERT INTO kd(id,b,c,d) VALUES (4,'104','204',4);" +
				" ALTER TABLE kd MODIFY c VARCHAR(40) FIRST; INSERT INTO kd(id,b,c,d) VALUES (3,'103','203',3);",
			want: with(
				shapeChange{"insert", ir.Row{"id": int64(4), "b": "104", "c": "204", "d": int64(4)}},
				shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "d": int64(3)}},
			),
		},
		{
			// An in-place redefinition the type code cannot see: an ENUM whose
			// labels were reordered keeps its code, and an index recorded
			// under the old list names the other label under the new one.
			// Without labels in the binlog the per-event recheck refuses.
			name: "enum-labels-reordered-in-place",
			history: "ALTER TABLE kd ADD COLUMN e ENUM('x','y'); INSERT INTO kd VALUES (2,'102','202','x');" +
				" ALTER TABLE kd MODIFY e ENUM('y','x'); INSERT INTO kd VALUES (3,'103','203','y');",
			want: []shapeChange{
				{"insert", ir.Row{"id": int64(2), "b": "102", "c": "202", "e": "x"}},
				{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "e": "y"}},
			},
		},
		{
			// A column made GENERATED after the row: FULL names it, but the
			// catalog now says the decoder must drop its value. FULL metadata
			// records no generated-ness, so the catalog must be proven to hold
			// at the row's position, and the MODIFY ahead refuses it.
			name: "column-made-generated",
			history: "INSERT INTO kd VALUES (2,'102','202');" +
				" ALTER TABLE kd MODIFY c VARCHAR(40) AS (CONCAT(b, 'g')) STORED; INSERT INTO kd(id,b) VALUES (3,'103');",
			fullRefuses: true,
		},
		{
			// The reverse: a generated column made regular. The row recorded
			// the generated value; decoding by the current (regular)
			// definition emits that stored value under the column's name.
			name: "generated-made-regular",
			history: "ALTER TABLE kd ADD COLUMN g VARCHAR(50) AS (CONCAT(b, 'g')) STORED;" +
				" INSERT INTO kd(id,b,c) VALUES (2,'102','202'); ALTER TABLE kd MODIFY g VARCHAR(50);" +
				" INSERT INTO kd VALUES (3,'103','203','x');",
			want: []shapeChange{
				{"insert", ir.Row{"id": int64(2), "b": "102", "c": "202", "g": "102g"}},
				{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203", "g": "x"}},
			},
			minimalDecodes: true,
			retypesInPlace: true,
		},
		{
			// A partial before-image (binlog_row_image=MINIMAL for one session)
			// recorded before a DROP COLUMN: only the key is in the image, and
			// the delete narrows to it.
			name: "partial-delete-image",
			history: "INSERT INTO kd VALUES (2,'102','202'); SET SESSION binlog_row_image = MINIMAL;" +
				" DELETE FROM kd WHERE id = 2; SET SESSION binlog_row_image = FULL;" +
				" ALTER TABLE kd DROP COLUMN c; INSERT INTO kd VALUES (3,'103');",
			want: []shapeChange{
				{"insert", ir.Row{"id": int64(2), "b": "102", "c": "202"}},
				{"delete", ir.Row{"id": int64(2)}},
				{"insert", ir.Row{"id": int64(3), "b": "103"}},
			},
		},
		{
			// A partial UPDATE image under FULL metadata still refuses as a
			// partial image (Bug 193), not as a decode of the names it carries.
			name: "partial-update-image",
			history: "SET SESSION binlog_row_image = MINIMAL; UPDATE kd SET b = '111' WHERE id = 1;" +
				" SET SESSION binlog_row_image = FULL; ALTER TABLE kd DROP COLUMN c; INSERT INTO kd VALUES (3,'103');",
			fullRefuses:     true,
			fullRefusalCode: sluicecode.CodeCDCRowImagePartial,
		},
		{
			// A signed TINYINT that is later dropped: the binlog does not say
			// whether it was TINYINT(1), which sluice reads as a boolean.
			name:        "drop-signed-tinyint",
			history:     "ALTER TABLE kd ADD COLUMN t TINYINT; INSERT INTO kd VALUES (2,'102','202',5); ALTER TABLE kd DROP COLUMN t;",
			fullRefuses: true,
		},
	}
}

// foreignDDLWant is what the out-of-scope cases below must decode: a DDL in
// another database, however unreadable to the parser, reshapes nothing here.
func foreignDDLWant() []shapeChange {
	return append(preDDLWant(), shapeChange{"insert", ir.Row{"id": int64(3), "b": "103", "c": "203"}})
}

// mysqlShapeCases are the MySQL-only spellings: DDL the Vitess parser cannot
// read, run in ANOTHER database while the stream is behind. Before the
// prefix bounding each reached every table and refused every stream on the
// server (Bug 301 review).
func mysqlShapeCases() []shapeCase {
	return []shapeCase{{
		name: "foreign-unparseable-ddl",
		history: preDDL + " DROP DATABASE IF EXISTS b301_other; CREATE DATABASE b301_other;" +
			" CREATE TABLE b301_other.t (id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M;" +
			" ALTER DATABASE b301_other READ ONLY = 1; ALTER DATABASE b301_other READ ONLY = 0;" +
			" CREATE TABLE b301_other.v (id INT PRIMARY KEY, b VARCHAR(40), c VARCHAR(40));" +
			" ALTER TABLE b301_other.v MODIFY c VARCHAR(40) AFTER id;" +
			" DROP DATABASE b301_other; INSERT INTO kd VALUES (3,'103','203');",
		want:           foreignDDLWant(),
		minimalDecodes: true,
	}}
}

// mariaDBShapeCases are the MariaDB-only shapes: a binary column retyped to
// the native UUID (a type the TABLE_MAP family table does not classify), and
// MariaDB-only DDL the parser cannot read, run in another database.
func mariaDBShapeCases() []shapeCase {
	return []shapeCase{
		{
			name: "binary16-to-native-uuid",
			history: "ALTER TABLE kd ADD COLUMN u BINARY(16);" +
				" INSERT INTO kd VALUES (2,'102','202',UNHEX('00112233445566778899aabbccddeeff'));" +
				" ALTER TABLE kd MODIFY u UUID; INSERT INTO kd VALUES (3,'103','203','00112233-4455-6677-8899-aabbccddeeff');",
			fullRefuses: true,
		},
		{
			name: "foreign-unparseable-ddl",
			history: preDDL + " DROP DATABASE IF EXISTS b301_other; CREATE DATABASE b301_other;" +
				" CREATE TABLE b301_other.t (id INT PRIMARY KEY, u UUID, i INET6);" +
				" ALTER TABLE b301_other.t ADD COLUMN IF NOT EXISTS v INT;" +
				" CREATE OR REPLACE TABLE b301_other.w (id INT PRIMARY KEY) WITH SYSTEM VERSIONING;" +
				" CREATE SEQUENCE b301_other.s;" +
				" DROP DATABASE b301_other; INSERT INTO kd VALUES (3,'103','203');",
			want:           foreignDDLWant(),
			minimalDecodes: true,
		},
	}
}

// TestCDCShapeAtPosition_MariaDBCompressedBinlog is the lane a first cut
// missed: with log_bin_compress=ON MariaDB writes a DDL of at least
// log_bin_compress_min_len bytes (256 by default) as Query_compressed, and a
// look-ahead that read only `Query` events proved a reorder away. Every
// reshaping case here carries a DDL padded past 256 bytes.
func TestCDCShapeAtPosition_MariaDBCompressedBinlog(t *testing.T) {
	base, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4", "--log-bin-compress=ON")
	defer cleanup()
	setRowMetadata(t, base, "NO_LOG")
	pad := " COMMENT '" + strings.Repeat("p", 260) + "'"
	for i, c := range []shapeCase{
		{
			name:    "reorder-long-ddl",
			history: preDDL + " ALTER TABLE kd MODIFY c VARCHAR(40)" + pad + " AFTER id; INSERT INTO kd(id,b,c) VALUES (3,'103','203');",
		},
		{
			name:    "drop-add-long-ddl",
			history: preDDL + " ALTER TABLE kd DROP COLUMN b, ADD COLUMN e VARCHAR(40)" + pad + "; INSERT INTO kd(id,c,e) VALUES (3,'203','303');",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			runShapeCase(t, Engine{Flavor: FlavorMariaDB}, base, fmt.Sprintf("b301z_%d", i), false, true, c)
			logCompressedDDLInfo(t, base)
		})
	}
}

// logCompressedDDLInfo records what SHOW BINLOG EVENTS shows for the
// server's compressed DDL — the look-ahead classifies a Query_compressed by
// its Info, so the log is the evidence of what that Info holds.
func logCompressedDDLInfo(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	files, err := showBinaryLogs(context.Background(), db)
	if err != nil || len(files) == 0 {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	rows, err := showBinlogEvents(context.Background(), db, files[len(files)-1], 4, 100000)
	if err != nil {
		t.Fatalf("SHOW BINLOG EVENTS: %v", err)
	}
	seen := 0
	for _, r := range rows {
		if r.typ == "Query_compressed" {
			seen++
			t.Logf("Query_compressed at %d: %.120q", r.pos, r.info)
		}
	}
	if seen == 0 {
		t.Errorf("no Query_compressed event in the binlog: the lane did not exercise compressed DDL")
	}
}

// TestCDCShapeAtPosition_Matrix runs every shape under MINIMAL and FULL
// metadata, on a GTID-mode server and a file/pos-mode one. The server image
// is the package default (MySQL 8.0) or SLUICE_TEST_MYSQL_IMAGE (8.4).
func TestCDCShapeAtPosition_Matrix(t *testing.T) {
	for _, mode := range []struct {
		name string
		args []string
	}{
		{"gtid", []string{"--gtid-mode=ON", "--enforce-gtid-consistency=ON"}},
		{"filepos", nil},
	} {
		t.Run(mode.name, func(t *testing.T) {
			base, cleanup := startMySQLM2PreflightImage(t, sharedMySQLImage, mode.args...)
			defer cleanup()
			logServerVersion(t, base)
			for _, metadata := range []string{"MINIMAL", "FULL"} {
				setRowMetadata(t, base, metadata)
				for i, c := range append(shapeCases(), mysqlShapeCases()...) {
					t.Run(metadata+"/"+c.name, func(t *testing.T) {
						runShapeCase(t, Engine{Flavor: FlavorVanilla}, base, fmt.Sprintf("b301_%s_%d", strings.ToLower(metadata), i), metadata == "FULL", false, c)
					})
				}
			}
		})
	}
}

// runShapeCase records c's history behind a stored position and streams it.
func runShapeCase(t *testing.T, eng Engine, base, db string, full, noLog bool, c shapeCase) {
	dsn := shapeCaseDB(t, base, db)
	applyMySQL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, b VARCHAR(40), c VARCHAR(40)) DEFAULT CHARSET=utf8mb4; INSERT INTO kd VALUES (1,'101','201');")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	from := stream.Position
	_ = stream.Close()
	applyMySQL(t, dsn, c.history)

	got, streamErr := streamShapeChanges(ctx, t, eng, dsn, from, len(c.want))
	expectRefusal := c.fullRefuses || (!full && (!c.minimalDecodes || (noLog && c.retypesInPlace)))
	if expectRefusal {
		if len(got) != 0 {
			t.Errorf("emitted %d change(s) of kd before refusing: %v; want none — the refusal must come before any row of the table is decoded", len(got), got)
		}
		wantCode := sluicecode.CodeCDCSchemaReplayMismatch
		if full && c.fullRefusalCode != "" {
			wantCode = c.fullRefusalCode
		}
		ce, ok := sluicecode.FromError(streamErr)
		if !ok || ce.Code != wantCode {
			t.Fatalf("stream error = %v; want %s", streamErr, wantCode)
		}
		return
	}
	if streamErr != nil {
		t.Fatalf("stream refused: %v", streamErr)
	}
	if !reflect.DeepEqual(got, c.want) {
		t.Errorf("decoded changes differ from what was written:\n got  %v\n want %v", got, c.want)
	}
}

// streamShapeChanges reads kd's row changes from `from` until want of them
// arrived or the stream ended, then returns them with the reader's error.
func streamShapeChanges(ctx context.Context, t *testing.T, eng Engine, dsn string, from ir.Position, want int) ([]shapeChange, error) {
	t.Helper()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc := rdr.(*CDCReader)
	defer func() { _ = cdc.Close() }()
	changes, err := cdc.StreamChanges(ctx, from)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	var got []shapeChange
	deadline := time.After(20 * time.Second)
	for want == 0 || len(got) < want {
		select {
		case ch, ok := <-changes:
			if !ok {
				return got, cdc.Err()
			}
			switch v := ch.(type) {
			case ir.Insert:
				if v.Table == "kd" {
					got = append(got, shapeChange{"insert", v.Row})
				}
			case ir.Update:
				if v.Table == "kd" {
					got = append(got, shapeChange{"update", v.After})
				}
			case ir.Delete:
				if v.Table == "kd" {
					got = append(got, shapeChange{"delete", v.Before})
				}
			}
		case <-deadline:
			return got, cdc.Err()
		}
	}
	return got, cdc.Err()
}

// shapeCaseDB creates a fresh database on base's server and returns its DSN.
func shapeCaseDB(t *testing.T, base, db string) string {
	t.Helper()
	applyMySQL(t, base, "DROP DATABASE IF EXISTS "+db+"; CREATE DATABASE "+db+" DEFAULT CHARSET=utf8mb4;")
	i, j := strings.Index(base, ")/"), strings.Index(base, "?")
	if i < 0 || j < i {
		t.Fatalf("unexpected DSN shape %q", base)
	}
	return base[:i+2] + db + base[j:]
}

// setRowMetadata sets binlog_row_metadata for the events written next.
func setRowMetadata(t *testing.T, dsn, value string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("SET GLOBAL binlog_row_metadata = " + value); err != nil {
		t.Fatalf("SET GLOBAL binlog_row_metadata=%s: %v", value, err)
	}
}

// TestCDCShapeAtPosition_ForeignDefaultDatabaseDDL is the head-of-binlog
// sibling: a reader at the head, its cache warm, crosses an ALTER issued
// from ANOTHER default database. Before Bug 301's fix the cache was not
// cleared (the event's schema is the session default), and every later
// row decoded against the pre-DDL column list — a reorder put each value
// under the other column's name. Under MINIMAL, because no names are
// needed here: the fix is that the cache is cleared at all.
func TestCDCShapeAtPosition_ForeignDefaultDatabaseDDL(t *testing.T) {
	base, cleanup := startMySQLM2PreflightImage(t, sharedMySQLImage, "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
	defer cleanup()
	setRowMetadata(t, base, "MINIMAL")
	dsn := shapeCaseDB(t, base, "b301_xdb")
	applyMySQL(t, dsn, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, b VARCHAR(40), c VARCHAR(40)); INSERT INTO kd VALUES (1,'101','201');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc := rdr.(*CDCReader)
	defer func() { _ = cdc.Close() }()
	changes, err := cdc.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	applyMySQL(t, dsn, "INSERT INTO kd VALUES (2,'102','202');") // warms the cache at head
	if got := drainChanges(t, ctx, changes, 1, 20*time.Second); len(got) != 1 {
		t.Fatalf("warm-up row: got %d changes", len(got))
	}
	applyMySQL(t, base, "USE mysql; ALTER TABLE b301_xdb.kd MODIFY c VARCHAR(40) AFTER id;")
	applyMySQL(t, dsn, "INSERT INTO kd(id,b,c) VALUES (3,'103','203');")
	got := drainChanges(t, ctx, changes, 1, 20*time.Second)
	if len(got) != 1 {
		t.Fatalf("post-DDL row: got %d changes (err %v)", len(got), cdc.Err())
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("post-DDL change is %T", got[0])
	}
	want := ir.Row{"id": int64(3), "b": "103", "c": "203"}
	if !reflect.DeepEqual(ins.Row, want) {
		t.Errorf("row after a foreign-database ALTER decoded as %v; want %v (the stale shape swaps b and c)", ins.Row, want)
	}
}

// logServerVersion records which server a matrix ran on, so a log says
// whether the run was the 8.0 default or an 8.4 override.
func logServerVersion(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRow("SELECT @@version").Scan(&v); err != nil {
		t.Fatalf("SELECT @@version: %v", err)
	}
	t.Logf("server version %s", v)
}

// TestCDCShapeAtPosition_MariaDB runs the shape matrix on MariaDB, which
// shares the binlog reader: its default binlog_row_metadata is NO_LOG (no
// names, no charsets), and FULL writes names as MySQL does.
func TestCDCShapeAtPosition_MariaDB(t *testing.T) {
	base, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanup()
	logServerVersion(t, base)
	for _, metadata := range []string{"NO_LOG", "FULL"} {
		setRowMetadata(t, base, metadata)
		for i, c := range append(shapeCases(), mariaDBShapeCases()...) {
			t.Run(metadata+"/"+c.name, func(t *testing.T) {
				runShapeCase(t, Engine{Flavor: FlavorMariaDB}, base, fmt.Sprintf("b301m_%s_%d", strings.ToLower(metadata), i), metadata == "FULL", metadata == "NO_LOG", c)
			})
		}
	}
}

// familyColumn is one column of the synthesis family test: its definition,
// the SQL literal written into it, and the value the decoder must produce —
// derived from that literal and the documented value contract
// (docs/value-types.md), not from any decode. A nil want with check=false
// means "compare to the catalog decode only".
type familyColumn struct {
	def, sqlValue string
	want          any
	check         bool
	// json compares semantically: the binlog renders JSON text in its own
	// spacing.
	json bool
}

func fam(def, sqlValue string, want any) familyColumn {
	return familyColumn{def: def, sqlValue: sqlValue, want: want, check: true}
}

func bigLabels(prefix string, n int) (def string, labels []string) {
	quoted := make([]string, n)
	labels = make([]string, n)
	for i := range n {
		labels[i] = fmt.Sprintf("%s%d", prefix, i)
		quoted[i] = "'" + labels[i] + "'"
	}
	return strings.Join(quoted, ","), labels
}

// commonFamilyColumns are the families both servers type alike.
func commonFamilyColumns() []familyColumn {
	enumDef, _ := bigLabels("l", 300)
	setDef, _ := bigLabels("m", 10)
	return []familyColumn{
		fam("c_tinyint_u TINYINT UNSIGNED", "255", uint64(255)),
		fam("c_smallint SMALLINT", "-32768", int64(-32768)),
		fam("c_smallint_u SMALLINT UNSIGNED", "65535", uint64(65535)),
		fam("c_mediumint MEDIUMINT", "-8388608", int64(-8388608)),
		fam("c_mediumint_u MEDIUMINT UNSIGNED", "16777215", uint64(16777215)),
		fam("c_int INT", "-2147483648", int64(-2147483648)),
		fam("c_int_u INT UNSIGNED", "4294967295", uint64(4294967295)),
		fam("c_bigint BIGINT", "-9223372036854775808", int64(math.MinInt64)),
		fam("c_bigint_u BIGINT UNSIGNED", "18446744073709551615", uint64(math.MaxUint64)),
		fam("c_year YEAR", "2155", int64(2155)),
		fam("c_decimal DECIMAL(12,3)", "-123456789.125", "-123456789.125"),
		fam("c_decimal_u DECIMAL(30,10) UNSIGNED", "12345678901234567890.0123456789", "12345678901234567890.0123456789"),
		fam("c_float FLOAT", "1.5", float64(1.5)),
		fam("c_double DOUBLE", "-2.25e300", float64(-2.25e300)),
		fam("c_bit1 BIT(1)", "b'1'", true),
		fam("c_bit BIT(12)", "b'101010101010'", "101010101010"),
		fam("c_char CHAR(8)", "'chr'", "chr"),
		fam("c_varchar VARCHAR(64)", "'vär chär'", "vär chär"),
		fam("c_longchar CHAR(200)", "REPEAT('é', 200)", strings.Repeat("é", 200)),
		fam("c_longvarchar VARCHAR(300)", "REPEAT('v', 300)", strings.Repeat("v", 300)),
		fam("c_latin1 VARCHAR(20) CHARACTER SET latin1", "'café'", "café"),
		fam("c_utf16 VARCHAR(10) CHARACTER SET utf16", "'Ωé'", "Ωé"),
		fam("c_utf32 VARCHAR(10) CHARACTER SET utf32", "'😀é'", "😀é"),
		fam("c_ucs2 VARCHAR(10) CHARACTER SET ucs2", "'Ωé'", "Ωé"),
		fam("c_sjis VARCHAR(10) CHARACTER SET sjis", "'ｱ漢'", "ｱ漢"),
		fam("c_gbk VARCHAR(10) CHARACTER SET gbk", "'汉字'", "汉字"),
		fam("c_tinytext TINYTEXT", "'tt'", "tt"),
		fam("c_text TEXT", "'txt'", "txt"),
		fam("c_mediumtext MEDIUMTEXT", "'mt'", "mt"),
		fam("c_longtext LONGTEXT", "'lt'", "lt"),
		fam("c_date DATE", "'2024-02-29'", time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)),
		fam("c_time TIME(3)", "'-12:34:56.789'", "-12:34:56.789"),
		fam("c_time0 TIME", "'01:02:03'", "01:02:03"),
		fam("c_datetime DATETIME(6)", "'9999-12-31 23:59:59.999999'", time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
		fam("c_datetime0 DATETIME", "'2001-01-01 01:02:03'", time.Date(2001, 1, 1, 1, 2, 3, 0, time.UTC)),
		fam("c_timestamp TIMESTAMP(2) NULL", "'2038-01-19 03:14:07.99'", time.Date(2038, 1, 19, 3, 14, 7, 990000000, time.UTC)),
		fam("c_timestamp0 TIMESTAMP NULL", "'2001-02-03 04:05:06'", time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)),
		fam("c_enum ENUM('a','b','c d')", "'c d'", "c d"),
		fam("c_enum_big ENUM("+enumDef+")", "'l299'", "l299"),
		fam("c_set SET('x','y','z')", "'x,z'", []string{"x", "z"}),
		fam("c_set_big SET("+setDef+")", "'m1,m9'", []string{"m1", "m9"}),
		fam("c_inv INT INVISIBLE", "7", int64(7)),
	}
}

// mysqlFamilyColumns adds MySQL's binary and JSON families. MariaDB refuses
// to type a binary column from the binlog (it may have been a native
// UUID/INET), and its JSON is LONGTEXT.
func mysqlFamilyColumns() []familyColumn {
	return append(
		commonFamilyColumns(),
		fam("c_binary BINARY(4)", "X'DEAD'", []byte{0xde, 0xad, 0, 0}),
		fam("c_varbinary VARBINARY(16)", "X'00FF00'", []byte{0, 0xff, 0}),
		fam("c_tinyblob TINYBLOB", "X'01'", []byte{1}),
		fam("c_blob BLOB", "X'0203'", []byte{2, 3}),
		fam("c_mediumblob MEDIUMBLOB", "X'04'", []byte{4}),
		fam("c_longblob LONGBLOB", "X'05'", []byte{5}),
		familyColumn{def: "c_json JSON", sqlValue: `'{"k": [1, "two", null]}'`, want: map[string]any{"k": []any{float64(1), "two", nil}}, check: true, json: true},
	)
}

// TestCDCRecordedColumnTypes_EveryFamily pins the FULL-metadata arm that
// types a column from the TABLE_MAP alone (recordedColumnMeta): every
// family sluice reads, a value row and an all-NULL row, decoded once with
// the catalog intact and once after every column was dropped — so the
// second decode can only type them from the binlog.
//
// Two expected values, and the first is the independent one: the literal
// each column was written with, as the value contract renders it (unsigned
// extremes, every charset, BIT, YEAR, SET, ENUM beyond 255 labels, SET
// beyond 8 members, fractional seconds, JSON semantically). The second is
// the catalog decode of the same events. Families the TABLE_MAP cannot type
// (a signed TINYINT, TINYINT(1), a spatial column) must refuse, not guess.
func TestCDCRecordedColumnTypes_EveryFamily(t *testing.T) {
	base, cleanup := startMySQLM2PreflightImage(t, sharedMySQLImage, "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
	defer cleanup()
	logServerVersion(t, base)
	setRowMetadata(t, base, "FULL")
	eng := Engine{Flavor: FlavorVanilla}
	runFamilySynthesis(t, eng, shapeCaseDB(t, base, "b301_fam"), mysqlFamilyColumns())

	// The families the binlog cannot type refuse.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for _, col := range []string{"t TINYINT", "t TINYINT(1)", "t POINT", "t GEOMETRY"} {
		t.Run(col, func(t *testing.T) {
			dsn := shapeCaseDB(t, base, "b301_untypable")
			applyMySQL(t, dsn, "CREATE TABLE u (id INT NOT NULL PRIMARY KEY, "+col+");")
			stream, err := eng.OpenSnapshotStream(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSnapshotStream: %v", err)
			}
			from := stream.Position
			_ = stream.Close()
			applyMySQL(t, dsn, "INSERT INTO u (id) VALUES (1); ALTER TABLE u DROP COLUMN t;")
			got, streamErr := streamShapeChanges(ctx, t, eng, dsn, from, 0)
			if len(got) != 0 {
				t.Errorf("emitted %v", got)
			}
			if ce, ok := sluicecode.FromError(streamErr); !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
				t.Fatalf("stream error = %v; want %s", streamErr, sluicecode.CodeCDCSchemaReplayMismatch)
			}
		})
	}
}

// TestCDCRecordedColumnTypes_EveryFamily_MariaDB is the same synthesis on
// MariaDB 11.4 under FULL, over the families it types from the binlog; a
// binary column there refuses (TestCDCShapeAtPosition_MariaDB's
// binary16-to-native-uuid).
func TestCDCRecordedColumnTypes_EveryFamily_MariaDB(t *testing.T) {
	base, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanup()
	logServerVersion(t, base)
	setRowMetadata(t, base, "FULL")
	runFamilySynthesis(t, Engine{Flavor: FlavorMariaDB}, shapeCaseDB(t, base, "b301_fam"), commonFamilyColumns())
}

// runFamilySynthesis writes a value row and an all-NULL row, decodes them
// with the catalog intact, drops every column, decodes them again, and
// grades both decodes against the literals.
func runFamilySynthesis(t *testing.T, eng Engine, dsn string, cols []familyColumn) {
	t.Helper()
	names := make([]string, len(cols))
	defs := make([]string, len(cols))
	values := make([]string, len(cols))
	for i, c := range cols {
		defs[i], values[i] = c.def, c.sqlValue
		names[i] = strings.Fields(c.def)[0]
	}
	applyMySQL(t, dsn, "CREATE TABLE fam (id INT NOT NULL PRIMARY KEY, "+strings.Join(defs, ", ")+") DEFAULT CHARSET=utf8mb4;")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	from := stream.Position
	_ = stream.Close()
	applyMySQL(t, dsn, "SET time_zone = '+00:00'; INSERT INTO fam (id, "+strings.Join(names, ", ")+") VALUES (1, "+strings.Join(values, ", ")+");"+
		" INSERT INTO fam (id) VALUES (2);")

	control := familyInserts(ctx, t, eng, dsn, from)
	drop := make([]string, len(names))
	for i, n := range names {
		drop[i] = "DROP COLUMN " + n
	}
	applyMySQL(t, dsn, "ALTER TABLE fam ADD COLUMN z INT, "+strings.Join(drop, ", ")+";")
	replayed := familyInserts(ctx, t, eng, dsn, from)
	for i, c := range cols {
		n := names[i]
		for _, run := range []struct {
			what string
			row  ir.Row
		}{{"typed from the catalog", control[0]}, {"typed from the binlog", replayed[0]}} {
			if !c.check {
				continue
			}
			got := run.row[n]
			if c.json {
				var v any
				b, _ := got.([]byte)
				if err := json.Unmarshal(b, &v); err != nil || !reflect.DeepEqual(v, c.want) {
					t.Errorf("%s %s: %#v; want JSON %v (err %v)", n, run.what, got, c.want, err)
				}
				continue
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s %s: %#v (%T); written as %s, want %#v (%T)", n, run.what, got, got, c.sqlValue, c.want, c.want)
			}
		}
		if v, ok := replayed[1][n]; !ok || v != nil {
			t.Errorf("%s NULL row typed from the binlog: %#v (present %v); want a present nil", n, v, ok)
		}
		if !reflect.DeepEqual(control[1][n], replayed[1][n]) {
			t.Errorf("%s NULL row: binlog %#v, catalog %#v", n, replayed[1][n], control[1][n])
		}
	}
}

// familyInserts streams fam's two inserts from `from`.
func familyInserts(ctx context.Context, t *testing.T, eng Engine, dsn string, from ir.Position) []ir.Row {
	t.Helper()
	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	cdc := rdr.(*CDCReader)
	defer func() { _ = cdc.Close() }()
	changes, err := cdc.StreamChanges(ctx, from)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	got := drainChanges(t, ctx, changes, 2, 20*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2 (err %v)", len(got), cdc.Err())
	}
	rows := make([]ir.Row, 2)
	for i, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("change %d is %T", i, c)
		}
		rows[i] = ins.Row
	}
	return rows
}
