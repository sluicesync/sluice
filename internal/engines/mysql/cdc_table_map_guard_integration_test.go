//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit CDC-4 against a real server. Two halves, and the second is the
// one that makes the first safe to ship:
//
//  1. the refusal fires on the scenario the finding describes — a
//     same-column-count DDL applied between a stored position and now;
//  2. the guard does NOT fire on a steady-state stream carrying every
//     data_type sluice supports. A family table derived by reading docs
//     is exactly the kind of thing that is subtly wrong for one type, and
//     a wrong entry here halts a production sync. This half ground-truths
//     the table against what mysqld actually writes into a TABLE_MAP.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCDCReplay_SameColumnCountTypeChangeRefusesLoudly reproduces the
// finding: events are recorded under one shape, a DDL that preserves the
// column COUNT lands, and the stored position is then replayed. Before the
// guard this decoded old DECIMAL values as DOUBLE with no error.
func TestCDCReplay_SameColumnCountTypeChangeRefusesLoudly(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "cdc4_replay_db")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE ledger (
			id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			amount DECIMAL(12,2) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// The stored position a warm resume would come back to.
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	resumeFrom := stream.Position
	_ = stream.Close()

	// Events recorded while `amount` is DECIMAL.
	applyMySQL(t, dsn, `INSERT INTO ledger (amount) VALUES (10.25), (99.99);`)

	// The downtime DDL. Same column count — the case the arity check
	// cannot see.
	applyMySQL(t, dsn, `ALTER TABLE ledger MODIFY amount DOUBLE NOT NULL;`)

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, resumeFrom)
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	// Drain; the refusal is stream-fatal, so the channel closes and
	// drainChanges returns short of the (deliberately unreachable) want.
	if got := drainChanges(t, ctx, changes, 100, 30*time.Second); len(got) > 0 {
		t.Errorf("the replay emitted %d change(s) decoded against the POST-DDL schema before stopping; "+
			"the guard must refuse before any row of a drifted table is emitted", len(got))
	}

	cdcRdr, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("reader is %T; want *CDCReader", rdr)
	}
	streamErr := cdcRdr.Err()
	if streamErr == nil {
		t.Fatal("replaying pre-DDL events against the post-DDL schema must refuse; the stream ended cleanly, " +
			"which means those DECIMAL values were silently decoded as DOUBLE (audit CDC-4)")
	}
	var ce *sluicecode.CodedError
	if !errors.As(streamErr, &ce) || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("stream error = %v; want %s", streamErr, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	for _, want := range []string{"ledger", "amount", "Re-snapshot the table"} {
		if !strings.Contains(streamErr.Error(), want) {
			t.Errorf("refusal must contain %q so the operator knows what and what to do; got: %v", want, streamErr)
		}
	}
}

// TestCDCSteadyState_EveryDataTypePassesTheGuard is the false-refusal
// gate, and it is the reason the family table can be trusted.
//
// The table below carries one column per data_type family sluice supports
// (the geometry members are represented by `point` and `geometry`; the
// MariaDB-native types are absent because MySQL does not have them and
// they are on the guard's written exemption list). A live INSERT must
// stream through with no refusal — which is only true if every entry in
// binlogTypeFamilyForDataType agrees with the type code mysqld actually
// puts in the TABLE_MAP for that column.
func TestCDCSteadyState_EveryDataTypePassesTheGuard(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "cdc4_types_db")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE all_types (
			id            BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			c_tinyint     TINYINT,
			c_smallint    SMALLINT,
			c_mediumint   MEDIUMINT,
			c_int         INT,
			c_bigint      BIGINT,
			c_year        YEAR,
			c_decimal     DECIMAL(12,3),
			c_float       FLOAT,
			c_double      DOUBLE,
			c_bit         BIT(8),
			c_char        CHAR(8),
			c_varchar     VARCHAR(64),
			-- A CHAR whose byte length exceeds 255 (200 chars × utf8mb4) is
			-- written with MySQL's EXTENDED metadata encoding, which packs
			-- length bits into the same high byte the guard reads to spot
			-- ENUM/SET. Reasoning says the encoded values (0xFE/0xEE/0xDE/
			-- 0xCE for CHAR, 0xFD/0xED/0xDD/0xCD for VAR_STRING) can never
			-- equal ENUM 0xF7 or SET 0xF8 — this makes the server assert it
			-- instead of leaving it as an argument.
			c_longchar    CHAR(200),
			c_longvarchar VARCHAR(300),
			c_tinytext    TINYTEXT,
			c_text        TEXT,
			c_mediumtext  MEDIUMTEXT,
			c_longtext    LONGTEXT,
			c_binary      BINARY(4),
			c_varbinary   VARBINARY(16),
			c_tinyblob    TINYBLOB,
			c_blob        BLOB,
			c_mediumblob  MEDIUMBLOB,
			c_longblob    LONGBLOB,
			c_date        DATE,
			c_time        TIME(3),
			c_datetime    DATETIME(6),
			c_timestamp   TIMESTAMP NULL,
			c_enum        ENUM('a','b','c'),
			c_set         SET('x','y','z'),
			c_json        JSON,
			c_point       POINT,
			c_geometry    GEOMETRY
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	time.Sleep(300 * time.Millisecond) // let the syncer reach "now"

	applyMySQL(t, dsn, `
		INSERT INTO all_types (
			c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_year,
			c_decimal, c_float, c_double, c_bit,
			c_char, c_varchar, c_longchar, c_longvarchar,
			c_tinytext, c_text, c_mediumtext, c_longtext,
			c_binary, c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob,
			c_date, c_time, c_datetime, c_timestamp,
			c_enum, c_set, c_json, c_point, c_geometry
		) VALUES (
			1, 2, 3, 4, 5, 2026,
			1.234, 1.5, 2.5, b'10101010',
			'chr', 'vchr', REPEAT('c', 200), REPEAT('v', 300),
			'tt', 'txt', 'mt', 'lt',
			'bin', 'vbin', 'tb', 'bl', 'mb', 'lb',
			'2026-08-05', '01:02:03.456', '2026-08-05 01:02:03.456789', '2026-08-05 01:02:03',
			'b', 'x,z', JSON_OBJECT('k','v'), ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)')
		);
	`)

	got := drainChanges(t, ctx, changes, 1, 30*time.Second)

	cdcRdr, ok := rdr.(*CDCReader)
	if !ok {
		t.Fatalf("reader is %T; want *CDCReader", rdr)
	}
	if streamErr := cdcRdr.Err(); streamErr != nil {
		var ce *sluicecode.CodedError
		if errors.As(streamErr, &ce) && ce.Code == sluicecode.CodeCDCSchemaReplayMismatch {
			t.Fatalf("FALSE REFUSAL: the CDC-4 guard refused a steady-state stream. One of "+
				"binlogTypeFamilyForDataType's entries disagrees with the type code mysqld writes into the "+
				"TABLE_MAP, and shipping it would halt every sync carrying that column type: %v", streamErr)
		}
		t.Fatalf("stream error: %v", streamErr)
	}
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1 insert (the guard must not have swallowed it)", len(got))
	}
	if _, isInsert := got[0].(ir.Insert); !isInsert {
		t.Fatalf("change[0] = %T; want ir.Insert", got[0])
	}
}

// TestLoadTableSchemaPopulatesDataTypes is the anti-vacuity floor under
// the guard: it skips a *tableSchema with no DataTypes, which is correct
// for the hand-built ones in unit tests and would silently disable the
// whole guard if production's loader ever stopped filling the field.
func TestLoadTableSchemaPopulatesDataTypes(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "cdc4_datatypes_db")
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE shapes (
			id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			amount DECIMAL(9,2),
			label  VARCHAR(32)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tbl, err := loadTableSchema(ctx, db, "cdc4_datatypes_db", "shapes")
	if err != nil {
		t.Fatalf("loadTableSchema: %v", err)
	}
	if len(tbl.DataTypes) != len(tbl.Columns) {
		t.Fatalf("DataTypes has %d entries for %d columns — the CDC-4 guard silently skips a schema whose "+
			"DataTypes are not parallel to Columns, so this makes the guard inert in production",
			len(tbl.DataTypes), len(tbl.Columns))
	}
	want := []string{"bigint", "decimal", "varchar"}
	for i, w := range want {
		if tbl.DataTypes[i] != w {
			t.Errorf("DataTypes[%d] = %q; want %q", i, tbl.DataTypes[i], w)
		}
	}
	// And the guard must actually be live for this schema: a deliberately
	// wrong TABLE_MAP against it refuses.
	if err := verifyTableMapMatchesSchema(eventFor([]byte{0x08, 0x0f, 0x0f}), tbl); err == nil {
		t.Error("the guard is inert for a real loader-produced schema — a VARCHAR-recorded decimal column passed")
	}
}
