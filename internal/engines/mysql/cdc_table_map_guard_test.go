// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit CDC-4 pins. The guard is a family-dispatched comparison, so per the
// Bug-74 rule the matrix below walks EVERY family it classifies — both
// directions (the recorded binlog code and the declared data_type) — rather
// than a representative or two. A family that only ever appears on one side
// of the comparison would be an untested arm.

// tblFor builds a loader-shaped *tableSchema: Columns and DataTypes
// parallel, the way loadTableSchema produces them.
func tblFor(dataTypes ...string) *tableSchema {
	ts := &tableSchema{Schema: "app", Name: "widgets"}
	for i, dt := range dataTypes {
		ts.Columns = append(ts.Columns, &ir.Column{Name: string(rune('a' + i)), Type: ir.Text{}})
		ts.DataTypes = append(ts.DataTypes, dt)
	}
	return ts
}

// eventFor builds a rows event whose TABLE_MAP carries the given per-column
// binlog type codes (and optional metadata words).
func eventFor(codes []byte, meta ...uint16) *replication.RowsEvent {
	if meta == nil {
		meta = make([]uint16, len(codes))
	}
	return &replication.RowsEvent{Table: &replication.TableMapEvent{
		Schema:      []byte("app"),
		Table:       []byte("widgets"),
		ColumnCount: uint64(len(codes)),
		ColumnType:  codes,
		ColumnMeta:  meta,
	}}
}

// familyMatrix is the full set of families the guard classifies, each with a
// representative catalog data_type and the binlog type code a server writes
// for it. Every family appears exactly once so the cross-product below
// exercises every (recorded, declared) pair.
var familyMatrix = []struct {
	family   binlogTypeFamily
	dataType string
	code     byte
	meta     uint16
}{
	{famInt, "int", mysql.MYSQL_TYPE_LONG, 0},
	{famDecimal, "decimal", mysql.MYSQL_TYPE_NEWDECIMAL, 0},
	{famFloat, "double", mysql.MYSQL_TYPE_DOUBLE, 0},
	{famBit, "bit", mysql.MYSQL_TYPE_BIT, 0},
	{famString, "varchar", mysql.MYSQL_TYPE_VARCHAR, 0},
	{famDate, "date", mysql.MYSQL_TYPE_DATE, 0},
	{famTime, "time", mysql.MYSQL_TYPE_TIME2, 0},
	{famDateTime, "datetime", mysql.MYSQL_TYPE_DATETIME2, 0},
	{famTimestamp, "timestamp", mysql.MYSQL_TYPE_TIMESTAMP2, 0},
	// ENUM and SET are written as MYSQL_TYPE_STRING with the real type in
	// the metadata's high byte — the normalisation the guard re-derives
	// from go-mysql's unexported realType.
	{famEnum, "enum", mysql.MYSQL_TYPE_STRING, uint16(mysql.MYSQL_TYPE_ENUM) << 8},
	{famSet, "set", mysql.MYSQL_TYPE_STRING, uint16(mysql.MYSQL_TYPE_SET) << 8},
	{famJSON, "json", mysql.MYSQL_TYPE_JSON, 0},
	{famGeometry, "point", mysql.MYSQL_TYPE_GEOMETRY, 0},
}

// TestVerifyTableMapMatchesSchema_FamilyMatrix is the class pin: for every
// ORDERED PAIR of families, a matching pair must pass and a mismatched pair
// must refuse. That is families² cells, not one representative.
func TestVerifyTableMapMatchesSchema_FamilyMatrix(t *testing.T) {
	for _, recorded := range familyMatrix {
		for _, declared := range familyMatrix {
			name := recorded.family.String() + "_recorded_vs_" + declared.family.String() + "_declared"
			t.Run(name, func(t *testing.T) {
				ev := eventFor([]byte{recorded.code}, recorded.meta)
				tbl := tblFor(declared.dataType)
				err := verifyTableMapMatchesSchema(ev, tbl)
				if recorded.family == declared.family {
					if err != nil {
						t.Fatalf("same family must pass, got refusal: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatalf("a %s value recorded against a column now declared %s (%s) must refuse — "+
						"this is the silent-remap case", recorded.family, declared.dataType, declared.family)
				}
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
					t.Errorf("refusal code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCDCSchemaReplayMismatch)
				}
				if !containsAll(err.Error(), "app.widgets", "Re-snapshot the table") {
					t.Errorf("refusal must name the table and the remedy; got: %v", err)
				}
			})
		}
	}
}

// TestVerifyTableMapMatchesSchema_SameCountTypeChange is the CDC-4 scenario
// verbatim: a DDL during downtime that keeps the column COUNT identical. The
// arity check cannot see it; the type vector can.
func TestVerifyTableMapMatchesSchema_SameCountTypeChange(t *testing.T) {
	// Recorded when `amount` was DECIMAL; the table now declares it DOUBLE.
	// Same three columns, same order, same count.
	ev := eventFor([]byte{
		mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_VARCHAR,
	})
	tbl := tblFor("int", "double", "varchar")
	err := verifyTableMapMatchesSchema(ev, tbl)
	if err == nil {
		t.Fatal("a same-column-count type change must refuse; got nil (this is the silent-remap path)")
	}
	if !strings.Contains(err.Error(), "decimal") {
		t.Errorf("the message must name the recorded family so the operator can see WHAT changed; got: %v", err)
	}
}

// TestVerifyTableMapMatchesSchema_WidthChangesAreAllowed is the
// false-refusal floor. These are the changes an operator makes routinely and
// which cannot corrupt a replayed value: they must NOT halt a stream.
func TestVerifyTableMapMatchesSchema_WidthChangesAreAllowed(t *testing.T) {
	cases := []struct {
		name     string
		code     byte
		dataType string
	}{
		{"INT recorded, BIGINT declared", mysql.MYSQL_TYPE_LONG, "bigint"},
		{"TINYINT recorded, INT declared", mysql.MYSQL_TYPE_TINY, "int"},
		{"VARCHAR recorded, TEXT declared", mysql.MYSQL_TYPE_VARCHAR, "text"},
		{"CHAR recorded, VARCHAR declared", mysql.MYSQL_TYPE_STRING, "varchar"},
		{"BLOB recorded, LONGBLOB declared", mysql.MYSQL_TYPE_BLOB, "longblob"},
		{"VAR_STRING recorded, VARCHAR declared", mysql.MYSQL_TYPE_VAR_STRING, "varchar"},
		{"legacy DATE recorded, NEWDATE-era date declared", mysql.MYSQL_TYPE_DATE, "date"},
		{"legacy DATETIME recorded, datetime declared", mysql.MYSQL_TYPE_DATETIME, "datetime"},
		{"legacy TIMESTAMP recorded, timestamp declared", mysql.MYSQL_TYPE_TIMESTAMP, "timestamp"},
		{"legacy DECIMAL recorded, decimal declared", mysql.MYSQL_TYPE_DECIMAL, "decimal"},
		{"FLOAT recorded, double declared", mysql.MYSQL_TYPE_FLOAT, "double"},
		{"YEAR recorded, smallint declared", mysql.MYSQL_TYPE_YEAR, "smallint"},
		{"GEOMETRY recorded, polygon declared", mysql.MYSQL_TYPE_GEOMETRY, "polygon"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := verifyTableMapMatchesSchema(eventFor([]byte{tc.code}), tblFor(tc.dataType)); err != nil {
				t.Errorf("must not refuse a decode-compatible change; got: %v", err)
			}
		})
	}
}

// TestVerifyTableMapMatchesSchema_ColumnCountMismatch pins the arity arm.
// This case is already fatal downstream (decodeBinlogRow), so the value here
// is the coded message and the remedy, not a new refusal.
func TestVerifyTableMapMatchesSchema_ColumnCountMismatch(t *testing.T) {
	ev := eventFor([]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR})
	err := verifyTableMapMatchesSchema(ev, tblFor("int", "varchar", "int"))
	if err == nil {
		t.Fatal("a column-count change must refuse")
	}
	if !containsAll(err.Error(), "app.widgets", "2 columns", "3") {
		t.Errorf("message must name both counts; got: %v", err)
	}
}

// TestVerifyTableMapMatchesSchema_SkipsWhenUnclassifiable pins that the
// guard refuses only on POSITIVE evidence of drift. Unknown on either side,
// a missing type vector, or a hand-built schema with no DataTypes all mean
// "no comparison available", never "refuse".
func TestVerifyTableMapMatchesSchema_SkipsWhenUnclassifiable(t *testing.T) {
	t.Run("unknown binlog code", func(t *testing.T) {
		// MYSQL_TYPE_VECTOR (0xf2) — a newer server type this build does
		// not classify. Not evidence of drift.
		if err := verifyTableMapMatchesSchema(eventFor([]byte{mysql.MYSQL_TYPE_VECTOR}), tblFor("int")); err != nil {
			t.Errorf("an unclassifiable binlog type must be skipped, not refused; got: %v", err)
		}
	})
	t.Run("unknown data_type", func(t *testing.T) {
		if err := verifyTableMapMatchesSchema(eventFor([]byte{mysql.MYSQL_TYPE_LONG}), tblFor("uuid")); err != nil {
			t.Errorf("an unclassifiable data_type must be skipped, not refused; got: %v", err)
		}
	})
	t.Run("hand-built schema with no DataTypes", func(t *testing.T) {
		tbl := &tableSchema{Schema: "app", Name: "widgets", Columns: []*ir.Column{{Name: "a", Type: ir.Text{}}}}
		if err := verifyTableMapMatchesSchema(eventFor([]byte{mysql.MYSQL_TYPE_LONG}), tbl); err != nil {
			t.Errorf("no DataTypes means no comparison; got: %v", err)
		}
	})
	t.Run("nil event and nil table", func(t *testing.T) {
		if err := verifyTableMapMatchesSchema(nil, tblFor("int")); err != nil {
			t.Errorf("nil event: %v", err)
		}
		if err := verifyTableMapMatchesSchema(eventFor([]byte{mysql.MYSQL_TYPE_LONG}), nil); err != nil {
			t.Errorf("nil table: %v", err)
		}
	})
}

// TestBinlogTypeFamilyRoster is the fail-by-default roster that keeps the
// guard's data_type table from falling behind [translateType].
//
// It walks every data_type translateType accepts and requires each to be
// either CLASSIFIED by binlogTypeFamilyForDataType or listed in
// deliberatelyUnclassified with a reason. A new supported type therefore
// cannot join the unchecked set by accident — someone has to write down why.
//
// Scope, stated so the name cannot be read as broader than the truth: this
// covers the data_type side only. The binlog-CODE side is covered by
// TestVerifyTableMapMatchesSchema_FamilyMatrix, which exercises one code per
// family; a code this build has never seen is unclassifiable by design and
// is pinned as a skip, not a refusal.
func TestBinlogTypeFamilyRoster(t *testing.T) {
	// deliberatelyUnclassified: types the guard does not compare, each with
	// the reason it is exempt rather than an oversight.
	deliberatelyUnclassified := map[string]string{
		"uuid":  "MariaDB-native fixed-width type; its binlog carrier is not pinned against a real MariaDB here, and guessing wrong would halt every MariaDB sync carrying one (ADR-0171 decodes it from raw storage bytes)",
		"inet4": "MariaDB-native, same reason as uuid",
		"inet6": "MariaDB-native, same reason as uuid",
	}

	// Every data_type translateType has an arm for, derived from its switch
	// and the two registries it delegates to. Kept explicit rather than
	// reflected: translateType dispatches on a string, so there is nothing
	// to reflect over, and an explicit list is what a reviewer can diff.
	all := []string{
		"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year",
		"decimal", "numeric", "float", "double", "real",
		"bit",
		"char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
		"date", "time", "datetime", "timestamp",
		"enum", "set", "json",
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection",
		"uuid", "inet4", "inet6",
	}

	// Anti-vacuity floor: if the list above ever stops naming real types the
	// roster proves nothing, so first assert translateType actually accepts
	// every entry. That also catches a typo silently shrinking the roster.
	for _, dt := range all {
		meta := columnMeta{DataType: dt, ColumnType: metaColumnTypeFor(dt)}
		if _, err := translateType(meta); err != nil {
			t.Errorf("roster names %q but translateType rejects it (%v) — the roster is drifting from the type mapper", dt, err)
		}
	}
	if len(all) < 40 {
		t.Fatalf("roster has only %d entries; the MySQL type surface is larger than that — the list is truncated", len(all))
	}

	classified := 0
	for _, dt := range all {
		_, ok := binlogTypeFamilyForDataType(dt)
		reason, exempt := deliberatelyUnclassified[dt]
		switch {
		case ok && exempt:
			t.Errorf("data_type %q is BOTH classified and listed as exempt (%q); remove the exemption", dt, reason)
		case ok:
			classified++
		case exempt:
			// Fine — a written-down decision.
		default:
			t.Errorf("data_type %q is neither classified by binlogTypeFamilyForDataType nor listed in "+
				"deliberatelyUnclassified. Classify it, or add it with the reason it cannot be compared — "+
				"an unclassified type is silently exempt from the CDC-4 guard", dt)
		}
	}
	if classified < len(all)-len(deliberatelyUnclassified) {
		t.Errorf("classified %d of %d non-exempt types", classified, len(all)-len(deliberatelyUnclassified))
	}
}

// metaColumnTypeFor supplies the column_type spelling translateType needs
// for the types that parse it (enum/set members, bit width).
func metaColumnTypeFor(dataType string) string {
	switch dataType {
	case "enum":
		return "enum('a','b')"
	case "set":
		return "set('a','b')"
	case "bit":
		return "bit(8)"
	default:
		return dataType
	}
}
