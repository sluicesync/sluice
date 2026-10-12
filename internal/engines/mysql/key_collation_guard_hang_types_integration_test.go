//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION review F-B: the guard's assignment hangs on a NON-key
// column, whose type is anything a table can hold. Its first spelling put
// the raise in a value arm — `IF(ok, new.h, <BIGINT UNSIGNED overflow>)` —
// and MariaDB type-checks an IF's arms against each other: with h a UUID,
// INET6 or GEOMETRY every guarded statement failed (error 4078) before any
// row, a wedge on the first insert. The obvious repair, a CAST of the raise
// to CHAR, passes that check and routes the value through a string, which
// was measured to SILENTLY change FLOAT values. This is the family matrix
// over the hang column's type.
//
// The independent expected value is a reference row written by a plain SQL
// literal on the same server, compared by `<=>` and by its bytes — never
// the applier's own account of what it wrote.

package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

type kcgHangType struct {
	name, ddl string
	// literal is the SQL spelling of the value; value is what the applier
	// is handed for it (the CDC reader's representation of that type).
	literal   string
	value     any
	mariaOnly bool
}

func kcgHangTypes() []kcgHangType {
	bit := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}
	return []kcgHangType{
		{"UUID", "UUID", "'00000000-0000-0000-0000-0000000000ff'", "00000000-0000-0000-0000-0000000000ff", true},
		{"INET6", "INET6", "'2001:db8::ff00:42:8329'", "2001:db8::ff00:42:8329", true},
		{"GEOMETRY", "GEOMETRY", "ST_GeomFromText('LINESTRING(0 0,1.5 2.25)')", nil, false}, // value read back from the server as WKB
		{"JSON", "JSON", `'{"b": [1, 2.50, "x"], "n": null}'`, `{"b": [1, 2.50, "x"], "n": null}`, false},
		{"TIMESTAMP6", "TIMESTAMP(6) NULL", "'2024-03-10 02:30:00.123456'", "2024-03-10 02:30:00.123456", false},
		{"DATETIME6", "DATETIME(6)", "'9999-12-31 23:59:59.999999'", "9999-12-31 23:59:59.999999", false},
		{"DECIMAL65", "DECIMAL(65,30)", "'12345678901234567890123456789012345.123456789012345678901234567891'", "12345678901234567890123456789012345.123456789012345678901234567891", false},
		{"BIT64", "BIT(64)", "0xFFFFFFFFFFFFFFFE", bit, false},
		{"FLOAT", "FLOAT", "1.17549435e-38", float32(1.17549435e-38), false},
		{"FLOAT-INT", "FLOAT", "16777216", float32(16777216), false},
		{"DOUBLE", "DOUBLE", "0.30000000000000004", 0.30000000000000004, false},
		{"VARBINARY", "VARBINARY(16)", "0xFF00FE80C0", []byte{0xFF, 0x00, 0xFE, 0x80, 0xC0}, false},
		{"BLOB", "BLOB", "0x00FF80", []byte{0x00, 0xFF, 0x80}, false},
		{"SET", "SET('x','y','z')", "'x,z'", "x,z", false},
		{"ENUM", "ENUM('p','q')", "'q'", "q", false},
		{"TIME6", "TIME(6)", "'-838:59:59.000000'", "-838:59:59.000000", false},
		{"YEAR", "YEAR", "2155", int64(2155), false},
		{"BIGINT-UNSIGNED", "BIGINT UNSIGNED", "18446744073709551615", uint64(18446744073709551615), false},
	}
}

// TestKeyCollationGuard_HangTypeFamilies pins the guard's assignment over
// the hang column's type family × both apply doors × MySQL 8.0 and MariaDB
// 11.4: a non-conflicting insert and a same-row conflict must land the
// value byte-exact, and a merge must refuse KEY-COLLATION-MERGE with the
// target untouched.
func TestKeyCollationGuard_HangTypeFamilies(t *testing.T) {
	for _, srv := range kcgServers(t) {
		for _, batched := range []bool{false, true} {
			door := "serial"
			if batched {
				door = "batched"
			}
			srv, batched := srv, batched
			t.Run(srv.name+"/"+door, func(t *testing.T) {
				db := fmt.Sprintf("kcght_%s_%t", strings.NewReplacer("-", "", ".", "").Replace(srv.name), batched)
				dsn, cleanup := srv.newDB(t, db)
				t.Cleanup(cleanup)
				maria := srv.eng.Flavor == FlavorMariaDB
				for i, ht := range kcgHangTypes() {
					if ht.mariaOnly && !maria {
						continue
					}
					t.Run(ht.name, func(t *testing.T) {
						kcgRunHangType(t, srv.eng, dsn, db, fmt.Sprintf("h%d", i), ht, batched)
					})
				}
			})
		}
	}
}

func kcgRunHangType(t *testing.T, eng Engine, dsn, db, table string, ht kcgHangType, batched bool) {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ref := table + "_ref"
	// The hang column sorts first among the insert's columns (`a_h` < `id`
	// < `k`) and is not the primary key, so the guard hangs on it.
	applyDDL(t, dsn, "CREATE TABLE "+table+" (id INT NOT NULL PRIMARY KEY, a_h "+ht.ddl+", "+
		"k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, UNIQUE KEY uk (k))")
	applyDDL(t, dsn, "CREATE TABLE "+ref+" (id INT NOT NULL PRIMARY KEY, a_h "+ht.ddl+")")
	applyDDL(t, dsn, "INSERT INTO "+ref+" VALUES (1, "+ht.literal+")")
	applyDDL(t, dsn, "INSERT INTO "+table+" (id, a_h, k) VALUES (1, NULL, 'a')")
	value := ht.value
	if value == nil {
		var raw []byte
		// The applier carries a geometry as WKB (no SRID prefix).
		if err := conn.QueryRow("SELECT ST_AsBinary(a_h) FROM " + ref + " WHERE id = 1").Scan(&raw); err != nil {
			t.Fatalf("read the reference value: %v", err)
		}
		value = raw
	}
	snap := kcgSnapshot(db, table, []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "a_h", Type: ir.Integer{Width: 32}, Nullable: true}, // not a character column: never armed
		{Name: "k", Type: kcgVarchar("utf8mb4_bin")},
	})
	exact := func(what string, id int) {
		t.Helper()
		var gotHex, refHex sql.NullString
		var same sql.NullInt64
		q := fmt.Sprintf("SELECT HEX(CAST(t.a_h AS BINARY)), HEX(CAST(r.a_h AS BINARY)), t.a_h <=> r.a_h FROM %s t, %s r WHERE t.id = %d AND r.id = 1", table, ref, id)
		if err := conn.QueryRow(q).Scan(&gotHex, &refHex, &same); err != nil {
			t.Fatalf("%s: read: %v", what, err)
		}
		if gotHex != refHex || same.Int64 != 1 {
			t.Fatalf("%s: the hang column landed %s, the reference is %s (<=> %d)", what, gotHex.String, refHex.String, same.Int64)
		}
	}
	insert := func(n, id int, k string) ir.Change {
		return ir.Insert{Position: kcgPos(n), Schema: db, Table: table, Row: ir.Row{"id": int64(id), "a_h": value, "k": k}}
	}

	if err := kcgApply(t, eng, dsn, batched, snap, insert(1, 2, "b")); err != nil {
		t.Fatalf("a non-conflicting insert refused: %v", err)
	}
	exact("non-conflicting insert", 2)
	if err := kcgApply(t, eng, dsn, batched, snap, insert(2, 1, "a")); err != nil {
		t.Fatalf("a same-row conflict refused: %v", err)
	}
	exact("same-row conflict", 1)
	before := kcgRows(t, dsn, table, "id")
	err = kcgApply(t, eng, dsn, batched, snap, insert(3, 3, "A"))
	if !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("merge: err = %v; want %s", err, ir.ErrKeyCollationMerge)
	}
	if got := kcgRows(t, dsn, table, "id"); got != before {
		t.Fatalf("the refusal changed the target: %s → %s", before, got)
	}
	exact("after the refused merge", 1)
}
