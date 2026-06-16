//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 148 binlog SET pin. go-mysql's RowsEvent decoder hands a SET cell back
// as its NUMERIC BITMASK (int64), not the member text. Before the fix,
// decodeSet only handled the comma-joined text form (snapshot / VStream) and
// wasn't even passed the column's member list, so a binlog-sourced SET('a','c')
// (mask 5) silently decoded to ["5"] instead of ["a","c"] — the sibling of the
// ENUM ordinal-index Bug 145, and entirely unpinned (no SET-over-binlog test
// existed). This drives real binlog INSERTs of SET columns and asserts the
// decoded ir.Row carries the MEMBER LABELS, including a >8-member SET (multi-
// byte mask), an empty SET, and an all-bits SET.

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_Set_Bug148(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE setrows (
			id  BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			s3  SET('a','b','c'),
			s10 SET('m0','m1','m2','m3','m4','m5','m6','m7','m8','m9')
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	const dml = `
		INSERT INTO setrows (s3, s10) VALUES
			('a,c', 'm0,m9'),
			('',    'm5'),
			('a,b,c', 'm0,m1,m2,m3,m4,m5,m6,m7,m8,m9');
	`
	applyMySQL(t, dsn, dml)

	got := drainChanges(t, ctx, changes, 3, 30*time.Second)
	if len(got) != 3 {
		if cdcRdr, ok := rdr.(*CDCReader); ok {
			if streamErr := cdcRdr.Err(); streamErr != nil {
				t.Fatalf("got %d changes; want 3 (stream error: %v)", len(got), streamErr)
			}
		}
		t.Fatalf("got %d changes; want 3", len(got))
	}

	rows := make([]ir.Row, 0, 3)
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			t.Fatalf("change = %T; want ir.Insert", c)
		}
		rows = append(rows, ins.Row)
	}

	// The load-bearing assertions: SET decodes to MEMBER LABELS over the real
	// binlog mask, in declaration order — never the numeric mask string.
	cases := []struct {
		row     int
		col     string
		want    []string
		comment string
	}{
		{0, "s3", []string{"a", "c"}, "bits 0,2 (mask 5) — the literal pre-fix victim"},
		{0, "s10", []string{"m0", "m9"}, "multi-byte mask (bit 9 set)"},
		{1, "s3", []string{}, "empty SET (mask 0)"},
		{1, "s10", []string{"m5"}, "single member"},
		{2, "s3", []string{"a", "b", "c"}, "all 3 bits"},
		{2, "s10", []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}, "all 10 bits"},
	}
	for _, c := range cases {
		got := rows[c.row][c.col]
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("row %d col %s = %#v; want %#v (%s)", c.row, c.col, got, c.want, c.comment)
		}
	}
}
