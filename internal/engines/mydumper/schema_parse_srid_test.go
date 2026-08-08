// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

// Audit 2026-08-05 C-14, the mydumper door. MySQL 8's SHOW CREATE TABLE —
// and therefore every mydumper dump — spells a geometry column's spatial
// reference as `/*!80003 SRID 4326 */`, and the lexer dropped it with every
// other comment, on the strength of a doc claiming versioned blocks in a
// CREATE TABLE body carry "only skippable physical options". The column
// arrived as SRID 0, the target column was created without one, and every
// row was re-stamped.
//
// These pin the unwrap and, just as importantly, its NARROWNESS: the
// exception is one shape, not "lex versioned comments".

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVersionedSRIDComment(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantSRID string
		wantOK   bool
	}{
		{"the SHOW CREATE TABLE form", "/*!80003 SRID 4326 */", "4326", true},
		{"lowercase keyword", "/*!80003 srid 3857 */", "3857", true},
		{"no padding", "/*!80003 SRID 0*/", "0", true},
		// Everything below keeps the blanket skip. If any of these started
		// unwrapping, the exception would have widened into a general
		// versioned-comment lexer by accident.
		{"a partition clause", "/*!50100 PARTITION BY RANGE (id) */", "", false},
		{"a plain block comment", "/* SRID 4326 */", "", false},
		{"SRID with a trailing clause", "/*!80003 SRID 4326 NOT NULL */", "", false},
		{"SRID with no value", "/*!80003 SRID */", "", false},
		{"a non-numeric SRID", "/*!80003 SRID abc */", "", false},
		{"an identifier that merely starts with SRID", "/*!80003 SRIDX 1 */", "", false},
		{"unterminated", "/*!80003 SRID 4326", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srid, end, ok := versionedSRIDComment(tc.in, 0)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v; want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if srid != tc.wantSRID {
				t.Errorf("srid = %q; want %q", srid, tc.wantSRID)
			}
			if end != len(tc.in) {
				t.Errorf("end = %d; want %d (past the terminator)", end, len(tc.in))
			}
		})
	}
}

// TestParseColumn_SRIDReachesTheIRType is the end-of-lexer half: the
// unwrapped tokens must actually land on ir.Geometry.SRID, not merely be
// lexed. Two facts pinned separately would leave the composition unpinned.
func TestParseColumn_SRIDReachesTheIRType(t *testing.T) {
	const ddl = "CREATE TABLE `geo` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `declared` point /*!80003 SRID 4326 */ DEFAULT NULL,\n" +
		"  `bare` point DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	table, err := parseSchemaFile(ddl, "db.geo-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	want := map[string]int{"declared": 4326, "bare": 0}
	for _, col := range table.Columns {
		wantSRID, graded := want[col.Name]
		if !graded {
			continue
		}
		geom, ok := col.Type.(ir.Geometry)
		if !ok {
			t.Fatalf("column %s type = %#v; want ir.Geometry", col.Name, col.Type)
		}
		if geom.SRID != wantSRID {
			t.Errorf("column %s SRID = %d; want %d", col.Name, geom.SRID, wantSRID)
		}
	}
}
