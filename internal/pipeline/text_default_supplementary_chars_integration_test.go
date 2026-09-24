//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (h): a literal DEFAULT on a character column that holds a
// character outside the Basic Multilingual Plane (an emoji, a CJK
// extension ideograph — anything four bytes in UTF-8) reached every target
// as '?'. MySQL and MariaDB store COLUMN_DEFAULT in utf8mb3, which cannot
// hold such a character, so information_schema reads `VARCHAR DEFAULT
// '😀x'` back as '?x' (MariaDB TEXT as '????x'), and sluice created the
// target's DEFAULT from that text at exit 0 — on migrate, sync cold start,
// restore and a forwarded ADD COLUMN alike. The readers now re-read such a
// default with a utf8mb4 DEFAULT() probe (internal/engines/mysql
// text_default_recovery.go).
//
// # The independent expected value
//
// The SOURCE server's own fill. On migrate: a row inserted on the source
// naming only its key, against a row inserted the same way on the target
// after the migrate (runLOBDefaultLane). On a forwarded ADD COLUMN, with
// the backfill suppressed so the target keeps what its own DEFAULT filled:
// the source rows that existed at the ALTER against the target's same rows
// (runForwardedDefaultLane). Neither side is read through sluice, and no
// catalog is consulted — the catalog is what loses the character.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// supplementaryTextDefaults is the literal-default matrix, as MySQL spells
// it: the lost character alone, beside ASCII, beside a BMP character, and
// around genuine '?'s; a genuine '?' alone, which must stay '?'; plain ASCII;
// a non-utf8mb4 column charset; and an ENUM whose default is a genuine '?'
// label. mariadb adds the TEXT literal MariaDB (unlike MySQL) accepts, where
// its catalog writes one '?' per byte of the lost character.
func supplementaryTextDefaults(mariadb bool) [][2]string {
	shapes := [][2]string{
		{"s_vc", "VARCHAR(20) DEFAULT '😀x'"},
		{"s_char", "CHAR(4) DEFAULT '😀'"},
		{"s_bmp", "VARCHAR(20) DEFAULT 'é😀'"},
		{"s_mix", "VARCHAR(20) DEFAULT '?😀?'"},
		{"s_q", "VARCHAR(20) DEFAULT '?'"},
		{"s_ascii", "VARCHAR(20) DEFAULT 'abc'"},
		{"s_utf16", "VARCHAR(10) CHARACTER SET utf16 DEFAULT '😀w'"},
		{"s_enumq", "ENUM('a','?b') DEFAULT '?b'"},
	}
	if mariadb {
		shapes = append(shapes, [2]string{"s_text", "TEXT DEFAULT '😀z'"})
	}
	return shapes
}

func supplementaryMigrateLane(t *testing.T, sourceEngine, targetEngine, src, tgt string, tgtDialect fdDialect) {
	t.Helper()
	var cells []lobDefaultCell
	for _, sh := range supplementaryTextDefaults(sourceEngine == "mariadb") {
		cells = append(cells, lobDefaultCell{
			col: sh[0], def: sh[1],
			srcExpr: fdCanonExpr(fdMySQL, fdText, sh[0]),
			tgtExpr: fdCanonExpr(tgtDialect, fdText, sh[0]),
		})
	}
	runLOBDefaultLane(t, lobDefaultLane{
		sourceEngine: sourceEngine, targetEngine: targetEngine,
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: tgtDialect,
		cells: cells,
	})
}

func TestMigrate_SupplementaryCharTextDefaults_MySQLToMySQL(t *testing.T) {
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()
	supplementaryMigrateLane(t, "mysql", "mysql", src, tgt, fdMySQL)
}

func TestMigrate_SupplementaryCharTextDefaults_MySQLToPostgres(t *testing.T) {
	src, _, myCleanup := startMySQL(t)
	defer myCleanup()
	_, tgt, pgCleanup := startPostgres(t)
	defer pgCleanup()
	supplementaryMigrateLane(t, "mysql", "postgres", src, tgt, fdPG)
}

func TestMigrate_SupplementaryCharTextDefaults_MariaDBToMariaDB(t *testing.T) {
	src, tgt, cleanup := startMariaDB(t)
	defer cleanup()
	supplementaryMigrateLane(t, "mariadb", "mariadb", src, tgt, fdMySQL)
}

// TestMigrate_SupplementaryCharEnumLabel_Refuses pins the loud half: an
// ENUM whose LABELS hold a supplementary character cannot be read
// faithfully (COLUMN_TYPE loses them exactly as COLUMN_DEFAULT does; GC-37
// (i)), and a default that uses such a label is how the reader notices. It
// must refuse, naming the column, rather than create the target with the
// '?' label — whose rows the binlog decodes by index into that '?' label.
func TestMigrate_SupplementaryCharEnumLabel_Refuses(t *testing.T) {
	for _, f := range []struct {
		engine string
		start  func(*testing.T) (string, string, func())
	}{
		{"mysql", startMySQL},
		{"mariadb", startMariaDB},
	} {
		t.Run(f.engine, func(t *testing.T) {
			src, tgt, cleanup := f.start(t)
			defer cleanup()
			fdExec(t, fdMySQL, src, "CREATE TABLE lbl (id BIGINT NOT NULL PRIMARY KEY, e ENUM('a','😀b') DEFAULT '😀b')")
			eng, ok := engines.Get(f.engine)
			if !ok {
				t.Fatalf("%s engine not registered", f.engine)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			err := (&Migrator{Source: eng, Target: eng, SourceDSN: src, TargetDSN: tgt}).Run(ctx)
			if err == nil {
				t.Fatal("Migrator.Run succeeded on an ENUM whose label information_schema reports as '?b'; want the loud type refusal")
			}
			for _, want := range []string{"cannot read this column's type faithfully", "lbl.e"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Migrator.Run error = %v; want it to contain %q", err, want)
				}
			}
		})
	}
}

// supplementaryForwardShapes is the same matrix as forwarded ADD COLUMN
// shapes, graded as text — less the utf16 column. Its DEFAULT forwards
// correctly (the migrate lanes grade it), but the binlog carries a row
// value in the COLUMN's charset and the CDC decoder hands those bytes on
// as if they were UTF-8, so the post-ALTER row the lane uses as its
// liveness signal fails to apply (MySQL Error 3988 / MariaDB 1366 / PG
// "contains a NUL byte", measured). That is GC-37 (j), not a DEFAULT.
func supplementaryForwardShapes(mariadb bool) []fdShape {
	var out []fdShape
	for _, sh := range supplementaryTextDefaults(mariadb) {
		if sh[0] == "s_utf16" {
			continue
		}
		out = append(out, fdShape{col: sh[0], def: sh[1], fam: fdText})
	}
	return out
}

// The forwarded-ADD-COLUMN lanes run with the backfill suppressed: the
// default-on backfill reads the pre-existing rows' values back from the
// source and would repair a wrong DEFAULT's fill, hiding exactly what this
// grades — the DEFAULT the forward carried, which every row the target
// inserts later omitting the column also gets.

func TestStreamer_AddColumnForward_SupplementaryCharTextDefaults_MySQLToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->mysql supplementary", sourceEngine: "mysql", targetEngine: "mysql",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes: supplementaryForwardShapes(false), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}

func TestStreamer_AddColumnForward_SupplementaryCharTextDefaults_MySQLToPostgres(t *testing.T) {
	src, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgt, tgtCleanup := startPostgres(t)
	defer tgtCleanup()
	runForwardedDefaultLane(t, fdLane{
		name: "mysql->postgres supplementary", sourceEngine: "mysql", targetEngine: "postgres",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdPG,
		shapes: supplementaryForwardShapes(false), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}

func TestStreamer_AddColumnForward_SupplementaryCharTextDefaults_MariaDBToMariaDB(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	tgtServer, tgtCleanup := startMariaDBBinlog(t)
	defer tgtCleanup()
	tgt := fdFreshDB(t, fdMySQL, tgtServer, "target_db")
	runForwardedDefaultLane(t, fdLane{
		name: "mariadb->mariadb supplementary", sourceEngine: "mariadb", targetEngine: "mariadb",
		sourceDSN: src, targetDSN: tgt, src: fdMySQL, tgt: fdMySQL,
		shapes: supplementaryForwardShapes(true), knownWrong: map[string]fdKnownWrong{},
		suppressBackfill: true,
	})
}
