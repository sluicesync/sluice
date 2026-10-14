// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"
)

// TestClassifyDDLReach pins what each DDL statement shape can reshape. The
// failure direction is the point: a wrong "neutral" or a missed table is a
// silent mis-decode (Bug 301), a wrong "reaches" is a cache reload or a
// loud refusal. So every shape that renames, moves, adds or drops a column
// must reach its table, and only index/option/data statements are neutral.
func TestClassifyDDLReach(t *testing.T) {
	app := func(table string) []ddlTableRef { return []ddlTableRef{{schema: "app", table: table}} }
	cases := []struct {
		stmt string
		want ddlReach
	}{
		// Column-list changes reach the table.
		{"ALTER TABLE kd ADD COLUMN d INT", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd DROP COLUMN c", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd MODIFY c VARCHAR(40) AFTER id", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd MODIFY c VARCHAR(40) FIRST", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd RENAME COLUMN b TO x", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd RENAME COLUMN b TO c, RENAME COLUMN c TO b", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd CHANGE b x VARCHAR(40)", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd DROP COLUMN b, ADD COLUMN e VARCHAR(40)", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd ADD PRIMARY KEY (id)", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd DROP PRIMARY KEY", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd MODIFY g INT AS (id + 1) VIRTUAL", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd RENAME TO kd2", ddlReach{tables: append(app("kd"), app("kd2")...)}},
		{"ALTER TABLE other.kd ADD COLUMN d INT", ddlReach{tables: []ddlTableRef{{schema: "other", table: "kd"}}}},
		{"ALTER TABLE `we``ird`.`k d` ADD COLUMN d INT", ddlReach{tables: []ddlTableRef{{schema: "we`ird", table: "k d"}}}},
		{"CREATE TABLE kd (id INT PRIMARY KEY)", ddlReach{tables: app("kd")}},
		{"DROP TABLE kd, other.t", ddlReach{tables: []ddlTableRef{{"app", "kd"}, {"other", "t"}}}},
		{"RENAME TABLE kd TO old_kd, new_kd TO kd", ddlReach{tables: []ddlTableRef{{"app", "kd"}, {"app", "old_kd"}, {"app", "new_kd"}, {"app", "kd"}}}},
		{"DROP DATABASE app", ddlReach{databases: []string{"app"}}},
		// A leading run of plain session assignments reshapes nothing: the
		// statement after it is classified (measured on mariadb:11.4, a
		// target's control-table CREATE reached every stream's table).
		{"set foreign_key_checks=0; CREATE TABLE IF NOT EXISTS `ctl` (id INT PRIMARY KEY)", ddlReach{tables: app("ctl")}},
		{"SET sql_log_bin=0, @@session.foreign_key_checks = 0; SET NAMES utf8mb4; ALTER TABLE kd ADD COLUMN d INT", ddlReach{tables: app("kd")}},
		// Not provably plain: unchanged, so as unbounded as before.
		{"SET sql_mode='ANSI'; CREATE TABLE ctl (id INT PRIMARY KEY)", ddlReach{all: true}},
		{"set foreign_key_checks=0; CREATE TABLE ctl (id INT PRIMARY KEY); ALTER TABLE kd DROP COLUMN c", ddlReach{all: true}},
		// A lone SET the parser reads: neutral, as before.
		{"SET foreign_key_checks=0", ddlReach{neutral: true}},

		// In place: names and positions kept, some types may change.
		{"ALTER TABLE kd MODIFY b VARCHAR(80)", ddlReach{tables: app("kd"), inPlace: true, retyped: []string{"b"}}},
		{"ALTER TABLE kd MODIFY b INT NOT NULL", ddlReach{tables: app("kd"), inPlace: true, retyped: []string{"b"}}},
		{"ALTER TABLE kd CHANGE b b VARCHAR(80)", ddlReach{tables: app("kd"), inPlace: true, retyped: []string{"b"}}},
		{"ALTER TABLE kd ALTER COLUMN b SET DEFAULT 'x'", ddlReach{tables: app("kd"), inPlace: true}},
		{"ALTER TABLE kd CONVERT TO CHARACTER SET latin1", ddlReach{tables: app("kd"), inPlace: true, retypedAll: true}},
		{"ALTER TABLE kd MODIFY b VARCHAR(80), ADD INDEX ib (b)", ddlReach{tables: app("kd"), inPlace: true, retyped: []string{"b"}}},

		// Neutral: no column list, column type or primary key changes.
		{"ALTER TABLE kd ADD INDEX ib (b)", ddlReach{neutral: true}},
		{"ALTER TABLE kd ADD UNIQUE KEY ub (b)", ddlReach{neutral: true}},
		{"ALTER TABLE kd DROP INDEX ib", ddlReach{neutral: true}},
		{"ALTER TABLE kd RENAME INDEX ib TO ib2", ddlReach{neutral: true}},
		{"ALTER TABLE kd COMMENT='x'", ddlReach{neutral: true}},
		{"ALTER TABLE kd ENGINE=InnoDB", ddlReach{neutral: true}},
		{"ALTER TABLE kd AUTO_INCREMENT=100", ddlReach{neutral: true}},
		{"ALTER TABLE kd DEFAULT CHARSET=latin1", ddlReach{neutral: true}},
		{"ALTER TABLE kd FORCE", ddlReach{neutral: true}},
		{"CREATE INDEX ib ON kd (b)", ddlReach{neutral: true}},
		{"DROP INDEX ib ON kd", ddlReach{neutral: true}},
		{"TRUNCATE TABLE kd", ddlReach{neutral: true}},
		{"CREATE VIEW v AS SELECT id FROM kd", ddlReach{neutral: true}},
		{"CREATE DATABASE x", ddlReach{neutral: true}},
		{"GRANT SELECT ON app.* TO 'u'@'%'", ddlReach{neutral: true}},
		{"CREATE DEFINER=`root`@`%` TRIGGER tr BEFORE INSERT ON kd FOR EACH ROW SET NEW.b = 'x'", ddlReach{neutral: true}},
		{"SAVEPOINT s1", ddlReach{neutral: true}},
		{"ROLLBACK TO SAVEPOINT s1", ddlReach{neutral: true}},

		// Every spelling of a primary-key change reshapes the table.
		{"ALTER TABLE kd DROP INDEX `PRIMARY`", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd DROP KEY `PRIMARY`", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd DROP CONSTRAINT `PRIMARY`", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd MODIFY b VARCHAR(40) PRIMARY KEY", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd CHANGE b b VARCHAR(40) UNIQUE KEY", ddlReach{tables: app("kd")}},

		// Unparseable, but its prefix names its target: bounded to it, and
		// reshaping (never neutral, never in place).
		{"ALTER TABLE kd FROBNICATE COLUMN b", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd ADD COLUMN u UUID", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd ADD COLUMN IF NOT EXISTS x INT", ddlReach{tables: app("kd")}},
		{"ALTER TABLE other.kd ADD COLUMN u UUID", ddlReach{tables: []ddlTableRef{{"other", "kd"}}}},
		{"CREATE OR REPLACE TABLE kd (id INT PRIMARY KEY, u UUID)", ddlReach{tables: app("kd")}},
		{"CREATE SEQUENCE other.s", ddlReach{tables: []ddlTableRef{{"other", "s"}}}},
		{"CREATE TABLE kd (id INT PRIMARY KEY) WITH SYSTEM VERSIONING", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd SECONDARY_ENGINE=RAPID", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd ORDER BY id", ddlReach{tables: app("kd")}},
		{"DROP TABLE IF EXISTS other.a, other.b", ddlReach{tables: []ddlTableRef{{"other", "a"}, {"other", "b"}}}},
		// Unparseable and names no table.
		{"CREATE TABLESPACE ts ADD DATAFILE 'x.ibd'", ddlReach{neutral: true}},
		{"ALTER INSTANCE ROTATE INNODB MASTER KEY", ddlReach{neutral: true}},
		{"CREATE RESOURCE GROUP rg TYPE = USER", ddlReach{neutral: true}},
		{"ALTER DATABASE x READ ONLY = 1", ddlReach{neutral: true}},
		{"CHECK TABLE kd", ddlReach{neutral: true}},

		// Unbounded: unreadable, or unqualified with no default database.
		{"FROBNICATE kd", ddlReach{all: true}},
		// A target inside a comment the server may execute cannot be bounded.
		{"ALTER TABLE /*!50100 kd */ ADD COLUMN u UUID", ddlReach{all: true}},
		{"ALTER TABLE kd/*!50100 _x */ ADD COLUMN u UUID", ddlReach{all: true}},
		{"CREATE OR REPLACE SEQUENCE /*!50100 other.s */", ddlReach{all: true}},
		// The parser reads a versioned comment as SQL, as the server does.
		{"ALTER TABLE /*!50100 kd */ ADD COLUMN x INT", ddlReach{tables: app("kd")}},
		{"ALTER TABLE kd ADD INDEX i (b) /*!50100 , ADD COLUMN x INT */", ddlReach{tables: app("kd")}},
		{"/*!50100 ALTER TABLE kd ADD COLUMN x INT */", ddlReach{tables: app("kd")}},
		{"ALTER TABLE", ddlReach{all: true}},
		{"", ddlReach{all: true}},
	}
	for _, c := range cases {
		got := classifyDDLReach(c.stmt, "app")
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("classifyDDLReach(%q)\n got  %+v\n want %+v", c.stmt, got, c.want)
		}
	}
	if got := classifyDDLReach("ALTER TABLE kd ADD COLUMN d INT", ""); !got.all {
		t.Errorf("an unqualified table with no default database must reach every table; got %+v", got)
	}
}

// TestDDLReach_Reaches pins the table and database matching, which is
// case-insensitive in the over-matching direction.
func TestDDLReach_Reaches(t *testing.T) {
	r := classifyDDLReach("ALTER TABLE KD ADD COLUMN d INT", "App")
	if !r.reaches("app", "kd") || r.reaches("app", "other") || r.reaches("other", "kd") {
		t.Errorf("table reach wrong: %+v", r)
	}
	db := classifyDDLReach("DROP DATABASE app", "")
	if !db.reaches("APP", "anything") || db.reaches("other", "kd") {
		t.Errorf("database reach wrong: %+v", db)
	}
	inScope := func(s string) bool { return s == "app" }
	if !classifyDDLReach("ALTER TABLE app.kd ADD COLUMN d INT", "mysql").reachesDatabase(inScope) {
		t.Error("an ALTER of an in-scope table issued from a foreign default database must reach the scope")
	}
	if classifyDDLReach("ALTER TABLE other.kd ADD COLUMN d INT", "mysql").reachesDatabase(inScope) {
		t.Error("an ALTER of an out-of-scope table must not reach the scope")
	}
	if classifyDDLReach("SAVEPOINT s", "mysql").reachesDatabase(inScope) {
		t.Error("a SAVEPOINT must not reach the scope (it would clear the decode cache per transaction)")
	}
}
