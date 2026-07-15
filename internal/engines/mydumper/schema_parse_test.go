// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// realSchemaFile is a mydumper-shaped schema file: version-comment SET
// headers plus one SHOW-CREATE-formatted CREATE TABLE covering every type
// family the engine maps (the schema half of the ADR-0161 pin matrix).
const realSchemaFile = "/*!40101 SET NAMES binary*/;\n" +
	"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
	"/*!40103 SET TIME_ZONE='+00:00' */;\n" +
	"CREATE TABLE `families` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `small` smallint DEFAULT '7',\n" +
	"  `flag` tinyint(1) NOT NULL DEFAULT '1',\n" +
	"  `price` decimal(20,4) DEFAULT NULL,\n" +
	"  `ratio` float DEFAULT NULL,\n" +
	"  `wide` double DEFAULT NULL,\n" +
	"  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',\n" +
	"  `fixed` char(4) DEFAULT NULL,\n" +
	"  `body` longtext,\n" +
	"  `bin` binary(2) DEFAULT '\\0\\0',\n" +
	"  `vbin` varbinary(64) DEFAULT NULL,\n" +
	"  `payload` longblob,\n" +
	"  `made` date DEFAULT NULL,\n" +
	"  `at` datetime(6) DEFAULT CURRENT_TIMESTAMP(6),\n" +
	"  `seen` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
	"  `dur` time DEFAULT NULL,\n" +
	"  `doc` json DEFAULT NULL,\n" +
	"  `role` enum('admin','it''s','a\\'b') NOT NULL DEFAULT 'admin',\n" +
	"  `tags` set('x','y') DEFAULT NULL,\n" +
	"  `mask` bit(5) DEFAULT b'101',\n" +
	"  `onebit` bit(1) DEFAULT b'1',\n" +
	"  `pt` point /*!80003 SRID 4326 */ DEFAULT NULL,\n" +
	"  `up` varchar(16) GENERATED ALWAYS AS (upper(`name`)) STORED,\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  UNIQUE KEY `families_name` (`name`,`fixed`(2)),\n" +
	"  KEY `families_small` (`small` DESC) USING BTREE,\n" +
	"  FULLTEXT KEY `families_body` (`body`),\n" +
	"  CONSTRAINT `chk_small` CHECK ((`small` >= 0))\n" +
	") ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='the corpus';\n"

func TestParseSchemaFile_FullFamilyCreateTable(t *testing.T) {
	table, err := parseSchemaFile(realSchemaFile, "db.families-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if table.Name != "families" {
		t.Fatalf("name = %q", table.Name)
	}
	if table.Comment != "the corpus" {
		t.Fatalf("comment = %q", table.Comment)
	}

	wantTypes := map[string]ir.Type{
		"id":      ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
		"small":   ir.Integer{Width: 16},
		"flag":    ir.Boolean{},
		"price":   ir.Decimal{Precision: 20, Scale: 4},
		"ratio":   ir.Float{Precision: ir.FloatSingle},
		"wide":    ir.Float{Precision: ir.FloatDouble},
		"name":    ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		"fixed":   ir.Char{Length: 4},
		"body":    ir.Text{Size: ir.TextLong},
		"bin":     ir.Binary{Length: 2},
		"vbin":    ir.Varbinary{Length: 64},
		"payload": ir.Blob{Size: ir.BlobLong},
		"made":    ir.Date{},
		"at":      ir.DateTime{Precision: 6},
		"seen":    ir.Timestamp{WithTimeZone: true},
		"dur":     ir.Time{},
		"doc":     ir.JSON{Binary: true},
		"role":    ir.Enum{Values: []string{"admin", "it's", "a'b"}},
		"tags":    ir.Set{Values: []string{"x", "y"}},
		"mask":    ir.Bit{Length: 5},
		"onebit":  ir.Boolean{},
		"pt":      ir.Geometry{Subtype: ir.GeometryPoint},
		"up":      ir.Varchar{Length: 16},
	}
	if len(table.Columns) != len(wantTypes) {
		t.Fatalf("columns = %d; want %d", len(table.Columns), len(wantTypes))
	}
	for _, col := range table.Columns {
		want, ok := wantTypes[col.Name]
		if !ok {
			t.Fatalf("unexpected column %q", col.Name)
		}
		if !reflect.DeepEqual(col.Type, want) {
			t.Errorf("column %s type = %#v; want %#v", col.Name, col.Type, want)
		}
	}

	// Nullability + defaults spot checks per shape.
	col := func(name string) *ir.Column {
		for _, c := range table.Columns {
			if c.Name == name {
				return c
			}
		}
		t.Fatalf("column %q missing", name)
		return nil
	}
	if col("id").Nullable {
		t.Error("id should be NOT NULL")
	}
	if d, ok := col("small").Default.(ir.DefaultLiteral); !ok || d.Value != "7" {
		t.Errorf("small default = %#v", col("small").Default)
	}
	if d, ok := col("price").Default.(ir.DefaultNone); !ok {
		t.Errorf("price default = %#v; want DefaultNone", d)
	}
	if d, ok := col("at").Default.(ir.DefaultExpression); !ok || d.Expr != "CURRENT_TIMESTAMP(6)" || d.Dialect != "mysql" {
		t.Errorf("at default = %#v", col("at").Default)
	}
	if d, ok := col("mask").Default.(ir.DefaultExpression); !ok || d.Expr != "b'101'" || d.Dialect != "bit" {
		t.Errorf("mask default = %#v", col("mask").Default)
	}
	if d, ok := col("onebit").Default.(ir.DefaultLiteral); !ok || d.Value != "1" {
		t.Errorf("onebit default = %#v", col("onebit").Default)
	}
	if d, ok := col("bin").Default.(ir.DefaultLiteral); !ok || d.Value != "\x00\x00" {
		t.Errorf("bin default = %#v", col("bin").Default)
	}
	if up := col("up"); up.GeneratedExpr != "upper(name)" || !up.GeneratedStored || up.GeneratedExprDialect != "mysql" {
		t.Errorf("generated column = %+v", up)
	}

	// PK + indexes.
	if table.PrimaryKey == nil || table.PrimaryKey.Name != "PRIMARY" ||
		len(table.PrimaryKey.Columns) != 1 || table.PrimaryKey.Columns[0].Column != "id" {
		t.Fatalf("primary key = %+v", table.PrimaryKey)
	}
	if len(table.Indexes) != 3 {
		t.Fatalf("indexes = %d; want 3", len(table.Indexes))
	}
	idx := map[string]*ir.Index{}
	for _, ix := range table.Indexes {
		idx[ix.Name] = ix
	}
	if u := idx["families_name"]; u == nil || !u.Unique || len(u.Columns) != 2 ||
		u.Columns[1].Column != "fixed" || u.Columns[1].Length != 2 {
		t.Errorf("unique index = %+v", idx["families_name"])
	}
	if k := idx["families_small"]; k == nil || k.Unique || !k.Columns[0].Desc || k.Kind != ir.IndexKindBTree {
		t.Errorf("plain index = %+v", idx["families_small"])
	}
	if f := idx["families_body"]; f == nil || f.Kind != ir.IndexKindFullText {
		t.Errorf("fulltext index = %+v", idx["families_body"])
	}

	// CHECK constraint, normalized to portable text.
	if len(table.CheckConstraints) != 1 {
		t.Fatalf("checks = %d; want 1", len(table.CheckConstraints))
	}
	if c := table.CheckConstraints[0]; c.Name != "chk_small" || c.Expr != "(small >= 0)" || c.ExprDialect != "mysql" {
		t.Errorf("check = %+v", c)
	}
}

func TestParseSchemaFile_ForeignKeys(t *testing.T) {
	src := "CREATE TABLE `posts` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `user_id` bigint NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  KEY `posts_user` (`user_id`),\n" +
		"  CONSTRAINT `posts_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) " +
		"ON DELETE CASCADE ON UPDATE SET NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	table, err := parseSchemaFile(src, "db.posts-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if len(table.ForeignKeys) != 1 {
		t.Fatalf("fks = %d; want 1", len(table.ForeignKeys))
	}
	fk := table.ForeignKeys[0]
	if fk.Name != "posts_fk" || fk.ReferencedTable != "users" ||
		!reflect.DeepEqual(fk.Columns, []string{"user_id"}) ||
		!reflect.DeepEqual(fk.ReferencedColumns, []string{"id"}) ||
		fk.OnDelete != ir.FKActionCascade || fk.OnUpdate != ir.FKActionSetNull {
		t.Fatalf("fk = %+v", fk)
	}
}

// TestParseSchemaFile_Refusals pins the bounded-parse scope line: anything
// other than comments/SET + exactly one CREATE TABLE refuses loudly naming
// the file (ADR-0161 §3).
func TestParseSchemaFile_Refusals(t *testing.T) {
	const create = "CREATE TABLE `t` (`id` bigint NOT NULL) ENGINE=InnoDB;\n"
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{"two-create-tables", create + create, "more than one CREATE TABLE"},
		{"alter", create + "ALTER TABLE `t` ADD COLUMN `x` int;\n", "ALTER statement"},
		{"insert", create + "INSERT INTO `t` VALUES (1);\n", "INSERT statement"},
		{"drop-before", "DROP TABLE IF EXISTS `t`;\n" + create, "DROP statement"},
		{"create-view", "CREATE VIEW `v` AS SELECT 1;\n", "not CREATE TABLE"},
		{"garbage", "hello world;\n", "HELLO statement"},
		{"empty", "-- nothing here\n", "no CREATE TABLE"},
		{"bad-charset-set", "SET NAMES latin1;\n" + create, "SET NAMES latin1"},
		{"unknown-attribute", "CREATE TABLE `t` (`id` bigint NOT NULL FROBNICATE);\n", "unsupported column attribute"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSchemaFile(tc.content, "db.t-schema.sql")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v; want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestParseSchemaFile_CharsetGate pins the utf8-or-refuse posture
// (ADR-0161 §5) across the column/table charset inheritance shapes.
func TestParseSchemaFile_CharsetGate(t *testing.T) {
	cases := []struct {
		name   string
		src    string
		wantOK bool
	}{
		{
			"table-latin1-refused",
			"CREATE TABLE `t` (`s` varchar(10)) DEFAULT CHARSET=latin1;", false,
		},
		{
			"column-latin1-refused",
			"CREATE TABLE `t` (`s` varchar(10) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4;", false,
		},
		{
			"column-utf8mb4-overrides-latin1-table",
			"CREATE TABLE `t` (`s` varchar(10) CHARACTER SET utf8mb4) DEFAULT CHARSET=latin1;", true,
		},
		{
			"latin1-table-with-no-string-columns-ok",
			"CREATE TABLE `t` (`n` bigint) DEFAULT CHARSET=latin1;", true,
		},
		{
			"utf8mb3-ok",
			"CREATE TABLE `t` (`s` text) DEFAULT CHARSET=utf8mb3;", true,
		},
		{
			"ascii-ok",
			"CREATE TABLE `t` (`s` char(4)) DEFAULT CHARSET=ascii;", true,
		},
		{
			"no-charset-assumes-utf8mb4",
			"CREATE TABLE `t` (`s` varchar(10));", true,
		},
		{
			"enum-under-latin1-refused",
			"CREATE TABLE `t` (`e` enum('a','b')) DEFAULT CHARSET=latin1;", false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSchemaFile(tc.src, "db.t-schema.sql")
			if tc.wantOK && err != nil {
				t.Fatalf("want ok; got %v", err)
			}
			if !tc.wantOK {
				if err == nil || !strings.Contains(err.Error(), "charset") {
					t.Fatalf("want a charset refusal; got %v", err)
				}
			}
		})
	}
}

func TestParseSchemaFile_NotEnforcedCheckDropped(t *testing.T) {
	src := "CREATE TABLE `t` (`n` int, CONSTRAINT `c` CHECK ((`n` > 0)) NOT ENFORCED);"
	table, err := parseSchemaFile(src, "db.t-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if len(table.CheckConstraints) != 0 {
		t.Fatalf("checks = %+v; want a NOT ENFORCED check dropped", table.CheckConstraints)
	}
}

func TestParseSchemaFile_FunctionalIndex(t *testing.T) {
	src := "CREATE TABLE `t` (`doc` json, KEY `fx` ((json_unquote(json_extract(`doc`, _utf8mb4'$.a')))));"
	table, err := parseSchemaFile(src, "db.t-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if len(table.Indexes) != 1 || len(table.Indexes[0].Columns) != 1 {
		t.Fatalf("indexes = %+v", table.Indexes)
	}
	entry := table.Indexes[0].Columns[0]
	if entry.Column != "" || entry.ExpressionDialect != "mysql" ||
		!strings.Contains(entry.Expression, "json_unquote") || strings.Contains(entry.Expression, "`") {
		t.Fatalf("functional entry = %+v", entry)
	}
}

// TestParseSchemaFile_PartitionClauseSkipped pins that the versioned
// partition comment SHOW CREATE appends is tolerated (partitioning is a
// physical layout the IR does not model — same posture as the live
// reader).
func TestParseSchemaFile_PartitionClauseSkipped(t *testing.T) {
	src := "CREATE TABLE `t` (`id` bigint NOT NULL, PRIMARY KEY (`id`))\n" +
		"/*!50100 PARTITION BY HASH (`id`) PARTITIONS 4 */;"
	table, err := parseSchemaFile(src, "db.t-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if table.PrimaryKey == nil {
		t.Fatal("primary key lost")
	}
}

func TestParseSchemaFile_InlineColumnShapes(t *testing.T) {
	src := "CREATE TABLE `t` (" +
		"`id` bigint NOT NULL PRIMARY KEY, " +
		"`u` varchar(10) UNIQUE, " +
		"`b` boolean DEFAULT '0', " +
		"`neg` int DEFAULT -3, " +
		"`v` int AS (`id` + 1) VIRTUAL" +
		");"
	table, err := parseSchemaFile(src, "db.t-schema.sql")
	if err != nil {
		t.Fatalf("parseSchemaFile: %v", err)
	}
	if table.PrimaryKey == nil || table.PrimaryKey.Columns[0].Column != "id" {
		t.Fatalf("pk = %+v", table.PrimaryKey)
	}
	if len(table.Indexes) != 1 || !table.Indexes[0].Unique || table.Indexes[0].Name != "u" {
		t.Fatalf("indexes = %+v", table.Indexes)
	}
	byName := map[string]*ir.Column{}
	for _, c := range table.Columns {
		byName[c.Name] = c
	}
	if _, ok := byName["b"].Type.(ir.Boolean); !ok {
		t.Fatalf("boolean sugar type = %#v", byName["b"].Type)
	}
	if d, ok := byName["neg"].Default.(ir.DefaultLiteral); !ok || d.Value != "-3" {
		t.Fatalf("neg default = %#v", byName["neg"].Default)
	}
	if v := byName["v"]; v.GeneratedExpr != "id + 1" || v.GeneratedStored {
		t.Fatalf("virtual generated = %+v", v)
	}
}

// TestParseSchemaFile_StatementSplitEdges runs a schema file through tiny
// stream blocks to pin that strings/comments straddling block boundaries
// re-assemble exactly (the carry re-scan contract).
func TestParseSchemaFile_StatementSplitEdges(t *testing.T) {
	content := realSchemaFile
	for _, blockSize := range []int{1, 3, 7, 64} {
		stream := newStatementStream(strings.NewReader(content), blockSize)
		var got []string
		for {
			stmt, err := stream.Next()
			if err != nil {
				break
			}
			got = append(got, stmt)
		}
		want, carry := splitMySQLChunk(content)
		if strings.TrimSpace(carry) != "" {
			want = append(want, strings.TrimSpace(carry))
		}
		if len(got) != len(want) {
			t.Fatalf("block %d: statements = %d; want %d", blockSize, len(got), len(want))
		}
		for i := range got {
			if strings.TrimSpace(got[i]) != strings.TrimSpace(want[i]) {
				t.Fatalf("block %d: statement %d diverged:\n%q\nvs\n%q", blockSize, i, got[i], want[i])
			}
		}
	}
}

func TestSplitMySQLChunk_SyntaxStates(t *testing.T) {
	script := "INSERT INTO `a;b` VALUES ('x;y\\';', \"z;\");\n" +
		"-- comment; with semicolon\n" +
		"# hash; comment\n" +
		"/* block; comment */ SET NAMES binary;\n" +
		"INSERT INTO t VALUES (1)"
	stmts, carry := splitMySQLChunk(script)
	if len(stmts) != 2 {
		t.Fatalf("stmts = %d (%q); want 2", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0], "a;b") || !strings.Contains(stmts[0], "x;y\\';") {
		t.Fatalf("stmt0 mis-split: %q", stmts[0])
	}
	if statementKeyword(stmts[1]) != "SET" {
		t.Fatalf("stmt1 = %q", stmts[1])
	}
	if strings.TrimSpace(carry) != "INSERT INTO t VALUES (1)" {
		t.Fatalf("carry = %q", carry)
	}
}

func TestCheckSetStatement(t *testing.T) {
	cases := []struct {
		stmt    string
		wantOK  bool
		wantSaw bool // a recognised (UTC) TIME_ZONE header
	}{
		{"/*!40101 SET NAMES binary*/", true, false},
		{"SET NAMES utf8mb4", true, false},
		{"SET NAMES utf8", true, false},
		{"SET NAMES latin1", false, false},
		{"SET NAMES cp1251", false, false},
		{"/*!40103 SET TIME_ZONE='+00:00' */", true, true},
		{"SET TIME_ZONE='+08:00'", false, false},
		{"SET FOREIGN_KEY_CHECKS=0", true, false},
		{"SET SQL_MODE=''", true, false},
		// Every TIME_ZONE spelling MySQL accepts must hit the same gate —
		// a non-UTC header must not slip past under a qualified form.
		{"SET SESSION TIME_ZONE='+08:00'", false, false},
		{"SET GLOBAL TIME_ZONE='+08:00'", false, false},
		{"SET @@time_zone='+08:00'", false, false},
		{"SET @@session.time_zone='+08:00'", false, false},
		{"SET @@SESSION.TIME_ZONE = '-05:00'", false, false},
		{"SET SESSION TIME_ZONE='+00:00'", true, true},
		{"SET @@session.time_zone = 'UTC'", true, true},
		{"SET @@time_zone='+00:00'", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.stmt, func(t *testing.T) {
			saw, err := checkSetStatement(tc.stmt)
			if tc.wantOK && err != nil {
				t.Fatalf("want ok; got %v", err)
			}
			if saw != tc.wantSaw {
				t.Fatalf("sawTimeZone = %v; want %v", saw, tc.wantSaw)
			}
			if !tc.wantOK && err == nil {
				t.Fatal("want a refusal; got nil")
			}
		})
	}
}

// TestParseCreateTable_EnumEscapeShapes pins that both MySQL label escape
// disciplines (backslash and doubled-quote) decode to the real bytes —
// the same contract the live reader's parseEnumOrSet carries.
func TestParseCreateTable_EnumEscapeShapes(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want []string
	}{
		{`enum('a','b')`, []string{"a", "b"}},
		{`enum('it''s','x')`, []string{"it's", "x"}},
		{`enum('a\'b','c,d')`, []string{"a'b", "c,d"}},
	} {
		src := fmt.Sprintf("CREATE TABLE `t` (`e` %s);", tc.src)
		table, err := parseSchemaFile(src, "db.t-schema.sql")
		if err != nil {
			t.Fatalf("%s: %v", tc.src, err)
		}
		e, ok := table.Columns[0].Type.(ir.Enum)
		if !ok || !reflect.DeepEqual(e.Values, tc.want) {
			t.Fatalf("%s → %#v; want %v", tc.src, table.Columns[0].Type, tc.want)
		}
	}
}
