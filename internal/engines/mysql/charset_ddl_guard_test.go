// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// serverNamerStub is a namer that knows one MariaDB-only collation the
// Vitess environment does not, the way [CDCReader.guardCollationNamer] does
// from the server's own table.
func serverNamerStub() collationNamer {
	n := vitessCollationNamer()
	base := n.charsetOf
	n.charsetOf = func(coll string) string {
		if coll == "latin1_swedish_nopad_ci" {
			return "latin1"
		}
		return base(coll)
	}
	return n
}

// TestClassifyCharsetDDL pins which statements the charset-DDL replay guard
// classifies — by the SQL parser in strict mode, never a pattern — which it
// declines, and which it hands to the fail-safe.
func TestClassifyCharsetDDL(t *testing.T) {
	type want struct {
		verdict       charsetDDLVerdict
		schema, table string
		all           *charsetSpec
		cols          map[string]charsetSpec
	}
	latin1 := charsetSpec{charset: "latin1"}
	for _, tc := range []struct {
		describe string
		stmt     string
		namer    collationNamer
		want     want
	}{
		{
			"CONVERT TO", "ALTER TABLE t CONVERT TO CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", &latin1, map[string]charsetSpec{}},
		},
		{
			"quoted CONVERT TO with a quoted collation", "ALTER TABLE t CONVERT TO CHARACTER SET 'latin1' COLLATE 'latin1_bin'", vitessCollationNamer(),
			want{ddlCharset, "", "t", &charsetSpec{"latin1", "latin1_bin"}, map[string]charsetSpec{}},
		},
		{
			"MODIFY with a quoted charset", "ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET 'cp1251' NULL", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": {charset: "cp1251"}}},
		},
		{
			"MODIFY with a double-quoted charset", `ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET "cp1251"`, vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": {charset: "cp1251"}}},
		},
		{
			"collation-only MODIFY", "ALTER TABLE t MODIFY v VARCHAR(16) COLLATE latin1_bin", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": {"latin1", "latin1_bin"}}},
		},
		{
			"CHANGE names the new column", "ALTER TABLE t CHANGE a B TEXT CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"b": latin1}},
		},
		{
			"qualified, backticked", "ALTER TABLE `d`.`t` MODIFY v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "d", "t", nil, map[string]charsetSpec{"v": latin1}},
		},

		// MariaDB prefix syntax the parser refuses or truncates at (MEASURED).
		{
			"ALTER TABLE IF EXISTS", "ALTER TABLE IF EXISTS t MODIFY v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": latin1}},
		},
		{
			"ALTER ONLINE TABLE", "ALTER ONLINE TABLE t MODIFY v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": latin1}},
		},
		{
			"ALTER IGNORE TABLE", "alter ignore table t modify v varchar(8) character set latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": latin1}},
		},
		{
			"WAIT n", "ALTER TABLE t WAIT 5 MODIFY v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": latin1}},
		},
		{
			"NOWAIT", "ALTER TABLE t NOWAIT MODIFY v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": latin1}},
		},
		{
			"all of them, after a comment", "/* x */ ALTER ONLINE IGNORE TABLE IF EXISTS d.t NOWAIT CONVERT TO CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlCharset, "d", "t", &latin1, map[string]charsetSpec{}},
		},

		// MariaDB-only collation names: named by the server's table.
		{
			"MariaDB-only collation, server-named", "ALTER TABLE t MODIFY v VARCHAR(8) COLLATE latin1_swedish_nopad_ci", serverNamerStub(),
			want{ddlCharset, "", "t", nil, map[string]charsetSpec{"v": {"latin1", "latin1_swedish_nopad_ci"}}},
		},
		{
			"MariaDB-only collation, nothing names it", "ALTER TABLE t MODIFY v VARCHAR(8) COLLATE latin1_swedish_nopad_ci", vitessCollationNamer(),
			want{ddlUnclassified, "", "t", nil, nil},
		},
		{
			"unparseable, mentions a charset", "ALTER TABLE t MODIFY COLUMN IF EXISTS v VARCHAR(8) CHARACTER SET latin1", vitessCollationNamer(),
			want{ddlUnclassified, "", "t", nil, nil},
		},

		// Not classified: UTF-8 targets, no charset, not an ALTER.
		{"CONVERT TO utf8mb4 with a collation", "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", vitessCollationNamer(), want{}},
		{"restated utf8mb4 MODIFY", "ALTER TABLE t MODIFY v VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", vitessCollationNamer(), want{}},
		{"utf8 folds to utf8mb3, a UTF-8 target", "ALTER TABLE d.t CONVERT TO CHARACTER SET utf8", vitessCollationNamer(), want{}},
		{"utf8mb4 collation-only MODIFY", "ALTER TABLE t MODIFY v VARCHAR(8) COLLATE utf8mb4_bin", vitessCollationNamer(), want{}},
		{"no explicit charset", "ALTER TABLE t MODIFY v VARCHAR(40)", vitessCollationNamer(), want{}},
		{"no charset change", "ALTER TABLE t ADD COLUMN c INT", vitessCollationNamer(), want{}},
		{"a table default changes no stored bytes", "ALTER TABLE t DEFAULT CHARSET = latin1", vitessCollationNamer(), want{}},
		{"unparseable, no charset keyword", "ALTER TABLE t MODIFY COLUMN IF EXISTS v INT", vitessCollationNamer(), want{}},
		{"not an ALTER", "CREATE TABLE t (id INT)", vitessCollationNamer(), want{}},
		{"not SQL", "this is not sql", vitessCollationNamer(), want{}},
	} {
		c, verdict := classifyCharsetDDL(tc.stmt, tc.namer)
		if verdict != tc.want.verdict {
			t.Errorf("%s: verdict = %d; want %d (%q)", tc.describe, verdict, tc.want.verdict, tc.stmt)
			continue
		}
		if verdict == ddlNotCharset {
			continue
		}
		if c.schema != tc.want.schema || c.table != tc.want.table {
			t.Errorf("%s: table = %q.%q; want %q.%q", tc.describe, c.schema, c.table, tc.want.schema, tc.want.table)
		}
		if verdict == ddlUnclassified {
			continue
		}
		if !reflect.DeepEqual(c.all, tc.want.all) || !reflect.DeepEqual(c.cols, tc.want.cols) {
			t.Errorf("%s: got all=%v cols=%v; want %v %v", tc.describe, c.all, c.cols, tc.want.all, tc.want.cols)
		}
	}
}

// TestCharsetDDLChange_AlreadyApplied pins the comparison: charset AND
// collation. A shape that still shows the OLD charset or collation (a live
// stream) passes; one that already shows the NEW one for every named column
// (a replay) is caught.
func TestCharsetDDLChange_AlreadyApplied(t *testing.T) {
	n := vitessCollationNamer()
	classify := func(stmt string) charsetDDLChange {
		c, v := classifyCharsetDDL(stmt, n)
		if v != ddlCharset {
			t.Fatalf("%q did not classify", stmt)
		}
		return c
	}
	swedish := charsetSpec{"latin1", "latin1_swedish_ci"}
	bin := charsetSpec{"latin1", "latin1_bin"}
	utf8 := charsetSpec{"utf8mb4", "utf8mb4_0900_ai_ci"}

	modify := classify("ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1")
	if ok, _ := modify.alreadyApplied(map[string]charsetSpec{"v": utf8}, n); ok {
		t.Error("live stream (shape still utf8mb4) flagged as a replay")
	}
	if ok, cols := modify.alreadyApplied(map[string]charsetSpec{"v": swedish, "w": utf8}, n); !ok || !reflect.DeepEqual(cols, []string{"v"}) {
		t.Errorf("replay (shape already latin1 at its default collation) = %v %v; want caught on v", ok, cols)
	}
	if ok, _ := modify.alreadyApplied(map[string]charsetSpec{"v": bin}, n); ok {
		t.Error("a latin1_bin shape matched a MODIFY to latin1's DEFAULT collation")
	}

	collOnly := classify("ALTER TABLE t MODIFY v VARCHAR(16) COLLATE latin1_bin")
	if ok, _ := collOnly.alreadyApplied(map[string]charsetSpec{"v": swedish}, n); ok {
		t.Error("live collation-only change (shape still latin1_swedish_ci) flagged as a replay")
	}
	if ok, _ := collOnly.alreadyApplied(map[string]charsetSpec{"v": bin}, n); !ok {
		t.Error("replayed collation-only change (shape already latin1_bin) not caught")
	}

	convert := classify("ALTER TABLE t CONVERT TO CHARACTER SET latin1")
	if ok, _ := convert.alreadyApplied(map[string]charsetSpec{"v": swedish, "w": utf8}, n); ok {
		t.Error("CONVERT TO with a column still utf8mb4 flagged as a replay")
	}
	if ok, _ := convert.alreadyApplied(map[string]charsetSpec{"v": swedish, "w": bin}, n); ok {
		t.Error("CONVERT TO latin1 (default collation) matched a latin1_bin column")
	}
	if ok, _ := convert.alreadyApplied(map[string]charsetSpec{"v": swedish, "w": swedish}, n); !ok {
		t.Error("CONVERT TO with every column already latin1_swedish_ci not caught")
	}
	if ok, _ := convert.alreadyApplied(map[string]charsetSpec{}, n); ok {
		t.Error("a table with no character columns flagged")
	}
	if ok, _ := convert.alreadyApplied(map[string]charsetSpec{"v": {charset: "latin1"}}, n); !ok {
		t.Error("a shape with no recorded collation must be compared by charset alone")
	}

	change := classify("ALTER TABLE t CHANGE a b VARCHAR(8) CHARACTER SET latin1")
	if ok, _ := change.alreadyApplied(map[string]charsetSpec{"a": utf8}, n); ok {
		t.Error("live CHANGE (shape still has the old name) flagged as a replay")
	}
}

// TestCollationCharsets_RetriesAFailedLoad pins review item 5: a failed
// load of the source's collation table (here: no DB, so it returns nil) is
// retried once the interval has passed, and not on every rows event.
func TestCollationCharsets_RetriesAFailedLoad(t *testing.T) {
	r := &CDCReader{}
	r.collationCharsets(context.Background())
	if r.serverCollationsTried.IsZero() {
		t.Fatal("the first call did not attempt the load")
	}
	// A sentinel inside the interval (not the first call's own stamp, which a
	// coarse wall clock could reproduce on an immediate retry).
	recent := time.Now().Add(-time.Second)
	r.serverCollationsTried = recent
	r.collationCharsets(context.Background())
	if !r.serverCollationsTried.Equal(recent) {
		t.Error("a second call inside the interval retried the load; want at most one query per interval")
	}
	r.serverCollationsTried = time.Now().Add(-serverCollationsRetry - time.Second)
	stale := r.serverCollationsTried
	r.collationCharsets(context.Background())
	if r.serverCollationsTried.Equal(stale) {
		t.Error("a failed load was not retried after the interval; it is cached for the reader's life")
	}
}

// TestVStreamCharsetDDLGuard: the VStream twin reads the FIELD cache,
// charset AND collation (the field's collation ID).
func TestVStreamCharsetDDLGuard(t *testing.T) {
	field := func(name string, coll uint32) *query.Field {
		return &query.Field{Name: name, Type: query.Type_VARCHAR, Charset: coll}
	}
	latin1Fields := map[string][]*query.Field{"0/ks.t": {{Name: "id", Type: query.Type_INT32}, field("v", 8)}} // latin1_swedish_ci
	utf8Fields := map[string][]*query.Field{"0/ks.t": {field("v", 255)}}                                       // utf8mb4_0900_ai_ci
	refuses := func(what, stmt string, fields map[string][]*query.Field) {
		t.Helper()
		var ce *sluicecode.CodedError
		if err := vstreamCharsetDDLGuard(stmt, "ks", fields); !errors.As(err, &ce) || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
			t.Errorf("%s: err = %v; want %s", what, err, sluicecode.CodeCDCSchemaReplayMismatch)
		}
	}
	passes := func(what, stmt string, fields map[string][]*query.Field) {
		t.Helper()
		if err := vstreamCharsetDDLGuard(stmt, "ks", fields); err != nil {
			t.Errorf("%s: err = %v; want nil", what, err)
		}
	}
	refuses("replayed MODIFY to latin1", "ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1", latin1Fields)
	passes("live MODIFY to cp1251", "ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET cp1251", latin1Fields)
	passes("live collation-only MODIFY on latin1", "ALTER TABLE t MODIFY v VARCHAR(16) COLLATE latin1_bin", latin1Fields)
	passes("CONVERT TO utf8mb4 COLLATE utf8mb4_unicode_ci", "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", utf8Fields)
	passes("restated utf8mb4 MODIFY", "ALTER TABLE t MODIFY v VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", utf8Fields)
	passes("a DDL on another table", "ALTER TABLE other MODIFY v VARCHAR(16) CHARACTER SET latin1", latin1Fields)
	refuses("unclassifiable charset ALTER on a latin1 table", "ALTER TABLE t MODIFY COLUMN IF EXISTS v VARCHAR(8) CHARACTER SET cp1251", latin1Fields)
	passes("unclassifiable charset ALTER on a UTF-8-only table", "ALTER TABLE t MODIFY COLUMN IF EXISTS v VARCHAR(8) CHARACTER SET cp1251", utf8Fields)

	err := vstreamCharsetDDLGuard("ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1", "ks", latin1Fields)
	if err == nil || !strings.Contains(err.Error(), "take a fresh full backup") || !strings.Contains(err.Error(), "--restart-from-scratch") {
		t.Errorf("the refusal must name the re-copy for both sync and a backup chain: %v", err)
	}
}

// TestCharsetShapeLedger: the engine side of the capture lanes' check (item
// 2) — a noted shape is reported until the session crosses an ALTER naming
// the table (in any spelling the prefix walk reads), and another table's
// ALTER does not hide it.
func TestCharsetShapeLedger(t *testing.T) {
	var l charsetShapeLedger
	l.note("CR", map[string]charsetSpec{"v": {"latin1", "latin1_swedish_ci"}})
	l.note("other", map[string]charsetSpec{"w": {"cp1251", ""}})
	if got := l.snapshot()["cr"]["v"]; got != [2]string{"latin1", "latin1_swedish_ci"} {
		t.Fatalf("snapshot cr.v = %v; want latin1/latin1_swedish_ci (table names lower-cased)", got)
	}
	l.crossedAlter("ALTER TABLE other ADD COLUMN z INT")
	if _, ok := l.snapshot()["cr"]; !ok {
		t.Error("an ALTER on another table hid cr")
	}
	l.crossedAlter("ALTER ONLINE TABLE IF EXISTS `d`.`Cr` NOWAIT MODIFY v VARCHAR(16)")
	snap := l.snapshot()
	if _, ok := snap["cr"]; ok {
		t.Error("cr is still reported after the session crossed an ALTER on it")
	}
	if _, ok := snap["other"]; ok {
		t.Error("other is still reported after the session crossed an ALTER on it")
	}
}
