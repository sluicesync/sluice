// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestParseCharsetDDL pins which statements the charset-DDL replay guard
// classifies — by the SQL parser, never a pattern — and which it declines.
func TestParseCharsetDDL(t *testing.T) {
	for _, tc := range []struct {
		stmt     string
		ok       bool
		table    string
		all      string
		cols     map[string]string
		describe string
	}{
		{"ALTER TABLE t CONVERT TO CHARACTER SET latin1", true, "t", "latin1", map[string]string{}, "CONVERT TO"},
		{"alter table d.t convert to character set utf8", true, "t", "utf8mb3", map[string]string{}, "utf8 folds to utf8mb3"},
		{"ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET cp1251 NULL", true, "t", "", map[string]string{"v": "cp1251"}, "MODIFY with a charset"},
		{"ALTER TABLE t MODIFY v VARCHAR(16) COLLATE latin1_swedish_ci", true, "t", "", map[string]string{"v": "latin1"}, "MODIFY with a collation"},
		{"ALTER TABLE t CHANGE a B TEXT CHARACTER SET utf8mb4", true, "t", "", map[string]string{"b": "utf8mb4"}, "CHANGE names the new column"},
		{"ALTER TABLE t MODIFY v VARCHAR(40)", false, "", "", nil, "no explicit charset: not classified"},
		{"ALTER TABLE t ADD COLUMN c INT", false, "", "", nil, "no charset change"},
		{"ALTER TABLE t DEFAULT CHARSET = latin1", false, "", "", nil, "a table default changes no stored bytes"},
		{"CREATE TABLE t (id INT)", false, "", "", nil, "not an ALTER"},
		{"this is not sql", false, "", "", nil, "unparseable"},
	} {
		c, ok := parseCharsetDDL(tc.stmt)
		if ok != tc.ok {
			t.Errorf("%s: ok = %v; want %v (%q)", tc.describe, ok, tc.ok, tc.stmt)
			continue
		}
		if !ok {
			continue
		}
		if c.table != tc.table || c.all != tc.all || !reflect.DeepEqual(c.cols, tc.cols) {
			t.Errorf("%s: got table=%q all=%q cols=%v; want %q %q %v", tc.describe, c.table, c.all, c.cols, tc.table, tc.all, tc.cols)
		}
	}
}

// TestCharsetDDLChange_AlreadyApplied pins the comparison: a shape that
// still shows the OLD charset (a live stream) passes; one that already shows
// the NEW charset for every named column (a replay) is caught.
func TestCharsetDDLChange_AlreadyApplied(t *testing.T) {
	modify, _ := parseCharsetDDL("ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1")
	if ok, _ := modify.alreadyApplied(map[string]string{"v": "utf8mb4"}); ok {
		t.Error("live stream (shape still utf8mb4) flagged as a replay")
	}
	if ok, cols := modify.alreadyApplied(map[string]string{"v": "latin1", "w": "utf8mb4"}); !ok || !reflect.DeepEqual(cols, []string{"v"}) {
		t.Errorf("replay (shape already latin1) = %v %v; want caught on v", ok, cols)
	}
	convert, _ := parseCharsetDDL("ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4")
	if ok, _ := convert.alreadyApplied(map[string]string{"v": "latin1", "w": "utf8mb4"}); ok {
		t.Error("CONVERT TO with a column still latin1 flagged as a replay")
	}
	if ok, _ := convert.alreadyApplied(map[string]string{"v": "utf8mb4", "w": "utf8mb4"}); !ok {
		t.Error("CONVERT TO with every column already utf8mb4 not caught")
	}
	if ok, _ := convert.alreadyApplied(map[string]string{}); ok {
		t.Error("a table with no character columns flagged")
	}
	change, _ := parseCharsetDDL("ALTER TABLE t CHANGE a b VARCHAR(8) CHARACTER SET latin1")
	if ok, _ := change.alreadyApplied(map[string]string{"a": "utf8mb4"}); ok {
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

// TestVStreamCharsetDDLGuard: the VStream twin reads the FIELD cache.
func TestVStreamCharsetDDLGuard(t *testing.T) {
	fields := map[string][]*query.Field{
		"0/ks.t": {{Name: "id", Type: query.Type_INT32}, {Name: "v", Type: query.Type_VARCHAR, Charset: 8}}, // latin1
	}
	var ce *sluicecode.CodedError
	if err := vstreamCharsetDDLGuard("ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET latin1", "ks", fields); !errors.As(err, &ce) ||
		ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("replayed DDL with the FIELD shape already latin1: err = %v; want %s", err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if err := vstreamCharsetDDLGuard("ALTER TABLE t MODIFY v VARCHAR(16) CHARACTER SET cp1251", "ks", fields); err != nil {
		t.Fatalf("live DDL (shape still latin1, DDL to cp1251): err = %v; want nil", err)
	}
	if err := vstreamCharsetDDLGuard("ALTER TABLE other MODIFY v VARCHAR(16) CHARACTER SET latin1", "ks", fields); err != nil {
		t.Fatalf("a DDL on another table: err = %v; want nil", err)
	}
}
