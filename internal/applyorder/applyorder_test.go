// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applyorder

import "testing"

func TestClassify(t *testing.T) {
	for sql, want := range map[string]Kind{
		"INSERT INTO `t` (a) VALUES (?)":                        Data,
		"  update \"public\".\"t\" SET a = $1":                  Data,
		"DELETE FROM t WHERE id IN (?, ?)":                      Data,
		"REPLACE INTO t VALUES (1)":                             Data,
		"TRUNCATE TABLE t":                                      Data,
		"INSERT INTO `sluice_cdc_state` (stream_id) VALUES (?)": Control,
		"DELETE FROM sluice_cdc_apply_marks WHERE tx_id = ?":    Control,
		"SELECT * FROM ctl.sluice_cdc_state FOR UPDATE":         Control,
		"SET LOCAL synchronous_commit = on":                     Other,
		"SELECT column_name FROM information_schema.columns":    Other,
	} {
		if got := Classify(sql); got != want {
			t.Errorf("Classify(%q) = %d; want %d", sql, got, want)
		}
	}
}

// TestViolation pins both directions of the rule the write-core rosters
// grade: data before control passes, and any data after the first control
// statement is named.
func TestViolation(t *testing.T) {
	ok := Tx{Stmts: []string{"SET x", "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)", "INSERT INTO sluice_cdc_apply_marks VALUES (1)", "INSERT INTO sluice_cdc_state VALUES (1)"}}
	if v := ok.Violation(); v != "" || !ok.Mixed() {
		t.Fatalf("data then control: violation %q, mixed %v; want none, true", v, ok.Mixed())
	}
	bad := Tx{Stmts: []string{"INSERT INTO t VALUES (1)", "INSERT INTO sluice_cdc_apply_marks VALUES (1)", "INSERT INTO t VALUES (2)"}}
	if bad.Violation() == "" {
		t.Fatal("a row after the marks was not reported")
	}
	controlOnly := Tx{Stmts: []string{"INSERT INTO sluice_cdc_state VALUES (1)"}}
	if controlOnly.Violation() != "" || controlOnly.Mixed() {
		t.Fatal("a control-only transaction is neither a violation nor mixed")
	}
}

func TestRecorder(t *testing.T) {
	var r Recorder
	a, b := new(int), new(int)
	r.Statement(a, "INSERT INTO t VALUES (0)") // outside a transaction: ignored
	r.Begin(a)
	r.Begin(b)
	r.Statement(a, "INSERT INTO t VALUES (1)")
	r.Statement(b, "INSERT INTO sluice_cdc_state VALUES (1)")
	r.Statement(a, "INSERT INTO sluice_cdc_state VALUES (1)")
	r.End(b, false)
	r.End(a, true)
	got := r.Take()
	if len(got) != 2 || got[0].Committed || !got[1].Committed || len(got[1].Stmts) != 2 {
		t.Fatalf("Take = %+v; want b rolled back then a committed with its two statements", got)
	}
	if len(r.Take()) != 0 {
		t.Fatal("Take did not reset")
	}
}
