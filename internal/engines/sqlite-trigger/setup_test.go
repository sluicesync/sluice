// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// sampleTable is a small IR table the render tests build trigger SQL from.
func sampleTable() *ir.Table {
	return &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{}},
			{Name: "payload", Type: ir.Blob{}},
			{Name: "gen", Type: ir.Integer{}, GeneratedExpr: "id + 1"},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

// TestRenderTableTriggers_FaithfulCaptureBody pins the §crux load-bearing
// decision: the capture trigger encodes EACH column as a (typeof, text/hex) pair
// via the SHARED faithful expression — NEVER a bare json_object on the raw column
// (which would round integers > 2^53 and drop blobs). It asserts the three
// per-op triggers, the op codes, the before/after wiring, the typeof+CASE/hex/
// format encoding, and that the generated column is EXCLUDED.
func TestRenderTableTriggers_FaithfulCaptureBody(t *testing.T) {
	stmts := renderTableTriggers(sampleTable())
	// 1 captured-column fingerprint upsert + 3 ops × (DROP + CREATE) = 7.
	if len(stmts) != 7 {
		t.Fatalf("got %d statements; want 7 (fingerprint upsert + DROP+CREATE per INSERT/UPDATE/DELETE)", len(stmts))
	}
	all := strings.Join(stmts, "\n")

	for _, want := range []string{
		// Captured-column fingerprint records the NON-generated set (id, payload)
		// — NOT the generated "gen" column — for the startup drift check.
		`INSERT INTO "sluice_change_log_columns" (tbl, columns) VALUES ('events', '["id","payload"]')`,
		`CREATE TRIGGER "sluice_capture_events_ins" AFTER INSERT ON "events"`,
		`CREATE TRIGGER "sluice_capture_events_upd" AFTER UPDATE ON "events"`,
		`CREATE TRIGGER "sluice_capture_events_del" AFTER DELETE ON "events"`,
		`DROP TRIGGER IF EXISTS "sluice_capture_events_ins"`,
		// Faithful encoding for the id column (INSERT after-image, NEW row).
		`json_object('t', typeof(NEW."id"), 'v', CASE typeof(NEW."id") WHEN 'blob' THEN hex(NEW."id") WHEN 'real' THEN format('%.17g', NEW."id") ELSE CAST(NEW."id" AS TEXT) END)`,
		// Faithful encoding for the payload (blob) column on the OLD row (DELETE).
		`json_object('t', typeof(OLD."payload"), 'v', CASE typeof(OLD."payload") WHEN 'blob' THEN hex(OLD."payload")`,
		// Op codes.
		`VALUES ('I', 'events', NULL,`,
		`VALUES ('D', 'events',`,
		`VALUES ('U', 'events',`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("rendered trigger SQL missing expected fragment:\n  %s\n--- full ---\n%s", want, all)
		}
	}

	// The cardinal sin: a bare json_object over the raw NEW.* / OLD.* row would
	// serialize integers as JSON doubles (rounding > 2^53) and drop blobs. Assert
	// the body never does that.
	if strings.Contains(all, "json_object(NEW)") || strings.Contains(all, "to_json(NEW)") {
		t.Error("capture body uses a lossy whole-row json serialization — must encode each column as a (typeof, text/hex) pair")
	}
	// The generated column must NOT appear in the captured image (the target
	// re-derives it, exactly as the cold-start reader omits it).
	if strings.Contains(all, `"gen"`) {
		t.Error("capture body includes the generated column \"gen\"; it must be excluded (target re-derives it)")
	}
}

// TestRenderSetupDDL_ChangeLogShape pins the change-log + meta DDL: the
// monotonic AUTOINCREMENT watermark id, the captured-at default, and the
// schema-version meta row.
func TestRenderSetupDDL_ChangeLogShape(t *testing.T) {
	stmts := renderSetupDDL([]*ir.Table{sampleTable()})
	all := strings.Join(stmts, "\n")
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "sluice_change_log"`,
		"id           INTEGER PRIMARY KEY AUTOINCREMENT",
		`CREATE TABLE IF NOT EXISTS "sluice_change_log_meta"`,
		`INSERT INTO "sluice_change_log_meta" (singleton_pk, schema_version) VALUES (1, 1)`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("setup DDL missing fragment:\n  %s", want)
		}
	}
}

// TestFilterInternal drops the engine's own bookkeeping tables.
func TestFilterInternal(t *testing.T) {
	got := filterInternal([]string{"users", ChangeLogTable, "posts", ChangeLogMetaTable})
	want := []string{"users", "posts"}
	if len(got) != len(want) {
		t.Fatalf("filterInternal = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filterInternal = %v; want %v", got, want)
		}
	}
}

// TestTriggersForTables renders the deterministic per-table trigger name set.
func TestTriggersForTables(t *testing.T) {
	got := triggersForTables([]string{"users"})
	want := map[string]bool{
		"sluice_capture_users_ins": true,
		"sluice_capture_users_upd": true,
		"sluice_capture_users_del": true,
	}
	if len(got) != 3 {
		t.Fatalf("got %v; want 3 trigger names", got)
	}
	for _, n := range got {
		if !want[n] {
			t.Errorf("unexpected trigger name %q", n)
		}
	}
}

// TestPositionRoundTrip pins encode/decode of the watermark position, the
// "from now" sentinel, the cross-engine rejection, and the last_id>=0 invariant.
func TestPositionRoundTrip(t *testing.T) {
	pos, err := encodePos(sqliteTriggerPos{LastID: 42})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	if pos.Engine != EngineName {
		t.Errorf("pos.Engine = %q; want %q", pos.Engine, EngineName)
	}
	got, ok, err := decodePos(pos)
	if err != nil || !ok {
		t.Fatalf("decodePos: ok=%v err=%v", ok, err)
	}
	if got.LastID != 42 {
		t.Errorf("LastID = %d; want 42", got.LastID)
	}

	// Zero position → "from now" sentinel (ok=false, no error).
	if _, ok, err := decodePos(ir.Position{}); ok || err != nil {
		t.Errorf("zero position: ok=%v err=%v; want ok=false err=nil", ok, err)
	}
	// Foreign engine → rejected.
	if _, _, err := decodePos(ir.Position{Engine: "postgres-trigger", Token: `{"last_id":1}`}); err == nil {
		t.Error("foreign-engine position should be rejected")
	}
	// Negative last_id → rejected (the watermark invariant).
	if _, _, err := decodePos(ir.Position{Engine: EngineName, Token: `{"last_id":-1}`}); err == nil {
		t.Error("negative last_id should be rejected")
	}
}
