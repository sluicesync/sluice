// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"
)

// TestEngine_Registered confirms the package's init() side-effect
// landed the engine under "postgres-trigger". The pipeline orchestrator
// resolves engines by name via engines.Get; a missing registration would
// be a silent breakage where the operator's --source-driver=postgres-trigger
// surfaces as "unknown engine" rather than the intended path.
func TestEngine_Registered(t *testing.T) {
	e, ok := engines.Get(EngineName)
	if !ok {
		t.Fatalf("engine %q not registered", EngineName)
	}
	if got := e.Name(); got != EngineName {
		t.Errorf("e.Name() = %q; want %q", got, EngineName)
	}
}

// TestEngine_Capabilities pins the §8 surface to the ADR's locked
// values. Any drift surfaces here as a failed assertion so the
// reviewer notices.
func TestEngine_Capabilities(t *testing.T) {
	c := Engine{}.Capabilities()

	if c.CDC != ir.CDCTriggers {
		t.Errorf("CDC = %v; want CDCTriggers", c.CDC)
	}
	if c.SupportsGeneratedColumns {
		t.Errorf("SupportsGeneratedColumns = true; want false (§14)")
	}
	if c.SchemaScope != ir.SchemaScopeNamespaced {
		t.Errorf("SchemaScope = %v; want SchemaScopeNamespaced", c.SchemaScope)
	}
	if c.JSONSupport != ir.JSONBoth {
		t.Errorf("JSONSupport = %v; want JSONBoth", c.JSONSupport)
	}
	if !c.SupportsCheckConstraint {
		t.Errorf("SupportsCheckConstraint = false; want true")
	}
}

// TestEngine_NoSlotManager asserts the engine does NOT expose
// [ir.SlotManagerOpener]. ADR-0066 §9: the trigger engine has no
// replication slots, so the optional surface must cleanly miss on
// type-assertion (the CLI's `sluice slot list` then reports a polished
// "engine does not support replication-slot management" rather than
// silently degrading).
func TestEngine_NoSlotManager(t *testing.T) {
	var e ir.Engine = Engine{}
	if _, ok := e.(ir.SlotManagerOpener); ok {
		t.Errorf("Engine satisfies ir.SlotManagerOpener; want NOT (§9 — no slots to manage)")
	}
}

// TestEngine_NoCDCReaderWithSlot is the symmetric pin for
// [ir.CDCReaderWithSlotOpener]. Same reason as TestEngine_NoSlotManager:
// the trigger engine doesn't have slots, so the `--slot-name` flag
// must silently route to the default OpenCDCReader instead of dialing
// a non-existent slot.
func TestEngine_NoCDCReaderWithSlot(t *testing.T) {
	var e ir.Engine = Engine{}
	if _, ok := e.(ir.CDCReaderWithSlotOpener); ok {
		t.Errorf("Engine satisfies ir.CDCReaderWithSlotOpener; want NOT (§9 — no slot to bind to)")
	}
}

// TestPosition_RoundTrip confirms encodePos / decodePos round-trips
// the LastID bookmark cleanly. The Engine tag is engine-specific so a
// vanilla-PG pgoutput position never decodes here (and vice versa).
func TestPosition_RoundTrip(t *testing.T) {
	pos, err := encodePos(pgTriggerPos{LastID: 1234567890})
	if err != nil {
		t.Fatalf("encodePos: %v", err)
	}
	if pos.Engine != EngineName {
		t.Errorf("pos.Engine = %q; want %q", pos.Engine, EngineName)
	}
	got, ok, err := decodePos(pos)
	if err != nil {
		t.Fatalf("decodePos: %v", err)
	}
	if !ok {
		t.Fatalf("decodePos: ok=false; want true on a valid token")
	}
	if got.LastID != 1234567890 {
		t.Errorf("got.LastID = %d; want 1234567890", got.LastID)
	}
}

// TestPosition_FromNow exercises the zero-value sentinel decode. The
// reader interprets this as "start at MAX(id) on the source"; the
// decoder must report ok=false without erroring.
func TestPosition_FromNow(t *testing.T) {
	_, ok, err := decodePos(ir.Position{})
	if err != nil {
		t.Errorf("decodePos(zero): unexpected err = %v", err)
	}
	if ok {
		t.Errorf("decodePos(zero): ok=true; want false")
	}
}

// TestPosition_WrongEngine rejects a position emitted by another
// engine. A misrouted token would otherwise advance the polling cursor
// to an unrelated source's bookmark — silent loss.
func TestPosition_WrongEngine(t *testing.T) {
	_, _, err := decodePos(ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/1"}`})
	if err == nil {
		t.Errorf("expected an engine-mismatch error; got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "engine") {
		t.Errorf("err = %v; want contains \"engine\"", err)
	}
}

// TestDecodeJSONBRow_NumericPrecision is the Bug-74-class pin: PG's
// unbounded `numeric` must round-trip through the JSONB decode path
// without precision loss. `Decoder.UseNumber()` keeps the value as a
// `json.Number`; the engine MUST not silently coerce it to float64.
func TestDecodeJSONBRow_NumericPrecision(t *testing.T) {
	// A 20-digit numeric value that doesn't fit in float64 (it
	// loses the trailing digits on conversion). PG emits `numeric`
	// columns as JSON numbers without quotes.
	const numLit = "12345678901234567890.1234567890"
	row, err := decodeJSONBRow(`{"n": ` + numLit + `}`)
	if err != nil {
		t.Fatalf("decodeJSONBRow: %v", err)
	}
	got, ok := row["n"]
	if !ok {
		t.Fatalf("row['n'] missing; row=%v", row)
	}
	// The decoder leaves non-integer numerics as json.Number.
	if _, isFloat := got.(float64); isFloat {
		t.Fatalf("got = float64(%v); want json.Number (UseNumber preserves precision)", got)
	}
	// Confirm the textual form round-trips byte-equivalently.
	s := ""
	switch v := got.(type) {
	case interface{ String() string }:
		s = v.String()
	default:
		t.Fatalf("got type %T = %v; want json.Number-shaped", got, got)
	}
	if s != numLit {
		t.Errorf("round-trip text = %q; want %q (UseNumber must preserve PG numeric precision)", s, numLit)
	}
}

// TestDecodeJSONBRow_NullValues — pg_attribute NULLs in the row image
// must survive the decode as Go nil (not "null" or a missing key).
func TestDecodeJSONBRow_NullValues(t *testing.T) {
	row, err := decodeJSONBRow(`{"id": 1, "deleted_at": null, "name": "alice"}`)
	if err != nil {
		t.Fatalf("decodeJSONBRow: %v", err)
	}
	if got, ok := row["deleted_at"]; !ok || got != nil {
		t.Errorf("deleted_at = %v (ok=%v); want nil, true", got, ok)
	}
	if got := row["name"]; got != "alice" {
		t.Errorf("name = %v; want alice", got)
	}
}

// TestDecodeJSONBRow_EmptyAndNull cover the two sentinel shapes
// the JSONB column can surface as TEXT: "" (empty / SQL NULL surfaced
// via NullString) and "null" (a JSON null literal in a non-NULL JSONB
// column).
func TestDecodeJSONBRow_EmptyAndNull(t *testing.T) {
	if row, err := decodeJSONBRow(""); err != nil || row != nil {
		t.Errorf("decodeJSONBRow(\"\") = (%v, %v); want (nil, nil)", row, err)
	}
	if row, err := decodeJSONBRow("null"); err != nil || row != nil {
		t.Errorf("decodeJSONBRow(\"null\") = (%v, %v); want (nil, nil)", row, err)
	}
}

// TestDecodeDDLTag pulls the command_tag from the §7 marker payload
// and falls back to "DDL" on a malformed payload.
func TestDecodeDDLTag(t *testing.T) {
	cases := []struct {
		payload string
		want    string
	}{
		{`{"command_tag":"ALTER TABLE","object_type":"table"}`, "ALTER TABLE"},
		{`{"command_tag":"CREATE INDEX"}`, "CREATE INDEX"},
		{`{}`, "DDL"},
		{``, "DDL"},
		{`not-json`, "DDL"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.payload, func(t *testing.T) {
			if got := decodeDDLTag(c.payload); got != c.want {
				t.Errorf("decodeDDLTag(%q) = %q; want %q", c.payload, got, c.want)
			}
		})
	}
}

// TestRenderSetupDDL_EventTriggerToggle pins the §7 hybrid: when the
// connecting role can create event triggers, the DDL block ends with
// the event-trigger CREATE; otherwise that block is absent and the
// polled-fingerprint loop's setup-time hook is the only DDL-detection
// surface.
func TestRenderSetupDDL_EventTriggerToggle(t *testing.T) {
	withET := renderSetupDDL("public", []string{"orders"}, true)
	withoutET := renderSetupDDL("public", []string{"orders"}, false)

	if !anyContains(withET, "CREATE EVENT TRIGGER") {
		t.Errorf("renderSetupDDL(canEventTrigger=true) missing CREATE EVENT TRIGGER")
	}
	if anyContains(withoutET, "CREATE EVENT TRIGGER") {
		t.Errorf("renderSetupDDL(canEventTrigger=false) emitted CREATE EVENT TRIGGER")
	}
	// Both shapes must install the row + truncate triggers on the
	// named table (the change-log table itself is always emitted).
	if !anyContains(withET, "CREATE TRIGGER \"sluice_capture\"") {
		t.Errorf("missing per-table CREATE TRIGGER in the event-trigger-supported plan")
	}
	if !anyContains(withET, "CREATE TABLE IF NOT EXISTS \"public\".\"sluice_change_log\"") {
		t.Errorf("missing change-log CREATE TABLE in the plan")
	}
}

// TestRenderTeardownDDL_KeepData toggles whether the change-log
// table is dropped. The default behaviour drops everything (the
// "remove every trace from the source" promise of §10).
func TestRenderTeardownDDL_KeepData(t *testing.T) {
	drop := renderTeardownDDL("public", []string{"orders"}, false)
	if !anyContains(drop, "DROP TABLE IF EXISTS \"public\".\"sluice_change_log\"") {
		t.Errorf("default teardown missing DROP TABLE sluice_change_log")
	}
	keep := renderTeardownDDL("public", []string{"orders"}, true)
	if anyContains(keep, "DROP TABLE IF EXISTS \"public\".\"sluice_change_log\"") {
		t.Errorf("--keep-data teardown emitted DROP TABLE sluice_change_log")
	}
	// Either shape drops the per-table triggers and the capture
	// functions — those are sluice-managed regardless.
	for _, want := range []string{
		"DROP TRIGGER IF EXISTS \"sluice_capture\" ON \"public\".\"orders\"",
		"DROP FUNCTION IF EXISTS \"public\".\"sluice_capture_change\"()",
	} {
		if !anyContains(drop, want) {
			t.Errorf("default teardown missing %q", want)
		}
		if !anyContains(keep, want) {
			t.Errorf("--keep-data teardown missing %q", want)
		}
	}
}

// TestTableRefusal_Error formats the operator-facing error string.
// The shape carries the schema-qualified table, the refusal reason,
// and the actionable hint so an operator can paste the message and
// have a runbook.
func TestTableRefusal_Error(t *testing.T) {
	r := TableRefusal{
		Schema: "public", Table: "orders",
		Reason: "no-primary-key",
		Hint:   "add a PRIMARY KEY to public.orders before including it in the trigger engine's replication set",
	}
	s := r.Error()
	for _, want := range []string{"no-primary-key", "public.orders", "add a PRIMARY KEY"} {
		if !strings.Contains(s, want) {
			t.Errorf("Error() = %q; missing substring %q", s, want)
		}
	}
}

// anyContains reports whether any string in ss contains substr.
func anyContains(ss []string, substr string) bool {
	for _, s := range ss {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
