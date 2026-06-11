// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
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
	if !c.PostgresBackend {
		t.Error("PostgresBackend = false; want true (genuine PG server — XID/partition preflights must fire)")
	}
	if c.PGExtensionCatalog {
		t.Error("PGExtensionCatalog = true; want false (extension passthrough unvalidated through the trigger capture path)")
	}
	if c.VerbatimExtensionTypes {
		t.Error("VerbatimExtensionTypes = true; want false (ADR-0047 verbatim tier unvalidated through the trigger capture path)")
	}
	if c.DDLDialect != ir.DDLDialectANSI {
		t.Errorf("DDLDialect = %v; want DDLDialectANSI", c.DDLDialect)
	}
}

// TestEngine_WithConnectionLabel pins the [ir.ConnectionLabeler]
// surface: the id is normalised and stored for the trigger-native
// pools (the CDC poller / trigger-native snapshot, opened through
// postgres.OpenPgxDB), AND the composed postgres engine is labeled too
// — the delegated schema/row/applier surfaces must carry the same id,
// not silently keep the "-" fallback.
func TestEngine_WithConnectionLabel(t *testing.T) {
	labeled, ok := Engine{}.WithConnectionLabel("mystream").(Engine)
	if !ok {
		t.Fatal("WithConnectionLabel should return the concrete pgtrigger.Engine")
	}
	if labeled.appID != "mystream" {
		t.Errorf("appID = %q, want %q", labeled.appID, "mystream")
	}
	if labeled.pg == (postgres.Engine{}) {
		t.Error("composed postgres engine was not labeled; delegated surfaces would silently keep the \"-\" fallback")
	}

	empty := Engine{}.WithConnectionLabel("").(Engine)
	if empty.appID != "-" {
		t.Errorf("empty id should normalise to %q, got %q", "-", empty.appID)
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
	withET := renderSetupDDL("public", []string{"orders"}, true, CapturePayloadFull)
	withoutET := renderSetupDDL("public", []string{"orders"}, false, CapturePayloadFull)

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

// TestRenderCaptureDDLFunction_OrphanedTriggerRecoveryMessage pins the
// Bug 101 (v0.92.0) fix: the DDL event-trigger function wraps its
// INSERT in an EXCEPTION handler so an operator who manually drops
// sluice_change_log without running `sluice trigger teardown` sees a
// clear recovery message naming the recovery command, instead of
// PG's raw "relation does not exist" + function-body dump (which
// blocks ALL DDL on the source with no operator-visible recovery
// path).
func TestRenderCaptureDDLFunction_OrphanedTriggerRecoveryMessage(t *testing.T) {
	ddl := renderCaptureDDLFunction("public", `"public"."sluice_change_log"`)

	// The EXCEPTION handler must be present in the function body.
	for _, want := range []string{
		"EXCEPTION",
		"WHEN undefined_table THEN",
		"RAISE EXCEPTION",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL function body missing %q (Bug 101 fix not wired):\n%s", want, ddl)
		}
	}

	// The recovery message must name BOTH the diagnostic ("partially
	// uninstalled") and the operator-actionable commands so the
	// operator can paste-and-run the fix.
	for _, want := range []string{
		"partially uninstalled",
		"sluice trigger teardown",
		"sluice trigger setup",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("recovery message missing %q (operator can't recover without it):\n%s", want, ddl)
		}
	}

	// Pin the SQLSTATE so monitoring / log-grep tools can key off it.
	// `object_not_in_prerequisite_state` (55000) is the closest match
	// for "the engine is half-installed".
	if !strings.Contains(ddl, "object_not_in_prerequisite_state") {
		t.Errorf("ERRCODE should be object_not_in_prerequisite_state for monitoring keyability:\n%s", ddl)
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

// captureRowFn renders the row-capture function for one mode. Helper
// keeps the ADR-0068 mode-shape tests terse.
func captureRowFn(payload CapturePayload) string {
	return renderCaptureRowFunction("public", `"public"."sluice_change_log"`, payload)
}

// TestCaptureRowFunction_Full pins the default mode (ADR-0068): the
// UPDATE branch writes BOTH full images and the DELETE before-image is
// the full OLD row — byte-identical to prior releases. It must NOT
// contain the changed-set diff.
func TestCaptureRowFunction_Full(t *testing.T) {
	ddl := captureRowFn(CapturePayloadFull)

	// UPDATE: full before AND full after.
	if !strings.Contains(ddl, "v_before := to_jsonb(OLD)") {
		t.Errorf("full mode: UPDATE before should be to_jsonb(OLD); ddl=\n%s", ddl)
	}
	if !strings.Contains(ddl, "v_after  := to_jsonb(NEW)") {
		t.Errorf("full mode: UPDATE after should be to_jsonb(NEW)")
	}
	// No changed-set diff in full mode.
	if strings.Contains(ddl, "IS DISTINCT FROM") {
		t.Errorf("full mode: must NOT compute the changed-set diff")
	}
	// DELETE before is full OLD, not PK-only.
	if strings.Contains(ddl, "v_before := v_pk") {
		t.Errorf("full mode: before must never be the PK-only v_pk")
	}
}

// TestCaptureRowFunction_Changed pins ADR-0068 `changed`: the UPDATE
// after-image is the PK ∪ changed-cols diff, but the before-image stays
// the full OLD row (divergence WHERE), and DELETE before is full OLD.
func TestCaptureRowFunction_Changed(t *testing.T) {
	ddl := captureRowFn(CapturePayloadChanged)

	// The changed-set diff (the load-bearing snippet) must be present,
	// referencing the cached v_new_json / v_old_json vars (NOT recomputing
	// to_jsonb each iteration — see TestCaptureRowFunction_CachesToJsonb).
	for _, want := range []string{
		"jsonb_object_agg(n.key, n.value)",
		"jsonb_each(v_new_json) n",
		"(v_old_json -> n.key) IS DISTINCT FROM n.value",
		"n.key = ANY(v_pk_cols)", // PK union
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("changed mode: missing changed-set fragment %q; ddl=\n%s", want, ddl)
		}
	}
	// before stays the FULL old row (the cached v_old_json) so the apply
	// WHERE keeps optimistic divergence detection.
	if !strings.Contains(ddl, "v_before   := v_old_json;  -- full before-image") {
		t.Errorf("changed mode: UPDATE before must be the full v_old_json (cached to_jsonb(OLD))")
	}
	if strings.Contains(ddl, "v_before := v_pk") {
		t.Errorf("changed mode: before must NOT be trimmed to the PK")
	}
}

// TestCaptureRowFunction_Minimal pins ADR-0068 `minimal`: the UPDATE
// after-image is the changed-set diff AND both the UPDATE and DELETE
// before-images are trimmed to the PK only (v_pk).
func TestCaptureRowFunction_Minimal(t *testing.T) {
	ddl := captureRowFn(CapturePayloadMinimal)

	// changed-set diff for the after image (same as `changed`).
	if !strings.Contains(ddl, "IS DISTINCT FROM n.value") {
		t.Errorf("minimal mode: UPDATE after must be the changed-set diff; ddl=\n%s", ddl)
	}
	// before trimmed to PK in BOTH UPDATE and DELETE. UPDATE before is
	// the OLD-PK projection (a PK-changing UPDATE must still locate the
	// target by its OLD PK — using the NEW PK would silently lose the
	// update); DELETE before is v_pk (also the OLD PK).
	if !strings.Contains(ddl, "v_before   := (SELECT jsonb_object_agg(key, value) FROM jsonb_each(v_old_json) WHERE key = ANY(v_pk_cols))") {
		t.Errorf("minimal mode: UPDATE before must be the OLD-PK projection of cached v_old_json (PK-changing-UPDATE correctness); ddl=\n%s", ddl)
	}
	if n := strings.Count(ddl, "v_before := v_pk"); n != 1 {
		t.Errorf("minimal mode: want 1 `v_before := v_pk` (DELETE only; UPDATE uses the OLD-PK projection); got %d; ddl=\n%s", n, ddl)
	}
	// The FULL OLD row must NOT be captured into before (that would be
	// the `changed`/`full` shape, defeating the source-write saving).
	// Note: the OLD-PK *projection* references to_jsonb(OLD) inside a
	// jsonb_each(...) — that is fine; what must be absent is the whole-row
	// assignment `v_before := to_jsonb(OLD);`.
	if strings.Contains(ddl, "v_before := to_jsonb(OLD);") {
		t.Errorf("minimal mode: before must be PK-only, never the full-row `v_before := to_jsonb(OLD);`")
	}
}

// TestCaptureRowFunction_CachesToJsonb pins the ADR-0068 §Follow-ups
// trigger-CPU optimization: the trim-mode UPDATE branch must compute
// to_jsonb(NEW) / to_jsonb(OLD) ONCE per row into v_new_json / v_old_json
// and reference the cached vars everywhere else. Without the cache, the
// branch re-evaluates to_jsonb(NEW) twice (v_pk + v_after's FROM) and
// to_jsonb(OLD) 1+N times (v_before / v_after's per-column WHERE lookup,
// N = column count) — which the 2026-05-29 head-to-head measurement
// showed lined up with the ~1.6× source-write slowdown vs `full`.
//
// Full mode's body must NOT contain these assignments (the DECLARE has
// the vars for the shared scaffold, but full mode references to_jsonb
// directly — exactly once each — and never the cache).
func TestCaptureRowFunction_CachesToJsonb(t *testing.T) {
	for _, c := range []struct {
		mode      CapturePayload
		wantCache bool
	}{
		{CapturePayloadFull, false},
		{CapturePayloadChanged, true},
		{CapturePayloadMinimal, true},
	} {
		ddl := captureRowFn(c.mode)
		gotNew := strings.Count(ddl, "v_new_json := to_jsonb(NEW);")
		gotOld := strings.Count(ddl, "v_old_json := to_jsonb(OLD);")
		if c.wantCache {
			if gotNew != 1 || gotOld != 1 {
				t.Errorf("%s mode: want exactly one `v_new_json := to_jsonb(NEW);` and one `v_old_json := to_jsonb(OLD);` (cached once per row); got new=%d old=%d; ddl=\n%s",
					c.mode, gotNew, gotOld, ddl)
			}
		} else {
			if gotNew != 0 || gotOld != 0 {
				t.Errorf("%s mode: must NOT assign the cache vars (full mode references to_jsonb directly); got new=%d old=%d; ddl=\n%s",
					c.mode, gotNew, gotOld, ddl)
			}
		}
	}
}

// TestCaptureRowFunction_InsertSharedAcrossModes confirms the INSERT
// branch (full new-row image) is identical in all three modes — ADR-0068
// trims only UPDATE/DELETE, never INSERT.
func TestCaptureRowFunction_InsertSharedAcrossModes(t *testing.T) {
	const insertWant = "IF TG_OP = 'INSERT' THEN"
	for _, m := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		ddl := captureRowFn(m)
		if !strings.Contains(ddl, insertWant) {
			t.Errorf("mode %q: missing shared INSERT branch", m)
		}
		// The INSERT after-image is always the full new row.
		if !strings.Contains(ddl, "v_after  := to_jsonb(NEW);\n        v_before := NULL;") {
			t.Errorf("mode %q: INSERT must write the full new-row image", m)
		}
		// Shared scaffold present in every mode.
		for _, want := range []string{"SECURITY DEFINER", "SET search_path = pg_catalog, pg_temp", "INSERT INTO"} {
			if !strings.Contains(ddl, want) {
				t.Errorf("mode %q: missing shared scaffold %q", m, want)
			}
		}
	}
}

// TestNormalizePayload validates the SetupOptions payload-mode contract:
// empty defaults to full, the three known modes pass through, and an
// unknown value refuses-loudly (ADR-0068).
func TestNormalizePayload(t *testing.T) {
	cases := []struct {
		in      CapturePayload
		want    CapturePayload
		wantErr bool
	}{
		{"", CapturePayloadFull, false},
		{CapturePayloadFull, CapturePayloadFull, false},
		{CapturePayloadChanged, CapturePayloadChanged, false},
		{CapturePayloadMinimal, CapturePayloadMinimal, false},
		{"lite", "", true},
		{"FULL", "", true}, // case-sensitive; the kong enum is lowercase
	}
	for _, c := range cases {
		c := c
		t.Run(string(c.in), func(t *testing.T) {
			got, err := normalizePayload(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("normalizePayload(%q): want error; got nil (got=%q)", c.in, got)
				}
				if !strings.Contains(err.Error(), "capture-payload") {
					t.Errorf("error %q should name --capture-payload", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizePayload(%q): unexpected error %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("normalizePayload(%q) = %q; want %q", c.in, got, c.want)
			}
		})
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
