// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the SEC-1 surface: the emitted shape of every
// SECURITY DEFINER function this engine installs, the pure grader
// [definerFunctionState.hijackable], and the probe-error arm of
// [warnInsecureCaptureFunctions]. The catalog-reading half, the
// end-to-end WARN, and the EXPLOIT itself run against real PG in
// definer_search_path_integration_test.go.

package pgtrigger

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestEverySecurityDefinerFunctionPinsSearchPath is the roster gate for
// SEC-1: EVERY function this engine emits as SECURITY DEFINER must carry
// the `SET search_path = pg_catalog, pg_temp` clause. The universe is the
// three renderers × all three capture-payload modes — the DDL function is
// the one that shipped without the clause from v0.85.0 to v0.134.0, and
// stating the other two here is the sibling enumeration, not decoration:
// the gate's name says "every", so it grades every one.
//
// Reach, stated so the name cannot be read as broader than the truth: the
// STRINGS these renderers produce. A SECURITY DEFINER function some future
// code path emits from elsewhere would not enter this universe —
// TestNoUnpinnedSecurityDefinerEmitters below is the AST floor that
// catches that.
func TestEverySecurityDefinerFunctionPinsSearchPath(t *testing.T) {
	t.Parallel()
	const ref = `"public"."sluice_change_log"`
	emitted := map[string]string{
		"capture DDL":      renderCaptureDDLFunction("public", ref),
		"capture truncate": renderCaptureTruncateFunction("public", ref),
	}
	for _, mode := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		emitted["capture row ("+string(mode)+")"] = renderCaptureRowFunction("public", ref, mode)
	}
	if len(emitted) < 5 {
		t.Fatalf("anti-vacuity: graded %d emitted functions, want the 2 singletons + 3 payload modes", len(emitted))
	}
	for name, ddl := range emitted {
		if !strings.Contains(ddl, "SECURITY DEFINER") {
			t.Fatalf("%s: fixture no longer emits SECURITY DEFINER — this gate is grading the wrong thing", name)
		}
		if !strings.Contains(ddl, "SET search_path = pg_catalog, pg_temp") {
			t.Errorf("%s: SECURITY DEFINER with NO pinned search_path — the function resolves unqualified names "+
				"against the FIRING session's path, and the DDL one is superuser-owned (SEC-1 privilege escalation):\n%s", name, ddl)
		}
	}
}

// TestCaptureDDLFunction_QualifiesEveryCall is the second belt: even with
// the search_path pinned, every built-in the DDL capture function's body
// calls is pg_catalog-qualified, so neither half of the fix is solely
// load-bearing. COALESCE is a reserved keyword (not resolvable to a
// user-defined function) and is deliberately left bare.
func TestCaptureDDLFunction_QualifiesEveryCall(t *testing.T) {
	t.Parallel()
	ddl := renderCaptureDDLFunction("public", `"public"."sluice_change_log"`)
	for _, fn := range []string{
		"pg_event_trigger_ddl_commands",
		"pg_current_xact_id",
		"jsonb_build_object",
		"current_setting",
	} {
		if !strings.Contains(ddl, "pg_catalog."+fn) {
			t.Errorf("body calls %s without the pg_catalog. qualification:\n%s", fn, ddl)
		}
		// And no BARE call survives: every occurrence must be preceded by
		// the qualification. (regexp rather than Count arithmetic so the
		// failure names the offending spelling.)
		if bare := regexp.MustCompile(`(^|[^.\w])` + fn + `\(`).FindString(ddl); bare != "" {
			t.Errorf("body still contains an UNQUALIFIED %s( call (%q) — an attacker-typed overload in a "+
				"reachable schema could win resolution if the search_path pin were ever lost:\n%s", fn, bare, ddl)
		}
	}
}

// TestDefinerFunctionState_Hijackable pins the grader's full truth table
// (2 booleans × both values): only SECURITY DEFINER + unpinned search_path
// is the vulnerable shape. A SECURITY INVOKER function runs as its caller
// and escalates nothing; a pinned definer resolves against pg_catalog with
// no attacker-writable schema in the path.
func TestDefinerFunctionState_Hijackable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		definer, pinned, want bool
	}{
		{definer: true, pinned: false, want: true}, // the SEC-1 shape
		{definer: true, pinned: true, want: false},
		{definer: false, pinned: false, want: false},
		{definer: false, pinned: true, want: false},
	}
	for _, tc := range cases {
		s := definerFunctionState{name: "f", securityDefiner: tc.definer, searchPathPinned: tc.pinned}
		if got := s.hijackable(); got != tc.want {
			t.Errorf("hijackable(definer=%t, pinned=%t) = %t, want %t", tc.definer, tc.pinned, got, tc.want)
		}
	}
}

// TestWarnInsecureCaptureFunctions_ProbeErrorAlsoWarns pins the SL-1
// probe-error discipline for this door: a failed pg_proc read must WARN
// ("cannot rule it out"), never silently skip.
func TestWarnInsecureCaptureFunctions_ProbeErrorAlsoWarns(t *testing.T) {
	// A syntactically valid DSN at a closed port: the lazy pool opens, the
	// first query fails, the WARN path runs.
	db, err := sql.Open("pgx", "postgres://u:p@127.0.0.1:1/nope?connect_timeout=1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logs := captureLogsForUnitTest(func() {
		warnInsecureCaptureFunctions(ctx, db, "public")
	})
	if !strings.Contains(logs, insecureDefinerMarker) {
		t.Fatalf("probe error did not WARN with the %s marker (silent skip — the SL-1 shape):\n%s",
			insecureDefinerMarker, logs)
	}
	for _, want := range []string{"cannot read", "trigger setup"} {
		if !strings.Contains(logs, want) {
			t.Errorf("probe-error WARN missing %q; got:\n%s", want, logs)
		}
	}
}
