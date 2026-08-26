// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// The capture-shape door (audit 2026-08-26 F2) — pgtrigger's mirror of
// sqlite-trigger's verifyCaptureTriggerShape (v0.131.2), the sibling that
// sweep missed because it scoped to the CapturedValueExpr mechanism rather
// than the door CLASS. The defect it closes: a `DROP TRIGGER sluice_capture`
// (or DISABLE TRIGGER, or a rewire to a foreign function) is invisible to
// both of the engine's drift tiers — the event-trigger tag filter only
// watches ALTER/CREATE/DROP TABLE + INDEX, and the polled-fingerprint tier
// watches NOTHING (its loop was never implemented — see
// preflight_ddl_detection.go, capture-completeness G1) — so every subsequent
// DML on that table is silently uncaptured at exit 0. `DROP EVENT TRIGGER sluice_capture_ddl_trg`
// compounds it by also blinding the DDL tier itself. This door refuses
// loudly at stream open, before any data moves, with a `sluice trigger
// setup` re-run as the remedy.
//
// # What this door reaches (the gate-scope enumeration, stated per CLAUDE.md)
//
//   - Every table where AT LEAST ONE sluice capture trigger still exists:
//     a missing partner (DROP TRIGGER leaves the pair broken), a disabled or
//     replica-only trigger, a wrong bound function, and a wrong trigger
//     shape (tgtype) all refuse.
//   - The dropped-EVERYTHING case, via the zero-trigger floor: a change-log
//     table with no capture trigger anywhere in the schema refuses.
//   - The event-trigger tier, via independent evidence: setup creates the
//     DDL capture FUNCTION and the event trigger together (renderSetupDDL's
//     canEventTrigger branch), so "function present, event trigger absent"
//     proves a manual DROP EVENT TRIGGER. A polled-fingerprint install
//     (no function) is exempt — no event trigger was ever expected.
//
// NOT reached: a single table whose row AND truncate triggers were BOTH
// dropped while other tables keep theirs. pgtrigger records no setup-time
// table roster (unlike sqlite-trigger, whose fingerprint table is the
// independent roster its door grades against), so the door has no evidence
// that table was ever replicated; teardown --tables legitimately produces
// exactly that shape. Closing it needs a roster artifact written at setup —
// recorded as the follow-up in the F2 commit, not silently implied here.
//
// Like the sqlite door, this runs at every CDC open — [openCDCReader] is the
// shared chokepoint of both stream-open paths ([Engine.OpenCDCReader] and
// [Engine.OpenSnapshotStream]) — and fails CLOSED: an error reading the
// catalogs refuses rather than streaming unverified.

// pg_trigger.tgtype bit layout (src/include/catalog/pg_trigger.h; stable
// across every supported PG). ROW = 1<<0, BEFORE = 1<<1, INSERT = 1<<2,
// DELETE = 1<<3, UPDATE = 1<<4, TRUNCATE = 1<<5, INSTEAD = 1<<6. AFTER
// triggers have the BEFORE and INSTEAD bits clear.
const (
	// expectedRowTgType is AFTER INSERT OR UPDATE OR DELETE ... FOR EACH ROW —
	// exactly what renderSetupDDL installs. ROW|INSERT|DELETE|UPDATE.
	expectedRowTgType = int16(1 | 1<<2 | 1<<3 | 1<<4)
	// expectedTruncateTgType is AFTER TRUNCATE ... FOR EACH STATEMENT.
	expectedTruncateTgType = int16(1 << 5)
)

// installedCaptureTrigger is one sluice-named trigger row read from
// pg_trigger, carrying everything the grader compares against the installed
// shape.
type installedCaptureTrigger struct {
	table   string
	name    string
	enabled string // pg_trigger.tgenabled: O(rigin), D(isabled), R(eplica), A(lways)
	fn      string // bound function's proname
	tgtype  int16
}

// eventTriggerState is the pg_event_trigger row for the DDL capture trigger
// (zero value = absent).
type eventTriggerState struct {
	present bool
	enabled string // pg_event_trigger.evtenabled: same domain as tgenabled
	fn      string
}

// verifyCaptureTriggerShape is the door's entry point: load the installed
// state, grade it. Fail-closed on any catalog read error.
func verifyCaptureTriggerShape(ctx context.Context, db *sql.DB, schema string) error {
	installed, err := loadInstalledCaptureTriggers(ctx, db, schema)
	if err != nil {
		return fmt.Errorf("pgtrigger: cannot read the installed capture triggers (%w); "+
			"refusing to stream without verifying the capture shape", err)
	}
	ddlFnPresent, evt, err := loadDDLCaptureState(ctx, db, schema)
	if err != nil {
		return fmt.Errorf("pgtrigger: cannot read the DDL capture event-trigger state (%w); "+
			"refusing to stream without verifying the capture shape", err)
	}
	return gradeCaptureShape(schema, installed, ddlFnPresent, evt)
}

// loadInstalledCaptureTriggers reads every sluice-named capture trigger in
// the schema with its enabled state, bound function, and tgtype.
func loadInstalledCaptureTriggers(ctx context.Context, db *sql.DB, schema string) ([]installedCaptureTrigger, error) {
	const q = `
SELECT c.relname, t.tgname, t.tgenabled::text, p.proname, t.tgtype
  FROM pg_trigger   t
  JOIN pg_class     c ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_proc      p ON p.oid = t.tgfoid
 WHERE n.nspname = $1
   AND t.tgname IN ($2, $3)
   AND NOT t.tgisinternal
 ORDER BY c.relname, t.tgname`
	rows, err := db.QueryContext(ctx, q, schema, CaptureTriggerRow, CaptureTriggerTruncate)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []installedCaptureTrigger
	for rows.Next() {
		var it installedCaptureTrigger
		if err := rows.Scan(&it.table, &it.name, &it.enabled, &it.fn, &it.tgtype); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// loadDDLCaptureState reads the two halves of the event-trigger tier's
// evidence: whether the schema carries the DDL capture function (the
// setup-time proof an event-trigger install was made), and the
// pg_event_trigger row for the DDL trigger, if any.
func loadDDLCaptureState(ctx context.Context, db *sql.DB, schema string) (ddlFnPresent bool, evt eventTriggerState, err error) {
	const fnQ = `
SELECT EXISTS (
    SELECT 1
      FROM pg_proc      p
      JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE p.proname = $1
       AND n.nspname = $2
)`
	if err := db.QueryRowContext(ctx, fnQ, CaptureFunctionDDL, schema).Scan(&ddlFnPresent); err != nil {
		return false, eventTriggerState{}, err
	}
	const evtQ = `
SELECT e.evtenabled::text, p.proname
  FROM pg_event_trigger e
  JOIN pg_proc          p ON p.oid = e.evtfoid
 WHERE e.evtname = $1`
	row := db.QueryRowContext(ctx, evtQ, CaptureTriggerDDL)
	switch err := row.Scan(&evt.enabled, &evt.fn); err {
	case nil:
		evt.present = true
	case sql.ErrNoRows:
		// absent — the grader decides whether that is a defect
	default:
		return false, eventTriggerState{}, err
	}
	return ddlFnPresent, evt, nil
}

// gradeCaptureShape is the pure grading half (unit-pinned without a
// database). It refuses on the FIRST defect so the operator sees one
// actionable message; re-running `sluice trigger setup` repairs every
// defect class at once (DROP IF EXISTS + CREATE per trigger, CREATE OR
// REPLACE per function), preserving the change-log, its watermark, and the
// consumer registry.
func gradeCaptureShape(schema string, installed []installedCaptureTrigger, ddlFnPresent bool, evt eventTriggerState) error {
	byTable := map[string]map[string]installedCaptureTrigger{}
	for _, it := range installed {
		m := byTable[it.table]
		if m == nil {
			m = map[string]installedCaptureTrigger{}
			byTable[it.table] = m
		}
		m[it.name] = it
	}

	// The zero-trigger floor (anti-vacuity): a change-log with no capture
	// trigger anywhere means NOTHING is being captured — every source DML
	// silently absent from the stream. The caller has already verified the
	// change-log table exists, so this cannot fire on a never-set-up source
	// (that refuses earlier with the run-setup-first message).
	if len(byTable) == 0 {
		return fmt.Errorf(
			"pgtrigger: %s.%s exists but NO capture trigger is installed on any table in schema %q — "+
				"nothing is being captured (every source change is silently absent from the stream); "+
				"the triggers were dropped without `sluice trigger teardown`. Re-run `sluice trigger setup --dsn=... --tables=...` "+
				"to reinstall (the change-log and resume watermark are preserved), or run `sluice trigger teardown` if the removal was intentional",
			schema, ChangeLogTable, schema,
		)
	}

	tables := make([]string, 0, len(byTable))
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	expected := []struct {
		name   string
		fn     string
		tgtype int16
		events string
	}{
		{CaptureTriggerRow, CaptureFunctionRow, expectedRowTgType, "INSERT/UPDATE/DELETE"},
		{CaptureTriggerTruncate, CaptureFunctionTruncate, expectedTruncateTgType, "TRUNCATE"},
	}
	for _, tbl := range tables {
		have := byTable[tbl]
		for _, want := range expected {
			got, ok := have[want.name]
			if !ok {
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is MISSING from the source (manually dropped?) — its %s changes are not being "+
						"captured at all (silently absent from the stream); re-run `sluice trigger setup --dsn=... --tables=...` to reinstall "+
						"(the change-log and resume watermark are preserved)",
					tbl, want.name, want.events,
				)
			}
			// 'O' (origin, what plain CREATE TRIGGER yields) is the installed
			// shape; 'A' (ENABLE ALWAYS) is accepted as strictly-more capture
			// (it additionally fires under replica role — see
			// preflight_replica_role.go for why setup does not install it).
			// 'D' captures nothing; 'R' fires ONLY under replica role, i.e.
			// for none of this database's own writes. Both are silent loss.
			switch got.enabled {
			case "O", "A":
			case "D":
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is DISABLED (ALTER TABLE ... DISABLE TRIGGER) — its %s changes are not being "+
						"captured (silently absent from the stream); re-enable it (ALTER TABLE %q ENABLE TRIGGER %q) or re-run `sluice trigger setup` to reinstall",
					tbl, want.name, want.events, tbl, want.name,
				)
			default: // "R"
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is set ENABLE REPLICA — it fires ONLY under session_replication_role=replica, "+
						"so none of this database's own %s changes are captured (silently absent from the stream); re-run `sluice trigger setup` to reinstall",
					tbl, want.name, want.events,
				)
			}
			if got.fn != want.fn {
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is bound to function %q, not sluice's %q — it is not what this sluice installs "+
						"(edited, or installed by something else); its %s changes may be mis-captured; re-run `sluice trigger setup` to reinstall",
					tbl, want.name, got.fn, want.fn, want.events,
				)
			}
			if got.tgtype != want.tgtype {
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q has shape tgtype=%d, want %d (AFTER %s, the shape setup installs) — "+
						"it was edited or installed by something else and may mis-capture; re-run `sluice trigger setup` to reinstall",
					tbl, want.name, got.tgtype, want.tgtype, want.events,
				)
			}
		}
	}

	// Event-trigger tier. Setup creates the DDL capture function and the
	// event trigger in the same canEventTrigger branch, so the function is
	// independent evidence the event trigger was installed — its absence
	// with the function present proves a manual DROP EVENT TRIGGER. Without
	// the function this is a polled-fingerprint install: no event trigger
	// was ever expected, and requiring one would false-refuse every
	// --allow-polled-fingerprint source (exempt, with this reason).
	if ddlFnPresent {
		if !evt.present {
			return fmt.Errorf(
				"pgtrigger: the DDL capture function %s.%s exists but event trigger %q is MISSING (DROP EVENT TRIGGER?) — source-side DDL "+
					"would go undetected, so a post-DDL capture would silently mis-capture instead of refusing; re-run `sluice trigger setup` to reinstall it",
				schema, CaptureFunctionDDL, CaptureTriggerDDL,
			)
		}
		switch evt.enabled {
		case "O", "A":
		default: // "D" or "R"
			return fmt.Errorf(
				"pgtrigger: event trigger %q is not enabled for origin sessions (evtenabled=%q) — source-side DDL would go undetected, "+
					"so a post-DDL capture would silently mis-capture instead of refusing; ALTER EVENT TRIGGER %s ENABLE, or re-run `sluice trigger setup`",
				CaptureTriggerDDL, evt.enabled, CaptureTriggerDDL,
			)
		}
		if evt.fn != CaptureFunctionDDL {
			return fmt.Errorf(
				"pgtrigger: event trigger %q is bound to function %q, not sluice's %q — it is not what this sluice installs; "+
					"re-run `sluice trigger setup` to reinstall it",
				CaptureTriggerDDL, evt.fn, CaptureFunctionDDL,
			)
		}
	}
	return nil
}
