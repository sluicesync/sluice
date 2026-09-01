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
//   - Every capture trigger the install creates — the per-table row and
//     truncate pair AND both event triggers — graded against the recorded
//     ADR-0185 posture (audit A-1). The mechanical statement of that scope
//     is the capture-tier roster (capture_tier_roster_test.go), which
//     derives its universe from what renderSetupDDL emits.
//   - Every table where AT LEAST ONE sluice capture trigger still exists:
//     a missing partner (DROP TRIGGER leaves the pair broken), a disabled or
//     replica-only trigger, a wrong bound function, a wrong trigger shape
//     (tgtype), and — since ADR-0185 — an enablement POSTURE that does not
//     match the installed intent all refuse. Setup records whether the
//     install opted into replicated-write capture (the meta table's
//     capture_replicated_writes, read by [readCaptureReplicatedWritesPosture]);
//     the door demands tgenabled 'A' (ENABLE ALWAYS) under the opt-in and
//     'O' (plain) without it, in BOTH directions: opt-in-recorded-but-plain
//     silently loses every replicated write (the exact class the opt-in
//     closes), and plain-recorded-but-ALWAYS is someone hand-flipping
//     enablement into capturing replica-role writes without the echo-loop
//     vetting — hand-flipped drift is exactly what this door exists for.
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

// eventTriggerState is the pg_event_trigger row for one capture event
// trigger (zero value = absent).
type eventTriggerState struct {
	present bool
	enabled string // pg_event_trigger.evtenabled: same domain as tgenabled
	fn      string
}

// eventTier is one of the §7 DDL tier's two event-trigger arms. Each is
// graded independently and each is EXEMPT when its own function is absent
// — that is what keeps a polled-fingerprint install (no functions at all)
// and a pre-v0.135 install (no sql_drop arm) from false-refusing. The
// pairing is the door's independent evidence: setup creates a tier's
// function and its event trigger in the same canEventTrigger branch, so
// "function present, trigger absent" proves a manual DROP EVENT TRIGGER.
type eventTier struct {
	fn      string // proname setup installs
	trigger string // evtname setup installs
	watches string // what the arm records, for the message
}

// eventTierRoster is the two arms, in the order setup renders them. A
// third arm added to renderSetupDDL and not added here is what the
// capture-tier roster gate (capture_tier_roster_test.go) fails on.
var eventTierRoster = []eventTier{
	{fn: CaptureFunctionDDL, trigger: CaptureTriggerDDL, watches: "source-side ALTER/CREATE DDL"},
	{fn: CaptureFunctionDrop, trigger: CaptureTriggerDrop, watches: "a DROP of a captured table"},
}

// verifyCaptureTriggerShape is the door's entry point: load the installed
// state, grade it against the recorded posture (captureReplicated — the
// ADR-0185 opt-in, read from the meta table by the caller). Fail-closed on
// any catalog read error — including a probe TIMEOUT (audit 2026-08-27 A5;
// rationale on [openProbeTimeout]): a hung shape check must not silently
// pass, so an expired probe deadline refuses with its own message rather
// than degrading to a WARN the way the WARN-only probes do.
func verifyCaptureTriggerShape(ctx context.Context, db *sql.DB, schema string, captureReplicated bool) error {
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	installed, err := loadInstalledCaptureTriggers(pctx, db, schema)
	if err != nil {
		return captureShapeProbeError(ctx, pctx, "installed capture triggers", err)
	}
	ddl, err := loadDDLCaptureState(pctx, db, schema)
	if err != nil {
		return captureShapeProbeError(ctx, pctx, "DDL capture event-trigger state", err)
	}
	return gradeCaptureShape(schema, installed, ddl, captureReplicated)
}

// captureShapeProbeError shapes the door's fail-closed refusal for a
// probe error, distinguishing the probe deadline expiring (pctx dead,
// caller ctx alive — the driver may surface that as a wrapped ctx error
// OR as PG's 57014 cancel, so the ctx states are the reliable signal)
// from an ordinary read failure or a caller-side cancellation.
func captureShapeProbeError(ctx, pctx context.Context, what string, err error) error {
	if pctx.Err() != nil && ctx.Err() == nil {
		return fmt.Errorf("pgtrigger: reading the %s timed out after %s (%w) — the source may be wedged behind a "+
			"queued lock or a dead connection; a hung shape check must NOT silently pass, so this open refuses rather "+
			"than streaming with an unverified capture shape. Clear the blocking and re-run",
			what, openProbeTimeout, err)
	}
	return fmt.Errorf("pgtrigger: cannot read the %s (%w); refusing to stream without verifying the capture shape",
		what, err)
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

// ddlCaptureState is the event-trigger tier's installed evidence, per arm:
// which capture FUNCTIONS the schema carries (the setup-time proof that
// arm was installed) and the pg_event_trigger row for each arm's trigger.
type ddlCaptureState struct {
	fnPresent map[string]bool              // proname → present
	triggers  map[string]eventTriggerState // evtname → row (absent = zero value)
}

// anyFnPresent reports whether ANY capture function of the event-trigger
// tier exists — the "this is not a polled-fingerprint install" signal.
func (s ddlCaptureState) anyFnPresent() bool {
	for _, present := range s.fnPresent {
		if present {
			return true
		}
	}
	return false
}

// loadDDLCaptureState reads both halves of the event-trigger tier's
// evidence for every arm in [eventTierRoster].
func loadDDLCaptureState(ctx context.Context, db *sql.DB, schema string) (ddlCaptureState, error) {
	out := ddlCaptureState{
		fnPresent: map[string]bool{},
		triggers:  map[string]eventTriggerState{},
	}
	const fnQ = `
SELECT EXISTS (
    SELECT 1
      FROM pg_proc      p
      JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE p.proname = $1
       AND n.nspname = $2
)`
	const evtQ = `
SELECT e.evtenabled::text, p.proname
  FROM pg_event_trigger e
  JOIN pg_proc          p ON p.oid = e.evtfoid
 WHERE e.evtname = $1`
	for _, tier := range eventTierRoster {
		var present bool
		if err := db.QueryRowContext(ctx, fnQ, tier.fn, schema).Scan(&present); err != nil {
			return ddlCaptureState{}, err
		}
		out.fnPresent[tier.fn] = present

		var evt eventTriggerState
		switch err := db.QueryRowContext(ctx, evtQ, tier.trigger).Scan(&evt.enabled, &evt.fn); err {
		case nil:
			evt.present = true
		case sql.ErrNoRows:
			// absent — the grader decides whether that is a defect
		default:
			return ddlCaptureState{}, err
		}
		out.triggers[tier.trigger] = evt
	}
	return out, nil
}

// gradeCaptureShape is the pure grading half (unit-pinned without a
// database). It refuses on the FIRST defect so the operator sees one
// actionable message; re-running `sluice trigger setup` (with the flags
// matching the intent) repairs every defect class at once (DROP IF EXISTS
// + CREATE + the ENABLE ALWAYS ALTERs per trigger, CREATE OR REPLACE per
// function, the posture upsert), preserving the change-log, its watermark,
// and the consumer registry.
//
// captureReplicated is the RECORDED intent, and since audit 2026-08-31 A-1
// the posture match it drives covers EVERY capture trigger the install
// creates — the two per-table triggers and both event triggers. The ADR's
// original scope ("the opt-in never alters the event trigger's enablement —
// logical replication does not replicate DDL") was true of a native
// subscription apply worker and false of the write class the opt-in
// actually admits: `session_replication_role = replica` is an ordinary
// operator/ETL idiom, and an 'O' event trigger does not fire for it
// (observed). Grading three of four triggers would leave the door narrower
// than its name.
func gradeCaptureShape(schema string, installed []installedCaptureTrigger, ddl ddlCaptureState, captureReplicated bool) error {
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
			// The expected enablement is the RECORDED posture (ADR-0185):
			// 'O' (origin-only, plain CREATE TRIGGER) for a default
			// install, 'A' (ENABLE ALWAYS — additionally fires under
			// replica role) under --capture-replicated-writes. 'D'
			// captures nothing; 'R' fires ONLY under replica role, i.e.
			// for none of this database's own writes — both are silent
			// loss under either posture. A posture MISMATCH ('A' where
			// 'O' was recorded, or 'O' where 'A' was) refuses in both
			// directions: the door's whole point is hand-flipped drift.
			wantEnabled := "O"
			if captureReplicated {
				wantEnabled = "A"
			}
			switch got.enabled {
			case wantEnabled:
			case "D":
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is DISABLED (ALTER TABLE ... DISABLE TRIGGER) — its %s changes are not being "+
						"captured (silently absent from the stream); re-enable it (ALTER TABLE %q ENABLE TRIGGER %q) or re-run `sluice trigger setup` to reinstall",
					tbl, want.name, want.events, tbl, want.name,
				)
			case "R":
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is set ENABLE REPLICA — it fires ONLY under session_replication_role=replica, "+
						"so none of this database's own %s changes are captured (silently absent from the stream); re-run `sluice trigger setup` to reinstall",
					tbl, want.name, want.events,
				)
			case "A": // reachable only when the recorded posture is origin-only
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is set ENABLE ALWAYS but this install recorded ORIGIN-ONLY capture — the trigger's "+
						"enablement was flipped by hand, so replica-role (replicated/applied) %s writes are being captured WITHOUT the echo-loop vetting "+
						"the --capture-replicated-writes opt-in runs (ADR-0185); re-run `sluice trigger setup` to restore origin-only capture, or re-run it "+
						"with --capture-replicated-writes to make replicated-write capture the recorded, vetted intent",
					tbl, want.name, want.events,
				)
			default: // got.enabled == "O" while the recorded posture is ENABLE ALWAYS
				return fmt.Errorf(
					"pgtrigger: table %q capture trigger %q is plain ENABLE (origin-only) but this install recorded --capture-replicated-writes — "+
						"replica-role (replicated/applied) %s writes are NOT being captured (silently absent from the stream — the exact loss the opt-in "+
						"exists to close; ADR-0185); re-run `sluice trigger setup --capture-replicated-writes` to restore the ENABLE ALWAYS triggers",
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

	// Event-trigger tier, arm by arm ([eventTierRoster]). Setup creates each
	// arm's capture function and its event trigger in the same
	// canEventTrigger branch, so the function is independent evidence that
	// arm was installed — its absence with the function present proves a
	// manual DROP EVENT TRIGGER. Without the function the arm is EXEMPT, and
	// that exemption carries two populations, not one: a polled-fingerprint
	// install (no functions at all — requiring an event trigger would
	// false-refuse every --allow-polled-fingerprint source) and, for the
	// sql_drop arm only, an install made before v0.135, which never had it.
	// The second population is not silently tolerated: warnDropCaptureAbsent
	// WARNs at every open, because "no drop detection" is the D-1 gap.
	for _, tier := range eventTierRoster {
		if !ddl.fnPresent[tier.fn] {
			continue
		}
		evt := ddl.triggers[tier.trigger]
		if !evt.present {
			return fmt.Errorf(
				"pgtrigger: the capture function %s.%s exists but event trigger %q is MISSING (DROP EVENT TRIGGER?) — %s "+
					"would go undetected, so a post-DDL capture would silently mis-capture instead of refusing; re-run `sluice trigger setup` to reinstall it",
				schema, tier.fn, tier.trigger, tier.watches,
			)
		}
		// The event triggers carry the SAME posture as the per-table pair
		// (audit 2026-08-31 A-1): 'A' under the opt-in, 'O' without it.
		// Before A-1 this arm accepted 'O' or 'A' blindly, on the ADR's
		// premise that "logical replication does not replicate DDL" — true
		// of a native apply worker, but the opt-in admits replica-role
		// writes from ANY session, so the two capture tiers could disagree.
		wantEvtEnabled := "O"
		if captureReplicated {
			wantEvtEnabled = "A"
		}
		switch evt.enabled {
		case wantEvtEnabled:
		case "D", "R":
			return fmt.Errorf(
				"pgtrigger: event trigger %q is not enabled for origin sessions (evtenabled=%q) — %s would go undetected, "+
					"so a post-DDL capture would silently mis-capture instead of refusing; ALTER EVENT TRIGGER %s ENABLE, or re-run `sluice trigger setup`",
				tier.trigger, evt.enabled, tier.watches, tier.trigger,
			)
		case "A": // reachable only when the recorded posture is origin-only
			return fmt.Errorf(
				"pgtrigger: event trigger %q is set ENABLE ALWAYS but this install recorded ORIGIN-ONLY capture — its enablement was flipped by hand, "+
					"so %s is captured for replica-role sessions whose DML this install does NOT capture, and the two tiers disagree; "+
					"re-run `sluice trigger setup` to restore origin-only capture, or re-run it with --capture-replicated-writes to make replicated capture the recorded, vetted intent",
				tier.trigger, tier.watches,
			)
		default: // evt.enabled == "O" while the recorded posture is ENABLE ALWAYS
			return fmt.Errorf(
				"pgtrigger: event trigger %q is plain ENABLE (origin-only) but this install recorded --capture-replicated-writes — %s is NOT detected for "+
					"replica-role (replicated/applied) sessions while their DML IS captured, so the applier would write post-DDL-shaped rows with no refusal "+
					"(ADR-0185, audit A-1); re-run `sluice trigger setup --capture-replicated-writes` to set it ENABLE ALWAYS",
				tier.trigger, tier.watches,
			)
		}
		if evt.fn != tier.fn {
			return fmt.Errorf(
				"pgtrigger: event trigger %q is bound to function %q, not sluice's %q — it is not what this sluice installs; "+
					"re-run `sluice trigger setup` to reinstall it",
				tier.trigger, evt.fn, tier.fn,
			)
		}
	}
	return nil
}
