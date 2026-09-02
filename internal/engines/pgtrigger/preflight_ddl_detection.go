// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"log/slog"
)

// Polled-fingerprint-tier DDL-detection blindness (capture-completeness
// sweep 2026-08-26, G1).
//
// `trigger setup --allow-polled-fingerprint` exists for roles that cannot
// CREATE EVENT TRIGGER (Heroku Essential is the documented case). The
// polled schema-fingerprint loop that tier PROMISED was never implemented
// — setup.go's own doc says "Phase 1 only records the operator's intent"
// — so a polled-tier install has NO DDL detection at all: any source DDL
// (DROP COLUMN, RENAME, and the worst member, a table-rewriting
// `ALTER … TYPE USING <expr>`, whose rewrite fires no row trigger) is
// invisible to capture, and the stream keeps exiting 0 while pre-DDL rows
// diverge permanently (reproduced end-to-end on real PG 16 by the sweep).
// The event-trigger tier refuses this whole class via the op='X' door;
// this tier silently lacks the door.
//
// This file ships the MINIMUM remediation: an unmissable WARN at every
// CDC open, plus the claim-surface fixes (the Heroku recipe and the
// setup-time mode string no longer promise detection that does not
// exist). The full fingerprint poll loop (sqlite-trigger's
// verifyNoSchemaDrift is the in-repo template) and the relfilenode
// rewrite belt are FILED as their own backlog item — deliberately not
// built here.

// ddlDetectionAbsentMarker is the grep-stable prefix the WARN carries;
// the integration pin and the mutation run key on it.
const ddlDetectionAbsentMarker = "DDL-DETECTION-ABSENT"

// warnDDLDetectionAbsent WARNs when the install is polled-tier — the DDL
// capture function is absent, the same evidence the F2 capture-shape door
// keys its event-trigger-tier exemption on ([loadDDLCaptureState]'s
// ddlFnPresent). It never fails the caller, but a probe ERROR also WARNs
// ("cannot rule the blindness out") rather than silently skipping — the
// SL-1 probe-error discipline, mirroring the
// plain-posture arm of [checkReplicaRoleCaptureShapes] one call above.
//
// Caller (the sibling-sweep list for this door): [openCDCReader] — the
// shared chokepoint of BOTH stream-open paths ([Engine.OpenCDCReader]
// warm resume and [Engine.OpenSnapshotStream] cold start construct the
// poller through it). Setup's claim surface is handled separately: the
// CLI's mode string now reports "NONE" for this tier, so the operator
// hears it at install time too.
func warnDDLDetectionAbsent(ctx context.Context, db *sql.DB, schema string) {
	// Bounded (audit 2026-08-27 A5; rationale on [openProbeTimeout]): on
	// expiry the error arm below fires the "could not rule it out" WARN —
	// the degrade, not a silent skip.
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	state, err := loadDDLCaptureState(pctx, db, schema)
	if err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+ddlDetectionAbsentMarker+": cannot read the DDL capture state to establish whether this install has DDL detection; "+
				"if it is a --allow-polled-fingerprint install, source DDL is INVISIBLE to capture (see the polled-tier warning) and this open could not rule that out; "+
				"the same read establishes whether the install carries the sql_drop capture arm ("+dropCaptureAbsentMarker+"), so that is unknown too",
			slog.Any("err", err))
		return
	}
	if state.anyFnPresent() {
		// Event-trigger tier: grade the ARMS, not just the tier. This rides
		// the read above deliberately — a second probe would add another
		// serial open-path timeout for state already in hand, and both
		// WARNs share the same posture (warn, degrade on error).
		warnDropCaptureAbsent(ctx, state, schema)
		return
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+ddlDetectionAbsentMarker+": this install is on the --allow-polled-fingerprint tier, which has NO DDL detection — "+
			"the promised polled schema-fingerprint loop is not yet implemented, so ANY source DDL is invisible to capture: "+
			"a table-rewriting ALTER (e.g. ALTER COLUMN ... TYPE ... USING) rewrites every stored row with NO change-log rows and NO refusal, "+
			"and the sync keeps exiting 0 while pre-DDL rows diverge permanently. "+
			"Apply source DDL with the drained model instead: `sluice sync stop --wait`, apply the DDL on source AND target, re-run `sluice trigger setup`, then `sluice sync start`. "+
			"If your role can CREATE EVENT TRIGGER, re-run `sluice trigger setup` WITHOUT --allow-polled-fingerprint to install the event-trigger tier, which refuses loudly on observed DDL",
		slog.String("schema", schema))
}

// dropCaptureAbsentMarker is the grep-stable prefix for the D-1 upgrade
// WARN; the integration pin and the mutation run key on it.
const dropCaptureAbsentMarker = "DROP-CAPTURE-ABSENT"

// warnDropCaptureAbsent WARNs when an event-trigger-tier install predates
// the `sql_drop` arm (audit 2026-08-31, D-1): the ddl_command_end tier is
// installed but the sql_drop one is not, so a DROP of a captured table is
// invisible — the stream keeps exiting 0 while the target holds the
// dropped table's last-synced rows forever.
//
// WARN rather than refuse, for the same reason [warnInsecureCaptureFunctions]
// warns: the check runs at every CDC open, which is every warm resume, so
// refusing would stop already-running syncs the moment the operator upgrades
// the binary — for a gap that is bounded and static (once the table is gone
// there are no further source writes to miss) rather than growing. The
// remedy is a seconds-long `sluice trigger setup` re-run that preserves the
// change log, its watermark and the consumer registry. A polled-fingerprint
// install is exempt: it has NO event-trigger tier at all, which
// [warnDDLDetectionAbsent] already says louder.
//
// Caller (the sibling-sweep list for this door): [warnDDLDetectionAbsent],
// which is itself called only from [openCDCReader] — the shared chokepoint
// of BOTH stream-open paths. It grades the state its caller already read
// rather than probing again (one fewer serial open-path timeout); the
// probe-error degrade is the caller's, and names this marker.
func warnDropCaptureAbsent(ctx context.Context, state ddlCaptureState, schema string) {
	if !state.fnPresent[CaptureFunctionDDL] || state.fnPresent[CaptureFunctionDrop] {
		return
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+dropCaptureAbsentMarker+": this install predates the sql_drop capture arm, so DROPPING a synced table is invisible to capture — "+
			"PostgreSQL reports drops only through pg_catalog.pg_event_trigger_dropped_objects() (a sql_drop event trigger), never through the "+
			"pg_catalog.pg_event_trigger_ddl_commands() the installed DDL tier reads, so the drop records NOTHING, the stream keeps exiting 0, and the target "+
			"keeps the dropped table's last-synced rows forever. Upgrading the sluice binary does not install the arm: re-run "+
			"`sluice trigger setup --dsn=... --tables=...` (the change log, its resume watermark and the consumer registry are all preserved, "+
			"and the stream resumes where it left off)",
		slog.String("schema", schema),
		slog.String("missing", CaptureTriggerDrop))
}
