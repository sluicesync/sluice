// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Replica-role capture shapes (audit 2026-08-26 F1, HIGH silent-loss;
// full fix ADR-0185).
//
// The engine's DEFAULT capture triggers are plain CREATE TRIGGER
// (setup.go's renderSetupDDL), so PostgreSQL fires them only for ORIGIN
// writes: any DML executed under `session_replication_role = 'replica'`
// bypasses them entirely (ground-truthed on real PG 16 by the audit AND
// pinned by TestReplicaRoleWrites_InvisibleToCapture_AndPreflightWarns).
// Two real configurations write under replica role and therefore lose
// data SILENTLY — the sync exits 0 with rows missing on the target:
//
//  1. The source is itself a native logical-replication SUBSCRIBER: its
//     apply workers run under replica role, so every row a subscription
//     applies is invisible to the capture triggers.
//  2. The all-sluice relay (A→B sluice sync, B→C pgtrigger sync): sluice's
//     own Postgres change applier issues `SET LOCAL session_replication_role
//     = replica` on every apply tx when the apply role holds the privilege
//     (postgres/change_applier.go, replicaRoleSQL / Bug 164) — so the B→C
//     capture sees NOTHING the A→B sync applies. Works in dev (unprivileged
//     applier → triggers fire), loses in prod (privileged applier → they
//     don't).
//
// ADR-0185 splits the two shapes by remedy:
//
//   - DEFAULT posture (plain triggers): both shapes WARN unmissably at
//     `trigger setup` and at every stream open, steering the subscriber
//     shape to `--capture-replicated-writes`. WARN rather than refuse
//     because both shapes can be intentional and safe (a subscription
//     feeding tables outside the replication set; a decommissioned relay
//     whose control table is residue).
//   - OPT-IN posture (`trigger setup --capture-replicated-writes`, ENABLE
//     ALWAYS triggers): the subscriber shape is the SUPPORTED scenario —
//     replicated writes ARE captured, no WARN — and the relay shape is
//     REFUSED (SLUICE-E-CDC-TRIGGER-ECHO-LOOP): sluice apply bookkeeping
//     on the source proves another sluice sync applies INTO this database,
//     and ENABLE ALWAYS triggers would re-capture its applied rows as new
//     changes — an echo loop (unbounded in a cycle, duplicated fan-out
//     otherwise). The probe erring under the opt-in FAILS CLOSED, per the
//     F2 door's discipline — a refusal-gating probe must not degrade to a
//     pass.
//
// The operator-facing story lives in docs/operator/cdc-streaming.md.

// captureGapRiskMarker is the grep-stable prefix the default posture's
// WARNs carry; the integration pin and the mutation run key on it.
const captureGapRiskMarker = "SILENT-CAPTURE-GAP RISK"

// checkReplicaRoleCaptureShapes is the single dispatch point both
// enforcement chokepoints call — [Setup] (so the operator sees it when
// installing, dry-run included) and [openCDCReader] (so every stream
// start re-checks — subscriptions and relays appear after setup too;
// openCDCReader is the shared chokepoint of BOTH stream-open paths:
// [Engine.OpenCDCReader] (warm resume) and [Engine.OpenSnapshotStream]
// (cold start) construct the poller through it).
//
// captureReplicated is the posture: the flag at Setup, the recorded
// meta-table posture ([readCaptureReplicatedWritesPosture]) at open.
// Plain posture WARNs on both shapes and never fails the caller; the
// opt-in refuses the relay shape (and skips the subscriber probe — that
// shape is what the opt-in supports).
func checkReplicaRoleCaptureShapes(ctx context.Context, db *sql.DB, schema string, captureReplicated bool) error {
	// Bounded so an open-path probe can never wedge the open it exists to
	// protect: the relay probe reads a USER table a queued ACCESS
	// EXCLUSIVE can park indefinitely (audit 2026-08-27 A5; rationale on
	// [openProbeTimeout]). Under the plain posture an expired probe fires
	// its "cannot rule the risk out" WARN — the degrade, not a silent
	// skip; under the opt-in it refuses (fail closed).
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	if captureReplicated {
		return refuseRelayEchoLoop(pctx, db, schema)
	}
	warnLogicalSubscriberShape(pctx, db)
	warnSluiceRelayShape(pctx, db, schema)
	return nil
}

// readCaptureReplicatedWritesPosture reads the install's recorded capture
// posture from the meta table (ADR-0185). The to_jsonb projection is the
// load-bearing shape: a pre-v3 install's meta table has no
// capture_replicated_writes column at all, and to_jsonb of the row simply
// omits the key there — `->>` yields NULL and the COALESCE lands on
// false, which is exactly what a pre-v3 (plain-trigger) install is. A
// direct column reference would instead hard-error 42703 on every old
// install. A missing meta ROW reads as false too (nothing ever recorded
// an opt-in); any other failure — including a missing meta TABLE — fails
// CLOSED, because the posture selects both the F2 door's expected
// enablement and whether the echo-loop refusal is armed.
func readCaptureReplicatedWritesPosture(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	// Bounded per the open-path probe convention (audit 2026-08-27 A5).
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	q := "SELECT COALESCE((to_jsonb(m) ->> '" + metaCaptureReplicatedCol + "')::boolean, FALSE) FROM " +
		quoteIdent(schema) + "." + quoteIdent(ChangeLogMetaTable) + " m WHERE m.singleton_pk"
	var v bool
	switch err := db.QueryRowContext(pctx, q).Scan(&v); {
	case err == nil:
		return v, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf(
			"pgtrigger: cannot read the recorded capture posture from %s.%s (%w) — refusing to stream without knowing "+
				"whether this install captures replicated writes (the posture selects the capture-shape door's expected "+
				"trigger enablement and arms the echo-loop refusal); repair the meta table by re-running `sluice trigger setup`",
			schema, ChangeLogMetaTable, err,
		)
	}
}

// refuseRelayEchoLoop is the opt-in posture's relay-shape door: refuse
// loudly (coded) when the source carries sluice's own per-target apply
// bookkeeping — proof another sluice sync applies INTO this database,
// whose applied rows the ENABLE ALWAYS triggers would re-capture and
// forward as new changes. Fail-closed on a probe error: a refusal-gating
// probe degrading to a pass is the SL-1 shape.
func refuseRelayEchoLoop(ctx context.Context, db *sql.DB, schema string) error {
	exists, streams, err := probeRelayControlTable(ctx, db, schema)
	if err != nil {
		return fmt.Errorf(
			"pgtrigger: cannot probe for sluice's own apply bookkeeping (%s.%s) on the source (%w) — replicated-write "+
				"capture (--capture-replicated-writes) refuses to proceed unverified: if another sluice sync applies into "+
				"this database, the ENABLE ALWAYS capture triggers would re-capture its applied rows as new changes (an "+
				"echo loop). Clear the probe failure and re-run",
			schema, appliershared.ControlTableName, err,
		)
	}
	if !exists {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCTriggerEchoLoop,
		"capture from the origin database instead, or decommission the upstream sync and drop "+
			schema+"."+appliershared.ControlTableName+", or run trigger setup without --capture-replicated-writes",
		fmt.Errorf(
			"pgtrigger: replicated-write capture refused: this source database carries sluice's own apply bookkeeping "+
				"(%s.%s, %d registered stream(s)) — it is (or was) the TARGET of another sluice sync, whose applier "+
				"writes under session_replication_role=replica. The ENABLE ALWAYS capture triggers this opt-in installs "+
				"fire for those writes too, so every row that sync applies here would be re-captured and forwarded as a "+
				"NEW change — an echo loop (unbounded re-application in a cyclic topology, duplicated fan-out otherwise). "+
				"Capture from the ORIGIN database instead of relaying through this one; or, if the upstream sync is "+
				"finished for good, decommission it and drop its control table, then re-run; or install without "+
				"--capture-replicated-writes (origin-only capture, with the replica-role WARN)",
			schema, appliershared.ControlTableName, streams.Int64,
		),
	)
}

// probeRelayControlTable reports whether the source carries sluice's
// per-target apply control table, plus a best-effort registered-stream
// count (invalid when the detail read fails — the caller treats that as
// detail, never as the signal).
func probeRelayControlTable(ctx context.Context, db *sql.DB, schema string) (bool, sql.NullInt64, error) {
	const existsQ = `
SELECT EXISTS (
    SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = $1
       AND n.nspname = $2
       AND c.relkind = 'r'
)`
	var exists bool
	if err := db.QueryRowContext(ctx, existsQ, appliershared.ControlTableName, schema).Scan(&exists); err != nil {
		return false, sql.NullInt64{}, err
	}
	if !exists {
		return false, sql.NullInt64{}, nil
	}
	// Best-effort detail: how many streams apply into this database. A
	// failure here degrades the message's detail, not the signal.
	var streams sql.NullInt64
	detailQ := "SELECT count(*) FROM " + quoteIdent(schema) + "." + quoteIdent(appliershared.ControlTableName)
	if err := db.QueryRowContext(ctx, detailQ).Scan(&streams); err != nil {
		streams = sql.NullInt64{}
	}
	return true, streams, nil
}

// warnLogicalSubscriberShape WARNs when the current database has
// logical-replication subscriptions: their apply workers write under
// session_replication_role=replica, which the plain capture triggers do
// not fire for. Plain-posture only — under --capture-replicated-writes
// this shape is the supported scenario and the probe is not run.
func warnLogicalSubscriberShape(ctx context.Context, db *sql.DB) {
	const q = `
SELECT s.subname, s.subenabled
  FROM pg_subscription s
  JOIN pg_database d ON d.oid = s.subdbid
 WHERE d.datname = current_database()
 ORDER BY s.subname`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+captureGapRiskMarker+": cannot read pg_subscription to check whether this source is a logical-replication subscriber; "+
				"if it is, rows its subscriptions apply arrive under session_replication_role=replica and the capture triggers will NOT fire for them (silently missing from the stream)",
			slog.Any("err", err))
		return
	}
	defer func() { _ = rows.Close() }()
	var subs []string
	for rows.Next() {
		var name string
		var enabled bool
		if err := rows.Scan(&name, &enabled); err != nil {
			slog.WarnContext(ctx,
				"pgtrigger: "+captureGapRiskMarker+": cannot read pg_subscription rows; subscriber-shape capture blindness cannot be ruled out",
				slog.Any("err", err))
			return
		}
		state := "enabled"
		if !enabled {
			state = "disabled"
		}
		subs = append(subs, name+" ("+state+")")
	}
	if err := rows.Err(); err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+captureGapRiskMarker+": cannot read pg_subscription rows; subscriber-shape capture blindness cannot be ruled out",
			slog.Any("err", err))
		return
	}
	if len(subs) == 0 {
		return
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+captureGapRiskMarker+": this source database is a logical-replication SUBSCRIBER — rows its subscriptions apply run under "+
			"session_replication_role=replica, and the capture triggers are plain CREATE TRIGGER (origin writes only), so every subscription-applied row is "+
			"INVISIBLE to the change stream: the sync exits 0 with those rows silently missing from the target. Only rows written directly on this database are captured. "+
			"Re-run `sluice trigger setup --capture-replicated-writes` to install ENABLE ALWAYS triggers that capture replicated writes (ADR-0185; refused if this "+
			"database is also the target of another sluice sync — the echo-loop shape), or sync from the publishing (origin) database instead, or keep subscribed "+
			"tables out of the replication set",
		slog.String("subscriptions", strings.Join(subs, ", ")))
}

// warnSluiceRelayShape WARNs when the source carries sluice's own per-target
// apply bookkeeping (sluice_cdc_state) — the all-sluice relay shape: this
// database is (or was) the TARGET of another sluice sync, whose privileged
// applier writes under replica role.
func warnSluiceRelayShape(ctx context.Context, db *sql.DB, schema string) {
	exists, streams, err := probeRelayControlTable(ctx, db, schema)
	if err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+captureGapRiskMarker+": cannot probe for sluice's own apply control table on the source; relay-shape capture blindness cannot be ruled out",
			slog.Any("err", err))
		return
	}
	if !exists {
		return
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+captureGapRiskMarker+": this source database carries sluice's own apply bookkeeping ("+appliershared.ControlTableName+") — it is (or was) "+
			"the TARGET of another sluice sync. A privileged applier (superuser/rds_superuser) runs every apply tx under session_replication_role=replica, which the plain "+
			"capture triggers do NOT fire for — in a relay (A→B sluice apply, B→C pgtrigger capture) the B→C stream forwards NOTHING the A→B sync applies, silently, at exit 0. "+
			"This works in dev (an unprivileged applier's rows ARE captured) and loses in prod. Sync the final target from the original source directly, or stop the upstream "+
			"sync before trusting this capture. Note --capture-replicated-writes (ADR-0185) deliberately REFUSES on this shape: ENABLE ALWAYS triggers would re-capture the "+
			"upstream sync's applied rows — an echo loop",
		slog.String("control_table", schema+"."+appliershared.ControlTableName),
		slog.Int64("registered_streams", streams.Int64))
}
