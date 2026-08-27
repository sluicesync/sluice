// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
)

// Replica-role capture blindness (audit 2026-08-26 F1, HIGH silent-loss).
//
// The engine's capture triggers are plain CREATE TRIGGER (setup.go's
// renderSetupDDL), so PostgreSQL fires them only for ORIGIN writes: any DML
// executed under `session_replication_role = 'replica'` bypasses them
// entirely (ground-truthed on real PG 16 by the audit). Two real
// configurations write under replica role and therefore lose data SILENTLY —
// the sync exits 0 with rows missing on the target:
//
//  1. The source is itself a native logical-replication SUBSCRIBER: its apply
//     workers run under replica role, so every row a subscription applies is
//     invisible to the capture triggers.
//  2. The all-sluice relay (A→B sluice sync, B→C pgtrigger sync): sluice's
//     own Postgres change applier issues `SET LOCAL session_replication_role
//     = replica` on every apply tx when the apply role holds the privilege
//     (postgres/change_applier.go, replicaRoleSQL / Bug 164) — so the B→C
//     capture sees NOTHING the A→B sync applies. Works in dev (unprivileged
//     applier → triggers fire), loses in prod (privileged applier → they
//     don't).
//
// The FULL fix — installing the capture triggers with ENABLE ALWAYS so they
// fire under replica role too — is deliberately NOT done here: it makes the
// capture see sluice's own applied rows, which in bidirectional/relay
// topologies is an echo loop. That trade needs an ADR (echo suppression,
// origin tagging) and is out of scope for this loud-now half. What this file
// ships is DETECTION: a preflight that recognises both at-risk shapes and
// WARNs unmissably at `trigger setup` and at every stream open. WARN rather
// than refuse because both shapes can be intentional and safe (a subscription
// feeding tables outside the replication set; a decommissioned relay whose
// control table is residue) and there is no per-table evidence cheap enough
// to grade them apart — refusing would need an escape hatch the engine does
// not have yet. The operator-facing caveat lives in
// docs/operator/cdc-streaming.md.

// captureGapRiskMarker is the grep-stable prefix both WARNs carry; the
// integration pin and the mutation run key on it.
const captureGapRiskMarker = "SILENT-CAPTURE-GAP RISK"

// warnReplicaRoleCaptureBlindness probes the source for the two replica-role
// write shapes above and WARNs on each hit. It never fails the caller — but a
// probe ERROR also WARNs ("cannot rule the risk out") rather than silently
// skipping the check: a probe error falling through to silence is exactly the
// SL-1 shape (a halt that reaches the schema-absent case but not the
// probe-error case).
//
// Callers (the sibling-sweep caller list for this door): [Setup] (so the
// operator sees it when installing) and [openCDCReader] (so every stream
// start re-checks — subscriptions and relays appear after setup too).
// openCDCReader is the shared chokepoint of BOTH stream-open paths:
// [Engine.OpenCDCReader] (warm resume) and [Engine.OpenSnapshotStream]
// (cold start) construct the poller through it.
func warnReplicaRoleCaptureBlindness(ctx context.Context, db *sql.DB, schema string) {
	// Bounded so a WARN-only detector can never wedge the open it exists
	// to protect: the relay probe reads a USER table a queued ACCESS
	// EXCLUSIVE can park indefinitely (audit 2026-08-27 A5; rationale on
	// [openProbeTimeout]). On expiry each probe's error arm fires its
	// "cannot rule the risk out" WARN — the degrade, not a silent skip.
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	warnLogicalSubscriberShape(pctx, db)
	warnSluiceRelayShape(pctx, db, schema)
}

// warnLogicalSubscriberShape WARNs when the current database has
// logical-replication subscriptions: their apply workers write under
// session_replication_role=replica, which the plain capture triggers do not
// fire for.
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
			"Sync from the publishing (origin) database instead, or keep subscribed tables out of the replication set. "+
			"sluice deliberately does not install ENABLE ALWAYS triggers (they would also capture replicated echoes; the full fix needs its own ADR)",
		slog.String("subscriptions", strings.Join(subs, ", ")))
}

// warnSluiceRelayShape WARNs when the source carries sluice's own per-target
// apply bookkeeping (sluice_cdc_state) — the all-sluice relay shape: this
// database is (or was) the TARGET of another sluice sync, whose privileged
// applier writes under replica role.
func warnSluiceRelayShape(ctx context.Context, db *sql.DB, schema string) {
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
		slog.WarnContext(ctx,
			"pgtrigger: "+captureGapRiskMarker+": cannot probe for sluice's own apply control table on the source; relay-shape capture blindness cannot be ruled out",
			slog.Any("err", err))
		return
	}
	if !exists {
		return
	}
	// Best-effort detail: how many streams apply into this database. A
	// failure here degrades the WARN's detail, not its firing.
	var streams sql.NullInt64
	detailQ := "SELECT count(*) FROM " + quoteIdent(schema) + "." + quoteIdent(appliershared.ControlTableName)
	if err := db.QueryRowContext(ctx, detailQ).Scan(&streams); err != nil {
		streams = sql.NullInt64{}
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+captureGapRiskMarker+": this source database carries sluice's own apply bookkeeping ("+appliershared.ControlTableName+") — it is (or was) "+
			"the TARGET of another sluice sync. A privileged applier (superuser/rds_superuser) runs every apply tx under session_replication_role=replica, which the plain "+
			"capture triggers do NOT fire for — in a relay (A→B sluice apply, B→C pgtrigger capture) the B→C stream forwards NOTHING the A→B sync applies, silently, at exit 0. "+
			"This works in dev (an unprivileged applier's rows ARE captured) and loses in prod. Sync the final target from the original source directly, or stop the upstream "+
			"sync before trusting this capture. sluice deliberately does not install ENABLE ALWAYS triggers (echo-loop implications; the full fix needs its own ADR)",
		slog.String("control_table", schema+"."+appliershared.ControlTableName),
		slog.Int64("registered_streams", streams.Int64))
}
