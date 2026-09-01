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

// installMeta is what one bounded read of the meta table tells the open
// path about the install: the ADR-0185 capture posture, the schema version
// that recorded it, and the capture-function provenance digest (SL-5).
type installMeta struct {
	captureFnDigest   string
	schemaVersion     int
	captureReplicated bool
}

// captureDigestMinSchemaVer is the schema version at which
// capture_fn_digest was INTRODUCED, and the floor for believing one.
//
// It is deliberately a frozen literal rather than [ChangeLogSchemaVer].
// Those are equal today and must not be written as one: the trust floor is
// a fact about when the digest started being recorded, which never changes,
// while ChangeLogSchemaVer moves on every future meta-table migration. Spelt
// as `>= ChangeLogSchemaVer`, the next unrelated bump to 6 would make every
// correctly-set-up v5 install's digest untrusted overnight — silently
// dropping this door from REFUSE to WARN for the tamper case it exists to
// catch, as a side effect of a change with nothing to do with it, and with
// no failing test to say so. Pinned by
// [TestCaptureDigestTrust_FloorIsFrozenNotTheCurrentSchemaVersion].
const captureDigestMinSchemaVer = 5

// captureDigestTrusted reports whether [installMeta.captureFnDigest] can be
// believed. The COLUMN's presence is not the signal — the VERSION is: an
// older binary's `trigger setup` run rewrites the capture functions without
// touching (or knowing) the digest column, and its upsert regresses
// schema_version to that binary's own value. Reading a stale digest as
// truth there would refuse a downgrade-then-upgrade install for tampering
// that never happened, so the version gate is what keeps this door's
// refusal arm honest.
func (m installMeta) captureDigestTrusted() bool {
	return m.captureFnDigest != "" && m.schemaVersion >= captureDigestMinSchemaVer
}

// readInstallMeta reads the install's recorded state from the meta table.
// The to_jsonb projection is the load-bearing shape: an older install's
// meta table has no capture_replicated_writes (pre-v3) or
// capture_fn_digest (pre-v5) column at all, and to_jsonb of the row simply
// omits the key there — `->>` yields NULL and the COALESCE lands on the
// pre-migration default, which is exactly what such an install is. A
// direct column reference would instead hard-error 42703 on every old
// install. A missing meta ROW reads the same way (nothing ever recorded
// anything); any other failure — including a missing meta TABLE — fails
// CLOSED, because the posture selects both the F2 door's expected
// enablement and whether the echo-loop refusal is armed.
func readInstallMeta(ctx context.Context, db *sql.DB, schema string) (installMeta, error) {
	// Bounded per the open-path probe convention (audit 2026-08-27 A5).
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	q := "SELECT COALESCE((to_jsonb(m) ->> '" + metaCaptureReplicatedCol + "')::boolean, FALSE), " +
		"COALESCE((to_jsonb(m) ->> 'schema_version')::int, 0), " +
		"COALESCE(to_jsonb(m) ->> '" + metaCaptureDigestCol + "', '') FROM " +
		quoteIdent(schema) + "." + quoteIdent(ChangeLogMetaTable) + " m WHERE m.singleton_pk"
	var m installMeta
	switch err := db.QueryRowContext(pctx, q).Scan(&m.captureReplicated, &m.schemaVersion, &m.captureFnDigest); {
	case err == nil:
		return m, nil
	case errors.Is(err, sql.ErrNoRows):
		return installMeta{}, nil
	default:
		return installMeta{}, fmt.Errorf(
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
	found, err := probeRelayControlTable(ctx, db)
	if err != nil {
		return fmt.Errorf(
			"pgtrigger: cannot probe for sluice's own apply bookkeeping (%s) anywhere in this database (%w) — replicated-write "+
				"capture (--capture-replicated-writes) refuses to proceed unverified: if another sluice sync applies into "+
				"this database, the ENABLE ALWAYS capture triggers would re-capture its applied rows as new changes (an "+
				"echo loop). Clear the probe failure and re-run",
			appliershared.ControlTableName, err,
		)
	}
	if len(found) == 0 {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCTriggerEchoLoop,
		"capture from the origin database instead, or decommission the upstream sync and drop "+
			relayControlTableList(found)+", or run trigger setup without --capture-replicated-writes",
		fmt.Errorf(
			"pgtrigger: replicated-write capture refused: this source database carries sluice's own apply bookkeeping "+
				"(%s) — it is (or was) the TARGET of another sluice sync, whose applier "+
				"writes under session_replication_role=replica. The ENABLE ALWAYS capture triggers this opt-in installs "+
				"fire for those writes too, so every row that sync applies here would be re-captured and forwarded as a "+
				"NEW change — an echo loop (unbounded re-application in a cyclic topology, duplicated fan-out otherwise). "+
				"Capture from the ORIGIN database instead of relaying through this one; or, if the upstream sync is "+
				"finished for good, decommission it and drop its control table, then re-run; or install without "+
				"--capture-replicated-writes (origin-only capture, with the replica-role WARN). "+
				"(The capture schema this setup targets is %q; the bookkeeping above is reported wherever it lives, "+
				"because the applier pins its control table to the target DSN's schema and --target-schema moves the "+
				"user data WITHOUT moving it — so the two schemas are designed to diverge, while the echo loop is a "+
				"property of the DATABASE)",
			relayControlTableDetail(found), schema,
		),
	)
}

// relayControlExistsQuery is the existence half of the relay probe. It is
// package-level so [TestProbeRelayControlTable_ExistenceQueryIsDatabaseWide]
// can grade the predicate itself: the ONE thing that must never come back
// is a `n.nspname = $2` scope (see [probeRelayControlTable] for why).
const relayControlExistsQuery = `
SELECT n.nspname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relname = $1
   AND c.relkind = 'r'
   AND n.nspname NOT LIKE 'pg\_%'
   AND n.nspname <> 'information_schema'
 ORDER BY n.nspname`

// relayControlTable is one sluice apply-control table found on the source:
// the schema holding it and, best effort, the number of streams registered
// in it. Streams is INVALID when the detail read failed, and every consumer
// MUST branch on Valid — audit 2026-08-31 A-5: both consumers formatted
// Int64 unconditionally, so a denied or lock-parked count(*) rendered
// "0 registered stream(s)" inside a refusal that fired precisely BECAUSE
// the table is there, pointing the operator at the "decommissioned residue,
// drop it" remedy on evidence nobody read.
type relayControlTable struct {
	schema  string
	streams sql.NullInt64
}

// probeRelayControlTable reports every schema of the CURRENT DATABASE that
// carries sluice's per-target apply control table, each with a best-effort
// registered-stream count.
//
// # The existence half is database-wide on purpose (audit 2026-08-31 SEC-4)
//
// It used to scope on `n.nspname = <the capture schema>`, i.e. pgtrigger's
// own DSN schema. But the hazard is not schema-shaped: the upstream sluice
// applier pins its control table to the TARGET DSN's schema at construction
// and `--target-schema` (ADR-0031) deliberately moves the user data without
// moving the control table, so an A→B sync landing rows in `customer_svc`
// keeps its bookkeeping in `public`. Setting the B→C capture up against
// `customer_svc` then probed a schema that could not hold the evidence, the
// refusal did not fire, and the ENABLE ALWAYS triggers went in on exactly
// the topology the refusal exists to stop. The echo loop is a property of
// the database (any schema's applied rows are replica-role writes the
// ALWAYS triggers capture), so the probe's scope is now the database and
// the message reports WHERE the evidence was found.
//
// System namespaces are excluded: `pg_temp_N` holds session-lifetime temp
// tables that no applier could be using as durable bookkeeping, and the
// `pg_` prefix is reserved by PostgreSQL so no user schema can be dropped
// by that filter.
//
// The per-hit count(*) stays schema-qualified — it is DETAIL, and a failure
// there degrades the message rather than the signal (see [relayControlTable]).
func probeRelayControlTable(ctx context.Context, db *sql.DB) ([]relayControlTable, error) {
	rows, err := db.QueryContext(ctx, relayControlExistsQuery, appliershared.ControlTableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var found []relayControlTable
	for rows.Next() {
		var t relayControlTable
		if err := rows.Scan(&t.schema); err != nil {
			return nil, err
		}
		found = append(found, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range found {
		// Best-effort detail: how many streams apply into this database. A
		// failure here degrades the message's detail, not the signal.
		detailQ := "SELECT count(*) FROM " + quoteIdent(found[i].schema) + "." + quoteIdent(appliershared.ControlTableName)
		if err := db.QueryRowContext(ctx, detailQ).Scan(&found[i].streams); err != nil {
			found[i].streams = sql.NullInt64{}
		}
	}
	return found, nil
}

// relayControlTableList renders the found tables as a bare
// schema-qualified list, for the remedy string.
func relayControlTableList(found []relayControlTable) string {
	names := make([]string, 0, len(found))
	for _, t := range found {
		names = append(names, t.schema+"."+appliershared.ControlTableName)
	}
	return strings.Join(names, ", ")
}

// relayControlTableDetail renders the found tables WITH their stream
// counts, honouring the A-5 validity: an unread count says so instead of
// printing a zero the probe never obtained.
func relayControlTableDetail(found []relayControlTable) string {
	parts := make([]string, 0, len(found))
	for _, t := range found {
		count := "stream count unavailable — the detail read failed"
		if t.streams.Valid {
			count = fmt.Sprintf("%d registered stream(s)", t.streams.Int64)
		}
		parts = append(parts, fmt.Sprintf("%s.%s, %s", t.schema, appliershared.ControlTableName, count))
	}
	return strings.Join(parts, "; ")
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
// applier writes under replica role. It shares [probeRelayControlTable] with
// the opt-in's refusal and therefore shares its DATABASE-WIDE scope (SEC-4):
// the plain posture lost this warning in exactly the topology the refusal
// missed, so the sibling is fixed by the same change rather than by a second
// one.
func warnSluiceRelayShape(ctx context.Context, db *sql.DB, schema string) {
	found, err := probeRelayControlTable(ctx, db)
	if err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+captureGapRiskMarker+": cannot probe for sluice's own apply control table on the source; relay-shape capture blindness cannot be ruled out",
			slog.Any("err", err))
		return
	}
	if len(found) == 0 {
		return
	}
	slog.WarnContext(ctx,
		"pgtrigger: "+captureGapRiskMarker+": this source database carries sluice's own apply bookkeeping ("+appliershared.ControlTableName+") — it is (or was) "+
			"the TARGET of another sluice sync. A privileged applier (superuser/rds_superuser) runs every apply tx under session_replication_role=replica, which the plain "+
			"capture triggers do NOT fire for — in a relay (A→B sluice apply, B→C pgtrigger capture) the B→C stream forwards NOTHING the A→B sync applies, silently, at exit 0. "+
			"This works in dev (an unprivileged applier's rows ARE captured) and loses in prod. Sync the final target from the original source directly, or stop the upstream "+
			"sync before trusting this capture. Note --capture-replicated-writes (ADR-0185) deliberately REFUSES on this shape: ENABLE ALWAYS triggers would re-capture the "+
			"upstream sync's applied rows — an echo loop",
		slog.String("capture_schema", schema),
		slog.String("control_tables", relayControlTableDetail(found)))
}
