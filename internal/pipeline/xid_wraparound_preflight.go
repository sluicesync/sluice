// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Postgres XID-wraparound preflight (pgcopydb PR #17 adoption; see
// `docs/dev/notes/pgcopydb-planetscale-fork-review.md`).
//
// Closes a confusing late-failure class when migrating from or
// streaming CDC against a Postgres source whose `age(datfrozenxid)`
// is near the wraparound horizon. PG's 32-bit XID counter wraps at
// ~2^31; well before the hard horizon PG enters emergency
// anti-wraparound autovacuum, and at the limit it stops accepting
// new writes globally until VACUUM FREEZE recovery completes. A
// migration / CDC stream against such a source either:
//
//   - Hits the global write-block mid-migration — opaque error,
//     half-migrated state, operator has to figure out the actual
//     cause from a SQLSTATE 54000 / "database is not accepting
//     commands" message.
//   - On the CDC path, ACTIVELY contributes to the problem: a
//     long-held replication-slot `xmin` (or the trigger-engine's
//     `pg_snapshot_xmin` safety-lag query) prevents autovacuum from
//     advancing the relfrozenxid, so the wraparound horizon gets
//     closer the longer the stream runs.
//
// The preflight catches both classes UPFRONT and refuses loudly with
// `VACUUM FREEZE` guidance.
//
// # Gating
//
// Fires for any source declaring [ir.Capabilities.PostgresBackend] —
// the slot-based `postgres` and the slot-less `postgres-trigger`
// engines both front a genuine PG server, whose 32-bit XID machinery
// this preflight probes. MySQL sources and every non-PG path
// short-circuit. The handle's [xidWraparoundProber] interface presence
// ALONE is insufficient — the capability gate excludes non-PG sources
// whose handles happen to satisfy the prober shape.
//
// # Threshold
//
// `xidWraparoundRefuseThreshold = 1_500_000_000` (1.5B). This leaves
// ~600M of headroom before PG's hard wraparound horizon (~2.15B)
// while sitting well above the autovacuum_freeze_max_age default
// (200M) — an operator running healthy autovacuum will never see
// this preflight refuse. The threshold catches the operationally-
// dangerous case (stuck long-lived transaction, mis-tuned
// autovacuum, very-long-uptime database) before sluice's CDC stream
// adds an `xmin` and makes it worse.
//
// # No opt-out flag
//
// Mirroring the REPLICATION preflight: no `--allow-xid-wraparound-
// risk` flag is provided. The recovery action (VACUUM FREEZE on the
// source, or kill the stuck long-lived transaction) is fast and
// genuinely required — deferring would only re-surface the late-
// failure class the preflight exists to replace. If demand surfaces
// for a "migrate FROM a wrapping DB" workflow, add an opt-out then.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errXIDWraparoundRefused is the sentinel cause for an XID-wraparound
// preflight refusal. Wrapped with the message naming the database, the
// observed age, and the recovery paths. Tests assert via [errors.Is] to
// avoid coupling to the message text.
var errXIDWraparoundRefused = errors.New("pipeline: XID-wraparound preflight refused")

// xidWraparoundRefuseThreshold is the `age(datfrozenxid)` value above
// which the preflight refuses. See the file-header §Threshold rationale.
const xidWraparoundRefuseThreshold = int64(1_500_000_000)

// xidWraparoundProber is the optional surface a Postgres source
// SchemaReader implements to drive the XID-wraparound preflight.
//
// SourceXIDWraparoundHorizon reports the current `age(datfrozenxid)`
// for the connecting database. The datname is surfaced in the refusal
// message so the operator knows which database to VACUUM FREEZE (an
// operator running multiple sluice streams against the same PG cluster
// would otherwise have to guess).
//
// Defined in the pipeline package rather than `ir` because it is
// orchestrator-private (matches [replicationCapabilityProber] and
// [rlsPreflightProber]).
type xidWraparoundProber interface {
	SourceXIDWraparoundHorizon(ctx context.Context) (age int64, datname string, err error)
}

// preflightSourceXIDWraparound runs the XID-wraparound preflight
// against the source handle. Returns nil when:
//
//   - The source doesn't declare [ir.Capabilities.PostgresBackend]
//     (the capability gate — excludes MySQL and every non-PG path;
//     both `postgres` and `postgres-trigger` declare it).
//   - The handle doesn't implement [xidWraparoundProber] (a PG
//     surface that doesn't expose the probe — opportunistic-skip
//     posture, matches [preflightSourceReplication]).
//   - The observed age is below [xidWraparoundRefuseThreshold].
//
// Returns a wrapped [errXIDWraparoundRefused] when the database is
// near the wraparound horizon. The message names the database, the
// observed age, the threshold, and the operator-actionable recovery
// paths (VACUUM FREEZE, kill the stuck long-lived transaction, or
// wait for autovacuum to advance the horizon).
func preflightSourceXIDWraparound(ctx context.Context, handle any, sourceCaps ir.Capabilities) error {
	if !sourceCaps.PostgresBackend {
		return nil
	}
	prober, ok := handle.(xidWraparoundProber)
	if !ok {
		return nil
	}

	age, datname, err := prober.SourceXIDWraparoundHorizon(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: XID-wraparound preflight: probe source age(datfrozenxid): %w", err,
		))
	}
	if age < xidWraparoundRefuseThreshold {
		return nil
	}

	return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
		"%w: %s",
		errXIDWraparoundRefused, formatXIDWraparoundRefusal(datname, age),
	))
}

// formatXIDWraparoundRefusal renders the operator-facing refusal
// message. Shape mirrors [formatReplicationRefusal]: name the concrete
// state (database + observed age), explain the mechanism (32-bit XID
// + autovacuum horizon), and list every operator-actionable recovery
// path so the operator can pick the one that fits their situation.
func formatXIDWraparoundRefusal(datname string, age int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "the source database %q is near the Postgres XID-wraparound horizon: ", datname)
	fmt.Fprintf(&b, "age(datfrozenxid) = %d, refuse-threshold = %d (the wraparound horizon is ~2,147,483,647). ", age, xidWraparoundRefuseThreshold)
	b.WriteString("Migrating / streaming CDC against a near-wraparound database is dangerous: PG will start refusing writes globally well before the horizon (SQLSTATE 54000, ")
	b.WriteString("\"database is not accepting commands\"), and on the CDC path the stream's xmin / safety-lag holds back autovacuum and makes the problem WORSE the longer the stream runs. ")
	b.WriteString("Recovery: (a) `VACUUM FREEZE` the source database (the canonical fix — advances relfrozenxid on every table; for large databases run `VACUUM FREEZE VERBOSE ANALYZE;` and expect it to take time); ")
	b.WriteString("(b) find and end any long-lived transaction holding back vacuum: `SELECT pid, state, age(backend_xmin), query FROM pg_stat_activity WHERE backend_xmin IS NOT NULL ORDER BY age(backend_xmin) DESC NULLS LAST LIMIT 10;`; ")
	b.WriteString("(c) wait for autovacuum to advance the horizon (`SELECT relname, age(relfrozenxid) FROM pg_class WHERE relkind = 'r' ORDER BY age(relfrozenxid) DESC LIMIT 10;` to track progress). ")
	b.WriteString("Re-run sluice once `age(datfrozenxid)` drops below the threshold")
	return b.String()
}
