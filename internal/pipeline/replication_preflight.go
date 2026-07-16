// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Postgres replication-capability preflight (task #61).
//
// Closes a confusing mid-cold-start failure class on managed-PG tiers
// that forbid the REPLICATION attribute (Heroku Postgres Essential,
// Render Basic, Supabase free). sluice's slot-based `postgres` CDC
// engine creates a logical replication slot at cold start; the
// connecting role needs to be a superuser OR carry the REPLICATION
// attribute to do so. Without it, slot creation fails MID-COLD-START
// with a raw wrapped PG permission error (`postgres: create replication
// slot "sluice_slot": ERROR: permission denied ...`, SQLSTATE 42501) —
// opaque, fires after schema-read + filter work, and gives the operator
// no hint that a slot-less path exists.
//
// This preflight detects the missing capability UPFRONT (before the CDC
// reader opens / the slot is created) and refuses loudly with an
// operator-actionable message that points at
// `--source-driver=postgres-trigger` — the slot-LESS trigger-capture
// engine that exists exactly for this managed-PG case.
//
// # Gating (correctness-critical)
//
// The preflight fires ONLY for a source whose declared CDC mechanism is
// [ir.CDCLogicalReplication] — the capability that MEANS "cold start
// creates a logical replication slot". It MUST NOT fire for:
//
//   - `postgres-trigger` — the slot-less engine is the RECOMMENDED FIX;
//     refusing on it would be absurd. Its SchemaReader delegates to the
//     composed [postgres.Engine], so it DOES expose
//     SourceReplicationCapability — interface-presence ALONE is
//     insufficient to exclude it. Its declared CDC capability is
//     [ir.CDCTriggers], which is what excludes it.
//   - MySQL sources — REPLICATION-attribute / slot creation is PG-only
//     (binlog / VStream CDC capabilities skip).
//   - Any non-CDC path — a one-shot bulk `migrate` needs only SELECT and
//     genuinely works on Heroku; refusing there would be wrong. This
//     preflight is wired only into the CDC cold-start path, not the pure
//     bulk-migrate path.
//
// No `--allow-missing-replication` opt-out flag is provided: the role
// genuinely cannot create a slot, so deferring to the raw mid-cold-start
// error would only re-surface the confusing failure this preflight
// exists to replace. The slot-less `postgres-trigger` engine IS the
// supported path for roles without REPLICATION.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errReplicationRefused is the sentinel cause for a replication-
// capability preflight refusal. Wrapped with the message naming the
// connecting role and the recovery paths. Tests assert via errors.Is to
// avoid coupling to the message text.
var errReplicationRefused = errors.New("pipeline: replication-capability preflight refused")

// replicationCapabilityProber is the optional surface a slot-based
// Postgres source SchemaReader implements to drive the replication-
// capability preflight.
//
// SourceReplicationCapability reports whether the connecting role can
// create a logical replication slot — i.e. it is a superuser, carries
// the REPLICATION attribute (`pg_roles.rolsuper OR rolreplication`), or
// holds a managed-provider grant the engine recognizes (AWS RDS /
// Aurora `rds_replication` membership). The role name is surfaced in
// the refusal message so the operator knows which role to grant
// REPLICATION to (or which role to swap for).
//
// Defined in the pipeline package rather than `ir` because it is
// orchestrator-private (matches the shape of [rlsPreflightProber]).
type replicationCapabilityProber interface {
	SourceReplicationCapability(ctx context.Context) (canReplicate bool, role string, err error)
}

// preflightSourceReplication runs the replication-capability preflight
// against the source handle. Returns nil when:
//
//   - The source's declared CDC capability is not
//     [ir.CDCLogicalReplication] (the capability gate — excludes
//     postgres-trigger ([ir.CDCTriggers]), MySQL ([ir.CDCBinlog] /
//     [ir.CDCVStream]), and every non-CDC path). This check runs FIRST
//     so postgres-trigger short-circuits before the prober type-assert,
//     since its delegated SchemaReader WOULD satisfy the prober
//     interface.
//   - The handle doesn't implement [replicationCapabilityProber] (a PG
//     surface that doesn't expose the probe — the opportunistic-skip
//     posture matches [preflightRLS]).
//   - The connecting role is a superuser or carries REPLICATION.
//
// Returns a wrapped [errReplicationRefused] when the role can't create a
// slot. The message names the role, explains that slot-based CDC needs
// the REPLICATION attribute (or a recognized provider grant), and lists
// the four operator-actionable recovery paths (grant REPLICATION, use a
// replication-enabled role, the RDS/Aurora `rds_replication` grant, or
// switch to `--source-driver=postgres-trigger`).
func preflightSourceReplication(ctx context.Context, handle any, sourceCaps ir.Capabilities) error {
	// Capability gate FIRST (correctness-critical). postgres-trigger's
	// SchemaReader delegates to the composed postgres.Engine, so it DOES
	// satisfy replicationCapabilityProber — its declared CDCTriggers
	// capability is the only thing that excludes it. MySQL and every
	// non-CDC / bulk-migrate path also short-circuit here: only an
	// engine whose CDC mechanism creates a logical replication slot
	// needs the REPLICATION attribute.
	if sourceCaps.CDC != ir.CDCLogicalReplication {
		return nil
	}
	prober, ok := handle.(replicationCapabilityProber)
	if !ok {
		// PG surface that doesn't expose the probe — silently skip. The
		// opportunistic-skip posture matches preflightRLS.
		return nil
	}

	canReplicate, role, err := prober.SourceReplicationCapability(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: replication-capability preflight: probe source role REPLICATION attribute: %w", err,
		))
	}
	if canReplicate {
		// Superuser, REPLICATION-enabled, or provider-granted (RDS
		// rds_replication membership) — slot creation will succeed.
		return nil
	}

	return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
		"%w: %s",
		errReplicationRefused, formatReplicationRefusal(role),
	))
}

// formatReplicationRefusal renders the operator-facing refusal message.
// The shape mirrors [formatRLSRefusal] — name the concrete state (the
// role), explain the mechanism (REPLICATION attribute / logical slot),
// and list every operator-actionable recovery path so the operator can
// pick the one that fits their hosting tier.
//
// The recovery text is provider-aware (RDS validation F1, 2026-07-16):
// on AWS RDS / Aurora the pre-fix paths were all wrong or lossy —
// `ALTER ROLE ... REPLICATION` requires real superuser (which RDS
// withholds), no superuser/REPLICATION role exists there at all, and
// the trigger-engine fallback forfeits slot CDC on a platform that
// supports it. The probe now recognizes `rds_replication` membership,
// so a refusal on RDS/Aurora means a CUSTOM role lacking the grant —
// and the grant (not the attribute) is the fix there.
func formatReplicationRefusal(role string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "the source connecting role %q is not a superuser, lacks the REPLICATION attribute, "+
		"and is not a member of a recognized managed-provider replication role (AWS RDS/Aurora `rds_replication`). ", role)
	b.WriteString("Slot-based Postgres CDC (`--source-driver=postgres`) creates a logical replication slot at cold start, " +
		"which requires one of those grants; without it, slot creation fails mid-cold-start with " +
		"`ERROR: permission denied to create replication slot` (SQLSTATE 42501). ")
	b.WriteString("Recovery: (a) if you control the server, grant the attribute: `ALTER ROLE ")
	b.WriteString(role)
	b.WriteString(" REPLICATION;`; ")
	b.WriteString("(b) re-run sluice with a superuser or replication-enabled role; ")
	b.WriteString("(c) on AWS RDS / Aurora Postgres (where the REPLICATION attribute is not grantable), connect as the " +
		"master user — it is a member of `rds_replication` at creation — or grant a custom role the membership: `GRANT rds_replication TO ")
	b.WriteString(role)
	b.WriteString(";`; ")
	b.WriteString("(d) on managed Postgres that forbids replication slots outright (Heroku Postgres Essential, " +
		"Render Basic, Supabase free), use `--source-driver=postgres-trigger` — sluice's slot-less trigger-capture " +
		"CDC engine, built for exactly this tier")
	return b.String()
}
