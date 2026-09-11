// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// # The FAILOVER flag is necessary and not sufficient, and nothing said so
//
// Since ADR-0012 sluice creates logical replication slots with
// `FAILOVER true` on PG 17+, specifically because PlanetScale Postgres
// customers were losing slots on failover. That flag marks a slot as
// ELIGIBLE for synchronization to standbys. Something still has to do the
// synchronizing, and on a PG 17+ cluster that is
// `sync_replication_slots = on` plus `hot_standby_feedback = on`.
//
// A slot flagged FAILOVER on a cluster that never syncs it is silently
// primary-local — the exact failure ADR-0012 exists to prevent — and the
// operator has every reason to believe they are covered, because sluice
// set the flag and said nothing.
//
// docs/postgres-source-prep.md has documented all three preservation
// mechanisms for a long time, and tells the operator to "confirm it's
// actually configured before betting your production CDC stream on it".
// Nothing confirmed it. That is a written invariant with no check, which
// this project's own rule says is indistinguishable from one that holds.
// PlanetScale began emailing customers about exactly this in September
// 2026, which is what surfaced it here — the alert on a database of ours
// named both GUCs and gave a deadline.
//
// # Why this WARNs and must never refuse
//
// Two of the three preservation mechanisms are INVISIBLE from SQL.
// Patroni permanent slots — the "Logical slot name" field on PlanetScale
// Postgres — preserve slots without touching `pg_settings`, so a cluster
// reading `off` on both GUCs may be perfectly well configured. A refusal
// here would break working production setups on no evidence, and a warning
// that asserts breakage would be almost as bad. The wording therefore
// states what sluice can see, names the mechanism it cannot, and leaves
// the judgement with the operator.
//
// A single-node cluster with no standby has no standby to be promoted, so
// the classic failover case cannot arise — but "single-node therefore
// nothing to fix" is an UNVERIFIED PREMISE and is deliberately not claimed.
// A cluster RESIZE plausibly replaces the node, which would have the same
// effect on a primary-local slot as a failover, and that is untested: the
// PlanetScale API refuses a Postgres branch resize outright (`Resize is not
// supported`, confirmed slot-independent by a zero-slot control on
// 2026-09-10), so it could not be measured from here. The message
// therefore offers single-node as a reason the warning MAY not apply,
// never as a guarantee.
//
// Either way the case is NOT detectable here.
// `pg_stat_replication` shows non-privileged roles only their own rows, so
// an empty result is equally consistent with "no standby" and "a standby
// this role cannot see"; suppressing on it would silence the advisory on
// exactly the HA clusters that need it. So single-node is named in the
// message as a reason to dismiss it, rather than guessed at in code. Same
// conclusion throughout: advisory only.
//
// # SCOPE: these are STANDBY-side settings, read here on the primary
//
// `sync_replication_slots` governs a physical standby synchronizing
// failover slots FROM its primary, and `hot_standby_feedback` is likewise
// applied on the standby. sluice reads them on the connection it already
// has, which is the primary.
//
// On a managed platform that applies one cluster configuration to every
// node — PlanetScale, and the alert that prompted this names "cluster
// parameters ... in your cluster configuration" — the primary's value IS
// the cluster's value, and the read is sound. On a hand-rolled
// primary/standby pair whose configs are maintained separately it is a
// proxy that can be wrong in BOTH directions: a primary reading `off` next
// to a correctly-configured standby warns for nothing, and a primary
// reading `on` next to a standby that is not can leave the advisory silent
// when it should speak.
//
// That second direction is the one that matters, and it is the reason this
// stays a WARN with no gate built on top of it. Reading the standby's own
// settings would need a connection to the standby, which sluice does not
// have and should not require.
type slotFailoverProber interface {
	SourceSlotFailoverPosture(ctx context.Context) (ir.SlotFailoverPosture, error)
}

// preflightSlotFailover emits an advisory WARN when sluice is about to
// depend on a logical replication slot that the source's own settings will
// not carry across a failover. It never returns an error — the return type
// is error-free deliberately, so no future caller can turn it into a gate
// without reading the doc above.
//
// Silent when: the source is not a logical-replication CDC source; the
// handle cannot probe; the probe fails; the server predates PG 17 (where
// the FAILOVER option does not exist and slot_create.go already warns on
// its own); or native slot sync is properly configured.
func preflightSlotFailover(ctx context.Context, handle any, sourceCaps ir.Capabilities) {
	if sourceCaps.CDC != ir.CDCLogicalReplication {
		return
	}
	prober, ok := handle.(slotFailoverProber)
	if !ok {
		return
	}
	posture, err := prober.SourceSlotFailoverPosture(ctx)
	if err != nil {
		// A census that cannot run says nothing. It must not become a
		// reason to warn, or every unreadable pg_settings turns into a
		// scary message about data loss.
		slog.DebugContext(ctx, "slot-failover posture probe failed; skipping the advisory",
			slog.String("err", err.Error()))
		return
	}
	// PG ≤ 16 has no FAILOVER option at all; createLogicalReplicationSlot
	// already emits its own one-time warning there, and repeating it here
	// would double the noise for an operator who cannot act on it anyway.
	if posture.ServerVersionNum < 170000 {
		return
	}
	if posture.NativeSlotSyncAvailable() {
		return
	}

	slog.WarnContext(
		ctx,
		"this stream's replication slot may not survive a failover: sluice created it with FAILOVER true, but the "+
			"source's own settings will not synchronize it to a standby, so on a switchover or failover the slot "+
			"can be left behind on the old primary and the stream would need a fresh slot and a re-copy",
		slog.Bool("sync_replication_slots", posture.SyncReplicationSlots),
		slog.Bool("hot_standby_feedback", posture.HotStandbyFeedback),
		slog.String("to_fix", "enable BOTH sync_replication_slots and hot_standby_feedback on the source cluster "+
			"(on PlanetScale Postgres: Cluster configuration > Parameters)"),
		slog.String("already_covered_if", "(a) this is a SINGLE-NODE instance with no standby — no standby can be "+
			"promoted, though note a cluster RESIZE may still replace the node, which would strand the slot the "+
			"same way (untested, so treat single-node as probably-fine rather than certainly-fine); or "+
			"(b) your cluster preserves slots by Patroni permanent slots "+
			"instead — the \"Logical slot name\" field on PlanetScale Postgres. sluice can see neither from SQL "+
			"(pg_stat_replication hides other roles' rows, so an empty one does not prove there is no standby), so "+
			"this warning does not mean you are unprotected; it means sluice cannot confirm that you are"),
		slog.String("see", "docs/postgres-source-prep.md, \"Slot lifetime under failover\""),
	)
}
