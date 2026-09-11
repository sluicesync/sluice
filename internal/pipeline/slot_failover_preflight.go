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
// A single-node cluster with no standby has nothing to fail over to and
// needs none of this either. Same conclusion: advisory only.
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
		slog.String("already_covered_if", "your cluster preserves slots by Patroni permanent slots instead — the "+
			"\"Logical slot name\" field on PlanetScale Postgres. sluice cannot see that from SQL, so this warning "+
			"does not mean you are unprotected; it means sluice cannot confirm that you are"),
		slog.String("see", "docs/postgres-source-prep.md, \"Slot lifetime under failover\""),
	)
}
