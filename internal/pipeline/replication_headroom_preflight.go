// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Replication-headroom preflight (roadmap item 68d).
//
// A slot-based Postgres CDC cold start creates a logical replication
// slot AND attaches a WAL sender. Both draw from bounded server
// resources (`max_replication_slots`, `max_wal_senders`, default 10
// each), and before this preflight a full server failed MID-cold-start
// with the raw `ERROR: all replication slots are in use` (SQLSTATE
// 53400) — after schema-read and preflight work, with no inventory of
// what occupies the ceiling and no pointer at the tooling that frees
// leftovers. The ADR-0175/0176 per-stream-publication world makes
// multi-slot sources the DOCUMENTED pattern (staged waves run one slot
// per stream), so exhausting the default ceiling with leftover slots
// from finished waves is a realistic operator state, not an exotic one.
//
// Posture (the three load-bearing rules):
//
//   - COLD START ONLY. A warm resume (and the interrupted-COPY resume)
//     reuses the stream's EXISTING slot — it consumes no new slot, so
//     probing there could only false-refuse a healthy resume on a full
//     server. The wiring in [Streamer.coldStartReadSourceSchema] skips
//     the probe when resuming.
//   - ADVISORY DEGRADE on probe failure. The census reads stats views a
//     managed platform could restrict; a probe error WARNs and
//     continues (never a new hard failure on a path that worked
//     before). The loud refusal fires only on a SUCCESSFUL probe that
//     proves the ceiling — a full server then still fails at slot
//     create, loudly, exactly as before.
//   - Capability-gated like [preflightSourceReplication]: only a source
//     whose declared CDC mechanism is [ir.CDCLogicalReplication]
//     creates a slot. postgres-trigger ([ir.CDCTriggers]) delegates the
//     same SchemaReader — so interface presence alone would NOT exclude
//     it — and MySQL / bulk-migrate paths never reach the wiring.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// headroomSlotsShown caps how many existing slots the refusal message
// spells out; the remainder is summarized as a count (`sluice slot
// list` shows them all).
const headroomSlotsShown = 8

// replicationHeadroomProber is the optional surface a slot-based
// Postgres source SchemaReader implements to drive the replication-
// headroom preflight. Orchestrator-private, matching the shape of
// [replicationCapabilityProber].
type replicationHeadroomProber interface {
	SourceReplicationHeadroom(ctx context.Context) (ir.ReplicationHeadroom, error)
}

// headroomRemedyHint is the machine-readable remedy carried on the
// coded refusal, mirroring the prose in the error message.
const headroomRemedyHint = "free a slot (`sluice slot list`, then `sluice sync decommission --stream-id <id> --yes` " +
	"for a finished stream or `sluice slot drop <name>` for an abandoned leftover), or raise " +
	"max_replication_slots / max_wal_senders on the source (restart required)"

// preflightReplicationHeadroom refuses a fresh slot-creating cold start
// when the source provably has no headroom for one more replication
// slot or WAL sender. Returns nil when:
//
//   - The source's declared CDC capability is not
//     [ir.CDCLogicalReplication] (the capability gate — excludes
//     postgres-trigger, MySQL, and every non-CDC path; runs FIRST, see
//     the file comment).
//   - The handle doesn't implement [replicationHeadroomProber] (the
//     opportunistic-skip posture of every optional prober).
//   - The probe itself fails — WARN and continue (advisory degrade;
//     the refusal only ever fires on a successful probe).
//   - The census shows headroom on both ceilings.
func preflightReplicationHeadroom(ctx context.Context, handle any, sourceCaps ir.Capabilities) error {
	if sourceCaps.CDC != ir.CDCLogicalReplication {
		return nil
	}
	prober, ok := handle.(replicationHeadroomProber)
	if !ok {
		return nil
	}

	h, err := prober.SourceReplicationHeadroom(ctx)
	if err != nil {
		slog.WarnContext(
			ctx, "replication-headroom preflight: census probe failed; continuing WITHOUT the headroom check "+
				"(a full server will still fail loudly at slot creation)",
			slog.String("err", err.Error()),
		)
		return nil
	}

	slotsFull := h.MaxReplicationSlots > 0 && h.SlotsInUse >= h.MaxReplicationSlots
	sendersFull := h.MaxWALSenders > 0 && h.ActiveWALSenders >= h.MaxWALSenders
	if !slotsFull && !sendersFull {
		return nil
	}

	return migcore.WrapWithHint(migcore.PhaseConnect, sluicecode.Wrap(
		sluicecode.CodeCDCReplicationHeadroom,
		headroomRemedyHint,
		fmt.Errorf("pipeline: cold start refused: %s", formatHeadroomRefusal(h, slotsFull, sendersFull)),
	))
}

// formatHeadroomRefusal renders the operator-facing refusal: the
// exhausted ceiling(s) with usage numbers, the existing slot inventory
// (name + active/inactive, capped), and every recovery path.
func formatHeadroomRefusal(h ir.ReplicationHeadroom, slotsFull, sendersFull bool) string {
	var b strings.Builder
	b.WriteString("this stream's CDC cold start would create a replication slot and attach a WAL sender, " +
		"but the source has no replication headroom: ")
	switch {
	case slotsFull && sendersFull:
		fmt.Fprintf(&b, "all %d of max_replication_slots are in use (%d slot(s)) and all %d of max_wal_senders "+
			"are attached (%d active sender(s))",
			h.MaxReplicationSlots, h.SlotsInUse, h.MaxWALSenders, h.ActiveWALSenders)
	case slotsFull:
		fmt.Fprintf(&b, "all %d of max_replication_slots are in use (%d slot(s); max_wal_senders is fine at %d/%d)",
			h.MaxReplicationSlots, h.SlotsInUse, h.ActiveWALSenders, h.MaxWALSenders)
	default:
		fmt.Fprintf(&b, "all %d of max_wal_senders are attached (%d active sender(s); max_replication_slots is fine at %d/%d)",
			h.MaxWALSenders, h.ActiveWALSenders, h.SlotsInUse, h.MaxReplicationSlots)
	}
	if len(h.Slots) > 0 {
		b.WriteString(". Existing slots: ")
		for i, s := range h.Slots {
			if i == headroomSlotsShown {
				fmt.Fprintf(&b, ", and %d more (see `sluice slot list`)", len(h.Slots)-headroomSlotsShown)
				break
			}
			if i > 0 {
				b.WriteString(", ")
			}
			state := "inactive"
			if s.Active {
				state = "active"
			}
			fmt.Fprintf(&b, "%q (%s)", s.Name, state)
		}
	}
	b.WriteString(". Recovery: (a) retire a finished sluice stream's slot — `sluice slot list` to inspect, " +
		"`sluice sync decommission --stream-id <id> --yes` to drop a finished stream's slot + publication + " +
		"control row, or `sluice slot drop <name>` for an abandoned leftover (an INACTIVE sluice_* slot is " +
		"usually a stream stopped mid-migration or never decommissioned — with per-stream publications, " +
		"staged-wave migration runs one slot per stream, so leftovers from finished waves are the common cause); " +
		"(b) raise max_replication_slots / max_wal_senders in postgresql.conf and restart (on managed Postgres, " +
		"use the provider's parameter console). A warm resume of an EXISTING stream is unaffected — it reuses " +
		"its slot and never trips this check")
	return b.String()
}
