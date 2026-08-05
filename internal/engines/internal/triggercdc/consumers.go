// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// The SOURCE-SIDE CONSUMER REGISTRY (roadmap item 115). A trigger-CDC change log
// is SHARED by every stream reading that database. The ADR-0137 Phase-B
// auto-prune originally cut at ONE stream's durably-applied frontier and deleted
// by change-log `id`, so a slower peer lost everything between the two frontiers
// — reaped before it read them, with no way to discover they existed. That is
// exactly the staged-wave shape the docs recommend (several syncs off one source,
// disjoint table sets, necessarily different speeds).
//
// The fix is a registry every consumer writes its own frontier into, with the
// prune cutting at the MIN across all of them. This file owns the LOAD-BEARING
// DECISION — how a registry snapshot becomes a cut — ONCE, so all three trigger
// engines (pgtrigger, sqlite-trigger, d1-trigger) share one implementation of it
// and cannot drift. The transports own only the SQL that produces the snapshot.

// ConsumerRegistryTable is the source-side registry table name, identical across
// every trigger engine (`sluice_change_log_consumers`). It lives here so the
// engines' installers, the schema readers' control-table roster, and the prune
// path can never drift on the spelling.
const ConsumerRegistryTable = "sluice_change_log_consumers"

// ConsumerRegistrySchemaVer is the `sluice_change_log_meta.schema_version` floor
// at which a change log is known to carry the consumer registry. Version 1 is
// every install shipped before item 115; version 2 adds the registry table.
//
// Both halves of the check matter, and the second is the interesting one: an
// OLDER sluice binary re-running `sluice trigger setup` against a migrated source
// rewrites `schema_version` back to 1 while leaving the registry table in place.
// That is the one on-source trace a pre-registry binary leaves, so requiring the
// version — not just the table — makes "an old binary is in play here" fail
// CLOSED instead of pruning against a registry that binary never writes to.
const ConsumerRegistrySchemaVer = 2

// ErrConsumerRegistryUnavailable is the fail-closed sentinel: the source's change
// log predates the consumer registry (no registry table, or a `schema_version`
// below [ConsumerRegistrySchemaVer]). Callers wrap it with the engine's operator
// hint; the sidecar logs it once per cadence and prunes NOTHING.
var ErrConsumerRegistryUnavailable = errors.New("the source change log predates the consumer registry")

// Consumer is one row of the registry, transport-neutral.
type Consumer struct {
	// ID is the opaque consumer identifier the pipeline composes (stream id +
	// target locator). The engine stores and compares it verbatim.
	ID string

	// AppliedID is that consumer's durably-applied change-log id frontier — the
	// highest id it has committed to its own target. 0 means "nothing applied
	// yet" (a stream still in cold copy), which blocks every prune.
	AppliedID int64

	// AgeSeconds is how long ago the row was last refreshed, measured by the
	// SOURCE database's own clock (so it is immune to skew between the hosts
	// running the syncs). Only used to WARN about a registration that has gone
	// quiet; it never evicts anyone.
	AgeSeconds int64
}

// StaleConsumerAfter is the age past which a registration that is holding the
// prune back is called out at WARN. The registration cadence is one minute, so
// half an hour is ~30 missed refreshes — a stream that is stopped, wedged, or
// decommissioned rather than merely slow.
//
// It is DELIBERATELY only a warning. Auto-evicting a stale consumer would delete
// the rows a stream that is down for maintenance has not read yet — the very
// silent loss the registry exists to prevent — so a stopped stream holds the
// change log until an operator removes its row.
const StaleConsumerAfter = 30 * time.Minute

// RegistryCut turns a registry snapshot into the inclusive DELETE bound for one
// prune, and is the single place the item-115 safety argument lives.
//
// It refuses loudly (deleting nothing) when:
//
//   - the registry is EMPTY. "No rows" and "nobody has registered yet" are the
//     same observable state, and reading either as "no consumers, prune
//     everything" is a WORSE silent-loss bug than the one being fixed. There is
//     no independent evidence available to distinguish them, so this refuses.
//   - selfID is not in the registry. The caller's own registration is what makes
//     the cut safe FOR THE CALLER; a cut derived from peers alone could sit above
//     the caller's own frontier and delete rows it has not applied.
//
// Otherwise the cut is `MIN(applied_id) - keep` over the registry with the
// CALLER'S OWN row replaced by ownFrontier — the frontier it just read from its
// own target. Substituting rather than merely clamping matters in both
// directions: the caller's registry row is a CACHE refreshed on a cadence, so it
// can sit BELOW the truth (a stream that registered 0 at cold start and has since
// applied thousands of rows would otherwise block its own prune until the next
// refresh) or, after a target rollback, ABOVE it. The fresh read is the
// authority for the caller; peers' rows are the only evidence available for
// them and are used as-is. A non-positive cut is a legal no-op (nothing is safely
// reapable yet), reported as cut <= 0.
func RegistryCut(
	ctx context.Context, engine string, consumers []Consumer, selfID string, ownFrontier, keep int64,
) (int64, error) {
	if len(consumers) == 0 {
		return 0, fmt.Errorf(
			"%s: auto-prune: the change-log consumer registry %q is EMPTY — refusing to prune. "+
				"An empty registry is indistinguishable from one no stream has written to yet, "+
				"so pruning here could delete rows a peer sync has not read",
			engine, ConsumerRegistryTable,
		)
	}
	minID, minOwner, minAge, selfSeen := registryMin(consumers, selfID, ownFrontier)
	if !selfSeen {
		return 0, fmt.Errorf(
			"%s: auto-prune: this stream (%q) is not in the change-log consumer registry %q — refusing to prune. "+
				"The registry has %d other consumer(s); a cut derived from peers alone is not a safe bound for this stream",
			engine, selfID, ConsumerRegistryTable, len(consumers),
		)
	}
	if minAge >= int64(StaleConsumerAfter/time.Second) && minOwner != selfID {
		slog.WarnContext(
			ctx,
			engine+": auto-prune: the change-log consumer holding the prune back has not refreshed its "+
				"registration recently — the change log will keep growing until it resumes or its registry row is "+
				"removed. sluice will NOT evict it automatically: its unread rows would be the silent loss the "+
				"registry exists to prevent",
			slog.String("consumer_id", minOwner),
			slog.Int64("applied_id", minID),
			slog.Int64("age_seconds", minAge),
			slog.String("registry_table", ConsumerRegistryTable),
		)
	}
	return minID - keep, nil
}

// RegistryMin is the non-refusing half of the same rule, for the operator-run
// `sluice trigger prune`: it reports the MIN frontier across the registry with
// the calling stream's own row replaced by selfFrontier. ok is false for an EMPTY
// registry — "no evidence", which that path treats as "keep the pre-registry
// behaviour" rather than as a refusal (refusing an explicit operator action that
// has been safe for a single stream since ADR-0137 Phase A would be a
// regression; clamping when there IS evidence is a strict improvement).
//
// An empty selfID means the caller has no identity in the registry (a direct
// engine call with no stream in play): nothing is substituted and every row
// counts as a peer.
func RegistryMin(consumers []Consumer, selfID string, selfFrontier int64) (minID int64, ok bool) {
	if len(consumers) == 0 {
		return 0, false
	}
	minID, _, _, _ = registryMin(consumers, selfID, selfFrontier)
	return minID, true
}

// registryMin computes the MIN over the registry with selfID's row replaced by
// selfFrontier, and reports who holds it, how stale that row is, and whether
// selfID was present at all. Shared by [RegistryCut] and [RegistryMin] so the
// automatic and operator paths cannot drift on what "the slowest consumer" means.
func registryMin(consumers []Consumer, selfID string, selfFrontier int64) (minID int64, minOwner string, minAge int64, selfSeen bool) {
	var found bool
	for _, c := range consumers {
		applied, age := c.AppliedID, c.AgeSeconds
		if c.ID == selfID {
			selfSeen = true
			applied, age = selfFrontier, 0
		}
		if !found || applied < minID {
			minID, minOwner, minAge, found = applied, c.ID, age, true
		}
	}
	return minID, minOwner, minAge, selfSeen
}
