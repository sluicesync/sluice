// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// The sqlite-trigger half of the source-side CONSUMER REGISTRY (roadmap item
// 115) — shared, like every other piece of this engine, with the `d1-trigger`
// transport sibling: both engines' CDC reader IS this package's [CDCReader], so
// the registry reaches D1 through the same [executor] seam as the prune. The
// registry table, the version floor, and the cut decision live in
// internal/triggercdc; this file owns only the transport-neutral flow.

// ChangeLogConsumersTable is the source-side registry of change-log consumers.
// Every trigger-CDC stream reading this database records its own durably-applied
// frontier here; the auto-prune cuts at the MIN across all of them, so no
// consumer's unread rows are ever reaped. Unlike the change-log/meta/columns
// trio (whose spellings live in the `sqlite` package because its schema reader
// once hard-coded the skip set), this name comes straight from the shared
// trigger-CDC package — the readers now skip control tables via the
// appliershared roster, which the name is registered in.
const ChangeLogConsumersTable = triggercdc.ConsumerRegistryTable

// RegisterChangeLogConsumer implements [ir.ChangeLogConsumerRegistry]: it
// records/refreshes this stream's durably-applied frontier in the source-side
// registry so every OTHER stream's prune can see it. Called on a cadence by
// every sqlite-trigger / d1-trigger stream — whether or not that stream opted
// into auto-prune, because a peer that never registers is a peer the pruner
// cannot see.
//
// Like the prune, it runs on a FRESH writable executor rather than the read-only
// poll connection: a held writable local-SQLite connection would pin the WAL
// read-mark (Bug 167), and the D1 executor is a stateless HTTP client whose open
// is free.
//
// An EMPTY token registers a frontier of 0 ("nothing durably applied yet"),
// which blocks every peer's prune — the safe direction, and why registration
// starts as soon as the source stream opens rather than at first apply.
func (r *CDCReader) RegisterChangeLogConsumer(ctx context.Context, consumerID, durablePositionToken string) error {
	appliedLastID, err := consumerFrontier(durablePositionToken)
	if err != nil {
		return err
	}
	exec, err := r.b.openExec(ctx, false)
	if err != nil {
		return fmt.Errorf("%s: register change-log consumer: open: %w", r.b.driver, err)
	}
	defer func() { _ = exec.close() }()
	if err := exec.upsertConsumer(ctx, consumerID, appliedLastID); err != nil {
		return fmt.Errorf(
			"%s: register change-log consumer %q: %w (if this source predates the consumer registry, "+
				"re-run `sluice trigger setup` to migrate its change log)",
			r.b.driver, consumerID, err,
		)
	}
	return nil
}

// PruneConsumedChangeLogToRegisteredMin implements [ir.ChangeLogConsumerRegistry]:
// the item-115 replacement for [CDCReader.PruneConsumedChangeLog] on the
// automatic path. It reaps `id <= min(registry MIN, this stream's frontier) -
// keep` in the same bounded keyset batches under the same tick budget; only the
// derivation of the cut changed.
//
// Fail-closed preconditions, all refusing with NOTHING deleted: the registry
// table absent or the change-log schema version below the floor
// ([triggercdc.ErrConsumerRegistryUnavailable]), the registry empty, or this
// stream absent from it (both refused by [triggercdc.RegistryCut]).
func (r *CDCReader) PruneConsumedChangeLogToRegisteredMin(
	ctx context.Context, consumerID, durablePositionToken string, keep int64,
) (int64, error) {
	ownFrontier, err := AppliedLastID(durablePositionToken)
	if err != nil {
		return 0, err
	}
	exec, err := r.b.openExec(ctx, false)
	if err != nil {
		return 0, fmt.Errorf("%s: prune: open: %w", r.b.driver, err)
	}
	defer func() { _ = exec.close() }()
	if err := requireConsumerRegistry(ctx, r.b.driver, exec); err != nil {
		return 0, err
	}
	consumers, err := exec.readConsumers(ctx)
	if err != nil {
		return 0, fmt.Errorf("%s: auto-prune: read consumer registry: %w", r.b.driver, err)
	}
	cut, err := triggercdc.RegistryCut(ctx, r.b.driver, consumers, consumerID, ownFrontier, keep)
	if err != nil {
		return 0, err
	}
	if cut <= 0 {
		// Nothing safely below the slowest consumer's frontier minus the margin.
		return 0, nil
	}
	minID, err := exec.minChangeLogID(ctx)
	if err != nil {
		return 0, fmt.Errorf("%s: prune: min id: %w", r.b.driver, err)
	}
	deleted, done, err := triggercdc.InBatches(
		ctx, minID, cut, exec.pruneBatchSize(), triggercdc.AutoPruneTickBudget, exec.pruneChangeLogBatch,
	)
	if err != nil {
		return deleted, fmt.Errorf("%s: prune: delete: %w", r.b.driver, err)
	}
	r.notePruneTick(ctx, exec, deleted, done)
	return deleted, nil
}

// requireConsumerRegistry is the fail-closed migration gate: the registry table
// must exist AND the change log's recorded schema version must be at or above
// the registry floor. The version half is what catches an OLDER sluice binary
// sharing this source — re-running its `trigger setup` rewrites schema_version
// back to 1 while leaving the table behind, and that binary never registers, so
// its stream would be invisible to this prune.
func requireConsumerRegistry(ctx context.Context, driver string, exec executor) error {
	exists, ver, err := exec.consumerRegistryState(ctx)
	if err != nil {
		return fmt.Errorf("%s: auto-prune: check consumer registry: %w", driver, err)
	}
	if !exists {
		return fmt.Errorf(
			"%s: auto-prune: %w — %q is absent from the source. Re-run `sluice trigger setup` against this source "+
				"to migrate its change log, or drop --auto-prune-change-log: without the registry sluice cannot see "+
				"whether another sync reads this change log, and pruning could delete its unread rows",
			driver, triggercdc.ErrConsumerRegistryUnavailable, ChangeLogConsumersTable,
		)
	}
	if ver < triggercdc.ConsumerRegistrySchemaVer {
		return fmt.Errorf(
			"%s: auto-prune: %w — %s.schema_version is %d, below the registry floor %d. A sluice older than the "+
				"consumer registry has run `trigger setup` against this source; such a binary streams WITHOUT "+
				"registering, so its sync would be invisible to this prune. Re-run `sluice trigger setup` with this "+
				"version once every sync on this source is upgraded",
			driver, triggercdc.ErrConsumerRegistryUnavailable, ChangeLogMetaTable, ver,
			triggercdc.ConsumerRegistrySchemaVer,
		)
	}
	return nil
}

// consumerFrontier decodes a registration's position token. An EMPTY token is
// the cold-start case ("nothing durably applied yet") and registers 0 rather
// than refusing; every other token goes through the engine's own codec, which
// refuses a FOREIGN token loudly exactly as the prune path does.
func consumerFrontier(durablePositionToken string) (int64, error) {
	if durablePositionToken == "" {
		return 0, nil
	}
	return AppliedLastID(durablePositionToken)
}

// registeredConsumerMin reports the MIN(applied_id) across the registry for the
// one-shot operator prune's clamp, with the calling stream's own row replaced by
// selfFrontier (see [triggercdc.RegistryMin] for why the substitution, not just
// a clamp). ok is false when the source has no registry evidence to consult (no
// table, or no rows) — that path keeps its pre-item-115 behaviour and says so,
// rather than refusing an operator action that has worked since ADR-0137 Phase A.
func registeredConsumerMin(
	ctx context.Context, exec executor, selfID string, selfFrontier int64,
) (minID int64, ok bool, err error) {
	exists, _, err := exec.consumerRegistryState(ctx)
	if err != nil || !exists {
		return 0, false, err
	}
	consumers, err := exec.readConsumers(ctx)
	if err != nil {
		return 0, false, err
	}
	minID, ok = triggercdc.RegistryMin(consumers, selfID, selfFrontier)
	return minID, ok, nil
}
