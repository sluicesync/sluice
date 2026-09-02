// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// The pgtrigger half of the source-side CONSUMER REGISTRY (roadmap item 115).
// The registry table, the version floor, and the cut decision are shared with
// every trigger engine (internal/triggercdc/consumers.go); this file owns only
// the PG SQL that reads and writes it.

// ChangeLogConsumersTable is the source-side registry of change-log consumers.
// Every trigger-CDC stream reading this database records its own durably-applied
// frontier here; the auto-prune cuts at the MIN across all of them, so no
// consumer's unread rows are ever reaped. The name is owned by the shared
// package so the installer, the schema readers' control-table roster, and the
// prune can never drift on the spelling.
const ChangeLogConsumersTable = triggercdc.ConsumerRegistryTable

// consumersRef renders the schema-qualified, quoted registry table reference.
func (r *CDCReader) consumersRef() string {
	return quoteIdent(r.schema) + "." + quoteIdent(ChangeLogConsumersTable)
}

// RegisterChangeLogConsumer implements [ir.ChangeLogConsumerRegistry]: it
// records/refreshes this stream's durably-applied frontier in the source-side
// registry so every OTHER stream's prune can see it. Called on a cadence by
// every pgtrigger stream — whether or not that stream opted into auto-prune,
// because a peer that never registers is a peer the pruner cannot see.
//
// It runs on the reader's POLL pool rather than the auto-prune pool: the upsert
// is a single short statement on a minute cadence, and a stream that never
// prunes must not pay for a second pool just to be visible.
//
// An EMPTY token registers a frontier of 0 ("nothing durably applied yet"),
// which blocks every peer's prune — the safe direction, and why registration
// starts as soon as the source stream opens rather than at first apply.
func (r *CDCReader) RegisterChangeLogConsumer(ctx context.Context, consumerID, durablePositionToken string) error {
	appliedLastID, err := consumerFrontier(durablePositionToken)
	if err != nil {
		return err
	}
	// applied_id is OVERWRITTEN, never max()'d forward. The writer of a row is
	// the consumer itself, so there is no rogue-writer to defend against — while
	// a frontier that legitimately moves BACKWARD (an operator restored an older
	// target and resumed the stream) must be able to lower the registry, or the
	// next prune would cut above what that target has actually applied.
	_, err = r.db.ExecContext(ctx,
		"INSERT INTO "+r.consumersRef()+" (consumer_id, applied_id, updated_at) VALUES ($1, $2, pg_catalog.now()) "+
			"ON CONFLICT (consumer_id) DO UPDATE SET applied_id = EXCLUDED.applied_id, updated_at = pg_catalog.now()",
		consumerID, appliedLastID)
	if err != nil {
		return fmt.Errorf(
			"pgtrigger: register change-log consumer %q: %w (if this source predates the consumer registry, "+
				"re-run `sluice trigger setup` to migrate its change log)",
			consumerID, err,
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
// ([ErrConsumerRegistryUnavailable]), the registry empty, or this stream absent
// from it (both refused by [triggercdc.RegistryCut]).
func (r *CDCReader) PruneConsumedChangeLogToRegisteredMin(
	ctx context.Context, consumerID, durablePositionToken string, keep int64,
) (int64, error) {
	ownFrontier, err := AppliedLastID(durablePositionToken)
	if err != nil {
		return 0, err
	}
	db, tableRef, err := r.prunePool(ctx)
	if err != nil {
		return 0, err
	}
	if err := r.requireConsumerRegistry(ctx, db); err != nil {
		return 0, err
	}
	consumers, err := readConsumerRegistry(ctx, db, r.consumersRef())
	if err != nil {
		return 0, err
	}
	cut, err := triggercdc.RegistryCut(ctx, "pgtrigger", consumers, consumerID, ownFrontier, keep)
	if err != nil {
		return 0, err
	}
	if cut <= 0 {
		// Nothing safely below the slowest consumer's frontier minus the margin.
		return 0, nil
	}
	minID, err := pgChangeLogMinID(ctx, db, tableRef)
	if err != nil {
		return 0, fmt.Errorf("pgtrigger: prune: min id: %w", err)
	}
	deleted, done, err := triggercdc.InBatches(
		ctx, minID, cut, pgPruneBatchSize, triggercdc.AutoPruneTickBudget, pgPruneBatch(db, tableRef),
	)
	if err != nil {
		return deleted, fmt.Errorf("pgtrigger: prune: delete: %w", err)
	}
	r.notePruneTick(ctx, db, tableRef, deleted, done)
	return deleted, nil
}

// requireConsumerRegistry is the fail-closed migration gate: the registry table
// must exist AND the change log's recorded schema version must be at or above
// the registry floor. The version half is what catches an OLDER sluice binary
// sharing this source — re-running its `trigger setup` rewrites schema_version
// back to 1 while leaving the table behind, and that binary never registers, so
// its stream would be invisible to this prune.
func (r *CDCReader) requireConsumerRegistry(ctx context.Context, db *sql.DB) error {
	var exists bool
	const q = `
SELECT EXISTS (
    SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = $1
       AND n.nspname = $2
       AND c.relkind = 'r'
)`
	if err := db.QueryRowContext(ctx, q, ChangeLogConsumersTable, r.schema).Scan(&exists); err != nil {
		return fmt.Errorf("pgtrigger: auto-prune: check consumer registry: %w", err)
	}
	if !exists {
		return fmt.Errorf(
			"pgtrigger: auto-prune: %w — %q is absent from schema %q. Re-run `sluice trigger setup` against this "+
				"source to migrate its change log, or drop --auto-prune-change-log: without the registry sluice "+
				"cannot see whether another sync reads this change log, and pruning could delete its unread rows",
			triggercdc.ErrConsumerRegistryUnavailable, ChangeLogConsumersTable, r.schema,
		)
	}
	var ver int
	err := db.QueryRowContext(
		ctx,
		"SELECT schema_version FROM "+quoteIdent(r.schema)+"."+quoteIdent(ChangeLogMetaTable)+" WHERE singleton_pk",
	).Scan(&ver)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pgtrigger: auto-prune: read change-log schema version: %w", err)
	}
	if errors.Is(err, sql.ErrNoRows) || ver < triggercdc.ConsumerRegistrySchemaVer {
		return fmt.Errorf(
			"pgtrigger: auto-prune: %w — %s.schema_version is %d, below the registry floor %d. A sluice older than "+
				"the consumer registry has run `trigger setup` against this source; such a binary streams WITHOUT "+
				"registering, so its sync would be invisible to this prune. Re-run `sluice trigger setup` with this "+
				"version once every sync on this source is upgraded",
			triggercdc.ErrConsumerRegistryUnavailable, ChangeLogMetaTable, ver, triggercdc.ConsumerRegistrySchemaVer,
		)
	}
	return nil
}

// readConsumerRegistry snapshots the registry. The row age is computed by the
// SOURCE's clock (now() - updated_at), so a stale-registration warning cannot be
// thrown off by skew between the hosts running the syncs. ref is the
// already-quoted schema.table reference.
func readConsumerRegistry(ctx context.Context, db *sql.DB, ref string) ([]triggercdc.Consumer, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT consumer_id, applied_id, GREATEST(EXTRACT(EPOCH FROM (pg_catalog.now() - updated_at)), 0)::BIGINT "+
			"FROM "+ref)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: auto-prune: read consumer registry: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []triggercdc.Consumer
	for rows.Next() {
		var c triggercdc.Consumer
		if err := rows.Scan(&c.ID, &c.AppliedID, &c.AgeSeconds); err != nil {
			return nil, fmt.Errorf("pgtrigger: auto-prune: scan consumer registry: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgtrigger: auto-prune: read consumer registry: %w", err)
	}
	return out, nil
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

// pgRegisteredConsumerMin reports the MIN(applied_id) across the registry for a
// one-shot connection (the operator-run `sluice trigger prune` clamp), with the
// calling stream's own row replaced by selfFrontier (see
// [triggercdc.RegistryMin] for why the substitution, not just a clamp). ok is
// false when the source has no registry evidence to consult — that path keeps
// its pre-item-115 behaviour and says so, rather than refusing an operator
// action that has worked since ADR-0137 Phase A.
func pgRegisteredConsumerMin(
	ctx context.Context, db *sql.DB, schema, selfID string, selfFrontier int64,
) (minID int64, ok bool, err error) {
	var exists bool
	const q = `
SELECT EXISTS (
    SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = $1
       AND n.nspname = $2
       AND c.relkind = 'r'
)`
	if err := db.QueryRowContext(ctx, q, ChangeLogConsumersTable, schema).Scan(&exists); err != nil {
		return 0, false, fmt.Errorf("pgtrigger: prune: check consumer registry: %w", err)
	}
	if !exists {
		return 0, false, nil
	}
	ref := quoteIdent(schema) + "." + quoteIdent(ChangeLogConsumersTable)
	consumers, err := readConsumerRegistry(ctx, db, ref)
	if err != nil {
		return 0, false, err
	}
	// Table present but EMPTY ⇒ ok=false: no consumer has registered, so there
	// is nothing to clamp TO. Unlike the automatic path, refusing an explicit
	// operator action here would break the single-stream workflow that has
	// always been safe; reported as "no registry evidence" so the CLI can say so.
	minID, ok = triggercdc.RegistryMin(consumers, selfID, selfFrontier)
	return minID, ok, nil
}
