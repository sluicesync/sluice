// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ShardKeyUpsertProber is the optional target-side surface a [ir.RowWriter]
// implements when it can tell whether a sharded target's ROUTING columns are
// contained in the key sluice's idempotent upsert would conflict on.
//
// It returns a human-readable detail string naming the first offending table
// and the columns involved, or "" when every table is safe. An error means the
// probe could not RUN and is never a verdict.
//
// Only the Postgres engine implements it, and only against a PlanetScale Neki
// endpoint; every other writer is skipped and pays nothing.
type ShardKeyUpsertProber interface {
	ShardKeyUpsertMismatch(ctx context.Context, tables []*ir.Table) (detail string, err error)
}

// PreflightShardKeyUpsert refuses, before anything is written, a sharded
// target on which sluice's idempotent write cannot be both accepted and
// correct.
//
// # What it is defending against
//
// sluice's idempotent write is `INSERT … ON CONFLICT (k) DO UPDATE SET <every
// other column>` — the CDC applier and the bulk-copy resume path both use it.
// On a sharded PlanetScale Neki table whose shard key is not in `k`, both
// available spellings fail, and the failures point opposite ways:
//
//   - naming the shard key in the SET list is refused outright (SQLSTATE
//     NK013), on the statement shape, even when the value is unchanged;
//   - leaving it out — the obvious workaround — silently inserts a DUPLICATE
//     of the conflict key whenever the incoming row's shard key differs from
//     the stored row's, because `ON CONFLICT` evaluates its conflict only on
//     the shard the incoming row routes to.
//
// Measured on a live 3-shard cluster 2026-09-10: two rows with `id = 3001`,
// physically resident on different shards, at exit 0 (reported to PlanetScale).
// A CDC replay of an update that changed the shard key produces exactly that,
// and an at-least-once pipeline will eventually emit one.
//
// # Why a refusal and not a degradation
//
// There is no third spelling. The condition that makes the write safe —
// every shard-key column contained in the conflict key — is a property of the
// SCHEMA, so it is knowable before any data moves and fixable by the operator
// (put the shard key in the primary key, which is the standard advice for a
// sharded schema anyway). Proceeding would trade a loud preflight failure for
// silent primary-key duplication discovered later, which is the trade this
// project's tenets exist to refuse.
//
// # Ordering
//
// This runs alongside [PreflightShardPlacement] and is deliberately separate
// from it: that one asks whether a table's rows are where its routing says
// they are (a DATA question, probed behaviourally), this one asks whether the
// routing columns are in the upsert key (a SCHEMA question, read from the
// topology). A target can fail either independently.
func PreflightShardKeyUpsert(ctx context.Context, schema *ir.Schema, rw any) error {
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	prober, ok := rw.(ShardKeyUpsertProber)
	if !ok {
		return nil
	}
	detail, err := prober.ShardKeyUpsertMismatch(ctx, schema.Tables)
	if err != nil {
		return fmt.Errorf("pipeline: preflight shard-key/upsert-key agreement on the target: %w", err)
	}
	if detail == "" {
		return nil
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeTargetShardKeyNotInUpsertKey,
		Hint: "add the shard-key column(s) to the table's PRIMARY KEY on the target (or to a NOT NULL UNIQUE " +
			"index sluice can key on), then re-run; alternatively take the table out of scope with --exclude-table",
		Err: errors.New("pipeline: " + detail +
			"\nsluice's idempotent write is INSERT … ON CONFLICT (key) DO UPDATE, and on a sharded target that " +
			"statement is evaluated only on the shard the incoming row routes to" +
			"\nso a row whose shard key changed would be INSERTED alongside the original instead of updating it, " +
			"leaving two rows with the same key and no error at any point" +
			"\nrefused before anything was written"),
	}
}
