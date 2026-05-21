// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// ShardColumnSetter is the optional surface a [ChangeApplier] can
// implement to receive the operator-configured Shape-A
// discriminator column (ADR-0048 / `--inject-shard-column
// NAME=VALUE`). The applier's Apply / ApplyBatch path stamps the
// supplied value onto every row-bearing change's `Row`/`Before`/
// `After` map before SQL emission, so consolidated CDC events from
// each per-shard stream carry their disjointness key on the wire
// the same way bulk-copy already does (via the orchestrator-side
// value wrap in `internal/pipeline.shardStampRows`).
//
// Sibling-tier to [RedactorSetter] / [StreamIDSetter] /
// [BatchSizeProviderSetter]: an optional applier surface probed
// via type-assertion; engines that don't implement it inherit
// the no-stamp default (pre-ADR-0048 behaviour). The shipping
// MySQL and Postgres engines both implement it.
//
// Implementations should treat empty Name or nil Value as a
// no-op (clear any prior wiring); a typical applier stores both
// on a per-applier field and consults them inside `dispatch` /
// `applyOne` before reaching the engine-specific INSERT/UPDATE/
// DELETE builder. Idempotent — the streamer may call this on
// every Run.
//
// The Value parameter is `any` (not `string`) to mirror the
// orchestrator-side value wrap's row contract: the discriminator's
// IR type is the operator's choice (today the CLI parses
// `NAME=VALUE` and hands a string; future expansion could thread
// an integer or UUID without changing this surface). The applier
// stamps Value verbatim — type coercion is the writer's job, the
// same way every other row-value lands.
type ShardColumnSetter interface {
	SetShardColumn(name string, value any)
}
