// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// checkShardColumnSupport refuses loudly when the operator engaged
// Shape A (`--inject-shard-column NAME=VALUE`) but the target engine
// doesn't implement [ir.ShardColumnSetter] — without the applier-side
// stamp CDC events would land on the consolidated target with the
// discriminator column NULL, then violate the rewritten composite-PK
// NOT NULL constraint, then silently mis-target rows across shards
// on Update/Delete. Pre-flighting here keeps the loud-failure tenet
// (no silent cross-shard corruption) when a future engine ships
// without the surface; the two currently-shipping engines (mysql,
// postgres) both implement it, so this is a defence-in-depth gate
// rather than a routinely-fired refusal.
//
// `target` is a freshly-opened engine handle (typically a
// [ir.ChangeApplier] for sync runs, or a [ir.RowWriter] for migrate
// runs); the check uses the same type-assertion shape the runtime
// wiring uses. Returns nil when the operator hasn't engaged Shape A
// or when the target implements the setter.
func checkShardColumnSupport(target any, shard ShardColumnSpec, contextID string) error {
	if !shard.Engaged() {
		return nil
	}
	if _, ok := target.(ir.ShardColumnSetter); ok {
		return nil
	}
	return fmt.Errorf(
		"%s: target engine does not implement ir.ShardColumnSetter — "+
			"--inject-shard-column %s=%v requires the CDC/bulk-apply path "+
			"to stamp the discriminator onto every row before SQL emission. "+
			"Without it, consolidated CDC events would land with the column "+
			"NULL and either violate the rewritten composite-PK NOT NULL "+
			"constraint or silently mis-target rows across shards (ADR-0048). "+
			"Recovery: pick a target engine that implements the surface "+
			"(today's shipping mysql/postgres both do), or drop "+
			"--inject-shard-column for this stream",
		contextID, shard.Name, shard.Value,
	)
}
