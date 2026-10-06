// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// registryPositionOrder is the [backup.PositionOrderResolver] `backup compact`
// injects: the registry's position order for a named source engine. The
// pipeline may not name engines (archgate), so CompactChain calls this with
// the source engine recorded in the catalog it loaded — no second catalog read.
// known is false for an engine this build does not register, which smart
// compaction refuses on; a registered engine without a position order (every
// engine but Postgres today) returns (nil, true), which it proceeds on and
// reports at INFO.
func registryPositionOrder(engine string) (cmp ir.PositionMonotonicChecker, known bool) {
	eng, ok := engines.Get(engine)
	if !ok {
		return nil, false
	}
	cmp, _ = eng.(ir.PositionMonotonicChecker)
	return cmp, true
}
