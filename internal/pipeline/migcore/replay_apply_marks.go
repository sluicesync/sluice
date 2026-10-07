// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ClearReplayApplyMarks deletes streamID's ADR-0190 apply marks on the target
// before a replay path that starts OVER applies anything (ADR-0191 §3.4 (2)):
// chain restore at the start of every run, and both `sync from-backup` cold
// starts (`--reset-target-data`, `--at-chain-id`).
//
// Why it is load-bearing for a backup replay. Changes read from a chain carry
// the reader's identities (ADR-0191), so the applier writes and consults marks
// for them. A replay that starts over re-applies the target's content from an
// EARLIER point than any mark was written against — chain restore re-applies
// the full, which puts a key a crashed first attempt had moved (`UPDATE
// k1→k2`, marked on both keys) back at `k1` — and a surviving mark of that
// transaction would then be trusted and SKIP the replayed move: `k1` and `k2`
// both survive, silently. Cleared, the replay decides from nothing and applies
// exactly as before identities were recorded.
//
// An applier without the surface has no marks. The engines' clear is a no-op
// on a mark table this role cannot use (it never consults one either).
func ClearReplayApplyMarks(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	clearer, ok := applier.(ir.ApplyMarksClearer)
	if !ok {
		return nil
	}
	if err := clearer.ClearApplyMarks(ctx, streamID); err != nil {
		return fmt.Errorf("clear the stream's apply marks before the replay starts over: %w", err)
	}
	return nil
}
