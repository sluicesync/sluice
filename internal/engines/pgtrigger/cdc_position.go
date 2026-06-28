// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"encoding/json"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// engineNamePosition is the [ir.Position.Engine] tag this engine
// writes and accepts. Other engines' positions are rejected on decode
// so a vanilla-PG pgoutput LSN can't be replayed into the trigger
// reader (and vice versa).
const engineNamePosition = EngineName

// pgTriggerPos is the engine-side representation of a polling
// position. ADR-0066 §2: the durable bookmark is the most-recently-
// committed `id` value from `sluice_change_log`. Serialised as JSON
// in [ir.Position.Token] so a future schema bump (e.g. adding a
// per-partition cursor for §5's `--use-partitioning`) doesn't break
// the wire shape — the IR contract treats Token as opaque.
type pgTriggerPos struct {
	// LastID is the change-log id of the last successfully-applied
	// change. The polling reader resumes by scanning rows with
	// id > LastID (filtered through the §2 xmin safety-lag predicate).
	LastID int64 `json:"last_id"`
}

// encodePos marshals p into an [ir.Position]. Engine is fixed to
// "postgres-trigger".
func encodePos(p pgTriggerPos) (ir.Position, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return ir.Position{}, fmt.Errorf("pgtrigger: encode position: %w", err)
	}
	return ir.Position{Engine: engineNamePosition, Token: string(b)}, nil
}

// AppliedLastID extracts the durably-applied change-log id (the
// {"last_id":N} token shape) from a persisted trigger-CDC position
// TOKEN. `sluice trigger prune` (ADR-0137) calls it to derive the prune
// bound from the TARGET's durable frontier — the only safe lower bound.
// It reuses [decodePos] so the wire shape has a single owner; the token
// is stamped with this engine's canonical name first because the TARGET
// re-stamps the position's Engine with its OWN name when it persists the
// source token (so the raw token alone carries the id).
//
// An empty, malformed, or FOREIGN token is a loud error (a durable
// position the prune can trust must decode cleanly and actually be a
// trigger-CDC token — never prune blind against a garbled or wrong-engine
// watermark).
func AppliedLastID(token string) (int64, error) {
	if token == "" {
		return 0, errors.New("pgtrigger: durable position token is empty (no applied watermark)")
	}
	// Require the last_id key. A FOREIGN (non-trigger-CDC) stream's token —
	// a vanilla-PG pgoutput {slot,lsn}, a mysql-gtid set, a broker envelope
	// — would otherwise json.Unmarshal cleanly into {LastID:0} and look like
	// "nothing to prune" against the wrong stream; refuse loudly instead.
	// Decode into a *int64 so an absent key is distinguishable from an
	// explicit 0.
	var probe struct {
		LastID *int64 `json:"last_id"`
	}
	if err := json.Unmarshal([]byte(token), &probe); err != nil || probe.LastID == nil {
		return 0, errors.New("pgtrigger: position token has no last_id — the stream is not a trigger-CDC stream")
	}
	d, ok, err := decodePos(ir.Position{Engine: engineNamePosition, Token: token})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("pgtrigger: durable position token is empty (no applied watermark)")
	}
	return d.LastID, nil
}

// decodePos parses an [ir.Position] back into a [pgTriggerPos]. The
// zero value of [ir.Position] (empty Engine + Token) is the "from now"
// sentinel and is reported via the second return value; callers
// distinguish it from a malformed token by checking ok before err.
func decodePos(p ir.Position) (decoded pgTriggerPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return pgTriggerPos{}, false, nil
	}
	if p.Engine != engineNamePosition {
		return pgTriggerPos{}, false, fmt.Errorf(
			"pgtrigger: decode position: engine = %q; want %q",
			p.Engine, engineNamePosition,
		)
	}
	if p.Token == "" {
		return pgTriggerPos{}, false, errors.New("pgtrigger: decode position: token is empty")
	}
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return pgTriggerPos{}, false, fmt.Errorf("pgtrigger: decode position: %w", err)
	}
	if decoded.LastID < 0 {
		return pgTriggerPos{}, false, fmt.Errorf("pgtrigger: decode position: last_id = %d; must be >= 0", decoded.LastID)
	}
	return decoded, true, nil
}
