// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"encoding/json"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// engineNamePosition is the [ir.Position.Engine] tag this engine WRITES.
// On decode the codec accepts the whole trigger-CDC FAMILY ([EngineName] and
// its D1 sibling [EngineNameD1]) — see [acceptsPositionEngine] — because both
// transports share this exact change-log-`id` position semantics, and the
// pipeline re-stamps a persisted position's Engine with the source engine's
// own Name() on warm-resume (the Bug-20 cross-engine re-stamp). A `d1-trigger`
// sync therefore presents a position tagged "d1-trigger" on resume; rejecting
// it would make every restart a poison-pill (Bug 166). This mirrors the
// engine-name-family acceptance the MySQL codec needed (Bug 2).
const engineNamePosition = EngineName

// acceptsPositionEngine reports whether a persisted position's Engine tag
// belongs to the trigger-CDC family this codec can decode. Both the local
// `sqlite-trigger` and the `d1-trigger` sibling use the identical
// change-log-id token shape, so a position from either is decodable here.
func acceptsPositionEngine(engine string) bool {
	return engine == EngineName || engine == EngineNameD1
}

// sqliteTriggerPos is the engine-side representation of a polling position
// (ADR-0135 §3). The durable bookmark is the most-recently-applied `id` value
// from `sluice_change_log`. Serialised as JSON in [ir.Position.Token] so a future
// schema bump doesn't break the wire shape — the IR treats Token as opaque, and
// the on-disk JSON shape mirrors pgtrigger's `{"last_id":N}` for operator
// familiarity.
type sqliteTriggerPos struct {
	// LastID is the change-log id of the last successfully-applied change. The
	// polling reader resumes by scanning rows with id > LastID. SQLite's
	// change-log id is INTEGER PRIMARY KEY AUTOINCREMENT and — because SQLite
	// serialises writers (single-writer) — is allocated in COMMIT order, so a
	// plain `id > LastID` scan is gap-free with no safety-lag predicate (unlike
	// PG's bigserial, which can commit out of allocation order; see cdc_reader.go).
	LastID int64 `json:"last_id"`
}

// encodePos marshals p into an [ir.Position]. Engine is fixed to "sqlite-trigger".
func encodePos(p sqliteTriggerPos) (ir.Position, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return ir.Position{}, fmt.Errorf("sqlite-trigger: encode position: %w", err)
	}
	return ir.Position{Engine: engineNamePosition, Token: string(b)}, nil
}

// AppliedLastID extracts the durably-applied change-log id (the {"last_id":N}
// token shape) from a persisted trigger-CDC position TOKEN. `sluice trigger
// prune` (ADR-0137) calls it to derive the prune bound from the TARGET's durable
// frontier — the only safe lower bound. It reuses [decodePos] so the wire shape
// has a single owner; the token is stamped with this engine's canonical name
// first because the TARGET re-stamps the position's Engine with its OWN name
// when it persists the source token (so the raw token alone carries the id).
// Both `sqlite-trigger` and `d1-trigger` share this exact {"last_id":N} shape.
//
// An empty, malformed, or FOREIGN token is a loud error (a durable position the
// prune can trust must decode cleanly and actually be a trigger-CDC token —
// never prune blind against a garbled or wrong-engine watermark).
func AppliedLastID(token string) (int64, error) {
	if token == "" {
		return 0, errors.New("sqlite-trigger: durable position token is empty (no applied watermark)")
	}
	// Require the last_id key. A FOREIGN (non-trigger-CDC) stream's token — a
	// pgoutput {slot,lsn}, a mysql-gtid set, a broker envelope — would otherwise
	// json.Unmarshal cleanly into {LastID:0} and look like "nothing to prune"
	// against the wrong stream; refuse loudly instead. Decode into a *int64 so an
	// absent key is distinguishable from an explicit 0.
	var probe struct {
		LastID *int64 `json:"last_id"`
	}
	if err := json.Unmarshal([]byte(token), &probe); err != nil || probe.LastID == nil {
		return 0, errors.New("sqlite-trigger: position token has no last_id — the stream is not a trigger-CDC stream")
	}
	d, ok, err := decodePos(ir.Position{Engine: engineNamePosition, Token: token})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("sqlite-trigger: durable position token is empty (no applied watermark)")
	}
	return d.LastID, nil
}

// decodePos parses an [ir.Position] back into a [sqliteTriggerPos]. The zero
// value of [ir.Position] (empty Engine + Token) is the "from now" sentinel and
// is reported via ok=false; callers distinguish it from a malformed token by
// checking ok before err.
func decodePos(p ir.Position) (decoded sqliteTriggerPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return sqliteTriggerPos{}, false, nil
	}
	if !acceptsPositionEngine(p.Engine) {
		return sqliteTriggerPos{}, false, fmt.Errorf(
			"sqlite-trigger: decode position: engine = %q; want %q or %q",
			p.Engine, EngineName, EngineNameD1,
		)
	}
	if p.Token == "" {
		return sqliteTriggerPos{}, false, errors.New("sqlite-trigger: decode position: token is empty")
	}
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return sqliteTriggerPos{}, false, fmt.Errorf("sqlite-trigger: decode position: %w", err)
	}
	if decoded.LastID < 0 {
		return sqliteTriggerPos{}, false, fmt.Errorf("sqlite-trigger: decode position: last_id = %d; must be >= 0", decoded.LastID)
	}
	return decoded, true, nil
}
