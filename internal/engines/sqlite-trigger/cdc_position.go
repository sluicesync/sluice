// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"encoding/json"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// engineNamePosition is the [ir.Position.Engine] tag this engine writes and
// accepts. A position produced by another engine is rejected on decode so a
// foreign bookmark can't be replayed into the trigger reader (and vice versa).
const engineNamePosition = EngineName

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

// decodePos parses an [ir.Position] back into a [sqliteTriggerPos]. The zero
// value of [ir.Position] (empty Engine + Token) is the "from now" sentinel and
// is reported via ok=false; callers distinguish it from a malformed token by
// checking ok before err.
func decodePos(p ir.Position) (decoded sqliteTriggerPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return sqliteTriggerPos{}, false, nil
	}
	if p.Engine != engineNamePosition {
		return sqliteTriggerPos{}, false, fmt.Errorf(
			"sqlite-trigger: decode position: engine = %q; want %q",
			p.Engine, engineNamePosition,
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
