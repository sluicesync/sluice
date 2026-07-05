// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
	"sluicesync.dev/sluice/internal/ir"
)

// posCodec is this engine's trigger-CDC position codec (ADR-0135 §3). The
// durable bookmark is the most-recently-applied `id` from `sluice_change_log`,
// serialised as the shared `{"last_id":N}` token. On decode it accepts the whole
// trigger-CDC FAMILY — [EngineName] and its D1 sibling [EngineNameD1] — because
// both transports share this exact change-log-`id` semantics and the pipeline
// re-stamps a persisted position's Engine with the source engine's own Name() on
// warm-resume (Bug-20 cross-engine re-stamp). A `d1-trigger` sync therefore
// presents a position tagged "d1-trigger" on resume; rejecting it would make
// every restart a poison-pill (Bug 166). The wire shape and the decode/refuse
// rules are owned by [triggercdc.Codec]; the accepted-name FAMILY is the dialect
// seam, so a future `mysql-trigger` widens cleanly.
var posCodec = triggercdc.Codec{
	ErrPrefix:   EngineName,
	WriteEngine: EngineName,
	Accept:      []string{EngineName, EngineNameD1},
}

// sqliteTriggerPos is the engine-side position value. It stays a named type so
// the reader/snapshot call sites read as before; it carries only the change-log
// id. SQLite's change-log id is INTEGER PRIMARY KEY AUTOINCREMENT and — because
// SQLite serialises writers — is allocated in COMMIT order, so a plain
// `id > LastID` scan is gap-free with no safety-lag predicate (unlike PG's
// bigserial, which can commit out of allocation order; see cdc_reader.go). The
// `json:"last_id"` tag keeps it a faithful mirror of the shared wire shape so a
// direct json.Unmarshal of a token into it still works (the codec is the
// production encoder; this tag is the type's own truth).
type sqliteTriggerPos struct {
	LastID int64 `json:"last_id"`
}

// encodePos marshals p into an [ir.Position] tagged "sqlite-trigger".
func encodePos(p sqliteTriggerPos) (ir.Position, error) {
	return posCodec.Encode(p.LastID)
}

// decodePos parses an [ir.Position] back into a [sqliteTriggerPos]. ok=false with
// a nil error is the zero-value "from now" sentinel; callers check ok before err.
func decodePos(p ir.Position) (sqliteTriggerPos, bool, error) {
	id, ok, err := posCodec.Decode(p)
	return sqliteTriggerPos{LastID: id}, ok, err
}

// AppliedLastID extracts the durably-applied change-log id from a persisted
// trigger-CDC position TOKEN for `sluice trigger prune` (ADR-0137). Both
// `sqlite-trigger` and `d1-trigger` share the `{"last_id":N}` shape. It refuses
// an empty, malformed, or FOREIGN token loudly. See [triggercdc.Codec.AppliedLastID].
func AppliedLastID(token string) (int64, error) {
	return posCodec.AppliedLastID(token)
}
