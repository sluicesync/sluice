// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
	"sluicesync.dev/sluice/internal/ir"
)

// posCodec is this engine's trigger-CDC position codec (ADR-0066 §2). The
// durable bookmark is the most-recently-committed `id` from `sluice_change_log`,
// serialised as the shared `{"last_id":N}` token. pgtrigger accepts ONLY its own
// "postgres-trigger" tag on decode — unlike sqlite-trigger it has no transport
// sibling, so there is no engine-name family to widen (yet). The wire shape and
// the decode/refuse rules are owned by [triggercdc.Codec]; this file is the
// engine adapter, so a vanilla-PG pgoutput LSN still can't be replayed into the
// trigger reader (foreign tags are refused) and the codec's rules live in one
// place for a future `mysql-trigger`.
var posCodec = triggercdc.Codec{
	ErrPrefix:   "pgtrigger",
	WriteEngine: EngineName,
	Accept:      []string{EngineName},
}

// pgTriggerPos is the engine-side position value. It stays a named type so the
// reader/snapshot call sites read as before; it carries only the change-log id.
// The `json:"last_id"` tag keeps it a faithful mirror of the shared wire shape so
// a direct json.Unmarshal of a token into it still works (the codec is the
// production encoder; this tag is the type's own truth).
type pgTriggerPos struct {
	LastID int64 `json:"last_id"`
}

// encodePos marshals p into an [ir.Position] tagged "postgres-trigger".
func encodePos(p pgTriggerPos) (ir.Position, error) {
	return posCodec.Encode(p.LastID)
}

// decodePos parses an [ir.Position] back into a [pgTriggerPos]. ok=false with a
// nil error is the zero-value "from now" sentinel; callers check ok before err.
func decodePos(p ir.Position) (pgTriggerPos, bool, error) {
	id, ok, err := posCodec.Decode(p)
	return pgTriggerPos{LastID: id}, ok, err
}

// AppliedLastID extracts the durably-applied change-log id from a persisted
// trigger-CDC position TOKEN for `sluice trigger prune` (ADR-0137). It refuses an
// empty, malformed, or FOREIGN token loudly. See [triggercdc.Codec.AppliedLastID].
func AppliedLastID(token string) (int64, error) {
	return posCodec.AppliedLastID(token)
}

// PositionAtOrAfter implements [ir.PositionOrderer]: p is at or after anchor
// when its change-log id is. Two ids of one change log are totally ordered —
// the id is an untouched BIGSERIAL, which openCDCReader's sequence preflight
// holds (cdc_gapfree.go) — so the order never answers "unorderable".
//
// It exists for the first-touch schema boundary (cdc_first_touch.go): once a
// stream records schema versions, a warm resume orders them against the
// persisted position (the applier's schema-history prime, the reader seed),
// and an engine with no orderer refuses that resolve outright; chain restore
// places a window's versions among its changes by it. The empty "from now"
// sentinel and a foreign or malformed token refuse loudly, as the postgres
// orderer does.
func (Engine) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pp, ok, err := decodePos(p)
	if err != nil {
		return false, fmt.Errorf("pgtrigger: position-orderer: decode p: %w", err)
	}
	if !ok {
		return false, errors.New("pgtrigger: position-orderer: p is the empty/from-now sentinel, not an orderable position")
	}
	ap, ok, err := decodePos(anchor)
	if err != nil {
		return false, fmt.Errorf("pgtrigger: position-orderer: decode anchor: %w", err)
	}
	if !ok {
		return false, errors.New("pgtrigger: position-orderer: anchor is the empty/from-now sentinel, not an orderable position")
	}
	return pp.LastID >= ap.LastID, nil
}
