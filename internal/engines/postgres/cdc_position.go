// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/ir"
)

// pgPos is the engine-side representation of a Postgres CDC position.
// JSON-serialised into [ir.Position.Token] when surfaced to the IR;
// the IR contract treats Token as opaque.
//
// Resume requires both core fields:
//
//   - Slot binds the server-side WAL retention. Without the slot the
//     LSN may already have been recycled.
//   - LSN is the confirmed-flush position to resume after, encoded
//     in the canonical Postgres "X/XXXXXXXX" form pglogrepl.LSN
//     emits via String().
//
// The SystemID and Timeline fields pin the source's identity (ADR-0051,
// finding F5 of the PG-internals research). They are populated from
// IDENTIFY_SYSTEM at stream-start; on subsequent reconnects the reader
// re-runs IDENTIFY_SYSTEM and refuses loudly when they diverge —
// otherwise sluice would silently advance LSN values that live in a
// different timeline's reference frame after a source-side PITR or
// standby promotion (silent-loss class).
//
// Both fields are additive: positions persisted by pre-ADR-0051 sluice
// have empty SystemID/Timeline and are accepted unchanged on the first
// reconnect — the pin is installed lazily with a one-time INFO log,
// after which subsequent reconnects must match exactly.
type pgPos struct {
	Slot     string `json:"slot"`
	LSN      string `json:"lsn"`
	SystemID string `json:"systemid,omitempty"`
	Timeline int32  `json:"timeline,omitempty"`
}

// engineNamePostgres is the [ir.Position.Engine] tag this engine
// writes and accepts. Other engines' positions are rejected on decode
// so a MySQL binlog token can't be replayed into a Postgres reader.
const engineNamePostgres = "postgres"

// encodePGPos marshals p into an [ir.Position] suitable for emission
// with an [ir.Change]. Engine is fixed to "postgres".
func encodePGPos(p pgPos) (ir.Position, error) {
	if p.Slot == "" {
		return ir.Position{}, errors.New("postgres: encode cdc position: slot is empty")
	}
	if p.LSN == "" {
		return ir.Position{}, errors.New("postgres: encode cdc position: lsn is empty")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: encode cdc position: %w", err)
	}
	return ir.Position{Engine: engineNamePostgres, Token: string(b)}, nil
}

// decodePGPos parses an [ir.Position] back into a [pgPos]. The zero
// value of [ir.Position] (empty Engine and Token) is the "from now"
// sentinel and is reported via the second return value; callers
// distinguish that case from a bad token by checking ok before err.
func decodePGPos(p ir.Position) (decoded pgPos, ok bool, err error) {
	if p.Engine == "" && p.Token == "" {
		return pgPos{}, false, nil
	}
	if p.Engine != engineNamePostgres {
		return pgPos{}, false, fmt.Errorf(
			"postgres: decode cdc position: engine = %q; want %q",
			p.Engine, engineNamePostgres,
		)
	}
	if p.Token == "" {
		return pgPos{}, false, errors.New("postgres: decode cdc position: token is empty")
	}
	if err := json.Unmarshal([]byte(p.Token), &decoded); err != nil {
		return pgPos{}, false, fmt.Errorf("postgres: decode cdc position: %w", err)
	}
	if decoded.Slot == "" {
		return pgPos{}, false, errors.New("postgres: decode cdc position: slot field is empty")
	}
	if decoded.LSN == "" {
		return pgPos{}, false, errors.New("postgres: decode cdc position: lsn field is empty")
	}
	// Reject malformed LSN strings up-front so a position that round-trips
	// through encode/decode is also a position pglogrepl can use.
	if _, err := pglogrepl.ParseLSN(decoded.LSN); err != nil {
		return pgPos{}, false, fmt.Errorf("postgres: decode cdc position: parse LSN: %w", err)
	}
	return decoded, true, nil
}
