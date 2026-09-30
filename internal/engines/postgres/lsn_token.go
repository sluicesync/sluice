// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// lsnFromPositionToken parses the LSN out of a [pgPos] token (the JSON
// blob the applier writes to the control table), for diagnostics that
// want to print a change's LSN without caring about pgPos's layout.
//
// (It once also fed ADR-0020's applied-LSN tracker from the applier's
// commit path. That tracker is gone — the slot ack now follows the
// target's persisted position, read back from the target; GC-41.)
//
// Returns 0 with a nil error when the token is empty or is not a
// Postgres position, and 0 with an error when a pgPos token is
// malformed.
func lsnFromPositionToken(token string) (pglogrepl.LSN, error) {
	if token == "" {
		return 0, nil
	}
	// A non-Postgres source (MySQL / VStream) writes its position as a JSON
	// array of per-shard GTIDs, never a pgPos object, so such a token simply
	// carries no LSN. Recognize the array shape and return 0 silently rather
	// than surfacing it as a parse *failure* (soak finding F2 — MySQL→PG
	// DEBUG log spam). A malformed pgPos *object* token still falls through
	// to the error path below, preserving that genuine-corruption diagnostic
	// for a Postgres source.
	for i := 0; i < len(token); i++ {
		switch token[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			return 0, nil
		}
		break
	}
	decoded, ok, err := decodePGPos(ir.Position{Engine: engineNamePostgres, Token: token})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return pglogrepl.ParseLSN(decoded.LSN)
}
