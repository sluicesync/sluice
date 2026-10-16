// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"fmt"
)

// ErrUniqueKeyConflictOtherRow classifies the apply-side refusal of an
// upsert whose conflict matched a DIFFERENT row through one of the target's
// unique keys (SOURCE-DROPPED-UNIQUE-KEY-MERGE). The target's
// `INSERT … ON DUPLICATE KEY UPDATE` (MySQL) or `ON CONFLICT … DO UPDATE`
// (Postgres, on a table whose conflict key is not a primary key) resolves a
// conflict through any unique key it has, and then overwrites the row it
// matched. When that row's primary key differs from the incoming row's (or,
// on a table without one and with two or more unique keys, when it
// disagrees on any unique key's columns), the incoming row is a second row
// of the source, not a re-delivery of the first: the source has no such
// unique key any more (a `DROP INDEX` or `DROP CONSTRAINT` sluice
// does not forward), and applying the upsert would replace one source row
// with another at exit 0.
//
// The row sits after the persisted position, so a restart re-delivers it
// onto the same target key and refuses again: it is in the fleet
// supervisor's do-not-restart roster. The remedy is to drop that key on the
// target, or re-copy the table.
var ErrUniqueKeyConflictOtherRow = errors.New("UNIQUE-KEY-CONFLICT-OTHER-ROW")

// UniqueKeyConflictError is the terminal refusal the appliers raise when an
// upsert's conflict-identity guard fired. Engine-neutral: MySQL builds it
// from the out-of-range error its guard raises, Postgres from the invalid
// cast its guard raises.
type UniqueKeyConflictError struct {
	Schema, Table string
	// Keys renders the target's unique keys the conflict can have come
	// through ("kd_k (k)"), as the engine read them; empty when it could not.
	Keys string
	// Remedy is the engine's advice for the command that hit it.
	Remedy string
	// Cause is the server error the guard raised.
	Cause error
}

func (e *UniqueKeyConflictError) Error() string {
	where := "an upsert"
	if e.Table != "" {
		where = fmt.Sprintf("an upsert into %s.%s", e.Schema, e.Table)
	}
	through := "one of the target's unique keys"
	if e.Keys != "" {
		through = "one of the target's unique keys (" + e.Keys + ")"
	}
	remedy := e.Remedy
	if remedy == "" {
		remedy = "Drop that unique key on the target if the source no longer has it, then restart; or re-copy the table."
	}
	return fmt.Sprintf("%s: %s matched an existing target row through %s, and that row is a different row: "+
		"its primary key (or, on a table without one, one of its unique keys) differs from the incoming row's. The probable "+
		"cause is a unique key the target still has and the source no longer does — a DROP INDEX or DROP "+
		"CONSTRAINT sluice does not forward — so the source now holds both rows and the target can hold only one. "+
		"Applying it would overwrite the existing row with the incoming one. Refusing before it is applied (the "+
		"whole statement rolled back). %s (server: %v)",
		ErrUniqueKeyConflictOtherRow, where, through, remedy, e.Cause)
}

// Unwrap exposes the sentinel and the server error.
func (e *UniqueKeyConflictError) Unwrap() []error {
	return []error{ErrUniqueKeyConflictOtherRow, e.Cause}
}

// Terminal reports that a retry replays the same row onto the same key.
func (e *UniqueKeyConflictError) Terminal() bool { return true }
