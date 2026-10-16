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
// disagrees on any unique key's columns — compared NULL-safely, so rows that
// differ only in NULL key columns, or outside a partial index's predicate,
// are not told apart), the incoming row is a second row
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

// UniqueKeyConflictRecopyRemedy is the re-copy half of the refusal's remedy,
// shared by both appliers. An applier cannot tell which command drives it, so
// it names both: `sync start` re-copies with --restart-from-scratch, while
// `sync from-backup run` has no such flag and recovers with
// --reset-target-data (which needs --yes off a terminal; cmd/sluice cli.go).
const UniqueKeyConflictRecopyRemedy = "a re-copy converges — for `sluice sync start`, restart with " +
	"`--restart-from-scratch` and the same `--stream-id` (from a Postgres pgoutput source, first drop the stream's " +
	"replication slot, which a fresh snapshot refuses to inherit: `sluice slot drop <SLOT> " +
	"--source-driver=postgres --source <DSN> --yes`, where <SLOT> is the full name `sluice slot list` prints: " +
	"`sluice_slot` by default, else `sluice_` + --slot-name unless that already starts with `sluice_`); " +
	"for `sluice sync from-backup run`, which has no `--restart-from-scratch`, re-run it with " +
	"`--reset-target-data --yes` (it drops the target tables and restores the chain again; this leg is unmeasured)."

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
