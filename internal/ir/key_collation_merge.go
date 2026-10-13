// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"fmt"
)

// ErrKeyCollationMerge classifies the apply-side refusal of an upsert that
// would merge two rows the SOURCE keeps apart (KEY-COLLATION-FIRST-BOUNDARY,
// option 3, the applier backstop). The incoming row matched an existing
// target row through a primary key or unique index whose collation equates
// the two key values — `'A'` and `'a'` under `_ci`, `'a '` and `'a'` under a
// PAD SPACE collation — while the source's own collation for that key, as
// its latest schema boundary declares it, tells the two byte strings apart.
// Applying it would replace or overwrite the existing row at exit 0.
//
// The row sits after the persisted position, so a restart re-delivers it and
// the target, unchanged, refuses it again: it is in the fleet supervisor's
// do-not-restart roster. The remedy is to give the target key the source's
// collation (or re-copy the table).
var ErrKeyCollationMerge = errors.New("KEY-COLLATION-MERGE")

// KeyCollationMergeError is the terminal refusal the appliers raise when the
// in-statement guard of an upsert fired. Engine-neutral: both appliers build
// it, MySQL from the server's out-of-range error the guard raises, Postgres
// from the invalid-cast error its guard raises, so the operator reads the
// same marker whatever the server's own text kept.
type KeyCollationMergeError struct {
	Schema, Table string
	// Remedy is the engine's own advice (the DDL that gives the target key
	// the source's collation), written for the commands that resume a
	// stream: `sync start` and `sync from-backup run`. A command that
	// recovers another way replaces it ([WithKeyCollationMergeRemedy]).
	Remedy string
	// Needs names what the target's key column(s) must be declared with for
	// the guard's comparison to be the key's own, as the engine names it
	// ("`k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); empty when the
	// engine cannot name it.
	Needs string
	// RecopyDiverges reports that recreating the table does not converge:
	// sluice creates the key under the target's default collation, which
	// equates the values (a key armed to compare bytes — a Postgres source
	// collation a MySQL target cannot declare). False when a re-copy creates
	// the key as Needs names it, or when the engine cannot tell.
	RecopyDiverges bool
	// Cause is the server error the guard raised.
	Cause error
}

// WithKeyCollationMergeRemedy is the [KeyCollationMergeError] err carries,
// with its remedy replaced by remedy(e) — for a command whose recovery is
// not the engine's (Bug 308: `restore` neither resumes nor accepts
// --restart-from-scratch, so the engine's "ALTER the key and restart, or
// re-copy with --restart-from-scratch" cannot run there). The wrapping
// between the engine and the caller is dropped; the error's own text names
// the table. An err carrying none is returned as is.
func WithKeyCollationMergeRemedy(err error, remedy func(e *KeyCollationMergeError) string) error {
	var e *KeyCollationMergeError
	if !errors.As(err, &e) {
		return err
	}
	cp := *e
	cp.Remedy = remedy(e)
	return &cp
}

func (e *KeyCollationMergeError) Error() string {
	where := "an upsert"
	if e.Table != "" {
		where = fmt.Sprintf("an upsert into %s.%s", e.Schema, e.Table)
	}
	remedy := e.Remedy
	if remedy == "" {
		remedy = "Give the target's key column the source's collation and restart, or re-copy the table."
	}
	return fmt.Sprintf("%s: %s matched an existing target row through a key whose target collation equates "+
		"the two key values, while the source's own collation keeps them apart, so applying it would merge two "+
		"source rows into one. Refusing before it is applied (the whole statement rolled back). %s (server: %v)",
		ErrKeyCollationMerge, where, remedy, e.Cause)
}

// Unwrap exposes the sentinel and the server error.
func (e *KeyCollationMergeError) Unwrap() []error { return []error{ErrKeyCollationMerge, e.Cause} }

// Terminal reports that a retry replays the same row onto the same key.
func (e *KeyCollationMergeError) Terminal() bool { return true }
