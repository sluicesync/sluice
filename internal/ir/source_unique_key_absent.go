// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"fmt"
	"strings"
)

// ErrSourceUniqueKeyAbsent classifies the refusal of a stream whose target
// holds a unique key the source does not enforce
// (UNIQUE-KEY-STOPPED-WINDOW-DOOR). Every insert reaches a sluice target as
// an upsert, and an upsert resolves a conflict through ANY unique key the
// target has, so the source's next row that duplicates such a key would
// overwrite a different row at exit 0 (SOURCE-DROPPED-UNIQUE-KEY-MERGE).
// The apply-time conflict-identity guard (UNIQUE-KEY-CONFLICT-OTHER-ROW)
// cannot see every such merge — not on a table with no primary key and one
// unique key, nor between rows that differ only in a NULL or partial-index
// column — so the stream compares the two catalogs instead: at every
// stream open, and periodically on a source whose change stream carries no
// DDL.
//
// The comparison is a pure function of the two catalogs, so a restart reads
// both back unchanged and refuses again until the operator drops the key
// on the target, re-copies, or accepts the key by name: it is in the fleet
// supervisor's do-not-restart roster.
var ErrSourceUniqueKeyAbsent = errors.New("SOURCE-UNIQUE-KEY-ABSENT")

// UniqueKeyDoorWhen says which check raised the refusal; the remedy differs.
type UniqueKeyDoorWhen int

const (
	// UniqueKeyDoorAtOpen is the check at stream open, before any change of
	// this run is applied.
	UniqueKeyDoorAtOpen UniqueKeyDoorWhen = iota
	// UniqueKeyDoorMidStream is the periodic re-check on a source whose
	// change stream carries no DDL; changes applied since the source's
	// DROP, up to one re-check interval of them, may already have merged.
	UniqueKeyDoorMidStream
	// UniqueKeyDoorAddTable is `sluice schema add-table`, before the table
	// is copied or handed to the stream.
	UniqueKeyDoorAddTable
)

// AbsentUniqueKey is one target unique key no source key implies.
type AbsentUniqueKey struct {
	// Table is the target table, qualified with its namespace when the
	// stream spans several.
	Table string
	// Key is the key's name on the target ("" for an unnamed one).
	Key string
	// Columns renders the key's parts: "(k)", "(lower(email))".
	Columns string
	// Constraint reports a key a table constraint owns (a Postgres UNIQUE
	// or PRIMARY KEY constraint), which `DROP INDEX` cannot drop: the
	// refusal marks it so the remedy names `ALTER TABLE … DROP CONSTRAINT`.
	Constraint bool
}

// SourceUniqueKeyAbsentError is the refusal. Engine-neutral: the pipeline
// raises it from two schema reads.
type SourceUniqueKeyAbsentError struct {
	Keys []AbsentUniqueKey
	When UniqueKeyDoorWhen
	// SourceSeesDDLLive reports a source whose change stream refuses a
	// unique-key DROP it reads (pgoutput, the MySQL binlog), so a DROP found
	// at stream open was made while the stream was stopped and nothing
	// after it has been applied.
	SourceSeesDDLLive bool
	// RecheckInterval is the mid-stream re-check's period, for the message.
	RecheckInterval string
}

func (e *SourceUniqueKeyAbsentError) Error() string {
	keys := make([]string, len(e.Keys))
	for i, k := range e.Keys {
		name := k.Key
		if name == "" {
			name = "(unnamed)"
		}
		keys[i] = k.Table + "." + name + " " + k.Columns
		if k.Constraint {
			keys[i] += " [a constraint: ALTER TABLE " + k.Table + " DROP CONSTRAINT " + name + "]"
		}
	}
	return fmt.Sprintf("%s: the target has %d unique key(s) the source does not enforce: %s. Every insert reaches "+
		"the target as an upsert that resolves a conflict through any unique key the target has, so the source's next "+
		"row duplicating one of these keys would overwrite a different row at exit 0. The usual cause is a source "+
		"DROP INDEX (or DROP CONSTRAINT) the stream did not see — made while it was stopped, or on a source whose "+
		"change stream carries no DDL — or a target created with a key the source never had. %s",
		ErrSourceUniqueKeyAbsent, len(e.Keys), strings.Join(keys, ", "), e.remedy())
}

// sourceUniqueKeyAcceptClause is the acknowledgement half of every remedy.
const sourceUniqueKeyAcceptClause = "If a key is one the target should enforce beyond the source (one the target " +
	"was built with on purpose), accept it by name with `--accept-target-only-unique-key <table>.<key>` (repeatable) " +
	"on every start; the apply-time guard (UNIQUE-KEY-CONFLICT-OTHER-ROW) still refuses a merge through it wherever " +
	"it can tell the rows apart."

// sourceUniqueKeyDropClause is the drop-the-key remedy.
const sourceUniqueKeyDropClause = "drop each named key on the target (`DROP INDEX`, or `ALTER TABLE … DROP " +
	"CONSTRAINT` for a constraint) and start again with the same `--stream-id`"

// sourceUniqueKeyRecopyClause is the remedy for rows that may already have
// merged: dropping and re-creating the tables from the source removes both
// the key and any row a merge left behind (on a table with no primary key
// the merged row carries the other row's identity). Both operator re-copies
// do it on every source: --reset-target-data drops the tables, and an
// operator --restart-from-scratch clears them too (streamer_coldstart.go,
// freshCopyOperatorRestart.clearsTarget — even for VStream, whose snapshot
// reader is the one ir.IdempotentCopyReader and whose AUTOMATIC re-snapshot
// alone upserts onto the existing rows; Bug 320). Both measured converging
// from this refusal (TestStreamer_UniqueKeyDoor_PostgresSource's clean
// re-copy cells, TestStreamer_UniqueKeyDoor_TriggerCDC).
const sourceUniqueKeyRecopyClause = "re-copy from a clean target: `sluice sync start --restart-from-scratch` " +
	"or `--reset-target-data --yes`, with the same `--stream-id` (" + PgoutputSlotDropClause + "); both drop and " +
	"re-create the stream's target tables from the source, these keys and any merged row with them (destructive)."

func (e *SourceUniqueKeyAbsentError) remedy() string {
	switch e.When {
	case UniqueKeyDoorAddTable:
		return "Refusing before the table is copied or handed to the stream. Drop each named key on the target table " +
			"and run add-table again, or accept it by name with `--accept-target-only-unique-key <table>.<key>`."
	case UniqueKeyDoorMidStream:
		interval := e.RecheckInterval
		if interval == "" {
			interval = "one re-check interval"
		}
		return "Found by the periodic re-check, which a source whose change stream carries no DDL needs: changes " +
			"applied since the source's DROP — up to " + interval + " of them — may already have merged, and dropping " +
			"the key alone would keep those rows. Stopped before the next change is applied. " +
			strings.ToUpper(sourceUniqueKeyRecopyClause[:1]) + sourceUniqueKeyRecopyClause[1:] + " " + sourceUniqueKeyAcceptClause
	}
	if e.SourceSeesDDLLive {
		return "Refusing at stream open, before any change of this run is applied. This source's change stream " +
			"refuses a unique-key DROP it reads, so the source dropped the key while the stream was stopped and nothing " +
			"written after the DROP has been applied: " + sourceUniqueKeyDropClause + ". If a sluice v0.161.0 or " +
			"earlier was streaming from a Postgres source when its plain unique index was dropped (those releases did " +
			"not see that DROP), rows may already have merged; " + sourceUniqueKeyRecopyClause + " " + sourceUniqueKeyAcceptClause
	}
	return "Refusing at stream open, before any change of this run is applied. This source's change stream carries " +
		"no DDL, so if the source dropped the key while a stream was RUNNING, changes written after the DROP may already " +
		"have merged on the target, and dropping the key alone would keep them; " + sourceUniqueKeyRecopyClause +
		" If the stream was stopped across the DROP (or the key was never the source's), nothing after it was " +
		"applied: " + sourceUniqueKeyDropClause + ". " + sourceUniqueKeyAcceptClause
}

// Unwrap exposes the sentinel.
func (e *SourceUniqueKeyAbsentError) Unwrap() error { return ErrSourceUniqueKeyAbsent }

// Terminal reports that a retry reads the same two catalogs.
func (e *SourceUniqueKeyAbsentError) Terminal() bool { return true }
