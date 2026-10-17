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
// an upsert, and the upsert resolves a conflict through a unique key — on a
// MySQL-family target through any unique key the table has, on Postgres
// through the table's ON CONFLICT key (another key raises an error there) —
// so the source's next row duplicating such a key can overwrite a different
// row at exit 0 (SOURCE-DROPPED-UNIQUE-KEY-MERGE). The apply-time
// conflict-identity guard (UNIQUE-KEY-CONFLICT-OTHER-ROW) cannot see every
// such merge — not on a table with no primary key and one unique key, nor
// between rows that differ only in a NULL or partial-index column — so the
// stream compares the two catalogs instead: at every stream open, at a
// source schema boundary that moves the table's columns or loses a unique key
// with a column (forwarded to the target or not), and periodically on a
// source whose change
// stream carries no DDL.
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
	// UniqueKeyDoorAtSchemaChange is the check at a source schema boundary
	// that loses a unique key with a column (the reader reports it,
	// [SchemaSnapshot.UniqueKeysMoved]) or
	// whose column list differs from the table's previous one — the open
	// door's read for the table's first boundary of the run (round-2 review
	// HIGH-1) — run after the schema-forward intercepts, whether or not they
	// forwarded the change (a forwarded DROP COLUMN can leave a MySQL target
	// a narrower key; one at the first boundary after a cold start is not
	// forwarded, ADR-0091 §3, leaving the target's column and its key).
	// Nothing after the boundary has been applied.
	UniqueKeyDoorAtSchemaChange
)

// AbsentUniqueKey is one target unique key no source key implies.
type AbsentUniqueKey struct {
	// Table is the target table, qualified with its target namespace when
	// the stream names one (a multi-database stream, --target-schema).
	Table string
	// Key is the key's name on the target ("" for an unnamed one).
	Key string
	// Columns renders the key's parts: "(k)", "(lower(email))".
	Columns string
	// Constraint reports a key a table constraint owns (a Postgres UNIQUE
	// or PRIMARY KEY constraint), which `DROP INDEX` cannot drop: the
	// refusal marks it so the remedy names `ALTER TABLE … DROP CONSTRAINT`.
	Constraint bool
	// LooserColumns names the key's columns whose target collation equates
	// values the source column's keeps apart (case, accents or trailing
	// spaces): the source may enforce a key on the same columns, under a
	// stricter equality. The refusal names them, and an ALTER of each to a
	// collation as strict as the source's is one more way to clear it.
	LooserColumns []string
	// Definition and Fingerprint are the key's definition as the target
	// engine reports it now (its first line, truncated, for the message) and
	// the short hash of the whole definition — the key and every column it
	// depends on ([UniqueKeyDefinitionReader]) — that an
	// --accept-target-only-unique-key acceptance must carry. The refusal
	// prints the exact flag value to pass.
	Definition  string
	Fingerprint string
	// DefinitionUnreadable says why the key has no fingerprint (a target
	// engine that cannot report a key's definition): such a key cannot be
	// accepted.
	DefinitionUnreadable string
	// AcceptedFingerprint is the fingerprint an acceptance of this key
	// carries when the key's definition no longer hashes to it (a forwarded
	// DROP COLUMN narrowed it, a collation change loosened it, a column it
	// depends on changed at all).
	AcceptedFingerprint string
}

// SourceUniqueKeyAbsentError is the refusal. Engine-neutral: the pipeline
// raises it from two schema reads.
type SourceUniqueKeyAbsentError struct {
	Keys []AbsentUniqueKey
	When UniqueKeyDoorWhen
	// SourceSeesDDLLive reports a source whose change stream refuses a
	// unique-key DROP it reads (pgoutput, the MySQL binlog), so a DROP found
	// at stream open was made while no stream was reading, and nothing
	// after it has been applied.
	SourceSeesDDLLive bool
	// ShardConsolidated reports an `--inject-shard-column` stream, whose
	// target table every shard shares: dropping and re-creating it would
	// take every shard's rows with it.
	ShardConsolidated bool
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
		if k.AcceptedFingerprint != "" {
			keys[i] += " [accepted with fingerprint " + k.AcceptedFingerprint + ", which is not this key's definition " +
				"now: the key or a column it depends on has changed since it was accepted, and a narrower or looser key " +
				"merges rows the accepted one kept apart]"
		}
		if k.Fingerprint != "" && name != "(unnamed)" {
			keys[i] += " [definition " + k.Definition + "; to accept it as it is now: --accept-target-only-unique-key " +
				k.Table + "." + name + "=" + k.Fingerprint + "]"
		}
		if k.DefinitionUnreadable != "" {
			keys[i] += " [cannot be accepted: " + k.DefinitionUnreadable + "]"
		}
		if len(k.LooserColumns) > 0 {
			keys[i] += " [the target's " + strings.Join(k.LooserColumns, ", ") + " compares case-, accent- or " +
				"trailing-space-insensitively where the source's does not: ALTERing the target column(s) to a collation " +
				"as strict as the source's also clears it]"
		}
	}
	return fmt.Sprintf("%s: the target has %d unique key(s) the source does not enforce: %s. Every insert reaches "+
		"the target as an upsert, which resolves a conflict through a unique key (on a MySQL-family target any the "+
		"table has; on Postgres its ON CONFLICT key), so the source's next row duplicating one of these keys can "+
		"overwrite a different row at exit 0. The usual cause is a source DROP INDEX (or DROP CONSTRAINT) no stream "+
		"saw — made while none was running, or on a source whose change stream carries no DDL — a key column whose "+
		"target collation compares more loosely than the source's, or a target created "+
		"with a key the source never had. %s",
		ErrSourceUniqueKeyAbsent, len(e.Keys), strings.Join(keys, ", "), e.remedy())
}

// sourceUniqueKeyAcceptClause is the acknowledgement half of every remedy.
const sourceUniqueKeyAcceptClause = "If a key is one the target should enforce beyond the source (one the target " +
	"was built with on purpose), accept it with the `--accept-target-only-unique-key <table>.<key>=<fingerprint>` value printed beside the key " +
	"(repeatable), on every start — the fingerprint is of the key's definition as the target reports it, the columns it depends on included, so any later change to the key or to one of those columns (a size-only widening and a server upgrade that respells a type included) refuses again until re-accepted; the apply-time guard (UNIQUE-KEY-CONFLICT-OTHER-ROW) still refuses a merge through it wherever " +
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
// re-copy cells, TestStreamer_UniqueKeyDoor_TriggerCDC). Not under
// --inject-shard-column: see sourceUniqueKeyShardRecopyClause.
const sourceUniqueKeyRecopyClause = "re-copy from a clean target: `sluice sync start --restart-from-scratch` " +
	"or `--reset-target-data --yes`, with the same `--stream-id` (" + PgoutputSlotDropClause + "); both drop and " +
	"re-create the stream's target tables from the source, these keys and any merged row with them (destructive)."

// sourceUniqueKeyShardRecopyClause replaces the re-copy clause on an
// `--inject-shard-column` stream. Its target table is shared by every shard:
// --reset-target-data drops it and every shard's rows with it
// (shard_preflight.go), and its --restart-from-scratch skips the clear
// (streamer_coldstart.go gates the clear on !InjectShardColumn.Engaged()),
// re-copying this shard onto the shared table, keys kept. Unmeasured.
const sourceUniqueKeyShardRecopyClause = "this is an `--inject-shard-column` stream, whose target table every " +
	"shard shares: do NOT use `--reset-target-data`, which drops that table and every shard's rows with it. Drop " +
	"each named key on the target first, then re-copy this shard with `sluice sync start --restart-from-scratch` " +
	"and the same `--stream-id` (" + PgoutputSlotDropClause + "), which re-copies onto the shared table without " +
	"clearing it: it refreshes this shard's rows by key but cannot remove a row a merge left behind."

func (e *SourceUniqueKeyAbsentError) recopy() string {
	if e.ShardConsolidated {
		return sourceUniqueKeyShardRecopyClause
	}
	return sourceUniqueKeyRecopyClause
}

func (e *SourceUniqueKeyAbsentError) remedy() string {
	switch e.When {
	case UniqueKeyDoorAddTable:
		return "Refusing before the table is copied or handed to the stream. Drop each named key on the target table " +
			"and run add-table again, or accept it with the `--accept-target-only-unique-key <table>.<key>=<fingerprint>` value printed beside the key."
	case UniqueKeyDoorAtSchemaChange:
		return "Found at a source schema change that moved the table's columns (a Postgres source drops an index " +
			"outright with a column it covers, while a forwarded DROP COLUMN narrows a MySQL-family key that " +
			"included it, and a DROP COLUMN at the first boundary after a cold start is not forwarded at all, " +
			"leaving the target's column and its key); stopped before anything after the change was applied: " +
			sourceUniqueKeyDropClause + ". " + sourceUniqueKeyAcceptClause
	case UniqueKeyDoorMidStream:
		interval := e.RecheckInterval
		if interval == "" {
			interval = "one re-check interval"
		}
		rc := e.recopy()
		return "Found by the periodic re-check, which a source whose change stream carries no DDL needs: changes " +
			"applied since the source's DROP — up to " + interval + " of them — may already have merged, and dropping " +
			"the key alone would keep those rows. Stopped before the next change is applied. " +
			strings.ToUpper(rc[:1]) + rc[1:] + " " + sourceUniqueKeyAcceptClause
	}
	if e.SourceSeesDDLLive {
		return "Refusing at stream open, before any change of this run is applied. This source's change stream " +
			"refuses a unique-key DROP it reads, and this release also checks the target's keys at every schema " +
			"change that moves a table's columns or loses a unique key with a column, forwarded or not. So if THIS " +
			"release was the one streaming whenever the key could have been dropped, it was dropped while no stream " +
			"was reading (or the target was created with the key) and nothing written after it has been applied: " +
			sourceUniqueKeyDropClause + ". That does not hold for a window an earlier sluice streamed across, where " +
			"rows may already have merged: v0.161.0 and earlier did not see a Postgres plain unique index's DROP, and " +
			"no release through v0.161.1 checked the target's keys at a DROP COLUMN of a key column (including a " +
			"dropped generated key column, and a key column dropped and re-added under its name); then " + e.recopy() +
			" " + sourceUniqueKeyAcceptClause
	}
	return "Refusing at stream open, before any change of this run is applied. This source's change stream carries " +
		"no DDL, so if the source dropped the key while a stream was RUNNING, changes written after the DROP may already " +
		"have merged on the target, and dropping the key alone would keep them; " + e.recopy() +
		" If no stream was running across the DROP (or the key was never the source's), nothing after it was " +
		"applied: " + sourceUniqueKeyDropClause + ". " + sourceUniqueKeyAcceptClause
}

// Unwrap exposes the sentinel.
func (e *SourceUniqueKeyAbsentError) Unwrap() error { return ErrSourceUniqueKeyAbsent }

// Terminal reports that a retry reads the same two catalogs.
func (e *SourceUniqueKeyAbsentError) Terminal() bool { return true }
