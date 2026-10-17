// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// UniqueKeyFact is what a catalog knows about one unique index that the
// schema IR does not carry, read for the stream's unique-key door
// (UNIQUE-KEY-STOPPED-WINDOW-DOOR) and nowhere else: a schema read feeds
// migrate, backups and schema history, and widening the IR would move
// every one of them.
type UniqueKeyFact struct {
	// Enforcing is false for an index the server does not enforce: a
	// Postgres index left invalid or not ready (an interrupted CREATE or
	// DROP INDEX CONCURRENTLY).
	Enforcing bool
	// NullsNotDistinct is the index's own NULLS NOT DISTINCT (PG 15+),
	// for a plain unique index as well as a constraint-owned one.
	NullsNotDistinct bool
	// PartCollations holds, per key part, the collation the INDEX
	// compares that part under when it differs from the column's own
	// (`CREATE UNIQUE INDEX … (k COLLATE "x")`); "" where it does not.
	PartCollations []string
	// Primary, Parts and Predicate describe the index itself, so the door
	// can judge one the schema read does not list: the Postgres reader's
	// index read covers ordinary tables only, so a PARTITIONED table's keys
	// are absent from the IR (round-2 review L-a). Parts carries a plain
	// column as Column and an expression as Expression (with its dialect).
	Primary          bool
	Parts            []IndexColumn
	Predicate        string
	PredicateDialect string
}

// CollationLoosening names the ways a column's equality is coarser than
// byte equality — which values a unique key on it counts as one.
type CollationLoosening uint8

const (
	// LooseCase means 'A' and 'a' are equal (a MySQL `_ci` collation; a Postgres
	// nondeterministic collation, conservatively).
	LooseCase CollationLoosening = 1 << iota
	// LooseAccent means 'e' and 'é' are equal (a MySQL `_ai` or legacy `_ci`
	// collation; a Postgres nondeterministic collation, conservatively).
	LooseAccent
	// LoosePad means 'a' and 'a ' are equal (a MySQL PAD SPACE collation; a
	// Postgres bpchar column).
	LoosePad
)

// CollationLooseningResolver is the optional Engine surface that reports a
// character column type's [CollationLoosening] in that engine's own
// vocabulary (ok=false when it cannot tell, e.g. no collation recorded).
// The stream's unique-key door compares a target key column's loosening
// with its source column's: a target column that equates values the source
// keeps apart turns two source rows into one (round-2 review MEDIUM-2).
type CollationLooseningResolver interface {
	CollationLoosening(t Type) (CollationLoosening, bool)
}

// UniqueKeyFactReader is the optional SchemaReader surface that serves
// [UniqueKeyFact]s for the tables the reader's last ReadSchema returned,
// keyed by table name then index name. Engines whose catalog carries no
// such facts do not implement it.
type UniqueKeyFactReader interface {
	UniqueKeyFacts(ctx context.Context) (map[string]map[string]UniqueKeyFact, error)
}
