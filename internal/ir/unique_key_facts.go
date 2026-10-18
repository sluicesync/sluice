// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
)

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
	// PartDeterministic holds, per key part, whether the collation the index
	// compares that part under is deterministic (pg_collation
	// .collisdeterministic). The door stamps it onto an ARRAY key column's
	// element, which the schema read leaves unclassified (round-6 review
	// LOW-B): stamping it in the schema read itself would change the emitted
	// DDL and the backup schema hash.
	PartDeterministic []bool
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

// UniqueKeyDefinitionReader is the optional SchemaReader surface that
// serves the server's own definition of one unique key of a table in the
// reader's scope, together with the definition of every column the key
// depends on (its parts, the columns an expression or a predicate reads),
// as one opaque string. The stream's unique-key door hashes it into the
// fingerprint an --accept-target-only-unique-key acceptance carries
// (round-7 review MEDIUM-1): a fingerprint over a list of the dimensions
// sluice knows matter missed one every review round (an array element's
// collation, an expression's, a literal's case, an operator class), so it
// is taken over everything the engine itself says instead, and any change
// to the key or to a column it depends on changes it.
//
// The text is the engine's authoritative rendering with no normalisation
// beyond a fixed order and separator: no case folding, no whitespace or
// literal rewriting. It is compared only with another reading of the same
// key, never parsed. A key the reader cannot find, or cannot fully
// describe, is an error, never a partial definition.
type UniqueKeyDefinitionReader interface {
	UniqueKeyDefinition(ctx context.Context, table, key string) (string, error)
}

// ErrUniqueKeyDefinitionUnsupported is a key whose definition the target
// cannot report completely — a target engine without
// [UniqueKeyDefinitionReader], or a key shape the engine's reader cannot
// describe on that server (a functional key part on PlanetScale or Vitess,
// whose comparison collation lives in a hidden column those servers do not
// expose). Such a key cannot be accepted: the door refuses it, naming the
// reason, and dropping the key is the remedy. A reader wraps it with the
// reason (fmt.Errorf("%w: …", ErrUniqueKeyDefinitionUnsupported)).
var ErrUniqueKeyDefinitionUnsupported = errors.New("the target cannot report this unique key's full definition, so it cannot be accepted")
