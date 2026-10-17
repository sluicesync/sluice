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
}

// UniqueKeyFactReader is the optional SchemaReader surface that serves
// [UniqueKeyFact]s for the tables the reader's last ReadSchema returned,
// keyed by table name then index name. Engines whose catalog carries no
// such facts do not implement it.
type UniqueKeyFactReader interface {
	UniqueKeyFacts(ctx context.Context) (map[string]map[string]UniqueKeyFact, error)
}
