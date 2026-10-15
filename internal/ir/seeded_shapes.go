// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "sync"

// GuardSourceShapeSeeder is the optional ChangeApplier surface for a replay
// whose change stream carries no schema version: `sync from-backup` applies
// a chain's incrementals as source events only, so the applier's active
// schema, which the key-collation guard arms from, stays empty and the
// guard never armed (KEY-COLLATION-BROKER-UNARMED, measured 2026-10-15: a
// Postgres UNIQUE key beside the primary key, replayed onto MySQL, merged
// 'A' onto 'a' at exit 0 in steady state, where live `sync start` refused).
//
// SeedSourceShapes hands the applier the source shape of each table the
// next changes belong to. It persists nothing: the shapes are a fallback
// the guard consults only when no schema version is active for a table,
// and a later call replaces an earlier shape for the same table. Call it
// between applies, never while one is running.
type GuardSourceShapeSeeder interface {
	SeedSourceShapes(tables []*Table)
}

// SeededShapes is the store behind [GuardSourceShapeSeeder], shared by the
// appliers so the namespace rule cannot drift between engines. The zero
// value is ready to use.
//
// A shape is found by (namespace, table). A shape that records no namespace
// — every MySQL-family manifest, whose tables are recorded without one
// while its row changes name the database (Bug 306) — also answers for its
// table name under any namespace, unless a shape for the exact namespace
// was seeded.
type SeededShapes struct {
	mu        sync.RWMutex
	qualified map[[2]string]*Table
	bare      map[string]*Table
}

// Seed records tables, replacing any earlier shape for the same table.
func (s *SeededShapes) Seed(tables []*Table) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.qualified == nil {
		s.qualified = map[[2]string]*Table{}
		s.bare = map[string]*Table{}
	}
	for _, t := range tables {
		if t == nil {
			continue
		}
		if t.Schema == "" {
			s.bare[t.Name] = t
			continue
		}
		s.qualified[[2]string{t.Schema, t.Name}] = t
	}
}

// Lookup returns the seeded shape for namespace.table, or nil.
func (s *SeededShapes) Lookup(namespace, table string) *Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.qualified[[2]string{namespace, table}]; ok {
		return t
	}
	return s.bare[table]
}
