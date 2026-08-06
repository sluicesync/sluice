// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package namecollide is the shared bookkeeping behind the target-side
// "two schema objects, one identifier" refusals (roadmap items 120 and 134).
//
// # The class
//
// Every engine sluice writes to emits its objects with an `IF NOT EXISTS`
// form — load-bearing for idempotent resume and, on Postgres, for ADR-0114's
// whole-phase reparent retry. That makes a name collision SILENT: the second
// CREATE is a no-op, the target permanently keeps whichever object was
// created first, and the run exits 0. When the loser is a UNIQUE index the
// target then accepts duplicate rows the source rejects, forever.
//
// Two engines reach it by different routes. Postgres index names are
// schema-scoped while MySQL's are table-scoped, so sluice prepends `<table>_`
// — a transformation that is not injective. SQLite needs no transformation at
// all: its index names are schema-scoped as written, so two tables' identically
// named indexes collide directly, and its identifiers fold ASCII case on top.
//
// # What is shared and what is NOT
//
// Shared: the bookkeeping — fold the identifier the engine will actually emit,
// remember the FIRST claimant, report it when a later one lands on the same
// key. First-claim-wins matches the emit order, so the claimant this reports
// is the object that would have survived on the target.
//
// Deliberately NOT shared: the refusal WORDING. What makes two names collide,
// and what the operator must do about it, differ per engine — Postgres has to
// explain a transform the operator never sees, SQLite has to explain that the
// name is schema-scoped and case-folded. A message parameterised over both
// would say less than either. Callers own [sluicecode.Wrap] and the text; this
// package owns only the map.
package namecollide

// Set accumulates claims on one target's flat object-name namespace.
//
// The type parameter is the caller's own origin record — Postgres carries a
// table+index pair, SQLite carries an object kind as well — so each engine can
// format its message from exactly the fields it needs without this package
// modelling either.
//
// The zero value is not usable; call [New].
type Set[T any] struct {
	fold func(string) string
	seen map[string]T
}

// New returns an empty [Set] whose keys are produced by fold.
//
// fold is how the TARGET compares two identifiers, not how Go does. Postgres
// passes an identity function (sluice always emits quoted identifiers, which
// Postgres compares byte-exactly); SQLite passes an ASCII lower-caser, because
// SQLite folds ASCII case in identifier comparison even inside double quotes —
// `CREATE INDEX IF NOT EXISTS "IDX_V"` silently no-ops against an existing
// `idx_v`, ground-truthed on modernc.org/sqlite. A nil fold is treated as the
// identity, so a caller that forgets is byte-exact rather than panicking.
func New[T any](fold func(string) string) *Set[T] {
	if fold == nil {
		fold = func(s string) string { return s }
	}
	return &Set[T]{fold: fold, seen: make(map[string]T)}
}

// Claim records ident as claimed by owner. It returns the PRIOR claimant and
// true when ident is already taken — in which case owner is NOT recorded, so a
// third object colliding on the same identifier still reports the original.
func (s *Set[T]) Claim(ident string, owner T) (prior T, collided bool) {
	key := s.fold(ident)
	if prior, collided = s.seen[key]; collided {
		return prior, true
	}
	s.seen[key] = owner
	var zero T
	return zero, false
}
