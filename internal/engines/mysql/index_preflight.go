// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// index-representability question, so the orchestrator's
// [ir.IndexEmitPreflighter] gate engages for every MySQL flavor — the
// registry holds Engine VALUES, so the assertion is on the value type.
var _ ir.IndexEmitPreflighter = Engine{}

// PreflightIndexes reports whether every index in s can be represented on a
// MySQL target, WITHOUT a connection and before any data moves (roadmap
// item 118).
//
// MySQL's one unrepresentable index attribute is a PARTIAL predicate on a
// uniqueness-enforcing key: MySQL has no partial-index feature, so the
// widened UNIQUE rejects rows the source holds legally. Every other shape
// this engine cannot carry verbatim — a dropped predicate on a NON-unique
// index, a SPATIAL/FULLTEXT prefix — changes cost, not which rows are legal,
// and is carried with a WARN at the emit site rather than refused. This
// method therefore delegates to [refuseUnrepresentablePredicate], the
// refusal half of the same [checkIndexPredicate] every emit site calls, so
// the early answer and the late one cannot drift.
//
// # Which indexes it reaches, and why exactly these
//
// Every entry of table.Indexes. All three MySQL index-emit sites draw from
// that slice — the shared ADD-clause builder plus the two CREATE TABLE
// inline sites (the GitHub #25 AUTO_INCREMENT supporting key and the Bug 125
// PK-less COPY unique key) — so no per-site skip-set replay is needed. A
// PRIMARY KEY cannot carry a predicate, so table.PrimaryKey is not walked.
//
// The one skip mirrors [emitCreateIndexesCombined]: a SQLite-source index
// whose expression has no MySQL form is WARN-skipped by the build path, so
// refusing it here would abort a migration that succeeds today. Such an
// index reaching an INLINE site is still refused — by the emitter, at CREATE
// TABLE, which is phase one and still before any row moves.
func (Engine) PreflightIndexes(s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: PreflightIndexes: schema is nil")
	}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, idx := range table.Indexes {
			if idx == nil {
				continue
			}
			if _, portable := sqliteIndexPortableMySQL(idx); !portable {
				continue
			}
			if err := refuseUnrepresentablePredicate(idx, "mysql: table "+table.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
