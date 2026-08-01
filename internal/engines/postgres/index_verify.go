// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// expectedIndex is one index the index phase was supposed to leave on
// the target: the pg_class relation name sluice actually emitted, the
// `table.index` spelling the operator recognises, and whether the
// source declared it UNIQUE.
type expectedIndex struct {
	ident  string
	source string
	unique bool
}

// VerifyIndexes implements the [ir.IndexVerifier] loud-failure safety net
// for Postgres: after the index phase, every secondary index the build
// TARGETED must exist on the target — and must still be UNIQUE where the
// source declared it UNIQUE — or it returns a SLUICE-E-INDEX-MISSING
// refusal naming the offending `table.index` list.
//
// Postgres is the engine that most needs this net, and until now was the
// one without it. The index build rewrites every statement to
// `CREATE INDEX IF NOT EXISTS` ([SchemaWriter.buildOneIndex]) — which is
// load-bearing for the ADR-0114 whole-phase reparent retry and cannot be
// removed — so ANY reason a build does not happen is a SILENT no-op
// rather than an error. The uniqueness half of the check is not
// decoration: a `CREATE UNIQUE INDEX IF NOT EXISTS` that no-ops onto a
// same-named NON-unique index leaves a target that accepts duplicate rows
// the source rejects, which is exactly the residue a name collision
// leaves behind (that collision is itself refused up front by
// [validatePGIndexNamespace]; this is the belt behind that brace, and it
// also catches an index some other actor pre-created under the same name).
//
// It reuses [indexBuildJobsForTables] so the verified set is EXACTLY the
// set the build path built — same inline-skip carve-out (the Bug 125
// PK-less COPY unique key is emitted at CREATE TABLE, never by the index
// phase) and same constraint-backed exclusion (those are re-created as
// `ADD CONSTRAINT` in the constraints phase). Two further carve-outs are
// PG-specific and have no MySQL counterpart: an index whose name is empty
// (PG auto-names it, so there is nothing to probe for) and an index
// carrying a non-portable SQLite expression, which [emitCreateIndex]
// WARN-skips by design (ADR-0133 follow-up) — probing for either would
// false-flag a healthy migration.
//
// The whole expected set is checked with ONE batched pg_index read, not
// one probe per index: a hundreds-of-index schema on a managed endpoint
// would otherwise pay hundreds of serial round trips at verify time.
func (w *SchemaWriter) VerifyIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: VerifyIndexes: schema is nil")
	}
	wanted := w.expectedIndexes(s)
	if len(wanted) == 0 {
		return nil
	}
	names := make([]string, 0, len(wanted))
	for _, e := range wanted {
		names = append(names, e.ident)
	}
	found, err := probePGIndexes(ctx, w.db, w.schema, names)
	if err != nil {
		return fmt.Errorf("postgres: VerifyIndexes: probe expected indexes: %w", err)
	}

	var missing, notUnique []string
	for _, e := range wanted {
		isUnique, ok := found[e.ident]
		switch {
		case !ok:
			missing = append(missing, e.source)
		case e.unique && !isUnique:
			notUnique = append(notUnique, e.source+" (as "+e.ident+")")
		}
	}
	if len(missing) == 0 && len(notUnique) == 0 {
		return nil
	}

	var detail []string
	if len(missing) > 0 {
		detail = append(detail, fmt.Sprintf("%d absent: %s", len(missing), strings.Join(missing, ", ")))
	}
	if len(notUnique) > 0 {
		detail = append(detail, fmt.Sprintf("%d present but NOT UNIQUE where the source declared UNIQUE: %s",
			len(notUnique), strings.Join(notUnique, ", ")))
	}
	return sluicecode.Wrap(
		sluicecode.CodeIndexMissing,
		"re-run with --resume to rebuild the missing indexes; a present-but-not-unique index means a same-named index already existed on the target — drop it and re-run",
		fmt.Errorf("postgres: VerifyIndexes: the target does not carry every expected secondary index after the index phase (%s)",
			strings.Join(detail, "; ")),
	)
}

// Compile-time proof the SchemaWriter exposes the post-index-phase
// loud-failure verification net (SLUICE-E-INDEX-MISSING) so the
// orchestrator's [ir.IndexVerifier] check engages on both the migrate and
// sync cold-start paths. Mirrored in capabilities_assert.go alongside its
// siblings; kept here too because this file is where a signature drift
// would originate.
var _ ir.IndexVerifier = (*SchemaWriter)(nil)

// expectedIndexes flattens the schema into the set of indexes the build
// phase was supposed to create, resolved to the identifiers the emitters
// actually wrote. See [SchemaWriter.VerifyIndexes] for why each carve-out
// exists.
func (w *SchemaWriter) expectedIndexes(s *ir.Schema) []expectedIndex {
	jobs := w.indexBuildJobsForTables(orderedTables(s))
	out := make([]expectedIndex, 0, len(jobs))
	for _, job := range jobs {
		if job.idx.Name == "" {
			continue
		}
		if _, portable := sqliteIndexPortablePG(job.idx); !portable {
			continue
		}
		out = append(out, expectedIndex{
			ident:  effectivePGIndexIdent(job.tableName, job.idx),
			source: job.tableName + "." + job.idx.Name,
			unique: job.idx.Unique,
		})
	}
	return out
}

// pgIndexProbeQuery reads the existence + uniqueness of a batch of index
// identifiers in the writer's schema. Names are compared BYTE-EXACTLY:
// sluice quotes every emitted identifier, so PG stores it verbatim (no
// MySQL-style case folding applies).
const pgIndexProbeQuery = `
SELECT i.relname, x.indisunique
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = $1 AND i.relname = ANY($2)`

// probePGIndexes returns, for each of names that exists in schema, whether
// it is a UNIQUE index. Absent names are simply absent from the map.
func probePGIndexes(ctx context.Context, db sqlExecQueryer, schema string, names []string) (map[string]bool, error) {
	if schema == "" {
		schema = "public"
	}
	rows, err := db.QueryContext(ctx, pgIndexProbeQuery, schema, names)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]bool, len(names))
	for rows.Next() {
		var (
			name   string
			unique bool
		)
		if scanErr := rows.Scan(&name, &unique); scanErr != nil {
			return nil, scanErr
		}
		out[name] = unique
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
