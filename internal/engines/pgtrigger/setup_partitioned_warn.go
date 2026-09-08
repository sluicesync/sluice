// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
)

// partitionedParentMarker is the grep-stable handle for the setup-time
// advisory below.
const partitionedParentMarker = "PARTITIONED-PARENT-CAPTURE"

// warnPartitionedParents tells an operator at SETUP time that a declaratively
// partitioned parent in the table set will be refused later, by the pipeline.
//
// # Why this is a WARN here and a REFUSAL there (audit SLP-5)
//
// `trigger setup --tables events` on a partitioned parent SUCCEEDS today, and
// PostgreSQL CLONES the row trigger onto every partition. Measured on real PG
// 16: an INSERT into the parent is then recorded in the change log under the
// PARTITION's name (`events_us`), never the parent's, because a cloned trigger
// runs on the partition and `TG_TABLE_NAME` is the relation it fired on.
//
// The filing read that as misattribution waiting to happen. It is bounded by a
// sibling door the filing did not credit: the pipeline REFUSES a declaratively
// partitioned source table at preflight, naming three recovery routes, so no
// sync ever reaches the mismatch. And in the recovery route people actually
// take — exclude the parent, let the children copy as ordinary heaps — the
// clone names are exactly RIGHT, because the children are the tables in scope.
//
// So the defect is not loss. It is that setup reports success and `sync start`
// then refuses, which is a door existing on one path and not its sibling, and
// the operator learns at the wrong end. This closes that gap where they are
// standing when they can still act on it.
//
// It WARNS rather than refuses on purpose. Installing capture on a parent is
// legitimate preparation for exactly the supported route above, and refusing
// would break a workflow the pipeline goes on to accept. A setup-time refusal
// for something the next command handles correctly would be the false-refusal
// shape this project spends its time removing.
//
// Best-effort by construction: a probe error logs at DEBUG and setup proceeds,
// because an advisory that can fail a working setup has been converted into an
// outage.
func warnPartitionedParents(ctx context.Context, db *sql.DB, schema string, tables []string) {
	if len(tables) == 0 {
		return
	}
	// Every partitioned parent in the schema, intersected in Go against the
	// requested set. Deliberately not an `= ANY($2)` array bind: this package
	// has no array-binding idiom, and a schema's partitioned-parent list is
	// small enough that introducing one would be the more expensive choice.
	const q = `
SELECT c.relname::text
  FROM pg_class     c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1
   AND c.relkind = 'p'
 ORDER BY c.relname`
	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		slog.DebugContext(ctx, "pgtrigger: setup: could not probe for partitioned parents; proceeding",
			slog.String("err", err.Error()))
		return
	}
	defer func() { _ = rows.Close() }()

	want := make(map[string]bool, len(tables))
	for _, t := range tables {
		want[t] = true
	}
	var parents []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			slog.DebugContext(ctx, "pgtrigger: setup: partitioned-parent probe scan failed; proceeding",
				slog.String("err", err.Error()))
			return
		}
		if want[name] {
			parents = append(parents, name)
		}
	}
	if err := rows.Err(); err != nil || len(parents) == 0 {
		return
	}

	slog.WarnContext(
		ctx, "pgtrigger: setup: "+partitionedParentMarker+": "+
			strings.Join(parents, ", ")+" is a declaratively PARTITIONED parent. PostgreSQL clones the "+
			"capture trigger onto every partition, and a cloned trigger records the PARTITION's name "+
			"(events_us), not the parent's — so change rows for this table will carry partition names. "+
			"`sync start` and `migrate` REFUSE a partitioned source table at preflight, so this setup "+
			"will not be usable for the parent as-is; the refusal names the recovery. The route that "+
			"works is to exclude the parent (--exclude-table) and let the partitions copy as ordinary "+
			"tables, and the clone names are correct for exactly that. Setup is not refusing here "+
			"because that route is legitimate and this install prepares it",
		slog.String("schema", schema),
		slog.Int("partitioned_parents", len(parents)),
	)
}
