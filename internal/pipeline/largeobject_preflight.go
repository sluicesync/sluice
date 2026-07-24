// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 68c (ps-discovery comparison, 2026-07-15). Postgres
// large-object CONTENTS live in pg_largeobject, outside every user
// table; sluice copies the referencing `oid`/`lo` column values as
// plain integers and the blobs themselves are STRANDED on the source —
// a skip that was previously silent.
//
// Deliberately a WARN, never a refusal: an oid column is also a
// perfectly ordinary integer-ish column, the referencing values copy
// faithfully, and only the operator knows whether the referenced blobs
// matter. Two volumes:
//
//   - large objects exist AND an in-scope column is typed oid/lo →
//     the full WARN naming the suspect table.column pairs and stating
//     the blobs are not copied (docs/type-mapping.md carries the
//     recovery recipes);
//   - large objects exist but NO in-scope column could reference them
//     → one quieter WARN (the census can't prove anything points at
//     them, but the operator should know the los won't travel).
//
// A probe failure skips SILENTLY (DEBUG only): this is an advisory
// census on catalogs a managed platform could restrict, and it must
// never add a failure mode — unlike the foreign-table census, whose
// probe failure is loud, nothing here feeds a refusal decision.

// largeObjectPreflightProber is the optional engine-side surface for
// the large-object census: the count of pg_largeobject_metadata rows
// plus the oid/lo-typed columns per table in the active namespace. PG
// implements it ([postgres.SchemaReader.LargeObjectCensus]); the
// opportunistic-skip posture matches [foreignTablePreflightProber].
type largeObjectPreflightProber interface {
	LargeObjectCensus(ctx context.Context) (loCount int64, suspects map[string][]string, err error)
}

// warnLargeObjects runs the large-object census against the source
// schema reader and WARNs — the run always proceeds — when the source
// holds large objects. schema is the FILTERED schema (post
// --include/--exclude), which scopes the suspect-column report:
// suspects on excluded tables don't name themselves (the quieter WARN
// still fires — the los exist regardless of scope).
//
// Silent no-op when:
//
//   - The source doesn't declare [ir.Capabilities.PostgresBackend]
//     (pg_largeobject is a PG-server concept).
//   - The handle doesn't implement the prober surface.
//   - The probe fails (advisory census — DEBUG log, never an error).
//   - The source has no large objects.
func warnLargeObjects(ctx context.Context, handle any, sourceCaps ir.Capabilities, schema *ir.Schema) {
	if !sourceCaps.PostgresBackend {
		return
	}
	prober, ok := handle.(largeObjectPreflightProber)
	if !ok {
		return
	}
	loCount, suspects, err := prober.LargeObjectCensus(ctx)
	if err != nil {
		slog.DebugContext(
			ctx, "large-object census probe failed; skipping the advisory (no refusal rides on it)",
			slog.String("err", err.Error()),
		)
		return
	}
	if loCount == 0 {
		return
	}

	// Scope the suspect columns to the filtered table set.
	inScope := make(map[string]bool, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			inScope[t.Name] = true
		}
	}
	var named []string
	for table, cols := range suspects {
		if !inScope[table] {
			continue
		}
		for _, col := range cols {
			named = append(named, table+"."+col)
		}
	}
	sort.Strings(named)

	if len(named) == 0 {
		slog.WarnContext(ctx, fmt.Sprintf(
			"the source database holds %d large object(s) (pg_largeobject), which sluice does not copy — "+
				"no in-scope column is typed oid/lo, so nothing in this run appears to reference them; if they "+
				"matter, carry them separately (see docs/type-mapping.md, \"Postgres large objects\")",
			loCount,
		))
		return
	}
	slog.WarnContext(ctx, fmt.Sprintf(
		"the source database holds %d large object(s) (pg_largeobject) and %d in-scope column(s) are typed "+
			"oid/lo — likely large-object references: %s. sluice copies the referencing oid values as plain "+
			"integers but NOT the large objects themselves: on the target those ids point at nothing. If the "+
			"blobs matter, export them separately (lo_export / pg_dump --blobs) or convert the columns to bytea "+
			"on the source first — see docs/type-mapping.md, \"Postgres large objects\"",
		loCount, len(named), strings.Join(named, ", "),
	))
}
