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
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// Roadmap item 68c (ps-discovery comparison, 2026-07-15). Postgres
// large-object CONTENTS live in pg_largeobject, outside every user
// table; sluice does not copy them, and the blobs are STRANDED on the
// source — a skip that was previously silent.
//
// Deliberately a WARN, never a refusal by itself: the census can't
// prove the blobs matter, only the operator knows. Two volumes:
//
//   - large objects exist AND an in-scope column is typed oid/lo →
//     the full WARN naming the suspect table.column pairs. An oid/lo
//     column is ALSO an unsupported column type the schema read
//     refuses loudly (Bug 205) — which is exactly why this census runs
//     BEFORE ReadSchema: the WARN gives the refusal its large-object
//     context (why the column exists, what the blobs mean, the
//     docs/type-mapping.md recovery recipes) instead of arriving as
//     dead code after the run has already refused;
//   - large objects exist but NO in-scope column could reference them
//     → one quieter WARN (nothing appears to point at them, but the
//     operator should know the los won't travel), and the run
//     proceeds.
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
// schema reader and WARNs when the source holds large objects. It runs
// BEFORE the schema read (Bug 205): the census probes the catalogs
// directly, so it needs no parsed schema, and an in-scope oid/lo
// column — the very suspect shape it names — refuses at ReadSchema as
// an unsupported column type, so a post-read census could never reach
// its named-suspect branch. Suspect scoping therefore rides the
// operator's [migcore.TableFilter] (the same in-scope notion as
// [warnForeignTables]): suspects on excluded tables don't name
// themselves (the quieter WARN still fires — the los exist regardless
// of scope).
//
// Silent no-op when:
//
//   - The source doesn't declare [ir.Capabilities.PostgresBackend]
//     (pg_largeobject is a PG-server concept).
//   - The handle doesn't implement the prober surface.
//   - The probe fails (advisory census — DEBUG log, never an error).
//   - The source has no large objects.
func warnLargeObjects(ctx context.Context, handle any, sourceCaps ir.Capabilities, filter migcore.TableFilter) {
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

	// Scope the suspect columns to the operator's table filter.
	var named []string
	for table, cols := range suspects {
		if !filter.Allows(table) {
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
			"oid/lo — likely large-object references: %s. sluice does not copy large objects, and oid/lo are "+
			"themselves unsupported column types, so the schema read will refuse these columns next. To proceed "+
			"without them, pass --exclude-table for each table; to carry the blobs, convert the columns to inline "+
			"bytea on the source first (ALTER ... TYPE bytea USING lo_get(...)) or export them separately "+
			"(lo_export / pg_dump --blobs) — see docs/type-mapping.md, \"Postgres large objects\"",
		loCount, len(named), strings.Join(named, ", "),
	))
}
