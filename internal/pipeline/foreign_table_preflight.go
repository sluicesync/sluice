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

// Roadmap item 68a (ps-discovery comparison, 2026-07-15). The PG
// schema reader filters information_schema on table_type='BASE TABLE',
// so FDW foreign tables never enter the migration — and until this
// preflight that skip was SILENT: no WARN, no refusal, no signal that
// a table the operator can SELECT from vanished from the plan.
//
// Unlike the Bug 100 partition class this is deliberately a WARN, not
// a refusal: a foreign table holds no LOCAL rows — its data lives on
// the remote server the FDW points at, a SELECT on it does not
// duplicate anything sluice copies, and materializing remote data
// into the target would itself be a surprising (and possibly huge)
// side effect. Skipping is the semantically honest behaviour; the
// wart was the silence. The WARN names every skipped foreign table,
// the server its data actually lives on, and the recovery paths.

// foreignTablePreflightProber is the optional engine-side surface for
// the foreign-table census: name → foreign-server name. PG implements
// it ([postgres.SchemaReader.ForeignTables]); the opportunistic-skip
// posture matches [partitionPreflightProber].
type foreignTablePreflightProber interface {
	ForeignTables(ctx context.Context) (map[string]string, error)
}

// warnForeignTables runs the foreign-table census against the source
// schema reader and WARNs — the run proceeds — when FDW foreign
// tables are being skipped. Returns a non-nil error only on a probe
// failure (the fail-loudly posture matches the partition preflight: a
// census that can't run is a connection problem, not a reason to skip
// detection silently).
//
// Silent no-op when:
//
//   - The source doesn't declare [ir.Capabilities.PostgresBackend]
//     (FDW foreign tables are a PG-server concept).
//   - The handle doesn't implement the prober surface.
//   - The namespace has no foreign tables.
//   - Every foreign table is excluded by the operator's
//     [migcore.TableFilter] (an explicit `--exclude-table=<name>`
//     acknowledges the skip; the WARN would be nagging).
func warnForeignTables(ctx context.Context, handle any, sourceCaps ir.Capabilities, filter migcore.TableFilter) error {
	if !sourceCaps.PostgresBackend {
		return nil
	}
	prober, ok := handle.(foreignTablePreflightProber)
	if !ok {
		return nil
	}
	foreign, err := prober.ForeignTables(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: foreign-table preflight: probe source for foreign tables: %w", err,
		))
	}
	if len(foreign) == 0 {
		return nil
	}

	// Honor the operator's table filter: an excluded foreign table is
	// an acknowledged skip.
	var names []string
	for name := range foreign {
		if filter.Allows(name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)

	var b strings.Builder
	for i, n := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q (server %q)", n, foreign[n])
	}
	slog.WarnContext(
		ctx,
		fmt.Sprintf(
			"skipping %d FDW foreign table(s) in the source schema: %s — a foreign table's rows live on its "+
				"foreign server, not in this database, so sluice does not copy them (materializing remote data "+
				"would be a silent side effect). To carry that data, migrate the foreign server directly as its "+
				"own sluice source, or recreate the FDW wiring (extension + server + user mapping + foreign "+
				"table) on the target; to silence this warning, pass --exclude-table for each name",
			len(names), b.String(),
		),
	)
	return nil
}
