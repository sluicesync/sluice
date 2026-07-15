// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// Roadmap item 68b (ps-discovery comparison, 2026-07-15): old-style
// (non-declarative) PG inheritance is the legacy twin of Bug 100's
// partition class, and until this preflight it was a silent-
// DUPLICATION hazard. Inheritance children are ordinary BASE TABLEs
// the reader copies independently — but a SELECT on the parent
// (without ONLY) ALSO returns every child's rows, so an unguarded
// migration lands the child data TWICE: flattened into the parent's
// target heap and again in each child's. Unlike Bug 100 there is no
// pg_partitioned_table row to trip over; the only catalog signal is a
// pg_inherits parent whose relkind is 'r', which is exactly what
// [postgres.SchemaReader.InheritanceParents] probes.
//
// Refuse (not WARN) by the Bug 100 precedent: both outcomes of
// proceeding — duplication (parent + children in scope) or quiet
// hierarchy loss — are data-shape corruption the operator would only
// discover by counting rows.

// errInheritanceTableRefused is the sentinel for the item-68b loud
// refusal. Wrapped with per-table detail.
var errInheritanceTableRefused = errors.New("pipeline: source schema contains old-style PG inheritance parent table(s) — sluice does not support inheritance-aware migration")

// inheritancePreflightProber is the optional engine-side surface for
// detecting old-style inheritance parents. PG implements it
// ([postgres.SchemaReader.InheritanceParents]); the opportunistic-skip
// posture matches [partitionPreflightProber].
type inheritancePreflightProber interface {
	InheritanceParents(ctx context.Context) ([]string, error)
}

// preflightInheritanceTables runs the legacy-inheritance preflight
// against the source schema reader. The gate/skip/in-scope shape
// mirrors [preflightPartitionedTables] exactly: nil on a non-PG
// source, a handle without the prober, a namespace with no
// inheritance parents, or when every parent is excluded via the
// operator's table filter (the schema arg reflects post-filter
// state). Returns a wrapped [errInheritanceTableRefused] when at
// least one inheritance parent is in scope, naming every offending
// parent (sorted by the prober) and the recovery paths.
func preflightInheritanceTables(ctx context.Context, handle any, sourceCaps ir.Capabilities, schema *ir.Schema) error {
	if !sourceCaps.PostgresBackend {
		return nil
	}
	prober, ok := handle.(inheritancePreflightProber)
	if !ok {
		return nil
	}
	parents, err := prober.InheritanceParents(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: inheritance preflight: probe source for inheritance parents: %w", err,
		))
	}
	if len(parents) == 0 {
		return nil
	}

	// Filter against the in-scope table set so an operator who already
	// passed `--exclude-table=<parent>` isn't refused on the table they
	// excluded — same shape as the partition preflight.
	var inScope []string
	if schema != nil {
		inScopeSet := map[string]struct{}{}
		for _, t := range schema.Tables {
			if t == nil {
				continue
			}
			inScopeSet[t.Name] = struct{}{}
		}
		for _, name := range parents {
			if _, ok := inScopeSet[name]; ok {
				inScope = append(inScope, name)
			}
		}
	} else {
		inScope = parents
	}
	if len(inScope) == 0 {
		return nil
	}

	return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
		"%w: %s",
		errInheritanceTableRefused, formatInheritanceRefusal(inScope),
	))
}

// formatInheritanceRefusal renders the operator-facing refusal
// message, mirroring [formatPartitionedRefusal]: name the concrete
// state, explain the duplication mechanism, list the recovery paths.
func formatInheritanceRefusal(parents []string) string {
	var b strings.Builder
	if len(parents) == 1 {
		fmt.Fprintf(&b, "table %q has INHERITS children in the source. ", parents[0])
	} else {
		b.WriteString("tables ")
		for i, n := range parents {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q", n)
		}
		b.WriteString(" have INHERITS children in the source. ")
	}
	b.WriteString("Sluice does not support old-style (non-declarative) inheritance: the children are ordinary " +
		"tables sluice copies independently, while reading the parent (a SELECT without ONLY) ALSO returns " +
		"every child's rows — so proceeding would land the child data twice (flattened into the parent's " +
		"target table AND in each child's), silently. ")
	b.WriteString("Recovery: ")
	b.WriteString("(a) exclude the parent(s) via `--exclude-table=<parent>`; the children copy as individual " +
		"tables — first verify the parent stores no rows of its own (`SELECT count(*) FROM ONLY <parent>`), " +
		"because those rows would not be copied; ")
	b.WriteString("(b) exclude the CHILDREN instead to deliberately flatten the hierarchy — all rows land in " +
		"the parent's target table (only valid when keys cannot collide across children); ")
	b.WriteString("(c) scope the migration to the non-inheriting subset via `--include-table`. ")
	b.WriteString("Recreating the INHERITS hierarchy on the target is a manual post-migration step either way")
	return b.String()
}
