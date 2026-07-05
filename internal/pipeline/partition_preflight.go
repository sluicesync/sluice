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

// Bug 100 (v0.92.0). PG native declarative partitioning
// (`CREATE TABLE … PARTITION BY RANGE|LIST|HASH (…)`) used to land
// on the target as a plain heap: the partition key declaration was
// silently dropped, the partition children disappeared from the
// migration narrative, AND the parent's composite PRIMARY KEY (which
// in a partitioned table must include every partition-key column)
// was silently dropped along with the partitioning. Worse, the
// children — which are individual heap tables in `information_schema.
// tables` — were also copied as ordinary BASE TABLEs, so the operator
// either lost the children (if filtered) or duplicated the data
// (parent flat copy + N child copies).
//
// The right answer is sluice does not yet support PG declarative
// partitioning; v0.92.0 turns the silent-flatten into a loud refusal
// at preflight, naming the partitioned-parent tables and listing the
// operator-actionable recovery paths. Proper partition support is a
// roadmap candidate, not a hotfix.

// errPartitionedTableRefused is the sentinel for the Bug 100 loud
// refusal. Wrapped with per-table detail.
var errPartitionedTableRefused = errors.New("pipeline: source schema contains PG declaratively-partitioned table(s) — sluice does not yet support partition-aware migration")

// partitionPreflightProber is the optional engine-side surface for
// detecting declarative partitioning. PG implements
// (`SchemaReader.PartitionedTables`); MySQL doesn't (its inheritance-
// style partitioning is a different concept and out of scope here).
//
// Engines that don't implement the surface (MySQL, every non-CDC
// path) are silently skipped — the opportunistic-skip posture matches
// [preflightRLS] and [preflightSourceReplication].
type partitionPreflightProber interface {
	PartitionedTables(ctx context.Context) ([]string, error)
}

// preflightPartitionedTables runs the partitioning preflight against
// the source schema reader. Returns nil when:
//
//   - The source doesn't declare [ir.Capabilities.PostgresBackend]
//     (PG declarative partitioning is a PG-server concept; MySQL
//     silently skips, mirroring [preflightSourceReplication]'s
//     capability gate).
//   - The handle doesn't implement [partitionPreflightProber] (the
//     opportunistic-skip posture matches [preflightRLS]).
//   - No table in the active schema is partitioned.
//   - Every partitioned parent is excluded via the operator's
//     [migcore.TableFilter] (so a `--exclude-table=parent` operator-supplied
//     workaround actually works, instead of refusing on a table the
//     operator already excluded).
//
// Returns a wrapped [errPartitionedTableRefused] when at least one
// partitioned parent table is in-scope. The message names every
// offending parent (sorted) and lists the three operator-actionable
// recovery paths.
func preflightPartitionedTables(ctx context.Context, handle any, sourceCaps ir.Capabilities, schema *ir.Schema) error {
	if !sourceCaps.PostgresBackend {
		return nil
	}
	prober, ok := handle.(partitionPreflightProber)
	if !ok {
		return nil
	}
	partitioned, err := prober.PartitionedTables(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
			"pipeline: partition preflight: probe source for partitioned tables: %w", err,
		))
	}
	if len(partitioned) == 0 {
		return nil
	}

	// Filter against the in-scope table set so an operator who's
	// already passed `--exclude-table=<parent>` isn't refused on the
	// table they already excluded. The schema arg reflects post-filter
	// state.
	var inScope []string
	if schema != nil {
		inScopeSet := map[string]struct{}{}
		for _, t := range schema.Tables {
			if t == nil {
				continue
			}
			inScopeSet[t.Name] = struct{}{}
		}
		for _, name := range partitioned {
			if _, ok := inScopeSet[name]; ok {
				inScope = append(inScope, name)
			}
		}
	} else {
		inScope = partitioned
	}
	if len(inScope) == 0 {
		return nil
	}

	return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf(
		"%w: %s",
		errPartitionedTableRefused, formatPartitionedRefusal(inScope),
	))
}

// formatPartitionedRefusal renders the operator-facing refusal
// message. Mirrors [formatRLSRefusal] / [formatReplicationRefusal]:
// name the concrete state (the partitioned parent tables), explain
// the mechanism (silent-flatten before this preflight), and list
// every operator-actionable recovery path.
func formatPartitionedRefusal(parents []string) string {
	var b strings.Builder
	if len(parents) == 1 {
		fmt.Fprintf(&b, "table %q is declared `PARTITION BY` in the source. ", parents[0])
	} else {
		b.WriteString("tables ")
		for i, n := range parents {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q", n)
		}
		b.WriteString(" are declared `PARTITION BY` in the source. ")
	}
	b.WriteString("Sluice does not yet support partition-aware migration: the partitioned parent's partition-key " +
		"declaration, partition children, and composite PRIMARY KEY (which in a partitioned table must include " +
		"every partition-key column) would all be silently dropped on the target, AND the child tables would be " +
		"separately copied as ordinary heaps — so without this refusal you'd get either data loss (children " +
		"excluded) or data duplication (parent flat copy + N child copies). ")
	b.WriteString("Recovery: ")
	b.WriteString("(a) exclude the partitioned parent(s) via `--exclude-table=<parent>`; the children remain " +
		"in scope and will copy as individual heap tables (you'll need to recreate the partitioned-parent " +
		"hierarchy on the target manually post-migration); ")
	b.WriteString("(b) exclude the partitioned tables AND their children via `--include-table` to scope the " +
		"migration to the non-partitioned subset; ")
	b.WriteString("(c) on a same-engine PG → PG run, use `pg_dump --schema-only` + `pg_restore` to land the " +
		"partition hierarchy first, then `sluice migrate --schema-already-applied` for the data. ")
	b.WriteString("Native partition-aware support is roadmap-tracked but not yet shipped")
	return b.String()
}
