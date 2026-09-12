// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Shard-placement probing for a PlanetScale Neki target.
//
// # The state this exists to refuse
//
// On a sharded Neki database, a table's ROUTING (which shard a keyed query is
// sent to) and its PLACEMENT (which shard the rows are physically on) are set
// by different operations. Writing the data topology changes routing;
// __neki.reshard_create moves data. Do the first without the second — which
// nothing prevents, and which __neki.validate_data_topology reports as valid —
// and the two disagree.
//
// Measured on a live cluster 2026-09-10 (reported to PlanetScale). On a table in
// that state:
//
//   - a scattering read returns every row;
//   - an equality-routed read on the shard key returns ZERO;
//   - and INSERT ... ON CONFLICT (pk) DO UPDATE INSERTS instead of updating,
//     because the conflict is looked for on the routed shard, which does not
//     hold the original row. Two rows then claim one primary key.
//
// That last one is why this is a REFUSAL and not a warning. It is exactly the
// statement sluice's CDC applier and idempotent bulk-copy writer use, so a
// target in this state does not fail — it silently accumulates duplicate rows
// at exit 0, against a PRIMARY KEY the operator reasonably believes is
// enforced.
//
// # How it is detected, and why not by computing the routing key
//
// The obvious check is to compute the row's routing key and compare it
// against the topology's key ranges. Neki catalogues a per-type hash family
// (__neki.neki_xxh3_64_*) that looks purpose-built for exactly that, and it
// is NOT CALLABLE ("opcode not implemented: user-defined function
// neki_xxh3_64_int8") — it appears to exist for the router's own use. So the
// check is behavioural instead, which is arguably better evidence anyway: ask
// the target the same question two ways and see whether the answers agree.
//
//	SELECT <pk cols> FROM t LIMIT 1              -- scatters; yields a real row
//	SELECT count(*) FROM t WHERE <pk> = <those>  -- routes by shard key
//
// A correctly-placed table answers 1. A mis-placed one answers 0 for a row it
// just handed us. There is no third possibility that is healthy.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
//   - NEKI ONLY. Every other PostgreSQL endpoint returns notApplicable and
//     the preflight is a no-op, so no ordinary target pays for this.
//   - TABLES THAT ALREADY HOLD ROWS. An empty target has nothing mis-placed:
//     rows sluice writes go to the hash-correct shard by construction. So a
//     fresh cold start into an empty database is not covered and does not
//     need to be; what this catches is a PRE-EXISTING target — a populated
//     one under --force-cold-start, a warm resume, or a sync against a
//     database somebody sharded underneath.
//   - TABLES WITH A PRIMARY KEY. Without one there is no keyed read to
//     compare against, and no upsert for the hazard to ride in on.
//   - It samples ONE row per table, not all of them. It answers "does routing
//     agree with placement for this table", which is a per-table property of
//     the topology, not a per-row one.

// placementVerdict is the outcome for one table.
type placementVerdict int

const (
	placementOK placementVerdict = iota
	placementNotApplicable
	placementMisplaced
)

// ShardPlacementMismatch implements the target-side probe the pipeline's
// preflight calls. It reports the first table whose routing and placement
// disagree, or "" when every checked table agrees.
//
// A probe that cannot RUN is not a verdict: an error reading the target is
// returned as an error, never as a mismatch, so a transient failure cannot
// manufacture a refusal.
func (w *RowWriter) ShardPlacementMismatch(ctx context.Context, tables []*ir.Table) (mismatched string, err error) {
	if !w.isNeki {
		return "", nil
	}
	for _, t := range tables {
		if t == nil || t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
			continue
		}
		v, verr := w.probeTablePlacement(ctx, t)
		if verr != nil {
			return "", verr
		}
		if v == placementMisplaced {
			return t.Name, nil
		}
	}
	return "", nil
}

// probeTablePlacement runs the two-question check for one table.
func (w *RowWriter) probeTablePlacement(ctx context.Context, t *ir.Table) (placementVerdict, error) {
	ref := w.qualified(t.Name)

	pk := t.PrimaryKey.Columns
	cols := make([]string, len(pk))
	for i, c := range pk {
		// An expression entry carries no column name, so there is no keyed
		// predicate to build. Skip the table rather than guess — a probe that
		// cannot be constructed yields no verdict.
		if c.Column == "" {
			return placementNotApplicable, nil
		}
		cols[i] = quoteIdent(c.Column)
	}
	sel := strings.Join(cols, ", ")

	// 1. A scattering read: no shard-key predicate, so the router asks every
	//    shard and returns whatever physically exists.
	sample := make([]any, len(pk))
	dest := make([]any, len(pk))
	for i := range sample {
		dest[i] = &sample[i]
	}
	row := w.db.QueryRowContext(ctx, "SELECT "+sel+" FROM "+ref+" LIMIT 1")
	if err := row.Scan(dest...); err != nil {
		// No rows (the common, healthy case for a fresh target), or a read
		// failure. Either way this table yields no verdict.
		return placementNotApplicable, nil //nolint:nilerr // an unreadable/empty table is not a mismatch
	}

	// 2. The same row, asked for by key: this one routes.
	preds := make([]string, len(pk))
	for i, c := range pk {
		preds[i] = quoteIdent(c.Column) + " = $" + fmt.Sprint(i+1)
	}
	var n int64
	q := "SELECT pg_catalog.count(*) FROM " + ref + " WHERE " + strings.Join(preds, " AND ")
	if err := w.db.QueryRowContext(ctx, q, sample...).Scan(&n); err != nil {
		return placementNotApplicable, fmt.Errorf("postgres: probe shard placement for %q: %w", t.Name, err)
	}
	if n == 0 {
		return placementMisplaced, nil
	}
	return placementOK, nil
}

// qualified renders a schema-qualified table reference for this writer.
func (w *RowWriter) qualified(table string) string {
	if w.schema == "" {
		return quoteIdent(table)
	}
	return quoteIdent(w.schema) + "." + quoteIdent(table)
}
