// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// Deferred views for the multi-namespace fan-out (Bug 317).
//
// The views twin of the deferred foreign-key pass: a view in one namespace
// routinely reads another namespace's tables (a reporting schema of views
// over the application schema), and both fan-outs — `migrate` and the
// `sync start` cold start — copy namespaces one at a time in lexicographic
// order, so a view created inline fails whenever its namespace sorts
// before the one it reads. Each per-namespace pass therefore hands its
// views to a final pass that runs once every namespace's tables exist.
//
// The views were through the view-emit preflight in their own namespace's
// pass, so a refusal fires before THAT namespace copies anything — but in
// a fan-out, namespace N's preflight runs after namespaces 1..N-1 have
// copied. That is the same per-namespace scope every fan-out preflight
// has (audit backlog C-6).

// deferredViews collects one migrate fan-out namespace's views. Carried on
// the per-database [Migrator] clone; nil on every single-database run.
type deferredViews struct {
	views []*ir.View
}

// take moves schema's views into d and returns a copy of schema without
// them, so the inline views phase has nothing to create. The caller's
// schema value is not mutated. A nil receiver is the single-database
// (non-deferring) case and returns schema unchanged.
func (d *deferredViews) take(schema *ir.Schema) *ir.Schema {
	if d == nil || schema == nil {
		return schema
	}
	d.views = append(d.views[:0], schema.Views...)
	return withoutViews(schema)
}

// pending reports how many views d holds. Nil-safe.
func (d *deferredViews) pending() int {
	if d == nil {
		return 0
	}
	return len(d.views)
}

// withoutViews returns a shallow copy of schema with no views.
func withoutViews(schema *ir.Schema) *ir.Schema {
	stripped := *schema
	stripped.Views = nil
	return &stripped
}

// namespaceViews is one namespace's deferred views plus the target routing
// they are created under — the same DSN / target schema its tables were
// created under.
type namespaceViews struct {
	database     string
	targetDSN    string
	targetSchema string
	views        []*ir.View
}

// pendingView is one view still to create in the deferred pass.
type pendingView struct {
	ns   int // index into the namespaces slice
	view *ir.View
	err  error // last failure, reported if the pass gives up
}

// createDeferredViews is the fan-out's final views pass.
//
// It works per VIEW, not per namespace: every view still pending is tried
// once per round, a created view is never issued again, and the pass
// continues while a round creates at least one view. A view can read a
// view in another namespace, in either direction (A={a2, a1→B.b1},
// B={b1→A.a2, b2→A.a1} completes in two rounds), so per-namespace retry
// would both re-issue views already created and give up on graphs a
// later round completes. Not re-issuing matters on Postgres: a
// materialized view has no CREATE OR REPLACE, so a second CREATE of one
// fails "already exists".
//
// When a round creates nothing, the pass fails loudly naming every view
// still pending with its last error — a view that cannot be created is
// never skipped. A view the reader marked unportable ([ir.View.Refusal])
// refuses before any view is created, as [migcore.RunViewsPhase] does.
//
// Across runs (a --resume after this pass failed) the pass starts from
// the full list again; the Postgres writer skips a materialized view that
// already exists with the same definition, and CREATE OR REPLACE VIEW
// covers plain views on both writers.
func createDeferredViews(ctx context.Context, target ir.Engine, namespaces []namespaceViews) error {
	var pending []pendingView
	var refused []error
	for i, nv := range namespaces {
		for _, v := range nv.views {
			if v == nil {
				continue
			}
			if v.Refusal != "" {
				refused = append(refused, fmt.Errorf("database %q: %s", nv.database, v.Refusal))
			}
			pending = append(pending, pendingView{ns: i, view: v})
		}
	}
	if len(refused) > 0 {
		return migcore.WrapWithHint(migcore.PhaseViews, fmt.Errorf("pipeline: views phase: %w", errors.Join(refused...)))
	}
	if len(pending) == 0 {
		return nil
	}
	slog.InfoContext(ctx, "multi-namespace: creating deferred views", slog.Int("views", len(pending)))

	writers := make(map[int]ir.SchemaWriter)
	defer func() {
		for _, sw := range writers {
			migcore.CloseIf(sw)
		}
	}()
	for len(pending) > 0 {
		var next []pendingView
		for _, p := range pending {
			sw, ok := writers[p.ns]
			if !ok {
				var err error
				if sw, err = namespaces[p.ns].openWriter(ctx, target); err != nil {
					return err
				}
				writers[p.ns] = sw
			}
			single := &ir.Schema{Views: []*ir.View{p.view}}
			p.err = migcore.RunDDLPhaseWithReparentRetry(ctx, "views", sw, func(ctx context.Context) error {
				return sw.CreateViews(ctx, single)
			})
			if p.err != nil {
				next = append(next, p)
			}
		}
		if len(next) == len(pending) {
			errs := make([]error, 0, len(next))
			for _, p := range next {
				errs = append(errs, fmt.Errorf("pipeline: create views for database %q: view %q: %w",
					namespaces[p.ns].database, p.view.Name, p.err))
			}
			return migcore.WrapWithHint(migcore.PhaseViews, errors.Join(errs...))
		}
		pending = next
	}
	slog.InfoContext(ctx, "multi-namespace: deferred views created")
	return nil
}

// openWriter opens a schema writer on nv's target routing.
func (nv namespaceViews) openWriter(ctx context.Context, target ir.Engine) (ir.SchemaWriter, error) {
	sw, err := target.OpenSchemaWriter(ctx, nv.targetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect,
			fmt.Errorf("pipeline: open target schema writer for database %q: %w", nv.database, err))
	}
	migcore.ApplyTargetSchema(sw, nv.targetSchema)
	return sw, nil
}
