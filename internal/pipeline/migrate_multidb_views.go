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
// views (already through the view-emit preflight, so a refusal still fires
// before that namespace copies anything) to a final pass that runs once
// every namespace's tables exist.

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

// createDeferredViews is the fan-out's final views pass over every
// namespace that deferred at least one view.
//
// A view can also read a VIEW in another namespace, so a namespace whose
// views fail is retried in a later round as long as some namespace made
// progress in the round before. The loop ends when every namespace
// succeeded or a round made no progress, and then it fails loudly naming
// every namespace still failing — a view that cannot be created is never
// skipped. Within one namespace, [migcore.RunViewsPhase] keeps its own
// view-on-view retry passes, and CREATE OR REPLACE VIEW (both writers)
// makes re-running a partially created namespace harmless.
func createDeferredViews(ctx context.Context, target ir.Engine, all []namespaceViews) error {
	var pending []namespaceViews
	total := 0
	for _, nv := range all {
		if len(nv.views) > 0 {
			pending = append(pending, nv)
			total += len(nv.views)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	slog.InfoContext(ctx, "multi-namespace: creating deferred views",
		slog.Int("namespaces", len(pending)), slog.Int("views", total))
	for len(pending) > 0 {
		var next []namespaceViews
		var errs []error
		for _, nv := range pending {
			if err := nv.create(ctx, target); err != nil {
				next = append(next, nv)
				errs = append(errs, fmt.Errorf("pipeline: create views for database %q: %w", nv.database, err))
			}
		}
		if len(next) == len(pending) {
			if len(errs) == 1 {
				return errs[0]
			}
			return errors.Join(errs...)
		}
		pending = next
	}
	return nil
}

// create opens a schema writer on nv's target routing and runs the views
// phase over its views.
func (nv namespaceViews) create(ctx context.Context, target ir.Engine) error {
	sw, err := target.OpenSchemaWriter(ctx, nv.targetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	defer migcore.CloseIf(sw)
	migcore.ApplyTargetSchema(sw, nv.targetSchema)
	if err := migcore.RunViewsPhase(ctx, &ir.Schema{Views: nv.views}, sw); err != nil {
		return migcore.WrapWithHint(migcore.PhaseViews, err)
	}
	return nil
}
