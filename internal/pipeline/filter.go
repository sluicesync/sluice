// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Table filtering lives at the orchestrator boundary, not in engine
// readers. Two reasons.
//
// First, "which tables are migrated" is a product-level decision the
// operator makes on the CLI; pushing it down into [ir.SchemaReader]
// implementations would force every engine to grow the same
// include/exclude logic and risk per-engine drift in glob semantics or
// case sensitivity. Engines stay narrow: read everything, hand the
// schema up, let the orchestrator decide what to do with it.
//
// Second, the same shape needs to apply to bulk-copy (where the list
// of tables comes from [ir.Schema]) and to CDC (where the source of
// truth is a stream of [ir.Change] events). Filtering at the
// orchestrator means a single struct describes both — the migrate
// path prunes [ir.Schema.Tables], the streamer drops dispatched
// changes whose [ir.Change.QualifiedName] doesn't pass.
//
// Glob support uses the stdlib [path.Match] semantics: literal names
// match by exact equality (also via path.Match — a pattern with no
// metacharacters is exact), "audit_*" matches any name starting with
// "audit_", "?" is a single character, and "[abc]" is a character
// class. The shape was chosen for the common operator pattern of
// "drop everything in the 'audit_' family"; full regex was not
// implemented because path.Match covers the observed need without
// the footgun of an unanchored regex.

import (
	"context"
	"fmt"
	"log/slog"
	"path"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// ViewFilter selects which views participate in the migration / sync /
// preview / diff. Same shape and semantics as [migcore.TableFilter]; views are
// filtered independently of tables so an operator can opt out of view
// processing entirely (`--skip-views`) or include / exclude a subset
// without touching the table filter.
type ViewFilter struct {
	Include []string
	Exclude []string
}

// NewViewFilter mirrors [migcore.NewTableFilter]: validates mutual exclusion of
// Include / Exclude and pattern shape.
func NewViewFilter(include, exclude []string) (ViewFilter, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return ViewFilter{}, fmt.Errorf(
			"pipeline: --include-view and --exclude-view are mutually exclusive (got include=%v exclude=%v)",
			include, exclude,
		)
	}
	for _, p := range include {
		if _, err := path.Match(p, ""); err != nil {
			return ViewFilter{}, fmt.Errorf("pipeline: invalid view include pattern %q: %w", p, err)
		}
	}
	for _, p := range exclude {
		if _, err := path.Match(p, ""); err != nil {
			return ViewFilter{}, fmt.Errorf("pipeline: invalid view exclude pattern %q: %w", p, err)
		}
	}
	return ViewFilter{Include: include, Exclude: exclude}, nil
}

// IsEmpty reports whether the view filter has no rules.
func (f ViewFilter) IsEmpty() bool {
	return len(f.Include) == 0 && len(f.Exclude) == 0
}

// Allows reports whether viewName is included by the filter. Same
// semantics as [migcore.TableFilter.Allows].
func (f ViewFilter) Allows(viewName string) bool {
	if len(f.Include) > 0 {
		return matchesAny(f.Include, viewName)
	}
	if len(f.Exclude) > 0 {
		return !matchesAny(f.Exclude, viewName)
	}
	return true
}

// applyViewFilter mutates schema.Views in place, retaining only the
// views the filter allows. When skipAll is true, every view is dropped
// regardless of the filter — used to wire `--skip-views` from the CLI.
//
// Unlike [migcore.ApplyTableFilter], an empty result is NOT an error: many
// schemas have no views, and a filter that drops them all is a
// legitimate operator choice. Schema-with-no-views was already a
// supported state before view-support landed.
func applyViewFilter(ctx context.Context, schema *ir.Schema, filter ViewFilter, skipAll bool) {
	if skipAll {
		original := len(schema.Views)
		schema.Views = nil
		if original > 0 {
			slog.InfoContext(
				ctx, "view processing skipped (--skip-views)",
				slog.Int("excluded", original),
			)
		}
		return
	}
	if filter.IsEmpty() {
		return
	}
	original := len(schema.Views)
	kept := schema.Views[:0]
	for _, v := range schema.Views {
		if v == nil {
			continue
		}
		if filter.Allows(v.Name) {
			kept = append(kept, v)
		}
	}
	schema.Views = kept
	slog.InfoContext(
		ctx, "view filter applied",
		slog.Int("matched", len(kept)),
		slog.Int("excluded", original-len(kept)),
	)
}

// DatabaseFilter selects which source databases participate in a
// multi-database fan-out migration (ADR-0074). Same shape and semantics
// as [migcore.TableFilter]: at most one of Include / Exclude is non-empty
// ([path.Match] glob patterns), validated at construction. The zero
// value is the "everything passes" filter.
//
// System databases (information_schema, performance_schema, mysql, sys)
// are excluded by the engine's [ir.DatabaseLister] before the filter
// ever sees the list, so an operator's `--include-database '*'` /
// `--all-databases` never picks them up.
type DatabaseFilter struct {
	Include []string
	Exclude []string
}

// NewDatabaseFilter mirrors [migcore.NewTableFilter]: validates mutual exclusion
// of Include / Exclude and that each pattern is well-formed under
// [path.Match].
func NewDatabaseFilter(include, exclude []string) (DatabaseFilter, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return DatabaseFilter{}, fmt.Errorf(
			"pipeline: --include-database and --exclude-database are mutually exclusive (got include=%v exclude=%v)",
			include, exclude,
		)
	}
	for _, p := range include {
		if _, err := path.Match(p, ""); err != nil {
			return DatabaseFilter{}, fmt.Errorf("pipeline: invalid include-database pattern %q: %w", p, err)
		}
	}
	for _, p := range exclude {
		if _, err := path.Match(p, ""); err != nil {
			return DatabaseFilter{}, fmt.Errorf("pipeline: invalid exclude-database pattern %q: %w", p, err)
		}
	}
	return DatabaseFilter{Include: include, Exclude: exclude}, nil
}

// IsEmpty reports whether the filter has no rules.
func (f DatabaseFilter) IsEmpty() bool {
	return len(f.Include) == 0 && len(f.Exclude) == 0
}

// Allows reports whether database participates. Same [path.Match]
// semantics as [migcore.TableFilter.Allows].
func (f DatabaseFilter) Allows(database string) bool {
	if len(f.Include) > 0 {
		return matchesAny(f.Include, database)
	}
	if len(f.Exclude) > 0 {
		return !matchesAny(f.Exclude, database)
	}
	return true
}

// filterChanges wraps in with a goroutine that drops [ir.Change]
// events whose table is excluded by filter. Used by the streamer's
// dispatch loop so the [ir.ChangeApplier] never sees events for
// tables the operator filtered out.
//
// Returns in unchanged when filter is empty: no goroutine, no
// channel hop, zero overhead on the streaming hot path. The
// per-event drop log is intentionally at debug level — info-level
// would spam aggregators on a busy stream.
//
// Position-advancement caveat: this layer drops events without
// committing their position to the target. The next not-dropped
// event's apply commits its own position, which (because positions
// are monotonically increasing) implicitly skips past every dropped
// event in between. A stream that consists exclusively of dropped
// events for a long time accumulates position lag bounded by the
// source's WAL/binlog retention; in normal mixed-traffic workloads
// this is invisible.
func filterChanges(ctx context.Context, in <-chan ir.Change, filter migcore.TableFilter) <-chan ir.Change {
	if filter.IsEmpty() {
		return in
	}
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				if !changeAllowed(c, filter) {
					slog.DebugContext(
						ctx, "cdc event dropped by table filter",
						slog.String("table", c.QualifiedName()),
					)
					continue
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// changeAllowed reports whether the change's table is permitted by
// the filter. Filter matching uses the unqualified table name only:
// schema-prefixed names ("public.users") are reduced to "users"
// before the lookup. Operators write filter patterns against the
// table name they see in CREATE TABLE / SHOW TABLES, not against
// the schema-qualified form.
//
// Source-tx boundary events ([ir.TxBegin], [ir.TxCommit]) carry no
// table reference (QualifiedName == "") and bypass the filter
// entirely — they're applier-internal signals (ADR-0027), not
// per-table data, and dropping them would defeat the
// transaction-aware batch flush.
func changeAllowed(c ir.Change, filter migcore.TableFilter) bool {
	switch c.(type) {
	case ir.TxBegin, ir.TxCommit:
		return true
	}
	name := c.QualifiedName()
	// Strip "schema." prefix if present — filter patterns target
	// unqualified names. The IR's QualifiedName returns either
	// "schema.table" or "table" depending on whether Schema is set.
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			name = name[i+1:]
			break
		}
	}
	return filter.Allows(name)
}

// matchesAny returns true when name matches at least one pattern
// under [path.Match]. Errors from path.Match (only possible from
// malformed character classes that migcore.NewTableFilter would already
// have rejected) are silently treated as non-match.
func matchesAny(patterns []string, name string) bool {
	for _, p := range patterns {
		ok, err := path.Match(p, name)
		if err == nil && ok {
			return true
		}
	}
	return false
}
