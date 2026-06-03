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
	"errors"
	"fmt"
	"log/slog"
	"path"

	"sluicesync.dev/sluice/internal/ir"
)

// TableFilter decides whether a table participates in the migration
// or sync stream. At most one of Include / Exclude is non-empty; the
// orchestrator validates this at construction time. Match patterns
// are stdlib [path.Match] glob style: literal names match exactly;
// "audit_*" matches any name starting with "audit_".
//
// The zero value is the "everything passes" filter — nil/empty
// Include and Exclude means no filtering. That matches the previous
// behaviour for callers who don't supply a filter.
type TableFilter struct {
	// Include, when non-empty, is the allow-list: a table name must
	// match at least one pattern to participate. Mutually exclusive
	// with Exclude.
	Include []string

	// Exclude, when non-empty, is the deny-list: a table name that
	// matches any pattern is dropped. Mutually exclusive with
	// Include.
	Exclude []string
}

// NewTableFilter validates that Include and Exclude are not both
// populated and that every pattern is well-formed under
// [path.Match]. Returns a usable TableFilter or a clear error
// suitable for surfacing to the operator.
func NewTableFilter(include, exclude []string) (TableFilter, error) {
	if len(include) > 0 && len(exclude) > 0 {
		return TableFilter{}, fmt.Errorf(
			"pipeline: --include-table and --exclude-table are mutually exclusive (got include=%v exclude=%v)",
			include, exclude,
		)
	}
	for _, p := range include {
		if _, err := path.Match(p, ""); err != nil {
			return TableFilter{}, fmt.Errorf("pipeline: invalid include pattern %q: %w", p, err)
		}
	}
	for _, p := range exclude {
		if _, err := path.Match(p, ""); err != nil {
			return TableFilter{}, fmt.Errorf("pipeline: invalid exclude pattern %q: %w", p, err)
		}
	}
	return TableFilter{Include: include, Exclude: exclude}, nil
}

// IsEmpty reports whether the filter has no rules — i.e. whether
// every table passes. Useful for skipping the post-prune
// "filter applied" log line when there's nothing to report.
func (f TableFilter) IsEmpty() bool {
	return len(f.Include) == 0 && len(f.Exclude) == 0
}

// Allows reports whether table participates in the migration. The
// match check uses [path.Match] semantics; an invalid pattern is
// treated as "no match" (a defensive choice — NewTableFilter rejects
// invalid patterns up front, so this branch is only reachable if a
// caller bypasses the constructor).
func (f TableFilter) Allows(tableName string) bool {
	if len(f.Include) > 0 {
		return matchesAny(f.Include, tableName)
	}
	if len(f.Exclude) > 0 {
		return !matchesAny(f.Exclude, tableName)
	}
	return true
}

// effectiveTableFilter merges engine-supplied default exclusion
// patterns into the operator's filter when the engine implements
// [ir.DefaultTableExcluder]. Used today for PlanetScale's `_vt_*`
// Vitess shadow-table prefix (Bug 22) — operators almost never want
// those in a migration or stream, and forgetting to exclude them
// generates quiet write churn against the target.
//
// Merge rules:
//
//   - Operator supplied --include-table: defaults are skipped
//     (include-mode is an explicit allow-list; the operator opted
//     into a precise table set and engine defaults shouldn't undermine
//     that). If the operator wants `_vt_*` tables, --include-table is
//     the override.
//   - Operator supplied --exclude-table or no filter: defaults are
//     appended to Exclude. Patterns the operator already specified
//     are deduplicated by string equality.
//   - Engine doesn't implement [ir.DefaultTableExcluder]: filter
//     returned unchanged.
//
// Returns the merged filter and the slice of patterns that came from
// engine defaults (for the "applying engine default exclusions" log
// line, distinct from the "operator filter applied" line).
func effectiveTableFilter(filter TableFilter, source ir.Engine, sourceDSN string) (effective TableFilter, addedDefaults []string) {
	excluder, ok := source.(ir.DefaultTableExcluder)
	if !ok {
		return filter, nil
	}
	defaults := excluder.DefaultExcludePatterns(sourceDSN)
	if len(defaults) == 0 {
		return filter, nil
	}
	if len(filter.Include) > 0 {
		// Explicit allow-list — engine defaults don't apply.
		return filter, nil
	}
	added := make([]string, 0, len(defaults))
	excludeSet := make(map[string]struct{}, len(filter.Exclude))
	for _, p := range filter.Exclude {
		excludeSet[p] = struct{}{}
	}
	merged := make([]string, 0, len(filter.Exclude)+len(defaults))
	merged = append(merged, filter.Exclude...)
	for _, p := range defaults {
		if _, dup := excludeSet[p]; dup {
			continue
		}
		merged = append(merged, p)
		added = append(added, p)
	}
	if len(added) == 0 {
		return filter, nil
	}
	return TableFilter{Include: nil, Exclude: merged}, added
}

// applyTableFilter mutates schema.Tables in place, retaining only
// the tables the filter allows. Logs the count at info level so
// operators can verify the filter matched what they expected. An
// all-empty result is treated as user error (the filter excluded
// every table) and surfaces a clear error.
//
// No-op when the filter is empty: avoids a noisy info line on every
// migration where no filter is configured.
func applyTableFilter(ctx context.Context, schema *ir.Schema, filter TableFilter) error {
	if filter.IsEmpty() {
		return nil
	}
	original := len(schema.Tables)
	kept := schema.Tables[:0]
	for _, t := range schema.Tables {
		if filter.Allows(t.Name) {
			kept = append(kept, t)
		}
	}
	schema.Tables = kept
	slog.InfoContext(
		ctx, "table filter applied",
		slog.Int("matched", len(kept)),
		slog.Int("excluded", original-len(kept)),
	)
	if len(kept) == 0 {
		return errors.New("pipeline: table filter excluded every source table; nothing to migrate (check --include-table / --exclude-table)")
	}
	return nil
}

// ViewFilter selects which views participate in the migration / sync /
// preview / diff. Same shape and semantics as [TableFilter]; views are
// filtered independently of tables so an operator can opt out of view
// processing entirely (`--skip-views`) or include / exclude a subset
// without touching the table filter.
type ViewFilter struct {
	Include []string
	Exclude []string
}

// NewViewFilter mirrors [NewTableFilter]: validates mutual exclusion of
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
// semantics as [TableFilter.Allows].
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
// Unlike [applyTableFilter], an empty result is NOT an error: many
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
func filterChanges(ctx context.Context, in <-chan ir.Change, filter TableFilter) <-chan ir.Change {
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
func changeAllowed(c ir.Change, filter TableFilter) bool {
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
// malformed character classes that NewTableFilter would already
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
