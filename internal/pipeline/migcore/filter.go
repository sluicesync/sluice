// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

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
//
// TableFilter lives in migcore (not pipeline root) so both the migrate
// orchestrator and the carved backup/restore domain can name it as a
// struct-field type without a root import (audit 3.7b). The view- and
// database-filter siblings stay in pipeline root — the carved cluster
// does not consume them.

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

// EffectiveTableFilter merges engine-supplied default exclusion
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
func EffectiveTableFilter(filter TableFilter, source ir.Engine, sourceDSN string) (effective TableFilter, addedDefaults []string) {
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

// ApplyTableFilter mutates schema.Tables in place, retaining only
// the tables the filter allows, and prunes standalone sequences whose
// owning table the filter excluded (see
// [dropSequencesOwnedByFilteredTables]). Logs the count at info level
// so operators can verify the filter matched what they expected. An
// all-empty result is treated as user error (the filter excluded
// every table) and surfaces a clear error.
//
// No-op when the filter is empty: avoids a noisy info line on every
// migration where no filter is configured.
func ApplyTableFilter(ctx context.Context, schema *ir.Schema, filter TableFilter) error {
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
	dropSequencesOwnedByFilteredTables(ctx, schema, filter)
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

// dropSequencesOwnedByFilteredTables prunes schema.Sequences of every
// standalone sequence whose owning table the filter excluded, WARN-
// logging each drop. Unowned standalone sequences always pass through.
//
// Why (audit N-4): a sequence carrying OwnedByTable/OwnedByColumn makes
// the target writer emit `ALTER SEQUENCE … OWNED BY table.column` after
// the tables phase (the postgres writer's bindSequenceOwners). With the
// owner filtered out, that ALTER references a table that was never
// created and the run dies with 42P01 — deterministically failing the
// shipped copy-table-subset use case for any source with a re-optioned
// serial outside the subset. Dropping the WHOLE sequence rather than
// just its ownership is deliberate: OWNED BY ties the sequence's
// lifecycle to the excluded column, so carrying it unowned would
// silently change its semantics on the target; the WARN names the
// sequence and its excluded owner so an operator who wants it can widen
// the filter.
func dropSequencesOwnedByFilteredTables(ctx context.Context, schema *ir.Schema, filter TableFilter) {
	if len(schema.Sequences) == 0 {
		return
	}
	kept := schema.Sequences[:0]
	for _, seq := range schema.Sequences {
		if seq != nil && seq.OwnedByTable != "" && !filter.Allows(seq.OwnedByTable) {
			slog.WarnContext(
				ctx, "table filter dropped a standalone sequence owned by an excluded table (its OWNED BY would reference a table the filter left uncreated)",
				slog.String("sequence", seq.Name),
				slog.String("owned_by", seq.OwnedByTable+"."+seq.OwnedByColumn),
			)
			continue
		}
		kept = append(kept, seq)
	}
	schema.Sequences = kept
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
