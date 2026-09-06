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
	"strconv"
	"strings"
	"sync"

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

	// census, when non-nil, records every table name the ENGINE-SIDE
	// scope push-down was asked about — the full source universe, before
	// anything was hidden. [UnmatchedPatterns] needs that universe, and
	// on Postgres it cannot get it from the post-read schema.
	//
	// WHY THIS EXISTS (Bug 273, a v0.142.0 regression). The Bug-76
	// push-down hands the Postgres reader `filter.Allows` via
	// [ApplyTableScope], and readTables SKIPS a scoped-out table
	// entirely, so it never reaches [ApplyTableFilter]. That was
	// harmless while the post-read prune was only PRUNING — the comment
	// at the push-down still says "the post-read TableFilter remains the
	// authoritative prune", and for pruning it is true. v0.142.0 made
	// that same post-read view answer a different question ("did this
	// pattern match anything?"), and for THAT question the view is
	// missing exactly the rows the answer depends on: an
	// `--exclude-table` pattern that worked perfectly looked unmatched,
	// so the new warning fired on every correct exclusion on Postgres.
	//
	// A pointer so a copied TableFilter still writes to one census —
	// this value is passed around by copy at a dozen call sites, and
	// threading a census parameter through all of them would have been a
	// larger, riskier change than the fix it carries.
	census *scopeCensus

	// engineDefaults are the patterns [EffectiveTableFilter] merged in on the
	// ENGINE's behalf (PlanetScale's `_vt_*` shadow-table prefix today), not
	// the operator's own.
	//
	// They are never reported as unmatched, and this field is what makes that
	// true. [UnmatchedPatterns] used to carry a comment SAYING engine
	// defaults "are never reported here -- see EffectiveTableFilter", and
	// that comment was simply false: EffectiveTableFilter merges the defaults
	// into Exclude, and the census reads Exclude. A PlanetScale database with
	// no `_vt_*` shadow tables — the quiet, healthy case — therefore warned
	// on every run that a pattern the operator never typed had matched
	// nothing. Two independent audit workers found it on the same day.
	//
	// The invariant is now enforced rather than asserted, which is the whole
	// difference: a comment claiming a property is a hypothesis until
	// something fails when it stops holding.
	engineDefaults map[string]bool
}

// scopeCensus records the table names the engine-side push-down was asked
// about. Guarded because a reader is free to walk its catalog
// concurrently; today's do not, and a census that silently raced would be
// a poor thing to discover later.
type scopeCensus struct {
	mu    sync.Mutex
	names map[string]bool
	order []string
}

// record adds one name. A SET rather than a slice: the census is fed from two
// sources (the engine push-down and each fan-out pass's own schema) which
// overlap heavily, and a multi-database run would otherwise grow it once per
// table per pass for no benefit.
func (c *scopeCensus) record(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.names == nil {
		c.names = map[string]bool{}
	}
	if !c.names[name] {
		c.names[name] = true
		c.order = append(c.order, name)
	}
}

// seen returns the accumulated universe in first-seen order, so a report
// derived from it is stable run to run.
func (c *scopeCensus) seen() []string {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
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
	return TableFilter{Include: include, Exclude: exclude, census: &scopeCensus{}}, nil
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
// bareName returns the last dot-separated segment of a pattern -- the form a
// table filter actually matches. Used only to render the remedy for an
// unmatched schema-qualified pattern; it never changes what matches.
func bareName(pattern string) string {
	if i := strings.LastIndex(pattern, "."); i >= 0 && i+1 < len(pattern) {
		return pattern[i+1:]
	}
	return pattern
}

// UnmatchedPatterns returns the operator-supplied patterns that matched NONE
// of the given table names, in the order supplied.
//
// WHY (Bug 272). A filter pattern that matches nothing is silent, and for
// --exclude-table it FAILS OPEN: the operator believes a table is excluded, it
// is not, and its rows are copied at exit 0 with no warning. The regression
// cycle found it with --exclude-table=public.pii, which copied the PII table
// and every row.
//
// The dominant cause is schema qualification: these patterns match the BARE
// table name, while sluice own diagnostics print names qualified
// ("- public.nopk_t: no-primary-key"), so the qualified form is exactly what
// an operator copies back into a flag. A typo or a dead glob produces the same
// silence.
//
// Deliberately NOT fixed by accepting the qualified form. These patterns are
// matched without schema context, so stripping a qualifier would make
// "other_schema.pii" also match "public.pii" -- over-excluding on the exclude
// path and, worse, over-INCLUDING on the include path. Reporting the mismatch
// is strictly safe; changing what matches is not.
//
// Engine-supplied default exclusions are not the caller supplied patterns and
// are never reported here -- enforced by the engineDefaults set, not merely
// asserted. This comment previously claimed the property while nothing
// implemented it, and PlanetScale runs with no _vt_* shadow tables warned on
// every clean run as a result.
func (f TableFilter) UnmatchedPatterns(tableNames []string) []string {
	patterns := f.Include
	if len(patterns) == 0 {
		patterns = f.Exclude
	}
	var unmatched []string
	for _, p := range patterns {
		var hit bool
		for _, n := range tableNames {
			if ok, err := path.Match(p, n); err == nil && ok {
				hit = true
				break
			}
		}
		if !hit && !f.engineDefaults[p] {
			unmatched = append(unmatched, p)
		}
	}
	return unmatched
}

// LooksSchemaQualified reports whether a pattern carries a dot, which for an
// unmatched pattern is very likely the reason. Split out so the caller can
// give that case its own remedy instead of a generic one.
func LooksSchemaQualified(pattern string) bool {
	return strings.Contains(pattern, ".")
}

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
	// Carry the census: a merged filter is the SAME run, and dropping the
	// pointer here would silently restore Bug 273 on any source that
	// contributes engine-default exclusions (PlanetScale's `_vt_*`).
	addedSet := make(map[string]bool, len(added))
	for _, p := range added {
		addedSet[p] = true
	}
	return TableFilter{Include: nil, Exclude: merged, census: filter.census, engineDefaults: addedSet}, added
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
	return applyTableFilter(ctx, schema, filter, true)
}

// ApplyTableFilterQuiet is [ApplyTableFilter] for ONE PASS of a multi-database
// fan-out: it prunes identically but does not report unmatched patterns.
//
// WHY IT EXISTS (Bug 273, fourth arm). The fan-out calls the filter door once
// per database, over that database's tables only, so a pattern naming a table
// in database A looks unmatched while database B is being processed — one
// false warning per pass, on a warning whose entire value is that it is
// trustworthy. Reporting per pass cannot be made correct: no single pass has
// the universe.
//
// THE CALLER OWES A REPORT. Every fan-out that uses this MUST call
// [ReportUnmatchedPatterns] once when it has finished every database, or the
// Bug 272 protection is silently gone for multi-database runs — which would
// be a worse outcome than the false fire it replaces.
func ApplyTableFilterQuiet(ctx context.Context, schema *ir.Schema, filter TableFilter) error {
	return applyTableFilter(ctx, schema, filter, false)
}

// ReportUnmatchedPatterns emits the unmatched-pattern warning from the
// accumulated census, for callers that suppressed it per pass.
//
// The census is the union of every table name seen across the whole fan-out —
// both the engine push-down (Postgres) and each pass's own schema (every
// engine) — so this is the only place in a multi-database run where the
// question "did this pattern match anything?" has a complete universe to be
// answered against.
func ReportUnmatchedPatterns(ctx context.Context, filter TableFilter) {
	if filter.IsEmpty() {
		return
	}
	reportUnmatched(ctx, filter, filter.census.seen())
}

func applyTableFilter(ctx context.Context, schema *ir.Schema, filter TableFilter, report bool) error {
	if filter.IsEmpty() {
		return nil
	}
	original := len(schema.Tables)
	// The universe for the unmatched-pattern census is the SOURCE's table
	// set, which is not the same thing as the schema that arrives here.
	// Where an engine implements the Bug-76 scope push-down (Postgres
	// does; MySQL does not), a scoped-out table was already dropped by the
	// reader and never appears below — so a census taken from
	// schema.Tables alone reports every WORKING exclusion as unmatched.
	// That was Bug 273. The push-down predicate records what it was asked
	// about; union it in, and de-duplicate because a kept table appears in
	// both.
	seen := map[string]bool{}
	allNames := make([]string, 0, original)
	addName := func(n string) {
		if seen[n] {
			return
		}
		seen[n] = true
		allNames = append(allNames, n)
	}
	for _, t := range schema.Tables {
		addName(t.Name)
		// Into the census as well as this pass's local list: a multi-database
		// fan-out reports ONCE at the end, and on an engine with no push-down
		// (MySQL) each pass's own schema is the only place those names appear.
		filter.census.record(t.Name)
	}
	for _, n := range filter.census.seen() {
		addName(n)
	}
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
	// Bug 272 report. Suppressed on a fan-out pass: no single pass has the
	// universe, so reporting per pass produces one false warning per database
	// (Bug 273, fourth arm). ApplyTableFilterQuiet callers owe a single
	// ReportUnmatchedPatterns when the fan-out completes.
	if report {
		reportUnmatched(ctx, filter, allNames)
	}
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

// PreflightTableReads consults reader's optional
// [ir.TableReadPreflighter] surface for every table remaining in
// schema — call it immediately AFTER [ApplyTableFilter], so a doomed
// table the operator --exclude-table'd never blocks the run (Bug 188),
// while an INCLUDED doomed table refuses loudly up front, before any
// DDL or data moves, with every violation named in one pass. Readers
// without the surface are a no-op.
func PreflightTableReads(reader ir.SchemaReader, schema *ir.Schema) error {
	p, ok := reader.(ir.TableReadPreflighter)
	if !ok || schema == nil {
		return nil
	}
	var errs []error
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		if err := p.PreflightTableRead(t.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("pipeline: %d table(s) cannot be read from this source (--exclude-table routes around them): %w",
		len(errs), errors.Join(errs...))
}

// reportUnmatched emits the operator warning for every supplied pattern that
// matched none of allNames. Extracted from the filter door so a fan-out can
// call it ONCE against the accumulated universe instead of once per pass.
func reportUnmatched(ctx context.Context, filter TableFilter, allNames []string) {
	// Bug 272: a pattern that matched NOTHING is the silent half of this
	// filter, and on the exclude path it fails OPEN -- the operator believes a
	// table is excluded, it is not, and its rows are copied at exit 0. Found
	// with --exclude-table=public.pii, which copied the PII table entirely.
	//
	// WARN rather than refuse, deliberately: a pattern naming a table absent
	// from THIS source is legitimate (one config across environments), so
	// refusing would break a working configuration. What was missing is that
	// the operator was never told.
	unmatched := filter.UnmatchedPatterns(allNames)
	if len(unmatched) == 0 {
		return
	}
	{
		mode := "--exclude-table"
		effect := "those tables are NOT excluded and their rows WILL be copied"
		if len(filter.Include) > 0 {
			mode = "--include-table"
			effect = "those patterns contribute nothing to the allow-list"
		}
		for _, pat := range unmatched {
			remedy := "check the spelling against the source table list"
			if LooksSchemaQualified(pat) {
				remedy = "table patterns match the BARE table name, not a schema-qualified one — " +
					"write " + strconv.Quote(bareName(pat)) + " instead of " + strconv.Quote(pat) +
					" (sluice diagnostics print names schema-qualified, which is the usual reason " +
					"this happens); use --include-schema / --exclude-schema to scope namespaces"
			}
			// TABLE-FILTER-PATTERN-UNMATCHED is the grep-stable handle, the
			// same convention as POSITION-MODE / STALE-CAPTURE-FUNCTION /
			// CHANGE-LOG-PAGE-UNORDERED. A warning an operator cannot search
			// their logs for is a warning they find only by reading every
			// line — and this one matters most in the case where they are not
			// reading, because the run exits 0 either way.
			slog.WarnContext(
				ctx, "TABLE-FILTER-PATTERN-UNMATCHED: table filter pattern matched NOTHING",
				slog.String("flag", mode),
				slog.String("pattern", pat),
				slog.String("effect", effect),
				slog.String("remedy", remedy),
			)
		}
	}
}
