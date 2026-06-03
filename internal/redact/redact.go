// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package redact implements PII redaction strategies and a per-column
// rule registry. Phase 1 of roadmap item 15a (GitHub issue #24).
//
// # Pipeline integration
//
// A Registry sits between the IR row read (source) and the
// per-engine `prepareValue` shaping step (target). The pipeline's
// `redactRow` helper iterates a row's (column, value) pairs and
// substitutes the value when a Strategy is registered. Empty
// Registry (the default) is a no-op: every Redact call returns the
// input verbatim. See ADR-0032's optional-engine-surface pattern;
// Phase 1 mirrors that shape for the value-shaping path.
//
// # Strategies (Phase 1)
//
//   - [Null]      replace with NULL (refuses on NOT NULL columns)
//   - [Static]    replace with a literal constant
//   - [Hash]      SHA-256 (stateless) or HMAC-SHA256 (keyed)
//   - [Truncate]  keep first N runes (string columns only)
//
// Phase 2 will add format-preserving (`mask:`), tokenize, and
// randomize strategies; Phase 3 JSON-path; Phase 4 cross-stream
// keyset persistence. The Strategy interface is stable across
// phases; future additions extend the strategy list without
// touching the registry or pipeline integration.
//
// # Determinism
//
// `Null` and `Static` are obviously deterministic. `Hash` with
// SHA-256 is stateless and produces the same hex output for the
// same input across runs and machines. `Hash` with HMAC-SHA256
// and `tokenize:dict` require a Key resolved from the operator
// keyset (`--keyset-source`, PII Phase 4 / ADR-0041); two streams
// sharing the same keyset key produce identical surrogates. The
// keyset is a startup snapshot — rotation takes effect on the next
// process restart only (no hot-reload).
//
// # Case-folding
//
// Registry keys are lowercased ("schema.table.column"). Phase 1's
// simplest workable behaviour. Documented limitation: operators on
// PG with case-sensitive identifiers (e.g. `CREATE TABLE "Users"`)
// see redactions matched only when the operator declares them in
// lowercase form. Phase 2+ can revisit if real-world demand
// surfaces.
package redact

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Strategy is the per-column redaction policy. Implementations are
// usually stateless (Null, Static, Hash:sha256, Truncate) or carry
// minimal state (Hash:hmac-sha256 with its Key). The pipeline's
// `redactRow` calls [Strategy.Redact] for every (column, value)
// pair when a Strategy is registered for the column.
//
// Implementations MUST be safe for concurrent use across goroutines
// — the pipeline can call Redact from the bulk-copy reader's reader-
// per-table goroutines simultaneously. None of the Phase 1
// strategies hold mutable state; new strategies that need state
// should synchronise it explicitly.
type Strategy interface {
	// Name returns a stable string identifier for the strategy,
	// used by schema-preview annotation and the audit log line.
	// Form: "null", "static:<elided>", "hash:sha256",
	// "hash:hmac-sha256", "truncate:<n>".
	Name() string

	// Redact returns the redacted value for the input. col is the
	// target column's IR metadata (Type, Nullable) so the strategy
	// can validate at runtime (e.g., Null refuses NOT NULL columns;
	// Truncate refuses non-string types). Returns the value as
	// `any` because [ir.Row] values are untyped at the wire layer;
	// the per-engine prepareValue downstream handles final type
	// coercion.
	//
	// seed is the per-row replay-stable RNG seed for `randomize:*`
	// strategies, derived from streamID + table + column + PK values
	// via [DeriveRowSeed]. Phase 2.c (v0.59.0) randomize generators
	// REQUIRE a non-nil seed and refuse with an operator-actionable
	// error when one is not supplied (a table without a primary key
	// cannot produce replay-stable values, so randomize:* is refused
	// at preflight time). Every other strategy (Phase 1 / Phase 2.a
	// / Phase 2.b) IGNORES the seed parameter — the existing
	// hash/static/mask/truncate strategies are already deterministic
	// without needing a per-row seed. Callers that don't have a PK
	// or stream-id context pass nil; randomize strategies refuse,
	// everything else proceeds.
	//
	// Phase 1 strategies never return a wrapped error chain — they
	// return a fresh error with the column identity in the message
	// when refusal applies. The caller (pipeline.redactRow) is
	// responsible for adding context (table, position, etc.) to the
	// outer error before propagating.
	Redact(col *ir.Column, val any, seed []byte) (any, error)
}

// Registry maps "schema.table.column" → Strategy. Lookups are O(1)
// via a flat map keyed by the lowercased schema/table/column triple.
// Empty Registry returns nil from Get and true from Empty; the
// pipeline's `redactRow` short-circuits the wrap entirely in that
// case to keep the no-redactions hot path zero-cost.
type Registry struct {
	rules map[string]Strategy
}

// New returns a new empty Registry. Rules are added via [Set].
func New() *Registry {
	return &Registry{rules: make(map[string]Strategy)}
}

// Set registers strategy for the given column triple. Schema +
// table + column are case-folded to lowercase before storage; see
// the package comment for the case-folding policy. Re-registering
// an existing triple is allowed and silently overwrites — the CLI
// layer should emit a WARN before calling Set when an operator-
// declared duplicate is detected.
//
// strategy must not be nil; pass an explicit Null to mean "redact
// with NULL". A nil strategy panics in [Set] to fail loud at the
// configuration step rather than at the first row that hits the
// rule.
func (r *Registry) Set(schema, table, column string, strategy Strategy) {
	if strategy == nil {
		panic(fmt.Sprintf("redact: Registry.Set called with nil strategy for %s.%s.%s", schema, table, column))
	}
	r.rules[registryKey(schema, table, column)] = strategy
}

// Get returns the Strategy for the column triple, or nil if no rule
// is registered. The pipeline's `redactRow` interprets nil as
// "pass the value through verbatim".
//
// Lookup order: (schema, table, column) keyed first; on miss, fall
// back to ("", table, column) — the operator-bare CLI form that
// matches any source schema. This matters at CDC apply time where
// engine-emitted change events carry a non-empty `Schema`:
//
//   - MySQL VStream populates `ir.Insert.Schema` with the keyspace
//     name (e.g., `sluice-validation-mysql-source`).
//   - Postgres CDC populates `Schema` with the relation's schema
//     (typically `public`).
//
// The operator-bare CLI form `--redact users.email=hash:sha256`
// registers the rule with `schema=""`; without the fallback, the
// engine-emitted-schema lookup misses and CDC rows pass through
// unredacted while bulk-copy rows (which use `table.Schema=""` on
// MySQL sources) match. Bug 58 fix in v0.54.1.
//
// Operators wanting strict per-schema rules (`customer_svc.users.email`
// vs `audit_svc.users.email`) still get the precise behaviour: the
// schema-qualified Set takes precedence over the bare fallback when
// both are registered.
func (r *Registry) Get(schema, table, column string) Strategy {
	if r == nil || len(r.rules) == 0 {
		return nil
	}
	if s, ok := r.rules[registryKey(schema, table, column)]; ok {
		return s
	}
	// Schema-qualified miss — fall back to the bare operator form so
	// CDC engine-emitted schemas match operator-bare CLI rules.
	if schema != "" {
		return r.rules[registryKey("", table, column)]
	}
	return nil
}

// Empty reports whether the Registry has no rules. Used by the
// pipeline's `redactRow` to short-circuit the per-row wrap when no
// rules are configured (the common case for operators not running
// in PII-redaction mode).
func (r *Registry) Empty() bool {
	return r == nil || len(r.rules) == 0
}

// Rules returns the registered (schema, table, column, strategy)
// quadruples in deterministic lexical order of the lowercased key.
// Used by the schema-preview annotation pass and the audit log
// line. The returned slice is freshly allocated; callers may sort
// or mutate it without affecting the Registry.
func (r *Registry) Rules() []Rule {
	if r == nil || len(r.rules) == 0 {
		return nil
	}
	out := make([]Rule, 0, len(r.rules))
	for k, s := range r.rules {
		parts := strings.SplitN(k, ".", 3)
		// SplitN guarantees at least one element; pad to 3 so
		// indexing below is safe for any malformed key (shouldn't
		// happen because Set produces well-formed keys).
		for len(parts) < 3 {
			parts = append(parts, "")
		}
		out = append(out, Rule{
			Schema:   parts[0],
			Table:    parts[1],
			Column:   parts[2],
			Strategy: s,
		})
	}
	// Sort by the registry key (lexical) for deterministic order.
	// The audit log line + schema-preview annotation both want
	// stable ordering across runs.
	stableSortByKey(out)
	return out
}

// Rule is a single redaction rule's full description. Exposed by
// [Registry.Rules] for schema-preview annotation + audit logging.
type Rule struct {
	Schema   string
	Table    string
	Column   string
	Strategy Strategy
}

// ApplyRow walks the row's column-name → value pairs and replaces
// values whose column triple has a matching strategy in the
// Registry. Modifies the row map in place. Returns a wrapped error
// on the first strategy refusal.
//
// Phase 1.5 entry point for CDC apply-path redaction: the engine
// applier calls ApplyRow before dispatching each change, since the
// applier doesn't always have the full target column metadata
// available at apply time. The col metadata passed to
// [Strategy.Redact] uses Nullable=true as a permissive default —
// if a Null strategy would silently produce nil for a NOT NULL
// target column, the engine catches it at INSERT time with a
// loud duplicate-key / constraint-violation error and ADR-0038's
// retry loop classifies it appropriately. The bulk-copy path's
// [Strategy] callers (in pipeline.redactRow) pass full *ir.Column
// metadata and get the earlier strategy-level refusal.
//
// pkColumns + streamID together produce the per-(row, column) seed
// passed to [Strategy.Redact] for randomize:* strategies (Phase 2.c,
// v0.59.0). When pkColumns is empty and a registered rule for this
// table is a randomize:* strategy, ApplyRow refuses with an
// operator-actionable error — replay-stable randomization requires
// a primary key. streamID may be empty (migrate path / chain
// handoff); the seed remains stable for a given (table, column,
// pkValues) tuple within that empty-streamID space. Phase 1 and
// Phase 2.a / 2.b strategies ignore the seed entirely.
//
// Zero-cost on nil/empty Registry: returns nil immediately without
// touching the row.
func (r *Registry) ApplyRow(schema, table string, pkColumns []string, row ir.Row, streamID string) error {
	if r.Empty() {
		return nil
	}
	for name, val := range row {
		strategy := r.Get(schema, table, name)
		if strategy == nil {
			continue
		}
		col := &ir.Column{Name: name, Nullable: true}
		var seed []byte
		if needsSeed(strategy) {
			if len(pkColumns) == 0 {
				return fmt.Errorf("redact %s.%s.%s via %s: randomize strategies require a primary key on the table (no PK columns available at apply time); use hash:sha256, mask:*, or static: instead",
					schema, table, name, strategy.Name())
			}
			pkValues := make([]any, len(pkColumns))
			for i, c := range pkColumns {
				pkValues[i] = row[c]
			}
			seed = DeriveRowSeed(streamID, table, name, pkColumns, pkValues)
		}
		newVal, err := strategy.Redact(col, val, seed)
		if err != nil {
			return fmt.Errorf("redact %s.%s.%s via %s: %w",
				schema, table, name, strategy.Name(), err)
		}
		row[name] = newVal
	}
	return nil
}

// needsSeed reports whether the strategy requires a per-row seed.
// Used by [Registry.ApplyRow] and pipeline.redactRow to decide
// whether to derive a seed and refuse on missing PK. Only the
// `randomize:*` family (Phase 2.c, v0.59.0) needs a seed; every
// other strategy is deterministic without one.
//
// Detection by Name() prefix ("randomize:") rather than a type
// switch keeps the redact package free of cross-strategy
// dependencies — a future strategy that wants seeded behaviour
// just names itself `randomize:<form>` and gets seed plumbing for
// free.
func needsSeed(s Strategy) bool {
	if s == nil {
		return false
	}
	return strings.HasPrefix(s.Name(), "randomize:")
}

// DeriveRowSeed produces a 32-byte SHA-256 seed for replay-safe
// `randomize:*` strategies (PII Phase 2.c, v0.59.0). The contract:
// same (streamID, table, column, pkColumns, pkValues) inputs
// produce the same seed every time, across runs and across machines.
// Different inputs produce different seeds.
//
// streamID may be empty (migrate path has no stream-id; chain
// handoff likewise). pkColumns MUST be non-empty for randomize:*
// rules; callers (ApplyRow / pipeline.redactRow) verify this and
// refuse loudly before reaching this function. pkValues holds the
// PK column values in pkColumns order.
//
// Canonical serialization of `any` is `fmt.Sprintf("%v", v)`. The
// operator's source data drives the values; we need only stable
// input → stable hash. Edge case: two PG NUMERIC values with
// different scales (`1.00` vs `1.0`) would format differently and
// thus seed differently — that's correct behaviour because the IR
// row values are distinct, not because we're trying to be clever.
//
// The format string is `streamID || "|" || table || "|" || column
// || "|" || pkCol1 || "=" || canonicalize(pkVal1) || "|" || ...`.
// The leading streamID + table + column ensure no two columns can
// produce identical seeds even when their PK values match; the
// trailing pkCol=val pairs guarantee per-row uniqueness within a
// column.
func DeriveRowSeed(streamID, table, column string, pkColumns []string, pkValues []any) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte(streamID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(table))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(column))
	for i, c := range pkColumns {
		_, _ = h.Write([]byte("|"))
		_, _ = h.Write([]byte(c))
		_, _ = h.Write([]byte("="))
		var v any
		if i < len(pkValues) {
			v = pkValues[i]
		}
		_, _ = fmt.Fprintf(h, "%v", v)
	}
	sum := h.Sum(nil)
	return sum
}

// ErrRandomizeRequiresPK is the sentinel cause for the randomize:*
// no-PK refusal. The pipeline's preflight uses this (via errors.Is)
// to detect randomize-on-no-PK and emit a cohesive operator-facing
// error grouping every offending rule. Strategies in the redact
// package return wrapping errors that surface this sentinel up the
// call chain.
var ErrRandomizeRequiresPK = errors.New("redact: randomize:* strategy requires a primary key on the table")

// registryKey produces the lowercased "schema.table.column" key
// for Set / Get lookups. Empty schema is allowed (some engines
// resolve schema implicitly); the resulting key starts with ".".
func registryKey(schema, table, column string) string {
	return strings.ToLower(schema) + "." + strings.ToLower(table) + "." + strings.ToLower(column)
}

// stableSortByKey sorts rules by their lowercased registry key.
// Pure-stdlib bubble-sort is sufficient — Rules() is called once
// at startup (audit log) and once per schema-preview run, both
// off the hot path; cardinality is bounded by operator-declared
// redaction rules (typically < 100).
//
// Using sort.Slice would pull in `sort` which we'd want anyway,
// but keeping this self-contained avoids the dependency creep —
// strategies.go's standard-library imports stay minimal.
func stableSortByKey(rules []Rule) {
	for i := 1; i < len(rules); i++ {
		for j := i; j > 0; j-- {
			a := registryKey(rules[j].Schema, rules[j].Table, rules[j].Column)
			b := registryKey(rules[j-1].Schema, rules[j-1].Table, rules[j-1].Column)
			if a >= b {
				break
			}
			rules[j], rules[j-1] = rules[j-1], rules[j]
		}
	}
}
