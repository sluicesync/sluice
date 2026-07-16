// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// applyInferredTypes runs the opt-in, data-validated rich-type inference
// (`--infer-types`, ADR-0144) for a SQLite/D1 source: it selects name-hinted
// candidates whose conservative type is the right source family, asks the
// source engine (via [ir.InferredTypeValidator]) to exhaustively validate each,
// and promotes only the conforming ones by injecting an
// [translate.InferredOverride] — which rides the SAME override decode the
// operator's `--type-override` path uses. An explicit `--type-override` always
// wins (the operator is authoritative); inference never touches an
// explicitly-overridden column.
//
// The pipeline stays engine-neutral: it knows the name-hints and the IR
// families, not the SQL. A source that does not implement the validator is
// refused loudly (inference is SQLite/D1-only). The result composes byte-
// identically with the rest of the schema-translate phase: with no promotions
// it returns the schema unchanged.
func (m *Migrator) applyInferredTypes(ctx context.Context, sr ir.SchemaReader, schema *ir.Schema) (*ir.Schema, error) {
	validator, ok := sr.(ir.InferredTypeValidator)
	if !ok {
		return nil, errors.New(
			"--infer-types is only supported for SQLite/D1 sources " +
				"(the source engine does not provide validated rich-type inference)",
		)
	}

	explicit := explicitOverrideSet(m.Mappings)
	var overrides []translate.InferredOverride
	candidates := 0

	for _, tbl := range schema.Tables {
		for _, col := range tbl.Columns {
			target, hint, isCand := inferTypeCandidate(col)
			if !isCand {
				continue
			}
			if explicit[overrideKey(tbl.Name, col.Name)] {
				// Explicit operator override wins; inference stays out of it.
				continue
			}
			candidates++
			conforms, resolved, validated, err := validator.ValidateInferredType(ctx, tbl.Name, col.Name, target)
			if err != nil {
				return nil, fmt.Errorf("pipeline: infer-types validate %s.%s: %w", tbl.Name, col.Name, err)
			}
			if !conforms {
				logInferKept(ctx, tbl.Name, col.Name, hint, col.Type, validated)
				continue
			}
			logInferPromoted(ctx, tbl.Name, col.Name, col.Type, resolved, validated)
			overrides = append(overrides, translate.InferredOverride{
				Table:  tbl.Name,
				Column: col.Name,
				Type:   resolved,
			})
		}
	}

	slog.InfoContext(ctx, "infer-types: validated rich-type candidates",
		slog.Int("candidates", candidates),
		slog.Int("promoted", len(overrides)))

	return translate.ApplyInferredOverrides(schema, overrides)
}

// inferTypeCandidate reports whether col is a rich-type inference candidate: its
// NAME hints a richer target AND its current (conservative) type is the right
// SOURCE family. It returns the candidate FAMILY target the validator should
// check (the temporal target is the tz-unresolved [ir.Timestamp]{} marker — the
// engine resolves timestamptz-vs-timestamp from the data) and the matched hint
// label for the report. A column already resolved to a rich type by the
// reader (a declared BOOLEAN/DATETIME — ADR-0129) is NOT a candidate: its
// family is already Boolean/Timestamp, not Integer/Text, so it falls through.
func inferTypeCandidate(col *ir.Column) (target ir.Type, hint string, ok bool) {
	name := strings.ToLower(col.Name)
	switch col.Type.(type) {
	case ir.Integer:
		if h, matched := matchHint(name, booleanHints); matched {
			return ir.Boolean{}, h, true
		}
	case ir.Text:
		// Order is the documented precedence for the rare multi-hint name.
		if h, matched := matchHint(name, temporalHints); matched {
			return ir.Timestamp{}, h, true
		}
		if h, matched := matchHint(name, jsonHints); matched {
			return ir.JSON{Binary: true}, h, true
		}
		if h, matched := matchHint(name, uuidHints); matched {
			return ir.UUID{}, h, true
		}
	}
	return nil, "", false
}

// hintRule matches a lower-cased column name and carries a human label for the
// report (e.g. "is_*", "*_id", "metadata").
type hintRule struct {
	label string
	match func(name string) bool
}

func hintPrefix(p string) hintRule {
	return hintRule{label: p + "*", match: func(n string) bool { return strings.HasPrefix(n, p) }}
}

func hintSuffix(s string) hintRule {
	return hintRule{label: "*" + s, match: func(n string) bool { return strings.HasSuffix(n, s) }}
}

func hintExact(e string) hintRule {
	return hintRule{label: e, match: func(n string) bool { return n == e }}
}

// matchHint returns the first matching rule's label. The hint sets are the
// ADR-0144 table; they only narrow candidates — the data validation is the
// safety gate, so a too-broad hint costs at most one cheap aggregate.
var (
	booleanHints  = []hintRule{hintPrefix("is_"), hintPrefix("has_"), hintSuffix("_flag")}
	temporalHints = []hintRule{hintSuffix("_at"), hintSuffix("_time"), hintExact("created"), hintExact("updated")}
	jsonHints     = []hintRule{hintSuffix("_json"), hintExact("metadata"), hintExact("payload"), hintExact("settings"), hintExact("attributes")}
	uuidHints     = []hintRule{hintSuffix("_id"), hintSuffix("_uuid"), hintExact("uuid"), hintExact("guid")}
)

func matchHint(name string, rules []hintRule) (string, bool) {
	for _, r := range rules {
		if r.match(name) {
			return r.label, true
		}
	}
	return "", false
}

// explicitOverrideSet keys the operator's explicit `--type-override` columns so
// inference can skip them (explicit wins). The key is table+column.
func explicitOverrideSet(mappings []config.Mapping) map[string]bool {
	set := make(map[string]bool, len(mappings))
	for _, mp := range mappings {
		set[overrideKey(mp.Table, mp.Column)] = true
	}
	return set
}

// overrideKey is the (table, column) map key. The NUL separator can't appear in
// an identifier, so it can't collide a `a` + `b.c` with `a.b` + `c`.
func overrideKey(table, column string) string {
	return table + "\x00" + column
}

// logInferPromoted emits the loud, structured promotion line (the operator
// opted in; tell them exactly what changed). A jsonb promotion additionally
// notes the document normalization (whitespace/key-order) the ADR calls out.
func logInferPromoted(ctx context.Context, table, column string, from, to ir.Type, validated int64) {
	slog.InfoContext(ctx, "infer-types: promoted column to a richer target type",
		slog.String("table", table),
		slog.String("column", column),
		slog.String("from", from.String()),
		slog.String("to", to.String()),
		slog.Int64("validated", validated))
	if j, isJSON := to.(ir.JSON); isJSON && j.Binary {
		slog.InfoContext(ctx, "infer-types: jsonb promotion normalizes the document "+
			"(the JSON value is equal; stored bytes differ from the source text — whitespace/key-order; "+
			"a column holding any duplicate-key document is never promoted, because jsonb would keep "+
			"only the last duplicate — such a column stays text)",
			slog.String("table", table),
			slog.String("column", column))
	}
}

// logInferKept emits the considered-but-kept-safe line: the column matched a
// name-hint but the data did not conform, so it stays at its safe type. This is
// the `cus_abc123` *_id case — reported, never promoted.
func logInferKept(ctx context.Context, table, column, hint string, kept ir.Type, validated int64) {
	reason := "one or more non-conforming values"
	if validated == 0 {
		reason = "no non-NULL values to validate"
	}
	slog.InfoContext(ctx, "infer-types: kept column at its safe type",
		slog.String("table", table),
		slog.String("column", column),
		slog.String("hint", hint),
		slog.String("kept", kept.String()),
		slog.Int64("non_null_values", validated),
		slog.String("reason", reason))
}
